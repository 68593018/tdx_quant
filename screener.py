import os
import sys
import time
import subprocess

# -------------------------------------------------------------
# 1. 动态检测并自动静默安装 DuckDB 库 (极致顺滑的免运维体验)
# -------------------------------------------------------------
try:
    import duckdb
except ImportError:
    print("⏳ 检测到当前环境未安装 DuckDB 依赖，正在为你自动静默安装...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "duckdb"], 
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        import duckdb
        print("✅ DuckDB 依赖安装成功！立即启动极速选股引擎...\n")
    except Exception as e:
        print(f"❌ 自动安装 DuckDB 失败，请在终端手动运行 'pip install duckdb'。错误详情: {e}")
        sys.exit(1)

# -------------------------------------------------------------
# 2. 路径配置与全局变量
# -------------------------------------------------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_STORE_DIR = os.path.join(CURRENT_DIR, "data")
BLOCK_MAPPINGS_PATH = os.path.join(DATA_STORE_DIR, "block_mappings.parquet")

def run_screener():
    print("=" * 85)
    print("        通达信本地数据池 - DuckDB 极速多线程因子共振选股器 (Phase 4)")
    print("=" * 85)
    
    if not os.path.exists(DATA_STORE_DIR):
        print(f"❌ 错误: 本地数据目录 {DATA_STORE_DIR} 不存在。请先运行 python3 sync_market.py 同步数据。")
        return
        
    if not os.path.exists(BLOCK_MAPPINGS_PATH):
        print(f"❌ 错误: 板块映射库 {BLOCK_MAPPINGS_PATH} 不存在。请先运行 python3 sync_market.py 同步板块数据。")
        return

    t_start = time.perf_counter()
    
    # 动态匹配现有的数据文件前缀，规避因某些前缀（如 bj）不存在而引发的 DuckDB IO Error
    prefixes = ['sh', 'sz', 'bj']
    existing_files = os.listdir(DATA_STORE_DIR)
    patterns = []
    for prefix in prefixes:
        if any(f.startswith(prefix) and f.endswith('.parquet') for f in existing_files):
            patterns.append(f"{DATA_STORE_DIR}/{prefix}*.parquet")
            
    if not patterns:
        print("❌ 错误: 未在本地数据库中找到任何有效 .parquet 缓存文件。")
        return
        
    patterns_str = ", ".join(f"'{p}'" for p in patterns)
    
    # -------------------------------------------------------------
    # 3. 构造并执行核心因子计算与三表 JOIN 选股 SQL
    # -------------------------------------------------------------
    query_sql = f"""
    WITH raw_data AS (
        -- 1. 高速扫描全量个股的 Parquet 文件，提取文件名作为 symbol 标识
        SELECT 
            regexp_extract(filename, '([^/]+)\\.parquet$', 1) AS symbol,
            date,
            close_adj,
            volume
        FROM read_parquet([{patterns_str}], filename=true)
        -- 过滤只扫描个股，排除大盘指数和板块指数，以求得最精确的个股分析
        WHERE NOT (
            filename LIKE '%sh000%' OR 
            filename LIKE '%sh88%' OR 
            filename LIKE '%sz88%' OR 
            filename LIKE '%sz399%'
        )
    ),
    calculated_factors AS (
        -- 2. 窗口函数并行计算 20日均线 (MA20) 与 5日均量 (Vol_MA5)
        SELECT 
            symbol,
            date,
            close_adj,
            volume,
            AVG(close_adj) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
            AVG(volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS vol_ma5,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) as row_num
        FROM raw_data
    ),
    latest_data AS (
        -- 3. 提取最新一个交易日 (row_num = 1) 的全量因子数据进行分析
        SELECT * 
        FROM calculated_factors
        WHERE row_num = 1
    ),
    breakout_stocks AS (
        -- 4. 筛选“个股价格站上 MA20 + 今日成交量大于 5日均量 1.5倍”的放量突破个股
        SELECT 
            symbol,
            close_adj,
            ma20,
            volume,
            vol_ma5,
            (close_adj - ma20) / ma20 * 100 AS dev_ma20_pct,
            volume / vol_ma5 AS vol_ratio
        FROM latest_data
        WHERE 
            close_adj > ma20
            AND volume > 1.5 * vol_ma5
    ),
    stock_sector_mapping AS (
        -- 5. 从板块库中读取个股与概念板块的映射关系
        SELECT 
            code,
            market,
            block_name,
            block_category
        FROM read_parquet('{BLOCK_MAPPINGS_PATH}')
    ),
    sector_total_count AS (
        -- 5.5 计算每个板块的总股票数 (以求得高价值的突破股票占比)
        SELECT 
            block_name,
            COUNT(DISTINCT market || code) AS total_count
        FROM stock_sector_mapping
        WHERE block_category = 'concept'
        GROUP BY block_name
    ),
    stock_with_sectors AS (
        -- 6. 关联个股和它所属的板块
        SELECT 
            s.symbol,
            s.close_adj,
            s.vol_ratio,
            s.dev_ma20_pct,
            m.block_name,
            m.block_category
        FROM breakout_stocks s
        JOIN stock_sector_mapping m 
          ON substr(s.symbol, 3) = m.code 
         AND substr(s.symbol, 1, 2) = m.market
    ),
    sector_breakout_count AS (
        -- 7. 统计概念板块内的突破股票数，并 JOIN 获取板块总股数，计算占比
        SELECT 
            sws.block_name,
            COUNT(DISTINCT sws.symbol) AS breakout_count,
            MAX(stc.total_count) AS total_count,
            COUNT(DISTINCT sws.symbol) * 100.0 / MAX(stc.total_count) AS breakout_ratio
        FROM stock_with_sectors sws
        JOIN sector_total_count stc ON sws.block_name = stc.block_name
        WHERE sws.block_category = 'concept'
        GROUP BY sws.block_name
    ),
    resonance_results AS (
        -- 8. 核心风口筛选：只保留其所属板块中“今天有 3 只或 3 只以上股票同时突破”的概念共振股
        SELECT 
            sws.symbol,
            sws.close_adj,
            sws.vol_ratio,
            sws.dev_ma20_pct,
            sws.block_name,
            sbc.breakout_count,
            sbc.total_count,
            sbc.breakout_ratio
        FROM stock_with_sectors sws
        JOIN sector_breakout_count sbc ON sws.block_name = sbc.block_name
        WHERE sbc.breakout_count >= 3 AND sws.block_category = 'concept'
    )
    -- 9. 最终输出结果：合并单只股票所属的多个共振风口板块，并输出爆发股票数及爆发百分比占比
    SELECT 
        symbol,
        close_adj AS Close,
        vol_ratio AS Vol_Ratio,
        dev_ma20_pct AS Dev_MA20_Pct,
        string_agg(block_name || '(' || breakout_count || '只突破/占' || round(breakout_ratio, 1) || '%)', ', ') AS Resonance_Sectors
    FROM resonance_results
    GROUP BY symbol, close_adj, vol_ratio, dev_ma20_pct
    ORDER BY vol_ratio DESC
    """
    
    t_sql_start = time.perf_counter()
    
    # 建立 DuckDB 连接，并执行多线程 SQL
    con = duckdb.connect()
    # 强制开启多线程以榨干多核 CPU 性能
    con.execute(f"SET threads = {os.cpu_count()}")
    
    try:
        res_df = con.execute(query_sql).fetchdf()
    except Exception as e:
        print(f"❌ SQL 执行失败: {e}")
        return
        
    t_end = time.perf_counter()
    
    # -------------------------------------------------------------
    # 4. 高颜值打印选股战报与性能指标
    # -------------------------------------------------------------
    total_time = t_end - t_start
    sql_time = t_end - t_sql_start
    
    # 格式化日期显示最新交易日
    latest_date_query = con.execute(f"SELECT MAX(date) FROM read_parquet('{DATA_STORE_DIR}/sh600000.parquet')").fetchone()[0]
    latest_date_str = latest_date_query.strftime('%Y-%m-%d') if latest_date_query else "最新交易日"
    
    print(f"✅ 因子共振筛选跑通！策略分析最新交易日: 【{latest_date_str}】")
    print(f"📊 全市场扫描个股/指数: {len(os.listdir(DATA_STORE_DIR))} 个 Parquet 文件")
    print(f"⏱️ 极速性能指标:")
    print(f"  - ⚡ DuckDB 多线程 SQL 因子计算与 JOIN 筛选耗时: {sql_time:.4f} 秒")
    print(f"  - 🚀 引擎冷启动总耗时（含依赖检查、连接初始化、多表加载）: {total_time:.4f} 秒")
    
    print("\n" + "=" * 85)
    print(f"🏆 【黄金共振突破个股列表】 (共筛选出 {len(res_df)} 只黄金个股，按放量倍数降序排列):")
    print("=" * 85)
    
    if res_df.empty:
        print("   -- 今日全市场未筛选出符合“行业板块共振突破”的股票，建议空仓避险。 --")
        print("=" * 85)
        return
        
    # 精美打印格式化
    res_df['Symbol'] = res_df['symbol'].str.upper()
    res_df['Close'] = res_df['Close'].map(lambda x: f"{x:.2f} 元")
    res_df['Vol_Ratio'] = res_df['Vol_Ratio'].map(lambda x: f"{x:.2f} 倍")
    res_df['Dev_MA20_Pct'] = res_df['Dev_MA20_Pct'].map(lambda x: f"+{x:.2f}%")
    
    show_df = res_df[['Symbol', 'Close', 'Vol_Ratio', 'Dev_MA20_Pct', 'Resonance_Sectors']].head(30)
    
    # 调整 pandas 打印配置
    pd_width = 1000
    try:
        import pandas as pd
        pd.set_option('display.width', pd_width)
        pd.set_option('display.max_colwidth', 50)
        pd.set_option('display.colheader_justify', 'center')
    except ImportError:
        pass
        
    print(show_df.to_string(index=False))
    
    if len(res_df) > 30:
        print(f"\n   ... (余下 {len(res_df) - 30} 只股票已省略，共筛选出 {len(res_df)} 只股票) ...")
        
    print("=" * 85)
    print("💡 选股策略释义：")
    print("  1. 个股今日价格向上站上 20日均线 (MA20)，且日成交量放大到 5日均量 (Vol_MA5) 的 1.5 倍以上（主力突破）；")
    print("  2. 个股至少归属于一个概念板块，且该板块今日至少有 3 只股票同时发生放量突破，形成“板块资金风口 + 个股突破”的共振爆发多头结构。")
    print("=" * 85)
    
    # -------------------------------------------------------------
    # 5. 自动生成高颜值的 Markdown 报告文件 (.md)
    # -------------------------------------------------------------
    report_path = os.path.join(CURRENT_DIR, "breakout_report.md")
    try:
        md_content = f"""# 🚀 全市场资金共振突破选股报告

**分析交易日**：`{latest_date_str}`
**分析标的总数**：`{len(os.listdir(DATA_STORE_DIR))} 个`
**报告生成时间**：`{time.strftime('%Y-%m-%d %H:%M:%S')}`

---

## ⏱️ 极速因子引擎性能
* **DuckDB 多线程 SQL 因子计算与 JOIN 耗时**：`{sql_time:.4f} 秒`
* **引擎冷启动总耗时（含依赖、连接与加载）**：`{total_time:.4f} 秒`

---

## 🏆 黄金共振突破股列表
共筛选出 **{len(res_df)}** 只黄金个股，已按今日放量倍数降序排列：

| 序号 | 股票代码 | 最新收盘价 | 今日放量倍数 | MA20 偏离度 | 触发共振爆发板块（突破只数/板块总股数占比） |
| :---: | :---: | :---: | :---: | :---: | :--- |
"""
        for idx, row in res_df.iterrows():
            md_content += f"| {idx+1} | `{row['Symbol']}` | {row['Close']} | {row['Vol_Ratio']} | {row['Dev_MA20_Pct']} | {row['Resonance_Sectors']} |\n"
            
        md_content += """
---

## 💡 选股策略释义
> [!NOTE]
> 1. **均线向上突破**：个股最新收盘价站上 20日均线 (MA20)，代表短期趋势由弱转强；
> 2. **成交放量加速**：今日成交量放大到 5日均量 (Vol_MA5) 的 1.5 倍以上，显示主力资金明显介入抢筹；
> 3. **行业风口共振**：统计每个概念板块中，当天有多少只股票同时触发上述“突破”。**只保留其所属板块中“当天至少有 3 只股票同时突破”的成分股**，并计算出突破只数占该板块总股数的比例，从而完美锁定主力资金最抱团、集聚突破度（Breadth）最高的核心市场风口！
"""
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(md_content)
        print(f"\n💾 已成功将极速因子选股报告保存至: {report_path}")
    except Exception as e:
        print(f"⚠️ 保存 Markdown 报告失败: {e}")

if __name__ == "__main__":
    run_screener()
