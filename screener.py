import os
import sys
import json
import time
import subprocess

# -------------------------------------------------------------
# 1. 动态检测并自动静默安装 DuckDB 库 (免运维极致顺滑体验)
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
CONFIG_PATH = os.path.join(CURRENT_DIR, "config.json")
STRATEGIES_PATH = os.path.join(CURRENT_DIR, "strategies.json")

def load_strategies() -> dict:
    """从 strategies.json 配置文件中载入所有可用的 SQL 选股策略"""
    if not os.path.exists(STRATEGIES_PATH):
        print(f"❌ 错误: 策略配置文件 {STRATEGIES_PATH} 不存在！")
        sys.exit(1)
        
    try:
        with open(STRATEGIES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ 解析策略配置文件 {STRATEGIES_PATH} 失败: {e}")
        sys.exit(1)

def print_usage(strategies: dict):
    print("=" * 80)
    print("      通达信本地数据池 - DuckDB 极速多策略因子选股引擎 (Phase 4 - Config)")
    print("=" * 80)
    print("使用方法:")
    print("  1. 单策略选股:")
    print("     python3 screener.py [策略名称]")
    print("  2. 多策略求交集 (双重/多重过滤策略融合):")
    print("     python3 screener.py [策略A] [策略B] ...")
    print("\n目前支持的策略列表:")
    for key, val in strategies.items():
        print(f"  🔹 {key:<20} - {val['name']}")
        print(f"       👉 描述: {val['description']}")
    print("-" * 80)
    print("运行示例:")
    print("  python3 screener.py resonance_breakout    (运行资金集聚概念共振突破策略)")
    print("  python3 screener.py ma_long_sequence      (运行均线多头排列共振爆发策略)")
    print("  python3 screener.py dual_resonance        (SQL合并一步法运行双重共振黄金选股)")
    print("\n  python3 screener.py resonance_breakout ma_long_sequence")
    print("                                            (求两个独立策略选股结果的黄金交集)")
    print("=" * 80)

def execute_single_sql(con, query_sql: str) -> str:
    """极速执行单条 SQL"""
    try:
        return con.execute(query_sql).fetchdf()
    except Exception as e:
        print(f"❌ SQL 执行失败: {e}")
        sys.exit(1)

def load_stock_names(tdx_dir: str) -> dict:
    """从通达信 shs.tnf 和 szs.tnf 二进制缓存中极速解析股票代码与名称的映射关系"""
    names_map = {}
    if not tdx_dir or not os.path.exists(tdx_dir):
        return names_map
        
    hq_cache_dir = os.path.join(tdx_dir, "T0002", "hq_cache")
    tnf_files = [
        ("sh", os.path.join(hq_cache_dir, "shs.tnf")),
        ("sz", os.path.join(hq_cache_dir, "szs.tnf")),
        ("bj", os.path.join(hq_cache_dir, "bjs.tnf")),
    ]
    
    for market, path in tnf_files:
        if os.path.exists(path):
            try:
                with open(path, "rb") as f:
                    data = f.read()
                record_len = 360
                header_len = 50
                num_records = (len(data) - header_len) // record_len
                for i in range(num_records):
                    offset = header_len + i * record_len
                    record = data[offset : offset + record_len]
                    code = record[:6].decode("gbk", errors="ignore").split("\x00")[0].strip()
                    name = record[31:51].decode("gbk", errors="ignore").split("\x00")[0].strip()
                    if code and name and len(code) == 6:
                        symbol = f"{market}{code}"
                        names_map[symbol] = name
            except Exception as e:
                print(f"⚠️ 警告: 解析 {path} 失败: {e}")
    return names_map

def main():
    # 1. 载入策略配置文件
    strategies = load_strategies()
    
    # 2. 解析选股策略参数 (支持多策略传入求交集)
    selected_keys = []
    if len(sys.argv) >= 2:
        for arg in sys.argv[1:]:
            arg_clean = arg.strip().lower()
            if arg_clean in ['-h', '--help', 'help']:
                print_usage(strategies)
                return
            if arg_clean in strategies:
                selected_keys.append(arg_clean)
            else:
                print(f"❌ 错误: 未知策略名称【{arg_clean}】！")
                print_usage(strategies)
                return
    else:
        selected_keys = ["resonance_breakout"]  # 默认策略

    # 3. 校验路径
    if not os.path.exists(DATA_STORE_DIR):
        print(f"❌ 错误: 本地数据目录 {DATA_STORE_DIR} 不存在。请先运行 python3 sync_market.py 同步数据。")
        return
        
    if not os.path.exists(BLOCK_MAPPINGS_PATH):
        print(f"❌ 错误: 板块映射库 {BLOCK_MAPPINGS_PATH} 不存在。请先运行 python3 sync_market.py 同步板块数据。")
        return

    # 4. 载入本地 config.json 获取通达信路径
    tdx_dir = "/mnt/e/Tools/tdx"
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                config = json.load(f)
                if "tdx_dir" in config:
                    tdx_dir = config["tdx_dir"]
        except Exception:
            pass
            
    # 载入股票名称映射字典
    names_map = load_stock_names(tdx_dir)

    # 动态匹配现有的数据文件前缀
    prefixes = ['sh', 'sz', 'bj']
    existing_files = os.listdir(DATA_STORE_DIR)
    patterns = []
    for prefix in prefixes:
        if any(f.startswith(prefix) and f.endswith('.parquet') for f in existing_files):
            patterns.append(f"{DATA_STORE_DIR}/{prefix}*.parquet")
            
    if not patterns:
        print("❌ 错误: 未在本地数据目录中找到任何有效 .parquet 缓存文件。")
        return
        
    patterns_str = ", ".join(f"'{p}'" for p in patterns)

    # 建立 DuckDB 连接，开启多线程
    con = duckdb.connect()
    con.execute(f"SET threads = {os.cpu_count()}")

    t_start = time.perf_counter()

    # =============================================================
    # 场景一：单策略执行流
    # =============================================================
    if len(selected_keys) == 1:
        strategy_key = selected_keys[0]
        selected_strategy = strategies[strategy_key]
        
        print("=" * 90)
        print(f"🚀 启动策略: 【{selected_strategy['name']}】")
        print(f"👉 策略描述: {selected_strategy['description']}")
        print("=" * 90)
        
        # 替换 SQL 占位符并执行
        raw_sql = selected_strategy["query_sql"]
        query_sql = raw_sql.replace("__PATTERNS_STR__", patterns_str).replace("__BLOCK_MAPPINGS_PATH__", BLOCK_MAPPINGS_PATH)
        
        t_sql_start = time.perf_counter()
        res_df = execute_single_sql(con, query_sql)
        t_end = time.perf_counter()
        
        sql_time = t_end - t_sql_start
        total_time = t_end - t_start
        
        # 获取最新交易日
        latest_date_query = con.execute(f"SELECT MAX(date) FROM read_parquet('{DATA_STORE_DIR}/sh600000.parquet')").fetchone()[0]
        latest_date_str = latest_date_query.strftime('%Y-%m-%d') if latest_date_query else "最新交易日"
        
        print(f"✅ 策略因子计算跑通！最新分析交易日: 【{latest_date_str}】")
        print(f"⏱️ 极速性能指标: SQL计算与关联 {sql_time:.4f} 秒 | 启动总耗时 {total_time:.4f} 秒")
        
        print("\n" + "=" * 90)
        print(f"🏆 【黄金共振个股列表】 (共筛选出 {len(res_df)} 只黄金个股，按放量倍数降序):")
        print("=" * 90)
        
        if res_df.empty:
            print("   -- 今日全市场未筛选出符合该策略的股票，建议空仓避险。 --")
            print("=" * 90)
            return
            
        # 格式化
        res_df['Symbol'] = res_df['symbol'].str.upper()
        res_df['Name'] = res_df['symbol'].map(lambda x: names_map.get(x.lower(), ""))
        res_df['Close_Formatted'] = res_df['Close'].map(lambda x: f"{x:.2f} 元")
        res_df['Vol_Ratio_Formatted'] = res_df['Vol_Ratio'].map(lambda x: f"{x:.2f} 倍")
        res_df['Dev_MA20_Pct_Formatted'] = res_df['Dev_MA20_Pct'].map(lambda x: f"+{x:.2f}%")
        
        show_df = res_df[['Symbol', 'Name', 'Close_Formatted', 'Vol_Ratio_Formatted', 'Dev_MA20_Pct_Formatted', 'Resonance_Sectors']].head(30)
        show_df.columns = ['Symbol', 'Name', 'Close', 'Vol_Ratio', 'Dev_MA20_Pct', 'Resonance_Sectors']
        
        try:
            import pandas as pd
            pd.set_option('display.width', 1000)
            pd.set_option('display.max_colwidth', 50)
        except ImportError:
            pass
            
        print(show_df.to_string(index=False))
        if len(res_df) > 30:
            print(f"\n   ... (余下 {len(res_df) - 30} 只股票已省略) ...")
        print("=" * 90)
        
        # 保存 Markdown 报告
        report_path = os.path.join(CURRENT_DIR, f"{strategy_key}_report.md")
        try:
            md_content = f"""# 🚀 全市场资金共振突破选股报告

**策略名称**：`{selected_strategy['name']}`
**策略描述**：{selected_strategy['description']}
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

| 序号 | 股票代码 | 股票名称 | 最新收盘价 | 今日放量倍数 | MA20 偏离度 | 触发共振爆发板块（突破只数/板块总股数占比） |
| :---: | :---: | :---: | :---: | :---: | :---: | :--- |
"""
            for idx, row in res_df.iterrows():
                md_content += f"| {idx+1} | `{row['Symbol']}` | {row['Name']} | {row['Close_Formatted']} | {row['Vol_Ratio_Formatted']} | {row['Dev_MA20_Pct_Formatted']} | {row['Resonance_Sectors']} |\n"
                
            md_content += """
---

## 💡 选股策略释义
> [!NOTE]
> * **策略核心**：本选股结果完全由 `strategies.json` 配置文件中的 SQL 逻辑驱动，完美实现了算法与源码的分离。
> * **行业共振**：统计每个概念板块中，当天有多少只股票同时触发该策略突破。**只保留其所属板块中“当天至少有 3 只股票同时突破”的成分股**，并计算出突破只数占该板块总股数的比例，完美锁定主力资金最抱团、集聚突破度（Breadth）最高的核心市场风口！
"""
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(md_content)
            print(f"💾 已成功将极速因子选股报告保存至: {report_path}")
        except Exception as e:
            print(f"⚠️ 保存报告失败: {e}")

    # =============================================================
    # 场景二：多策略融合交集流 (Strategy Fusion)
    # =============================================================
    else:
        print("=" * 90)
        print("🚀 启动策略交集融合 (Strategy Fusion)...")
        print("   求以下策略选股结果的【黄金交集】（必须同时满足所有策略过滤条件）：")
        for key in selected_keys:
            print(f"     🔹 【{strategies[key]['name']}】")
        print("=" * 90)
        
        t_sql_start = time.perf_counter()
        
        # 依次运行每个策略的 SQL
        dfs = []
        for key in selected_keys:
            raw_sql = strategies[key]["query_sql"]
            query_sql = raw_sql.replace("__PATTERNS_STR__", patterns_str).replace("__BLOCK_MAPPINGS_PATH__", BLOCK_MAPPINGS_PATH)
            df = execute_single_sql(con, query_sql)
            if not df.empty:
                dfs.append(df)
            else:
                # 只要有一个策略结果为空，交集必然为空，提前退出以节省时间
                dfs = []
                break
                
        t_end = time.perf_counter()
        sql_time = t_end - t_sql_start
        total_time = t_end - t_start
        
        # 获取最新交易日
        latest_date_query = con.execute(f"SELECT MAX(date) FROM read_parquet('{DATA_STORE_DIR}/sh600000.parquet')").fetchone()[0]
        latest_date_str = latest_date_query.strftime('%Y-%m-%d') if latest_date_query else "最新交易日"
        
        print(f"✅ 策略融合因子计算跑通！最新分析交易日: 【{latest_date_str}】")
        print(f"⏱️ 极速性能指标: 多个 SQL 并发筛选耗时: {sql_time:.4f} 秒 | 启动总耗时: {total_time:.4f} 秒")
        
        # 求交集
        res_df = None
        if dfs:
            import pandas as pd
            res_df = dfs[0]
            for next_df in dfs[1:]:
                # 以 symbol 进行 inner join 关联
                res_df = pd.merge(res_df, next_df, on='symbol', suffixes=('', '_other'))
                
        print("\n" + "=" * 90)
        print(f"🏆 【黄金多重共振交集股列表】 (共筛选出 {len(res_df) if res_df is not None else 0} 只黄金个股，按放量倍数降序):")
        print("=" * 90)
        
        if res_df is None or res_df.empty:
            print("   -- 今日多策略求交集后结果为空。提示：多重条件极为严苛，主力尚未形成全面合力，建议继续空仓防守。 --")
            print("=" * 90)
            return
            
        # 格式化
        import pandas as pd
        res_df['Symbol'] = res_df['symbol'].str.upper()
        res_df['Name'] = res_df['symbol'].map(lambda x: names_map.get(x.lower(), ""))
        res_df['Close_Formatted'] = res_df['Close'].map(lambda x: f"{x:.2f} 元")
        res_df['Vol_Ratio_Formatted'] = res_df['Vol_Ratio'].map(lambda x: f"{x:.2f} 倍")
        res_df['Dev_MA20_Pct_Formatted'] = res_df['Dev_MA20_Pct'].map(lambda x: f"+{x:.2f}%")
        
        # 合并不同策略关联到的板块中文显示
        # 找出以 Resonance_Sectors 开头的所有列，合并并去重
        sector_cols = [c for c in res_df.columns if c.startswith('Resonance_Sectors')]
        def merge_sectors(row):
            sectors = []
            for col in sector_cols:
                if pd.notnull(row[col]):
                    sectors.extend([s.strip() for s in row[col].split(',')])
            return ", ".join(sorted(list(set(sectors))))
            
        res_df['Merged_Sectors'] = res_df.apply(merge_sectors, axis=1)
        
        show_df = res_df[['Symbol', 'Name', 'Close_Formatted', 'Vol_Ratio_Formatted', 'Dev_MA20_Pct_Formatted', 'Merged_Sectors']].head(30)
        show_df.columns = ['Symbol', 'Name', 'Close', 'Vol_Ratio', 'Dev_MA20_Pct', 'Resonance_Sectors']
        
        pd.set_option('display.width', 1000)
        pd.set_option('display.max_colwidth', 50)
        print(show_df.to_string(index=False))
        if len(res_df) > 30:
            print(f"\n   ... (余下 {len(res_df) - 30} 只股票已省略) ...")
        print("=" * 90)
        
        # 导出多策略交集 Markdown 报告
        report_filename = "dual_intersection_report.md"
        report_path = os.path.join(CURRENT_DIR, report_filename)
        try:
            md_content = f"""# 🚀 全市场多策略融合黄金交集选股报告

**分析交易日**：`{latest_date_str}`
**参与融合的策略列表**：
"""
            for key in selected_keys:
                md_content += f"* 🔹 **{strategies[key]['name']}**：{strategies[key]['description']}\n"
                
            md_content += f"""**报告生成时间**：`{time.strftime('%Y-%m-%d %H:%M:%S')}`

---

## 🏆 黄金多重共振交集股列表
本列表中的个股**必须同时百分之百满足以上所有选股策略**，代表了市场中最强悍的量化共鸣点：

| 序号 | 股票代码 | 股票名称 | 最新收盘价 | 今日放量倍数 | MA20 偏离度 | 综合触发共振板块（突破只数/占比） |
| :---: | :---: | :---: | :---: | :---: | :---: | :--- |
"""
            for idx, row in res_df.iterrows():
                md_content += f"| {idx+1} | `{row['Symbol']}` | {row['Name']} | {row['Close_Formatted']} | {row['Vol_Ratio_Formatted']} | {row['Dev_MA20_Pct_Formatted']} | {row['Merged_Sectors']} |\n"
                
            md_content += """
---

## 💡 多策略融合（Strategy Fusion）释义
> [!IMPORTANT]
> * **黄金交集（Intersection）原理**：在量化实战中，单个策略往往容易受到噪音干扰。我们通过对**独立多策略的选股结果在 Python 层进行 inner join 求取交集**，强力过滤掉不合规的杂音，只保留了在**均线形态（Trend）、资金量能（Volume）以及板块集聚爆发（Sector Breadth）**三大周期上形成全面多头共鸣的极致黑马个股。
"""
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(md_content)
            print(f"💾 已成功将极速多策略融合交集选股报告保存至: {report_path}")
        except Exception as e:
            print(f"⚠️ 保存交集报告失败: {e}")

if __name__ == "__main__":
    main()
