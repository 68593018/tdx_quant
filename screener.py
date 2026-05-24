import os
import sys
import json
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
    print("  python3 screener.py [策略名称]")
    print("\n目前支持的策略列表:")
    for key, val in strategies.items():
        print(f"  🔹 {key:<20} - {val['name']}")
        print(f"       👉 描述: {val['description']}")
    print("-" * 80)
    print("运行示例:")
    print("  python3 screener.py resonance_breakout    (运行资金集聚概念共振突破策略)")
    print("  python3 screener.py ma_long_sequence      (运行均线多头排列共振爆发策略)")
    print("=" * 80)

def main():
    # 1. 载入策略配置文件
    strategies = load_strategies()
    
    # 2. 解析选股策略参数
    selected_strategy_key = "resonance_breakout"  # 默认策略
    if len(sys.argv) >= 2:
        arg_key = sys.argv[1].strip().lower()
        if arg_key in ['-h', '--help', 'help']:
            print_usage(strategies)
            return
            
        if arg_key not in strategies:
            print(f"❌ 错误: 未知策略名称【{arg_key}】！")
            print_usage(strategies)
            return
        selected_strategy_key = arg_key

    selected_strategy = strategies[selected_strategy_key]
    
    print("=" * 90)
    print(f"🚀 启动策略: 【{selected_strategy['name']}】")
    print(f"👉 策略描述: {selected_strategy['description']}")
    print("=" * 90)
    
    # 3. 校验路径
    if not os.path.exists(DATA_STORE_DIR):
        print(f"❌ 错误: 本地数据目录 {DATA_STORE_DIR} 不存在。请先运行 python3 sync_market.py 同步数据。")
        return
        
    if not os.path.exists(BLOCK_MAPPINGS_PATH):
        print(f"❌ 错误: 板块映射库 {BLOCK_MAPPINGS_PATH} 不存在。请先运行 python3 sync_market.py 同步板块数据。")
        return

    t_start = time.perf_counter()
    
    # 4. 载入本地 config.json 获取通达信路径 (OCP设计)
    tdx_dir = "/mnt/e/Tools/tdx"
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                config = json.load(f)
                if "tdx_dir" in config:
                    tdx_dir = config["tdx_dir"]
        except Exception:
            pass

    # 动态匹配现有的数据文件前缀，规避因某些前缀不存在而引发的 DuckDB IO Error
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
    
    # 5. 读取 SQL 模板并动态替换占位符 (防止 f-string 对大括号的干扰)
    raw_sql = selected_strategy["query_sql"]
    query_sql = raw_sql.replace("__PATTERNS_STR__", patterns_str).replace("__BLOCK_MAPPINGS_PATH__", BLOCK_MAPPINGS_PATH)
    
    t_sql_start = time.perf_counter()
    
    # 建立 DuckDB 连接，并执行多线程 SQL
    con = duckdb.connect()
    con.execute(f"SET threads = {os.cpu_count()}")
    
    try:
        res_df = con.execute(query_sql).fetchdf()
    except Exception as e:
        print(f"❌ SQL 执行失败: {e}")
        return
        
    t_end = time.perf_counter()
    
    # -------------------------------------------------------------
    # 6. 高颜值打印选股战报与性能指标
    # -------------------------------------------------------------
    total_time = t_end - t_start
    sql_time = t_end - t_sql_start
    
    # 格式化日期显示最新交易日
    latest_date_query = con.execute(f"SELECT MAX(date) FROM read_parquet('{DATA_STORE_DIR}/sh600000.parquet')").fetchone()[0]
    latest_date_str = latest_date_query.strftime('%Y-%m-%d') if latest_date_query else "最新交易日"
    
    print(f"✅ 策略因子计算跑通！策略分析最新交易日: 【{latest_date_str}】")
    print(f"📊 全市场扫描个股/指数: {len(os.listdir(DATA_STORE_DIR))} 个 Parquet 文件")
    print(f"⏱️ 极速性能指标:")
    print(f"  - ⚡ DuckDB 多线程 SQL 因子计算与 JOIN 筛选耗时: {sql_time:.4f} 秒")
    print(f"  - 🚀 引擎冷启动总耗时（含依赖检查、连接初始化、多表加载）: {total_time:.4f} 秒")
    
    print("\n" + "=" * 90)
    print(f"🏆 【黄金共振突破股列表】 (共筛选出 {len(res_df)} 只黄金个股，按放量倍数降序排列):")
    print("=" * 90)
    
    if res_df.empty:
        print("   -- 今日全市场未筛选出符合该策略的股票，建议空仓避险。 --")
        print("=" * 90)
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
        
    print("=" * 90)
    
    # -------------------------------------------------------------
    # 7. 自动生成高颜值的 Markdown 报告文件 (.md)
    # -------------------------------------------------------------
    report_filename = f"{selected_strategy_key}_report.md"
    report_path = os.path.join(CURRENT_DIR, report_filename)
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

| 序号 | 股票代码 | 最新收盘价 | 今日放量倍数 | MA20 偏离度 | 触发共振爆发板块（突破只数/板块总股数占比） |
| :---: | :---: | :---: | :---: | :---: | :--- |
"""
        for idx, row in res_df.iterrows():
            md_content += f"| {idx+1} | `{row['Symbol']}` | {row['Close']} | {row['Vol_Ratio']} | {row['Dev_MA20_Pct']} | {row['Resonance_Sectors']} |\n"
            
        md_content += f"""
---

## 💡 选股策略释义
> [!NOTE]
> * **策略判定核心**：本选股结果完全由 `strategies.json` 配置文件中的 SQL 引擎驱动，完美实现了量化算法与主执行器代码的分离。
> * **行业共振**：统计每个概念板块中，当天有多少只股票同时触发该策略突破。**只保留其所属板块中“当天至少有 3 只股票同时突破”的成分股**，并实时算出占比（突破数 / 该板块总股数），完美锁定主力资金最抱团、集聚突破度（Breadth）最高的核心市场风口！
"""
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(md_content)
        print(f"💾 已成功将极速因子选股报告保存至: {report_path}")
    except Exception as e:
        print(f"⚠️ 保存 Markdown 报告失败: {e}")

if __name__ == "__main__":
    main()
