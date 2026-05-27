import os
import sys
import json
import time
import subprocess
from datetime import datetime
import pandas as pd

# 强制 stdout / stderr 使用 UTF-8 编码，消除 Windows 平台下 Emoji 及中文打印的 UnicodeEncodeError 崩溃风险
if sys.platform.startswith('win'):
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# -------------------------------------------------------------
# 1. 动态检测并自动静默安装 DuckDB 库 (极致顺滑的免运维体验)
# -------------------------------------------------------------
try:
    import duckdb
except ImportError:
    print("⏳ 检测到当前环境未安装 DuckDB 依赖，正在为您自动静默安装...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "duckdb"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        import duckdb
        print("✅ DuckDB 依赖安装成功！立即启动极速市场分析引擎...\n")
    except Exception as e:
        print(f"❌ 自动安装 DuckDB 失败，请在终端手动运行 'pip install duckdb'。错误详情: {e}")
        sys.exit(1)

# -------------------------------------------------------------
# 2. 路径配置与全局变量
# -------------------------------------------------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_STORE_DIR = os.path.join(CURRENT_DIR, "data")
BLOCK_MAPPINGS_PATH = os.path.join(DATA_STORE_DIR, "block_mappings.parquet")
INDUSTRY_MAPPINGS_PATH = os.path.join(DATA_STORE_DIR, "industry_mappings.parquet")
CONFIG_PATH = os.path.join(CURRENT_DIR, "config.json")
STRATEGIES_PATH = os.path.join(CURRENT_DIR, "strategies.json")
OUTPUT_MD_PATH = os.path.join(CURRENT_DIR, "market_analysis_report.md")
OUTPUT_HTML_PATH = os.path.join(CURRENT_DIR, "market_dashboard.html")

def load_strategies() -> dict:
    """从 strategies.json 配置文件中载入所有可用的 SQL 策略"""
    if not os.path.exists(STRATEGIES_PATH):
        print(f"❌ 错误: 配置文件 {STRATEGIES_PATH} 不存在！")
        sys.exit(1)
    try:
        with open(STRATEGIES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ 解析配置文件 {STRATEGIES_PATH} 失败: {e}")
        sys.exit(1)

def execute_sql(con, sql: str):
    """安全极速执行单条 SQL"""
    try:
        return con.execute(sql).fetchdf()
    except Exception as e:
        print(f"❌ SQL 执行失败: {e}")
        sys.exit(1)

def process_flow_data(df, name_col) -> dict:
    """处理板块资金流向 DataFrame，提取最新交易日 TOP 10 和 BOTTOM 10 的 30 日时序数据"""
    if df.empty:
        return {"dates": [], "top_10": [], "bottom_10": [], "series": []}
    
    # 格式化日期字符串
    df['date_str'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else str(x)[:10])
    dates = sorted(list(df['date_str'].unique()))
    
    if not dates:
        return {"dates": [], "top_10": [], "bottom_10": [], "series": []}
        
    latest_date_str = dates[-1]
    
    # 筛选最新交易日，排序获取 TOP 10 和 BOTTOM 10
    df_latest = df[df['date_str'] == latest_date_str].sort_values(by='sector_ratio', ascending=False)
    top_10_names = df_latest.head(10)[name_col].tolist()
    bottom_10_names = df_latest.tail(10)[name_col].tolist()
    
    # 构建数据字典加速时序组装
    lookup = {}
    for _, row in df.iterrows():
        lookup[(row[name_col], row['date_str'])] = (
            float(row['sector_amount']) / 1e8,  # 转换为亿元
            float(row['sector_ratio'])
        )
        
    series = []
    
    # TOP 10
    for name in top_10_names:
        amount_history = []
        ratio_history = []
        for d in dates:
            val = lookup.get((name, d), (0.0, 0.0))
            amount_history.append(round(val[0], 2))
            ratio_history.append(round(val[1], 4))
        series.append({
            "name": name,
            "type": "top",
            "amount": amount_history,
            "ratio": ratio_history
        })
        
    # BOTTOM 10
    for name in bottom_10_names:
        amount_history = []
        ratio_history = []
        for d in dates:
            val = lookup.get((name, d), (0.0, 0.0))
            amount_history.append(round(val[0], 2))
            ratio_history.append(round(val[1], 4))
        series.append({
            "name": name,
            "type": "bottom",
            "amount": amount_history,
            "ratio": ratio_history
        })
        
    return {
        "dates": dates,
        "top_10": top_10_names,
        "bottom_10": bottom_10_names,
        "series": series
    }

def load_tdx_dir() -> str:
    """从 config.json 配置文件中载入通达信安装路径，规避硬编码"""
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                config = json.load(f)
                if "tdx_dir" in config:
                    return config["tdx_dir"]
        except Exception:
            pass
    return "/mnt/e/Tools/tdx"

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
                        # 转换成大写标的前缀，例如 sh600000, sz000001
                        symbol = f"{market}{code}"
                        names_map[symbol] = name
            except Exception as e:
                print(f"⚠️ 警告: 解析 {path} 失败: {e}")
    return names_map

def main():
    t_start = time.perf_counter()
    
    print("=" * 80)
    print("      通达信本地数据池 - DuckDB 全市场温度与情绪分析引擎 (Phase 5)")
    print("=" * 80)

    # 0. 载入股票名称映射字典
    tdx_dir = load_tdx_dir()
    names_map = load_stock_names(tdx_dir)

    # 1. 路径和文件校验
    if not os.path.exists(DATA_STORE_DIR):
        print(f"❌ 错误: 数据池目录 {DATA_STORE_DIR} 不存在。请先运行 'python3 sync_market.py'。")
        return
        
    if not os.path.exists(BLOCK_MAPPINGS_PATH):
        print(f"❌ 错误: 板块映射库 {BLOCK_MAPPINGS_PATH} 不存在。请先运行 'python3 sync_market.py'。")
        return

    if not os.path.exists(INDUSTRY_MAPPINGS_PATH):
        print(f"❌ 错误: 行业映射库 {INDUSTRY_MAPPINGS_PATH} 不存在。请先运行 'python3 sync_market.py'。")
        return

    # 2. 载入 SQL 模板
    strategies = load_strategies()
    
    # 动态构建数据文件通配符 (sh*.parquet, sz*.parquet, bj*.parquet)
    prefixes = ['sh', 'sz', 'bj']
    existing_files = os.listdir(DATA_STORE_DIR)
    patterns = []
    for prefix in prefixes:
        if any(f.startswith(prefix) and f.endswith('.parquet') for f in existing_files):
            patterns.append(f"{DATA_STORE_DIR}/{prefix}*.parquet")
            
    if not patterns:
        print("❌ 错误: 未在数据目录中找到任何有效 *.parquet 缓存文件。")
        return
        
    patterns_str = ", ".join(f"'{p}'" for p in patterns)

    # 3. 建立 DuckDB 连接并开启最高并发线程
    con = duckdb.connect()
    con.execute(f"SET threads = {os.cpu_count()}")

    print("⚡ 正在执行全量多线程窗口分析计算...")
    
    default_stock_filter = "(filename LIKE '%sh60%' OR filename LIKE '%sh68%' OR filename LIKE '%sz00%' OR filename LIKE '%sz30%' OR filename LIKE '%/bj%')"
    # =========================================================================
    # 执行 4 大核心 SQL 进行全方位聚合分析
    # =========================================================================
    
    # --- 模块一：市场温度与涨跌分布 ---
    sql_temp = strategies["market_temperature"]["query_sql"]\
        .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
        .replace("__PATTERNS_STR__", patterns_str)\
        .replace("__CATEGORY_FILTER__", default_stock_filter)
    
    t_sql1 = time.perf_counter()
    df_temp = execute_sql(con, sql_temp)
    t_sql1_end = time.perf_counter()
    
    # --- 模块二：高度板与短线投机梯队 ---
    sql_streaks = strategies["limit_up_streaks"]["query_sql"]\
        .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
        .replace("__PATTERNS_STR__", patterns_str)\
        .replace("__CATEGORY_FILTER__", default_stock_filter)
        
    t_sql2 = time.perf_counter()
    df_streaks = execute_sql(con, sql_streaks)
    t_sql2_end = time.perf_counter()

    # --- 模块三：板块资金宽度与动能轮动 ---
    sql_breadth = strategies["sector_breadth"]["query_sql"]\
        .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
        .replace("__PATTERNS_STR__", patterns_str)\
        .replace("__BLOCK_MAPPINGS_PATH__", BLOCK_MAPPINGS_PATH)\
        .replace("__CATEGORY_FILTER__", default_stock_filter)
        
    t_sql3 = time.perf_counter()
    df_breadth = execute_sql(con, sql_breadth)
    t_sql3_end = time.perf_counter()

    # --- 模块四：大盘多空压力支撑 (筹码分布) ---
    sql_support = strategies["index_support"]["query_sql"]\
        .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)
        
    t_sql4 = time.perf_counter()
    df_support = execute_sql(con, sql_support)
    t_sql4_end = time.perf_counter()

    # --- 模块五：行业板块均线多头占比与动能轮动 ---
    sql_ind_breadth = strategies["industry_breadth"]["query_sql"]\
        .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
        .replace("__PATTERNS_STR__", patterns_str)\
        .replace("__INDUSTRY_MAPPINGS_PATH__", INDUSTRY_MAPPINGS_PATH)\
        .replace("__CATEGORY_FILTER__", default_stock_filter)
        
    t_sql5 = time.perf_counter()
    df_ind_breadth = execute_sql(con, sql_ind_breadth)
    t_sql5_end = time.perf_counter()

    # --- 模块六：行业板块30日资金时序流向 ---
    sql_ind_flow = strategies["industry_flow_30d"]["query_sql"]\
        .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
        .replace("__PATTERNS_STR__", patterns_str)\
        .replace("__INDUSTRY_MAPPINGS_PATH__", INDUSTRY_MAPPINGS_PATH)\
        .replace("__CATEGORY_FILTER__", default_stock_filter)
        
    t_sql6 = time.perf_counter()
    df_ind_flow = execute_sql(con, sql_ind_flow)
    t_sql6_end = time.perf_counter()

    # --- 模块七：概念板块30日资金时序流向 ---
    sql_concept_flow = strategies["concept_flow_30d"]["query_sql"]\
        .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
        .replace("__PATTERNS_STR__", patterns_str)\
        .replace("__BLOCK_MAPPINGS_PATH__", BLOCK_MAPPINGS_PATH)\
        .replace("__CATEGORY_FILTER__", default_stock_filter)
        
    t_sql7 = time.perf_counter()
    df_concept_flow = execute_sql(con, sql_concept_flow)
    t_sql7_end = time.perf_counter()

    # 4. 解析整理数据
    # 提取全局基础变量
    row_temp = df_temp.iloc[0]
    total_stocks = int(row_temp['total_stocks'])
    limit_up = int(row_temp['limit_up'])
    limit_down = int(row_temp['limit_down'])
    median_return = float(row_temp['median_return'])
    trade_date = row_temp['trade_date']
    if pd.notnull(trade_date) and hasattr(trade_date, 'strftime'):
        try:
            trade_date_str = trade_date.strftime('%Y-%m-%d')
        except ValueError:
            trade_date_str = str(trade_date)[:10] if pd.notnull(trade_date) else ""
    else:
        # 如果是数字类型，格式化为 YYYY-MM-DD
        s = str(trade_date) if pd.notnull(trade_date) else ""
        trade_date_str = f"{s[:4]}-{s[4:6]}-{s[6:8]}" if len(s) == 8 else s

    # 统计连板梯队数据
    streaks_list = []
    streak_counts = {}  # 连板高度计数器
    for _, r in df_streaks.iterrows():
        stk = int(r['streak'])
        sym = r['symbol'].upper()
        # 从名称映射字典中读取股票名称
        cname = names_map.get(sym.lower(), "")
        streaks_list.append({"symbol": sym, "name": cname, "streak": stk})
        streak_counts[stk] = streak_counts.get(stk, 0) + 1

    # 整理涨跌分布列表
    dist = {
        "limit_up": int(row_temp['limit_up']),
        "p7_10": int(row_temp['p7_10']),
        "p5_7": int(row_temp['p5_7']),
        "p3_5": int(row_temp['p3_5']),
        "p0_3": int(row_temp['p0_3']),
        "n0_3": int(row_temp['n0_3']),
        "n3_5": int(row_temp['n3_5']),
        "n5_7": int(row_temp['n5_7']),
        "n7_10": int(row_temp['n7_10']),
        "limit_down": int(row_temp['limit_down'])
    }
    
    flat_count = int(row_temp['flat_count'])
    rising_count = dist['limit_up'] + dist['p7_10'] + dist['p5_7'] + dist['p3_5'] + dist['p0_3']
    falling_count = dist['limit_down'] + dist['n7_10'] + dist['n5_7'] + dist['n3_5'] + dist['n0_3']

    # 整理板块宽度列表
    breadth_list = []
    for _, r in df_breadth.iterrows():
        breakout_stocks = []
        if 'breakout_symbols' in r and pd.notnull(r['breakout_symbols']) and r['breakout_symbols']:
            symbols = [s.strip() for s in r['breakout_symbols'].split(',') if s.strip()]
            for s in symbols:
                sym_upper = s.upper()
                cname = names_map.get(s.lower(), "")
                breakout_stocks.append({"symbol": sym_upper, "name": cname})
                
        breadth_list.append({
            "block_name": r['block_name'],
            "total_stocks": int(r['total_stocks']),
            "above_ma20_count": int(r['above_ma20_count']),
            "above_ma20_ratio": float(r['above_ma20_ratio']),
            "bullish_count": int(r['bullish_count']),
            "bullish_ratio": float(r['bullish_ratio']),
            "breakout_count": int(r['breakout_count']),
            "breakout_stocks": breakout_stocks
        })

    # 整理行业板块宽度列表
    ind_breadth_list = []
    for _, r in df_ind_breadth.iterrows():
        breakout_stocks = []
        if 'breakout_symbols' in r and pd.notnull(r['breakout_symbols']) and r['breakout_symbols']:
            symbols = [s.strip() for s in r['breakout_symbols'].split(',') if s.strip()]
            for s in symbols:
                sym_upper = s.upper()
                cname = names_map.get(s.lower(), "")
                breakout_stocks.append({"symbol": sym_upper, "name": cname})
                
        ind_breadth_list.append({
            "industry_name": r['industry_name'],
            "total_stocks": int(r['total_stocks']),
            "above_ma20_count": int(r['above_ma20_count']),
            "above_ma20_ratio": float(r['above_ma20_ratio']),
            "bullish_count": int(r['bullish_count']),
            "bullish_ratio": float(r['bullish_ratio']),
            "breakout_count": int(r['breakout_count']),
            "breakout_stocks": breakout_stocks
        })

    # 整理大盘支撑列表
    support_list = []
    for _, r in df_support.iterrows():
        support_list.append({
            "bucket": int(r['bucket']),
            "min_price": float(r['min_price']),
            "max_price": float(r['max_price']),
            "total_amount_billions": float(r['total_amount']) / 1e8  # 转换为亿元
        })

    # 整理30日板块资金时序流向
    industry_flow_processed = process_flow_data(df_ind_flow, 'industry_name')
    concept_flow_processed = process_flow_data(df_concept_flow, 'block_name')

    # 整理全套数据字典用于前端注入
    full_data = {
        "trade_date": trade_date_str,
        "total_stocks": total_stocks,
        "median_return": median_return,
        "rising_count": rising_count,
        "falling_count": falling_count,
        "flat_count": flat_count,
        "dist": dist,
        "streak_counts": {str(k): v for k, v in sorted(streak_counts.items(), reverse=True)},
        "streaks": streaks_list,
        "breadth": breadth_list,
        "industry_breadth": ind_breadth_list,
        "support": support_list,
        "industry_flow": industry_flow_processed,
        "concept_flow": concept_flow_processed
    }

    t_calc = time.perf_counter()
    print(f"✅ 全市场宏观数据链算完毕！耗时: {t_calc - t_start:.2f} 秒")
    print(f"   ├─ 市场温度与涨跌分布 SQL 耗时: {t_sql1_end - t_sql1:.4f} 秒")
    print(f"   ├─ 高度连板梯队计算 SQL 耗时: {t_sql2_end - t_sql2:.4f} 秒")
    print(f"   ├─ 概念板块资金宽度计算 SQL 耗时: {t_sql3_end - t_sql3:.4f} 秒")
    print(f"   ├─ 行业板块资金宽度计算 SQL 耗时: {t_sql5_end - t_sql5:.4f} 秒")
    print(f"   ├─ 指数支撑筹码分布 SQL 耗时: {t_sql4_end - t_sql4:.4f} 秒")
    print(f"   ├─ 行业板块30日资金流向 SQL 耗时: {t_sql6_end - t_sql6:.4f} 秒")
    print(f"   └─ 概念板块30日资金流向 SQL 耗时: {t_sql7_end - t_sql7:.4f} 秒")

    # =========================================================================
    # 输出通道一：高颜值控制台战报与 Markdown 报告生成
    # =========================================================================
    temp_color = "\033[91m" if median_return >= 0 else "\033[92m"
    reset_color = "\033[0m"
    
    print("\n" + "=" * 80)
    print(f"                 📊 市场情绪大势战报 (交易日期: {trade_date_str})")
    print("=" * 80)
    print(f"🔹 统计总数: {total_stocks} 只个股   |   上涨家数: {rising_count} 只   |   平盘家数: {flat_count} 只   |   下跌家数: {falling_count} 只")
    print(f"🔹 市场中位数涨幅: {temp_color}{median_return:+.2f}%{reset_color} (当前市场实际赚钱效应指标)")
    print(f"🔹 极限情绪指标: 涨停 {limit_up} 家   |   跌停 {limit_down} 家   |   涨跌比 (A/D): {rising_count / max(1, falling_count):.2f}")
    print("-" * 80)
    
    print("🪜 短线投机连板梯队 (最新交易日仍封死涨停股高度板排版):")
    if not streaks_list:
        print("   [ 当前短线接力投机冰封，无符合高度板个股 ]")
    else:
        # 按板数分组打印
        groups = {}
        for s in streaks_list:
            disp = f"{s['symbol']}({s['name']})" if s['name'] else s['symbol']
            groups[s['streak']] = groups.get(s['streak'], []) + [disp]
        for k in sorted(groups.keys(), reverse=True):
            print(f"   ⭐ 【{k} 连板】({len(groups[k])}只): {', '.join(groups[k])}")
            
    print("-" * 80)
    print("🚀 行业板块多头动能与资金集聚 TOP 5:")
    for i, b in enumerate(ind_breadth_list[:5]):
        print(f"   🔥 No.{i+1} {b['industry_name']:<12} | 均线完美多头占比: {b['bullish_ratio']:.1f}% ({b['bullish_count']}/{b['total_stocks']}只) | 今日温和突破: {b['breakout_count']}只")
    print("-" * 80)
    print("🚀 概念板块多头动能与资金集聚 TOP 5:")
    for i, b in enumerate(breadth_list[:5]):
        print(f"   🔥 No.{i+1} {b['block_name']:<12} | 均线完美多头占比: {b['bullish_ratio']:.1f}% ({b['bullish_count']}/{b['total_stocks']}只) | 今日温和突破: {b['breakout_count']}只")
    print("=" * 80)

    # 自动保存为 Markdown 报告
    output_md_path = save_markdown_report(full_data)

    # =========================================================================
    # 输出通道二：赛博霓虹交互式本地仪表盘网页模板生成 (零CORS限制)
    # =========================================================================
    generate_html_dashboard(full_data)
    
    print(f"\n🎉 市场数据分析报告圆满交付！")
    print(f"📄 Markdown战报: [market_analysis_report.md](file://{output_md_path})")
    print(f"📺 交互式仪表盘: [market_dashboard.html](file://{OUTPUT_HTML_PATH}) (可直接双击用浏览器极速查看！)")
    print("=" * 80)

def save_markdown_report(data):
    """自动生成并刷新 market_analysis_report.md 报告"""
    temp_color = "🔴 偏暖" if data['median_return'] >= 0 else "🟢 偏冷"
    
    # 极美 Markdown 内容排版
    content = f"""# 📊 全市场情绪温度与动能分析报告

本报告由 **DuckDB 极速多进程数据分析引擎** 自动扫描本地通达信列式 Parquet 数据库并聚合生成，旨在对全市场交易情绪、连板投机高度、板块动能轮动以及大盘筹码压力进行战略深度剖析。

---

## 🌡️ 一、 核心大势指标与市场温度

*   **交易日期**：`{data['trade_date']}`
*   **市场温度**：{temp_color} (以全市场中位数涨幅计算)
*   **市场中位数涨幅**：`{data['median_return']:+.2f}%` *(这是衡量全市场个股平均赚钱效能的黄金指针)*
*   **上涨家数**：`{data['rising_count']}` 只 *(占比 {data['rising_count'] * 100.0 / data['total_stocks']:.1f}%)*
*   **平盘家数**：`{data['flat_count']}` 只 *(占比 {data['flat_count'] * 100.0 / data['total_stocks']:.1f}%)*
*   **下跌家数**：`{data['falling_count']}` 只 *(占比 {data['falling_count'] * 100.0 / data['total_stocks']:.1f}%)*
*   **极限多空家数**：涨停 `{data['dist']['limit_up']}` 家 | 跌停 `{data['dist']['limit_down']}` 家

### 📊 日涨跌幅多维区间分布

| 跌停 (<-9.9%) | -7% ~ -9.9% | -5% ~ -7% | -3% ~ -5% | -3% ~ 0% | 0% ~ 3% | 3% ~ 5% | 5% ~ 7% | 7% ~ 9.9% | 涨停 (>9.9%) |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| {data['dist']['limit_down']} | {data['dist']['n7_10']} | {data['dist']['n5_7']} | {data['dist']['n3_5']} | {data['dist']['n0_3']} | {data['dist']['p0_3']} | {data['dist']['p3_5']} | {data['dist']['p5_7']} | {data['dist']['p7_10']} | {data['dist']['limit_up']} |

---

## 🪜 二、 短线游资投机连板梯队

短线情绪以**最新封死涨停板的个股连板高度**做核心度量：

| 连板高度 (板) | 连板个股数 (只) | 对应强势个股代码 |
| :---: | :---: | :--- |
"""
    groups = {}
    for s in data['streaks']:
        disp = f"{s['symbol']}({s['name']})" if s.get('name') else s['symbol']
        groups[s['streak']] = groups.get(s['streak'], []) + [disp]
        
    if not groups:
        content += "| 暂无高度连板 | 0 | 市场投机情绪极弱，建议空仓避险 |\n"
    else:
        for k in sorted(groups.keys(), reverse=True):
            content += f"| **{k} 连板** | {len(groups[k])} | {', '.join(groups[k])} |\n"

    content += """
---

## 🚀 三、 行业板块均线多头占比与资金集聚 (TOP 10)

我们自下而上统计行业板块内部个股的走势，**均线多头排列占比越高，说明该行业机构/主力资金介入越深，中线动能最强**：

| 行业板块名称 | 总个股数 (只) | 站上20日线个股数 | 均线完美多头占比 (%) | 今日放量突破数 (只) | 战略风向指标 |
| :--- | :---: | :---: | :---: | :---: | :--- |
"""
    for b in data['industry_breadth'][:10]:
        indicator = "🔥 绝对主线" if b['bullish_ratio'] >= 40 else ("✨ 中期多头" if b['bullish_ratio'] >= 25 else "⚡ 局部活跃")
        content += f"| **{b['industry_name']}** | {b['total_stocks']} | {b['above_ma20_count']} | **{b['bullish_ratio']:.1f}%** | {b['breakout_count']} | {indicator} |\n"

    content += """
---

## 🚀 四、 概念板块均线多头占比与资金集聚 (TOP 10)

我们自下而上统计板块内部个股的走势，**均线多头排列占比越高，说明该板块机构/主力建仓介入越深，中线动能最强**：

| 概念板块名称 | 总个股数 (只) | 站上20日线个股数 | 均线完美多头占比 (%) | 今日放量突破数 (只) | 战略风向指标 |
| :--- | :---: | :---: | :---: | :---: | :--- |
"""
    for b in data['breadth'][:10]:
        indicator = "🔥 绝对主线" if b['bullish_ratio'] >= 40 else ("✨ 中期多头" if b['bullish_ratio'] >= 25 else "⚡ 局部活跃")
        content += f"| **{b['block_name']}** | {b['total_stocks']} | {b['above_ma20_count']} | **{b['bullish_ratio']:.1f}%** | {b['breakout_count']} | {indicator} |\n"

    content += """
---

## 📊 五、 上证指数筹码压力位分布 (120日累积成交额)

利用 120 个交易日的价格分段筹码堆积带，能够直接预警大盘在反弹和调整过程中的**中期强力支撑带**与**抛压阻力带**：

| 价格分段区间 (元) | 累积成交额 (亿元) | 占历史总成交比例 | 可视化筹码堆积带 |
| :--- | :---: | :---: | :--- |
"""
    total_amount = sum(s['total_amount_billions'] for s in data['support'])
    for s in data['support']:
        pct = s['total_amount_billions'] * 100.0 / total_amount if total_amount > 0 else 0
        bars = "█" * int(pct / 2)
        content += f"| {s['min_price']:.2f} ~ {s['max_price']:.2f} | {s['total_amount_billions']:.1f} | {pct:.1f}% | {bars} |\n"

    content += """
---
> 💡 **投研建议**：
> 1. 如果**市场中位数涨幅 < 0** 且 **涨幅分布集中在 [-3%, 0%] 甚至更低区间**，说明当下市场极度缺乏持续性，个股炸板率增高，建议严防亏钱效应。
> 2. 观察 **行业板块与概念板块 TOP 3** 的放量突破股，这是寻找次日**资金流向最热板块核心龙一/龙二个股**的最强罗盘。
"""
    report_dir = os.path.join(CURRENT_DIR, "report")
    os.makedirs(report_dir, exist_ok=True)
    date_suffix = data['trade_date'].replace("-", "")
    output_md_path = os.path.join(report_dir, f"market_analysis_report_{date_suffix}.md")
    
    with open(output_md_path, "w", encoding="utf-8") as f:
        f.write(content)
    return output_md_path

def generate_html_dashboard(data):
    """从 web/index.html 读取模板并生成 market_dashboard.html 本地交互式看板"""
    template_path = os.path.join(CURRENT_DIR, 'web', 'index.html')
    if not os.path.exists(template_path):
        print(f'⚠️ 警告: 未找到 Web 模板 {template_path}，无法生成最新交互式看板！')
        return
        
    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()
            
        json_data_str = json.dumps(data, ensure_ascii=False)
        html_content = template_content.replace('__MARKET_DATA_JSON__', json_data_str)
        
        with open(OUTPUT_HTML_PATH, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f'📺 完美生成多功能 Web 看板门户: [market_dashboard.html](file://{OUTPUT_HTML_PATH}) (可直接双击或通过 Web 访问！)')
    except Exception as e:
        print(f'❌ 生成看板 HTML 失败: {e}')


if __name__ == '__main__':
    main()
