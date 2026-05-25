import os
import sys
import json
import time
import subprocess
from datetime import datetime

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
    
    # =========================================================================
    # 执行 4 大核心 SQL 进行全方位聚合分析
    # =========================================================================
    
    # --- 模块一：市场温度与涨跌分布 ---
    sql_temp = strategies["market_temperature"]["query_sql"]\
        .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
        .replace("__PATTERNS_STR__", patterns_str)
    
    t_sql1 = time.perf_counter()
    df_temp = execute_sql(con, sql_temp)
    t_sql1_end = time.perf_counter()
    
    # --- 模块二：高度板与短线投机梯队 ---
    sql_streaks = strategies["limit_up_streaks"]["query_sql"]\
        .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
        .replace("__PATTERNS_STR__", patterns_str)
        
    t_sql2 = time.perf_counter()
    df_streaks = execute_sql(con, sql_streaks)
    t_sql2_end = time.perf_counter()

    # --- 模块三：板块资金宽度与动能轮动 ---
    sql_breadth = strategies["sector_breadth"]["query_sql"]\
        .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
        .replace("__PATTERNS_STR__", patterns_str)\
        .replace("__BLOCK_MAPPINGS_PATH__", BLOCK_MAPPINGS_PATH)
        
    t_sql3 = time.perf_counter()
    df_breadth = execute_sql(con, sql_breadth)
    t_sql3_end = time.perf_counter()

    # --- 模块四：大盘多空压力支撑 (筹码分布) ---
    sql_support = strategies["index_support"]["query_sql"]\
        .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)
        
    t_sql4 = time.perf_counter()
    df_support = execute_sql(con, sql_support)
    t_sql4_end = time.perf_counter()

    # --- 模块五：行业板块资金宽度与动能轮动 ---
    sql_ind_breadth = strategies["industry_breadth"]["query_sql"]\
        .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
        .replace("__PATTERNS_STR__", patterns_str)\
        .replace("__INDUSTRY_MAPPINGS_PATH__", INDUSTRY_MAPPINGS_PATH)
        
    t_sql5 = time.perf_counter()
    df_ind_breadth = execute_sql(con, sql_ind_breadth)
    t_sql5_end = time.perf_counter()

    # 4. 解析整理数据
    # 提取全局基础变量
    row_temp = df_temp.iloc[0]
    total_stocks = int(row_temp['total_stocks'])
    limit_up = int(row_temp['limit_up'])
    limit_down = int(row_temp['limit_down'])
    median_return = float(row_temp['median_return'])
    trade_date = row_temp['trade_date']
    if hasattr(trade_date, 'strftime'):
        trade_date_str = trade_date.strftime('%Y-%m-%d')
    else:
        # 如果是数字类型，格式化为 YYYY-MM-DD
        s = str(trade_date)
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
        breadth_list.append({
            "block_name": r['block_name'],
            "total_stocks": int(r['total_stocks']),
            "above_ma20_count": int(r['above_ma20_count']),
            "above_ma20_ratio": float(r['above_ma20_ratio']),
            "bullish_count": int(r['bullish_count']),
            "bullish_ratio": float(r['bullish_ratio']),
            "breakout_count": int(r['breakout_count'])
        })

    # 整理行业板块宽度列表
    ind_breadth_list = []
    for _, r in df_ind_breadth.iterrows():
        ind_breadth_list.append({
            "industry_name": r['industry_name'],
            "total_stocks": int(r['total_stocks']),
            "above_ma20_count": int(r['above_ma20_count']),
            "above_ma20_ratio": float(r['above_ma20_ratio']),
            "bullish_count": int(r['bullish_count']),
            "bullish_ratio": float(r['bullish_ratio']),
            "breakout_count": int(r['breakout_count'])
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
        "support": support_list
    }

    t_calc = time.perf_counter()
    print(f"✅ 全市场宏观数据链算完毕！耗时: {t_calc - t_start:.2f} 秒")
    print(f"   ├─ 市场温度与涨跌分布 SQL 耗时: {t_sql1_end - t_sql1:.4f} 秒")
    print(f"   ├─ 高度连板梯队计算 SQL 耗时: {t_sql2_end - t_sql2:.4f} 秒")
    print(f"   ├─ 概念板块资金宽度计算 SQL 耗时: {t_sql3_end - t_sql3:.4f} 秒")
    print(f"   ├─ 行业板块资金宽度计算 SQL 耗时: {t_sql5_end - t_sql5:.4f} 秒")
    print(f"   └─ 指数支撑筹码分布 SQL 耗时: {t_sql4_end - t_sql4:.4f} 秒")

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
    save_markdown_report(full_data)

    # =========================================================================
    # 输出通道二：赛博霓虹交互式本地仪表盘网页模板生成 (零CORS限制)
    # =========================================================================
    generate_html_dashboard(full_data)
    
    print(f"\n🎉 市场数据分析报告圆满交付！")
    print(f"📄 Markdown战报: [market_analysis_report.md](file://{OUTPUT_MD_PATH})")
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
    with open(OUTPUT_MD_PATH, "w", encoding="utf-8") as f:
        f.write(content)

def generate_html_dashboard(data):
    """生成 market_dashboard.html 本地交互式看板"""
    # 注入的 JSON 字符串
    json_data_str = json.dumps(data, ensure_ascii=False)
    
    html_template = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>通达信极速多因子量化选股系统 - 全市场大势分析看板</title>
    <!-- 引入 premium 字体与 ECharts -->
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=Inter:wght@300;400;500;600;700&family=Fira+Code:wght@400;500&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/echarts@5.4.2/dist/echarts.min.js"></script>
    
    <style>
        :root {
            --bg-base: #06090f;
            --bg-surface: rgba(13, 20, 35, 0.7);
            --bg-card: rgba(22, 32, 54, 0.45);
            --border-subtle: rgba(255, 255, 255, 0.05);
            --primary: #06b6d4;      /* 霓虹青 */
            --secondary: #6366f1;    /* 极光蓝 */
            --accent: #a855f7;       /* 朋克紫 */
            --up-color: #ef4444;     /* 涨停红 */
            --down-color: #10b981;   /* 跌停绿 */
            --text-main: #f3f4f6;
            --text-muted: #9ca3af;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            font-family: 'Inter', -apple-system, sans-serif;
            background-color: var(--bg-base);
            color: var(--text-main);
            padding: 2.5rem 3.5rem;
            min-height: 100vh;
            background-image: 
                radial-gradient(at 0% 0%, rgba(6, 182, 212, 0.04) 0px, transparent 50%),
                radial-gradient(at 100% 100%, rgba(168, 85, 247, 0.04) 0px, transparent 50%);
        }

        /* Header section */
        header {
            margin-bottom: 2.5rem;
            display: flex;
            justify-content: space-between;
            align-items: flex-end;
            border-bottom: 1px solid var(--border-subtle);
            padding-bottom: 1.5rem;
        }

        .logo-title {
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
        }

        .badge {
            display: inline-flex;
            background: rgba(6, 182, 212, 0.1);
            border: 1px solid rgba(6, 182, 212, 0.2);
            color: var(--primary);
            padding: 0.25rem 0.75rem;
            border-radius: 100px;
            font-size: 0.8rem;
            font-weight: 600;
            letter-spacing: 1px;
            width: fit-content;
        }

        h1 {
            font-family: 'Outfit', sans-serif;
            font-size: 2.2rem;
            font-weight: 700;
            background: linear-gradient(135deg, #ffffff 40%, #c7d2fe 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .trade-date {
            font-family: 'Outfit', sans-serif;
            font-size: 1.1rem;
            color: var(--text-muted);
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid var(--border-subtle);
            padding: 0.5rem 1.25rem;
            border-radius: 12px;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .trade-date::before {
            content: '📅';
        }

        /* 🌡️ 宏观情绪大盘排版 */
        .grid-kpis {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2.5rem;
        }

        .card-kpi {
            background: var(--bg-surface);
            border: 1px solid var(--border-subtle);
            border-radius: 16px;
            padding: 1.5rem 1.75rem;
            position: relative;
            overflow: hidden;
            backdrop-filter: blur(10px);
        }

        .kpi-title {
            font-size: 0.85rem;
            color: var(--text-muted);
            margin-bottom: 0.5rem;
        }

        .kpi-value {
            font-family: 'Outfit', sans-serif;
            font-size: 1.85rem;
            font-weight: 700;
        }

        .kpi-value.up { color: var(--up-color); text-shadow: 0 0 10px rgba(239, 68, 68, 0.15); }
        .kpi-value.down { color: var(--down-color); text-shadow: 0 0 10px rgba(16, 185, 129, 0.15); }

        /* 📊 主仪表盘图表排列网格 */
        .grid-dashboard {
            display: grid;
            grid-template-columns: 3fr 2fr;
            gap: 2rem;
            margin-bottom: 2.5rem;
        }

        @media (max-width: 1024px) {
            .grid-dashboard {
                grid-template-columns: 1fr;
            }
        }

        .card-chart {
            background: var(--bg-surface);
            border: 1px solid var(--border-subtle);
            border-radius: 20px;
            padding: 2rem;
            backdrop-filter: blur(10px);
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
        }

        .chart-title {
            font-family: 'Outfit', sans-serif;
            font-size: 1.25rem;
            font-weight: 600;
            margin-bottom: 1.5rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.03);
            padding-bottom: 0.5rem;
        }

        .chart-box {
            width: 100%;
            height: 350px;
        }

        /* 📋 概念及板块连板表格与排版 */
        .table-area {
            width: 100%;
            border-collapse: collapse;
        }

        .table-area th {
            text-align: left;
            padding: 0.75rem 1rem;
            font-size: 0.85rem;
            color: var(--text-muted);
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
        }

        .table-area td {
            padding: 0.85rem 1rem;
            font-size: 0.9rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.02);
            color: var(--text-main);
        }

        .streak-badge {
            background: linear-gradient(135deg, var(--accent), var(--secondary));
            border-radius: 6px;
            color: #fff;
            font-family: 'Outfit', sans-serif;
            font-weight: 700;
            padding: 0.2rem 0.5rem;
            font-size: 0.8rem;
            box-shadow: 0 0 10px rgba(99, 102, 241, 0.3);
        }

        .progress-bar-bg {
            width: 100px;
            height: 6px;
            background: rgba(255, 255, 255, 0.05);
            border-radius: 10px;
            display: inline-block;
            vertical-align: middle;
            margin-right: 0.5rem;
            overflow: hidden;
        }

        .progress-bar-fill {
            height: 100%;
            background: linear-gradient(to right, var(--primary), var(--secondary));
            border-radius: 10px;
        }
    </style>
</head>
<body>

    <header>
        <div class="logo-title">
            <div class="badge">TACTICAL MARKET DASHBOARD</div>
            <h1>🚀 全市场大势与短线投机温度仪表盘</h1>
        </div>
        <div class="trade-date" id="dom-trade-date">---- -- --</div>
    </header>

    <!-- 🌡️ 全局 KPI 温度计 -->
    <div class="grid-kpis">
        <div class="card-kpi">
            <div class="kpi-title">全市场中位数涨幅</div>
            <div class="kpi-value" id="dom-median-return">0.00%</div>
        </div>
        <div class="card-kpi">
            <div class="kpi-title">上涨家数 (多头)</div>
            <div class="kpi-value up" id="dom-rising-count">-- 只</div>
        </div>
        <div class="card-kpi">
            <div class="kpi-title">平盘家数 (震荡)</div>
            <div class="kpi-value" id="dom-flat-count" style="color: var(--text-muted);">-- 只</div>
        </div>
        <div class="card-kpi">
            <div class="kpi-title">下跌家数 (空头)</div>
            <div class="kpi-value down" id="dom-falling-count">-- 只</div>
        </div>
        <div class="card-kpi">
            <div class="kpi-title">涨停 / 跌停家数</div>
            <div class="kpi-value" id="dom-limit-ratio" style="background: linear-gradient(to right, var(--up-color), var(--down-color)); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">-- / --</div>
        </div>
    </div>

    <!-- 📊 图表仪表盘网格 -->
    <div class="grid-dashboard">
        <!-- 涨跌幅分布 -->
        <div class="card-chart">
            <div class="chart-title">📊 沪深京全市场涨跌幅多维区间分布图</div>
            <div class="chart-box" id="chart-distribution"></div>
        </div>
        
        <!-- 上证指数筹码分布 -->
        <div class="card-chart">
            <div class="chart-title">🧱 上证指数筹码堆积带 (120日成交额压力支撑)</div>
            <div class="chart-box" id="chart-index-support"></div>
        </div>
    </div>

    <div class="grid-dashboard" style="grid-template-columns: 1fr 1fr;">
        <!-- 行业板块宽度排行 -->
        <div class="card-chart">
            <div class="chart-title">🚀 行业板块均线多头占比排行 (前十强主线)</div>
            <table class="table-area" id="table-industries">
                <thead>
                    <tr>
                        <th>行业名称</th>
                        <th>成分股数</th>
                        <th>中线多头排列比</th>
                        <th>今日放量突破股</th>
                    </tr>
                </thead>
                <tbody>
                    <!-- JS 自动注入 -->
                </tbody>
            </table>
        </div>

        <!-- 概念板块宽度排行 -->
        <div class="card-chart">
            <div class="chart-title">🚀 概念板块均线多头占比排行 (前十强主线)</div>
            <table class="table-area" id="table-sectors">
                <thead>
                    <tr>
                        <th>板块名称</th>
                        <th>成分股数</th>
                        <th>中线多头排列比</th>
                        <th>今日放量突破股</th>
                    </tr>
                </thead>
                <tbody>
                    <!-- JS 自动注入 -->
                </tbody>
            </table>
        </div>
    </div>

    <div class="grid-dashboard">
        <!-- 短线连板梯队 -->
        <div class="card-chart">
            <div class="chart-title">🪜 最新活跃游资投机连板高度梯队</div>
            <table class="table-area" id="table-streaks">
                <thead>
                    <tr>
                        <th>连板天数</th>
                        <th>连板股数 (只)</th>
                        <th>热门连板个股列表</th>
                    </tr>
                </thead>
                <tbody>
                    <!-- JS 自动注入 -->
                </tbody>
            </table>
        </div>
        
        <!-- 量化风向与投研建议 -->
        <div class="card-chart">
            <div class="chart-title">💡 投研建议与战术指标指南</div>
            <div style="padding: 1rem 0; line-height: 1.8; color: var(--text-main);">
                <p style="margin-bottom: 1rem;"><strong style="color: var(--primary);">1. 市场温度判定：</strong>当中位数涨幅为正且上涨家数显著多于下跌家数时，市场赚钱效应偏暖，适合积极介入；反之，若中位数涨幅为负且跌停个股增加，说明市场赚钱效应偏冷，宜控仓避险。</p>
                <p style="margin-bottom: 1rem;"><strong style="color: var(--secondary);">2. 寻找绝对主线：</strong>关注完美多头占比超过 40% 的行业板块。此类板块通常有持续不断的机构或主力资金流入，是中期持股的首选方向。</p>
                <p><strong style="color: var(--accent);">3. 狙击短线先锋：</strong>在多头占比高的概念或行业中，寻找今日放量突破 MA20 的个股。这往往是板块启动或加速时的最强信号股，结合连板梯队高度可以精准捕捉游资炒作的核心龙头。</p>
            </div>
        </div>
    </div>

    <!-- -------------------------------------------------------------
     * 数据接收与前端逻辑 
     * ------------------------------------------------------------- -->
    <script>
        // 动态注入的全局变量，绝对无 CORS 限制！
        const MARKET_DATA = __MARKET_DATA_JSON__;

        // 1. 初始化头部与KPI数据
        document.getElementById("dom-trade-date").innerText = MARKET_DATA.trade_date;
        
        const medianVal = MARKET_DATA.median_return;
        const domMedian = document.getElementById("dom-median-return");
        domMedian.innerText = (medianVal >= 0 ? "+" : "") + medianVal.toFixed(2) + "%";
        domMedian.className = "kpi-value " + (medianVal >= 0 ? "up" : "down");
        
        document.getElementById("dom-rising-count").innerText = MARKET_DATA.rising_count + " 只";
        document.getElementById("dom-flat-count").innerText = MARKET_DATA.flat_count + " 只";
        document.getElementById("dom-falling-count").innerText = MARKET_DATA.falling_count + " 只";
        document.getElementById("dom-limit-ratio").innerText = MARKET_DATA.dist.limit_up + " / " + MARKET_DATA.dist.limit_down;

        // 2. 渲染全市场区间涨跌分布
        const chartDist = echarts.init(document.getElementById("chart-distribution"));
        const distData = MARKET_DATA.dist;
        
        const distOption = {
            backgroundColor: 'transparent',
            tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
            grid: { left: '3%', right: '3%', bottom: '3%', top: '10%', containLabel: true },
            xAxis: {
                type: 'category',
                data: ['跌停(<-9.9%)', '-7%~-9.9%', '-5%~-7%', '-3%~-5%', '-3%~0%', '0%~3%', '3%~5%', '5%~7%', '7%~9.9%', '涨停(>9.9%)'],
                axisLabel: { color: '#9ca3af', fontSize: 11 },
                axisLine: { lineStyle: { color: 'rgba(255, 255, 255, 0.08)' } }
            },
            yAxis: {
                type: 'value',
                axisLabel: { color: '#9ca3af' },
                splitLine: { lineStyle: { color: 'rgba(255, 255, 255, 0.03)' } }
            },
            series: [{
                data: [
                    { value: distData.limit_down, itemStyle: { color: '#10b981' } },
                    { value: distData.n7_10, itemStyle: { color: '#34d399' } },
                    { value: distData.n5_7, itemStyle: { color: '#6ee7b7' } },
                    { value: distData.n3_5, itemStyle: { color: '#a7f3d0' } },
                    { value: distData.n0_3, itemStyle: { color: '#d1fae5' } },
                    { value: distData.p0_3, itemStyle: { color: '#fee2e2' } },
                    { value: distData.p3_5, itemStyle: { color: '#fca5a5' } },
                    { value: distData.p5_7, itemStyle: { color: '#f87171' } },
                    { value: distData.p7_10, itemStyle: { color: '#f87171' } },
                    { value: distData.limit_up, itemStyle: { color: '#ef4444' } }
                ],
                type: 'bar',
                barWidth: '55%',
                label: { show: true, position: 'top', color: '#f3f4f6', fontSize: 11 }
            }]
        };
        chartDist.setOption(distOption);

        // 3. 渲染上证指数筹码压力支撑分布
        const chartSupport = echarts.init(document.getElementById("chart-index-support"));
        const supportData = MARKET_DATA.support;
        
        const supportOption = {
            backgroundColor: 'transparent',
            tooltip: { trigger: 'axis', formatter: '区间: {b}<br/>历史成交额: {c} 亿元' },
            grid: { left: '3%', right: '5%', bottom: '3%', top: '5%', containLabel: true },
            xAxis: {
                type: 'value',
                axisLabel: { color: '#9ca3af' },
                splitLine: { lineStyle: { color: 'rgba(255, 255, 255, 0.03)' } }
            },
            yAxis: {
                type: 'category',
                data: supportData.map(s => `${s.min_price.toFixed(0)}~${s.max_price.toFixed(0)}`),
                axisLabel: { color: '#9ca3af', fontSize: 11 },
                axisLine: { lineStyle: { color: 'rgba(255, 255, 255, 0.08)' } }
            },
            series: [{
                data: supportData.map(s => s.total_amount_billions.toFixed(1)),
                type: 'bar',
                itemStyle: {
                    color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                        { offset: 0, color: 'rgba(99, 102, 241, 0.3)' },
                        { offset: 1, color: '#06b6d4' }
                    ]),
                    borderRadius: [0, 4, 4, 0]
                },
                label: { show: true, position: 'right', color: '#f3f4f6', formatter: '{c} 亿' }
            }]
        };
        chartSupport.setOption(supportOption);

        // 4.1 填充行业板块多头动能表格 (Top 10)
        const indTbody = document.querySelector("#table-industries tbody");
        MARKET_DATA.industry_breadth.slice(0, 10).forEach(b => {
            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td><strong>${b.industry_name}</strong></td>
                <td>${b.total_stocks} 只</td>
                <td>
                    <div class="progress-bar-bg">
                        <div class="progress-bar-fill" style="width: ${b.bullish_ratio}%"></div>
                    </div>
                    <strong>${b.bullish_ratio.toFixed(1)}%</strong>
                </td>
                <td><span style="color: var(--primary); font-weight: bold;">${b.breakout_count} 只</span></td>
            `;
            indTbody.appendChild(tr);
        });

        // 4.2 填充概念板块多头动能表格 (Top 10)
        const secTbody = document.querySelector("#table-sectors tbody");
        MARKET_DATA.breadth.slice(0, 10).forEach(b => {
            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td><strong>${b.block_name}</strong></td>
                <td>${b.total_stocks} 只</td>
                <td>
                    <div class="progress-bar-bg">
                        <div class="progress-bar-fill" style="width: ${b.bullish_ratio}%"></div>
                    </div>
                    <strong>${b.bullish_ratio.toFixed(1)}%</strong>
                </td>
                <td><span style="color: var(--primary); font-weight: bold;">${b.breakout_count} 只</span></td>
            `;
            secTbody.appendChild(tr);
        });

        // 5. 填充短线连板情绪梯队表格
        const streakTbody = document.querySelector("#table-streaks tbody");
        
        // 按 streak 分组
        const streakGroups = {};
        MARKET_DATA.streaks.forEach(s => {
            streakGroups[s.streak] = streakGroups[s.streak] || [];
            const disp = s.name ? `${s.symbol}(${s.name})` : s.symbol;
            streakGroups[s.streak].push(disp);
        });
        
        const sortedStreaks = Object.keys(streakGroups).map(Number).sort((a,b) => b-a);
        
        if (sortedStreaks.length === 0) {
            const tr = document.createElement("tr");
            tr.innerHTML = `<td colspan="3" style="text-align: center; color: var(--text-muted); padding: 2rem;">当前短线冰点，无个股连板</td>`;
            streakTbody.appendChild(tr);
        } else {
            sortedStreaks.forEach(streak => {
                const tr = document.createElement("tr");
                tr.innerHTML = `
                    <td><span class="streak-badge">${streak} 连板</span></td>
                    <td><strong>${streakGroups[streak].length} 只</strong></td>
                    <td style="color: var(--primary);">${streakGroups[streak].join(", ")}</td>
                `;
                streakTbody.appendChild(tr);
            });
        }

        // 6. 响应式布局自适应
        window.addEventListener('resize', () => {
            chartDist.resize();
            chartSupport.resize();
        });
    </script>
</body>
</html>"""
    
    html_content = html_template.replace("__MARKET_DATA_JSON__", json_data_str)
    with open(OUTPUT_HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html_content)

if __name__ == '__main__':
    main()
