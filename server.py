import os
import sys
import json
import time
from datetime import datetime
import subprocess

# -------------------------------------------------------------
# 1. 动态检测并自动静默安装 FastAPI 与 Uvicorn (极简免运维体验)
# -------------------------------------------------------------
try:
    import fastapi
    import uvicorn
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import HTMLResponse, JSONResponse
    from pydantic import BaseModel
except ImportError:
    print("⏳ 检测到当前环境未安装 FastAPI 或 Uvicorn 依赖，正在为您自动静默安装...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "fastapi", "uvicorn", "pydantic"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        import fastapi
        import uvicorn
        from fastapi.middleware.cors import CORSMiddleware
        from fastapi.responses import HTMLResponse, JSONResponse
        from pydantic import BaseModel
        print("✅ FastAPI, Uvicorn & Pydantic 依赖自动安装成功！立即启动 Web 量化服务...\n")
    except Exception as e:
        print(f"❌ 自动安装 Web 依赖失败，请在终端手动运行 'pip install fastapi uvicorn pydantic'。错误详情: {e}")
        sys.exit(1)

try:
    import duckdb
except ImportError:
    print("⏳ 检测到当前环境未安装 DuckDB 依赖，正在为您自动静默安装...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "duckdb"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        import duckdb
        print("✅ DuckDB 依赖自动安装成功！\n")
    except Exception as e:
        print(f"❌ 自动安装 DuckDB 失败，请手动安装。错误: {e}")
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
HTML_TEMPLATE_PATH = os.path.join(CURRENT_DIR, "market_dashboard.html")

# 缓存大势计算结果
_MARKET_CACHE = {
    "data": None,
    "timestamp": 0.0
}
CACHE_EXPIRE_SECONDS = 15.0  # 15秒内重复请求直接走缓存，避免频繁穿透 DuckDB

# -------------------------------------------------------------
# 3. 核心工具与数据解析函数
# -------------------------------------------------------------
def load_tdx_dir() -> str:
    """从 config.json 载入通达信路径"""
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
    """极速解析股票代码与名称的映射"""
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
                        names_map[f"{market}{code}"] = name
            except Exception as e:
                print(f"⚠️ 警告: 解析 {path} 失败: {e}")
    return names_map

def load_strategies() -> dict:
    """动态载入 strategies.json"""
    if not os.path.exists(STRATEGIES_PATH):
        return {}
    try:
        with open(STRATEGIES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def process_flow_data(df, name_col) -> dict:
    """处理板块资金流向 DataFrame，提取最新交易日 TOP 10 和 BOTTOM 10 的 30 日时序数据"""
    if df.empty:
        return {"dates": [], "top_10": [], "bottom_10": [], "series": []}
    
    df['date_str'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else str(x)[:10])
    dates = sorted(list(df['date_str'].unique()))
    
    if not dates:
        return {"dates": [], "top_10": [], "bottom_10": [], "series": []}
        
    latest_date_str = dates[-1]
    
    # 筛选最新交易日，排序获取 TOP 10 和 BOTTOM 10
    df_latest = df[df['date_str'] == latest_date_str].sort_values(by='sector_ratio', ascending=False)
    top_10_names = df_latest.head(10)[name_col].tolist()
    bottom_10_names = df_latest.tail(10)[name_col].tolist()
    
    # 构建数据字典加速组装
    lookup = {}
    for _, row in df.iterrows():
        lookup[(row[name_col], row['date_str'])] = (
            float(row['sector_amount']) / 1e8,  # 亿元
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

def get_parquet_patterns() -> str:
    """动态获取 K线文件匹配模式"""
    prefixes = ['sh', 'sz', 'bj']
    existing_files = os.listdir(DATA_STORE_DIR)
    patterns = []
    for prefix in prefixes:
        if any(f.startswith(prefix) and f.endswith('.parquet') for f in existing_files):
            patterns.append(f"{DATA_STORE_DIR}/{prefix}*.parquet")
            
    if not patterns:
        raise FileNotFoundError("未在数据目录中找到任何有效 *.parquet 缓存文件。")
    return ", ".join(f"'{p}'" for p in patterns)

# -------------------------------------------------------------
# 4. FastAPI 服务实例初始化
# -------------------------------------------------------------
app = fastapi.FastAPI(
    title="通达信极速多因子量化选股系统 - Web API 服务平台",
    description="基于 FastAPI + DuckDB 的本地极速 B/S 量化后台",
    version="1.0.0"
)

# 跨域设置，支持前后端分离部署
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic 策略参数模型
class ScreenerRequest(BaseModel):
    strategies: list[str]

class AddStrategyRequest(BaseModel):
    key: str
    name: str
    description: str
    query_sql: str

# -------------------------------------------------------------
# 5. RESTful APIs 接口路由定义
# -------------------------------------------------------------

@app.get("/api/strategies", summary="列出所有可用的选股及分析策略")
def get_all_strategies():
    strategies = load_strategies()
    return JSONResponse(content={
        "status": "success",
        "count": len(strategies),
        "strategies": [
            {
                "id": key,
                "name": val["name"],
                "description": val["description"]
            } for key, val in strategies.items()
        ]
    })

@app.post("/api/strategies", summary="动态增加或修改 SQL 量化选股策略")
def add_new_strategy(req: AddStrategyRequest):
    strategies = load_strategies()
    strategies[req.key] = {
        "name": req.name,
        "description": req.description,
        "query_sql": req.query_sql
    }
    try:
        with open(STRATEGIES_PATH, "w", encoding="utf-8") as f:
            json.dump(strategies, f, ensure_ascii=False, indent=4)
        return {"status": "success", "message": f"策略 {req.name} 保存成功！"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": f"保存失败: {e}"})

@app.get("/api/market/data", summary="极速多线程计算并获取全市场情绪与大盘雷达 JSON 数据")
def get_market_data():
    global _MARKET_CACHE
    now = time.time()
    
    # 命中缓存
    if _MARKET_CACHE["data"] is not None and (now - _MARKET_CACHE["timestamp"]) < CACHE_EXPIRE_SECONDS:
        return _MARKET_CACHE["data"]

    t_start = time.perf_counter()
    tdx_dir = load_tdx_dir()
    names_map = load_stock_names(tdx_dir)
    strategies = load_strategies()
    
    try:
        patterns_str = get_parquet_patterns()
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

    # DuckDB 并行计算连接
    con = duckdb.connect()
    con.execute(f"SET threads = {os.cpu_count()}")

    try:
        # 1. 市场温度与涨跌区间
        sql_temp = strategies["market_temperature"]["query_sql"]\
            .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
            .replace("__PATTERNS_STR__", patterns_str)
        df_temp = con.execute(sql_temp).fetchdf()

        # 2. 连板高度梯队
        sql_streaks = strategies["limit_up_streaks"]["query_sql"]\
            .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
            .replace("__PATTERNS_STR__", patterns_str)
        df_streaks = con.execute(sql_streaks).fetchdf()

        # 3. 概念板块宽度
        sql_breadth = strategies["sector_breadth"]["query_sql"]\
            .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
            .replace("__PATTERNS_STR__", patterns_str)\
            .replace("__BLOCK_MAPPINGS_PATH__", BLOCK_MAPPINGS_PATH)
        df_breadth = con.execute(sql_breadth).fetchdf()

        # 4. 指数支撑压力
        sql_support = strategies["index_support"]["query_sql"]\
            .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)
        df_support = con.execute(sql_support).fetchdf()

        # 5. 行业板块宽度
        sql_ind_breadth = strategies["industry_breadth"]["query_sql"]\
            .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
            .replace("__PATTERNS_STR__", patterns_str)\
            .replace("__INDUSTRY_MAPPINGS_PATH__", INDUSTRY_MAPPINGS_PATH)
        df_ind_breadth = con.execute(sql_ind_breadth).fetchdf()

        # 6. 行业30日资金流向
        sql_ind_flow = strategies["industry_flow_30d"]["query_sql"]\
            .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
            .replace("__PATTERNS_STR__", patterns_str)\
            .replace("__INDUSTRY_MAPPINGS_PATH__", INDUSTRY_MAPPINGS_PATH)
        df_ind_flow = con.execute(sql_ind_flow).fetchdf()

        # 7. 概念30日资金流向
        sql_concept_flow = strategies["concept_flow_30d"]["query_sql"]\
            .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
            .replace("__PATTERNS_STR__", patterns_str)\
            .replace("__BLOCK_MAPPINGS_PATH__", BLOCK_MAPPINGS_PATH)
        df_concept_flow = con.execute(sql_concept_flow).fetchdf()
        
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": f"DuckDB 核心SQL执行失败: {e}"})

    # 数据组装与整理
    row_temp = df_temp.iloc[0]
    total_stocks = int(row_temp['total_stocks'])
    limit_up = int(row_temp['limit_up'])
    limit_down = int(row_temp['limit_down'])
    median_return = float(row_temp['median_return'])
    trade_date = row_temp['trade_date']
    if hasattr(trade_date, 'strftime'):
        trade_date_str = trade_date.strftime('%Y-%m-%d')
    else:
        s = str(trade_date)
        trade_date_str = f"{s[:4]}-{s[4:6]}-{s[6:8]}" if len(s) == 8 else s

    streaks_list = []
    streak_counts = {}
    for _, r in df_streaks.iterrows():
        stk = int(r['streak'])
        sym = r['symbol'].upper()
        cname = names_map.get(sym.lower(), "")
        streaks_list.append({"symbol": sym, "name": cname, "streak": stk})
        streak_counts[stk] = streak_counts.get(stk, 0) + 1

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

    support_list = []
    for _, r in df_support.iterrows():
        support_list.append({
            "bucket": int(r['bucket']),
            "min_price": float(r['min_price']),
            "max_price": float(r['max_price']),
            "total_amount_billions": float(r['total_amount']) / 1e8
        })

    industry_flow_processed = process_flow_data(df_ind_flow, 'industry_name')
    concept_flow_processed = process_flow_data(df_concept_flow, 'block_name')

    full_data = {
        "status": "success",
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
        "concept_flow": concept_flow_processed,
        "compute_time_seconds": round(time.perf_counter() - t_start, 2)
    }

    # 写入缓存
    _MARKET_CACHE["data"] = full_data
    _MARKET_CACHE["timestamp"] = now
    
    return full_data

@app.post("/api/screener/run", summary="极速计算并获取多因子策略选股名册")
def run_screener(req: ScreenerRequest):
    if not req.strategies:
        return JSONResponse(status_code=400, content={"status": "error", "message": "必须指定至少一个选股策略名称！"})
        
    t_start = time.perf_counter()
    tdx_dir = load_tdx_dir()
    names_map = load_stock_names(tdx_dir)
    strategies = load_strategies()
    
    # 策略合法性检测
    invalid_keys = [k for k in req.strategies if k not in strategies]
    if invalid_keys:
        return JSONResponse(status_code=400, content={"status": "error", "message": f"未定义的策略: {invalid_keys}"})

    try:
        patterns_str = get_parquet_patterns()
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

    # DuckDB 并行计算连接
    con = duckdb.connect()
    con.execute(f"SET threads = {os.cpu_count()}")

    import pandas as pd
    dfs = []
    
    try:
        for key in req.strategies:
            sql = strategies[key]["query_sql"]\
                .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
                .replace("__PATTERNS_STR__", patterns_str)\
                .replace("__BLOCK_MAPPINGS_PATH__", BLOCK_MAPPINGS_PATH)\
                .replace("__INDUSTRY_MAPPINGS_PATH__", INDUSTRY_MAPPINGS_PATH)
            
            df = con.execute(sql).fetchdf()
            dfs.append(df)
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": f"执行策略 SQL 失败: {e}"})

    # 多策略结果求交集
    res_df = None
    if dfs:
        res_df = dfs[0]
        for next_df in dfs[1:]:
            res_df = pd.merge(res_df, next_df, on='symbol', suffixes=('', '_other'))

    if res_df is None or res_df.empty:
        return {
            "status": "success",
            "count": 0,
            "stocks": [],
            "message": "策略过滤后结果为空。提示：多重条件极为严苛，主力尚未形成全面合力，建议继续空仓防守。"
        }

    # 规范与整理选股数据列表
    res_df['Symbol'] = res_df['symbol'].str.upper()
    res_df['Name'] = res_df['symbol'].map(lambda x: names_map.get(x.lower(), ""))
    res_df['Close_Formatted'] = res_df['Close'].map(lambda x: round(float(x), 2))
    res_df['Vol_Ratio_Formatted'] = res_df['Vol_Ratio'].map(lambda x: round(float(x), 2))
    res_df['Dev_MA20_Pct_Formatted'] = res_df['Dev_MA20_Pct'].map(lambda x: round(float(x), 2))

    sector_cols = [c for c in res_df.columns if c.startswith('Resonance_Sectors')]
    def merge_sectors(row):
        sectors = []
        for col in sector_cols:
            if pd.notnull(row[col]):
                sectors.extend([s.strip() for s in row[col].split(',')])
        return ", ".join(sorted(list(set(sectors))))
        
    res_df['Merged_Sectors'] = res_df.apply(merge_sectors, axis=1)
    
    stocks_list = []
    for _, row in res_df.iterrows():
        stocks_list.append({
            "symbol": row['Symbol'],
            "name": row['Name'],
            "close": row['Close_Formatted'],
            "vol_ratio": row['Vol_Ratio_Formatted'],
            "dev_ma20_pct": row['Dev_MA20_Pct_Formatted'],
            "sectors": row['Merged_Sectors']
        })

    return {
        "status": "success",
        "count": len(stocks_list),
        "compute_time_seconds": round(time.perf_counter() - t_start, 2),
        "stocks": stocks_list
    }

# -------------------------------------------------------------
# 6. HTML 赛博黑暗大势看板动态渲染接口 (GET / /dashboard)
# -------------------------------------------------------------

@app.get("/", response_class=HTMLResponse, summary="动态读取并渲染赛博大势看板仪表盘(完美免除CORS)")
@app.get("/dashboard", response_class=HTMLResponse)
def get_dashboard():
    # 1. 动态生成最新市场大势与情绪数据
    try:
        market_data = get_market_data()
    except Exception as e:
        return HTMLResponse(content=f"<h1>❌ 市场数据链算失败: {e}</h1>", status_code=500)

    # 2. 读取本地看板模版文件
    if not os.path.exists(HTML_TEMPLATE_PATH):
        return HTMLResponse(content=f"<h1>❌ 错误: 板板模版 {HTML_TEMPLATE_PATH} 不存在！</h1>", status_code=404)
        
    try:
        with open(HTML_TEMPLATE_PATH, "r", encoding="utf-8") as f:
            template = f.read()
            
        # 3. 动态字符串替换注入，零CORS限制！
        html_rendered = template.replace("__MARKET_DATA_JSON__", json.dumps(market_data, ensure_ascii=False))
        return HTMLResponse(content=html_rendered)
    except Exception as e:
        return HTMLResponse(content=f"<h1>❌ 服务端看板渲染失败: {e}</h1>", status_code=500)

# -------------------------------------------------------------
# 7. Web 后端启动入口
# -------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 80)
    print("      通达信极速多因子量化选股平台 - Web 独立服务器启动 (B/S 架构底座)")
    print("=" * 80)
    # 本地局域网支持：监听 0.0.0.0 端口 8000
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
