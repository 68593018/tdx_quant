import os
import sys
import json
import time
import re
import threading
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
    from fastapi.staticfiles import StaticFiles
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
        from fastapi.staticfiles import StaticFiles
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

try:
    import pandas as pd
except ImportError:
    print("⏳ 检测到当前环境未安装 Pandas 依赖，正在为您自动静默安装...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        import pandas as pd
        print("✅ Pandas 依赖自动安装成功！\n")
    except Exception as e:
        print(f"❌ 自动安装 Pandas 失败，请手动安装。错误: {e}")
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
    "signature": None
}

def get_data_signature() -> float:
    """获取数据池文件的最新修改时间特征签名，用于智能缓存校验"""
    try:
        if not os.path.exists(DATA_STORE_DIR):
            return 0.0
        files = [os.path.join(DATA_STORE_DIR, f) for f in os.listdir(DATA_STORE_DIR) if f.endswith('.parquet')]
        if not files:
            return 0.0
        return max(os.path.getmtime(f) for f in files)
    except Exception:
        return 0.0

# 异步数据同步全局状态
sync_task_status = {
    "active": False,
    "progress": 0,
    "logs": []
}
sync_lock = threading.Lock()

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
# 4. 异步数据同步子进程引擎
# -------------------------------------------------------------
def async_sync_worker():
    global sync_task_status
    with sync_lock:
        if sync_task_status["active"]:
            return
        sync_task_status["active"] = True
        sync_task_status["progress"] = 0
        sync_task_status["logs"] = ["🚀 开始启动全市场多进程增量数据同步... (Phase 2 & 3)\n"]

    try:
        process = subprocess.Popen(
            [sys.executable, "sync_market.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        progress_pattern = re.compile(r'进度:\s*\[(\d+)/(\d+)\]')
        
        for line in process.stdout:
            sync_task_status["logs"].append(line)
            if len(sync_task_status["logs"]) > 500:
                sync_task_status["logs"].pop(0)
                
            # 解析进度
            if "所有本地数据已是最新状态" in line:
                sync_task_status["progress"] = 100
            else:
                match = progress_pattern.search(line)
                if match:
                    completed, total = int(match.group(1)), int(match.group(2))
                    if total > 0:
                        sync_task_status["progress"] = int(completed * 100.0 / total)
                        
        process.wait()
        sync_task_status["progress"] = 100
        if process.returncode == 0:
            sync_task_status["logs"].append("\n✅ 数据同步圆满成功！数据池已对齐至最新状态。\n")
        else:
            sync_task_status["logs"].append(f"\n❌ 数据同步异常退出，退出码: {process.returncode}\n")
    except Exception as e:
        sync_task_status["logs"].append(f"\n❌ 启动同步子进程失败: {e}\n")
    finally:
        sync_task_status["active"] = False

# -------------------------------------------------------------
# 5. FastAPI 服务实例初始化
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

def generate_category_filter(categories: list[str]) -> str:
    """根据标的分类生成 DuckDB filename 模糊匹配 SQL 子句"""
    if not categories:
        return "(filename LIKE '%sh60%' OR filename LIKE '%sh68%' OR filename LIKE '%sz00%' OR filename LIKE '%sz30%' OR filename LIKE '%/bj%')"
        
    mapping = {
        "stock": "(filename LIKE '%sh60%' OR filename LIKE '%sh68%' OR filename LIKE '%sz00%' OR filename LIKE '%sz30%' OR filename LIKE '%/bj%')",
        "index": "(filename LIKE '%sh000%' OR filename LIKE '%sz399%')",
        "sector": "(filename LIKE '%sh88%' OR filename LIKE '%sz88%')",
        "fund": "(filename LIKE '%sh50%' OR filename LIKE '%sh51%' OR filename LIKE '%sh52%' OR filename LIKE '%sh58%' OR filename LIKE '%sz15%' OR filename LIKE '%sz16%' OR filename LIKE '%sz18%')",
        "bond": "(filename LIKE '%sh11%' OR filename LIKE '%sh13%' OR filename LIKE '%sz12%')"
    }
    
    clauses = []
    for cat in categories:
        cat_lower = cat.lower()
        if cat_lower in mapping:
            clauses.append(mapping[cat_lower])
            
    if not clauses:
        return mapping["stock"]
        
    return f"({' OR '.join(clauses)})"

# Pydantic 策略参数模型
class ScreenerRequest(BaseModel):
    strategies: list[str]
    categories: list[str] = ["stock"]

class AddStrategyRequest(BaseModel):
    key: str
    name: str
    description: str
    query_sql: str

# -------------------------------------------------------------
# 6. RESTful APIs 接口路由定义
# -------------------------------------------------------------

ANALYTICAL_STRATEGY_KEYS = {
    "market_temperature", "sector_breadth", "index_support", 
    "limit_up_streaks", "industry_breadth", "industry_flow_30d", 
    "concept_flow_30d"
}

@app.get("/api/strategies", summary="列出所有可用的选股及分析策略")
def get_all_strategies():
    strategies = load_strategies()
    filtered_strategies = [
        {
            "id": key,
            "name": val["name"],
            "description": val["description"]
        } for key, val in strategies.items() if key not in ANALYTICAL_STRATEGY_KEYS
    ]
    return JSONResponse(content={
        "status": "success",
        "count": len(filtered_strategies),
        "strategies": filtered_strategies
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
def get_market_data(refresh: bool = False):
    global _MARKET_CACHE
    
    # 智能数据指纹指征，当 underlying 数据没有发生更新且没有强制刷新时，直接秒级返回缓存数据
    current_sig = get_data_signature()
    if not refresh and _MARKET_CACHE["data"] is not None and _MARKET_CACHE["signature"] == current_sig:
        cached_data = _MARKET_CACHE["data"].copy()
        cached_data["is_cached"] = True
        return cached_data

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

    default_stock_filter = "(filename LIKE '%sh60%' OR filename LIKE '%sh68%' OR filename LIKE '%sz00%' OR filename LIKE '%sz30%' OR filename LIKE '%/bj%')"

    try:
        # 1. 市场温度与涨跌区间
        sql_temp = strategies["market_temperature"]["query_sql"]\
            .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
            .replace("__PATTERNS_STR__", patterns_str)\
            .replace("__CATEGORY_FILTER__", default_stock_filter)
        df_temp = con.execute(sql_temp).fetchdf()

        # 2. 连板高度梯队
        sql_streaks = strategies["limit_up_streaks"]["query_sql"]\
            .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
            .replace("__PATTERNS_STR__", patterns_str)\
            .replace("__CATEGORY_FILTER__", default_stock_filter)
        df_streaks = con.execute(sql_streaks).fetchdf()

        # 3. 概念板块宽度
        sql_breadth = strategies["sector_breadth"]["query_sql"]\
            .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
            .replace("__PATTERNS_STR__", patterns_str)\
            .replace("__BLOCK_MAPPINGS_PATH__", BLOCK_MAPPINGS_PATH)\
            .replace("__CATEGORY_FILTER__", default_stock_filter)
        df_breadth = con.execute(sql_breadth).fetchdf()

        # 4. 指数支撑压力
        sql_support = strategies["index_support"]["query_sql"]\
            .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)
        df_support = con.execute(sql_support).fetchdf()

        # 5. 行业板块宽度
        sql_ind_breadth = strategies["industry_breadth"]["query_sql"]\
            .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
            .replace("__PATTERNS_STR__", patterns_str)\
            .replace("__INDUSTRY_MAPPINGS_PATH__", INDUSTRY_MAPPINGS_PATH)\
            .replace("__CATEGORY_FILTER__", default_stock_filter)
        df_ind_breadth = con.execute(sql_ind_breadth).fetchdf()

        # 6. 行业30日资金流向
        sql_ind_flow = strategies["industry_flow_30d"]["query_sql"]\
            .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
            .replace("__PATTERNS_STR__", patterns_str)\
            .replace("__INDUSTRY_MAPPINGS_PATH__", INDUSTRY_MAPPINGS_PATH)\
            .replace("__CATEGORY_FILTER__", default_stock_filter)
        df_ind_flow = con.execute(sql_ind_flow).fetchdf()

        # 7. 概念30日资金流向
        sql_concept_flow = strategies["concept_flow_30d"]["query_sql"]\
            .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
            .replace("__PATTERNS_STR__", patterns_str)\
            .replace("__BLOCK_MAPPINGS_PATH__", BLOCK_MAPPINGS_PATH)\
            .replace("__CATEGORY_FILTER__", default_stock_filter)
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

    calc_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_data = {
        "status": "success",
        "is_cached": False,
        "cache_time": calc_time,
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
    _MARKET_CACHE["signature"] = current_sig
    
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
        categories = req.categories if req.categories is not None else ["stock"]
        category_filter = generate_category_filter(categories)
        
        for key in req.strategies:
            sql = strategies[key]["query_sql"]\
                .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
                .replace("__PATTERNS_STR__", patterns_str)\
                .replace("__BLOCK_MAPPINGS_PATH__", BLOCK_MAPPINGS_PATH)\
                .replace("__INDUSTRY_MAPPINGS_PATH__", INDUSTRY_MAPPINGS_PATH)\
                .replace("__CATEGORY_FILTER__", category_filter)
            
            # 自动剔除今日未交易/停牌股票 (date为大盘最新交易日且volume > 0)
            sql = sql.replace("row_num = 1", f"row_num = 1 AND date = (SELECT MAX(date) FROM read_parquet('{DATA_STORE_DIR}/sh600000.parquet')) AND volume > 0")
            sql = sql.replace("rn = 1", f"rn = 1 AND date = (SELECT MAX(date) FROM read_parquet('{DATA_STORE_DIR}/sh600000.parquet')) AND volume > 0")

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
    if 'symbol' not in res_df.columns or 'Close' not in res_df.columns or 'Vol_Ratio' not in res_df.columns:
        return JSONResponse(status_code=400, content={
            "status": "error",
            "message": "执行因子选股失败！您选择的策略非‘个股选股策略’（结果集缺少 symbol, Close 或 Vol_Ratio 列）。大盘情绪与分析数据请直接在‘市场总览’或‘大势分析’选项卡中查看！"
        })

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

@app.post("/api/screener/save_report", summary="生成并保存选股报告到report文件夹中")
def save_screener_report(req: ScreenerRequest):
    if not req.strategies:
        return JSONResponse(status_code=400, content={"status": "error", "message": "必须指定至少一个选股策略名称！"})
        
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
        categories = req.categories if req.categories is not None else ["stock"]
        category_filter = generate_category_filter(categories)
        
        for key in req.strategies:
            sql = strategies[key]["query_sql"]\
                .replace("__DATA_STORE_DIR__", DATA_STORE_DIR)\
                .replace("__PATTERNS_STR__", patterns_str)\
                .replace("__BLOCK_MAPPINGS_PATH__", BLOCK_MAPPINGS_PATH)\
                .replace("__INDUSTRY_MAPPINGS_PATH__", INDUSTRY_MAPPINGS_PATH)\
                .replace("__CATEGORY_FILTER__", category_filter)
            
            # 自动剔除今日未交易/停牌股票 (date为大盘最新交易日且volume > 0)
            sql = sql.replace("row_num = 1", f"row_num = 1 AND date = (SELECT MAX(date) FROM read_parquet('{DATA_STORE_DIR}/sh600000.parquet')) AND volume > 0")
            sql = sql.replace("rn = 1", f"rn = 1 AND date = (SELECT MAX(date) FROM read_parquet('{DATA_STORE_DIR}/sh600000.parquet')) AND volume > 0")

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

    # 获取最新交易日
    try:
        latest_date_df = con.execute(f"SELECT MAX(date) AS mdate FROM read_parquet('{DATA_STORE_DIR}/sh600000.parquet')").fetchdf()
        latest_date_str = latest_date_df['mdate'].iloc[0].strftime('%Y-%m-%d')
    except Exception:
        latest_date_str = datetime.now().strftime('%Y-%m-%d')

    date_suffix = latest_date_str.replace("-", "")
    report_dir = os.path.join(CURRENT_DIR, "report")
    os.makedirs(report_dir, exist_ok=True)

    # 判断是单策略还是多策略融合
    if len(req.strategies) == 1:
        strategy_key = req.strategies[0]
        filename = f"{strategy_key}_report_{date_suffix}.md"
        report_path = os.path.join(report_dir, filename)
        
        # 格式化数据
        if res_df is not None and not res_df.empty:
            res_df['Symbol'] = res_df['symbol'].str.upper()
            res_df['Name'] = res_df['symbol'].map(lambda x: names_map.get(x.lower(), ""))
            res_df['Close_Formatted'] = res_df['Close'].map(lambda x: round(float(x), 2))
            res_df['Vol_Ratio_Formatted'] = res_df['Vol_Ratio'].map(lambda x: round(float(x), 2))
            res_df['Dev_MA20_Pct_Formatted'] = res_df['Dev_MA20_Pct'].map(lambda x: round(float(x), 2))
            
            # Extract 5th column dynamically
            fifth_col = res_df.columns[4]
            res_df['Resonance_Sectors'] = res_df[fifth_col]
        
        md_content = f"""# 🚀 全市场资金共振突破选股报告

**策略名称**：`{strategies[strategy_key]['name']}`
**策略描述**：{strategies[strategy_key]['description']}
**分析交易日**：`{latest_date_str}`
**分析标的总数**：`{len(os.listdir(DATA_STORE_DIR))} 个`
**报告生成时间**：`{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`

---

## 🏆 黄金共振突破股列表
共筛选出 **{len(res_df) if res_df is not None else 0}** 只黄金个股，已按今日放量倍数降序排列：

| 序号 | 股票代码 | 股票名称 | 最新收盘价 | 今日放量倍数 | MA20 偏离度 | 触发共振爆发板块（突破只数/板块总股数占比） |
| :---: | :---: | :---: | :---: | :---: | :---: | :--- |
"""
        if res_df is not None and not res_df.empty:
            for idx, row in res_df.reset_index(drop=True).iterrows():
                md_content += f"| {idx+1} | `{row['Symbol']}` | {row['Name']} | {row['Close_Formatted']} | {row['Vol_Ratio_Formatted']} | {row['Dev_MA20_Pct_Formatted']} | {row['Resonance_Sectors']} |\n"
        else:
            md_content += "| -- | -- | -- | -- | -- | -- | 暂无筛选结果 |\n"
            
        md_content += """
---

## 💡 选股策略释义
> [!NOTE]
> * **策略核心**：本选股结果完全由 `strategies.json` 配置文件中的 SQL 逻辑驱动，完美实现了算法与源码的分离。
> * **行业共振**：统计每个概念板块中，当天有多少只股票同时触发该策略突破。**只保留其所属板块中“当天至少有 3 只股票同时突破”的成分股**，并计算出突破只数占该板块总股数的比例，完美锁定主力资金最抱团、集聚突破度（Breadth）最高的核心市场风口！
"""
    else:
        # 多策略融合
        filename = f"dual_intersection_report_{date_suffix}.md"
        report_path = os.path.join(report_dir, filename)
        
        if res_df is not None and not res_df.empty:
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

        md_content = f"""# 🚀 全市场多策略融合黄金交集选股报告

**分析交易日**：`{latest_date_str}`
**参与融合的策略列表**：
"""
        for key in req.strategies:
            md_content += f"* 🔹 **{strategies[key]['name']}**：{strategies[key]['description']}\n"
            
        md_content += f"""**报告生成时间**：`{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`

---

## 🏆 黄金多重共振交集股列表
本列表中的个股**必须同时百分之百满足以上所有选股策略**，代表了市场中最强悍的量化共鸣点：

| 序号 | 股票代码 | 股票名称 | 最新收盘价 | 今日放量倍数 | MA20 偏离度 | 综合触发共振板块（突破只数/占比） |
| :---: | :---: | :---: | :---: | :---: | :---: | :--- |
"""
        if res_df is not None and not res_df.empty:
            for idx, row in res_df.reset_index(drop=True).iterrows():
                md_content += f"| {idx+1} | `{row['Symbol']}` | {row['Name']} | {row['Close_Formatted']} | {row['Vol_Ratio_Formatted']} | {row['Dev_MA20_Pct_Formatted']} | {row['Merged_Sectors']} |\n"
        else:
            md_content += "| -- | -- | -- | -- | -- | -- | 暂无筛选结果 |\n"
            
        md_content += """
---

## 💡 多策略融合（Strategy Fusion）释义
> [!IMPORTANT]
> * **黄金交集（Intersection）原理**：在量化实战中，单个策略往往容易受到噪音干扰。我们通过对**独立多策略的选股结果在 Python 层进行 inner join 求取交集**，强力过滤掉不合规的杂音，只保留了在**均线形态（Trend）、资金量能（Volume）以及板块集聚爆发（Sector Breadth）**三大周期上形成全面多头共鸣的极致黑马个股。
"""

    try:
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(md_content)
        return {
            "status": "success",
            "message": f"报告保存成功：{filename}",
            "filename": filename
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": f"写入报告失败: {e}"})

# -------------------------------------------------------------
# 7. Web 交易平台动态同步与查询 API 扩展 (GET/POST)
# -------------------------------------------------------------
@app.post("/api/market/sync", summary="触发异步后台多进程数据同步任务")
def start_market_sync():
    global sync_task_status
    if sync_task_status["active"]:
        return JSONResponse(status_code=400, content={"status": "error", "message": "同步进程已经在运行中！"})
        
    # 启动后台线程异步运行同步任务
    t = threading.Thread(target=async_sync_worker)
    t.daemon = True
    t.start()
    return {"status": "success", "message": "数据同步后台进程已成功拉起，正在同步！"}

@app.get("/api/market/sync/status", summary="查询当前后台数据同步进度及日志控制台")
def get_market_sync_status():
    return {
        "status": "success",
        "active": sync_task_status["active"],
        "progress": sync_task_status["progress"],
        "logs": "".join(sync_task_status["logs"])
    }

@app.get("/api/market/query", summary="板块与股票双向极速交叉搜索 API")
def query_stocks_or_sectors(keyword: str):
    if not keyword or len(keyword.strip()) == 0:
        return JSONResponse(status_code=400, content={"status": "error", "message": "查询关键词不能为空！"})
        
    keyword = keyword.strip()
    tdx_dir = load_tdx_dir()
    names_map = load_stock_names(tdx_dir)
    con = duckdb.connect()
    
    # 1. 股票代码/简拼查询所属板块 (例如 600000)
    if re.match(r'^\d{6}$', keyword) or keyword.lower().startswith(('sh', 'sz', 'bj')):
        code_only = keyword[-6:] if len(keyword) > 6 else keyword
        # 查询所属概念
        sql_c = f"SELECT block_name FROM read_parquet('{BLOCK_MAPPINGS_PATH}') WHERE code = '{code_only}'"
        df_c = con.execute(sql_c).fetchdf()
        
        # 查询所属行业
        symbol_pattern = f"%{code_only}"
        sql_i = f"SELECT industry_name FROM read_parquet('{INDUSTRY_MAPPINGS_PATH}') WHERE symbol LIKE '{symbol_pattern}'"
        df_i = con.execute(sql_i).fetchdf()
        
        concepts = df_c['block_name'].tolist()
        industries = df_i['industry_name'].tolist()
        symbol_full = [s for s in names_map.keys() if s.endswith(code_only)]
        stock_name = names_map.get(symbol_full[0], "") if symbol_full else "未知"
        
        return {
            "status": "success",
            "type": "stock",
            "symbol": symbol_full[0].upper() if symbol_full else code_only,
            "name": stock_name,
            "concepts": concepts,
            "industries": industries
        }
        
    # 2. 板块名称查询成分股 (例如 半导体 / 华为概念)
    else:
        # 模糊查询行业名称
        sql_i_match = f"SELECT symbol FROM read_parquet('{INDUSTRY_MAPPINGS_PATH}') WHERE industry_name LIKE '%{keyword}%'"
        df_i_match = con.execute(sql_i_match).fetchdf()
        
        # 模糊查询概念名称
        sql_c_match = f"SELECT market, code FROM read_parquet('{BLOCK_MAPPINGS_PATH}') WHERE block_name LIKE '%{keyword}%'"
        df_c_match = con.execute(sql_c_match).fetchdf()
        
        stocks = []
        seen = set()
        
        # 组装行业匹配
        for _, row in df_i_match.iterrows():
            sym = row['symbol'].lower()
            if sym not in seen:
                seen.add(sym)
                stocks.append({"symbol": sym.upper(), "name": names_map.get(sym, "")})
                
        # 组装概念匹配
        for _, row in df_c_match.iterrows():
            sym = f"{row['market']}{row['code']}".lower()
            if sym not in seen:
                seen.add(sym)
                stocks.append({"symbol": sym.upper(), "name": names_map.get(sym, "")})
                
        return {
            "status": "success",
            "type": "sector",
            "query": keyword,
            "count": len(stocks),
            "stocks": stocks
        }

# -------------------------------------------------------------
# 8. 报告归档列表与 Markdown 内容渲染 APIs (GET)
# -------------------------------------------------------------
REPORT_DIR = os.path.join(CURRENT_DIR, "report")
if not os.path.exists(REPORT_DIR):
    os.makedirs(REPORT_DIR, exist_ok=True)

@app.get("/api/reports", summary="获取项目目录下的历史量化选股报告列表")
def list_reports():
    reports = []
    if os.path.exists(REPORT_DIR):
        files = os.listdir(REPORT_DIR)
        for f in files:
            if f.endswith(".md") and f not in ["README.md", "README_cn.md", "README_zh.md"]:
                path = os.path.join(REPORT_DIR, f)
                stat = os.stat(path)
                reports.append({
                    "filename": f,
                    "size_kb": round(stat.st_size / 1024, 2),
                    "last_modified": datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                })
            
    # 按照最近修改时间倒序
    reports.sort(key=lambda x: x["last_modified"], reverse=True)
    return {
        "status": "success",
        "count": len(reports),
        "reports": reports
    }

@app.get("/api/reports/content", summary="读取指定 Markdown 选股报告的源码内容")
def get_report_content(filename: str):
    # 安全验证，防止目录穿越攻击
    safe_filename = os.path.basename(filename)
    if not safe_filename.endswith(".md") or safe_filename in ["README.md"]:
         return JSONResponse(status_code=400, content={"status": "error", "message": "非法或受限的文件名称！"})
         
    path = os.path.join(REPORT_DIR, safe_filename)
    if not os.path.exists(path):
         return JSONResponse(status_code=404, content={"status": "error", "message": f"报告文件 {safe_filename} 不存在！"})
         
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        return {
            "status": "success",
            "filename": safe_filename,
            "content": content
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": f"读取失败: {e}"})

# -------------------------------------------------------------
# 9. 动态托管 HTML 赛博看板 (GET /)
# -------------------------------------------------------------
@app.get("/", response_class=HTMLResponse, summary="动态读取并渲染赛博大势看板仪表盘(完美免除CORS)")
@app.get("/dashboard", response_class=HTMLResponse)
def get_dashboard():
    try:
        market_data = get_market_data()
    except Exception as e:
        return HTMLResponse(content=f"<h1>❌ 市场数据链算失败: {e}</h1>", status_code=500)

    if not os.path.exists(HTML_TEMPLATE_PATH):
        return HTMLResponse(content=f"<h1>❌ 错误: 板板模版 {HTML_TEMPLATE_PATH} 不存在！</h1>", status_code=404)
        
    try:
        with open(HTML_TEMPLATE_PATH, "r", encoding="utf-8") as f:
            template = f.read()
        html_rendered = template.replace("__MARKET_DATA_JSON__", json.dumps(market_data, ensure_ascii=False))
        return HTMLResponse(content=html_rendered)
    except Exception as e:
        return HTMLResponse(content=f"<h1>❌ 服务端看板渲染失败: {e}</h1>", status_code=500)

# -------------------------------------------------------------
# 10. 静态挂载 Web 主独立网页门户文件夹
# -------------------------------------------------------------
web_folder_path = os.path.join(CURRENT_DIR, "web")
if not os.path.exists(web_folder_path):
    os.makedirs(web_folder_path, exist_ok=True)
    
app.mount("/web", StaticFiles(directory=web_folder_path), name="web")

# -------------------------------------------------------------
# 11. Web 后端启动入口
# -------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 80)
    print("      通达信极速多因子量化选股平台 - Web 独立服务器启动 (B/S 架构底座)")
    print("=" * 80)
    # 本地局域网支持：监听 0.0.0.0 端口 8000
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
