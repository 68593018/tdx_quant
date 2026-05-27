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

_STOCK_NAMES_CACHE = []

LEVEL2_CHAR_MAP = {
    "亳": "b",
    "兖": "y",
    "冢": "z",
    "厝": "c",
    "圳": "z",
    "塍": "c",
    "奕": "y",
    "孚": "f",
    "宸": "c",
    "寰": "h",
    "岚": "l",
    "岱": "d",
    "崛": "j",
    "崧": "s",
    "崴": "w",
    "嵊": "s",
    "嵘": "r",
    "弋": "y",
    "徕": "l",
    "怡": "y",
    "恺": "k",
    "憬": "j",
    "懋": "m",
    "攸": "y",
    "昀": "y",
    "昊": "h",
    "昕": "x",
    "昱": "y",
    "晁": "c",
    "晏": "y",
    "晖": "h",
    "晟": "c",
    "曦": "x",
    "朐": "q",
    "枞": "c",
    "柘": "z",
    "栾": "l",
    "梓": "z",
    "楠": "n",
    "楹": "y",
    "榈": "l",
    "榕": "r",
    "毂": "g",
    "毓": "y",
    "汴": "b",
    "汶": "w",
    "沐": "m",
    "沩": "w",
    "泓": "h",
    "泗": "s",
    "泸": "l",
    "泾": "j",
    "浏": "l",
    "浔": "x",
    "淅": "x",
    "淼": "m",
    "渌": "l",
    "渚": "z",
    "渥": "w",
    "湔": "j",
    "溧": "l",
    "滕": "t",
    "濂": "l",
    "濉": "s",
    "濠": "h",
    "濮": "p",
    "瀚": "h",
    "瀛": "y",
    "灏": "h",
    "灞": "b",
    "炀": "y",
    "炜": "w",
    "烨": "y",
    "焱": "y",
    "煊": "x",
    "煜": "y",
    "熠": "y",
    "熵": "s",
    "熹": "x",
    "獐": "z",
    "玑": "j",
    "玮": "w",
    "珀": "p",
    "珂": "k",
    "珈": "j",
    "珑": "l",
    "珩": "h",
    "琏": "l",
    "琚": "j",
    "琛": "c",
    "琪": "q",
    "瑜": "y",
    "璞": "p",
    "璧": "b",
    "瓯": "o",
    "甬": "y",
    "癀": "h",
    "皓": "h",
    "盱": "x",
    "睢": "s",
    "睿": "r",
    "砀": "d",
    "砻": "l",
    "碚": "b",
    "祯": "z",
    "祺": "q",
    "禅": "c",
    "禧": "x",
    "禺": "y",
    "秭": "z",
    "綦": "q",
    "纾": "s",
    "缙": "j",
    "缤": "b",
    "罡": "g",
    "翎": "l",
    "聆": "l",
    "胤": "y",
    "膦": "l",
    "芸": "y",
    "茗": "m",
    "荃": "q",
    "荟": "h",
    "荻": "d",
    "莞": "g",
    "萃": "c",
    "萱": "x",
    "葆": "b",
    "蒽": "e",
    "薇": "w",
    "蘅": "h",
    "蜓": "t",
    "蜻": "q",
    "螂": "l",
    "螳": "t",
    "蟒": "m",
    "蟠": "p",
    "蠡": "l",
    "衢": "q",
    "賨": "c",
    "迦": "j",
    "邕": "y",
    "邗": "h",
    "邛": "q",
    "邡": "f",
    "邺": "y",
    "郏": "j",
    "郓": "y",
    "鄱": "p",
    "酯": "z",
    "醴": "l",
    "鑫": "x",
    "钛": "t",
    "钜": "j",
    "钰": "y",
    "钴": "g",
    "钼": "m",
    "钽": "t",
    "铖": "c",
    "锂": "l",
    "锆": "g",
    "锝": "d",
    "锴": "k",
    "韬": "t",
    "颀": "q",
    "颍": "y",
    "馨": "x",
    "驿": "y",
    "骐": "q",
    "骼": "g",
    "魅": "m",
    "鲲": "k",
    "鳌": "a",
    "鹄": "g",
    "鹞": "y",
    "鹭": "l",
    "麒": "q",
    "麟": "l",
    "麾": "h",
    "黉": "h",
    "黏": "n",
    "黛": "d"
}

def clean_stock_name(name: str) -> str:
    """去除 XD/XR/DR 等除权除息前缀"""
    upper_name = name.upper()
    if upper_name.startswith("XD") or upper_name.startswith("XR") or upper_name.startswith("DR"):
        return name[2:]
    return name

def get_pinyin_initials(name: str) -> str:
    """获取中文名称的拼音首字母"""
    name = clean_stock_name(name)
    initials = []
    for char in name:
        if char in LEVEL2_CHAR_MAP:
            initials.append(LEVEL2_CHAR_MAP[char])
            continue
        if 'a' <= char.lower() <= 'z' or '0' <= char <= '9':
            initials.append(char.lower())
            continue
        try:
            gbk_bytes = char.encode('gbk')
        except Exception:
            continue
        if len(gbk_bytes) != 2:
            continue
        code = (gbk_bytes[0] << 8) + gbk_bytes[1]
        
        if 0xB0A1 <= code <= 0xB0C4: initials.append('a')
        elif 0xB0C5 <= code <= 0xB2C0: initials.append('b')
        elif 0xB2C1 <= code <= 0xB4ED: initials.append('c')
        elif 0xB4EE <= code <= 0xB6E9: initials.append('d')
        elif 0xB6EA <= code <= 0xB7A1: initials.append('e')
        elif 0xB7A2 <= code <= 0xB8C0: initials.append('f')
        elif 0xB8C1 <= code <= 0xB9FD: initials.append('g')
        elif 0xB9FE <= code <= 0xBBF6: initials.append('h')
        elif 0xBBF7 <= code <= 0xBFA5: initials.append('j')
        elif 0xBFA6 <= code <= 0xC0AB: initials.append('k')
        elif 0xC0AC <= code <= 0xC2E7: initials.append('l')
        elif 0xC2E8 <= code <= 0xC4C2: initials.append('m')
        elif 0xC4C3 <= code <= 0xC5B5: initials.append('n')
        elif 0xC5B6 <= code <= 0xC5BD: initials.append('o')
        elif 0xC5BE <= code <= 0xC6D9: initials.append('p')
        elif 0xC6DA <= code <= 0xC8BA: initials.append('q')
        elif 0xC8BB <= code <= 0xC8F5: initials.append('r')
        elif 0xC8F6 <= code <= 0xCBF9: initials.append('s')
        elif 0xCBFA <= code <= 0xCDD9: initials.append('t')
        elif 0xCDDA <= code <= 0xCEF3: initials.append('w')
        elif 0xCEF4 <= code <= 0xD1B8: initials.append('x')
        elif 0xD1B9 <= code <= 0xD4D0: initials.append('y')
        elif 0xD4D1 <= code <= 0xF7FE: initials.append('z')
    return "".join(initials)

def get_stock_names_with_initials():
    global _STOCK_NAMES_CACHE
    if _STOCK_NAMES_CACHE:
        return _STOCK_NAMES_CACHE
    
    tdx_dir = load_tdx_dir()
    names_map = load_stock_names(tdx_dir)
    
    cache = []
    for sym, name in names_map.items():
        initials = get_pinyin_initials(name)
        initials_alt = None
        
        # 多音字特判优化
        if "行" in name:
            initials_alt = initials.replace('x', 'h')
        elif "重" in name:
            initials_alt = initials.replace('z', 'c')
        elif "长" in name:
            initials_alt = initials.replace('z', 'c')
        elif "厦" in name:
            initials_alt = initials.replace('s', 'x')
            
        cache.append({
            "symbol": sym,
            "code": sym[2:],
            "name": name,
            "initials": initials,
            "initials_alt": initials_alt
        })
    _STOCK_NAMES_CACHE = cache
    return cache

def load_strategies() -> dict:
    """动态载入 strategies.json 并自动做 Windows 路径正则兼容"""
    if not os.path.exists(STRATEGIES_PATH):
        return {}
    try:
        with open(STRATEGIES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            # 自动进行跨平台/Windows路径正则兼容替换
            for key, val in data.items():
                if "query_sql" in val:
                    val["query_sql"] = val["query_sql"].replace("([^/]+)", "([^/\\\\\\\\\\\\]+)")
            return data
    except Exception:
        return {}

def process_flow_data(df, name_col) -> dict:
    """处理板块资金流向 DataFrame，提取最新交易日 TOP 10 和 BOTTOM 10 的 30 日时序数据"""
    if df.empty:
        return {"dates": [], "top_10": [], "bottom_10": [], "series": []}
    
    df['date_str'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d') if (pd.notnull(x) and hasattr(x, 'strftime')) else (str(x)[:10] if pd.notnull(x) else ""))
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
            encoding='utf-8',
            errors='replace',
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

@app.get("/api/stocks/search", summary="股票名称/代码/拼音首字母智能联想推荐")
def search_stocks(q: str = ""):
    q = q.strip().lower()
    if not q:
        return JSONResponse(content={"status": "success", "results": []})
        
    try:
        stocks = get_stock_names_with_initials()
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": f"加载股票列表失败: {e}"})
        
    results = []
    for s in stocks:
        match = False
        if s["code"].startswith(q) or s["symbol"].startswith(q):
            match = True
        elif q in s["name"].lower():
            match = True
        elif s["initials"].startswith(q) or (s["initials_alt"] and s["initials_alt"].startswith(q)):
            match = True
        elif (len(s["initials"]) >= 3 and q.startswith(s["initials"]) and len(q) - len(s["initials"]) <= 1) or \
             (s["initials_alt"] and len(s["initials_alt"]) >= 3 and q.startswith(s["initials_alt"]) and len(q) - len(s["initials_alt"]) <= 1):
            match = True
            
        if match:
            results.append({
                "symbol": s["symbol"].upper(),
                "name": s["name"],
                "pinyin": s["initials"]
            })
            if len(results) >= 15:
                break
                
    return JSONResponse(content={
        "status": "success",
        "count": len(results),
        "results": results
    })

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

        # 8. 主要指数及全市场30日成交额时序数据
        sql_turnover = f"""
        WITH latest_dates AS (
            SELECT DISTINCT date 
            FROM read_parquet('{DATA_STORE_DIR}/sh000001.parquet')
            ORDER BY date DESC
            LIMIT 30
        ),
        raw_data AS (
            SELECT 
                date,
                regexp_extract(filename, '([^/\\\\\\\\]+)[.]parquet$', 1) AS symbol,
                amount
            FROM read_parquet([{patterns_str}], filename=true)
            WHERE date IN (SELECT date FROM latest_dates)
        )
        SELECT 
            date,
            SUM(CASE WHEN symbol LIKE 'sh60%' OR symbol LIKE 'sh68%' THEN amount ELSE 0 END) AS sh_amount,
            SUM(CASE WHEN symbol LIKE 'sz00%' OR symbol LIKE 'sz30%' THEN amount ELSE 0 END) AS sz_amount,
            SUM(CASE WHEN symbol LIKE 'sz30%' THEN amount ELSE 0 END) AS cyb_amount,
            SUM(CASE WHEN symbol LIKE 'bj%' THEN amount ELSE 0 END) AS bj_amount,
            SUM(CASE WHEN symbol LIKE 'sh68%' THEN amount ELSE 0 END) AS kcb_amount,
            SUM(CASE WHEN symbol LIKE 'sh60%' OR symbol LIKE 'sh68%' OR symbol LIKE 'sz00%' OR symbol LIKE 'sz30%' OR symbol LIKE 'bj%' THEN amount ELSE 0 END) AS all_amount
        FROM raw_data
        GROUP BY date
        ORDER BY date ASC
        """
        df_turnover = con.execute(sql_turnover).fetchdf()
        
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": f"DuckDB 核心SQL执行失败: {e}"})

    # 数据组装与整理
    row_temp = df_temp.iloc[0]
    total_stocks = int(row_temp['total_stocks'])
    limit_up = int(row_temp['limit_up'])
    limit_down = int(row_temp['limit_down'])
    median_return = float(row_temp['median_return'])
    trade_date = row_temp['trade_date']
    if pd.notnull(trade_date) and hasattr(trade_date, 'strftime'):
        trade_date_str = trade_date.strftime('%Y-%m-%d')
    else:
        s = str(trade_date)
        trade_date_str = f"{s[:4]}-{s[4:6]}-{s[6:8]}" if (pd.notnull(trade_date) and len(s) == 8) else str(trade_date)

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

    # 格式化30日主要指数及全市场成交额
    df_turnover['date_str'] = df_turnover['date'].apply(lambda x: x.strftime('%Y-%m-%d') if (pd.notnull(x) and hasattr(x, 'strftime')) else str(x)[:10])
    market_turnover = {
        "dates": df_turnover['date_str'].tolist(),
        "sh": (df_turnover['sh_amount'] / 1e8).round(2).tolist(),
        "sz": (df_turnover['sz_amount'] / 1e8).round(2).tolist(),
        "cyb": (df_turnover['cyb_amount'] / 1e8).round(2).tolist(),
        "bj": (df_turnover['bj_amount'] / 1e8).round(2).tolist(),
        "kcb": (df_turnover['kcb_amount'] / 1e8).round(2).tolist(),
        "all": (df_turnover['all_amount'] / 1e8).round(2).tolist()
    }

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
        "market_turnover": market_turnover,
        "compute_time_seconds": round(time.perf_counter() - t_start, 2)
    }

    # 写入缓存
    _MARKET_CACHE["data"] = full_data
    _MARKET_CACHE["signature"] = current_sig
    
    return full_data

def calculate_slopes_for_symbols(con, symbols: list[str], patterns_str: str, data_store_dir: str) -> dict:
    """极速为选中的股票计算5日MA20与MA30的百分比变动斜率"""
    if not symbols:
        return {}
    
    # 格式化为 SQL IN 表达式需要的 lowercase 列表
    symbols_lower = [s.lower() for s in symbols]
    symbols_str = ", ".join([f"'{s}'" for s in symbols_lower])
    
    sql = f"""
    WITH raw_data AS (
        SELECT 
            regexp_extract(filename, '([^/\\\\\\\\]+)[.]parquet$', 1) AS symbol,
            date,
            close_adj
        FROM read_parquet([{patterns_str}], filename=true)
        WHERE regexp_extract(filename, '([^/\\\\\\\\]+)[.]parquet$', 1) IN ({symbols_str})
    ),
    calculated AS (
        SELECT 
            symbol,
            date,
            close_adj,
            AVG(close_adj) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
            AVG(close_adj) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS ma30,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
        FROM raw_data
    ),
    latest_data AS (
        SELECT 
            symbol,
            ma20 AS ma20_today,
            ma30 AS ma30_today
        FROM calculated
        WHERE rn = 1
    ),
    prev_data AS (
        SELECT 
            symbol,
            ma20 AS ma20_prev,
            ma30 AS ma30_prev
        FROM calculated
        WHERE rn = 6
    )
    SELECT 
        l.symbol,
        (l.ma20_today - coalesce(p.ma20_prev, l.ma20_today)) / coalesce(p.ma20_prev, l.ma20_today) * 100 AS slope_ma20,
        (l.ma30_today - coalesce(p.ma30_prev, l.ma30_today)) / coalesce(p.ma30_prev, l.ma30_today) * 100 AS slope_ma30
    FROM latest_data l
    LEFT JOIN prev_data p ON l.symbol = p.symbol
    """
    try:
        df = con.execute(sql).fetchdf()
        slopes_map = {}
        for _, r in df.iterrows():
            slopes_map[r['symbol'].lower()] = {
                "slope_ma20": round(float(r['slope_ma20']), 2) if pd.notnull(r['slope_ma20']) else 0.0,
                "slope_ma30": round(float(r['slope_ma30']), 2) if pd.notnull(r['slope_ma30']) else 0.0
            }
        return slopes_map
    except Exception as e:
        print(f"⚠️ 计算斜率失败: {e}")
        return {}

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
    
    # 极速计算选中股票的 MA20/MA30 变动斜率 (5日变化百分比)
    selected_symbols = res_df['symbol'].tolist()
    slopes_map = calculate_slopes_for_symbols(con, selected_symbols, patterns_str, DATA_STORE_DIR)
    
    stocks_list = []
    for _, row in res_df.iterrows():
        sym_lower = row['symbol'].lower()
        slopes = slopes_map.get(sym_lower, {"slope_ma20": 0.0, "slope_ma30": 0.0})
        stocks_list.append({
            "symbol": row['Symbol'],
            "name": row['Name'],
            "close": row['Close_Formatted'],
            "vol_ratio": row['Vol_Ratio_Formatted'],
            "dev_ma20_pct": row['Dev_MA20_Pct_Formatted'],
            "slope_ma20": slopes["slope_ma20"],
            "slope_ma30": slopes["slope_ma30"],
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
        mdate = latest_date_df['mdate'].iloc[0]
        if pd.notnull(mdate):
            latest_date_str = mdate.strftime('%Y-%m-%d')
        else:
            latest_date_str = datetime.now().strftime('%Y-%m-%d')
    except Exception:
        latest_date_str = datetime.now().strftime('%Y-%m-%d')

    date_suffix = latest_date_str.replace("-", "")
    report_dir = os.path.join(CURRENT_DIR, "report")
    os.makedirs(report_dir, exist_ok=True)

    # 极速计算选中股票的 MA20/MA30 变动斜率 (5日变化百分比)
    selected_symbols = res_df['symbol'].tolist() if res_df is not None and not res_df.empty else []
    slopes_map = calculate_slopes_for_symbols(con, selected_symbols, patterns_str, DATA_STORE_DIR)

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

| 序号 | 股票代码 | 股票名称 | 最新收盘价 | 今日放量倍数 | MA20 偏离度 | MA20斜率(5日) | MA30斜率(5日) | 触发共振爆发板块（突破只数/板块总股数占比） |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
"""
        if res_df is not None and not res_df.empty:
            for idx, row in res_df.reset_index(drop=True).iterrows():
                sym_lower = row['symbol'].lower()
                slopes = slopes_map.get(sym_lower, {"slope_ma20": 0.0, "slope_ma30": 0.0})
                s_ma20 = f"+{slopes['slope_ma20']}%" if slopes['slope_ma20'] > 0 else f"{slopes['slope_ma20']}%"
                s_ma30 = f"+{slopes['slope_ma30']}%" if slopes['slope_ma30'] > 0 else f"{slopes['slope_ma30']}%"
                dev_val = row['Dev_MA20_Pct_Formatted']
                dev_sign = "+" if dev_val > 0 else ""
                md_content += f"| {idx+1} | `{row['Symbol']}` | {row['Name']} | {row['Close_Formatted']} | {row['Vol_Ratio_Formatted']} | {dev_sign}{dev_val}% | {s_ma20} | {s_ma30} | {row['Resonance_Sectors']} |\n"
        else:
            md_content += "| -- | -- | -- | -- | -- | -- | -- | -- | 暂无筛选结果 |\n"
            
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

| 序号 | 股票代码 | 股票名称 | 最新收盘价 | 今日放量倍数 | MA20 偏离度 | MA20斜率(5日) | MA30斜率(5日) | 综合触发共振板块（突破只数/占比） |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
"""
        if res_df is not None and not res_df.empty:
            for idx, row in res_df.reset_index(drop=True).iterrows():
                sym_lower = row['symbol'].lower()
                slopes = slopes_map.get(sym_lower, {"slope_ma20": 0.0, "slope_ma30": 0.0})
                s_ma20 = f"+{slopes['slope_ma20']}%" if slopes['slope_ma20'] > 0 else f"{slopes['slope_ma20']}%"
                s_ma30 = f"+{slopes['slope_ma30']}%" if slopes['slope_ma30'] > 0 else f"{slopes['slope_ma30']}%"
                dev_val = row['Dev_MA20_Pct_Formatted']
                dev_sign = "+" if dev_val > 0 else ""
                md_content += f"| {idx+1} | `{row['Symbol']}` | {row['Name']} | {row['Close_Formatted']} | {row['Vol_Ratio_Formatted']} | {dev_sign}{dev_val}% | {s_ma20} | {s_ma30} | {row['Merged_Sectors']} |\n"
        else:
            md_content += "| -- | -- | -- | -- | -- | -- | -- | -- | 暂无筛选结果 |\n"
            
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

@app.get("/api/stock/analyze", summary="对指定股票进行多维量化特征提取与规律统计回测")
def analyze_single_stock(symbol: str):
    if not symbol or len(symbol.strip()) == 0:
        return JSONResponse(status_code=400, content={"status": "error", "message": "股票代码不能为空！"})
        
    symbol = symbol.strip().lower()
    tdx_dir = load_tdx_dir()
    names_map = load_stock_names(tdx_dir)
    
    # 自动解析股票代码 (支持带前缀如 sh600000, 或是 6 位纯数字代码 600000)
    resolved_symbol = None
    if re.match(r'^\d{6}$', symbol):
        code_only = symbol
        symbol_full = [s for s in names_map.keys() if s.endswith(code_only)]
        if symbol_full:
            resolved_symbol = symbol_full[0]
        else:
            # 尝试在数据目录寻找匹配文件
            files = os.listdir(DATA_STORE_DIR)
            matched_files = [f for f in files if f.endswith('.parquet') and f.startswith(('sh', 'sz', 'bj')) and f[2:8] == code_only]
            if matched_files:
                resolved_symbol = matched_files[0].replace('.parquet', '')
            else:
                # 默认补齐规则
                if code_only.startswith(('60', '68')):
                    resolved_symbol = f"sh{code_only}"
                elif code_only.startswith(('00', '30')):
                    resolved_symbol = f"sz{code_only}"
                else:
                    resolved_symbol = f"bj{code_only}"
    else:
        resolved_symbol = symbol
        
    pq_path = os.path.join(DATA_STORE_DIR, f"{resolved_symbol}.parquet")
    if not os.path.exists(pq_path):
        return JSONResponse(status_code=404, content={"status": "error", "message": f"未找到该股票数据！请先确认股票代码，或执行数据同步以创建数据池。"})
        
    con = duckdb.connect()
    
    # 1. 股票基础信息
    stock_name = names_map.get(resolved_symbol, "未知个股")
    code_only = resolved_symbol[-6:]
    
    # 查询所属概念与行业
    concepts = []
    industries = []
    try:
        sql_c = f"SELECT block_name FROM read_parquet('{BLOCK_MAPPINGS_PATH}') WHERE code = '{code_only}'"
        concepts = con.execute(sql_c).fetchdf()['block_name'].tolist()
        
        symbol_pattern = f"%{code_only}"
        sql_i = f"SELECT industry_name FROM read_parquet('{INDUSTRY_MAPPINGS_PATH}') WHERE symbol LIKE '{symbol_pattern}'"
        industries = con.execute(sql_i).fetchdf()['industry_name'].tolist()
    except Exception:
        pass
        
    # 2. 提取特征数据 (加载最近 260 天数据计算特征)
    try:
        sql_features = f"""
        WITH raw_data AS (
            SELECT date, open_adj, high_adj, low_adj, close_adj, volume, amount
            FROM read_parquet('{pq_path}')
            ORDER BY date DESC
            LIMIT 260
        ),
        raw_with_return AS (
            SELECT 
                date,
                open_adj,
                high_adj,
                low_adj,
                close_adj,
                volume,
                amount,
                (close_adj - LAG(close_adj) OVER (ORDER BY date ASC)) / LAG(close_adj) OVER (ORDER BY date ASC) * 100 AS pct_change
            FROM raw_data
        ),
        features AS (
            SELECT 
                date,
                close_adj,
                volume,
                amount,
                pct_change,
                AVG(close_adj) OVER (ORDER BY date ASC ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS ma5,
                AVG(close_adj) OVER (ORDER BY date ASC ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma10,
                AVG(close_adj) OVER (ORDER BY date ASC ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
                AVG(close_adj) OVER (ORDER BY date ASC ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS ma30,
                AVG(close_adj) OVER (ORDER BY date ASC ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60,
                AVG(close_adj) OVER (ORDER BY date ASC ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS ma120,
                AVG(close_adj) OVER (ORDER BY date ASC ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) AS ma250,
                AVG(volume) OVER (ORDER BY date ASC ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS vol_ma5,
                AVG(volume) OVER (ORDER BY date ASC ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vol_ma20,
                STDDEV(pct_change) OVER (ORDER BY date ASC ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vola_20d,
                MAX(close_adj) OVER (ORDER BY date ASC ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) AS max_high_250,
                MIN(close_adj) OVER (ORDER BY date ASC ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) AS min_low_250
            FROM raw_with_return
        )
        SELECT * FROM features ORDER BY date DESC LIMIT 100
        """
        df_feat = con.execute(sql_features).fetchdf()
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": f"加载特征数据失败: {e}"})
        
    if df_feat.empty:
        return JSONResponse(status_code=404, content={"status": "error", "message": "该股票数据行数过少，无法进行特征计算！"})
        
    # 最新的一行作为当前特征
    latest = df_feat.iloc[0]
    
    # 提取多维量化特征
    # 格式化日期列表和收盘价/均线历史，用于 K 线时序图 (取 90 天)
    chart_df = df_feat.head(90).iloc[::-1] # 转为升序
    chart_df['date_str'] = chart_df['date'].apply(lambda x: x.strftime('%Y-%m-%d') if (pd.notnull(x) and hasattr(x, 'strftime')) else str(x)[:10])
    
    price_feat = {
        "close": round(float(latest['close_adj']), 2),
        "pct_change": round(float(latest['pct_change']), 2) if pd.notnull(latest['pct_change']) else 0.0,
        "high_250d": round(float(latest['max_high_250']), 2),
        "low_250d": round(float(latest['min_low_250']), 2),
        "dist_high_pct": round(float((latest['max_high_250'] - latest['close_adj']) / latest['max_high_250'] * 100), 2)
    }
    
    ma_feat = {
        "ma5": round(float(latest['ma5']), 2),
        "ma10": round(float(latest['ma10']), 2),
        "ma20": round(float(latest['ma20']), 2),
        "ma30": round(float(latest['ma30']), 2),
        "ma60": round(float(latest['ma60']), 2) if pd.notnull(latest['ma60']) else 0.0,
        "ma120": round(float(latest['ma120']), 2) if pd.notnull(latest['ma120']) else 0.0,
        "ma250": round(float(latest['ma250']), 2) if pd.notnull(latest['ma250']) else 0.0,
        "dev_ma20": round(float((latest['close_adj'] - latest['ma20']) / latest['ma20'] * 100), 2)
    }
    
    # 估算 slopes (最近5天MA20/MA30变化率)
    prev_5d = df_feat.iloc[5] if len(df_feat) > 5 else latest
    ma_feat["slope_ma20"] = round(float((latest['ma20'] - prev_5d['ma20']) / prev_5d['ma20'] * 100), 2)
    ma_feat["slope_ma30"] = round(float((latest['ma30'] - prev_5d['ma30']) / prev_5d['ma30'] * 100), 2)
    
    vol_feat = {
        "volume": round(float(latest['volume']), 2),
        "amount_billions": round(float(latest['amount'] / 1e8), 2),
        "vol_ratio_5d": round(float(latest['volume'] / latest['vol_ma5']), 2) if latest['vol_ma5'] > 0 else 1.0,
        "vol_ratio_20d": round(float(latest['volume'] / latest['vol_ma20']), 2) if latest['vol_ma20'] > 0 else 1.0
    }
    
    # 年化对数收益率波动率
    volatility_20d = float(latest['vola_20d']) * (250 ** 0.5) if pd.notnull(latest['vola_20d']) else 0.0
    vola_feat = {
        "volatility_20d": round(volatility_20d, 2),
        "risk_level": "极高波动" if volatility_20d > 45 else ("高波动" if volatility_20d > 30 else ("中等波动" if volatility_20d > 18 else "低波动"))
    }
    
    # 大势环境关联 (拉取最近大盘情绪)
    market_temp = 50.0
    try:
        if _MARKET_CACHE["data"] is not None:
            rising = _MARKET_CACHE["data"].get("rising_count", 2500)
            falling = _MARKET_CACHE["data"].get("falling_count", 2500)
            market_temp = round(rising * 100.0 / (rising + falling), 1) if (rising + falling) > 0 else 50.0
    except Exception:
        pass
        
    market_feat = {
        "market_temp": market_temp,
        "market_env": "多头共振" if market_temp > 65 else ("空头防守" if market_temp < 35 else "震荡平衡")
    }

    # 3. 统计 4 种量化规律（大样本历史回测仿真，加载全量历史数据计算未来 5 日收益）
    try:
        sql_backtest = f"""
        WITH full_history AS (
            SELECT date, open_adj, high_adj, low_adj, close_adj, volume, amount
            FROM read_parquet('{pq_path}')
            ORDER BY date ASC
        ),
        history_with_return AS (
            SELECT 
                date,
                open_adj,
                high_adj,
                low_adj,
                close_adj,
                volume,
                amount,
                (close_adj - LAG(close_adj) OVER (ORDER BY date)) / LAG(close_adj) OVER (ORDER BY date) * 100 AS daily_return,
                (LEAD(close_adj, 5) OVER (ORDER BY date) - close_adj) / close_adj * 100 AS fwd_5d_return
            FROM full_history
        ),
        backtest_factors AS (
            SELECT 
                date,
                close_adj,
                low_adj,
                volume,
                daily_return,
                ma20,
                vol_ma5,
                vol_ma20,
                max_return_recent_15d,
                fwd_5d_return
            FROM (
                SELECT 
                    date,
                    close_adj,
                    low_adj,
                    volume,
                    daily_return,
                    AVG(close_adj) OVER (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
                    AVG(volume) OVER (ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS vol_ma5,
                    AVG(volume) OVER (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vol_ma20,
                    MAX(daily_return) OVER (ORDER BY date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS max_return_recent_15d,
                    fwd_5d_return
                FROM history_with_return
            )
            WHERE ma20 IS NOT NULL AND vol_ma5 IS NOT NULL AND vol_ma20 IS NOT NULL AND fwd_5d_return IS NOT NULL
        )
        SELECT
            -- 1. 放量突破规律统计
            COUNT(CASE WHEN close_adj > ma20 AND volume > 1.5 * vol_ma5 THEN 1 END) AS pb_count,
            COUNT(CASE WHEN close_adj > ma20 AND volume > 1.5 * vol_ma5 AND fwd_5d_return > 0 THEN 1 END) AS pb_win,
            AVG(CASE WHEN close_adj > ma20 AND volume > 1.5 * vol_ma5 THEN fwd_5d_return END) AS pb_ret,
            
            -- 2. 均线支撑规律统计
            COUNT(CASE WHEN low_adj <= ma20 AND close_adj >= ma20 THEN 1 END) AS sup_count,
            COUNT(CASE WHEN low_adj <= ma20 AND close_adj >= ma20 AND fwd_5d_return > 0 THEN 1 END) AS sup_win,
            AVG(CASE WHEN low_adj <= ma20 AND close_adj >= ma20 THEN fwd_5d_return END) AS sup_ret,
            
            -- 3. 超跌反弹规律统计
            COUNT(CASE WHEN (close_adj - ma20) / ma20 < -0.12 AND daily_return >= 3.0 THEN 1 END) AS rev_count,
            COUNT(CASE WHEN (close_adj - ma20) / ma20 < -0.12 AND daily_return >= 3.0 AND fwd_5d_return > 0 THEN 1 END) AS rev_win,
            AVG(CASE WHEN (close_adj - ma20) / ma20 < -0.12 AND daily_return >= 3.0 THEN fwd_5d_return END) AS rev_ret,
            
            -- 4. 缩量洗盘规律统计
            COUNT(CASE WHEN max_return_recent_15d >= 7.0 AND volume < 0.65 * vol_ma20 THEN 1 END) AS dry_count,
            COUNT(CASE WHEN max_return_recent_15d >= 7.0 AND volume < 0.65 * vol_ma20 AND fwd_5d_return > 0 THEN 1 END) AS dry_win,
            AVG(CASE WHEN max_return_recent_15d >= 7.0 AND volume < 0.65 * vol_ma20 THEN fwd_5d_return END) AS dry_ret
        FROM backtest_factors
        """
        df_bt = con.execute(sql_backtest).fetchdf()
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": f"规律模拟回测失败: {e}"})
        
    bt_row = df_bt.iloc[0]
    
    # 整理4个规律统计
    def get_stats(cnt_key, win_key, ret_key, name):
        cnt = int(bt_row[cnt_key]) if pd.notnull(bt_row[cnt_key]) else 0
        win = int(bt_row[win_key]) if pd.notnull(bt_row[win_key]) else 0
        win_rate = round(win * 100.0 / cnt, 1) if cnt > 0 else 50.0
        avg_ret = round(float(bt_row[ret_key]), 2) if cnt > 0 and pd.notnull(bt_row[ret_key]) else 0.0
        return {"name": name, "count": cnt, "win_rate": win_rate, "avg_return": avg_ret}
        
    patterns_list = [
        get_stats("pb_count", "pb_win", "pb_ret", "放量突破规律"),
        get_stats("sup_count", "sup_win", "sup_ret", "生命均线支撑规律"),
        get_stats("rev_count", "rev_win", "rev_ret", "超跌极值反弹规律"),
        get_stats("dry_count", "dry_win", "dry_ret", "缩量洗盘突破规律")
    ]
    
    # 4. 未来走势期望概率推演 (取过去所有交易日 5 天未来收益的真实大样本概率)
    try:
        sql_pred = f"""
        WITH full_history AS (
            SELECT date, close_adj
            FROM read_parquet('{pq_path}')
            ORDER BY date ASC
        ),
        fwd_returns AS (
            SELECT 
                (LEAD(close_adj, 5) OVER (ORDER BY date) - close_adj) / close_adj * 100 AS fwd_5d_return
            FROM full_history
        )
        SELECT 
            COUNT(*) AS total_samples,
            COUNT(CASE WHEN fwd_5d_return > 0 THEN 1 END) AS win_samples,
            AVG(CASE WHEN fwd_5d_return > 0 THEN fwd_5d_return END) AS avg_gain,
            AVG(CASE WHEN fwd_5d_return < 0 THEN fwd_5d_return END) AS avg_loss
        FROM fwd_returns
        WHERE fwd_5d_return IS NOT NULL
        """
        df_pred = con.execute(sql_pred).fetchdf()
    except Exception:
        df_pred = pd.DataFrame()
        
    win_rate = 52.4
    expected_return = 1.85
    risk_reward_ratio = 1.35
    
    if not df_pred.empty:
        pred_row = df_pred.iloc[0]
        total_s = int(pred_row['total_samples']) if pd.notnull(pred_row['total_samples']) else 0
        if total_s > 100:
            win_s = int(pred_row['win_samples'])
            win_rate = round(win_s * 100.0 / total_s, 1)
            
            avg_gain = float(pred_row['avg_gain']) if pd.notnull(pred_row['avg_gain']) else 2.5
            avg_loss = abs(float(pred_row['avg_loss'])) if pd.notnull(pred_row['avg_loss']) else 2.0
            
            risk_reward_ratio = round(avg_gain / avg_loss, 2) if avg_loss > 0 else 1.5
            
            # 结合当前斜率与大盘温度微调
            slope_mod = (ma_feat["slope_ma20"] + ma_feat["slope_ma30"]) / 2.0
            temp_mod = (market_temp - 50.0) / 10.0
            win_rate = min(92.0, max(18.0, round(win_rate + slope_mod + temp_mod, 1)))
            
            expected_return = round((win_rate/100.0 * avg_gain) - ((1 - win_rate/100.0) * avg_loss), 2)
            
    # 5. 智能诊股建议
    if win_rate >= 62.0 and risk_reward_ratio >= 1.5:
        suggestion = "🌟 强力多头共振蓄势！主力吸筹迹象极为显著，短期5日上涨期望巨大，建议积极逢低分批建仓买入。"
        suggestion_color = "cyber-up"
    elif win_rate >= 54.0 and ma_feat["dev_ma20"] > 0:
        suggestion = "📈 趋势震荡偏多。均线形态多头排列保持良好，量能温和，建议底仓持有，关注前高阻力位。"
        suggestion_color = "cyber-primary"
    elif win_rate >= 45.0 and vol_feat["vol_ratio_5d"] < 0.7:
        suggestion = "洗盘调整中。量能快速缩减显示浮筹清洗充分，主力惜售，建议观望等待地量确认后再次大阳线突破。"
        suggestion_color = "cyber-textMuted"
    elif win_rate < 45.0 and ma_feat["dev_ma20"] < -8.0:
        suggestion = "⚠️ 极值超跌状态。短期价格严重偏离生命线，虽然存在技术性反弹动能，但上方抛压沉重，建议控制仓位，不宜盲目超短线抄底。"
        suggestion_color = "cyber-accent"
    else:
        suggestion = "🛑 空头防守减仓信号！跌破关键生命均线，均线斜率开始转下，建议以避险防守为主，跌破支撑位坚决减仓。"
        suggestion_color = "cyber-down"
        
    # 6. 蒙特卡洛未来 10 日走势随机模拟 (Geometric Brownian Motion)
    import numpy as np
    
    # 提取最近 60 交易日对数收益率估计漂移与波动率
    returns_60d = df_feat.head(60)['pct_change'].dropna() / 100.0
    mu = float(returns_60d.mean()) if not returns_60d.empty else 0.0005
    sigma = float(returns_60d.std()) if not returns_60d.empty else 0.02
    
    if pd.isna(mu): mu = 0.0005
    if pd.isna(sigma) or sigma <= 0: sigma = 0.02
    
    S0 = float(latest['close_adj'])
    N = 500  # 模拟路径数
    T = 10   # 预测交易日天数
    
    # 路径数组预分配，shape = (500, 11)，第0天为当前收盘价
    mc_paths = np.zeros((N, T + 1))
    mc_paths[:, 0] = S0
    
    # 漂移项与随机模拟
    drift = mu - 0.5 * (sigma ** 2)
    for t in range(1, T + 1):
        Z = np.random.normal(0, 1, N)
        mc_paths[:, t] = mc_paths[:, t - 1] * np.exp(drift + sigma * Z)
        
    # 计算每日的分位数
    p5 = np.percentile(mc_paths, 5, axis=0).round(2).tolist()
    p16 = np.percentile(mc_paths, 16, axis=0).round(2).tolist()
    p50 = np.percentile(mc_paths, 50, axis=0).round(2).tolist()
    p84 = np.percentile(mc_paths, 84, axis=0).round(2).tolist()
    p95 = np.percentile(mc_paths, 95, axis=0).round(2).tolist()
    
    # 随机抽取 3 条轨迹展示
    sample_indices = np.random.choice(N, 3, replace=False)
    samples = [mc_paths[idx, :].round(2).tolist() for idx in sample_indices]
    
    # 生成未来 10 个工作日坐标 (跳过周六周日)
    latest_date = pd.to_datetime(latest['date'])
    future_dates = []
    curr = latest_date
    while len(future_dates) < T:
        curr += pd.Timedelta(days=1)
        if curr.weekday() < 5:
            future_dates.append(curr.strftime('%Y-%m-%d'))
            
    latest_date_str = latest['date'].strftime('%Y-%m-%d') if (pd.notnull(latest['date']) and hasattr(latest['date'], 'strftime')) else str(latest['date'])[:10]
    mc_dates = [latest_date_str] + future_dates
    
    return {
        "status": "success",
        "symbol": resolved_symbol.upper(),
        "name": stock_name,
        "concepts": concepts,
        "industries": industries,
        "suggestions": {
            "text": suggestion,
            "color": suggestion_color
        },
        "features": {
            "price": price_feat,
            "ma": ma_feat,
            "volume": vol_feat,
            "volatility": vola_feat,
            "market": market_feat
        },
        "patterns": patterns_list,
        "predictions": {
            "win_rate": win_rate,
            "expected_return": expected_return,
            "risk_reward_ratio": risk_reward_ratio
        },
        "chart_data": {
            "dates": chart_df['date_str'].tolist(),
            "close": chart_df['close_adj'].round(2).tolist(),
            "ma20": chart_df['ma20'].round(2).tolist() if 'ma20' in chart_df.columns else [],
            "ma30": chart_df['ma30'].round(2).tolist() if 'ma30' in chart_df.columns else []
        },
        "monte_carlo": {
            "dates": mc_dates,
            "p95": p95,
            "p84": p84,
            "p50": p50,
            "p16": p16,
            "p5": p5,
            "samples": samples
        }
    }

# -------------------------------------------------------------
# 7.5. DTW 走势匹配与未来概率投影 API (GET)
# -------------------------------------------------------------
@app.get("/api/stock/pattern_match", summary="基于 DTW 算法匹配最相似的历史K线形态并投影未来走势")
def get_stock_pattern_match(symbol: str, window_size: int = 30, predict_size: int = 20):
    if not symbol or len(symbol.strip()) == 0:
        return JSONResponse(status_code=400, content={"status": "error", "message": "股票代码不能为空！"})
        
    symbol = symbol.strip().lower()
    tdx_dir = load_tdx_dir()
    names_map = load_stock_names(tdx_dir)
    
    # 自动解析股票代码
    resolved_symbol = None
    if re.match(r'^\d{6}$', symbol):
        code_only = symbol
        symbol_full = [s for s in names_map.keys() if s.endswith(code_only)]
        if symbol_full:
            resolved_symbol = symbol_full[0]
        else:
            files = os.listdir(DATA_STORE_DIR)
            matched_files = [f for f in files if f.endswith('.parquet') and f.startswith(('sh', 'sz', 'bj')) and f[2:8] == code_only]
            if matched_files:
                resolved_symbol = matched_files[0].replace('.parquet', '')
            else:
                if code_only.startswith(('60', '68')):
                    resolved_symbol = f"sh{code_only}"
                elif code_only.startswith(('00', '30')):
                    resolved_symbol = f"sz{code_only}"
                else:
                    resolved_symbol = f"bj{code_only}"
    else:
        resolved_symbol = symbol
        
    pq_path = os.path.join(DATA_STORE_DIR, f"{resolved_symbol}.parquet")
    if not os.path.exists(pq_path):
        return JSONResponse(status_code=404, content={"status": "error", "message": f"未找到该股票数据！"})
        
    import numpy as np
    
    # 定义带 Sakoe-Chiba 窗约束的 DTW 距离计算函数
    def dtw_distance(s1, s2, w=4):
        l1, l2 = len(s1), len(s2)
        w = max(w, abs(l1 - l2))
        dp = np.full((l1 + 1, l2 + 1), np.inf)
        dp[0, 0] = 0.0
        
        for i in range(1, l1 + 1):
            for j in range(max(1, i - w), min(l2 + 1, i + w + 1)):
                cost = abs(s1[i-1] - s2[j-1])
                dp[i, j] = cost + min(dp[i-1, j], dp[i, j-1], dp[i-1, j-1])
        return dp[l1, l2]

    con = duckdb.connect()
    
    # 1. 加载目标股票最近 window_size 日价格
    try:
        sql_target = f"""
        SELECT date, close_adj
        FROM read_parquet('{pq_path}')
        WHERE close_adj IS NOT NULL AND volume > 0
        ORDER BY date DESC
        LIMIT {window_size}
        """
        df_target = con.execute(sql_target).fetchdf()
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": f"加载目标股票数据失败: {e}"})
        
    if len(df_target) < window_size:
        return JSONResponse(status_code=400, content={"status": "error", "message": f"数据样本过少（当前仅有{len(df_target)}天），无法进行走势匹配！"})
        
    # 转为按时间升序
    df_target = df_target.iloc[::-1].reset_index(drop=True)
    target_dates = [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)[:10] for d in df_target['date']]
    target_prices = df_target['close_adj'].astype(float).tolist()
    
    # 归一化 Target
    min_t, max_t = min(target_prices), max(target_prices)
    range_t = max_t - min_t if max_t > min_t else 1.0
    target_norm = [(p - min_t) / range_t for p in target_prices]
    
    stock_name = names_map.get(resolved_symbol, "本股")
    
    # 2. 加载检索走势池
    # 2.1 加载本股所有历史走势
    try:
        sql_history = f"""
        SELECT date, close_adj
        FROM read_parquet('{pq_path}')
        WHERE close_adj IS NOT NULL AND volume > 0
        ORDER BY date ASC
        """
        df_history = con.execute(sql_history).fetchdf()
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": f"加载本股历史走势失败: {e}"})
        
    # 2.2 加载上证指数历史走势作为形态匹配对照池
    df_sh_history = pd.DataFrame()
    sh_pq_path = os.path.join(DATA_STORE_DIR, "sh000001.parquet")
    if os.path.exists(sh_pq_path) and resolved_symbol != "sh000001":
        try:
            sql_sh = f"""
            SELECT date, close_adj
            FROM read_parquet('{sh_pq_path}')
            WHERE close_adj IS NOT NULL AND volume > 0
            ORDER BY date ASC
            """
            df_sh_history = con.execute(sql_sh).fetchdf()
        except Exception:
            pass
            
    # 3. 提取所有可能的滑动窗口
    windows = []
    window_meta = []
    
    # 提取本股历史窗口
    h_prices = df_history['close_adj'].astype(float).to_numpy()
    h_dates = df_history['date'].tolist()
    N = len(h_prices)
    
    # 避开最近 window_size + predict_size + 10 天，防止匹配到当前走势本身
    for i in range(N - window_size - predict_size - 10):
        w_prices = h_prices[i : i + window_size]
        min_w, max_w = w_prices.min(), w_prices.max()
        range_w = max_w - min_w if max_w > min_w else 1.0
        w_norm = (w_prices - min_w) / range_w
        windows.append(w_norm)
        window_meta.append({
            "source": stock_name,
            "prices": w_prices.tolist(),
            "future_prices": h_prices[i + window_size : i + window_size + predict_size].tolist(),
            "dates": [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)[:10] for d in h_dates[i : i + window_size]],
            "future_dates": [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)[:10] for d in h_dates[i + window_size : i + window_size + predict_size]],
            "start_idx": i
        })
        
    # 提取上证指数窗口
    if not df_sh_history.empty:
        sh_prices = df_sh_history['close_adj'].astype(float).to_numpy()
        sh_dates = df_sh_history['date'].tolist()
        N_sh = len(sh_prices)
        for i in range(N_sh - window_size - predict_size - 10):
            w_prices = sh_prices[i : i + window_size]
            min_w, max_w = w_prices.min(), w_prices.max()
            range_w = max_w - min_w if max_w > min_w else 1.0
            w_norm = (w_prices - min_w) / range_w
            windows.append(w_norm)
            window_meta.append({
                "source": "上证指数",
                "prices": w_prices.tolist(),
                "future_prices": sh_prices[i + window_size : i + window_size + predict_size].tolist(),
                "dates": [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)[:10] for d in sh_dates[i : i + window_size]],
                "future_dates": [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)[:10] for d in sh_dates[i + window_size : i + window_size + predict_size]],
                "start_idx": i
            })
            
    if len(windows) == 0:
        return JSONResponse(status_code=400, content={"status": "error", "message": "未找到可用于匹配的历史数据区间！"})
        
    # 4. 向量化计算欧氏距离进行极速粗筛
    windows_arr = np.array(windows)
    target_norm_arr = np.array(target_norm)
    diffs = windows_arr - target_norm_arr
    l2_dists = np.sqrt(np.mean(diffs**2, axis=1))
    
    # 粗筛出 L2 距离最小的前 150 个候选窗口，进入 DTW 精细计算
    candidate_indices = np.argsort(l2_dists)[:150]
    
    candidates = []
    for idx in candidate_indices:
        w_norm = windows[idx]
        dtw_dist = dtw_distance(target_norm, w_norm, w=4)
        candidates.append({
            "meta": window_meta[idx],
            "dtw_dist": dtw_dist,
            "l2_dist": float(l2_dists[idx])
        })
        
    # 根据精细 DTW 距离排序
    candidates.sort(key=lambda x: x['dtw_dist'])
    
    # 5. 30 日非重叠去重逻辑过滤
    top_matches = []
    selected_intervals = []
    for cand in candidates:
        meta = cand['meta']
        src = meta['source']
        s_idx = meta['start_idx']
        e_idx = s_idx + window_size + predict_size
        
        # 检查是否重叠超过 10 天
        overlap = False
        for sel_src, sel_s, sel_e in selected_intervals:
            if sel_src == src:
                ol = max(0, min(e_idx, sel_e) - max(s_idx, sel_s))
                if ol > 10:
                    overlap = True
                    break
        if not overlap:
            selected_intervals.append((src, s_idx, e_idx))
            top_matches.append(cand)
            if len(top_matches) >= 5:
                break
                
    # 6. 对齐 Top 5 走势，计算胜率及未来统计投影
    aligned_matches = []
    target_start_price = target_prices[0]
    
    for idx, match in enumerate(top_matches):
        meta = match['meta']
        # 合并匹配区间 30 天与未来投影区间 20 天
        h_prices = meta['prices'] + meta['future_prices']
        h_dates = meta['dates'] + meta['future_dates']
        
        # 按照 Target 起始价的相对变动进行对齐缩放
        base_h = h_prices[0] if h_prices[0] > 0 else 1.0
        aligned_prices = [round(target_start_price * (p / base_h), 2) for p in h_prices]
        
        # 计算该样本后 20 天最终涨跌幅
        start_future_p = h_prices[window_size - 1]
        end_future_p = h_prices[-1]
        sample_return = round(((end_future_p - start_future_p) / start_future_p) * 100, 2) if start_future_p > 0 else 0.0
        
        # 将 DTW 距离转换成一个直观的相似度百分比得分
        similarity = round(max(50.0, min(99.5, 100.0 * (1 - match['dtw_dist'] / (window_size * 0.15)))), 1)
        
        aligned_matches.append({
            "id": idx + 1,
            "source": meta['source'],
            "start_date": meta['dates'][0],
            "end_date": meta['dates'][-1],
            "similarity": similarity,
            "sample_return": sample_return,
            "aligned_prices": aligned_prices,
            "dates": h_dates
        })
        
    # 计算胜率及收益统计
    win_rate = 50.0
    avg_return = 0.0
    max_gain = 0.0
    max_loss = 0.0
    
    if aligned_matches:
        win_count = sum(1 for m in aligned_matches if m['sample_return'] > 0)
        win_rate = round(win_count * 100.0 / len(aligned_matches), 1)
        avg_return = round(sum(m['sample_return'] for m in aligned_matches) / len(aligned_matches), 2)
        max_gain = round(max(m['sample_return'] for m in aligned_matches), 2)
        max_loss = round(min(m['sample_return'] for m in aligned_matches), 2)
        
    return {
        "status": "success",
        "symbol": resolved_symbol,
        "name": stock_name,
        "win_rate": win_rate,
        "expected_return": avg_return,
        "max_gain": max_gain,
        "max_loss": max_loss,
        "target": {
            "dates": target_dates,
            "prices": target_prices
        },
        "matches": aligned_matches
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
# 8.5 组合回测大屏极速引擎 API (POST)
# -------------------------------------------------------------
class BacktestRequest(BaseModel):
    strategy_id: str
    start_date: str
    end_date: str
    initial_cash: float = 1000000.0
    stop_loss_pct: float = 8.0          # Hard Stop Loss % (e.g. 8.0 means -8% exit)
    trailing_stop_pct: float = 6.0      # Trailing Stop-Loss % (e.g. 6.0 means -6% exit from peak)
    ma_breakout_exit: bool = True       # Exit if Close < MA20
    holding_days: int = 10              # Max holding days limit (e.g. 10 days, 0 to disable)
    max_stocks: int = 5                 # Max concurrent stock positions
    rebalance_period: str = "weekly"    # "weekly" or "daily"
    commission_pct: float = 0.03        # commission fee % per trade (e.g. 0.03%)
    stamp_duty_pct: float = 0.1         # stamp duty % (0.1% sell-side only)
    slippage_pct: float = 0.1           # slippage % per trade (e.g. 0.1%)
    benchmark_id: str = "sh000300"      # "sh000300" (CSI 300) or "sh000001" (SSE Index)

@app.post("/api/backtest", summary="系统多因子策略 portfolio 极速组合回测引擎")
def run_backtest(req: BacktestRequest):
    t_start = time.perf_counter()
    strategies = load_strategies()
    
    if req.strategy_id not in strategies:
        return JSONResponse(status_code=400, content={"status": "error", "message": f"未定义的选股策略: {req.strategy_id}"})
        
    strategy = strategies[req.strategy_id]
    tdx_dir = load_tdx_dir()
    names_map = load_stock_names(tdx_dir)
    
    # 路径规范化 (全面兼容 Windows 反斜杠转义漏洞)
    data_store_dir_norm = DATA_STORE_DIR.replace('\\', '/')
    block_mappings_path_norm = BLOCK_MAPPINGS_PATH.replace('\\', '/')
    industry_mappings_path_norm = INDUSTRY_MAPPINGS_PATH.replace('\\', '/')
    
    # 动态匹配现有的 Parquet 数据文件前缀
    prefixes = ['sh', 'sz', 'bj']
    existing_files = os.listdir(DATA_STORE_DIR)
    patterns = [f"{data_store_dir_norm}/{p}*.parquet" for p in prefixes if any(f.startswith(p) and f.endswith('.parquet') for f in existing_files)]
    patterns_str = ", ".join(f"'{p}'" for p in patterns)
    
    if not patterns:
        return JSONResponse(status_code=500, content={"status": "error", "message": "未在本地数据目录中找到任何有效 Parquet 缓存文件。"})
        
    con = duckdb.connect()
    con.execute(f"SET threads = {os.cpu_count()}")
    
    try:
        con.execute("DROP TABLE IF EXISTS mem_block_mappings")
        con.execute("DROP TABLE IF EXISTS mem_industry_mappings")
    except Exception:
        pass
        
    con.execute(f"CREATE TABLE mem_block_mappings AS SELECT * FROM read_parquet('{block_mappings_path_norm}')")
    con.execute(f"CREATE TABLE mem_industry_mappings AS SELECT * FROM read_parquet('{industry_mappings_path_norm}')")
    
    # 1. 组合选股分类过滤 (A股核心池)
    category_filter = "filename LIKE '%sh60%' OR filename LIKE '%sh68%' OR filename LIKE '%sz00%' OR filename LIKE '%sz30%' OR filename LIKE '%/bj%'"
    
    try:
        # 清理已存在的同名临时内存表
        con.execute("DROP TABLE IF EXISTS mem_data")
        con.execute("DROP TABLE IF EXISTS mem_factors")
    except Exception:
        pass
        
    t_db_start = time.perf_counter()
    try:
        # 极速加载最近 4 年的日线行情进入内存表
        con.execute(f"""
        CREATE TABLE mem_data AS 
        SELECT 
            regexp_extract(filename, '([^/\\\\\\\\]+)[.]parquet$', 1) AS symbol,
            date,
            open_adj,
            high_adj,
            low_adj,
            close_adj,
            volume,
            amount
        FROM read_parquet([{patterns_str}], filename=true)
        WHERE date >= CAST('{req.start_date}' AS TIMESTAMP) - INTERVAL 380 DAY 
          AND date <= CAST('{req.end_date}' AS TIMESTAMP)
          AND ({category_filter})
        """)
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": f"初始化内存数据表失败: {e}"})
    print(f"[Telemetry] - loaded mem_data in {time.perf_counter() - t_db_start:.4f}s")
        
    # 2. 动态分析并拆分策略 SQL 实现一键式通用全因子预计算 (mem_factors)
    raw_sql = strategy["query_sql"].replace('\r\n', '\n')
    sql_base = raw_sql.replace("read_parquet('__BLOCK_MAPPINGS_PATH__')", "mem_block_mappings")\
                      .replace("read_parquet('__INDUSTRY_MAPPINGS_PATH__')", "mem_industry_mappings")\
                      .replace("read_parquet([__PATTERNS_STR__], filename=true)", "(SELECT symbol || '.parquet' AS filename, date, open_adj, high_adj, low_adj, close_adj, volume, amount FROM mem_data)")\
                      .replace("__BLOCK_MAPPINGS_PATH__", block_mappings_path_norm)\
                      .replace("__CATEGORY_FILTER__", "1=1")
                      
    # 动态匹配 strategies 里的最新天分位数筛选子句进行前缀截断
    split_str = "latest_data AS ("
    factors_table = "calculated_factors"
    idx = sql_base.find(split_str)
    if idx == -1:
        split_str = "latest_stock AS ("
        factors_table = "stock_returns"
        idx = sql_base.find(split_str)
        
    if idx == -1:
        # 兜底清理
        con.execute("DROP TABLE IF EXISTS mem_data")
        return JSONResponse(status_code=500, content={"status": "error", "message": f"解析策略 {req.strategy_id} 的 SQL 拓扑结构失败 (找不到最新截断 CTE)！"})
        
    part1 = sql_base[:idx].strip()
    if part1.endswith(","):
        part1 = part1[:-1].strip()
        
    sql_factors = part1 + f"\nSELECT * FROM {factors_table}"
    
    t_f_start = time.perf_counter()
    try:
        # 运行 Part 1 极速生成指标表 (仅需 1-2秒即可完成几百万行的大算力指标计算)
        con.execute(f"CREATE TABLE mem_factors AS {sql_factors}")
    except Exception as e:
        con.execute("DROP TABLE IF EXISTS mem_data")
        return JSONResponse(status_code=500, content={"status": "error", "message": f"因子计算引擎在 Part 1 编译失败: {e}"})
    print(f"[Telemetry] - precalculated factors in {time.perf_counter() - t_f_start:.4f}s")
        
    # 3. 提取时间序列内的交易日及调仓日
    df_all_days = con.execute(f"""
    SELECT DISTINCT date 
    FROM mem_data 
    WHERE date >= '{req.start_date}' AND date <= '{req.end_date}'
    ORDER BY date ASC
    """).fetchdf()
    all_trading_days = [d.strftime('%Y-%m-%d') for d in df_all_days['date']]
    
    if not all_trading_days:
        con.execute("DROP TABLE IF EXISTS mem_data")
        con.execute("DROP TABLE IF EXISTS mem_factors")
        return JSONResponse(status_code=400, content={"status": "error", "message": "该时间区间内无有效交易日数据！"})
        
    # 根据调仓周期提取 rebalance_dates
    if req.rebalance_period == "weekly":
        # 提取区间内的所有星期一作为调仓日
        df_rebal = con.execute(f"""
        SELECT DISTINCT date 
        FROM mem_data 
        WHERE date >= '{req.start_date}' AND date <= '{req.end_date}'
          AND dayofweek(date) = 1
        ORDER BY date ASC
        """).fetchdf()
        rebalance_dates = set([d.strftime('%Y-%m-%d') for d in df_rebal['date']])
        
        # 兼容性兜底：如果首个交易日不是周一，强制加入作为初始调仓点
        if all_trading_days and all_trading_days[0] not in rebalance_dates:
            rebalance_dates.add(all_trading_days[0])
    else:
        # 每日调仓
        rebalance_dates = set(all_trading_days)
        
    # 4. 执行策略 Part 2 循环提取所有调仓日的选股名册 (每调仓日提取仅需 10-30毫秒)
    part2 = sql_base[idx:].strip()
    part2_base = part2.replace(f"FROM {factors_table}\n    WHERE row_num = 1", "FROM mem_factors WHERE date = '{date_str}'")\
                      .replace(f"FROM {factors_table}\n    WHERE rn = 1", "FROM mem_factors WHERE date = '{date_str}'")\
                      .replace(f"FROM {factors_table} WHERE row_num = 1", "FROM mem_factors WHERE date = '{date_str}'")\
                      .replace(f"FROM {factors_table} WHERE rn = 1", "FROM mem_factors WHERE date = '{date_str}'")\
                      .replace("WHERE row_num = 1", "WHERE date = '{date_str}'")\
                      .replace("WHERE rn = 1", "WHERE date = '{date_str}'")
                      
    t_cands_start = time.perf_counter()
    candidates = {}
    for d_str in sorted(list(rebalance_dates)):
        sql_run = f"WITH {part2_base.replace('{date_str}', d_str)}"
        try:
            df_cands = con.execute(sql_run).fetchdf()
            if not df_cands.empty:
                candidates[d_str] = df_cands['symbol'].tolist()
        except Exception:
            # 静默容错
            pass
    print(f"[Telemetry] - queried candidates in {time.perf_counter() - t_cands_start:.4f}s")
            
    # 5. 提取全部选中过的股票池进行极速价格缓存 (Prices Memory Cache)
    t_cache_start = time.perf_counter()
    unique_symbols = set()
    for syms in candidates.values():
        unique_symbols.update(syms)
    unique_symbols_list = list(unique_symbols)
    
    prices_cache = {}
    if unique_symbols_list:
        symbols_in_sql = ", ".join(f"'{s}'" for s in unique_symbols_list)
        prices_rows = con.execute(f"""
        SELECT 
            symbol,
            strftime(date, '%Y-%m-%d') AS date_str,
            open_adj,
            high_adj,
            low_adj,
            close_adj,
            volume,
            AVG(close_adj) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20
        FROM mem_data
        WHERE symbol IN ({symbols_in_sql})
        """).fetchall()
        
        for sym, d_str, op, hi, lo, cl, vol, ma in prices_rows:
            if sym not in prices_cache:
                prices_cache[sym] = {}
            prices_cache[sym][d_str] = {
                "open": op,
                "high": hi,
                "low": lo,
                "close": cl,
                "volume": vol,
                "ma20": ma
            }
    print(f"[Telemetry] - generated price cache in {time.perf_counter() - t_cache_start:.4f}s (stocks: {len(unique_symbols_list)})")
            
    # 6. 读取沪深300指数作为业绩基准
    t_bench_start = time.perf_counter()
    bench_file = f"{req.benchmark_id}.parquet"
    bench_path = os.path.join(data_store_dir_norm, bench_file)
    bench_prices = {}
    if os.path.exists(bench_path):
        try:
            df_bench = con.execute(f"""
            SELECT date, close_adj AS close 
            FROM read_parquet('{bench_path}')
            WHERE date >= '{req.start_date}' AND date <= '{req.end_date}'
            ORDER BY date ASC
            """).fetchdf()
            bench_prices = {r['date'].strftime('%Y-%m-%d'): r['close'] for _, r in df_bench.iterrows()}
        except Exception:
            pass
    print(f"[Telemetry] - loaded benchmark in {time.perf_counter() - t_bench_start:.4f}s")
            
    # 释放 DuckDB 内存临时表以保平台极致轻量化
    con.execute("DROP TABLE IF EXISTS mem_data")
    con.execute("DROP TABLE IF EXISTS mem_factors")
    try:
        con.execute("DROP TABLE IF EXISTS mem_block_mappings")
        con.execute("DROP TABLE IF EXISTS mem_industry_mappings")
    except Exception:
        pass
    
    # 7. 跑高保真日线交易流模拟 (Daily Broker Accounting Loop)
    cash = req.initial_cash
    initial_cash = cash
    holdings = {}       # symbol: {buy_price, buy_date, qty, highest_price, holding_days, buy_pnl}
    trades_log = []
    equity_history = []
    
    bench_start_price = None
    max_equity_peak = initial_cash
    
    for d_str in all_trading_days:
        # A. 标记盯市市值与更新持有期天数
        stock_value = 0.0
        for sym, h in list(holdings.items()):
            cache = prices_cache.get(sym, {}).get(d_str)
            if cache:
                h["current_price"] = cache["close"]
                h["highest_price"] = max(h["highest_price"], cache["close"])
                h["holding_days"] += 1
            stock_value += h["qty"] * h.get("current_price", h["buy_price"])
            
        today_equity = cash + stock_value
        max_equity_peak = max(max_equity_peak, today_equity)
        
        # B. 持有仓位风控退出检查 (Stop Loss / Trailing Stop / MA20 Breakout / Time Stop)
        for sym, h in list(holdings.items()):
            cache = prices_cache.get(sym, {}).get(d_str)
            if not cache or cache["volume"] <= 0:
                continue # 停牌跳过
                
            close = cache["close"]
            ma20 = cache["ma20"]
            buy_price = h["buy_price"]
            highest = h["highest_price"]
            
            triggered = False
            reason = ""
            
            # (1) 硬止损退出
            if req.stop_loss_pct > 0 and close < buy_price * (1 - abs(req.stop_loss_pct)/100.0):
                triggered = True
                reason = "硬止损触发"
            # (2) 移动追踪止盈退出
            elif req.trailing_stop_pct > 0 and close < highest * (1 - abs(req.trailing_stop_pct)/100.0):
                triggered = True
                reason = "追踪止盈触发"
            # (3) 均线破位退出
            elif req.ma_breakout_exit and close < ma20:
                triggered = True
                reason = "均线破位退出"
            # (4) 持有时间限制出局
            elif req.holding_days > 0 and h["holding_days"] >= req.holding_days:
                triggered = True
                reason = "持有天数届满"
                
            if triggered:
                # 扣除滑点后的卖出价格
                sell_price = close * (1 - abs(req.slippage_pct)/100.0)
                qty = h["qty"]
                comm = qty * sell_price * (abs(req.commission_pct)/100.0)
                stamp = qty * sell_price * (abs(req.stamp_duty_pct)/100.0) # 印花税仅卖方
                fee = comm + stamp
                
                proceeds = qty * sell_price - fee
                cash += proceeds
                pnl_pct = (sell_price / buy_price - 1) * 100.0
                
                trades_log.append({
                    "date": d_str,
                    "symbol": sym.upper(),
                    "name": names_map.get(sym, "未知股票"),
                    "action": "SELL",
                    "price": round(sell_price, 2),
                    "qty": qty,
                    "commission": round(fee, 2),
                    "pnl": round(pnl_pct, 2),
                    "reason": reason
                })
                del holdings[sym]
                
        # C. 调仓日开新仓 rebalance (买入候选个股)
        if d_str in rebalance_dates and len(holdings) < req.max_stocks:
            vacant = req.max_stocks - len(holdings)
            allocated_cash_per_stock = cash / vacant
            
            cands = [c for c in candidates.get(d_str, []) if c not in holdings]
            for sym in cands[:vacant]:
                cache = prices_cache.get(sym, {}).get(d_str)
                if not cache or cache["volume"] <= 0:
                    continue # 停牌跳过
                    
                close = cache["close"]
                buy_price = close * (1 + abs(req.slippage_pct)/100.0) # 买入滑点
                
                # A股板100股 board lot 整数倍购买，预留 0.5% 交易费余量以防爆仓
                qty = int(allocated_cash_per_stock / (buy_price * 1.005) / 100) * 100
                if qty >= 100:
                    cost = qty * buy_price
                    comm = cost * (abs(req.commission_pct)/100.0)
                    total_cost = cost + comm
                    
                    if total_cost <= cash:
                        cash -= total_cost
                        holdings[sym] = {
                            "buy_price": buy_price,
                            "buy_date": d_str,
                            "qty": qty,
                            "highest_price": buy_price,
                            "holding_days": 0,
                            "current_price": close
                        }
                        
                        trades_log.append({
                            "date": d_str,
                            "symbol": sym.upper(),
                            "name": names_map.get(sym, "未知股票"),
                            "action": "BUY",
                            "price": round(buy_price, 2),
                            "qty": qty,
                            "commission": round(comm, 2),
                            "pnl": 0.0,
                            "reason": "策略买点"
                        })
                        
        # D. 今日最终资产计价与基准对比
        stock_value = sum(h["qty"] * h.get("current_price", h["buy_price"]) for h in holdings.values())
        today_equity = cash + stock_value
        
        bench_close = bench_prices.get(d_str)
        if bench_close is not None:
            if bench_start_price is None:
                bench_start_price = bench_close
            bench_nav = (bench_close / bench_start_price) * initial_cash
        else:
            bench_nav = initial_cash
            
        peak_dd = (today_equity - max_equity_peak) / max_equity_peak * 100.0
        
        equity_history.append({
            "date": d_str,
            "total_value": round(today_equity, 2),
            "benchmark_value": round(bench_nav, 2),
            "drawdown": round(peak_dd, 2)
        })
        
    # 8. 统计科学测度与指标 Deck (Sharpe, Calmar, WinRate)
    metrics = calculate_metrics(equity_history, trades_log, initial_cash)
    
    t_end = time.perf_counter()
    return {
        "status": "success",
        "elapsed_seconds": round(t_end - t_start, 4),
        "metrics": metrics,
        "history": equity_history,
        "trades": trades_log
    }

def calculate_metrics(equity_history, trades_log, initial_cash):
    if not equity_history:
        return {}
        
    df_eq = pd.DataFrame(equity_history)
    df_eq['pct_change'] = df_eq['total_value'].pct_change().fillna(0.0)
    
    # 最大回撤
    df_eq['peak'] = df_eq['total_value'].cummax()
    df_eq['drawdown'] = (df_eq['total_value'] - df_eq['peak']) / df_eq['peak'] * 100.0
    max_dd = float(df_eq['drawdown'].min())
    
    # 累计收益与年化收益 (按242个交易日/年折算)
    total_days = len(df_eq)
    total_ret = (df_eq['total_value'].iloc[-1] / initial_cash - 1) * 100.0
    ann_ret = ((df_eq['total_value'].iloc[-1] / initial_cash) ** (242.0 / max(total_days, 1)) - 1) * 100.0
    
    # 夏普比率 (无风险利率设定为 2% 年化)
    daily_rf = 0.02 / 242.0
    excess_returns = df_eq['pct_change'] - daily_rf
    std_dev = df_eq['pct_change'].std()
    sharpe = float((excess_returns.mean() / std_dev) * (242.0 ** 0.5)) if std_dev > 0 else 0.0
    
    # 卡玛比率 (年化收益/最大回撤绝对值)
    calmar = float(ann_ret / abs(max_dd)) if abs(max_dd) > 0 else 0.0
    
    # 交易胜率及盈亏比 (仅针对卖出完成的交易)
    sells = [t for t in trades_log if t['action'] == 'SELL']
    if sells:
        winning_trades = sum(1 for t in sells if t['pnl'] > 0)
        win_rate = (winning_trades / len(sells)) * 100.0
        
        gains = [t['pnl'] for t in sells if t['pnl'] > 0]
        losses = [abs(t['pnl']) for t in sells if t['pnl'] <= 0]
        avg_gain = sum(gains) / len(gains) if gains else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0
        profit_loss_ratio = float(avg_gain / avg_loss) if avg_loss > 0 else 99.0
    else:
        win_rate = 0.0
        profit_loss_ratio = 0.0
        
    return {
        "total_return": round(total_ret, 2),
        "annualized_return": round(ann_ret, 2),
        "sharpe_ratio": round(sharpe, 2),
        "max_drawdown": round(max_dd, 2),
        "calmar_ratio": round(calmar, 2),
        "win_rate": round(win_rate, 2),
        "profit_loss_ratio": round(profit_loss_ratio, 2)
    }

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
