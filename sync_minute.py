import os
import sys
import json
import time
import multiprocessing
from multiprocessing import Pool
import pandas as pd
import numpy as np
from parser import parse_tdx_day_file, parse_tdx_lc_file, parse_tdx_gbbq_file, compute_forward_adjustment

# 消除 Windows / WSL 平台下的编码打印崩溃问题
if sys.platform.startswith('win'):
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_STORE_DIR = os.path.join(CURRENT_DIR, "data")
CONFIG_PATH = os.path.join(CURRENT_DIR, "config.json")

# 1. 默认通达信路径与配置加载
TDX_DIR = "/mnt/e/Tools/tdx"
if os.path.exists(CONFIG_PATH):
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = json.load(f)
            if "tdx_dir" in config:
                path = config["tdx_dir"]
                if sys.platform.startswith('linux') and (':' in path or '\\' in path):
                    path = path.replace('\\', '/')
                    import re
                    match = re.match(r'^([a-zA-Z]):/(.*)', path)
                    if match:
                        drive = match.group(1).lower()
                        subpath = match.group(2)
                        path = f"/mnt/{drive}/{subpath}"
                TDX_DIR = path
    except Exception as e:
        print(f"⚠️ 警告: 载入 config.json 失败，将采用默认路径。错误: {e}")

GBBQ_PATH = os.path.join(TDX_DIR, "T0002", "hq_cache", "gbbq")

# 全局共享的除权因子变量，通过进程池初始化函数继承，减少进程通信开销
global_gbbq_df = None

def init_worker(gbbq_df):
    global global_gbbq_df
    global_gbbq_df = gbbq_df

def process_single_minute_file(args):
    """
    处理单个股票的分钟 K 线文件（lc1 或 lc5）并进行复权计算与 Parquet 写入。
    """
    filepath, freq, tdx_dir, output_dir = args
    filename = os.path.basename(filepath)
    code = filename.split('.')[0]  # e.g., sh600000
    market = code[:2]             # sh, sz, bj
    
    try:
        # 1. 极速读取并解析通达信分钟二进制 K 线数据
        df = parse_tdx_lc_file(filepath)
        if df.empty:
            return code, True, "数据为空"
            
        # 2. 查询对应的日线数据，提取复权因子映射
        factor_map = {}
        day_filename = f"{code}.day"
        day_filepath = os.path.join(tdx_dir, "vipdoc", market, "lday", day_filename)
        
        if os.path.exists(day_filepath):
            try:
                # 读日线并计算每日的累计复权因子
                day_df = parse_tdx_day_file(day_filepath)
                if not day_df.empty:
                    day_adj = compute_forward_adjustment(day_df, global_gbbq_df)
                    # 建立 date -> factor 的高速查找表
                    factor_map = dict(zip(day_adj['date'].dt.date, day_adj['factor']))
            except Exception as ex:
                # 日线文件解析异常时忽略，以 unadjusted 原价输出
                pass
                
        # 3. 对高频分钟 K 线进行前复权处理
        df['date_only'] = df['datetime'].dt.date
        df['factor'] = df['date_only'].map(factor_map).fillna(1.0)
        
        # 计算前复权 OHLC 与成交量
        df['open_adj'] = (df['open'] * df['factor']).round(2)
        df['high_adj'] = (df['high'] * df['factor']).round(2)
        df['low_adj'] = (df['low'] * df['factor']).round(2)
        df['close_adj'] = (df['close'] * df['factor']).round(2)
        df['volume_adj'] = (df['volume'] / df['factor']).round(0)
        
        # 4. 剔除多余的辅助列并保存为 Parquet 文件
        df.drop(columns=['date_only'], inplace=True)
        
        target_path = os.path.join(output_dir, f"{code}.parquet")
        df.to_parquet(target_path, index=False, compression='zstd')
        
        return code, True, "成功"
    except Exception as e:
        return code, False, str(e)

def main():
    print("=" * 70)
    print("🚀 开始执行通达信 1分钟 & 5分钟高频 K 线极速解析与同步程序")
    print(f"📂 通达信目录: {TDX_DIR}")
    print(f"💾 输出数据目录: {DATA_STORE_DIR}")
    print("=" * 70)
    
    if not os.path.exists(TDX_DIR):
        print(f"❌ 错误: 通达信路径不存在: {TDX_DIR}，请检查 config.json。")
        sys.exit(1)
        
    # 1. 载入权息事件数据库 (GBBQ)
    print("📦 正在解析通达信除权除息股本变动数据库 (GBBQ)...")
    gbbq_df = pd.DataFrame()
    if os.path.exists(GBBQ_PATH):
        try:
            gbbq_df = parse_tdx_gbbq_file(GBBQ_PATH)
            print(f"✅ GBBQ 载入成功，共计 {len(gbbq_df)} 条除权除息记录。")
        except Exception as e:
            print(f"⚠️ 警告: 读取 GBBQ 股本变动文件失败: {e}，分钟数据将不执行复权。")
    else:
        print("⚠️ 未找到 GBBQ 文件，分钟数据将采用未复权原价。")
        
    # 2. 扫描待处理的高频文件
    tasks = []
    
    # 支持 1分钟（minline）和 5分钟（fzline 或 minline 里的 lc5）
    # 通达信中：
    # - sh/minline/sh*.lc1 是 1分钟，sh*.lc5 是 5分钟
    # - sz/minline/sz*.lc1 是 1分钟，sz*.lc5 是 5分钟
    # - bj/minline/bj*.lc1 是 1分钟，bj*.lc5 是 5分钟
    for freq, ext in [("1m", ".lc1"), ("5m", ".lc5")]:
        output_dir = os.path.join(DATA_STORE_DIR, "minute_bars", f"freq={freq}")
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"🔍 正在扫描全市场 {freq} 分钟线二进制文件...")
        freq_count = 0
        
        for market in ["sh", "sz", "bj"]:
            minline_dir = os.path.join(TDX_DIR, "vipdoc", market, "minline")
            if not os.path.exists(minline_dir):
                continue
                
            files = [os.path.join(minline_dir, f) for f in os.listdir(minline_dir) if f.endswith(ext)]
            for filepath in files:
                tasks.append((filepath, freq, TDX_DIR, output_dir))
                freq_count += 1
                
        print(f"   发现 {freq} 任务共计 {freq_count} 个标的。")
        
    total_tasks = len(tasks)
    if total_tasks == 0:
        print("💡 未发现任何以 .lc1 或 .lc5 结尾的分时 K 线文件，程序结束。")
        return
        
    print(f"🔥 开始进行多核并行同步 (任务总数: {total_tasks})...")
    
    # 3. 多核并行处理
    start_time = time.time()
    cpu_count = multiprocessing.cpu_count()
    success_count = 0
    fail_count = 0
    
    # 使用 Pool 进行极速批处理，同时共享 GBBQ 变量避免子进程反序列化瓶颈
    with Pool(processes=cpu_count, initializer=init_worker, initargs=(gbbq_df,)) as pool:
        # 分批处理并打印进度
        results = pool.imap_unordered(process_single_minute_file, tasks, chunksize=10)
        
        for idx, (code, success, msg) in enumerate(results, 1):
            if success:
                success_count += 1
            else:
                fail_count += 1
                print(f"❌ 同步失败: {code}，错误原因: {msg}")
                
            if idx % 200 == 0 or idx == total_tasks:
                elapsed = time.time() - start_time
                speed = idx / elapsed if elapsed > 0 else 0
                progress = (idx / total_tasks) * 100
                print(f"⏳ 进度: {progress:6.2f}% | 已处理: {idx}/{total_tasks} | 速度: {speed:6.1f} 个/秒 | 耗时: {elapsed:5.1f}s")
                
    elapsed_time = time.time() - start_time
    print("=" * 70)
    print("🎉 同步执行完毕！")
    print(f"📊 成功: {success_count} 个 | 失败: {fail_count} 个 | 总耗时: {elapsed_time:.1f} 秒")
    print(f"📈 平均同步吞吐速度: {total_tasks / elapsed_time:.1f} 个/秒")
    print(f"📁 1分钟线存储在: {os.path.join(DATA_STORE_DIR, 'minute_bars', 'freq=1m')}")
    print(f"📁 5分钟线存储在: {os.path.join(DATA_STORE_DIR, 'minute_bars', 'freq=5m')}")
    print("=" * 70)

if __name__ == "__main__":
    main()
