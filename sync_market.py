import os
import json
import time
import multiprocessing
from multiprocessing import Pool
import pandas as pd
from parser import parse_tdx_day_file, parse_tdx_gbbq_file, compute_forward_adjustment, sync_all_blocks
from storage import save_to_parquet

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_STORE_DIR = os.path.join(CURRENT_DIR, "data")

# -------------------------------------------------------------
# 路径与配置管理 (代码与配置分离的 OCP 设计)
# -------------------------------------------------------------
TDX_DIR = "/mnt/e/Tools/tdx"  # 默认通达信路径
CONFIG_PATH = os.path.join(CURRENT_DIR, "config.json")

# 自动从本地的 config.json 配置文件中载入通达信路径，规避直接修改源代码
if os.path.exists(CONFIG_PATH):
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = json.load(f)
            if "tdx_dir" in config:
                TDX_DIR = config["tdx_dir"]
    except Exception as e:
        print(f"⚠️ 警告: 载入 config.json 失败，将采用默认路径。错误: {e}")

GBBQ_PATH = os.path.join(TDX_DIR, "T0002", "hq_cache", "gbbq")
SH_LDAY_DIR = os.path.join(TDX_DIR, "vipdoc", "sh", "lday")
SZ_LDAY_DIR = os.path.join(TDX_DIR, "vipdoc", "sz", "lday")

# 全局变量，子进程在 fork 后会自动继承它以避免庞大的序列化开销
global_gbbq_df = None

def init_worker(gbbq_df):
    """
    初始化进程池子进程，共享同一份复权事件表
    """
    global global_gbbq_df
    global_gbbq_df = gbbq_df

def sync_single_file(task_args):
    """
    单个二进制日K线文件的处理函数
    """
    filepath, data_dir = task_args
    filename = os.path.basename(filepath)
    code = filename.split('.')[0]
    
    try:
        # 1. 解析二进制数据
        df = parse_tdx_day_file(filepath)
        if df.empty:
            return code, False, "空数据"
            
        # 2. 判断标的属性 (个股 vs 指数/板块)
        # 常见个股代码规则: 沪市 60/68, 深市 00/30, 北交所 43/83/87/88/92
        is_stock = False
        if (code.startswith("sh60") or code.startswith("sh68") or 
            code.startswith("sz00") or code.startswith("sz30") or 
            code.startswith("bj43") or code.startswith("bj83") or 
            code.startswith("bj87") or code.startswith("bj88") or 
            code.startswith("bj92")):
            is_stock = True
            
        # 3. 复权计算
        if is_stock and global_gbbq_df is not None:
            df_adj = compute_forward_adjustment(df, global_gbbq_df)
        else:
            # 大盘指数和板块指数不进行除权，直接生成默认复权价格
            df_adj = df.copy()
            for col in ['open', 'high', 'low', 'close']:
                df_adj[f'{col}_adj'] = df_adj[col]
            df_adj['volume_adj'] = df_adj['volume']
            df_adj['factor'] = 1.0
            
        # 4. 持久化为 Parquet
        save_to_parquet(df_adj, data_dir)
        return code, True, "成功"
    except Exception as e:
        return code, False, str(e)

def sync_all_industries(tdx_dir: str, output_parquet_path: str):
    """
    Sync stock-to-industry classifications using tdxhy.cfg and incon.dat
    """
    incon_path = os.path.join(tdx_dir, "incon.dat")
    hy_path = os.path.join(tdx_dir, "T0002", "hq_cache", "tdxhy.cfg")
    
    if not os.path.exists(incon_path) or not os.path.exists(hy_path):
        print("⚠️ 警告: 行业配置文件不存在，跳过行业数据解析")
        return None
        
    # 1. 解析 incon.dat
    industry_names = {}
    with open(incon_path, "rb") as f:
        lines = f.read().decode("gbk", errors="ignore").splitlines()
    
    in_section = False
    for line in lines:
        line = line.strip()
        if line == "#TDXNHY":
            in_section = True
            continue
        elif line.startswith("#") and in_section:
            in_section = False
            continue
        
        if in_section and "|" in line:
            code, name = line.split("|", 1)
            if len(code) == 5:
                industry_names[code] = name
                
    # 2. 解析 tdxhy.cfg
    results = []
    with open(hy_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("|")
            if len(parts) >= 3:
                market_id, code, hy_code = parts[0], parts[1], parts[2]
                if len(hy_code) >= 5:
                    hy_level2 = hy_code[:5]
                    market = "sz" if market_id == "0" else ("sh" if market_id == "1" else "bj")
                    name = industry_names.get(hy_level2, "其它行业")
                    results.append({
                        "symbol": f"{market}{code}",
                        "industry_code": hy_level2,
                        "industry_name": name
                    })
                    
    df = pd.DataFrame(results)
    os.makedirs(os.path.dirname(output_parquet_path), exist_ok=True)
    df.to_parquet(output_parquet_path, index=False, compression="snappy")
    print(f"✅ 行业映射数据同步完成! 写入 {output_parquet_path}，共 {len(df)} 条映射。")
    return df

def main():
    print("=" * 70)
    print("      通达信本地数据池极速多进程同步引擎 (Phase 2)")
    print("=" * 70)
    
    # 确保存储目录存在
    os.makedirs(DATA_STORE_DIR, exist_ok=True)
    
    t0 = time.perf_counter()
    
    # 1. 极速读取解密股本变动数据
    print("[Step 1] 正在加载并解密全局 GBBQ 复权数据库...")
    try:
        gbbq_df = parse_tdx_gbbq_file(GBBQ_PATH)
        print(f"✅ GBBQ 加载完成! 提取复权变更事件: {len(gbbq_df)} 条 | 耗时: {time.perf_counter() - t0:.2f} 秒")
    except Exception as e:
        print(f"❌ 加载 GBBQ 失败: {e}。将无法计算前复权价格！")
        gbbq_df = pd.DataFrame()
        
    # 2. 扫描通达信目录中的全部源二进制 .day 文件
    print("\n[Step 2] 正在扫描本地通达信日K线目录...")
    src_files = []
    for directory in [SH_LDAY_DIR, SZ_LDAY_DIR]:
        if os.path.exists(directory):
            for file in os.listdir(directory):
                if file.endswith(".day") and len(file.split('.')[0]) >= 8:
                    src_files.append(os.path.join(directory, file))
                    
    total_files = len(src_files)
    print(f"✅ 扫描完毕! 发现通达信日K线二进制文件共: {total_files} 个")
    
    # 3. 增量比对算法：检测需要更新的文件
    print("\n[Step 3] 正在进行增量修改对比 (检测 mtime)...")
    t_diff = time.perf_counter()
    tasks = []
    
    for filepath in src_files:
        filename = os.path.basename(filepath)
        code = filename.split('.')[0]
        pq_path = os.path.join(DATA_STORE_DIR, f"{code}.parquet")
        
        # 判断是否需要更新：
        # - 目标 Parquet 不存在
        # - 或者源二进制文件的最后修改时间晚于 Parquet 文件的最后修改时间
        if not os.path.exists(pq_path) or os.path.getmtime(filepath) > os.path.getmtime(pq_path):
            tasks.append((filepath, DATA_STORE_DIR))
            
    print(f"✅ 对比完成! 耗时: {time.perf_counter() - t_diff:.2f} 秒")
    print(f"📊 待同步文件: {len(tasks)} 个 | 已是最新状态无需同步: {total_files - len(tasks)} 个")
    
    # 4. 同步板块映射数据
    print("\n[Step 4] 正在同步全局板块映射关系 (概念/风格/指数)...")
    try:
        hq_cache_dir = os.path.join(TDX_DIR, "T0002", "hq_cache")
        output_parquet_path = os.path.join(DATA_STORE_DIR, "block_mappings.parquet")
        sync_all_blocks(hq_cache_dir, output_parquet_path)
    except Exception as e:
        print(f"❌ 同步板块映射失败: {e}")
        
    print("\n[Step 4.5] 正在同步全局行业板块分类映射关系...")
    try:
        output_ind_path = os.path.join(DATA_STORE_DIR, "industry_mappings.parquet")
        sync_all_industries(TDX_DIR, output_ind_path)
    except Exception as e:
        print(f"❌ 同步行业映射失败: {e}")
        
    if not tasks:
        print("\n🎉 完美！所有本地数据已是最新状态，无需同步！")
        print(f"总计运行总耗时: {time.perf_counter() - t0:.2f} 秒")
        print("=" * 70)
        return
        
    # 5. 并行多进程同步核心
    cpu_count = multiprocessing.cpu_count()
    print(f"\n[Step 5] 启动多进程日K线同步，满载调用 {cpu_count} 核 CPU 并行加速中...")
    
    t_sync = time.perf_counter()
    completed = 0
    success_count = 0
    failed_tasks = []
    
    # 创建进程池，使用 initializer 共享全局 GBBQ 变量，避免进程间庞大的网络数据序列化开销
    with Pool(processes=cpu_count, initializer=init_worker, initargs=(gbbq_df,)) as pool:
        # 使用 imap_unordered 结合合理的 chunksize 提升并行分发吞吐率
        chunksize = max(1, len(tasks) // (cpu_count * 4))
        
        for code, success, msg in pool.imap_unordered(sync_single_file, tasks, chunksize=chunksize):
            completed += 1
            if success:
                success_count += 1
            else:
                failed_tasks.append((code, msg))
                
            # 每隔 500 个文件或者到最后打印一次漂亮的进度条
            if completed % 500 == 0 or completed == len(tasks):
                elapsed = time.perf_counter() - t_sync
                speed = completed / elapsed if elapsed > 0 else 0
                print(f"  ⚡ 进度: [{completed}/{len(tasks)}] | 成功: {success_count} | 速度: {speed:.1f} 文件/秒")
                
    t_total_sync = time.perf_counter() - t_sync
    print(f"\n✅ K线同步完成！共耗时: {t_total_sync:.2f} 秒")
    print(f"📊 结果汇总: 同步成功: {success_count} 个 | 失败: {len(failed_tasks)} 个")
    
    if failed_tasks:
        print("\n❌ 失败任务明细 (前5条):")
        for f_code, f_err in failed_tasks[:5]:
            print(f"  - {f_code}: {f_err}")
        
    print(f"\n🎉 恭喜！本地 Parquet 数据池同步完毕，共占用文件数: {len(os.listdir(DATA_STORE_DIR))} 个")
    print(f"总计运行总耗时: {time.perf_counter() - t0:.2f} 秒")
    print("=" * 70)

if __name__ == "__main__":
    main()
