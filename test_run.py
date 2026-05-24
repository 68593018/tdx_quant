import os
import time
import pandas as pd
from parser import parse_tdx_day_file, parse_tdx_gbbq_file, compute_forward_adjustment
from storage import save_to_parquet, load_from_parquet

# -------------------------------------------------------------
# 1. 路径配置 (WSL 通达信挂载路径与本地 Parquet 缓存目录)
# -------------------------------------------------------------
TDX_DIR = "/mnt/e/Tools/tdx"
GBBQ_PATH = os.path.join(TDX_DIR, "T0002", "hq_cache", "gbbq")
SH_LDAY_DIR = os.path.join(TDX_DIR, "vipdoc", "sh", "lday")
SZ_LDAY_DIR = os.path.join(TDX_DIR, "vipdoc", "sz", "lday")

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_STORE_DIR = os.path.join(CURRENT_DIR, "data")

def main():
    print("=" * 70)
    print("      通达信本地二进制日K线与复权系统 (WSL) - 数据通路验证")
    print("=" * 70)
    
    # -------------------------------------------------------------
    # 2. 解析 GBBQ 股本变迁文件 (全局仅需解析一次)
    # -------------------------------------------------------------
    print(f"\n[Step 1] 开始解析全局 GBBQ 复权因子数据库...")
    t0 = time.perf_counter()
    try:
        gbbq_df = parse_tdx_gbbq_file(GBBQ_PATH)
        t_elap = time.perf_counter() - t0
        print(f"✅ GBBQ 解析成功！耗时: {t_elap:.4f} 秒 | 提取复权变动记录数: {len(gbbq_df)} 条")
        print(gbbq_df.head(5))
    except Exception as e:
        print(f"❌ GBBQ 文件加载失败: {e}")
        return

    # -------------------------------------------------------------
    # 3. 寻找待测试的个股、大盘指数及板块指数文件
    # -------------------------------------------------------------
    print(f"\n[Step 2] 正在检索待测试的日K线数据文件...")
    
    # 定义我们要找的测试标的
    targets = {
        'index': 'sh000001',   # 上证指数
        'sector': 'sh881395',  # 板块指数 (半导体)
        'stock': 'sh600000'    # 浦发银行
    }
    
    # 自动搜索路径
    target_paths = {}
    for key, code in targets.items():
        sh_path = os.path.join(SH_LDAY_DIR, f"{code}.day")
        sz_path = os.path.join(SZ_LDAY_DIR, f"{code}.day")
        
        if os.path.exists(sh_path):
            target_paths[key] = sh_path
        elif os.path.exists(sz_path):
            target_paths[key] = sz_path
            
    # 如果指定的个股不在，寻找任何一个沪市/深市个股文件
    if 'stock' not in target_paths:
        for file in os.listdir(SH_LDAY_DIR):
            if file.startswith("sh60") and file.endswith(".day"):
                target_paths['stock'] = os.path.join(SH_LDAY_DIR, file)
                break
                
    # 如果指定的板块不在，寻找任何一个板块文件
    if 'sector' not in target_paths:
        for file in os.listdir(SH_LDAY_DIR):
            if file.startswith("sh88") and file.endswith(".day"):
                target_paths['sector'] = os.path.join(SH_LDAY_DIR, file)
                break

    print("待测试文件路径:")
    for k, p in target_paths.items():
        print(f"  - {k.upper()}: {p}")

    # -------------------------------------------------------------
    # 4. 逐个解析、计算复权并持久化为 Parquet
    # -------------------------------------------------------------
    print(f"\n[Step 3] 开始读取、清洗并生成前复权 K 线...")
    
    for category, filepath in target_paths.items():
        print("-" * 60)
        print(f"正在处理 【{category.upper()}】: {os.path.basename(filepath)}")
        
        # 1. 极速读取二进制
        t_start = time.perf_counter()
        k_df = parse_tdx_day_file(filepath)
        t_read = time.perf_counter() - t_start
        print(f"  -> 二进制解析成功! 行数: {len(k_df)} | 耗时: {t_read:.4f} 秒")
        
        # 2. 如果是股票，计算前复权 (指数和大盘没有除权事件，直接复制)
        t_adj_start = time.perf_counter()
        if category == 'stock':
            k_df_adj = compute_forward_adjustment(k_df, gbbq_df)
        else:
            k_df_adj = k_df.copy()
            for col in ['open', 'high', 'low', 'close']:
                k_df_adj[f'{col}_adj'] = k_df_adj[col]
            k_df_adj['volume_adj'] = k_df_adj['volume']
            k_df_adj['factor'] = 1.0
        t_adj = time.perf_counter() - t_adj_start
        
        if category == 'stock':
            print(f"  -> 复权因子计算完毕! 耗时: {t_adj:.4f} 秒")
            # 打印部分复权发生变化的记录进行验证
            splits = k_df_adj[k_df_adj['factor'] != 1.0]
            if not splits.empty:
                print(f"  -> [复权验证] 历史复权修改了 {len(splits)} 天的价格。")
                print("     [最远历史前复权 vs 未复权对比]:")
                print(k_df_adj[['date', 'close', 'close_adj', 'factor']].head(3))
            else:
                print("  -> [复权验证] 该股历史无除权除息记录。")
                
        # 3. 存储为本地 Snappy Parquet 列式存储
        t_write_start = time.perf_counter()
        pq_path = save_to_parquet(k_df_adj, DATA_STORE_DIR)
        t_write = time.perf_counter() - t_write_start
        print(f"  -> Parquet 持久化成功! 保存位置: {pq_path} | 耗时: {t_write:.4f} 秒")
        
        # 4. 演示 DuckDB/Pandas 最爱的列式过滤读取
        t_load_start = time.perf_counter()
        # 我们仅加载 date, close_adj, volume_adj 三列，模拟计算某因子的场景
        loaded_df = load_from_parquet(k_df_adj['code'].iloc[0], DATA_STORE_DIR, columns=['date', 'close_adj', 'volume_adj'])
        t_load = time.perf_counter() - t_load_start
        print(f"  -> [列式读取演示] 仅加载 [date, close_adj, volume_adj] 共 {len(loaded_df)} 行 | 耗时: {t_load:.4f} 秒")
        print(loaded_df.head(3))
        
    print("\n" + "=" * 70)
    print("                     数据通路第一阶段全部验证通过！")
    print("=" * 70)

if __name__ == "__main__":
    main()
