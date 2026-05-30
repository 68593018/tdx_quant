import os
import sys
import time
import multiprocessing
from multiprocessing import Pool
import pandas as pd
import numpy as np

# 避免 Windows / WSL 平台下的编码崩溃问题
if sys.platform.startswith('win'):
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
MINUTE_BARS_DIR = os.path.join(CURRENT_DIR, "data", "minute_bars", "freq=1m")
OUTPUT_DIR = os.path.join(CURRENT_DIR, "data", "factors")

def calculate_factors_for_stock(args):
    """
    对单个股票在指定交易日的高频 1分钟 K 线数据进行微观结构因子挖掘。
    """
    filepath, target_date = args
    try:
        # 只加载需要的列，极致节省 I/O 耗时
        df = pd.read_parquet(
            filepath, 
            columns=['code', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'amount']
        )
        if df.empty:
            return None
            
        # 过滤指定交易日
        day_df = df[df['datetime'].dt.strftime('%Y-%m-%d') == target_date].copy()
        if day_df.empty:
            return None
            
        # 按时间排序
        day_df.sort_values(by='datetime', inplace=True)
        
        # 1. 基础日内聚合指标
        total_vol = day_df['volume'].sum()
        total_amt = day_df['amount'].sum()
        if total_vol <= 0:
            return None
            
        # 2. 计算【高频实现波动率 (Realized Volatility)】
        day_df['prev_close'] = day_df['close'].shift(1).fillna(day_df['open'])
        day_df['log_ret'] = np.log(day_df['close'] / day_df['prev_close'])
        realized_vol = np.sqrt(np.sum(day_df['log_ret'] ** 2))
        
        # 日内分时筛选
        day_df['hm'] = day_df['datetime'].dt.strftime('%H:%M')
        
        # 3. 计算【早盘开盘抢筹因子 (Morning Inflow Ratio)】
        # 计算 09:31 - 09:45 期间的涨幅与成交量占比乘积
        morn = day_df[(day_df['hm'] >= '09:31') & (day_df['hm'] <= '09:45')]
        if not morn.empty:
            morn_open = morn['open'].iloc[0]
            morn_close = morn['close'].iloc[-1]
            morn_vol = morn['volume'].sum()
            # 动量强度 * 对数成交占比
            morning_inflow = (morn_close / morn_open - 1.0) * np.log(morn_vol / total_vol + 1e-5)
        else:
            morning_inflow = 0.0
            
        # 4. 计算【尾盘博弈因子 (Afternoon Momentum)】
        # 计算 14:31 - 15:00 期间的涨幅（尾盘动量）
        aft = day_df[(day_df['hm'] >= '14:31') & (day_df['hm'] <= '15:00')]
        if not aft.empty:
            aft_open = aft['open'].iloc[0]
            aft_close = aft['close'].iloc[-1]
            afternoon_mom = (aft_close / aft_open - 1.0)
        else:
            afternoon_mom = 0.0
            
        # 5. 计算【成交量分布熵 (Volume Entropy)】
        # 熵值低说明成交量高度集中在少部分时间（如主力爆发性扫货/出货），熵值高说明成交分布均匀
        probs = day_df['volume'] / total_vol
        probs = probs[probs > 0]
        volume_entropy = -np.sum(probs * np.log(probs))
        
        code = day_df['code'].iloc[0]
        
        return {
            'code': code,
            'date': target_date,
            'realized_volatility': float(realized_vol),
            'morning_inflow_ratio': float(morning_inflow),
            'afternoon_momentum': float(afternoon_mom),
            'volume_entropy': float(volume_entropy),
            'total_volume': float(total_vol),
            'total_amount': float(total_amt)
        }
    except Exception:
        return None

def main():
    target_date = "2026-05-29"  # 默认使用最新一天数据
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        
    print("=" * 70)
    print(f"🧬 开始运行日内高频微观结构因子挖掘程序")
    print(f"📅 目标分析交易日: {target_date}")
    print(f"📂 高频数据源目录: {MINUTE_BARS_DIR}")
    print("=" * 70)
    
    if not os.path.exists(MINUTE_BARS_DIR):
        print(f"❌ 错误: 未找到分钟高频数据存储目录: {MINUTE_BARS_DIR}")
        sys.exit(1)
        
    # 1. 扫描所有的股票文件
    files = [os.path.join(MINUTE_BARS_DIR, f) for f in os.listdir(MINUTE_BARS_DIR) if f.endswith(".parquet")]
    total_files = len(files)
    
    if total_files == 0:
        print("❌ 错误: 未扫描到任何分钟 Parquet 数据文件，请确保已运行同步数据。")
        sys.exit(1)
        
    print(f"🔍 已扫描到 {total_files} 个标的的高频分时序列。")
    print(f"🚀 启动多核并行因子挖掘处理器...")
    
    # 2. 构造任务参数列表
    tasks = [(f, target_date) for f in files]
    
    start_time = time.time()
    cpu_count = multiprocessing.cpu_count()
    
    results = []
    with Pool(processes=cpu_count) as pool:
        # 极速并行计算
        raw_results = pool.map(calculate_factors_for_stock, tasks, chunksize=20)
        results = [r for r in raw_results if r is not None]
        
    elapsed = time.time() - start_time
    total_computed = len(results)
    
    print(f"✅ 计算完成！共计成功提取 {total_computed} 只个股的日内因子。")
    print(f"⏱️ 耗时: {elapsed:.2f} 秒 | 吞吐速度: {total_files / elapsed:.1f} 只/秒")
    print("=" * 70)
    
    if total_computed == 0:
        print("⚠️ 未能在该交易日计算出任何有效因子，可能是因为该日无交易数据。")
        return
        
    # 3. 转化为 DataFrame 进行排行展示
    factor_df = pd.DataFrame(results)
    
    # 4. 保存为 Parquet 格式便于回测和策略接入
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"high_freq_factors_{target_date.replace('-', '')}.parquet")
    factor_df.to_parquet(out_path, index=False, compression='zstd')
    print(f"💾 因子数据集已成功归档到: {out_path}")
    print("=" * 70)
    
    # 5. 展示因子领涨领跌与异动排行榜 (以极具科技感的终端看板输出)
    # A. 【早盘强势抢筹 Top 5】 (极速主力进场，高 morning_inflow_ratio)
    top_morn = factor_df.sort_values(by='morning_inflow_ratio', ascending=False).head(5)
    print("🔥 早盘强势抢筹排行榜 (Top 5 Active Inflow) :")
    for i, (_, row) in enumerate(top_morn.iterrows(), 1):
        print(f"  [{i}] 代码: {row['code']} | 抢筹因子: {row['morning_inflow_ratio']:7.4f} | 日内波动: {row['realized_volatility']:6.2%} | 成交额: {row['total_amount']/1e8:6.2f} 亿")
    print("-" * 70)
    
    # B. 【尾盘动量喷发 Top 5】 (afternoon_momentum 最高)
    top_aft = factor_df.sort_values(by='afternoon_momentum', ascending=False).head(5)
    print("🌌 尾盘动量拉升排行榜 (Top 5 Afternoon Drift) :")
    for i, (_, row) in enumerate(top_aft.iterrows(), 1):
        print(f"  [{i}] 代码: {row['code']} | 尾盘涨幅: {row['afternoon_momentum']:7.2%} | 量能分布熵: {row['volume_entropy']:5.2f} | 成交额: {row['total_amount']/1e8:6.2f} 亿")
    print("-" * 70)
    
    # C. 【日内超常剧烈波动 Top 5】 (realized_volatility 最高，高频实现波动率)
    top_vol = factor_df.sort_values(by='realized_volatility', ascending=False).head(5)
    print("⚡ 日内超高频剧烈波动排行榜 (Top 5 High Volatility) :")
    for i, (_, row) in enumerate(top_vol.iterrows(), 1):
        print(f"  [{i}] 代码: {row['code']} | 物理波动率: {row['realized_volatility']:7.2%} | 尾盘涨幅: {row['afternoon_momentum']:6.2%} | 成交额: {row['total_amount']/1e8:6.2f} 亿")
    print("=" * 70)

if __name__ == "__main__":
    main()
