import pandas as pd
import numpy as np

def compute_forward_adjustment(df: pd.DataFrame, gbbq_df: pd.DataFrame) -> pd.DataFrame:
    """
    对个股日线数据进行前复权（Forward Adjustment）计算。
    
    前复权原则：最新一日的价格保持不变，历史价格乘以复权因子。
    复权因子公式（在每个除权除息日 t 之前的一天 t-1）：
        f_t = (P_{t-1} - D + A * R) / (P_{t-1} * (1 + S + R))
    其中：
        P_{t-1} 为除权除息日前一天的收盘价（未复权）
        D 为每股派现 (dividend)
        A 为配股价 (allocated_price)
        R 为每股配股比例 (allocated_ratio)
        S 为每股送转股比例 (split_ratio)
        
    对于任意交易日 T，其累积复权因子为所有在其之后的除权除息日 t 的复权因子 f_t 的乘积：
        Factor(T) = \prod_{t > T} f_t
    """
    # 复制一份以防修改原数据
    res_df = df.copy()
    res_df.sort_values(by='date', inplace=True)
    res_df.reset_index(drop=True, inplace=True)
    
    # 初始化复权因子列为 1.0
    res_df['factor'] = 1.0
    
    if gbbq_df.empty:
        # 如果没有复权数据，直接返回
        for col in ['open', 'high', 'low', 'close']:
            res_df[f'{col}_adj'] = res_df[col]
        res_df['volume_adj'] = res_df['volume']
        return res_df
        
    # 提取 6 位股票代码（从如 'sh600000' 中提取 '600000'）
    code_raw = res_df['code'].iloc[0]
    code_6 = code_raw[-6:]
    
    # 过滤属于这只股票的复权事件
    # category == 1 代表除权除息
    events = gbbq_df[
        (gbbq_df['code'] == code_6) & 
        (gbbq_df['category'] == 1)
    ].copy()
    
    if events.empty:
        # 如果没有相关的复权事件，直接复制原价格
        for col in ['open', 'high', 'low', 'close']:
            res_df[f'{col}_adj'] = res_df[col]
        res_df['volume_adj'] = res_df['volume']
        return res_df
        
    # 按照日期升序排列事件
    events.sort_values(by='date', inplace=True)
    
    # 逐个计算每个除权日的复权因子
    for _, event in events.iterrows():
        event_date = event['date']
        # 通达信 GBBQ 文件中，派现金额(D)、送转比(S)、配股比(R) 都是以“每 10 股”为单位存储的，
        # 我们必须将其除以 10.0，转化为“每 1 股”的比率来进行数学计算。
        # 配股价 (A) 是单股价格，保持不变。
        D = event['dividend'] / 10.0
        A = event['allocated_price']
        R = event['allocated_ratio'] / 10.0
        S = event['split_ratio'] / 10.0
        
        # 过滤出事件日之前的所有交易日
        prev_trading_days = res_df[res_df['date'] < event_date]
        if prev_trading_days.empty:
            # 如果事件日前没有交易数据，说明事件在可追溯的历史之前，使用无价格假设的除数因子
            f_t = 1.0 / (1.0 + S + R) if (1.0 + S + R) > 0 else 1.0
        else:
            # 找到最靠近事件日前一天的交易日的索引和收盘价
            prev_idx = prev_trading_days.index[-1]
            P_prev = res_df.loc[prev_idx, 'close']
            
            # 计算这一天的复权因子
            denominator = P_prev * (1.0 + S + R)
            if denominator > 0:
                f_t = (P_prev - D + A * R) / denominator
            else:
                f_t = 1.0
                
        # 将事件日之前的所有交易日的复权因子乘上 f_t
        # 这就实现了累积乘积的效果
        res_df.loc[res_df['date'] < event_date, 'factor'] *= f_t
        
    # 根据累积复权因子计算复权价格和成交量
    for col in ['open', 'high', 'low', 'close']:
        res_df[f'{col}_adj'] = (res_df[col] * res_df['factor']).round(2)
        
    # 成交量需要除以复权因子以保持金额比例
    res_df['volume_adj'] = (res_df['volume'] / res_df['factor']).round(0)
    
    return res_df
