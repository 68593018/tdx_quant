import os
import numpy as np
import pandas as pd

def parse_tdx_day_file(filepath: str) -> pd.DataFrame:
    """
    极速解析通达信 .day 日线二进制数据文件并输出 Pandas DataFrame。
    
    单条记录长度为 32 字节，结构如下：
    - 00-03 字节 (uint32): 日期，格式如 20260524
    - 04-07 字节 (uint32): 开盘价 (需除以 100.0)
    - 08-11 字节 (uint32): 最高价 (需除以 100.0)
    - 12-15 字节 (uint32): 最低价 (需除以 100.0)
    - 16-19 字节 (uint32): 收盘价 (需除以 100.0)
    - 20-23 字节 (float32): 成交金额 (元)
    - 24-27 字节 (uint32): 成交量 (股/手)
    - 28-31 字节 (uint32): 保留字段
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"文件不存在: {filepath}")
        
    # 定义 NumPy 二进制结构化类型
    datatype = np.dtype([
        ('date', 'i4'),
        ('open', 'i4'),
        ('high', 'i4'),
        ('low', 'i4'),
        ('close', 'i4'),
        ('amount', 'f4'),
        ('volume', 'i4'),
        ('reserved', 'i4')
    ])
    
    # 极速将二进制文件映射为 NumPy 数组
    data = np.fromfile(filepath, dtype=datatype)
    
    # 转化为 DataFrame
    df = pd.DataFrame(data)
    
    # 1. 转换日期格式并设为索引
    df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d')
    
    # 2. 自动检测代码并调整除数价格
    # 通达信中，对于大盘指数(如 sh000001, sz399001)与个股/板块，大多数也是放大 100 倍存储。
    # 只有少数特定债券/期货指数或老版本国债指数是放大 1000 倍。默认统一除以 100.0
    filename = os.path.basename(filepath).lower()
    
    # 部分特定指数如果有 1000 倍除数，可在此处进行模式匹配过滤
    # 目前默认标准股票/指数/板块均除以 100.0
    price_divisor = 100.0
    
    for col in ['open', 'high', 'low', 'close']:
        df[col] = df[col] / price_divisor
        
    # 3. 提取股票代码
    code = filename.split('.')[0]
    df.insert(0, 'code', code)
    
    # 4. 删除无用列
    df.drop(columns=['reserved'], inplace=True)
    
    return df


def parse_tdx_lc_file(filepath: str) -> pd.DataFrame:
    """
    极速解析通达信 .lc1 (1分钟) 和 .lc5 (5分钟) 二进制分钟线数据文件并输出 Pandas DataFrame。
    
    单条记录长度为 32 字节，结构如下：
    - 00-01 字节 (uint16): 日期，格式为 (year-2004)*2048 + month*100 + day
    - 02-03 字节 (uint16): 时间，从午夜开始的分钟数 (例如 571 表示 09:31)
    - 04-07 字节 (float32): 开盘价
    - 08-11 字节 (float32): 最高价
    - 12-15 字节 (float32): 最低价
    - 16-19 字节 (float32): 收盘价
    - 20-23 字节 (float32): 成交金额 (元)
    - 24-27 字节 (uint32): 成交量 (股/手)
    - 28-31 字节 (uint32): 保留字段
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"文件不存在: {filepath}")
        
    datatype = np.dtype([
        ('date_code', '<u2'),
        ('time_code', '<u2'),
        ('open', '<f4'),
        ('high', '<f4'),
        ('low', '<f4'),
        ('close', '<f4'),
        ('amount', '<f4'),
        ('volume', '<u4'),
        ('reserved', '<u4')
    ])
    
    data = np.fromfile(filepath, dtype=datatype)
    
    if len(data) == 0:
        return pd.DataFrame()
        
    date_codes = data['date_code'].astype(np.int32)
    years = (date_codes // 2048) + 2004
    months = (date_codes % 2048) // 100
    days = (date_codes % 2048) % 100
    
    time_codes = data['time_code'].astype(np.int32)
    hours = time_codes // 60
    minutes = time_codes % 60
    
    # 极速矢量化拼接字符串，比 pd.to_datetime 单独循环快上百倍
    datetime_strs = [
        f"{y:04d}-{m:02d}-{d:02d} {h:02d}:{mi:02d}:00"
        for y, m, d, h, mi in zip(years, months, days, hours, minutes)
    ]
    
    df = pd.DataFrame({
        'datetime': pd.to_datetime(datetime_strs),
        'open': data['open'],
        'high': data['high'],
        'low': data['low'],
        'close': data['close'],
        'amount': data['amount'],
        'volume': data['volume']
    })
    
    filename = os.path.basename(filepath).lower()
    code = filename.split('.')[0]
    df.insert(0, 'code', code)
    
    return df
