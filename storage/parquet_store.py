import os
import pandas as pd

def save_to_parquet(df: pd.DataFrame, data_dir: str) -> str:
    """
    将处理好（含复权数据）的股票日K线 DataFrame 写入本地 Parquet 文件中。
    
    文件名采用格式: {code}.parquet (例如 sh600000.parquet)
    """
    if df.empty:
        return ""
        
    if not os.path.exists(data_dir):
        os.makedirs(data_dir, exist_ok=True)
        
    code = df['code'].iloc[0]
    filepath = os.path.join(data_dir, f"{code}.parquet")
    
    # 写入 Parquet，使用 Snappy 压缩算法（速度最快，压缩比优异）
    df.to_parquet(filepath, index=False, compression='snappy', engine='pyarrow')
    return filepath

def load_from_parquet(code: str, data_dir: str, columns: list = None) -> pd.DataFrame:
    """
    极速从本地 Parquet 文件夹加载指定股票的数据。
    
    支持可选参数 `columns`，仅读取需要的列以达到极致的磁盘 I/O 速度。
    """
    filepath = os.path.join(data_dir, f"{code}.parquet")
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"未找到股票 {code} 的 Parquet 缓存文件，请先同步数据！")
        
    # 直接读取指定的列，零冗余磁盘读写
    return pd.read_parquet(filepath, columns=columns, engine='pyarrow')
