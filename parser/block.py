import os
import struct
import pandas as pd

def parse_stock_code(one_code: str) -> tuple[str, str]:
    """
    Parse TDX raw stock code string to get market and standard 6-digit code.
    TDX sometimes prefixes the code with market indicators ('0' for sz, '1' for sh).
    """
    if len(one_code) >= 7 and one_code[0] in ('0', '1'):
        market = 'sz' if one_code[0] == '0' else 'sh'
        code = one_code[1:7]
    else:
        code = one_code[:6]
        if code.startswith(('6', '900', '688', '600', '601', '603', '605')):
            market = 'sh'
        elif code.startswith(('00', '30', '200', '002', '300', '000', '001', '003')):
            market = 'sz'
        elif code.startswith(('43', '83', '87', '88')):
            market = 'bj'
        else:
            market = 'unknown'
    return market, code

def parse_tdx_block_file(filepath: str, block_type_name: str) -> pd.DataFrame:
    """
    Parse a TDX binary block file (e.g., block_gn.dat, block_fg.dat, block_zs.dat)
    Each block in the file occupies exactly 2813 bytes.
    """
    if not os.path.exists(filepath):
        print(f"警告: 板块文件不存在: {filepath}")
        return pd.DataFrame()
        
    with open(filepath, "rb") as f:
        data = f.read()
        
    pos = 384
    if len(data) < pos + 2:
        return pd.DataFrame()
        
    (num,) = struct.unpack("<H", data[pos: pos+2])
    pos += 2
    
    results = []
    block_size = 2813
    
    for i in range(num):
        block_start = pos + i * block_size
        if block_start + 13 > len(data):
            break
            
        blockname_raw = data[block_start : block_start + 9]
        blockname = blockname_raw.decode("gbk", "ignore").rstrip("\x00").strip()
        
        if not blockname:
            continue
            
        stock_count, block_type = struct.unpack("<HH", data[block_start + 9 : block_start + 13])
        
        # Read stock codes
        stock_pos = block_start + 13
        for j in range(stock_count):
            offset = stock_pos + j * 7
            if offset + 7 > len(data):
                break
            code_raw = data[offset : offset + 7]
            one_code = code_raw.decode("utf-8", "ignore").rstrip("\x00").strip()
            
            if not one_code:
                continue
                
            market, code = parse_stock_code(one_code)
            results.append({
                'block_name': blockname,
                'block_type_id': block_type,
                'block_category': block_type_name,
                'code': code,
                'market': market
            })
            
    return pd.DataFrame(results)

def sync_all_blocks(hq_cache_dir: str, output_parquet_path: str) -> pd.DataFrame:
    """
    Sync concepts, styles and index blocks into a unified Parquet mapping store.
    """
    files = {
        'concept': os.path.join(hq_cache_dir, 'block_gn.dat'),
        'style': os.path.join(hq_cache_dir, 'block_fg.dat'),
        'index': os.path.join(hq_cache_dir, 'block_zs.dat')
    }
    
    dfs = []
    for category, path in files.items():
        if os.path.exists(path):
            print(f"正在解析 {category} 板块文件: {path} ...")
            df = parse_tdx_block_file(path, category)
            if not df.empty:
                print(f"成功解析 {category}，包含 {df['block_name'].nunique()} 个板块，共 {len(df)} 条股票映射关系。")
                dfs.append(df)
                
    if not dfs:
        print("未找到任何可解析的板块文件。")
        return pd.DataFrame()
        
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Save to Parquet
    os.makedirs(os.path.dirname(output_parquet_path), exist_ok=True)
    combined_df.to_parquet(output_parquet_path, index=False, compression='snappy')
    print(f"已成功将全量板块映射数据保存至: {output_parquet_path} (共 {len(combined_df)} 条记录)")
    
    return combined_df
