import os
import struct
import pandas as pd

def parse_tdx_block_file(filepath: str) -> pd.DataFrame:
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"文件不存在: {filepath}")
        
    with open(filepath, "rb") as f:
        data = f.read()
        
    pos = 384
    if len(data) < pos + 2:
        print("Data is too short:", len(data))
        return pd.DataFrame()
        
    (num,) = struct.unpack("<H", data[pos: pos+2])
    print(f"Data length: {len(data)}, pos 384, number of blocks: {num}")
    pos += 2
    
    results = []
    for i in range(num):
        if pos + 9 + 4 > len(data):
            print(f"Break early at block index {i}, pos {pos}")
            break
            
        blockname_raw = data[pos: pos+9]
        pos += 9
        blockname = blockname_raw.decode("gbk", 'ignore').rstrip("\x00")
        
        stock_count, block_type = struct.unpack("<HH", data[pos: pos+4])
        pos += 4
        
        if i < 3:
            print(f"Block {i}: name={blockname}, stock_count={stock_count}, type={block_type}")
            
        for j in range(stock_count):
            if pos + 7 > len(data):
                break
            code_raw = data[pos: pos+7]
            one_code = code_raw.decode("utf-8", 'ignore').rstrip("\x00")
            pos += 7
            
            if i < 3 and j < 5:
                print(f"  Stock {j} raw: {code_raw}, decoded: '{one_code}', len={len(one_code)}")
                
            if len(one_code) >= 7:
                market = one_code[0]
                code = one_code[1:7]
                results.append({
                    'block_name': blockname,
                    'block_type': block_type,
                    'code': code,
                    'market': market
                })
            elif len(one_code) == 6:
                # Some files might not have market prefix, or maybe it's structured differently?
                results.append({
                    'block_name': blockname,
                    'block_type': block_type,
                    'code': one_code,
                    'market': 'unknown'
                })
                
    return pd.DataFrame(results)

block_path = '/mnt/e/Tools/tdx/T0002/hq_cache/block_gn.dat'
try:
    df = parse_tdx_block_file(block_path)
    print("=== Block Parsing Success ===")
    print("Total rows:", len(df))
    print("Unique sectors found:", df['block_name'].nunique())
    print("\nSample sectors:")
    print(df['block_name'].unique()[:15])
    print("\nSample records:")
    print(df.head(10))
except Exception as e:
    print("Failed:", e)
