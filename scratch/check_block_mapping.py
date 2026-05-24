import pandas as pd
import os

path = '/home/liliiflora/work/wsl-agy-projects/tdx_quant/data/block_mappings.parquet'
if os.path.exists(path):
    df = pd.read_parquet(path)
    print("DataFrame shape:", df.shape)
    print("\nColumns:", df.columns)
    print("\nUnique categories:", df['block_category'].unique())
    print("\nSample category 'index':")
    print(df[df['block_category'] == 'index'].head(20))
    print("\nSample category 'concept':")
    print(df[df['block_category'] == 'concept'].head(20))
    
    # Check if there are any codes starting with '88'
    idx_88 = df[df['code'].str.startswith('88', na=False)]
    print("\nNumber of records starting with 88:", len(idx_88))
    if not idx_88.empty:
        print(idx_88.head(10))
else:
    print("File not found")
