import os
import sys

sys.path.insert(0, '/home/liliiflora/work/wsl-agy-projects/tdx_quant')
from parser.gbbq import parse_tdx_gbbq_file

GBBQ_PATH = '/mnt/e/Tools/tdx/T0002/hq_cache/gbbq'
gbbq_df = parse_tdx_gbbq_file(GBBQ_PATH)

# 过滤出 sh600000 的除权除息事件
stock_code = '600000'
events = gbbq_df[(gbbq_df['code'] == stock_code) & (gbbq_df['category'] == 1)]

print(f"=== GBBQ category==1 events for {stock_code} ===")
print(events[['date', 'category', 'dividend', 'allocated_price', 'split_ratio', 'allocated_ratio']])
