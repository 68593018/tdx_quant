import re
import sys

# 1. 读取 gbbq.py 中的键
with open('/home/liliiflora/work/wsl-agy-projects/tdx_quant/parser/gbbq.py', 'r') as f:
    gbbq_py = f.read()

match1 = re.search(r'GBBQ_HEX_KEYS = \(\s+([^\)]+)\)', gbbq_py)
if not match1:
    # 尝试另一种匹配
    match1 = re.search(r'GBBQ_HEX_KEYS = \x22([^\x22]+)\x22', gbbq_py)
    
if match1:
    keys1 = match1.group(1).replace('\n', '').replace(' ', '').replace('"', '').replace('(', '').replace(')', '')
else:
    print("gbbq.py keys not found")
    sys.exit(1)

# 2. 读取 content.md 中的键
with open('/home/liliiflora/.gemini/antigravity-cli/brain/1d142931-0e2f-4fa2-bac5-53c402224cac/.system_generated/steps/112/content.md', 'r') as f:
    content_md = f.read()

match2 = re.search(r'hexdump_keys = \x22([^\x22]+)\x22', content_md)
if match2:
    keys2 = match2.group(1).replace('\n', '').replace(' ', '')
else:
    print("content.md keys not found")
    sys.exit(1)

print("gbbq.py keys length:", len(keys1))
print("content.md keys length:", len(keys2))

if keys1 == keys2:
    print("Keys are identical!")
else:
    print("Keys mismatch!")
    # 找出第一个不匹配的地方
    for i in range(min(len(keys1), len(keys2))):
        if keys1[i] != keys2[i]:
            print(f"Mismatch at char index {i}: gbbq.py={keys1[i]}, content.md={keys2[i]}")
            break
