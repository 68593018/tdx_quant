import urllib.request
import re
import os

print("Starting automatic key repair from GitHub raw source...")

url = 'https://raw.githubusercontent.com/rainx/pytdx/master/pytdx/reader/gbbq_reader.py'
gbbq_py_path = '/home/liliiflora/work/wsl-agy-projects/tdx_quant/parser/gbbq.py'

try:
    # 1. 直接从 GitHub 下载原始未截断的代码
    print(f"Downloading from {url}...")
    with urllib.request.urlopen(url, timeout=10) as response:
        html = response.read().decode('utf-8')
        
    # 2. 提取完整 4176 字节的 hexdump_keys
    match = re.search(r'hexdump_keys = \x22([^\x22]+)\x22', html)
    if not match:
        # 尝试单引号匹配
        match = re.search(r"hexdump_keys = '([^']+)'", html)
        
    if not match:
        print("Error: Could not extract hexdump_keys from raw web file.")
        exit(1)
        
    full_hex = match.group(1).replace('\n', '').replace('\r', '').strip()
    
    # 格式化密钥
    hex_pairs = [h for h in full_hex.split(' ') if h]
    formatted_lines = []
    chunk_size = 32
    for i in range(0, len(hex_pairs), chunk_size):
        chunk = hex_pairs[i:i+chunk_size]
        formatted_lines.append(f'    "{ " ".join(chunk) } "')

    formatted_keys = "(\n" + "\n".join(formatted_lines) + "\n)"

    # 3. 覆盖写入到 gbbq.py 中
    with open(gbbq_py_path, 'r') as f:
        gbbq_content = f.read()

    new_gbbq_content = re.sub(
        r'GBBQ_HEX_KEYS = \([\s\S]+?\n\)',
        f'GBBQ_HEX_KEYS = {formatted_keys}',
        gbbq_content
    )

    with open(gbbq_py_path, 'w') as f:
        f.write(new_gbbq_content)

    print("=" * 60)
    print("🎉 成功！已自动从 GitHub 提取完整无损的 4176 字节解密密钥库覆盖写入到 gbbq.py！")
    print(f"密钥字符长度: {len(full_hex)} | 解析后的字节数: {len(bytes.fromhex(full_hex.replace(' ', '')))}")
    print("现在，请重新运行: python3 test_run.py")
    print("=" * 60)
    
except Exception as e:
    print("Error during GBBQ web fix:", e)
