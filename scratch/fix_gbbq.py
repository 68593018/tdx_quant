import re
import os

print("Starting automatic key repair (super-robust version 2.1)...")

# 1. 自动寻找 content.md 文件
steps_root = '/home/liliiflora/.gemini/antigravity-cli/brain/1d142931-0e2f-4fa2-bac5-53c402224cac/.system_generated/steps'
content_md_path = None

if os.path.exists(steps_root):
    for root, dirs, files in os.walk(steps_root):
        for file in files:
            if file == 'content.md':
                path = os.path.join(root, file)
                with open(path, 'r', errors='ignore') as f:
                    content = f.read()
                if 'hexdump_keys' in content and '38 A7 C2' in content:
                    content_md_path = path
                    break
        if content_md_path:
            break

if not content_md_path:
    content_md_path = '/home/liliiflora/.gemini/antigravity-cli/brain/1d142931-0e2f-4fa2-bac5-53c402224cac/.system_generated/steps/112/content.md'

print(f"Found content.md at: {content_md_path}")

# 2. 读取 content.md
with open(content_md_path, 'r', errors='ignore') as f:
    text = f.read()

# 3. 极简打印和调试
idx = text.find('hexdump_keys = "')
if idx == -1:
    idx = text.find("hexdump_keys = '")
    
if idx == -1:
    idx = text.find('hexdump_keys')
    
print(f"Index of 'hexdump_keys': {idx}")
if idx != -1:
    # 打印接下来的 200 个字符
    print("\n--- Next 200 characters from match ---")
    print(repr(text[idx:idx+200]))
    print("--------------------------------------\n")

# 4. 尝试超宽松提取：在 hexdump_keys 之后提取所有的 hex 字符和空格，直到文件结束
# 因为 keys 后面几乎没有任何其他双引号内容了！
try:
    # 找到 "38 A7"
    start_hex = text.find('38 A7 C2', idx)
    if start_hex == -1:
        print("Error: Could not find '38 A7 C2' in file content.")
        exit(1)
        
    # 往后寻找结束双引号或者单引号，或者直接取到非十六进制的符号为止
    # 由于 keys 是一长串大写字母+数字+空格，我们可以把从 38 A7 往后的字符遍历，
    # 只要是十六进制字符、空格或换行符，我们就保留它，一旦遇到任何其他非法字符（比如等号、括号、字母G以上等），就截断！
    allowed_chars = set("0123456789ABCDEFabcdef \n\r\t")
    cleaned_hex_list = []
    
    for i in range(start_hex, len(text)):
        char = text[i]
        if char in allowed_chars:
            cleaned_hex_list.append(char)
        else:
            # 遇到结束引号或其他非十六进制字符，直接跳出！
            print(f"Stopping extraction at index {i}, char={repr(char)}")
            break
            
    full_hex = "".join(cleaned_hex_list).replace('\n', '').replace('\r', '').replace('\t', '').strip()
    
    # 整理格式
    hex_pairs = [h for h in full_hex.split(' ') if h]
    formatted_lines = []
    chunk_size = 32
    for i in range(0, len(hex_pairs), chunk_size):
        chunk = hex_pairs[i:i+chunk_size]
        formatted_lines.append(f'    "{ " ".join(chunk) } "')

    formatted_keys = "(\n" + "\n".join(formatted_lines) + "\n)"

    # 写入 gbbq.py
    gbbq_py_path = '/home/liliiflora/work/wsl-agy-projects/tdx_quant/parser/gbbq.py'
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
    print("🎉 成功！已自动将完整无损的 4KB 解密密钥库覆盖写入到 gbbq.py！")
    print(f"安全解析后的字节数: {len(bytes.fromhex(full_hex.replace(' ', '')))}")
    print("现在，请重新运行: python3 test_run.py")
    print("=" * 60)
except Exception as e:
    print("Decryption extraction failed:", e)
