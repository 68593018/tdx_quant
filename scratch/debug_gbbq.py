import re
import struct
from ctypes import c_uint32
import pandas as pd
import os

print("Starting GBBQ decryption debugger (super-robust version)...")

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

# 3. 稳健定位 hexdump_keys
idx = text.find('hexdump_keys = "')
if idx == -1:
    idx = text.find("hexdump_keys = '")
if idx == -1:
    idx = text.find('hexdump_keys')

if idx == -1:
    print("Error: hexdump_keys not found")
    exit(1)

start_hex = text.find('38 A7 C2', idx)
if start_hex == -1:
    print("Error: '38 A7 C2' not found")
    exit(1)

# 扫取所有 hex 字符
allowed_chars = set("0123456789ABCDEFabcdef \n\r\t")
cleaned_hex_list = []
for i in range(start_hex, len(text)):
    char = text[i]
    if char in allowed_chars:
        cleaned_hex_list.append(char)
    else:
        break

full_hex = "".join(cleaned_hex_list).replace('\n', '').replace('\r', '').replace('\t', '').strip()
bin_keys = bytes.fromhex(full_hex.replace(' ', ''))
print("Keys binary length:", len(bin_keys))

# 4. 尝试解密 GBBQ
gbbq_path = '/mnt/e/Tools/tdx/T0002/hq_cache/gbbq'
print("GBBQ file size:", os.path.getsize(gbbq_path))

results = []
with open(gbbq_path, "rb") as f:
    content = f.read()
    pos = 0
    (count,) = struct.unpack("<I", content[pos:pos+4])
    pos += 4
    print("Record count in file:", count)
    
    encrypt_data = content
    data_offset = pos

    try:
        for r_idx in range(min(count, 10)):  # 仅测试前10条
            clear_data = bytearray()
            for i in range(3):
                (eax,) = struct.unpack("<I", bin_keys[0x44: 0x44 + 4])
                (ebx,) = struct.unpack("<I", encrypt_data[data_offset: data_offset+4])
                num = c_uint32(eax ^ ebx).value
                (numold,) = struct.unpack("<I", encrypt_data[data_offset + 0x4: data_offset + 0x4 + 4])
                
                for j in reversed(range(4, 0x40+4, 4)):
                    ebx = (num & 0xff0000) >> 16
                    idx_keys = ebx * 4 + 0x448
                    if idx_keys + 4 > len(bin_keys):
                        print(f"Index out of bounds at Record {r_idx}, loop i={i}, j={j}!")
                        print(f"ebx = {ebx}, computed index = {idx_keys}, key len = {len(bin_keys)}")
                        raise IndexError("Key index out of bounds")
                        
                    (eax,) = struct.unpack("<I", bin_keys[idx_keys: idx_keys + 4])
                    ebx = num >> 24
                    (eax_add,) = struct.unpack("<I", bin_keys[ebx * 4 + 0x48: ebx * 4 + 0x48 + 4])
                    eax += eax_add
                    eax = c_uint32(eax).value
                    ebx = (num & 0xff00) >> 8
                    (eax_xor,) = struct.unpack("<I", bin_keys[ebx * 4 + 0x848: ebx * 4 + 0x848 + 4])
                    eax ^= eax_xor
                    eax = c_uint32(eax).value
                    ebx = num & 0xff
                    (eax_add,) = struct.unpack("<I", bin_keys[ebx * 4 + 0xC48: ebx * 4 + 0xC48 + 4])
                    eax += eax_add
                    eax = c_uint32(eax).value
                    (eax_xor,) = struct.unpack("<I", bin_keys[j: j + 4])
                    eax ^= eax_xor
                    eax = c_uint32(eax).value
                    ebx = num
                    num = numold ^ eax
                    num = c_uint32(num).value
                    numold = ebx

                (numold_op,) = struct.unpack("<I", bin_keys[0:4])
                numold ^= numold_op
                numold = c_uint32(numold).value
                clear_data.extend(struct.pack("<II", numold, num))
                data_offset += 8

            clear_data.extend(encrypt_data[data_offset: data_offset+5])
            data_offset += 5

            (v1, v2, v3, v4, v5, v6, v7, v8) = struct.unpack("<B7sIBffff", clear_data)
            code_clean = v2.rstrip(b"\x00").decode("utf-8")
            print(f"Record {r_idx} success: code={code_clean}, date={v3}, div={v5}")
            
    except Exception as e:
        print("Error during test decryption:", e)
