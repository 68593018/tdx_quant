import os
import struct

filepath = '/mnt/e/Tools/tdx/T0002/hq_cache/block_gn.dat'
with open(filepath, 'rb') as f:
    data = f.read()

print("File size:", len(data))
print("Header (0-384) sample (last 64 bytes):")
print(data[320:384].hex())

print("\nBytes from 384 to 450:")
print(data[384:450].hex())

# Let's inspect Block 1 around offset 3199 (386 + 2813)
pos = 386 + 2813
print(f"\nExpected Block 1 start at {pos}:")
print(data[pos:pos+50].hex())
try:
    name_raw = data[pos:pos+9]
    name = name_raw.decode('gbk', 'ignore').strip('\x00')
    stock_count, block_type = struct.unpack("<HH", data[pos+9:pos+13])
    print(f"Parsed Block 1: name='{name}', stock_count={stock_count}, type={block_type}")
except Exception as e:
    print("Failed to parse Block 1:", e)

# Let's search for GBK Chinese words around 3199
for offset in range(pos - 10, pos + 50):
    try:
        sub = data[offset:offset+15]
        decoded = sub.decode('gbk', 'ignore').strip('\x00')
        if any(u'\u4e00' <= char <= u'\u9fff' for char in decoded):
            print(f"Found Chinese chars at offset {offset}: '{decoded}' -> hex: {sub.hex()}")
    except:
        pass
