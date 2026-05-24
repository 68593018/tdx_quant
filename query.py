import os
import sys
import pandas as pd

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_STORE_DIR = os.path.join(CURRENT_DIR, "data")

def print_help():
    print("=" * 75)
    print("        通达信本地 Parquet 数据池 - 命令行极速查询工具 (Phase 3)")
    print("=" * 75)
    print("使用方法:")
    print("  1. 查询个股/指数/板块指数K线数据:")
    print("     python3 query.py <代码或六位数字> [显示行数]")
    print("     示例:")
    print("       python3 query.py sh600000      (查询浦发银行，默认显示最新10行)")
    print("       python3 query.py 000002 20     (查询万科A，显示最新20行)")
    print("       python3 query.py sh000001      (查询上证指数)")
    print("       python3 query.py sh881395      (查询半导体板块指数)")
    print("\n  2. 模糊查询板块名称:")
    print("     python3 query.py <板块部分名称>")
    print("     示例:")
    print("       python3 query.py 半导体        (查询含有'半导体'的概念/风格/指数板块)")
    print("       python3 query.py 芯片")
    print("\n  3. 查询具体板块下的所有成分股:")
    print("     python3 query.py <板块完整名称>")
    print("     示例:")
    print("       python3 query.py 芯片概念      (列出'芯片概念'板块下的所有股票代码)")
    print("\n  4. 列出某一类别的所有板块名称:")
    print("     python3 query.py <category>")
    print("     可用类别: concept (概念), style (风格), index (指数)")
    print("     示例:")
    print("       python3 query.py concept")
    print("=" * 75)

def main():
    if len(sys.argv) < 2 or sys.argv[1] in ['-h', '--help', 'help']:
        print_help()
        return

    raw_input = sys.argv[1].strip()
    raw_code = raw_input.lower()
    
    # 默认显示 10 行，若传入第二个参数则解析为行数限制
    limit = 10
    if len(sys.argv) >= 3:
        try:
            limit = int(sys.argv[2])
        except ValueError:
            pass

    # 加载板块全局映射数据以供查询和展现
    block_parquet_path = os.path.join(DATA_STORE_DIR, "block_mappings.parquet")
    block_df = None
    if os.path.exists(block_parquet_path):
        try:
            block_df = pd.read_parquet(block_parquet_path)
        except Exception as e:
            print(f"⚠️ 警告: 加载板块映射数据失败: {e}")

    # =============================================================
    # 分支一: 板块模糊查询/类别查询/成分股列表查询
    # =============================================================
    if block_df is not None:
        # 如果不是纯数字，且不以 sh/sz/bj 开头跟着数字，就很有可能是板块查询
        is_stock_input = (raw_code.isdigit() and len(raw_code) == 6) or \
                         ((raw_code.startswith('sh') or raw_code.startswith('sz') or raw_code.startswith('bj')) and \
                          len(raw_code) >= 8 and raw_code[2:].isdigit())
                          
        if not is_stock_input:
            # 1. 检查是否是类别查询 (concept / style / index)
            cat_map = {
                'concept': 'concept', '概念': 'concept',
                'style': 'style', '风格': 'style',
                'index': 'index', '指数': 'index'
            }
            if raw_code in cat_map:
                cat = cat_map[raw_code]
                label = {'concept': '概念', 'style': '风格', 'index': '指数'}[cat]
                unique_blocks = block_df[block_df['block_category'] == cat]['block_name'].unique()
                print("=" * 75)
                print(f"       📂 【{label.upper()}】类别下的所有板块 (共 {len(unique_blocks)} 个):")
                print("=" * 75)
                # 每行打印 4 个，左对齐，保持美观
                for i in range(0, len(unique_blocks), 4):
                    print(" | ".join(f"{b:<14}" for b in unique_blocks[i:i+4]))
                print("=" * 75)
                return

            # 2. 模糊匹配板块名称
            # 过滤出包含输入字样的板块
            matched_records = block_df[block_df['block_name'].str.contains(raw_input, case=False, na=False)]
            matched_blocks = matched_records['block_name'].unique()
            
            if len(matched_blocks) > 0:
                # 检查是否有完全一致的精确匹配
                exact_matches = [b for b in matched_blocks if b == raw_input]
                
                if len(exact_matches) == 1 or len(matched_blocks) == 1:
                    # 精确匹配到了一个板块，直接打印成分股
                    b_name = exact_matches[0] if exact_matches else matched_blocks[0]
                    stocks_in_block = block_df[block_df['block_name'] == b_name]
                    print("=" * 75)
                    print(f"📂 板块 【{b_name}】 下的成分股列表 (共 {len(stocks_in_block)} 只):")
                    print("=" * 75)
                    # 组合成 SH600000 形式
                    stock_list = [f"{row['market'].upper()}{row['code']}" for _, row in stocks_in_block.iterrows()]
                    # 每行打印 6 个
                    for i in range(0, len(stock_list), 6):
                        print("  ".join(stock_list[i:i+6]))
                    print("=" * 75)
                    return
                else:
                    # 匹配到了多个相关的板块，列出来让用户选择
                    print("=" * 75)
                    print(f"🔍 匹配到以下 {len(matched_blocks)} 个相关板块:")
                    print("=" * 75)
                    for i in range(0, len(matched_blocks), 4):
                        print(" | ".join(f"{b:<14}" for b in matched_blocks[i:i+4]))
                    print("-" * 75)
                    print(f"提示: 请输入完整的板块名称，如 'python3 query.py {matched_blocks[0]}' 即可查询其名下的所有股票。")
                    print("=" * 75)
                    return

    # =============================================================
    # 分支二: 个股 K 线数据及关联板块展示
    # =============================================================
    # 1. 股票代码智能补全逻辑
    filepath = None
    filename = ""
    
    if (raw_code.startswith("sh") or raw_code.startswith("sz") or raw_code.startswith("bj")) and len(raw_code) >= 8:
        filename = f"{raw_code}.parquet"
        filepath = os.path.join(DATA_STORE_DIR, filename)
    else:
        if len(raw_code) == 6 and raw_code.isdigit():
            for prefix in ["sh", "sz", "bj"]:
                check_path = os.path.join(DATA_STORE_DIR, f"{prefix}{raw_code}.parquet")
                if os.path.exists(check_path):
                    filepath = check_path
                    filename = f"{prefix}{raw_code}.parquet"
                    break
        elif len(raw_code) == 6 and (raw_code.startswith("880") or raw_code.startswith("881")):
            for prefix in ["sh", "sz"]:
                check_path = os.path.join(DATA_STORE_DIR, f"{prefix}{raw_code}.parquet")
                if os.path.exists(check_path):
                    filepath = check_path
                    filename = f"{prefix}{raw_code}.parquet"
                    break

    if not filepath or not os.path.exists(filepath):
        print(f"❌ 错误: 未在本地数据库中找到标的【{raw_input}】的 Parquet 缓存，也未匹配到任何板块。")
        print(f"提示: 请确认代码或板块名是否输入正确。若是新股票，请运行 python3 sync_market.py 完成同步。")
        return

    # 2. 读取并展示股票 K 线数据
    code = filename.split('.')[0]
    print(f"\n📂 正在加载标的 【{code.upper()}】 的前复权日K线数据...")
    
    try:
        df = pd.read_parquet(filepath)
        
        # 格式化日期显示
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        total_rows = len(df)
        start_date = df['date'].iloc[0]
        end_date = df['date'].iloc[-1]
        
        print(f"✅ 加载成功！历史交易日共: {total_rows} 天 | 时间跨度: {start_date} 至 {end_date}")
        
        # -------------------------------------------------------------
        # 展示股票归属的板块信息 (Phase 3 核心亮点)
        # -------------------------------------------------------------
        if block_df is not None:
            # 提取 6 位代码
            code_digits = code[2:] if len(code) >= 8 else code
            my_blocks = block_df[block_df['code'] == code_digits]
            if not my_blocks.empty:
                print("=" * 115)
                print("所属板块信息:")
                # 按 category 分类展示
                for cat, label in [('concept', '概念板块'), ('style', '风格板块'), ('index', '指数板块')]:
                    cat_blocks = my_blocks[my_blocks['block_category'] == cat]['block_name'].unique()
                    if len(cat_blocks) > 0:
                        # 换行排版，多于 6 个板块时折行显示
                        blocks_str = ""
                        for idx, b in enumerate(cat_blocks):
                            if idx > 0 and idx % 7 == 0:
                                blocks_str += f"\n              {b}"
                            else:
                                blocks_str += (", " if idx > 0 else "") + b
                        print(f"  🔹 {label}: {blocks_str}")
        
        print("=" * 115)
        print(f"显示最新 {min(limit, total_rows)} 行日K线数据:")
        print("=" * 115)
        
        # 格式化 DataFrame 以供美观显示
        show_cols = ['date', 'open', 'close', 'open_adj', 'close_adj', 'factor', 'volume', 'volume_adj']
        tail_df = df[show_cols].tail(limit).copy()
        
        # 调整格式方便对齐
        tail_df['volume'] = tail_df['volume'].map(lambda x: f"{x:,.0f}")
        tail_df['volume_adj'] = tail_df['volume_adj'].map(lambda x: f"{x:,.0f}" if pd.notnull(x) else "N/A")
        tail_df['factor'] = tail_df['factor'].map(lambda x: f"{x:.6f}")
        
        tail_df.columns = ['Date', 'Open', 'Close', 'Open_Adj', 'Close_Adj', 'Factor', 'Volume', 'Volume_Adj']
        
        pd.set_option('display.width', 1000)
        pd.set_option('display.colheader_justify', 'center')
        print(tail_df.to_string(index=False))
        print("=" * 115)
        
    except Exception as e:
        print(f"❌ 读取日线数据失败: {e}")

if __name__ == "__main__":
    main()
