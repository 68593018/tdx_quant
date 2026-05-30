import os
import re
import sys
import glob
import pandas as pd
import numpy as np
import json

# Add project path to sys.path
sys.path.append("/mnt/e/agy-workspace/tdx_quant")

from server import load_tdx_dir, load_stock_names

DATA_DIR = "/mnt/e/agy-workspace/tdx_quant/data"
CONFIG_PATH = "/mnt/e/agy-workspace/tdx_quant/config.json"

def load_gbbq_shares():
    from parser.gbbq import parse_tdx_gbbq_file
    tdx_dir = load_tdx_dir()
    gbbq_path = os.path.join(tdx_dir, "T0002", "hq_cache", "gbbq")
    if not os.path.exists(gbbq_path):
        print(f"⚠️ 警告: GBBQ 股本文件不存在于 {gbbq_path}！将无法计算历史精确市值。")
        return None
        
    try:
        gbbq_df = parse_tdx_gbbq_file(gbbq_path)
        shares_df = gbbq_df[gbbq_df['category'] != 1].copy()
        shares_df['date'] = pd.to_datetime(shares_df['date'])
        shares_df.sort_values(by=['code', 'date'], inplace=True)
        
        gbbq_map = {}
        for code, group in shares_df.groupby('code'):
            gbbq_map[code] = list(zip(group['date'], group['allocated_ratio']))
        return gbbq_map
    except Exception as e:
        print(f"❌ 解析 GBBQ 失败: {e}")
        return None

def get_float_market_cap(gbbq_shares, symbol, date_dt, close_price):
    code = symbol[-6:]
    if gbbq_shares and code in gbbq_shares:
        records = gbbq_shares[code]
        float_shares_wan = None
        for r_date, r_shares in records:
            if r_date <= date_dt:
                float_shares_wan = r_shares
            else:
                break
        
        if float_shares_wan is not None and float_shares_wan > 0:
            return float_shares_wan * 10000.0 * close_price
    return None

def main():
    print("🚀 启动「10倍黑马股基因图谱」全市场智能筛查引擎...")
    
    print("💡 第一步：加载 GBBQ 股本变更历史与名称映射...")
    gbbq_shares = load_gbbq_shares()
    if not gbbq_shares:
        print("❌ 无法加载股本权息数据库，将影响市值精准过滤！")
        return

    tdx_dir = load_tdx_dir()
    names_map = load_stock_names(tdx_dir)
    
    # 获取所有股票的 parquet 文件
    files = glob.glob(os.path.join(DATA_DIR, "*.parquet"))
    stock_files = []
    for f in files:
        basename = os.path.basename(f)
        code = basename.replace(".parquet", "").lower()
        if (code.startswith("sh60") or code.startswith("sh68") or 
            code.startswith("sz00") or code.startswith("sz30") or 
            code.startswith("bj43") or code.startswith("bj83") or 
            code.startswith("bj87") or code.startswith("bj88") or 
            code.startswith("bj92")):
            stock_files.append((code, f))
            
    print(f"📂 发现待筛查 A 股个股共 {len(stock_files)} 只。开始匹配 DNA 特征...")
    
    candidates = []
    
    for i, (code, filepath) in enumerate(stock_files):
        try:
            df = pd.read_parquet(filepath)
            if df.empty or len(df) < 250: # 过滤新上市不足1年的个股
                continue
                
            df['date'] = pd.to_datetime(df['date'])
            df.sort_values('date', inplace=True)
            df.reset_index(drop=True, inplace=True)
            
            latest = df.iloc[-1]
            latest_date = latest['date']
            latest_close = latest['close']
            latest_close_adj = latest['close_adj']
            
            # 1. 黄金市值过滤 (8亿 ~ 30亿之间)
            mcap = get_float_market_cap(gbbq_shares, code, latest_date, latest_close)
            if not mcap or not (8e8 <= mcap <= 3.0e9):
                continue
                
            # 2. 价格极度超跌底分型判断 (相比3年内最高复权价回撤大于 60%)
            # 3年交易日大约为 750 天
            max_price_3y = df['close_adj'].tail(750).max()
            if max_price_3y <= 0:
                continue
            drawdown = (max_price_3y - latest_close_adj) / max_price_3y
            if drawdown < 0.60:
                continue
                
            # 3. 月级异常放量筑底 (最近20个交易日成交量是前12个月均值成交量的 1.5 倍以上)
            recent_vol = df['volume'].tail(20).sum()
            historical_vol_avg_20d = df['volume'].tail(250).mean() * 20
            if historical_vol_avg_20d <= 0:
                continue
            vol_ratio = recent_vol / historical_vol_avg_20d
            if vol_ratio < 1.5:
                continue
                
            # 4. 中长期趋势突破确立 (当前股价站上 120 日/半年均线)
            ma120 = df['close_adj'].tail(120).mean()
            if latest_close_adj < ma120:
                continue
                
            name = names_map.get(code, "未知个股")
            
            # 计算目前距离 3 年最低复权收盘价的比率 (表明是否刚脱离底部)
            min_price_3y = df['close_adj'].tail(750).min()
            from_bottom_ratio = (latest_close_adj / min_price_3y) - 1.0 if min_price_3y > 0 else 0.0
            
            candidates.append({
                "代码": code.upper(),
                "名称": name,
                "当前价格": round(float(latest_close), 2),
                "流通市值(亿)": round(mcap / 1e8, 2),
                "三年回撤(%)": round(drawdown * 100, 2),
                "量能异常倍数": round(vol_ratio, 2),
                "脱离底部幅度(%)": round(from_bottom_ratio * 100, 2),
                "最近更新": latest_date.strftime('%Y-%m-%d')
            })
        except Exception as e:
            pass

    candidates_df = pd.DataFrame(candidates)
    if candidates_df.empty:
        print("\nℹ️ 筛查结束！当前市场中未发现完全满足“10倍股启动基因”的候选个股。")
        return
        
    # 按照市值从小到大排序（市值越小弹性越大）
    candidates_df.sort_values(by="流通市值(亿)", inplace=True)
    
    print(f"\n🏆 筛查成功！共发现 {len(candidates_df)} 只符合「10倍股黄金启动基因」的候选个股池：")
    print(candidates_df.to_string(index=False))
    
    # 自动保存一份精美的 Markdown 筛查战报到 report 文件夹下
    report_path = "/mnt/e/agy-workspace/tdx_quant/report/next_10x_candidates.md"
    try:
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("# 🎯 A股「下一个10倍股」量化智能筛查战报\n\n")
            f.write("本战报采用基于历史 **636 只 2年10倍股** 提炼出的黄金量化基因模型，对全市场进行穿透筛查。只保留同时满足以下条件的“黑马种子公司”：\n\n")
            f.write("> **筛查因子口径（DNA Filter）**\n")
            f.write("> 1. **黄金市值**：流通市值处于 **8 亿至 30 亿** 之间（最易控盘拉升的弹性温床）。\n")
            f.write("> 2. **极致出清**：当前价格相较 3 年内最高价**回撤超过 60%**（泡沫已彻底挤干，估值历史大底）。\n")
            f.write("> 3. **主力抢筹**：最近 20 天成交量较过去 1 年均值**放大 1.5 倍以上**（有大资金底部异动建仓）。\n")
            f.write("> 4. **右侧确立**：当前价格成功**站上 120 日中长期生命线**（右侧趋势突破确立）。\n\n")
            f.write(f"### 🏆 今日筛查结果（共 **{len(candidates_df)}** 只候选个股，按市值从小到大排序）：\n\n")
            f.write(candidates_df.to_markdown(index=False))
            f.write("\n\n---\n")
            f.write("> **💡 操盘手量化建议**：\n")
            f.write("> 1. **市值排序优先**：市值越小越容易被资金抱团，通常 10~15 亿的个股弹性最为狂暴。\n")
            f.write("> 2. **脱离底部幅度**：重点关注脱离底部幅度在 **10% ~ 30%** 之间的个股，这表明它们刚刚启动突破均线，风险回报比极高。\n")
            f.write("> 3. **题材共鸣筛查**：拿到此名册后，请重点对照**AI 算力替代、低空经济、人形机器人及特种精细化工**四大主线。凡是题材与此量化异动发生重合的个股，将是“下下一个10倍股”极高概率的火种！\n")
        print(f"\n💾 精美量化筛查战报已保存至本地: {report_path}")
    except Exception as e:
        print(f"❌ 保存战报失败: {e}")

if __name__ == "__main__":
    main()
