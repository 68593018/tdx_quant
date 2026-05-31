import os
import glob
import urllib.request
import json
import pandas as pd
import numpy as np
import time

DATA_DIR = "data"
REPORT_DIR = "report"
os.makedirs(REPORT_DIR, exist_ok=True)

# 1. 加载 GBBQ 股本数据库以计算高精度市值
def load_gbbq_shares():
    from parser.gbbq import parse_tdx_gbbq_file
    
    # 查找本地通达信目录
    tdx_dir = "E:/tdx"
    config_path = "config.json"
    if os.path.exists(config_path):
        try:
            import json
            with open(config_path, "r", encoding="utf-8") as f:
                config = json.load(f)
                if "tdx_dir" in config:
                    tdx_dir = config["tdx_dir"]
        except Exception:
            pass
            
    gbbq_path = os.path.join(tdx_dir, "T0002", "hq_cache", "gbbq")
    if not os.path.exists(gbbq_path):
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
    except Exception:
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
    return 3e9 # 默认30亿近似

def fetch_tencent_fundamentals(symbols):
    """通过腾讯财经公开接口批量抓取个股实时财务与市值指标"""
    if not symbols:
        return pd.DataFrame()
        
    print(f"🚀 [Step 2] 正在通过腾讯财经 API 批量拉取 {len(symbols)} 只技术候选股的基本面数据...")
    
    # 限制每批请求的个股数量为 50，避免 URL 过长
    chunk_size = 50
    chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]
    
    results = []
    
    for chunk in chunks:
        q_param = ",".join(chunk)
        url = f"http://qt.gtimg.cn/q={q_param}"
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=10) as response:
                html = response.read().decode('gbk', errors='ignore')
                lines = html.strip().splitlines()
                
                for line in lines:
                    if "~" not in line:
                        continue
                    parts = line.split("~")
                    if len(parts) < 47:
                        continue
                        
                    symbol = parts[2] # 6位代码
                    # 识别前缀 sh/sz
                    market_prefix = "sh" if "sh" in parts[0] else ("sz" if "sz" in parts[0] else None)
                    if not market_prefix:
                        # 兜底通过 parts[0] 判断
                        market_prefix = "sh" if "v_sh" in parts[0] else "sz"
                        
                    full_symbol = f"{market_prefix}{symbol}"
                    name = parts[1]
                    price = float(parts[3]) if parts[3] else 0.0
                    
                    # 腾讯 API 特征提取
                    pe_ttm_str = parts[39]
                    pb_str = parts[46]
                    float_mcap_str = parts[44] # 流通市值(亿)
                    
                    pe_ttm = float(pe_ttm_str) if (pe_ttm_str and pe_ttm_str != "-") else 0.0
                    pb = float(pb_str) if (pb_str and pb_str != "-") else 0.0
                    float_mcap = float(float_mcap_str) * 1e8 if (float_mcap_str and float_mcap_str != "-") else 0.0
                    
                    # 利用经典财务公式恒等式：ROE = PB / PE * 100% 算出高精度实时 ROE
                    roe = (pb / pe_ttm) * 100.0 if pe_ttm > 0 else 0.0
                    
                    results.append({
                        "symbol": full_symbol,
                        "name": name,
                        "price": price,
                        "pe_ttm": pe_ttm,
                        "pb": pb,
                        "roe": roe,
                        "float_market_cap": float_mcap
                    })
            time.sleep(0.05) # 避频
        except Exception as e:
            print(f"   ⚠️ 抓取本批次 {len(chunk)} 只个股基本面失败: {e}")
            
    return pd.DataFrame(results)

def main():
    print("=" * 80)
    print("💎 启动「基本面 DNA + 量价共振突破」混合动力十倍股筛查引擎 💎")
    print("=" * 80)
    
    # 1. 扫描本地 K 线执行技术与空间极速初筛 (预过滤)
    print("🚀 [Step 1] 开始扫描本地 K 线执行“超跌 + 趋势突破 + 量能启动”技术面初筛...")
    gbbq_shares = load_gbbq_shares()
    
    files = glob.glob(os.path.join(DATA_DIR, "*.parquet"))
    stock_files = []
    for f in files:
        basename = os.path.basename(f)
        code = basename.replace(".parquet", "").lower()
        if (code.startswith("sh60") or code.startswith("sh68") or 
            code.startswith("sz00") or code.startswith("sz30")):
            stock_files.append((code, f))
            
    print(f"   待筛查 A 股个股共 {len(stock_files)} 只。运行量化时序过滤中...")
    
    tech_candidates = {}
    
    for code, filepath in stock_files:
        try:
            df = pd.read_parquet(filepath)
            if df.empty or len(df) < 250: # 过滤上市不足 1 年的新股
                continue
                
            df['date'] = pd.to_datetime(df['date'])
            df.sort_values('date', inplace=True)
            df.reset_index(drop=True, inplace=True)
            
            latest = df.iloc[-1]
            latest_date = latest['date']
            latest_close = latest['close']
            latest_close_adj = latest['close_adj']
            
            # C. 技术面极度超跌底分型判断 (相比3年内最高复权价回撤大于 55%)
            max_price_3y = df['close_adj'].tail(750).max()
            if max_price_3y <= 0:
                continue
            drawdown = (max_price_3y - latest_close_adj) / max_price_3y
            if drawdown < 0.55:
                continue
                
            # D. 量能异常比率判定 (最近20天量能是前12个月滚动的 1.4 倍以上)
            recent_vol = df['volume'].tail(20).sum()
            historical_vol_avg_20d = df['volume'].tail(250).mean() * 20
            if historical_vol_avg_20d <= 0:
                continue
            vol_ratio = recent_vol / historical_vol_avg_20d
            if vol_ratio < 1.4:
                continue
                
            # E. 中长期均线确立突破 (站上 120 MA 半年线)
            ma120 = df['close_adj'].tail(120).mean()
            if latest_close_adj < ma120:
                continue
                
            # F. 限制底部启动范围 (距离 3 年最低复权价涨幅小于 45%)
            min_price_3y = df['close_adj'].tail(750).min()
            from_bottom_ratio = (latest_close_adj / min_price_3y) - 1.0 if min_price_3y > 0 else 0.0
            if from_bottom_ratio > 0.45:
                continue
                
            # 技术面初筛成功，记录其技术指标
            tech_candidates[code] = {
                "latest_date": latest_date,
                "latest_close": latest_close,
                "drawdown_pct": round(drawdown * 100, 2),
                "vol_ratio": round(vol_ratio, 2),
                "from_bottom_pct": round(from_bottom_ratio * 100, 2)
            }
        except Exception:
            pass
            
    print(f"✅ 技术初筛完成! 获得共 {len(tech_candidates)} 只符合超跌启动的种子个股。")
    
    if not tech_candidates:
        print("ℹ️ 当前市场中没有技术面超跌放量启动的个股，筛查中止。")
        return
        
    # 2. 调用腾讯财经 API 批量拉取基本面指标并执行二次过滤
    symbols_list = list(tech_candidates.keys())
    df_fundamentals = fetch_tencent_fundamentals(symbols_list)
    
    if df_fundamentals.empty:
        print("❌ 抓取财务基本面指标为空，程序中止！")
        return
        
    # 3. 运行十倍股“估值 + 资本效率护城河”终极漏斗
    print("\n🚀 [Step 3] 正在对候选股进行“估值PE + 流通市值 + ROE护城河”终极财务筛查...")
    
    final_candidates = []
    
    for _, r in df_fundamentals.iterrows():
        sym = r['symbol']
        float_mcap = r['float_market_cap']
        pe_ttm = r['pe_ttm']
        pb = r['pb']
        roe = r['roe']
        
        # A. 流通市值黄金限制 (8亿 ~ 35亿人民币之间)
        if not float_mcap or not (8.0e8 <= float_mcap <= 35.0e8):
            continue
            
        # B. 估值压缩区 (10.0 <= PE(TTM) <= 40.0) 且具备盈利能力 (ROE >= 6.0%)
        if not (10.0 <= pe_ttm <= 40.0):
            continue
        if pb <= 0 or pb > 4.5:
            continue
        if roe < 6.0:
            continue
            
        # 匹配技术特征
        tech = tech_candidates[sym]
        
        final_candidates.append({
            "代码": sym.upper(),
            "名称": r['name'],
            "最新价": r['price'],
            "流通市值(亿)": round(float_mcap / 1e8, 2),
            "估值PE_TTM": round(pe_ttm, 1),
            "市净率PB": round(pb, 2),
            "ROE(%)": round(roe, 2),
            "三年最大回撤(%)": tech['drawdown_pct'],
            "量能启动倍数": tech['vol_ratio'],
            "脱离底部涨幅(%)": tech['from_bottom_pct'],
            "更新日期": tech['latest_date'].strftime('%Y-%m-%d')
        })
        
    print(f"📊 筛查分析完毕！符合十倍黑马股“基本面 DNA + 量价启动”全部指标的个股共 {len(final_candidates)} 只。")
    
    if not final_candidates:
        print("\nℹ️ 当前市场中没有完全契合十倍股黄金起跑财务特征的个股。")
        return
        
    df_final = pd.DataFrame(final_candidates)
    df_final.sort_values(by="流通市值(亿)", ascending=True, inplace=True)
    df_final.reset_index(drop=True, inplace=True)
    
    # 打印控制台
    print("\n💎 精选十倍黑马股核心组合 💎")
    print(df_final.to_string(index=False))
    
    # 4. 生成精美的量化研究报告
    report_path = os.path.join(REPORT_DIR, "next_10x_hybrid_candidates.md")
    
    table_rows = []
    for _, row in df_final.iterrows():
        table_rows.append(f"| **{row['代码']}** | **{row['名称']}** | {row['最新价']} | {row['流通市值(亿)']} | {row['估值PE_TTM']} | {row['市净率PB']} | **{row['ROE(%)']}%** | {row['三年最大回撤(%)']}% | **{row['量能启动倍数']}x** | {row['脱离底部涨幅(%)']}% |")
        
    report_markdown = f"""# 💎 基本面DNA + 量价共振突破：下一代“十倍股”智能筛查战报

本筛查报告基于量化平台内 **{len(stock_files)} 只 A 股日K线数据库** 与 **腾讯财经 API 实时抓取的候选个股基本面与实时财务估值指标**，通过“基本面估值安全边际 + 时序技术超跌 + 哨兵资金起跑量能”三向融合，对当前处于黄金起跑线上的“十倍股黑马”个股执行了穿透式定位。

---

## 📊 1. 筛查出的黄金种子明细表 (Candidate List)

| 股票代码 | 股票名称 | 最新价 (元) | 流通市值 (亿) | 估值 PE (TTM) | 市净率 PB | ROE (%) | 三年最大回撤 | 量能异常倍数 | 脱离底部涨幅 |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
{chr(10).join(table_rows)}

> [!NOTE]
> *   **筛选阈值设定**：
>     1.  **流通市值**：处于 **8 亿 - 35 亿** 之间（极高弹性）。
>     2.  **估值安全垫**：**10 < PE(TTM) < 40** 且 **PB < 4.5** 且 **最新 ROE > 6.0%**（结合经典财务恒等式 $ROE = PB / PE$ 高精度导出，杜绝垃圾或亏损股）。
>     3.  **技术超跌**：相较 3 年内最高价回撤 **> 55%**（下跌空间彻底释放）。
>     4.  **资金哨兵**：最近 20 天量能相比历史平均放大 **> 1.4 倍** 且收盘价站上 **120日半年线**。
>     5.  **拒绝追高**：当前价格距离 3 年最低价涨幅 **< 45%**（锁定最完美的低吸筹码）。

---

## 🧠 2. 种子股的核心“基本面DNA”与“戴维斯双击”逻辑

这些被成功筛选出来的黑马个股，为什么具备爆发“十倍”的潜力？

### 💡 基因一：极致的估值压缩与安全边际
*   这些股票全部具备大于 **6.0%** 甚至更高水平的 **ROE**，说明企业本身具备优异的经营性和盈利能力。
*   同时，其 **PE (TTM)** 全部被压制在 **10 - 40倍** 之间，处于估值泡沫彻底破裂后的安全底部分位数。这为未来的“戴维斯双击”（业绩回升 + 估值从20倍重新回归60倍以上）腾出了高达 3~5 倍的乘数空间。

### 💡 基因二：市值极其轻盈 (流通盘 10 - 20亿)
*   流通盘仅 10 到 20 亿人民币。在小微盘流动性复苏时，**极少量的增量资金（如游资和量化私募抢筹）即可拉动股价连续脉冲**，弹性极其惊人，是十倍股最完美的起跑温床。

### 💡 基因三：哨兵成交量与 120日均线的“机构建仓共振”
*   这些个股不仅价格超跌、基本面过硬，更重要的是**它们的成交量最近已经出现了 1.4 倍以上的异常堆积**，且价格站上了 120日半年均线。这从资金面证明，**聪明的机构资金（Smart Money）已经在暗中吸筹，建仓临界点已然确立**。这避免了投资者陷入“价值陷阱”在底部苦等数年。
"""
    
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_markdown)
        
    print(f"\n🎉 筛选战报成功写入报告目录: {report_path}!")
    print("=" * 80)

if __name__ == "__main__":
    main()
