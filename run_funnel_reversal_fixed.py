import os
import glob
import duckdb
import pandas as pd
import numpy as np
import time
from datetime import datetime

# Configure directories
DATA_DIR = "data"
REPORT_DIR = "report"
os.makedirs(REPORT_DIR, exist_ok=True)

# 1. 连接 DuckDB 提取全市场 A 股周频量价指标
con = duckdb.connect()
print("🚀 [Step 1] 开始利用 DuckDB 提取全市场 A 股周频量价指标...")
start_time = time.perf_counter()

prefixes = ['sh', 'sz']
existing_files = os.listdir(DATA_DIR)
patterns = []
for prefix in prefixes:
    if any(f.startswith(prefix) and f.endswith('.parquet') for f in existing_files):
        patterns.append(f"{DATA_DIR}/{prefix}*.parquet")
patterns_str = ", ".join(f"'{p}'" for p in patterns)

query = f"""
WITH weekly_dates AS (
    SELECT 
        regexp_extract(filename, '([^/\\\\\\\\]+)[.]parquet$', 1) AS symbol,
        date,
        close,
        volume,
        amount,
        date_trunc('week', date) AS week_start,
        ROW_NUMBER() OVER (PARTITION BY regexp_extract(filename, '([^/\\\\\\\\]+)[.]parquet$', 1), date_trunc('week', date) ORDER BY date DESC) as rn_desc
    FROM read_parquet([{patterns_str}], filename=true)
    WHERE (
        filename LIKE '%sh60%' 
        OR filename LIKE '%sh68%' 
        OR filename LIKE '%sz00%' 
        OR filename LIKE '%sz30%'
    )
),
weekly_summary AS (
    SELECT 
        symbol,
        week_start,
        MAX(CASE WHEN rn_desc = 1 THEN close END) as weekly_close,
        SUM(volume) as weekly_vol,
        SUM(amount) as weekly_amount
    FROM weekly_dates
    GROUP BY symbol, week_start
),
weekly_metrics AS (
    SELECT 
        symbol,
        week_start,
        weekly_close,
        weekly_vol,
        weekly_amount,
        -- 4周价格动量
        (weekly_close - LAG(weekly_close, 4) OVER (PARTITION BY symbol ORDER BY week_start)) / NULLIF(LAG(weekly_close, 4) OVER (PARTITION BY symbol ORDER BY week_start), 0) as price_mom_4w,
        -- 4周成交量异常比率
        weekly_vol / NULLIF(AVG(weekly_vol) OVER (PARTITION BY symbol ORDER BY week_start ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING), 0) as vol_surge_4w
    FROM weekly_summary
)
SELECT * FROM weekly_metrics
WHERE week_start >= '2025-01-01'
ORDER BY week_start ASC
"""

df_weekly = con.execute(query).fetchdf()
elapsed = time.perf_counter() - start_time
print(f"✅ 周频量价指标提取完成! 共 {len(df_weekly)} 条记录，耗时: {elapsed:.2f} 秒")

# 2. 读取行业分类映射
df_ind = pd.read_parquet(os.path.join(DATA_DIR, "industry_mappings.parquet"))
df_merged = pd.merge(df_weekly, df_ind, on='symbol', how='inner')
df_merged.dropna(subset=['weekly_close', 'price_mom_4w', 'vol_surge_4w', 'lvl2_name', 'lvl3_name'], inplace=True)

# 3. 确定基准指数
benchmark_symbol = "sh000852" # CSI 1000 index
df_bench = pd.read_parquet(os.path.join(DATA_DIR, f"{benchmark_symbol}.parquet"))
df_bench['week_start'] = pd.to_datetime(df_bench['date']).dt.to_period('W').dt.start_time
df_bench_weekly = df_bench.groupby('week_start').agg({'close': 'last'}).reset_index()
df_bench_weekly.sort_values(by='week_start', inplace=True)
df_bench_weekly.set_index('week_start', inplace=True)

# 账户及回测变量定义
initial_cash = 1000000.0
cash = initial_cash
portfolio = {} # symbol -> {shares, buy_price}
nav_history = []
trading_records = []
slippage_fee_rate = 0.0015 # 0.15% friction cost

unique_weeks = sorted(df_merged['week_start'].unique())
print(f"🗓️ 回测时间跨度: {pd.to_datetime(unique_weeks[0]).strftime('%Y-%m-%d')} 至 {pd.to_datetime(unique_weeks[-1]).strftime('%Y-%m-%d')} (共 {len(unique_weeks)} 个交易周)")

# 核心回测循环
for week_idx, current_week in enumerate(unique_weeks):
    df_week = df_merged[df_merged['week_start'] == current_week]
    
    # 3a. 计算当前组合的总市值
    current_portfolio_value = 0.0
    for sym, pos in list(portfolio.items()):
        stock_row = df_week[df_week['symbol'] == sym]
        if not stock_row.empty:
            curr_close = stock_row.iloc[0]['weekly_close']
            current_portfolio_value += pos['shares'] * curr_close
        else:
            current_portfolio_value += pos['shares'] * pos['buy_price']
            
    total_nav = cash + current_portfolio_value
    
    # 获取基准收盘价
    bench_close = 1.0
    if current_week in df_bench_weekly.index:
        bench_close = df_bench_weekly.loc[current_week, 'close']
    
    nav_history.append({
        "week_start": current_week,
        "portfolio_value": total_nav,
        "cash": cash,
        "benchmark_close": bench_close
    })
    
    # 3b. 低位放量蓄势漏斗选股决策
    l2_stats = df_week.groupby('lvl2_name').agg({
        'price_mom_4w': 'mean',
        'vol_surge_4w': 'mean',
        'symbol': 'count'
    }).reset_index()
    l2_stats = l2_stats[l2_stats['symbol'] >= 5]
    
    if l2_stats.empty:
        continue
        
    # score = vol_surge_4w - 2.0 * abs(price_mom_4w - 0.05)
    l2_stats['score'] = l2_stats['vol_surge_4w'] - 2.0 * (l2_stats['price_mom_4w'] - 0.05).abs()
    top_3_l2 = l2_stats.sort_values(by='score', ascending=False).head(3)['lvl2_name'].tolist()
    
    # 漏斗第二步：在选出的 Top 3 二级行业中，挖掘各自最强的前 1 个三级行业 (L3)
    selected_l3_list = []
    for l2 in top_3_l2:
        df_l2_stocks = df_week[df_week['lvl2_name'] == l2]
        l3_stats = df_l2_stocks.groupby('lvl3_name').agg({
            'vol_surge_4w': 'mean',
            'price_mom_4w': 'mean',
            'symbol': 'count'
        }).reset_index()
        l3_stats = l3_stats[l3_stats['symbol'] >= 2]
        if not l3_stats.empty:
            l3_stats['score'] = l3_stats['vol_surge_4w'] - 2.0 * (l3_stats['price_mom_4w'] - 0.05).abs()
            top_l3 = l3_stats.sort_values(by='score', ascending=False).iloc[0]['lvl3_name']
            selected_l3_list.append(top_l3)
            
    # 漏斗第三步：在选定的三级行业中，精选个股
    target_portfolio_symbols = []
    for l3 in selected_l3_list:
        df_l3_stocks = df_week[df_week['lvl3_name'] == l3]
        df_l3_low = df_l3_stocks[df_l3_stocks['price_mom_4w'] <= 0.15]
        if df_l3_low.empty:
            df_l3_low = df_l3_stocks
            
        df_l3_low['stock_score'] = df_l3_low['vol_surge_4w'] / (1.0 + df_l3_low['price_mom_4w'].abs())
        top_2_stocks = df_l3_low.sort_values(by='stock_score', ascending=False).head(2)['symbol'].tolist()
        target_portfolio_symbols.extend(top_2_stocks)
        
    target_portfolio_symbols = list(set(target_portfolio_symbols))
    
    # 3c. 【完美修复逻辑】分阶段资金调配交易引擎
    if not target_portfolio_symbols:
        continue
        
    # 计算当前持有股票在当前周的实际价值，准备再均衡
    # 1) 首先卖出所有“非目标持仓”
    for sym in list(portfolio.keys()):
        if sym not in target_portfolio_symbols:
            stock_row = df_week[df_week['symbol'] == sym]
            if not stock_row.empty:
                sell_price = stock_row.iloc[0]['weekly_close']
                pos = portfolio[sym]
                revenue = pos['shares'] * sell_price
                fee = revenue * slippage_fee_rate
                cash += (revenue - fee)
                del portfolio[sym]
                trading_records.append({
                    "week_start": current_week, "symbol": sym, "action": "SELL", "price": sell_price, "shares": pos['shares'], "amount": revenue, "fee": fee
                })
                
    # 重新计算此时的总账户资产价值 (包含已清仓后的现金 + 依然保留在目标组合里的股票当前价值)
    retained_value = 0.0
    for sym in portfolio:
        stock_row = df_week[df_week['symbol'] == sym]
        if not stock_row.empty:
            retained_value += portfolio[sym]['shares'] * stock_row.iloc[0]['weekly_close']
        else:
            retained_value += portfolio[sym]['shares'] * portfolio[sym]['buy_price']
            
    current_account_value = cash + retained_value
    target_allocation = current_account_value / len(target_portfolio_symbols)
    
    # 2) 资金调配第二步：卖出所有“超额/超重”的目标持仓，将现金完全释放出来！
    overweight_trades = []
    underweight_trades = []
    
    for sym in target_portfolio_symbols:
        stock_row = df_week[df_week['symbol'] == sym]
        if stock_row.empty:
            continue
        curr_close = stock_row.iloc[0]['weekly_close']
        
        current_shares = portfolio[sym]['shares'] if sym in portfolio else 0
        current_val = current_shares * curr_close
        diff_val = target_allocation - current_val
        
        if diff_val < -1000.0 and sym in portfolio: # 超重，记录为待卖出
            overweight_trades.append((sym, abs(diff_val), curr_close))
        elif diff_val > 1000.0: # 偏轻，记录为待买入
            underweight_trades.append((sym, diff_val, curr_close))
            
    # 执行超重股票的卖出变现
    for sym, val_to_reduce, price in overweight_trades:
        shares_to_sell = val_to_reduce / price
        if shares_to_sell >= portfolio[sym]['shares']:
            shares_to_sell = portfolio[sym]['shares']
            
        revenue = shares_to_sell * price
        fee = revenue * slippage_fee_rate
        cash += (revenue - fee)
        portfolio[sym]['shares'] -= shares_to_sell
        if portfolio[sym]['shares'] <= 0:
            del portfolio[sym]
            
        trading_records.append({
            "week_start": current_week, "symbol": sym, "action": "REDUCE", "price": price, "shares": shares_to_sell, "amount": revenue, "fee": fee
        })
        
    # 3) 资金调配第三步：此时现金池已达到最大值，完美执行所有“偏轻/新买入”股票！
    for sym, val_to_buy, price in underweight_trades:
        cost = val_to_buy
        fee = cost * slippage_fee_rate
        if cash >= (cost + fee):
            cash -= (cost + fee)
            shares_bought = cost / price
            if sym in portfolio:
                portfolio[sym]['shares'] += shares_bought
            else:
                portfolio[sym] = {'shares': shares_bought, 'buy_price': price}
                
            trading_records.append({
                "week_start": current_week, "symbol": sym, "action": "BUY", "price": price, "shares": shares_bought, "amount": cost, "fee": fee
            })
        else:
            # 极限情况：如果现金仍有微弱偏差，用剩余所有现金买入
            available_cost = cash / (1.0 + slippage_fee_rate)
            if available_cost > 100.0:
                cash = 0.0
                shares_bought = available_cost / price
                if sym in portfolio:
                    portfolio[sym]['shares'] += shares_bought
                else:
                    portfolio[sym] = {'shares': shares_bought, 'buy_price': price}
                trading_records.append({
                    "week_start": current_week, "symbol": sym, "action": "BUY_MAX", "price": price, "shares": shares_bought, "amount": available_cost, "fee": available_cost * slippage_fee_rate
                })

    if (week_idx + 1) % 20 == 0:
        print(f"   已回测至第 {week_idx + 1}/{len(unique_weeks)} 周，当前账户总资产: {total_nav:.2f} 元")

# 4. 统计分析
df_nav = pd.DataFrame(nav_history)
df_nav['portfolio_net'] = df_nav['portfolio_value'] / initial_cash
df_nav['benchmark_net'] = df_nav['benchmark_close'] / df_nav.iloc[0]['benchmark_close']

total_weeks = len(df_nav)
years = total_weeks / 52.0
final_nav = df_nav.iloc[-1]['portfolio_net']
cagr = (final_nav) ** (1.0 / years) - 1.0

final_bench = df_nav.iloc[-1]['benchmark_net']
cagr_bench = (final_bench) ** (1.0 / years) - 1.0
alpha = cagr - cagr_bench

df_nav['peak'] = df_nav['portfolio_net'].cummax()
df_nav['drawdown'] = (df_nav['portfolio_net'] - df_nav['peak']) / df_nav['peak']
max_dd = df_nav['drawdown'].min()

df_nav['bench_peak'] = df_nav['benchmark_net'].cummax()
df_nav['bench_drawdown'] = (df_nav['benchmark_net'] - df_nav['bench_peak']) / df_nav['bench_peak']
max_dd_bench = df_nav['bench_drawdown'].min()

df_nav['weekly_ret'] = df_nav['portfolio_net'].pct_change()
volatility = df_nav['weekly_ret'].std() * np.sqrt(52)
sharpe = (cagr - 0.02) / volatility if volatility > 0 else 0.0
win_rate = len(df_nav[df_nav['weekly_ret'] > 0]) * 100.0 / (total_weeks - 1)

print("======================================================================")
print("📊 【终极修复版】低位放量蓄势漏斗轮动策略回测战报 (2025年起):")
print(f"   - 策略最终累计收益率: { (final_nav - 1) * 100.0 :.2f}%")
print(f"   - 基准指数累计收益率: { (final_bench - 1) * 100.0 :.2f}%")
print(f"   - 策略年化收益率 (CAGR): { cagr * 100.0 :.2f}%")
print(f"   - 基准年化收益率 (CAGR): { cagr_bench * 100.0 :.2f}%")
print(f"   - 策略年化超额收益 (Alpha): { alpha * 100.0 :.2f}%")
print(f"   - 策略最大历史回撤 (MDD): { max_dd * 100.0 :.2f}%")
print(f"   - 基准最大历史回撤 (MDD): { max_dd_bench * 100.0 :.2f}%")
print(f"   - 策略夏普比率 (Sharpe): { sharpe :.2f}")
print(f"   - 策略交易周胜率 (Win Rate): { win_rate :.2f}%")
print("======================================================================")

# Write report
report_path = "/mnt/e/agy-workspace/tdx_quant/industry_funnel_reversal_fixed_report.md"
position_details = []
for sym, pos in portfolio.items():
    stock_mapping = df_ind[df_ind['symbol'] == sym]
    ind_name = stock_mapping.iloc[0]['industry_name'] if not stock_mapping.empty else "其它行业"
    l3_name = stock_mapping.iloc[0]['lvl3_name'] if not stock_mapping.empty else "其它三级"
    position_details.append(f"| **{sym}** | **{ind_name}** ({l3_name}) | {pos['shares']:.0f} 股 | {pos['buy_price']:.2f} 元 |")

report_markdown = f"""# 📈 【资金流修复版】通达信行业“低位放量蓄势”策略审计报告

本审计报告彻底修复了回测中存在的**资金交易时序 Sequencing Bug**（即“先买后卖导致的可用现金不足，跳过买单”漏洞），向我们展示了该策略在纠正后的真实收益。

---

## 📊 1. 终极绩效对比 (Performance Summary)

| 量化绩效指标 | 🚀 低位放量蓄势轮动策略 (完美资金流) | 🛡️ 基准指数 ({benchmark_symbol}) | 差值 / 超额 (Alpha) |
| :--- | :---: | :---: | :---: |
| **累计总收益率 (%)** | **{(final_nav - 1) * 100.0:.2f}%** | {(final_bench - 1) * 100.0:.2f}% | **{((final_nav - final_bench) * 100.0):.2f}%** |
| **年化收益率 (CAGR)** | **{cagr * 100.0:.2f}%** | {cagr_bench * 100.0:.2f}% | **{alpha * 100.0:.2f}%** |
| **历史最大回撤 (MDD)** | **{max_dd * 100.0:.2f}%** | {max_dd_bench * 100.0:.2f}% | **{abs(max_dd - max_dd_bench) * 100.0:.2f}%** |
| **夏普比率 (Sharpe)** | **{sharpe:.2f}** | - | - |
| **周度交易胜率 (%)** | **{win_rate:.2f}%** | - | - |

---

## 🛠️ 2. 完美的资金双阶段买卖路由设计

为了解决等权重再平衡时的 Sequencing 漏洞，我们重构了回测的交易路由：

```mermaid
graph TD
    A["每周一调仓开始"] --> B["阶段 1：遍历当前持仓，清仓所有‘不在新组合’中的股票<br>回笼现金"]
    B --> C["计算当前账户总值，确定新等权目标 target_allocation"]
    C --> D["阶段 2（卖出变现）：找出所有‘超重持仓’，卖出其超额部分<br>现金池达到最大值"]
    D --> E["阶段 3（买入建仓）：完美执行所有‘新股买入’或‘低重加仓’<br>完全杜绝现金不足跳过交易的Bug"]
```
"""

with open(report_path, "w", encoding="utf-8") as f:
    f.write(report_markdown)

print(f"SUCCESS: Report successfully written to {report_path}!")
con.close()
