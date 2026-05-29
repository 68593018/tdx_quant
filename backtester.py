import os
import sys
import duckdb
import numpy as np
import pandas as pd
from datetime import datetime

# Force stdout/stderr to UTF-8
if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

class StrategyBacktester:
    def __init__(self, data_dir="data", benchmark_symbol="sh000852", start_date="2018-01-01", end_date="2026-05-27"):
        self.data_dir = data_dir
        self.benchmark_symbol = benchmark_symbol
        self.start_date = start_date
        self.end_date = end_date
        self.con = duckdb.connect()
        self.con.execute(f"SET threads = {os.cpu_count()}")
        
        # Load mappings
        self.block_df = pd.read_parquet(os.path.join(data_dir, "block_mappings.parquet"))
        self.ind_df = pd.read_parquet(os.path.join(data_dir, "industry_mappings.parquet"))
        
        # Load benchmark index
        benchmark_path = os.path.join(data_dir, f"{benchmark_symbol}.parquet")
        self.benchmark_df = pd.read_parquet(benchmark_path)
        self.benchmark_df['date'] = pd.to_datetime(self.benchmark_df['date'])
        self.benchmark_df.sort_values(by='date', inplace=True)
        self.benchmark_df.set_index('date', inplace=True)
        
        # Build trade calendar from benchmark or sh000001
        cal_path = os.path.join(data_dir, "sh000001.parquet")
        cal_df = pd.read_parquet(cal_path)
        self.trade_calendar = sorted(pd.to_datetime(cal_df['date']).unique())
        
        # Parse GBBQ for float shares history
        self.gbbq_shares = self._load_gbbq_shares()

    def _load_gbbq_shares(self):
        """Loads and processes GBBQ capital shares history from raw GBBQ file"""
        from parser.gbbq import parse_tdx_gbbq_file
        
        # Try to find GBBQ path in config
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
            print("⚠️ 警告: GBBQ 股本文件不存在，市值分层将采用当前最新市值进行近似。")
            return None
            
        print("💡 正在解析 GBBQ 全局股本变更历史...")
        try:
            gbbq_df = parse_tdx_gbbq_file(gbbq_path)
            # Filter non-dividend records (categories other than 1 are capital stock changes)
            shares_df = gbbq_df[gbbq_df['category'] != 1].copy()
            shares_df['date'] = pd.to_datetime(shares_df['date'])
            shares_df.sort_values(by=['code', 'date'], inplace=True)
            
            # Map symbol code to list of (date, float_shares_wan)
            gbbq_map = {}
            for code, group in shares_df.groupby('code'):
                gbbq_map[code] = list(zip(group['date'], group['allocated_ratio']))
            return gbbq_map
        except Exception as e:
            print(f"❌ 解析 GBBQ 失败: {e}。将退回到当前最新市值进行近似。")
            return None

    def get_float_market_cap(self, symbol, date_dt, close_price):
        """Calculates exact float market capitalization on a specific date using GBBQ records"""
        code = symbol[-6:]
        if self.gbbq_shares and code in self.gbbq_shares:
            records = self.gbbq_shares[code]
            # Find the last record on or before date_dt
            float_shares_wan = None
            for r_date, r_shares in records:
                if r_date <= date_dt:
                    float_shares_wan = r_shares
                else:
                    break
            
            if float_shares_wan is not None and float_shares_wan > 0:
                # float_shares_wan is in ten thousand shares
                return float_shares_wan * 10000.0 * close_price
                
        # Approximate using current file length or standard calculation if GBBQ fails
        return 3e9 # Default 3 billion approximation if not found

    def run_signal_scan(self):
        """Scans the entire database for Monthly Abnormal Volume & Limit Up signals using DuckDB"""
        print("🚀 启动全市场数据扫描，正在计算月级环比与涨停指标...")
        
        # Prepare patterns
        prefixes = ['sh', 'sz']
        existing_files = os.listdir(self.data_dir)
        patterns = []
        for prefix in prefixes:
            if any(f.startswith(prefix) and f.endswith('.parquet') for f in existing_files):
                patterns.append(f"{self.data_dir}/{prefix}*.parquet")
        patterns_str = ", ".join(f"'{p}'" for p in patterns)

        # Main optimized DuckDB Query
        query = f"""
        WITH raw_daily AS (
            SELECT 
                regexp_extract(filename, '([^/\\\\\\\\]+)[.]parquet$', 1) AS symbol,
                date,
                close,
                low,
                amount,
                volume,
                close_adj,
                low_adj,
                factor,
                MIN(date) OVER (PARTITION BY regexp_extract(filename, '([^/\\\\\\\\]+)[.]parquet$', 1)) AS first_date
            FROM read_parquet([{patterns_str}], filename=true)
            WHERE (
                filename LIKE '%sh60%' 
                OR filename LIKE '%sh68%' 
                OR filename LIKE '%sz00%' 
                OR filename LIKE '%sz30%'
            )
        ),
        daily_returns AS (
            SELECT 
                symbol,
                date,
                close,
                low,
                amount,
                volume,
                close_adj,
                low_adj,
                factor,
                first_date,
                LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
                (close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY date), 0) * 100 AS daily_return
            FROM raw_daily
        ),
        limit_ups AS (
            SELECT 
                *,
                CASE 
                    WHEN (symbol LIKE 'sh60%' OR symbol LIKE 'sz00%') AND daily_return >= 9.91 THEN 1
                    WHEN (symbol LIKE 'sh68%' OR symbol LIKE 'sz30%') AND daily_return >= 19.91 THEN 1
                    ELSE 0
                END AS is_limit_up
            FROM daily_returns
        ),
        monthly_amount AS (
            SELECT 
                symbol,
                date_trunc('month', date) AS month_date,
                SUM(amount) AS m_amount
            FROM raw_daily
            GROUP BY symbol, month_date
        ),
        monthly_ratio AS (
            SELECT 
                symbol,
                month_date,
                m_amount,
                LAG(m_amount) OVER (PARTITION BY symbol ORDER BY month_date) AS prev_amount,
                m_amount / NULLIF(LAG(m_amount) OVER (PARTITION BY symbol ORDER BY month_date), 0) AS vol_ratio
            FROM monthly_amount
        ),
        monthly_volume_flag AS (
            SELECT 
                symbol,
                month_date,
                SUM(CASE WHEN vol_ratio >= 3.0 THEN 1 ELSE 0 END) OVER (
                    PARTITION BY symbol 
                    ORDER BY month_date 
                    ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
                ) AS abnormal_months_count
            FROM monthly_ratio
        ),
        limit_up_ranked AS (
            SELECT 
                *,
                ROW_NUMBER() OVER (PARTITION BY symbol, date_trunc('month', date) ORDER BY date) AS limit_up_rank
            FROM limit_ups
            WHERE is_limit_up = 1
        )
        SELECT 
            l.symbol,
            l.date,
            l.close,
            l.close_adj,
            l.low,
            l.low_adj,
            l.factor,
            l.limit_up_rank,
            f.abnormal_months_count
        FROM limit_up_ranked l
        JOIN monthly_volume_flag f 
          ON l.symbol = f.symbol 
         AND date_trunc('month', l.date) = f.month_date
        WHERE f.abnormal_months_count >= 2
          AND l.date - l.first_date >= INTERVAL 390 DAY
          AND l.limit_up_rank IN (1, 2)
          AND l.date >= '{self.start_date}'
          AND l.date <= '{self.end_date}'
        ORDER BY l.date ASC, l.symbol ASC
        """
        
        signals_df = self.con.execute(query).fetchdf()
        print(f"✅ 信号扫描完毕！共筛出候选信号: {len(signals_df)} 条。")
        return signals_df

    def process_trades(self, signals_df):
        """Simulates dip-buy orders at T+1 (or secondary attempts) and calculates returns across holding periods"""
        print("💡 开始处理各股日K线，运行低吸挂单成交与持有期收益测算...")
        
        # Load unique symbols that triggered signals
        unique_symbols = signals_df['symbol'].unique()
        
        # Cache daily K-line lookup tables for the signal stocks in memory
        stock_kline_cache = {}
        for sym in unique_symbols:
            filepath = os.path.join(self.data_dir, f"{sym}.parquet")
            if os.path.exists(filepath):
                df = pd.read_parquet(filepath)
                df['date'] = pd.to_datetime(df['date'])
                df.sort_values(by='date', inplace=True)
                df.reset_index(drop=True, inplace=True)
                stock_kline_cache[sym] = df

        # Load stock-industry and stock-concept mappings
        ind_map = dict(zip(self.ind_df['symbol'], self.ind_df['industry_name']))
        
        # Concepts block mapping is a bit different since a stock can belong to multiple concepts.
        concept_map = {}
        for _, row in self.block_df[self.block_df['block_category'] == 'concept'].iterrows():
            sym = f"{row['market']}{row['code']}"
            concept_map.setdefault(sym, []).append(row['block_name'])

        # Process each signal
        trades = []
        signals_grouped = signals_df.groupby('symbol')
        
        for sym, sym_signals in signals_grouped:
            if sym not in stock_kline_cache:
                continue
                
            df = stock_kline_cache[sym]
            date_to_idx = {d: i for i, d in enumerate(df['date'])}
            
            # Sort signals by date
            sym_signals = sym_signals.sort_values(by='date')
            
            # We process month by month to handle rank 1 and rank 2
            # Group signals by natural month
            sym_signals['month'] = sym_signals['date'].apply(lambda x: x.strftime('%Y-%m'))
            
            for month_str, month_signals in sym_signals.groupby('month'):
                # Get the first rank 1 signal
                rank1_signals = month_signals[month_signals['limit_up_rank'] == 1]
                rank2_signals = month_signals[month_signals['limit_up_rank'] == 2]
                
                if rank1_signals.empty:
                    continue
                    
                sig_1 = rank1_signals.iloc[0]
                t_date = pd.to_datetime(sig_1['date'])
                t_idx = date_to_idx.get(t_date)
                
                if t_idx is None or t_idx + 1 >= len(df):
                    continue
                    
                # Try rank 1 low buy at T+1
                t_plus_1_row = df.iloc[t_idx + 1]
                t_close_adj = sig_1['close_adj']
                
                bought = False
                buy_idx = None
                buy_price_adj = None
                buy_price = None
                buy_date = None
                buy_type = None
                
                # Condition: T+1 low_adj <= T close_adj
                if t_plus_1_row['low_adj'] <= t_close_adj:
                    bought = True
                    buy_idx = t_idx + 1
                    buy_price_adj = t_close_adj
                    # Calculate unadjusted buy price based on factor
                    buy_price = sig_1['close']
                    buy_date = t_plus_1_row['date']
                    buy_type = "First_Limit_Up"
                else:
                    # T+1 failed to buy, try secondary attempt (rank 2)
                    if not rank2_signals.empty:
                        sig_2 = rank2_signals.iloc[0]
                        t2_date = pd.to_datetime(sig_2['date'])
                        t2_idx = date_to_idx.get(t2_date)
                        
                        if t2_idx is not None and t2_idx + 1 < len(df):
                            # Conditions: 
                            # 1. T_2 - T <= 10 trading days
                            # 2. T_2 close_adj <= 1.20 * T close_adj
                            trading_days_gap = t2_idx - t_idx
                            price_gain = (sig_2['close_adj'] - t_close_adj) / t_close_adj
                            
                            if trading_days_gap <= 10 and price_gain <= 0.20:
                                t2_plus_1_row = df.iloc[t2_idx + 1]
                                t2_close_adj = sig_2['close_adj']
                                if t2_plus_1_row['low_adj'] <= t2_close_adj:
                                    bought = True
                                    buy_idx = t2_idx + 1
                                    buy_price_adj = t2_close_adj
                                    buy_price = sig_2['close']
                                    buy_date = t2_plus_1_row['date']
                                    buy_type = "Second_Limit_Up"

                if bought:
                    # Calculate float market cap at buy date
                    mcap = self.get_float_market_cap(sym, buy_date, buy_price)
                    industry = ind_map.get(sym, "其它行业")
                    concepts = concept_map.get(sym, ["其它概念"])
                    
                    trade_record = {
                        "symbol": sym,
                        "buy_date": buy_date,
                        "buy_price_adj": buy_price_adj,
                        "buy_price": buy_price,
                        "buy_type": buy_type,
                        "float_market_cap": mcap,
                        "industry": industry,
                        "concepts": concepts,
                        "signal_date": t_date
                    }
                    
                    # Calculate holding period outcomes for H = 20, 40, 60, 120 days
                    for H in [20, 40, 60, 120]:
                        exit_idx = buy_idx + H
                        if exit_idx < len(df):
                            exit_row = df.iloc[exit_idx]
                            exit_price_adj = exit_row['close_adj']
                            exit_date = exit_row['date']
                            ret = (exit_price_adj - buy_price_adj) / buy_price_adj
                            
                            # Max drawdown during holding
                            hold_period_df = df.iloc[buy_idx + 1 : exit_idx + 1]
                            min_low_adj = hold_period_df['low_adj'].min()
                            max_dd = (min_low_adj - buy_price_adj) / buy_price_adj
                            
                            # Baseline index return
                            idx_ret = 0.0
                            if buy_date in self.benchmark_df.index and exit_date in self.benchmark_df.index:
                                idx_entry = self.benchmark_df.loc[buy_date, 'close']
                                idx_exit = self.benchmark_df.loc[exit_date, 'close']
                                if idx_entry > 0:
                                    idx_ret = (idx_exit - idx_entry) / idx_entry
                            
                            trade_record[f"ret_{H}"] = ret
                            trade_record[f"dd_{H}"] = max_dd
                            trade_record[f"idx_ret_{H}"] = idx_ret
                            trade_record[f"alpha_{H}"] = ret - idx_ret
                            trade_record[f"exit_date_{H}"] = exit_date
                            trade_record[f"exit_price_adj_{H}"] = exit_price_adj
                        else:
                            # Not enough days to complete holding period, mark latest
                            latest_row = df.iloc[-1]
                            exit_price_adj = latest_row['close_adj']
                            exit_date = latest_row['date']
                            ret = (exit_price_adj - buy_price_adj) / buy_price_adj
                            
                            hold_period_df = df.iloc[buy_idx + 1 :]
                            min_low_adj = hold_period_df['low_adj'].min() if not hold_period_df.empty else buy_price_adj
                            max_dd = (min_low_adj - buy_price_adj) / buy_price_adj
                            
                            idx_ret = 0.0
                            if buy_date in self.benchmark_df.index and exit_date in self.benchmark_df.index:
                                idx_entry = self.benchmark_df.loc[buy_date, 'close']
                                idx_exit = self.benchmark_df.loc[exit_date, 'close']
                                if idx_entry > 0:
                                    idx_ret = (idx_exit - idx_entry) / idx_entry
                                    
                            trade_record[f"ret_{H}"] = ret
                            trade_record[f"dd_{H}"] = max_dd
                            trade_record[f"idx_ret_{H}"] = idx_ret
                            trade_record[f"alpha_{H}"] = ret - idx_ret
                            trade_record[f"exit_date_{H}"] = exit_date
                            trade_record[f"exit_price_adj_{H}"] = exit_price_adj
                            trade_record[f"completed_{H}"] = False
                            
                        if f"completed_{H}" not in trade_record:
                            trade_record[f"completed_{H}"] = True
                            
                    trades.append(trade_record)
                    
        trades_df = pd.DataFrame(trades)
        if not trades_df.empty:
            trades_df['buy_date'] = pd.to_datetime(trades_df['buy_date'])
            trades_df.sort_values(by='buy_date', inplace=True)
            trades_df.reset_index(drop=True, inplace=True)
        print(f"✅ 交易匹配处理完毕！总成交信号交易样本数: {len(trades_df)} 条。")
        return trades_df

    def simulate_portfolio(self, trades_df, holding_days=40, commission_buy=0.0015, commission_sell=0.0025, max_positions=10):
        """Simulates full portfolio-level performance on a daily loop with equal weight allocation and risk controls"""
        print(f"📊 启动组合资金级回测 (持有周期: {holding_days} 天，最大仓位: {max_positions} 只，双边成本: {commission_buy+commission_sell:.2%})...")
        
        # Build index MA250 for market filter
        benchmark_close = self.benchmark_df['close']
        benchmark_ma250 = benchmark_close.rolling(window=250).mean()
        
        initial_capital = 10000000.0 # 10 Million Yuan
        cash = initial_capital
        portfolio_value = initial_capital
        
        # Track active positions
        # Record schema: {symbol, buy_date, buy_price_adj, shares, industry, concepts, remaining_days}
        active_positions = []
        
        # Keep a history of daily portfolio net value
        equity_curve = []
        
        # Track last trade dates for each stock for cooling mechanism
        last_trade_dates = {}
        
        # Prepare trading calendar within backtest range
        sim_calendar = [d for d in self.trade_calendar if d >= pd.to_datetime(self.start_date) and d <= pd.to_datetime(self.end_date)]
        
        # Group trades by buy_date for fast lookup during daily loop
        trades_by_date = {}
        for _, t in trades_df.iterrows():
            d_key = t['buy_date'].strftime('%Y-%m-%d')
            trades_by_date.setdefault(d_key, []).append(t)

        # Cache daily stock closes for valuation
        daily_closes_cache = {}
        
        # Compile all unique active symbols in trades
        all_symbols = trades_df['symbol'].unique()
        for sym in all_symbols:
            filepath = os.path.join(self.data_dir, f"{sym}.parquet")
            if os.path.exists(filepath):
                df = pd.read_parquet(filepath)
                df['date'] = pd.to_datetime(df['date'])
                daily_closes_cache[sym] = dict(zip(df['date'], df['close_adj']))

        for day_dt in sim_calendar:
            day_str = day_dt.strftime('%Y-%m-%d')
            
            # --- 1. Sell expired positions ---
            new_active_positions = []
            for pos in active_positions:
                pos['remaining_days'] -= 1
                if pos['remaining_days'] <= 0:
                    # Sell at today's close
                    close_dict = daily_closes_cache.get(pos['symbol'], {})
                    sell_price_adj = close_dict.get(day_dt)
                    
                    if sell_price_adj is None:
                        # Fallback if suspended or missing, use buy price as approximation
                        sell_price_adj = pos['buy_price_adj']
                        
                    sell_value = pos['shares'] * sell_price_adj
                    sell_cost = sell_value * commission_sell
                    cash += (sell_value - sell_cost)
                    
                    # Record for cooling mechanism
                    last_trade_dates[pos['symbol']] = day_dt
                else:
                    new_active_positions.append(pos)
            active_positions = new_active_positions
            
            # --- 2. Calculate current equity value before buying ---
            stock_value = 0.0
            for pos in active_positions:
                close_dict = daily_closes_cache.get(pos['symbol'], {})
                curr_price = close_dict.get(day_dt)
                if curr_price is None:
                    curr_price = pos['buy_price_adj'] # Fallback
                stock_value += pos['shares'] * curr_price
            
            portfolio_value = cash + stock_value
            
            # --- 3. Open new positions ---
            # Check market filter
            market_ok = True
            if day_dt in benchmark_ma250.index:
                ma_val = benchmark_ma250.loc[day_dt]
                close_val = benchmark_close.loc[day_dt]
                if pd.notnull(ma_val) and close_val <= ma_val:
                    market_ok = False # CSI 1000 <= MA250, block buying
            
            if market_ok and len(active_positions) < max_positions:
                # Check if there are buy orders executing today
                candidates = trades_by_date.get(day_str, [])
                
                # Filter candidates by:
                # 1. Sector cap (at most 2 in same concept, at most 2 in same industry)
                # 2. Cooling mechanism (6 months = 180 calendar days since last trade)
                valid_candidates = []
                for cand in candidates:
                    sym = cand['symbol']
                    
                    # Check cooling
                    if sym in last_trade_dates:
                        days_since_trade = (day_dt - last_trade_dates[sym]).days
                        if days_since_trade < 180:
                            continue
                            
                    # Check industry and concept concentration
                    curr_ind_count = sum(1 for p in active_positions if p['industry'] == cand['industry'])
                    if curr_ind_count >= 2:
                        continue
                        
                    # Check concept concentration
                    concept_overlap = False
                    for pos in active_positions:
                        common_concepts = set(pos['concepts']).intersection(set(cand['concepts']))
                        # If overlapping concepts have too many counts, skip (concept is broad, so let's simplify:
                        # if the stock shares a concept block with an active position, we check if we already have >= 2 of that concept)
                        for c in common_concepts:
                            c_count = sum(1 for p in active_positions if c in p['concepts'])
                            if c_count >= 2:
                                concept_overlap = True
                                break
                        if concept_overlap:
                            break
                            
                    if concept_overlap:
                        continue
                        
                    valid_candidates.append(cand)
                    
                # Sort candidates: prioritize lower float market cap to target high-elasticity small caps
                valid_candidates.sort(key=lambda x: x['float_market_cap'])
                
                # Execute buys up to slot limit
                slots_remaining = max_positions - len(active_positions)
                for cand in valid_candidates[:slots_remaining]:
                    # Target weight is exactly 10% (initial/current equity equal weight)
                    target_size = portfolio_value * (1.0 / max_positions)
                    
                    if cash >= target_size:
                        buy_val = target_size
                    else:
                        buy_val = cash # Buy with whatever cash is left
                        
                    if buy_val > 10000.0: # Minimum transaction size 10k
                        buy_cost = buy_val * commission_buy
                        net_buy_val = buy_val - buy_cost
                        shares = net_buy_val / cand['buy_price_adj']
                        
                        active_positions.append({
                            "symbol": cand['symbol'],
                            "buy_date": day_dt,
                            "buy_price_adj": cand['buy_price_adj'],
                            "shares": shares,
                            "industry": cand['industry'],
                            "concepts": cand['concepts'],
                            "remaining_days": holding_days
                        })
                        cash -= buy_val
                        
            # --- 4. Record daily equity ---
            stock_value = 0.0
            for pos in active_positions:
                close_dict = daily_closes_cache.get(pos['symbol'], {})
                curr_price = close_dict.get(day_dt)
                if curr_price is None:
                    curr_price = pos['buy_price_adj']
                stock_value += pos['shares'] * curr_price
            
            portfolio_value = cash + stock_value
            equity_curve.append({
                "date": day_dt,
                "portfolio_value": portfolio_value,
                "cash": cash,
                "stock_value": stock_value,
                "net_value": portfolio_value / initial_capital,
                "positions_count": len(active_positions)
            })
            
        equity_df = pd.DataFrame(equity_curve)
        equity_df.set_index('date', inplace=True)
        
        # Calculate daily returns and drawdowns
        equity_df['daily_return'] = equity_df['portfolio_value'].pct_change().fillna(0)
        equity_df['cum_max'] = equity_df['portfolio_value'].cummax()
        equity_df['drawdown'] = (equity_df['portfolio_value'] - equity_df['cum_max']) / equity_df['cum_max']
        
        # Align with benchmark returns
        equity_df['benchmark_close'] = self.benchmark_df.loc[equity_df.index, 'close']
        equity_df['benchmark_return'] = equity_df['benchmark_close'].pct_change().fillna(0)
        equity_df['benchmark_net_value'] = equity_df['benchmark_close'] / equity_df['benchmark_close'].iloc[0]
        
        print("✅ 组合资金级模拟运行成功！")
        return equity_df
