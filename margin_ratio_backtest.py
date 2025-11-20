"""
融資維持率策略回測系統
根據 TEJ 量化交易策略進行回測
"""

import sqlite3
import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from collections import defaultdict

# 設定編碼（Windows）
if os.name == 'nt':
    try:
        os.system('chcp 65001 > NUL')
    except:
        pass
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding='utf-8')
            except ValueError:
                pass

# 設定中文字體
plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Arial Unicode MS', 'SimHei']
plt.rcParams['axes.unicode_minus'] = False


class MarginRatioBacktest:
    """融資維持率策略回測系統"""
    
    def __init__(self, db_path='taiwan_stock.db', initial_capital=1000000, 
                 enable_take_profit=True, enable_stop_loss=True):
        """
        初始化回測系統
        
        參數:
        - db_path: 資料庫路徑
        - initial_capital: 初始資金（新台幣）
        - enable_take_profit: 是否啟用停利（預設 True）
        - enable_stop_loss: 是否啟用停損（預設 True）
        """
        self.db_path = db_path
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions = {}  # {ticker: {'shares': int, 'entry_date': str, 'entry_price': float, 'entry_signal_date': str}}
        self.trades = []  # 記錄所有交易
        self.daily_portfolio_value = []  # 每日投資組合價值
        self.pending_orders = []  # 待成交的掛單 [{ticker, stock_name, order_price, signal_date, order_date, shares, is_odd_lot, total_cost}]
        self.stop_loss_orders = {}  # {ticker: {'stop_loss_price': float, 'shares': int, 'entry_price': float}}
        
        # 交易成本設定（根據玉山證券）
        self.commission_rate = 0.001425  # 0.1425% 手續費
        self.commission_min = 1  # 零股最低手續費 1 元
        self.commission_min_full = 20  # 整張最低手續費 20 元
        self.tax_rate = 0.003  # 0.3% 證交稅（賣出時）
        self.tax_rate_day_trade = 0.0015  # 0.15% 當沖證交稅
        
        # 策略參數
        self.position_size_ratio = 0.1  # 每次使用 1/10 的現金
        self.holding_period = 15  # 持有15個交易日
        self.take_profit = 0.40  # 停利 +40%
        self.stop_loss = 0.10  # 停損 -10%
        self.enable_take_profit = enable_take_profit  # 是否啟用停利
        self.enable_stop_loss = enable_stop_loss  # 是否啟用停損
        
    def calculate_commission(self, value, is_odd_lot=False):
        """
        計算手續費
        
        參數:
        - value: 交易金額
        - is_odd_lot: 是否為零股交易
        
        回傳: 手續費金額
        """
        commission = value * self.commission_rate
        min_commission = self.commission_min if is_odd_lot else self.commission_min_full
        return max(commission, min_commission)
    
    def calculate_tax(self, value, is_day_trade=False):
        """
        計算證交稅（僅賣出時）
        
        參數:
        - value: 交易金額
        - is_day_trade: 是否為當沖
        
        回傳: 證交稅金額
        """
        tax_rate = self.tax_rate_day_trade if is_day_trade else self.tax_rate
        return value * tax_rate
    
    def get_trading_dates(self, start_date, end_date):
        """取得交易日列表"""
        conn = sqlite3.connect(self.db_path)
        query = """
        SELECT DISTINCT date 
        FROM strategy_result 
        WHERE date >= ? AND date <= ?
        ORDER BY date
        """
        df = pd.read_sql_query(query, conn, params=(start_date, end_date))
        conn.close()
        return df['date'].tolist()
    
    def check_margin_ratio_drop_condition(self, data_row):
        """
        檢查是否符合融資維持率跌幅條件（第一階段篩選）
        
        條件：
        1. 融資維持率 < 過去10日移動平均值
        
        回傳: (符合條件, 跌幅百分比)
        """
        if pd.isna(data_row.get('margin_ratio')) or pd.isna(data_row.get('avg_10day_ratio')):
            return False, None
        
        # 檢查是否低於平均
        if data_row['margin_ratio'] >= data_row['avg_10day_ratio']:
            return False, None
        
        # 計算跌幅百分比
        drop_pct = (data_row['margin_ratio'] - data_row['avg_10day_ratio']) / data_row['avg_10day_ratio'] * 100
        
        return True, drop_pct
    
    def check_filter_conditions(self, data_row):
        """
        檢查是否符合三項濾網條件（第二階段篩選）
        
        條件：
        1. 成交量 > 過去10日平均量（測試版本：改為大於）
        2. 當日為紅K（收盤價 > 開盤價）
        3. 融資餘額 > 前5日平均融資餘額 × 0.95
        """
        # 檢查必要欄位
        if pd.isna(data_row.get('volume')) or pd.isna(data_row.get('avg_10day_volume')):
            return False
        
        if pd.isna(data_row.get('open_price')) or pd.isna(data_row.get('close_price')):
            return False
        
        if pd.isna(data_row.get('margin_balance_shares')) or pd.isna(data_row.get('avg_5day_balance_95')):
            return False
        
        # 濾網1: 成交量 > 過去10日平均量（測試版本：改為大於）
        filter1 = data_row['volume'] > data_row['avg_10day_volume']
        
        # 濾網2: 當日為紅K（收盤價 > 開盤價）
        filter2 = data_row['close_price'] > data_row['open_price']
        
        # 濾網3: 融資餘額 > 前5日平均融資餘額 × 0.95
        filter3 = data_row['margin_balance_shares'] > data_row['avg_5day_balance_95']
        
        return filter1 and filter2 and filter3
    
    def get_entry_signals(self, date, df, top_n=10):
        """
        取得進場訊號（兩階段篩選）
        
        步驟：
        1. 先找出所有符合「融資維持率 < 過去10日移動平均值」的股票
        2. 計算跌幅百分比，按跌幅排序，取前 top_n 名
        3. 對這 top_n 檔股票，逐一檢查三項濾網
        4. 通過所有濾網的才進場
        
        參數:
        - date: 日期
        - df: 當日所有股票資料 DataFrame
        - top_n: 取前幾名（預設10名）
        
        回傳:
        - 符合條件的股票列表（DataFrame）
        """
        # 第一階段：找出符合融資維持率跌幅條件的股票
        candidates = []
        for _, row in df.iterrows():
            has_drop, drop_pct = self.check_margin_ratio_drop_condition(row)
            if has_drop:
                candidates.append({
                    'ticker': row['ticker'],
                    'stock_name': row['stock_name'],
                    'margin_ratio': row['margin_ratio'],
                    'avg_10day_ratio': row['avg_10day_ratio'],
                    'drop_pct': drop_pct,
                    'volume': row['volume'],
                    'avg_10day_volume': row['avg_10day_volume'],
                    'open_price': row['open_price'],
                    'close_price': row['close_price'],
                    'margin_balance_shares': row['margin_balance_shares'],
                    'avg_5day_balance_95': row['avg_5day_balance_95']
                })
        
        if len(candidates) == 0:
            return pd.DataFrame()
        
        # 轉換為 DataFrame 並排序（跌幅百分比由小到大，跌越多越前面）
        candidates_df = pd.DataFrame(candidates)
        candidates_df = candidates_df.sort_values('drop_pct', ascending=True)
        
        # 取前 top_n 名
        top_candidates = candidates_df.head(top_n).copy()
        
        # 第二階段：對前 top_n 名逐一檢查三項濾網
        final_signals = []
        for _, row in top_candidates.iterrows():
            if self.check_filter_conditions(row):
                final_signals.append(row)
        
        if len(final_signals) == 0:
            return pd.DataFrame()
        
        return pd.DataFrame(final_signals)
    
    def check_entry_signal(self, date, ticker, data_row):
        """
        檢查是否符合進場條件（已廢棄，保留向後相容）
        新的邏輯應該使用 get_entry_signals 方法
        """
        # 這個方法保留用於向後相容，但實際邏輯已改為兩階段篩選
        has_drop, _ = self.check_margin_ratio_drop_condition(data_row)
        if not has_drop:
            return False
        return self.check_filter_conditions(data_row)
    
    def place_order(self, order_date, ticker, stock_name, order_price, signal_date):
        """
        掛限價單（隔日開盤時掛前一日開盤價）
        
        參數:
        - order_date: 掛單日期（實際執行日期，通常是 signal_date 的隔日）
        - ticker: 股票代號
        - stock_name: 股票名稱
        - order_price: 掛單價格（前一日開盤價）
        - signal_date: 訊號產生日期
        
        回傳: 是否成功掛單
        """
        # 計算可用的資金（1/10 的現金）
        available_cash = self.cash * self.position_size_ratio
        
        # 計算可買的股數（先以整張計算）
        shares_per_lot = 1000  # 1張 = 1000股
        order_value = available_cash
        shares = int(order_value // order_price)
        
        # 如果買不起整張，改買零股
        is_odd_lot = False
        if shares < shares_per_lot:
            # 零股交易
            is_odd_lot = True
            shares = int(order_value // order_price)
        else:
            # 整張交易
            shares = (shares // shares_per_lot) * shares_per_lot
        
        if shares <= 0:
            return False
        
        # 計算實際交易金額
        trade_value = shares * order_price
        
        # 計算手續費（預估）
        commission = self.calculate_commission(trade_value, is_odd_lot=is_odd_lot)
        
        # 總成本（預估）
        total_cost = trade_value + commission
        
        # 檢查現金是否足夠（先預留資金）
        if total_cost > self.cash:
            return False
        
        # 記錄掛單（不立即扣款，等成交時才扣款）
        self.pending_orders.append({
            'order_date': order_date,
            'ticker': ticker,
            'stock_name': stock_name,
            'order_price': order_price,
            'shares': shares,
            'signal_date': signal_date,
            'is_odd_lot': is_odd_lot,
            'total_cost': total_cost
        })
        
        return True
    
    def check_and_execute_orders(self, date):
        """
        檢查並執行掛單（檢查當日最低價是否觸及掛單價格）
        
        參數:
        - date: 當日日期
        
        邏輯:
        - 如果當日最低價 <= 掛單價格，則成交（以掛單價格成交）
        - 如果當日最低價 > 掛單價格，則訂單作廢
        """
        if not self.pending_orders:
            return
        
        # 取得當日的最高價和最低價
        conn = sqlite3.connect(self.db_path)
        
        executed_orders = []
        expired_orders = []
        
        for order in self.pending_orders:
            if order['order_date'] != date:
                continue  # 只處理當日的掛單
            
            ticker = order['ticker']
            
            # 從 tw_stock_price_data 表取得當日最高價和最低價
            query = """
            SELECT low, high, open, close
            FROM tw_stock_price_data
            WHERE ticker = ? AND date = ?
            LIMIT 1
            """
            cursor = conn.cursor()
            cursor.execute(query, (ticker, date))
            result = cursor.fetchone()
            
            if result and result[0] is not None:
                low_price = result[0]
                high_price = result[1]
                open_price = result[2]
                close_price = result[3]
                
                # 檢查是否成交：如果最低價 <= 掛單價格，則成交
                if low_price <= order['order_price']:
                    # 成交（以掛單價格成交）
                    executed_orders.append(order)
                else:
                    # 沒成交，訂單作廢
                    expired_orders.append(order)
            else:
                # 沒有資料，訂單作廢
                expired_orders.append(order)
        
        conn.close()
        
        # 執行成交的訂單
        for order in executed_orders:
            self.execute_order(order, date)
        
        # 移除已處理的訂單（成交或作廢）
        self.pending_orders = [o for o in self.pending_orders if o not in executed_orders and o not in expired_orders]
    
    def execute_order(self, order, date):
        """
        執行成交的訂單
        
        參數:
        - order: 掛單資訊
        - date: 成交日期
        """
        ticker = order['ticker']
        stock_name = order['stock_name']
        order_price = order['order_price']
        shares = order['shares']
        signal_date = order['signal_date']
        is_odd_lot = order['is_odd_lot']
        
        # 計算實際交易金額
        trade_value = shares * order_price
        
        # 計算手續費
        commission = self.calculate_commission(trade_value, is_odd_lot=is_odd_lot)
        
        # 總成本
        total_cost = trade_value + commission
        
        # 檢查現金是否足夠
        if total_cost > self.cash:
            return False
        
        # 執行買進（扣款）
        self.cash -= total_cost
        
        # 如果已經持有該股票，更新持倉（以最新訊號重新判斷）
        if ticker in self.positions:
            old_position = self.positions[ticker]
            old_shares = old_position['shares']
            old_entry_price = old_position['entry_price']
            
            # 計算加權平均成本
            total_shares = old_shares + shares
            weighted_price = (old_entry_price * old_shares + order_price * shares) / total_shares
            
            self.positions[ticker] = {
                'shares': total_shares,
                'entry_date': date,
                'entry_price': weighted_price,
                'entry_signal_date': signal_date,
                'stock_name': stock_name
            }
        else:
            self.positions[ticker] = {
                'shares': shares,
                'entry_date': date,
                'entry_price': order_price,
                'entry_signal_date': signal_date,
                'stock_name': stock_name
            }
        
        # 記錄交易
        self.trades.append({
            'date': date,
            'action': 'BUY',
            'ticker': ticker,
            'stock_name': stock_name,
            'shares': shares,
            'price': order_price,
            'value': trade_value,
            'commission': commission,
            'total_cost': total_cost,
            'signal_date': signal_date,
            'is_odd_lot': is_odd_lot,
            'order_type': 'market_order'  # 標記為市價單（符合 TEJ 版本）
        })
        
        return True
    
    def place_stop_loss_order(self, ticker, entry_price, shares):
        """
        掛停損單（-10%）
        
        參數:
        - ticker: 股票代號
        - entry_price: 進場價格（加權平均成本）
        - shares: 股數
        """
        stop_loss_price = entry_price * (1 - self.stop_loss)
        self.stop_loss_orders[ticker] = {
            'stop_loss_price': stop_loss_price,
            'shares': shares,
            'entry_price': entry_price
        }
    
    def check_stop_loss_orders(self, date):
        """
        檢查停損單是否觸發
        
        邏輯:
        - 如果當日最低價 <= 停損價格，則觸發停損（以停損價格成交）
        """
        if not self.stop_loss_orders:
            return
        
        conn = sqlite3.connect(self.db_path)
        triggered_orders = []
        
        for ticker, order_info in list(self.stop_loss_orders.items()):
            if ticker not in self.positions:
                # 如果已經出場，移除停損單
                del self.stop_loss_orders[ticker]
                continue
            
            # 從 tw_stock_price_data 表取得當日最低價
            query = """
            SELECT low
            FROM tw_stock_price_data
            WHERE ticker = ? AND date = ?
            LIMIT 1
            """
            cursor = conn.cursor()
            cursor.execute(query, (ticker, date))
            result = cursor.fetchone()
            
            if result and result[0]:
                low_price = result[0]
                # 如果最低價 <= 停損價格，觸發停損
                if low_price <= order_info['stop_loss_price']:
                    triggered_orders.append((ticker, order_info['stop_loss_price']))
        
        conn.close()
        
        # 執行停損
        for ticker, stop_loss_price in triggered_orders:
            if ticker in self.positions:
                self.sell_stock(date, ticker, stop_loss_price, 'stop_loss')
                if ticker in self.stop_loss_orders:
                    del self.stop_loss_orders[ticker]
    
    def buy_stock(self, date, ticker, stock_name, price, signal_date):
        """
        買進股票
        
        參數:
        - date: 買進日期（實際執行日期，通常是 signal_date 的隔日）
        - ticker: 股票代號
        - stock_name: 股票名稱
        - price: 買進價格（開盤價）
        - signal_date: 訊號產生日期
        """
        # 計算可用的資金（1/10 的現金）
        available_cash = self.cash * self.position_size_ratio
        
        # 計算可買的股數（先以整張計算）
        shares_per_lot = 1000  # 1張 = 1000股
        order_value = available_cash
        shares = int(order_value // price)
        
        # 如果買不起整張，改買零股
        is_odd_lot = False
        if shares < shares_per_lot:
            # 零股交易
            is_odd_lot = True
            # 零股可以買任意股數
            shares = int(order_value // price)
        else:
            # 整張交易
            shares = (shares // shares_per_lot) * shares_per_lot
        
        if shares <= 0:
            return False
        
        # 計算實際交易金額
        trade_value = shares * price
        
        # 計算手續費
        commission = self.calculate_commission(trade_value, is_odd_lot=is_odd_lot)
        
        # 總成本
        total_cost = trade_value + commission
        
        # 檢查現金是否足夠
        if total_cost > self.cash:
            return False
        
        # 執行買進
        self.cash -= total_cost
        
        # 如果已經持有該股票，更新持倉（以最新訊號重新判斷）
        if ticker in self.positions:
            # 先賣出舊持倉（假設以當前價格賣出，但實際上我們會合併持倉）
            old_position = self.positions[ticker]
            # 合併持倉：更新為新的進場價格和日期
            old_shares = old_position['shares']
            old_entry_price = old_position['entry_price']
            
            # 計算加權平均成本
            total_shares = old_shares + shares
            weighted_price = (old_entry_price * old_shares + price * shares) / total_shares
            
            self.positions[ticker] = {
                'shares': total_shares,
                'entry_date': date,
                'entry_price': weighted_price,
                'entry_signal_date': signal_date,
                'stock_name': stock_name
            }
            
            # 更新停損單（如果啟用停損，以新的加權平均成本重新計算）
            if self.enable_stop_loss:
                self.place_stop_loss_order(ticker, weighted_price, total_shares)
        else:
            self.positions[ticker] = {
                'shares': shares,
                'entry_date': date,
                'entry_price': price,
                'entry_signal_date': signal_date,
                'stock_name': stock_name
            }
            
            # 掛停損單（如果啟用停損）
            if self.enable_stop_loss:
                self.place_stop_loss_order(ticker, price, shares)
        
        # 記錄交易
        self.trades.append({
            'date': date,
            'action': 'BUY',
            'ticker': ticker,
            'stock_name': stock_name,
            'shares': shares,
            'price': price,
            'value': trade_value,
            'commission': commission,
            'total_cost': total_cost,
            'signal_date': signal_date,
            'is_odd_lot': is_odd_lot
        })
        
        return True
    
    def sell_stock(self, date, ticker, price, reason):
        """
        賣出股票
        
        參數:
        - date: 賣出日期
        - ticker: 股票代號
        - price: 賣出價格
        - reason: 賣出原因（'take_profit', 'stop_loss', 'holding_period', 'rebalance'）
        """
        if ticker not in self.positions:
            return False
        
        position = self.positions[ticker]
        shares = position['shares']
        entry_price = position['entry_price']
        stock_name = position.get('stock_name', ticker)
        
        # 計算交易金額
        trade_value = shares * price
        
        # 計算手續費
        is_odd_lot = shares < 1000
        commission = self.calculate_commission(trade_value, is_odd_lot=is_odd_lot)
        
        # 計算證交稅（賣出時）
        # 判斷是否為當沖（當日買賣）
        is_day_trade = position['entry_date'] == date
        tax = self.calculate_tax(trade_value, is_day_trade=is_day_trade)
        
        # 實際收到金額
        net_proceeds = trade_value - commission - tax
        
        # 更新現金
        self.cash += net_proceeds
        
        # 計算損益
        total_cost = entry_price * shares
        pnl = net_proceeds - total_cost
        pnl_pct = (pnl / total_cost) * 100 if total_cost > 0 else 0
        
        # 記錄交易
        self.trades.append({
            'date': date,
            'action': 'SELL',
            'ticker': ticker,
            'stock_name': stock_name,
            'shares': shares,
            'price': price,
            'entry_price': entry_price,
            'value': trade_value,
            'commission': commission,
            'tax': tax,
            'net_proceeds': net_proceeds,
            'pnl': pnl,
            'pnl_pct': pnl_pct,
            'reason': reason,
            'holding_days': self.get_holding_days(position['entry_date'], date)
        })
        
        # 移除持倉
        del self.positions[ticker]
        
        # 移除停損單（如果存在）
        if ticker in self.stop_loss_orders:
            del self.stop_loss_orders[ticker]
        
        return True
    
    def get_holding_days(self, entry_date, current_date):
        """計算持有天數（交易日）"""
        conn = sqlite3.connect(self.db_path)
        query = """
        SELECT COUNT(*) as days
        FROM (
            SELECT DISTINCT date 
            FROM strategy_result 
            WHERE date > ? AND date <= ?
            ORDER BY date
        )
        """
        cursor = conn.cursor()
        cursor.execute(query, (entry_date, current_date))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else 0
    
    def check_exit_conditions(self, date, ticker, current_price, position):
        """
        檢查是否符合出場條件
        
        回傳: (should_exit, reason)
        """
        entry_price = position['entry_price']
        entry_date = position['entry_date']
        
        # 計算報酬率
        return_pct = (current_price - entry_price) / entry_price
        
        # 停利條件（如果啟用）
        if self.enable_take_profit and return_pct >= self.take_profit:
            return True, 'take_profit'
        
        # 停損條件（如果啟用）
        if self.enable_stop_loss and return_pct <= -self.stop_loss:
            return True, 'stop_loss'
        
        # 持有期滿
        holding_days = self.get_holding_days(entry_date, date)
        if holding_days >= self.holding_period:
            return True, 'holding_period'
        
        return False, None
    
    def get_portfolio_value(self, date):
        """計算投資組合總價值（現金 + 持倉市值）"""
        conn = sqlite3.connect(self.db_path)
        
        total_value = self.cash
        
        for ticker, position in self.positions.items():
            # 取得當日收盤價
            query = """
            SELECT close_price 
            FROM strategy_result 
            WHERE ticker = ? AND date = ?
            LIMIT 1
            """
            cursor = conn.cursor()
            cursor.execute(query, (ticker, date))
            result = cursor.fetchone()
            
            if result and result[0]:
                current_price = result[0]
                total_value += position['shares'] * current_price
        
        conn.close()
        return total_value
    
    def run_backtest(self, start_date='20200101', end_date='20251117'):
        """
        執行回測
        
        參數:
        - start_date: 開始日期（YYYYMMDD）
        - end_date: 結束日期（YYYYMMDD）
        """
        print("=" * 80)
        print("融資維持率策略回測系統")
        print("=" * 80)
        print(f"\n回測期間: {start_date} - {end_date}")
        print(f"初始資金: NT$ {self.initial_capital:,.0f}")
        print(f"每次進場資金比例: {self.position_size_ratio*100}%")
        if self.enable_take_profit:
            print(f"停利: +{self.take_profit*100}%")
        else:
            print(f"停利: 未啟用")
        if self.enable_stop_loss:
            print(f"停損: -{self.stop_loss*100}%")
        else:
            print(f"停損: 未啟用")
        print(f"持有期: {self.holding_period} 個交易日")
        print("\n" + "=" * 80)
        
        # 取得交易日列表
        trading_dates = self.get_trading_dates(start_date, end_date)
        print(f"\n[Info] 共 {len(trading_dates)} 個交易日")
        
        if len(trading_dates) == 0:
            print("[Error] 沒有找到交易日資料，請先執行資料更新和滾動計算")
            return None
        
        # 連接資料庫
        conn = sqlite3.connect(self.db_path)
        
        # 逐日回測
        signals_today = {}  # 記錄當日產生的訊號 {ticker: data_row}
        
        for i, date in enumerate(trading_dates):
            if (i + 1) % 100 == 0:
                print(f"  進度: {i + 1}/{len(trading_dates)} ({((i+1)/len(trading_dates)*100):.1f}%)")
            
            # 1. 先檢查進場訊號（當日收盤後判斷），如果有新訊號且在15日內，先更新持倉
            query = """
            SELECT 
                ticker,
                stock_name,
                margin_ratio,
                avg_10day_ratio,
                volume,
                avg_10day_volume,
                open_price,
                close_price,
                margin_balance_shares,
                avg_5day_balance_95
            FROM strategy_result
            WHERE date = ?
                AND margin_ratio IS NOT NULL
                AND avg_10day_ratio IS NOT NULL
                AND volume IS NOT NULL
                AND avg_10day_volume IS NOT NULL
                AND open_price IS NOT NULL
                AND close_price IS NOT NULL
                AND margin_balance_shares > 0
                AND avg_5day_balance_95 IS NOT NULL
            """
            
            df = pd.read_sql_query(query, conn, params=(date,))
            
            # 使用新的兩階段篩選邏輯
            signals_df = self.get_entry_signals(date, df, top_n=10)
            
            # 處理進場訊號（如果有新訊號且在15日內，先更新持倉）
            if not signals_df.empty and i < len(trading_dates) - 1:
                next_date = trading_dates[i + 1]
                
                for _, signal_row in signals_df.iterrows():
                    ticker = signal_row['ticker']
                    
                    # 檢查是否已在持倉中（如果在15日內有新訊號，再次買進加碼）
                    if ticker in self.positions:
                        position = self.positions[ticker]
                        entry_signal_date = position['entry_signal_date']
                        holding_days = self.get_holding_days(entry_signal_date, date)
                        
                        # 如果在15個交易日內，以最新訊號重新判斷（再次買進加碼）
                        if holding_days < self.holding_period:
                            # 取得隔日開盤價並執行買進（加碼）
                            query_next = """
                            SELECT open_price 
                            FROM strategy_result 
                            WHERE ticker = ? AND date = ?
                            LIMIT 1
                            """
                            cursor = conn.cursor()
                            cursor.execute(query_next, (ticker, next_date))
                            result = cursor.fetchone()
                            
                            if result and result[0]:
                                next_open_price = result[0]
                                # 再次買進（加碼），buy_stock 會自動計算加權平均成本並更新持倉
                                self.buy_stock(
                                    next_date, 
                                    ticker, 
                                    signal_row['stock_name'], 
                                    next_open_price,
                                    date  # 新的訊號產生日期
                                )
                    else:
                        # 新進場：取得隔日開盤價並執行買進（市價單）
                        query_next = """
                        SELECT open_price 
                        FROM strategy_result 
                        WHERE ticker = ? AND date = ?
                        LIMIT 1
                        """
                        cursor = conn.cursor()
                        cursor.execute(query_next, (ticker, next_date))
                        result = cursor.fetchone()
                        
                        if result and result[0]:
                            next_open_price = result[0]
                            self.buy_stock(
                                next_date, 
                                ticker, 
                                signal_row['stock_name'], 
                                next_open_price,
                                date  # 訊號產生日期
                            )
            
            # 2. 檢查停損單是否觸發（如果啟用停損，優先檢查）
            if self.enable_stop_loss:
                self.check_stop_loss_orders(date)
            
            # 3. 檢查持倉是否需要出場（基於更新後的 entry_date）
            positions_to_exit = []
            for ticker, position in list(self.positions.items()):
                # 取得當日收盤價
                query = """
                SELECT close_price 
                FROM strategy_result 
                WHERE ticker = ? AND date = ?
                LIMIT 1
                """
                cursor = conn.cursor()
                cursor.execute(query, (ticker, date))
                result = cursor.fetchone()
                
                if result and result[0]:
                    current_price = result[0]
                    should_exit, reason = self.check_exit_conditions(date, ticker, current_price, position)
                    if should_exit:
                        positions_to_exit.append((ticker, current_price, reason))
            
            # 執行出場
            for ticker, price, reason in positions_to_exit:
                self.sell_stock(date, ticker, price, reason)
            
            # 6. 記錄每日投資組合價值
            portfolio_value = self.get_portfolio_value(date)
            self.daily_portfolio_value.append({
                'date': date,
                'portfolio_value': portfolio_value,
                'cash': self.cash,
                'positions_count': len(self.positions)
            })
        
        conn.close()
        
        # 在回測結束時，保留所有持倉（不賣出）
        final_date = trading_dates[-1]
        if len(self.positions) > 0:
            print(f"\n[Info] 回測結束時仍有 {len(self.positions)} 檔股票持倉，將保留在投資組合價值中")
            # 記錄最終持倉資訊
            for ticker, position in self.positions.items():
                query = """
                SELECT close_price 
                FROM strategy_result 
                WHERE ticker = ? AND date = ?
                LIMIT 1
                """
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute(query, (ticker, final_date))
                result = cursor.fetchone()
                conn.close()
                
                if result and result[0]:
                    final_price = result[0]
                    position_value = position['shares'] * final_price
                    print(f"  - {ticker} {position.get('stock_name', '')}: {position['shares']} 股 @ {final_price:.2f} = NT$ {position_value:,.0f}")
        
        print("\n[Info] 回測完成！")
        
        return self.generate_report()
    
    def generate_report(self):
        """產生回測報告"""
        print("\n" + "=" * 80)
        print("回測結果報告")
        print("=" * 80)
        
        # 基本統計
        trades_df = pd.DataFrame(self.trades)
        buy_trades = trades_df[trades_df['action'] == 'BUY']
        sell_trades = trades_df[trades_df['action'] == 'SELL']
        
        print(f"\n【基本統計】")
        print(f"總交易次數: {len(buy_trades)} 筆買進，{len(sell_trades)} 筆賣出")
        print(f"最終現金: NT$ {self.cash:,.0f}")
        
        # 計算最終投資組合價值
        total_return = 0
        if self.daily_portfolio_value:
            final_value = self.daily_portfolio_value[-1]['portfolio_value']
            total_return = (final_value - self.initial_capital) / self.initial_capital * 100
            print(f"最終投資組合價值: NT$ {final_value:,.0f}")
            print(f"總報酬率: {total_return:.2f}%")
        
        # 賣出交易統計
        if len(sell_trades) > 0:
            print(f"\n【賣出交易統計】")
            print(f"總損益: NT$ {sell_trades['pnl'].sum():,.0f}")
            print(f"平均報酬率: {sell_trades['pnl_pct'].mean():.2f}%")
            print(f"勝率: {(sell_trades['pnl'] > 0).sum() / len(sell_trades) * 100:.2f}%")
            if (sell_trades['pnl'] > 0).sum() > 0:
                print(f"平均獲利: NT$ {sell_trades[sell_trades['pnl'] > 0]['pnl'].mean():,.0f}")
            if (sell_trades['pnl'] <= 0).sum() > 0:
                print(f"平均虧損: NT$ {sell_trades[sell_trades['pnl'] <= 0]['pnl'].mean():,.0f}")
            
            # 按出場原因統計
            print(f"\n【出場原因統計】")
            exit_reasons = sell_trades['reason'].value_counts()
            for reason, count in exit_reasons.items():
                reason_name = {
                    'take_profit': '停利',
                    'stop_loss': '停損',
                    'holding_period': '持有期滿',
                    'backtest_end': '回測結束',
                    'rebalance': '重新平衡'
                }.get(reason, reason)
                avg_pnl = sell_trades[sell_trades['reason'] == reason]['pnl_pct'].mean()
                print(f"  {reason_name}: {count} 筆，平均報酬率 {avg_pnl:.2f}%")
        
        # 計算夏普比率
        if len(self.daily_portfolio_value) > 1:
            portfolio_df = pd.DataFrame(self.daily_portfolio_value)
            portfolio_df['date'] = pd.to_datetime(portfolio_df['date'], format='%Y%m%d')
            portfolio_df = portfolio_df.sort_values('date')
            portfolio_df['daily_return'] = portfolio_df['portfolio_value'].pct_change()
            
            daily_returns = portfolio_df['daily_return'].dropna()
            if len(daily_returns) > 0 and daily_returns.std() > 0:
                sharpe_ratio = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252)
                print(f"\n【風險指標】")
                print(f"夏普比率: {sharpe_ratio:.4f}")
                print(f"最大回落: {self.calculate_max_drawdown(portfolio_df):.2f}%")
        
        # 儲存交易記錄
        if len(trades_df) > 0:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            trades_file = f'backtest_trades_{timestamp}.csv'
            trades_df.to_csv(trades_file, index=False, encoding='utf-8-sig')
            print(f"\n[Info] 交易記錄已儲存至: {trades_file}")
        
        # 繪製績效圖表
        self.plot_performance()
        
        return {
            'trades': trades_df,
            'daily_portfolio_value': pd.DataFrame(self.daily_portfolio_value),
            'final_value': self.daily_portfolio_value[-1]['portfolio_value'] if self.daily_portfolio_value else self.initial_capital,
            'total_return': total_return
        }
    
    def calculate_max_drawdown(self, portfolio_df):
        """計算最大回落"""
        portfolio_df = portfolio_df.copy()
        portfolio_df['cummax'] = portfolio_df['portfolio_value'].cummax()
        portfolio_df['drawdown'] = (portfolio_df['portfolio_value'] - portfolio_df['cummax']) / portfolio_df['cummax'] * 100
        return portfolio_df['drawdown'].min()
    
    def plot_performance(self):
        """繪製績效圖表"""
        if not self.daily_portfolio_value:
            return
        
        portfolio_df = pd.DataFrame(self.daily_portfolio_value)
        portfolio_df['date'] = pd.to_datetime(portfolio_df['date'], format='%Y%m%d')
        portfolio_df = portfolio_df.sort_values('date')
        
        fig, axes = plt.subplots(2, 1, figsize=(14, 10))
        
        # 圖1: 投資組合價值變化
        ax1 = axes[0]
        ax1.plot(portfolio_df['date'], portfolio_df['portfolio_value'], label='投資組合價值', linewidth=2)
        ax1.axhline(y=self.initial_capital, color='r', linestyle='--', label='初始資金')
        ax1.set_xlabel('日期')
        ax1.set_ylabel('價值 (NT$)')
        ax1.set_title('投資組合價值變化')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
        
        # 圖2: 現金與持倉數量
        ax2 = axes[1]
        ax2_twin = ax2.twinx()
        ax2.plot(portfolio_df['date'], portfolio_df['cash'], label='現金', color='green', linewidth=2)
        ax2_twin.plot(portfolio_df['date'], portfolio_df['positions_count'], label='持倉數量', color='blue', linewidth=2, linestyle='--')
        ax2.set_xlabel('日期')
        ax2.set_ylabel('現金 (NT$)', color='green')
        ax2_twin.set_ylabel('持倉數量', color='blue')
        ax2.set_title('現金與持倉變化')
        ax2.legend(loc='upper left')
        ax2_twin.legend(loc='upper right')
        ax2.grid(True, alpha=0.3)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
        
        plt.tight_layout()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f'backtest_performance_{timestamp}.png'
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"[Info] 績效圖表已儲存至: {output_file}")
        
        plt.close()


def main():
    """主程式"""
    import argparse
    
    parser = argparse.ArgumentParser(description='融資維持率策略回測系統')
    parser.add_argument('--db', default='taiwan_stock.db', help='資料庫路徑')
    parser.add_argument('--start-date', default='20200101', help='開始日期 (YYYYMMDD)')
    parser.add_argument('--end-date', default='20251117', help='結束日期 (YYYYMMDD)')
    parser.add_argument('--capital', type=float, default=1000000, help='初始資金（新台幣）')
    parser.add_argument('--no-take-profit', action='store_true', help='停用停利（無止盈）')
    parser.add_argument('--no-stop-loss', action='store_true', help='停用停損（無止損）')
    
    args = parser.parse_args()
    
    # 建立回測系統
    backtest = MarginRatioBacktest(
        db_path=args.db, 
        initial_capital=args.capital,
        enable_take_profit=not args.no_take_profit,
        enable_stop_loss=not args.no_stop_loss
    )
    
    # 執行回測
    results = backtest.run_backtest(
        start_date=args.start_date,
        end_date=args.end_date
    )
    
    print("\n回測完成！")


if __name__ == '__main__':
    main()

