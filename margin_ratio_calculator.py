"""
台股融資維持率計算工具
功能:
1. 從證交所API取得融資融券資料
2. 從證交所API取得股價資料
3. 計算融資維持率
4. 儲存到SQLite資料庫
"""

import os
import sys
import re
import requests
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import time
import json
import pandas_market_calendars as pmc


def setup_console():
    """確保在 Windows 終端機也能正確顯示 UTF-8 文字。"""
    if os.name == 'nt':  # 僅在 Windows 執行編碼調整
        try:
            os.system('chcp 65001 > NUL')  # 將主控台切換為 UTF-8
        except Exception:
            pass
        for stream in (sys.stdout, sys.stderr):
            if hasattr(stream, "reconfigure"):
                try:
                    stream.reconfigure(encoding='utf-8')  # 讓 stdout/stderr 使用 UTF-8
                except ValueError:
                    pass


setup_console()  # 程式啟動時立即設定輸出編碼，避免中文亂碼

class MarginRatioCalculator:
    """融資維持率計算器"""
    
    def __init__(self, db_path='taiwan_stock.db', loan_ratio=0.6, mysql_config=None, config_path='config.ini'):
        """
        初始化
        
        參數:
        - db_path: SQLite 資料庫路徑（預設: 'taiwan_stock.db'）
        - loan_ratio: 融資成數（預設 0.6，即 60%）
        - mysql_config: MySQL 連接設定（字典），格式如下：
            {
                'host': 'localhost',
                'port': 3306,
                'user': 'root',
                'password': 'your_password',
                'database': 'taiwan_stock'
            }
            如果為 None，則只使用 SQLite
        - config_path: 玉山證券 API 設定檔路徑（預設: 'config.ini'）
        """
        self.db_path = db_path  # 儲存 SQLite 檔案路徑，方便日後查詢
        self.loan_ratio = loan_ratio  # 假設券商融資成數 60%，可依實務情況調整
        self.mysql_config = mysql_config  # MySQL 連接設定
        self.mysql_enabled = mysql_config is not None  # 是否啟用 MySQL
        self.config_path = config_path  # 儲存設定檔路徑，用於重新讀取
        
        # 收集回溯計算失敗的警告（用於匯出 CSV）
        self.backward_calc_warnings = []
        
        # 初始化玉山證券 API 客戶端（不立即登入，避免超過每日300次限制）
        self.esun_client = None
        self.esun_logged_in = False
        if config_path and os.path.exists(config_path):
            try:
                from configparser import ConfigParser
                from esun_marketdata import EsunMarketdata
                
                config = ConfigParser()
                config.read(config_path)
                self.esun_client = EsunMarketdata(config)
                print("[Info] 玉山證券 API 客戶端初始化成功（未登入）")
            except Exception as e:
                print(f"[Warning] 玉山證券 API 客戶端初始化失敗: {e}")
                print("[Info] 將只使用證交所 API")
        
        self.init_database()
        
    def init_database(self):
        """建立資料庫表格（SQLite 和 MySQL）- 三張表設計"""
        # SQLite 初始化
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 第一張表：證交所融資融券資料（原始資料）
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS twse_margin_data (
                date TEXT NOT NULL,
                ticker TEXT NOT NULL,
                stock_name TEXT,
                margin_balance_shares INTEGER,
                margin_prev_balance INTEGER,
                margin_buy_shares INTEGER,
                margin_sell_shares INTEGER,
                margin_cash_repay_shares INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (date, ticker)
            )
        ''')
        
        # 第二張表：證交所股價資料（原始資料）
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tw_stock_price_data (
                date TEXT NOT NULL,
                ticker TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                turnover REAL,
                change REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (date, ticker)
            )
        ''')
        
        # 第三張表：策略結果表（計算後的資料）
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS strategy_result (
                date TEXT NOT NULL,
                ticker TEXT NOT NULL,
                stock_name TEXT,
                margin_ratio REAL,
                margin_cost_est REAL,
                margin_balance_amount REAL,
                margin_balance_shares INTEGER,
                avg_10day_ratio REAL,
                volume INTEGER,
                avg_10day_volume REAL,
                open_price REAL,
                close_price REAL,
                avg_5day_balance_95 REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (date, ticker)
            )
        ''')
        
        # 為了向後相容，保留舊的 margin_data 表（如果存在）
        # 但新資料將寫入三張新表
        
        conn.commit()
        conn.close()
        print(f"[Info] SQLite 資料庫初始化完成: {self.db_path}")
        print(f"[Info] 已建立三張表: twse_margin_data, tw_stock_price_data, strategy_result")
        
        # MySQL 初始化（如果啟用）
        if self.mysql_enabled:
            try:
                import pymysql
                mysql_conn = pymysql.connect(**self.mysql_config)
                mysql_cursor = mysql_conn.cursor()
                
                # 第一張表：證交所融資融券資料（原始資料）
                mysql_cursor.execute('''
                    CREATE TABLE IF NOT EXISTS twse_margin_data (
                        date VARCHAR(8) NOT NULL,
                        ticker VARCHAR(10) NOT NULL,
                        stock_name VARCHAR(100),
                        margin_balance_shares INT,
                        margin_prev_balance INT,
                        margin_buy_shares INT,
                        margin_sell_shares INT,
                        margin_cash_repay_shares INT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (date, ticker)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                ''')
                
                # 第二張表：證交所股價資料（原始資料）
                mysql_cursor.execute('''
                    CREATE TABLE IF NOT EXISTS tw_stock_price_data (
                        date VARCHAR(8) NOT NULL,
                        ticker VARCHAR(10) NOT NULL,
                        open DECIMAL(10, 2),
                        high DECIMAL(10, 2),
                        low DECIMAL(10, 2),
                        close DECIMAL(10, 2),
                        volume BIGINT,
                        turnover DECIMAL(15, 2),
                        `change` DECIMAL(10, 2),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (date, ticker)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                ''')
                
                # 第三張表：策略結果表（計算後的資料）
                mysql_cursor.execute('''
                    CREATE TABLE IF NOT EXISTS strategy_result (
                        date VARCHAR(8) NOT NULL,
                        ticker VARCHAR(10) NOT NULL,
                        stock_name VARCHAR(100),
                        margin_ratio DECIMAL(10, 4),
                        margin_cost_est DECIMAL(10, 4),
                        margin_balance_amount DECIMAL(15, 2),
                        margin_balance_shares INT,
                        avg_10day_ratio DECIMAL(10, 4),
                        volume BIGINT,
                        avg_10day_volume BIGINT,
                        open_price DECIMAL(10, 2),
                        close_price DECIMAL(10, 2),
                        avg_5day_balance_95 DECIMAL(15, 2),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (date, ticker)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                ''')
                
                mysql_conn.commit()
                mysql_cursor.close()
                mysql_conn.close()
                print(f"[Info] MySQL 資料庫初始化完成: {self.mysql_config.get('database', 'unknown')}")
                print(f"[Info] 已建立三張表: twse_margin_data, tw_stock_price_data, strategy_result")
            except ImportError:
                print("[Warning] pymysql 未安裝，無法使用 MySQL。請執行: pip install pymysql")
                self.mysql_enabled = False
            except Exception as e:
                print(f"[Warning] MySQL 初始化失敗: {e}，將只使用 SQLite")
                self.mysql_enabled = False
    
    def esun_login(self, retry_on_failure=False):
        """
        登入玉山證券 API（只登入一次，避免超過每日300次限制）
        
        參數:
        - retry_on_failure: 登入失敗時是否允許重新輸入密碼（預設 False，避免自動重試導致帳號被鎖）
        
        回傳:
        - True: 登入成功或已登入
        - False: 登入失敗
        """
        if self.esun_client is None:
            print("[Error] 玉山證券 API 客戶端未初始化，請檢查 config.ini 是否存在")
            return False
        
        # 如果已經登入，直接返回
        if self.esun_logged_in:
            return True
        
        # 只嘗試一次，避免自動重試導致帳號被鎖
        try:
            # 直接調用 login()，讓玉山 API 顯示原始訊息並處理密碼輸入
            self.esun_client.login()
            self.esun_logged_in = True
            print("[Info] 玉山證券 API 登入成功")
            return True
            
        except KeyboardInterrupt:
            print("\n[Info] 使用者取消輸入")
            return False
        except Exception as e:
            # 直接顯示玉山證券 API 的原始錯誤訊息
            error_msg = str(e)
            print(f"\n{error_msg}")
            print("\n[警告] 登入失敗，請檢查密碼是否正確")
            print("[提示] 如需重新嘗試，請重新執行程式（避免自動重試導致帳號被鎖）")
            return False
    
    def esun_logout(self):
        """
        登出玉山證券 API（在不需要時才登出）
        """
        if self.esun_client and self.esun_logged_in:
            try:
                self.esun_client.logout()
                self.esun_logged_in = False
                print("[Info] 玉山證券 API 已登出")
            except Exception as e:
                print(f"[Warning] 玉山證券 API 登出失敗: {e}")
    
    def is_open_trading_day(self, date):
        """
        傳入字串 YYYYMMDD，回傳該日是否為台股開盤日。
        """
        cal = pmc.get_calendar('XTAI')  # 台灣證券交易所官方曆
        dt = pd.Timestamp(date)
        return cal.valid_days(start_date=dt, end_date=dt).size > 0

    def get_last_trading_day(self, date=None):
        """
        給定日期（預設今天），自動向前搜尋最近一個台股開市日。
        """
        cal = pmc.get_calendar('XTAI')
        if date is None:
            date = datetime.now().strftime('%Y%m%d')
        dt = pd.Timestamp(date)
        opened_days = cal.valid_days(end_date=dt, start_date=dt - pd.Timedelta(days=30))
        if len(opened_days) == 0:
            raise Exception(f"找不到最近一個月內的台股交易日: {date}")
        last_day = opened_days[-1].strftime('%Y%m%d')
        return last_day

    def resolve_trade_date(self, date=None):
        """
        利用台股官方日曆決定查詢交易日：
        - 若傳入指定日期，驗證是否開盤
        - 若無，回傳今天前最靠近的開盤日（可避開休市/特殊假日/颱風等情境）
        """
        if date is not None:
            if self.is_open_trading_day(date):
                return date
            # 若指定日不是開盤日，自動回推
            print(f"[Info] 指定日({date})不是開盤日，自動查最近交易日")
            return self.get_last_trading_day(date)
        # 預設用今天，若非開盤也往前回推
        today = datetime.now().strftime('%Y%m%d')
        return self.get_last_trading_day(today)
    
    def generate_date_candidates(self, start_date, max_back=5):
        """
        產生最多 max_back 個依序往前的工作天日期字串，用於遇到 API 尚未更新時往回補抓資料。
        """
        current = datetime.strptime(start_date, '%Y%m%d')
        candidates = []
        while len(candidates) < max_back:
            if current.weekday() < 5:  # 週一~週五
                candidates.append(current.strftime('%Y%m%d'))
            current -= timedelta(days=1)
        return candidates
    
    def load_previous_snapshot(self, date):
        """
        取得指定日期前最新一筆融資資料，用於推算融資本金。
        回傳格式: { ticker: {'amount': float, 'shares': int} }
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT ticker, margin_balance_amount, margin_balance_shares
            FROM strategy_result
            WHERE date = (
                SELECT MAX(date) FROM strategy_result WHERE date < ?
            )
            """,
            (date,),
        )
        records = {
            ticker: {
                'amount': amount if amount is not None else 0.0,
                'shares': shares if shares is not None else 0
            }
            for ticker, amount, shares in cursor.fetchall()
        }  # 將前一交易日的金額、張數記錄成字典
        conn.close()
        return records
    
    def fetch_margin_data(self, date=None):
        """
        透過證交所官方網站的 JSON 介面取得融資融券個股資料。
        API 範例: https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date=YYYYMMDD&selectType=STOCK
        
        回傳:
            (df, actual_date, summary_info)
            df 為整理後的 DataFrame，欄位使用中文名稱（重複欄位已加上前綴）:
                代號, 名稱, 融資買進, 融資賣出, 融資現金償還, 融資前日餘額, 融資今日餘額,
                融券買進, 融券賣出, 融券現券償還, 融券前日餘額, 融券今日餘額, ...
            actual_date 為實際成功取得資料的日期字串 YYYYMMDD
            summary_info 含有彙總資訊（可選，不影響主要計算）
        """
        base_date = self.resolve_trade_date(date)
        user_specified = date is not None
        candidates = [base_date] if user_specified else self.generate_date_candidates(base_date)
        
        url = "https://www.twse.com.tw/exchangeReport/MI_MARGN"
        
        for query_date in candidates:
            try:
                print(f"[Info] 正在取得 {query_date} 的融資融券資料...")  # 紀錄請求的目標日期
                response = requests.get(
                    url,
                    params={
                        'response': 'json',
                        'date': query_date,
                        'selectType': 'STOCK'
                    },
                    timeout=15
                )
                response.raise_for_status()
                
                payload = response.json()
                if payload.get('stat') != 'OK':
                    print(f"[Warning] {query_date} 尚未提供完整資料: {payload.get('stat')}")  # 可能是尚未更新或非交易日
                    if user_specified:
                        break
                    continue
                
                tables = payload.get('tables', [])
                
                summary_info = {}
                if tables:
                    try:
                        summary_rows = tables[0].get('data', [])
                        total_amount_str = summary_rows[2][5] if len(summary_rows) > 2 and len(summary_rows[2]) > 5 else None
                        if total_amount_str:
                            total_amount = float(str(total_amount_str).replace(',', '')) * 1000
                            summary_info['total_margin_amount'] = total_amount
                    except (KeyError, ValueError, IndexError, TypeError):
                        pass
                
                data_table = next((tbl for tbl in tables if 'data' in tbl and tbl.get('data')), None)
                if not data_table:
                    print(f"[Warning] {query_date} 無個股明細資料")  # 若查無個股資料則換下一天
                    if user_specified:
                        break
                    continue
                
                raw_df = pd.DataFrame(data_table['data'], columns=data_table['fields'])
                if raw_df.empty:
                    print(f"[Warning] {query_date} 取得資料筆數為 0")  # 無資料筆數
                    if user_specified:
                        break
                    continue
                
                # 重新命名欄位，保留中文名稱，在重複欄位前加上「融資」、「融券」前綴
                # 原始欄位順序：["代號", "名稱", "買進", "賣出", "現金償還", "前日餘額", "今日餘額", "次一營業日限額",
                #                "買進", "賣出", "現券償還", "前日餘額", "今日餘額", "次一營業日限額", "資券互抵", "註記"]
                renamed_columns = [
                    '代號', '名稱',  # 前兩個不變
                    # 融資相關（第3-8欄位）
                    '融資買進', '融資賣出', '融資現金償還', '融資前日餘額', '融資今日餘額', '融資次一營業日限額',
                    # 融券相關（第9-14欄位）
                    '融券買進', '融券賣出', '融券現券償還', '融券前日餘額', '融券今日餘額', '融券次一營業日限額',
                    # 最後兩個
                    '資券互抵', '註記'
                ]
                raw_df.columns = renamed_columns
                
                # 僅保留上市股票 (4 碼，且首碼不得為 0)，同時排除合計列
                raw_df['代號'] = raw_df['代號'].astype(str).str.strip()
                stock_filter = raw_df['代號'].str.fullmatch(r'[1-9]\d{3}[A-Z]?')
                clean_df = raw_df[stock_filter].copy()
                
                if clean_df.empty:
                    print(f"[Warning] {query_date} 篩選後無上市個股資料")
                    if user_specified:
                        break
                    continue
                
                clean_df['名稱'] = clean_df['名稱'].astype(str).str.strip()
                
                # 數值轉換（使用中文欄位名稱）
                numeric_cols = [
                    '融資買進', '融資賣出', '融資現金償還',
                    '融資前日餘額', '融資今日餘額'
                ]
                for col in numeric_cols:
                    if col in clean_df.columns:
                        clean_df[col] = pd.to_numeric(
                            clean_df[col].astype(str).str.replace(',', '').replace('', '0'),
                            errors='coerce'
                        ).fillna(0).astype(int)
                
                print(f"[Info] 成功取得 {len(clean_df)} 檔上市個股的融資融券資料")  # 告知成功筆數
                return clean_df, query_date, summary_info
            
            except requests.exceptions.RequestException as e:
                print(f"[Error] API 請求失敗 ({query_date}): {e}")  # 異常時顯示錯誤內容
                if user_specified:
                    break
            except json.JSONDecodeError as e:
                print(f"[Error] 無法解析 JSON ({query_date}): {e}")
                if user_specified:
                    break
        
        print("[Error] 無法取得融資融券資料，請確認日期或稍後再試")  # 若所有日期皆失敗則回報錯誤
        return None, None, {}
    
    def fetch_all_stocks_daily_data_from_twse(self, date):
        """
        從證交所 MI_INDEX API 取得指定日期的所有個股收盤行情（一次取得所有個股）
        
        參數:
        - date: 日期（YYYYMMDD，例如 '20251114'）
        
        回傳:
        - DataFrame 包含該日期所有個股的完整成交資訊，欄位: date, ticker, open, high, low, close, volume, turnover, change
        """
        url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
        
        try:
            response = requests.get(
                url,
                params={
                    'date': date,
                    'type': 'ALLBUT0999',  # 全部(不含權證、牛熊證)
                    'response': 'json'
                },
                timeout=10
            )
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('stat') != 'OK':
                print(f"[Error] API 回傳錯誤: {data.get('stat')}")
                return pd.DataFrame()
            
            # 找到個股資料表格（title 包含 "每日收盤行情(全部(不含權證、牛熊證))"）
            stock_table = None
            for table in data.get('tables', []):
                title = table.get('title', '')
                if '每日收盤行情' in title and '全部(不含權證、牛熊證)' in title:
                    stock_table = table
                    break
            
            if stock_table is None:
                print(f"[Error] 找不到個股資料表格")
                return pd.DataFrame()
            
            # 取得欄位定義
            fields = stock_table.get('fields', [])
            # fields: ["證券代號", "證券名稱", "成交股數", "成交筆數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "漲跌(+/-)", "漲跌價差", ...]
            
            # 定義欄位索引
            field_map = {
                'ticker': None,      # 證券代號
                'stock_name': None,   # 證券名稱
                'volume': None,       # 成交股數
                'turnover': None,     # 成交金額
                'open': None,         # 開盤價
                'high': None,         # 最高價
                'low': None,          # 最低價
                'close': None,        # 收盤價
                'change': None        # 漲跌價差
            }
            
            # 對應欄位索引
            for i, field in enumerate(fields):
                if field == '證券代號':
                    field_map['ticker'] = i
                elif field == '證券名稱':
                    field_map['stock_name'] = i
                elif field == '成交股數':
                    field_map['volume'] = i
                elif field == '成交金額':
                    field_map['turnover'] = i
                elif field == '開盤價':
                    field_map['open'] = i
                elif field == '最高價':
                    field_map['high'] = i
                elif field == '最低價':
                    field_map['low'] = i
                elif field == '收盤價':
                    field_map['close'] = i
                elif field == '漲跌價差':
                    field_map['change'] = i
            
            # 檢查必要欄位是否存在
            if field_map['ticker'] is None or field_map['close'] is None:
                print(f"[Error] 找不到必要欄位")
                return pd.DataFrame()
            
            # 解析資料
            records = []
            for row in stock_table.get('data', []):
                if len(row) <= max([v for v in field_map.values() if v is not None]):
                    continue
                
                ticker = str(row[field_map['ticker']]).strip()
                
                # 過濾出個股：排除 ETF（00 開頭）
                if ticker.startswith('00'):
                    continue
                
                # 只保留 4 位數字代號的個股
                if not (len(ticker) == 4 and ticker.isdigit()):
                    continue
                
                try:
                    # 轉換數值（移除逗號和特殊符號）
                    def safe_float(s):
                        if s is None or s == '':
                            return None
                        s_clean = str(s).replace(',', '').replace('--', '0').replace('+', '').strip()
                        # 處理 HTML 標籤（如 <p style= color:green>-</p>）
                        if '<' in s_clean:
                            # 提取數字部分
                            import re
                            numbers = re.findall(r'-?\d+\.?\d*', s_clean)
                            if numbers:
                                s_clean = numbers[0]
                            else:
                                return None
                        if s_clean == '' or s_clean == '-':
                            return None
                        return float(s_clean)
                    
                    def safe_int(s):
                        if s is None or s == '':
                            return None
                        s_clean = str(s).replace(',', '').replace('--', '0').strip()
                        if '<' in s_clean:
                            import re
                            numbers = re.findall(r'-?\d+', s_clean)
                            if numbers:
                                s_clean = numbers[0]
                            else:
                                return None
                        if s_clean == '':
                            return None
                        return int(float(s_clean))
                    
                    record = {
                        'date': date,
                        'ticker': ticker,
                        'open': safe_float(row[field_map['open']]) if field_map['open'] is not None else None,
                        'high': safe_float(row[field_map['high']]) if field_map['high'] is not None else None,
                        'low': safe_float(row[field_map['low']]) if field_map['low'] is not None else None,
                        'close': safe_float(row[field_map['close']]) if field_map['close'] is not None else None,
                        'volume': safe_int(row[field_map['volume']]) if field_map['volume'] is not None else None,
                        'turnover': safe_float(row[field_map['turnover']]) if field_map['turnover'] is not None else None,
                        'change': safe_float(row[field_map['change']]) if field_map['change'] is not None else None
                    }
                    
                    # 只保留有收盤價的資料
                    if record['close'] is not None:
                        records.append(record)
                        
                except (ValueError, TypeError, IndexError) as e:
                    continue
            
            df = pd.DataFrame(records)
            return df
            
        except Exception as e:
            print(f"[Error] 取得 {date} 的個股資料失敗: {e}")
            return pd.DataFrame()
    
    def fetch_stock_day_data_from_twse(self, ticker, year_month):
        """
        從證交所 API 取得指定股票在指定月份的完整成交資訊（使用 STOCK_DAY API）
        
        參數:
        - ticker: 股票代號（字串，例如 '1101'）
        - year_month: 年月（字串格式 YYYYMM，例如 '202511'）
        
        回傳:
        - DataFrame 包含該月份每日的完整成交資訊，欄位: date, ticker, open, high, low, close, volume, turnover, change
        """
        url = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
        
        # 轉換為月份第一天（例如 202511 -> 20251101）
        date_str = f"{year_month}01"
        
        try:
            response = requests.get(
                url,
                params={
                    'date': date_str,
                    'stockNo': ticker,
                    'response': 'json'
                },
                timeout=10
            )
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('stat') != 'OK':
                return pd.DataFrame()
            
            # 解析資料
            # fields: ["日期","成交股數","成交金額","開盤價","最高價","最低價","收盤價","漲跌價差","成交筆數"]
            records = []
            for row in data.get('data', []):
                if len(row) >= 9:
                    date_str_tw = row[0]  # 格式: "114/11/14"
                    volume_str = row[1]    # 成交股數
                    turnover_str = row[2]  # 成交金額
                    open_str = row[3]       # 開盤價
                    high_str = row[4]      # 最高價
                    low_str = row[5]       # 最低價
                    close_str = row[6]     # 收盤價
                    change_str = row[7]    # 漲跌價差
                    # row[8] 是成交筆數，我們不需要
                    
                    # 轉換日期格式：114/11/14 -> 20241114
                    parts = date_str_tw.split('/')
                    if len(parts) == 3:
                        roc_year = int(parts[0])
                        ad_year = roc_year + 1911
                        month = parts[1].zfill(2)
                        day = parts[2].zfill(2)
                        date_iso = f"{ad_year}{month}{day}"
                        
                        try:
                            # 轉換數值（移除逗號和特殊符號）
                            def safe_float(s):
                                if s is None or s == '':
                                    return None
                                s_clean = str(s).replace(',', '').replace('--', '0').replace('+', '').strip()
                                if s_clean == '' or s_clean == '-':
                                    return None
                                return float(s_clean)
                            
                            def safe_int(s):
                                if s is None or s == '':
                                    return None
                                s_clean = str(s).replace(',', '').replace('--', '0').strip()
                                if s_clean == '':
                                    return None
                                return int(float(s_clean))
                            
                            records.append({
                                'date': date_iso,
                                'ticker': ticker,
                                'open': safe_float(open_str),
                                'high': safe_float(high_str),
                                'low': safe_float(low_str),
                                'close': safe_float(close_str),
                                'volume': safe_int(volume_str),
                                'turnover': safe_float(turnover_str),
                                'change': safe_float(change_str)
                            })
                        except (ValueError, TypeError) as e:
                            continue
            
            df = pd.DataFrame(records)
            return df
            
        except Exception as e:
            print(f"[Error] 取得 {ticker} 在 {year_month} 的成交資訊失敗: {e}")
            return pd.DataFrame()
    
    def fetch_historical_stock_price(self, ticker, year_month):
        """
        從證交所 API 取得指定股票在指定月份的歷史收盤價（保留向後相容）
        
        參數:
        - ticker: 股票代號（字串，例如 '1101'）
        - year_month: 年月（字串格式 YYYYMM，例如 '202511'）
        
        回傳:
        - DataFrame 包含該月份每日的收盤價，欄位: date, ticker, closing_price
        """
        # 使用新的 STOCK_DAY API 取得完整資料，然後只提取收盤價
        df_full = self.fetch_stock_day_data_from_twse(ticker, year_month)
        if df_full.empty:
            return pd.DataFrame()
        
        # 轉換為舊格式（只包含收盤價）
        df = pd.DataFrame({
            'date': df_full['date'],
            'ticker': df_full['ticker'],
            'closing_price': df_full['close']
        })
        return df
    
    def fetch_stock_prices_by_months(self, ticker, year_months):
        """
        批次取得指定股票在多個月份的歷史收盤價
        
        參數:
        - ticker: 股票代號（字串，例如 '1101'）
        - year_months: 年月列表（例如 ['202508', '202509', '202510', '202511']）
        
        回傳:
        - DataFrame 包含所有月份的收盤價，欄位: date, ticker, closing_price
        """
        all_data = []
        
        for year_month in year_months:
            df_month = self.fetch_historical_stock_price(ticker, year_month)
            if not df_month.empty:
                all_data.append(df_month)
            time.sleep(5)  # 禮貌休息，避免請求過快
        
        if not all_data:
            return pd.DataFrame()
        
        # 合併所有月份的資料
        result = pd.concat(all_data, ignore_index=True)
        return result
    
    def get_required_months(self, start_date, end_date):
        """
        取得日期範圍內所需的所有月份（用於批次取得股價）
        
        參數:
        - start_date: 開始日期（YYYYMMDD）
        - end_date: 結束日期（YYYYMMDD）
        
        回傳:
        - 年月列表（例如 ['202508', '202509', '202510', '202511']）
        """
        start = pd.Timestamp(start_date)
        end = pd.Timestamp(end_date)
        
        months = []
        current = start.replace(day=1)  # 從月份第一天開始
        
        while current <= end:
            months.append(current.strftime('%Y%m'))
            # 移到下個月
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        
        return months
    
    def _fetch_latest_stock_price_all(self):
        """
        使用 STOCK_DAY_AVG_ALL API 取得最新一日的所有股票收盤價（只保留個股，排除 ETF）
        
        回傳:
        - DataFrame 包含收盤價，欄位: Code, ClosingPrice
        """
        url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_AVG_ALL"
        
        try:
            print("[Info] 正在取得最新一日的所有股票收盤價...")
            time.sleep(5)  # 禮貌休息
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            
            if not data:
                print("[Warning] 無法取得股價資料")
                return None
            
            # 轉換為 DataFrame
            df = pd.DataFrame(data)
            
            # 轉換日期格式：1141113 -> 20241113
            def convert_date(date_str):
                if len(date_str) == 7:  # 1141113
                    roc_year = int(date_str[:3])
                    ad_year = roc_year + 1911
                    return f"{ad_year}{date_str[3:]}"
                return date_str
            
            df['Date'] = df['Date'].apply(convert_date)
            
            # 過濾出個股：排除 ETF（00 開頭）和非 4 位數字代號
            df['Code'] = df['Code'].astype(str).str.strip()
            df = df[~df['Code'].str.startswith('00')]  # 排除 ETF（00 開頭）
            df = df[df['Code'].str.len() == 4]  # 只保留 4 位數字代號
            df = df[df['Code'].str.isdigit()]  # 確保是數字
            
            # 轉換收盤價為數值
            df['ClosingPrice'] = pd.to_numeric(
                df['ClosingPrice'].astype(str).str.replace(',', '').str.replace('--', '0'),
                errors='coerce'
            )
            
            # 重新命名欄位以符合舊格式
            df = df.rename(columns={'Code': 'Code', 'ClosingPrice': 'ClosingPrice'})
            
            print(f"[Info] 成功取得 {len(df)} 筆個股股價資料（已過濾 ETF）")
            return df[['Code', 'ClosingPrice']]
            
        except Exception as e:
            print(f"[Error] 取得最新股價失敗: {e}")
            return None

    def _get_price_from_cache(self, price_cache, date):
        """
        從快取中取得指定日期的股價資料
        
        參數:
        - price_cache: 股價快取字典 {ticker: DataFrame}
        - date: 日期（YYYYMMDD）
        
        回傳:
        - DataFrame 包含該日期的收盤價，欄位: Code, ClosingPrice
        """
        records = []
        
        for ticker, df_ticker in price_cache.items():
            df_date = df_ticker[df_ticker['date'] == date]
            if not df_date.empty:
                records.append({
                    'Code': ticker,
                    'ClosingPrice': df_date.iloc[0]['closing_price']
                })
        
        if not records:
            return None
        
        return pd.DataFrame(records)
    
    def fetch_historical_candles_from_esun(self, tickers, start_date, end_date, max_retries=5):
        """
        使用玉山證券 API 取得歷史 K 線資料（支援單一或批次股票代號）
        
        參數:
        - tickers: 股票代號列表（例如 ['2330', '0050']）或單一字串（例如 '2330'）
        - start_date: 開始日期（YYYYMMDD）
        - end_date: 結束日期（YYYYMMDD）
        - max_retries: 最大重試次數（預設 5 次）
        
        回傳:
        - DataFrame 包含 date, ticker, open, high, low, close, volume, turnover, change
        """
        if self.esun_client is None:
            return pd.DataFrame()
        
        # 確保已登入（只登入一次）
        if not self.esun_login():
            return pd.DataFrame()
        
        # 轉換日期格式：YYYYMMDD -> YYYY-MM-DD
        start = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
        end = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
        
        # 處理 tickers：統一轉換為列表
        if isinstance(tickers, str):
            tickers = [tickers]
        
        # 確保股票代號是 4 位數（補零）
        symbols = [str(ticker).zfill(4) for ticker in tickers]
        
        all_records = []
        
        # 單一請求模式（逐檔取得，確保穩定）
        for ticker, symbol in zip(tickers, symbols):
            for attempt in range(1, max_retries + 1):
                try:
                    rest_stock = self.esun_client.rest_client.stock
                    response = rest_stock.historical.candles(**{
                        "symbol": symbol,  # 單一股票
                        "from": start,
                        "to": end
                    })
                    
                    if response and 'data' in response and len(response['data']) > 0:
                        # 轉換為記錄
                        for candle in response['data']:
                            all_records.append({
                                'date': candle['date'].replace('-', ''),  # YYYY-MM-DD -> YYYYMMDD
                                'ticker': ticker,  # 使用原始代號（不補零）
                                'open': float(candle['open']),
                                'high': float(candle['high']),
                                'low': float(candle['low']),
                                'close': float(candle['close']),
                                'volume': int(candle['volume']),
                                'turnover': float(candle.get('turnover', 0)),  # 成交金額
                                'change': float(candle.get('change', 0))  # 漲跌
                            })
                        break  # 成功就跳出重試迴圈
                    else:
                        # 首次呼叫可能無資料，需要重試
                        if attempt < max_retries:
                            wait_time = attempt + 1  # 遞增等待時間
                            print(f"[Warning] {ticker} 首次呼叫無資料，{wait_time} 秒後重試（第 {attempt}/{max_retries} 次）...")
                            time.sleep(wait_time)
                        else:
                            print(f"[Error] {ticker} 無法取得資料，已重試 {max_retries} 次")
                            
                except Exception as e:
                    if attempt < max_retries:
                        wait_time = attempt + 1
                        print(f"[Error] {ticker} API 呼叫失敗: {e}")
                        print(f"[Info] {wait_time} 秒後重試（第 {attempt}/{max_retries} 次）...")
                        time.sleep(wait_time)
                    else:
                        print(f"[Error] {ticker} API 呼叫失敗: {e}")
                
                # 流量控制：每次請求後等待至少 1 秒（每分鐘最多 60 次）
                if attempt < max_retries:
                    time.sleep(1.5)
        
        if all_records:
            df = pd.DataFrame(all_records)
            # 按日期和股票代號排序
            df = df.sort_values(['date', 'ticker'])
            return df
        else:
            return pd.DataFrame()
    
    def fetch_stock_price(self, date=None, tickers=None):
        """
        取得收盤價（支援最新資料和歷史資料）
        
        參數:
        - date: 日期（YYYYMMDD），如果為 None 則取得最新資料
        - tickers: 股票代號列表（歷史資料查詢時需要）
        
        回傳:
        - DataFrame 包含收盤價，欄位: Code, ClosingPrice
        """
        if date is None:
            # 使用 STOCK_DAY_AVG_ALL 取得最新資料（所有股票）
            time.sleep(5)  # 禮貌休息
            return self._fetch_latest_stock_price_all()
        else:
            # 使用 STOCK_DAY_AVG 取得歷史資料（需要指定股票）
            if tickers is None:
                print("[Warning] 歷史資料查詢需要提供股票代號列表")
                return None
            
            # 取得該日期所屬的月份
            date_obj = pd.Timestamp(date)
            year_month = date_obj.strftime('%Y%m')
            
            print(f"[Info] 正在取得 {len(tickers)} 檔股票在 {year_month} 的收盤價...")
            
            all_prices = []
            
            for i, ticker in enumerate(tickers, 1):
                if i % 10 == 0:
                    print(f"[Info] 已處理 {i}/{len(tickers)} 檔股票...")
                
                df_month = self.fetch_historical_stock_price(ticker, year_month)
                
                if not df_month.empty:
                    # 篩選出指定日期的資料
                    df_date = df_month[df_month['date'] == date]
                    if not df_date.empty:
                        all_prices.append({
                            'Code': ticker,
                            'ClosingPrice': df_date.iloc[0]['closing_price']
                        })
                
                time.sleep(2)  # 禮貌休息，避免請求過快
            
            if not all_prices:
                print("[Warning] 無法取得任何股價資料")
                return None
            
            result_df = pd.DataFrame(all_prices)
            print(f"[Info] 成功取得 {len(result_df)} 筆股價資料")
            return result_df
    
    def estimate_margin_cost(self, prev_balance, prev_cost, cash_repay, sell, buy, balance_today, close_today):
        """
        根據 CMoney 公式推估今日融資成本：
        (1) 若今日資餘為零，則成本=0
        (2) 若昨日資餘為零，則成本=今日收盤價
        (3) 其他：成本=((昨日餘額-現金償還-資賣)*昨日成本+今日資買*今日收盤)/今日餘額
        """
        if balance_today == 0:
            return 0.0
        if prev_balance == 0:
            return close_today
        numerator = (prev_balance - cash_repay - sell) * prev_cost + buy * close_today
        try:
            cost = numerator / balance_today
        except ZeroDivisionError:
            cost = 0.0
        return cost
    
    def calculate_margin_ratio(self, margin_df, price_df, date, summary_info=None):
        """
        用 CMoney 融資成本推估法計算個股維持率。
        """
        if margin_df is None or price_df is None or date is None:
            print("[Error] 缺少必要資料，中止維持率計算")
            return None
        print("[Info] 正在計算融資維持率...")
        prev_snapshot = self.load_previous_snapshot(date)  # 取前日金額與成本
        prev_costs = self.load_previous_costs(date)  # 新增：取前日平均持有成本
        price_clean = price_df.copy()
        price_clean['Code'] = price_clean['Code'].astype(str).str.strip()
        if 'ClosingPrice' in price_clean.columns:
            price_clean['ClosingPrice'] = pd.to_numeric(
                price_clean['ClosingPrice'].astype(str).str.replace(',', '').str.replace('--', '0'),
                errors='coerce'
            )
        # 合併資料時使用中文欄位名稱
        merged = pd.merge(
            margin_df,
            price_clean[['Code', 'ClosingPrice']],
            left_on='代號',  # 使用中文欄位
            right_on='Code',
            how='inner'
        )
        # 紀錄今日成本以便於隔日推算
        today_costs = {}
        records = []
        for _, row in merged.iterrows():
            ticker = row['代號']  # 使用中文欄位
            stock_name = row['名稱']  # 使用中文欄位
            price = row['ClosingPrice']
            shares_today = row['融資今日餘額']  # 使用中文欄位（已加上前綴）
            prev_balance = row['融資前日餘額']  # 使用中文欄位（已加上前綴）
            buy = row['融資買進']  # 使用中文欄位（已加上前綴）
            sell = row['融資賣出']  # 使用中文欄位（已加上前綴）
            cash_repay = row['融資現金償還']  # 使用中文欄位（已加上前綴）
            
            # 取得前一日成本
            # 根據 CMoney 公式：
            # - 如果前一日沒有融資餘額（prev_balance == 0），使用今日收盤價
            # - 如果前一日有融資餘額（prev_balance > 0），必須使用前一日成本；如果找不到，跳過該股票
            if prev_balance == 0:
                # 前一日沒有融資餘額，使用今日收盤價（符合 CMoney 公式）
                prev_cost = price
            else:
                # 前一日有融資餘額，必須使用前一日成本
                prev_cost = prev_costs.get(ticker)
                if prev_cost is None:
                    # 前一日有融資餘額但找不到成本，需要回溯計算
                    print(f"[Info] {ticker} 前一日有融資餘額({prev_balance})但找不到成本，開始回溯計算...")
                    
                    # 找到該股票最早出現融資餘額的日期
                    first_date = self._find_first_margin_balance_date(ticker, date)
                    
                    if first_date is None:
                        # 無法找到起始計算日期，可能是：
                        # 1. 從頭開始計算（第一次計算這個日期範圍）
                        # 2. 新上市的個股出現
                        # 強制直接計算第一筆資料，使用當日收盤價作為前日成本
                        # 記錄警告資訊
                        warning_info = {
                            'date': date,
                            'ticker': ticker,
                            'stock_name': stock_name,
                            'prev_balance': prev_balance,
                            'first_date': None,
                            'prev_date': None,
                            'current_price': price,
                            'warning_type': '無法找到起始計算日期',
                            'message': '無法找到起始計算日期，視為第一筆資料，使用當日收盤價作為前日成本'
                        }
                        self.backward_calc_warnings.append(warning_info)
                        print(f"[Info] {ticker} 無法找到起始計算日期，視為第一筆資料，使用當日收盤價作為前日成本")
                        prev_cost = price
                    else:
                        # 回溯計算從起始日期到當前日期的前一天
                        import pandas_market_calendars as pmc
                        cal = pmc.get_calendar('XTAI')
                        
                        # 找到前一天日期（前一個交易日）
                        date_obj = pd.Timestamp(date[:4] + '-' + date[4:6] + '-' + date[6:8])
                        # 取得包含當前日期在內的交易日列表（往前推30天，確保能找到前一個交易日）
                        trading_days = cal.valid_days(start_date=date_obj - pd.Timedelta(days=30), end_date=date_obj)
                        trading_days_str = [day.strftime('%Y%m%d') for day in trading_days]
                        
                        # 找到當前日期在交易日列表中的位置，然後取前一個
                        prev_date = None
                        if date in trading_days_str:
                            date_idx = trading_days_str.index(date)
                            if date_idx > 0:
                                prev_date = trading_days_str[date_idx - 1]
                        else:
                            # 如果當前日期不在交易日列表中，找列表中倒數第二個（即前一個交易日）
                            if len(trading_days_str) >= 2:
                                prev_date = trading_days_str[-2]
                            elif len(trading_days_str) == 1:
                                # 只有一個交易日，可能是起始日期，前一日融資餘額應該為0，成本應該是當日收盤價
                                prev_date = None
                        
                        if prev_date is None:
                            # 如果找不到前一個交易日，可能是起始日期，使用當日收盤價作為成本
                            # 記錄警告資訊
                            warning_info = {
                                'date': date,
                                'ticker': ticker,
                                'stock_name': stock_name,
                                'prev_balance': prev_balance,
                                'first_date': first_date,
                                'prev_date': None,
                                'current_price': price,
                                'warning_type': '無法找到前一個交易日',
                                'message': '無法找到前一個交易日，假設為起始日期，使用當日收盤價作為前日成本'
                            }
                            self.backward_calc_warnings.append(warning_info)
                            print(f"[Info] {ticker} 無法找到前一個交易日，假設為起始日期，使用當日收盤價作為前日成本")
                            prev_cost = price
                        else:
                            # 回溯計算成本
                            prev_cost = self._calculate_cost_backward(ticker, first_date, prev_date)
                            
                            if prev_cost is None:
                                # 記錄回溯計算失敗的警告資訊
                                warning_info = {
                                    'date': date,
                                    'ticker': ticker,
                                    'stock_name': stock_name,
                                    'prev_balance': prev_balance,
                                    'first_date': first_date,
                                    'prev_date': prev_date,
                                    'current_price': price,
                                    'warning_type': '回溯計算成本失敗',
                                    'message': '回溯計算成本失敗，嘗試使用當日收盤價作為前日成本'
                                }
                                self.backward_calc_warnings.append(warning_info)
                                print(f"[Warning] {ticker} 回溯計算成本失敗，嘗試使用當日收盤價作為前日成本")
                                # 如果回溯計算失敗，可能是起始日期，使用當日收盤價作為成本
                                prev_cost = price
                            else:
                                print(f"[Info] {ticker} 回溯計算完成，前一日成本: {prev_cost:.2f}")
            
            # 使用CMoney公式計算融資成本
            # 公式：((昨日餘額 - 今日資現償 - 資賣) × 昨日融資成本 + 今日資買 × 今日收盤) / 今日資餘
            cost_today = self.estimate_margin_cost(prev_balance, prev_cost, cash_repay, sell, buy, shares_today, price)
            today_costs[ticker] = cost_today
            
            # 計算維持率：收盤價 / (融資成本(推估) × 融資成數) × 100
            # 當融資成本(推估)是0時，維持率是空值
            if shares_today == 0 or cost_today == 0:
                margin_ratio = None
                margin_balance_amount = None
            else:
                margin_ratio = price / (cost_today * self.loan_ratio) * 100
                # 計算融資餘額金額：融資餘額股數 × 融資成本(推估)
                margin_balance_amount = shares_today * cost_today
            
            records.append({
                'ticker': ticker,  # 資料庫仍用英文欄位名
                'stock_name': stock_name,
                'closing_price': price,
                'margin_balance_amount': margin_balance_amount,  # 計算融資餘額金額
                'margin_balance_shares': shares_today,
                'margin_prev_balance': prev_balance,
                'margin_buy_shares': buy,
                'margin_sell_shares': sell,
                'margin_cash_repay_shares': cash_repay,
                'margin_cost_est': cost_today,
                'margin_ratio': margin_ratio
            })
        
        result = pd.DataFrame(records)
        
        # 從資料庫讀取原始股價數據並合併（open_price, high_price, low_price, volume, turnover, change）
        raw_data = self.get_raw_data_from_database(date)
        if not raw_data.empty:
            result = result.merge(raw_data, on='ticker', how='left')
        
        # 計算移動平均欄位
        result = self._calculate_moving_averages(result, date)
        
        print(f"[Info] 成功計算 {len(result)} 檔股票的融資維持率/成本")
        result = result[result['margin_ratio'].notnull()].copy()
        return result
    
    def _calculate_moving_averages(self, df, date):
        """
        計算移動平均欄位（avg_10day_ratio, avg_10day_volume, avg_5day_balance_95）
        
        參數:
        - df: DataFrame 包含當日計算結果
        - date: 日期（YYYYMMDD）
        
        回傳:
        - DataFrame 加上移動平均欄位
        """
        import pandas_market_calendars as pmc
        
        # 計算10個交易日和5個交易日前的日期
        cal = pmc.get_calendar('XTAI')
        target_dt = pd.Timestamp(date)
        start_dt_10day = target_dt - pd.Timedelta(days=20)
        start_dt_5day = target_dt - pd.Timedelta(days=12)
        
        trading_days_10 = cal.valid_days(start_date=start_dt_10day, end_date=target_dt)
        trading_days_5 = cal.valid_days(start_date=start_dt_5day, end_date=target_dt)
        
        if len(trading_days_10) >= 10:
            target_date_10day = trading_days_10[-10].strftime('%Y%m%d')
        else:
            target_date_10day = trading_days_10[0].strftime('%Y%m%d') if len(trading_days_10) > 0 else date
        
        if len(trading_days_5) >= 5:
            target_date_5day = trading_days_5[-5].strftime('%Y%m%d')
        else:
            target_date_5day = trading_days_5[0].strftime('%Y%m%d') if len(trading_days_5) > 0 else date
        
        # 從第三張表讀取歷史資料計算移動平均
        conn = sqlite3.connect(self.db_path)
        
        # 計算每個股票的移動平均
        for idx, row in df.iterrows():
            ticker = row['ticker']
            
            # 10日平均融資維持率
            cursor = conn.cursor()
            cursor.execute("""
                SELECT AVG(margin_ratio) 
                FROM strategy_result 
                WHERE ticker = ? AND date >= ? AND date < ? AND margin_ratio IS NOT NULL
            """, (ticker, target_date_10day, date))
            avg_10day_ratio = cursor.fetchone()[0]
            df.at[idx, 'avg_10day_ratio'] = avg_10day_ratio if avg_10day_ratio else row.get('margin_ratio')
            
            # 10日平均成交量
            cursor.execute("""
                SELECT AVG(volume) 
                FROM strategy_result 
                WHERE ticker = ? AND date >= ? AND date < ? AND volume IS NOT NULL AND volume > 0
            """, (ticker, target_date_10day, date))
            avg_10day_volume = cursor.fetchone()[0]
            df.at[idx, 'avg_10day_volume'] = avg_10day_volume if avg_10day_volume else row.get('volume')
            
            # 5日平均融資餘額 × 0.95
            cursor.execute("""
                SELECT AVG(margin_balance_shares) 
                FROM strategy_result 
                WHERE ticker = ? AND date >= ? AND date < ? AND margin_balance_shares > 0
            """, (ticker, target_date_5day, date))
            avg_5day_balance = cursor.fetchone()[0]
            df.at[idx, 'avg_5day_balance_95'] = (avg_5day_balance * 0.95) if avg_5day_balance else (row.get('margin_balance_shares', 0) * 0.95)
        
        conn.close()
        
        # 確保欄位名稱正確（close_price 對應 close_price）
        if 'closing_price' in df.columns:
            df['close_price'] = df['closing_price']
        
        return df
    
    def save_twse_margin_data(self, df, date):
        """
        儲存證交所融資融券資料到第一張表（twse_margin_data）
        
        參數:
        - df: DataFrame 包含證交所融資融券資料
        - date: 日期（YYYYMMDD）
        """
        if df is None or df.empty:
            return
        
        # 定義安全轉換函式
        def safe_int(val):
            if pd.isna(val) or val is None:
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None
        
        # 準備資料（只包含證交所的原始欄位）
        records = []
        for _, row in df.iterrows():
            records.append({
                'date': date,
                'ticker': row['代號'],
                'stock_name': row['名稱'],
                'margin_balance_shares': safe_int(row.get('融資今日餘額')),
                'margin_prev_balance': safe_int(row.get('融資前日餘額')),
                'margin_buy_shares': safe_int(row.get('融資買進')),
                'margin_sell_shares': safe_int(row.get('融資賣出')),
                'margin_cash_repay_shares': safe_int(row.get('融資現金償還'))
            })
        
        save_df = pd.DataFrame(records)
        
        # 儲存到 SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            for _, row in save_df.iterrows():
                cursor.execute("""
                    INSERT OR REPLACE INTO twse_margin_data 
                    (date, ticker, stock_name, margin_balance_shares, margin_prev_balance,
                     margin_buy_shares, margin_sell_shares, margin_cash_repay_shares)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row['date'],
                    row['ticker'],
                    row['stock_name'],
                    row['margin_balance_shares'],
                    row['margin_prev_balance'],
                    row['margin_buy_shares'],
                    row['margin_sell_shares'],
                    row['margin_cash_repay_shares']
                ))
            conn.commit()
            print(f"[Info] 已儲存 {len(save_df)} 筆證交所融資融券資料到 SQLite")
        except Exception as e:
            conn.rollback()
            print(f"[Error] SQLite 儲存失敗: {e}")
        finally:
            conn.close()
        
        # 儲存到 MySQL（如果啟用）
        if self.mysql_enabled:
            try:
                import pymysql
                mysql_conn = pymysql.connect(**self.mysql_config)
                mysql_cursor = mysql_conn.cursor()
                
                insert_sql = """
                    INSERT INTO twse_margin_data 
                    (date, ticker, stock_name, margin_balance_shares, margin_prev_balance,
                     margin_buy_shares, margin_sell_shares, margin_cash_repay_shares)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        stock_name = VALUES(stock_name),
                        margin_balance_shares = VALUES(margin_balance_shares),
                        margin_prev_balance = VALUES(margin_prev_balance),
                        margin_buy_shares = VALUES(margin_buy_shares),
                        margin_sell_shares = VALUES(margin_sell_shares),
                        margin_cash_repay_shares = VALUES(margin_cash_repay_shares)
                """
                
                values = []
                for _, row in save_df.iterrows():
                    values.append((
                        row['date'],
                        row['ticker'],
                        row['stock_name'],
                        row['margin_balance_shares'],
                        row['margin_prev_balance'],
                        row['margin_buy_shares'],
                        row['margin_sell_shares'],
                        row['margin_cash_repay_shares']
                    ))
                
                mysql_cursor.executemany(insert_sql, values)
                mysql_conn.commit()
                print(f"[Info] 已儲存 {len(save_df)} 筆證交所融資融券資料到 MySQL")
                mysql_cursor.close()
                mysql_conn.close()
            except Exception as e:
                print(f"[Warning] MySQL 儲存失敗: {e}")
    
    def save_tw_stock_price_data(self, df, date):
        """
        儲存證交所股價資料到第二張表（tw_stock_price_data）
        
        參數:
        - df: DataFrame 包含證交所股價資料（包含 date, ticker, open, high, low, close, volume, turnover, change）
        - date: 日期（YYYYMMDD），如果 df 中已有 date 欄位則可忽略
        """
        if df is None or df.empty:
            return
        
        # 定義安全轉換函式
        def safe_float(val):
            if pd.isna(val) or val is None:
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None
        
        def safe_int(val):
            if pd.isna(val) or val is None:
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None
        
        # 準備資料
        records = []
        for _, row in df.iterrows():
            record_date = row.get('date', date) if 'date' in row else date
            records.append({
                'date': record_date,
                'ticker': row['ticker'],
                'open': safe_float(row.get('open')),
                'high': safe_float(row.get('high')),
                'low': safe_float(row.get('low')),
                'close': safe_float(row.get('close')),
                'volume': safe_int(row.get('volume')),
                'turnover': safe_float(row.get('turnover')),
                'change': safe_float(row.get('change'))
            })
        
        save_df = pd.DataFrame(records)
        
        # 儲存到 SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            for _, row in save_df.iterrows():
                cursor.execute("""
                    INSERT OR REPLACE INTO tw_stock_price_data 
                    (date, ticker, open, high, low, close, volume, turnover, change)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row['date'],
                    row['ticker'],
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['volume'],
                    row['turnover'],
                    row['change']
                ))
            conn.commit()
            print(f"[Info] 已儲存 {len(save_df)} 筆證交所股價資料到 SQLite")
        except Exception as e:
            conn.rollback()
            print(f"[Error] SQLite 儲存失敗: {e}")
        finally:
            conn.close()
        
        # 儲存到 MySQL（如果啟用）
        if self.mysql_enabled:
            try:
                import pymysql
                mysql_conn = pymysql.connect(**self.mysql_config)
                mysql_cursor = mysql_conn.cursor()
                
                insert_sql = """
                    INSERT INTO tw_stock_price_data 
                    (date, ticker, open, high, low, close, volume, turnover, `change`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        open = VALUES(open),
                        high = VALUES(high),
                        low = VALUES(low),
                        close = VALUES(close),
                        volume = VALUES(volume),
                        turnover = VALUES(turnover),
                        `change` = VALUES(`change`)
                """
                
                values = []
                for _, row in save_df.iterrows():
                    values.append((
                        row['date'],
                        row['ticker'],
                        row['open'],
                        row['high'],
                        row['low'],
                        row['close'],
                        row['volume'],
                        row['turnover'],
                        row['change']
                    ))
                
                mysql_cursor.executemany(insert_sql, values)
                mysql_conn.commit()
                print(f"[Info] 已儲存 {len(save_df)} 筆證交所股價資料到 MySQL")
                mysql_cursor.close()
                mysql_conn.close()
            except Exception as e:
                print(f"[Warning] MySQL 儲存失敗: {e}")
    
    def save_strategy_result(self, df, date):
        """
        儲存計算結果到第三張表（strategy_result）
        
        參數:
        - df: DataFrame 包含計算後的策略資料
        - date: 日期（YYYYMMDD）
        """
        if df is None or df.empty:
            return
        
        # 定義安全轉換函式
        def safe_float(val):
            if pd.isna(val) or val is None:
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None
        
        def safe_int(val):
            if pd.isna(val) or val is None:
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None
        
        # 準備資料
        records = []
        for _, row in df.iterrows():
            records.append({
                'date': date,
                'ticker': row.get('ticker'),
                'stock_name': row.get('stock_name'),
                'margin_ratio': safe_float(row.get('margin_ratio')),
                'margin_cost_est': safe_float(row.get('margin_cost_est')),
                'margin_balance_amount': safe_float(row.get('margin_balance_amount')),
                'margin_balance_shares': safe_int(row.get('margin_balance_shares')),
                'avg_10day_ratio': safe_float(row.get('avg_10day_ratio')),
                'volume': safe_int(row.get('volume')),
                'avg_10day_volume': safe_int(row.get('avg_10day_volume')),
                'open_price': safe_float(row.get('open_price')),
                'close_price': safe_float(row.get('close_price')),
                'avg_5day_balance_95': safe_float(row.get('avg_5day_balance_95'))
            })
        
        save_df = pd.DataFrame(records)
        
        # 儲存到 SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            for _, row in save_df.iterrows():
                cursor.execute("""
                    INSERT OR REPLACE INTO strategy_result 
                    (date, ticker, stock_name, margin_ratio, margin_cost_est, margin_balance_amount,
                     margin_balance_shares, avg_10day_ratio, volume, avg_10day_volume,
                     open_price, close_price, avg_5day_balance_95)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row['date'],
                    row['ticker'],
                    row['stock_name'],
                    row['margin_ratio'],
                    row['margin_cost_est'],
                    row['margin_balance_amount'],
                    row['margin_balance_shares'],
                    row['avg_10day_ratio'],
                    row['volume'],
                    row['avg_10day_volume'],
                    row['open_price'],
                    row['close_price'],
                    row['avg_5day_balance_95']
                ))
            conn.commit()
            print(f"[Info] 已儲存 {len(save_df)} 筆策略結果到 SQLite")
        except Exception as e:
            conn.rollback()
            print(f"[Error] SQLite 儲存失敗: {e}")
        finally:
            conn.close()
        
        # 儲存到 MySQL（如果啟用）
        if self.mysql_enabled:
            try:
                import pymysql
                mysql_conn = pymysql.connect(**self.mysql_config)
                mysql_cursor = mysql_conn.cursor()
                
                insert_sql = """
                    INSERT INTO strategy_result 
                    (date, ticker, stock_name, margin_ratio, margin_cost_est, margin_balance_amount,
                     margin_balance_shares, avg_10day_ratio, volume, avg_10day_volume,
                     open_price, close_price, avg_5day_balance_95)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        stock_name = VALUES(stock_name),
                        margin_ratio = VALUES(margin_ratio),
                        margin_cost_est = VALUES(margin_cost_est),
                        margin_balance_amount = VALUES(margin_balance_amount),
                        margin_balance_shares = VALUES(margin_balance_shares),
                        avg_10day_ratio = VALUES(avg_10day_ratio),
                        volume = VALUES(volume),
                        avg_10day_volume = VALUES(avg_10day_volume),
                        open_price = VALUES(open_price),
                        close_price = VALUES(close_price),
                        avg_5day_balance_95 = VALUES(avg_5day_balance_95)
                """
                
                values = []
                for _, row in save_df.iterrows():
                    values.append((
                        row['date'],
                        row['ticker'],
                        row['stock_name'],
                        row['margin_ratio'],
                        row['margin_cost_est'],
                        row['margin_balance_amount'],
                        row['margin_balance_shares'],
                        row['avg_10day_ratio'],
                        row['volume'],
                        row['avg_10day_volume'],
                        row['open_price'],
                        row['close_price'],
                        row['avg_5day_balance_95']
                    ))
                
                mysql_cursor.executemany(insert_sql, values)
                mysql_conn.commit()
                print(f"[Info] 已儲存 {len(save_df)} 筆策略結果到 MySQL")
                mysql_cursor.close()
                mysql_conn.close()
            except Exception as e:
                print(f"[Warning] MySQL 儲存失敗: {e}")
    
    def save_to_database(self, df, date):
        """儲存資料到資料庫（SQLite 和 MySQL）"""
        if df is None or df.empty:
            print("[Warning] 無資料可儲存")  # 提醒目前沒有可寫入的資料
            return
        
        # 添加日期欄位
        df['date'] = date
        
        # 定義安全轉換函式
        def safe_float(val):
            """安全轉換為 float，NaN 轉為 None"""
            if pd.isna(val) or val is None:
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None
        
        def safe_int(val):
            """安全轉換為 int，NaN 轉為 None"""
            if pd.isna(val) or val is None:
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None
        
        # 儲存到 SQLite
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # 檢查資料是否已存在
            cursor.execute("SELECT COUNT(*) FROM margin_data WHERE date = ?", (date,))
            exists = cursor.fetchone()[0] > 0
            
            if exists:
                # 如果資料已存在，使用 UPDATE 只更新計算結果欄位，保留原始數據
                update_sql = """
                    UPDATE margin_data SET
                        stock_name = ?,
                        closing_price = ?,
                        margin_balance_amount = ?,
                        margin_balance_shares = ?,
                        margin_prev_balance = ?,
                        margin_buy_shares = ?,
                        margin_sell_shares = ?,
                        margin_cash_repay_shares = ?,
                        margin_cost_est = ?,
                        margin_ratio = ?,
                        open_price = COALESCE(?, open_price),
                        high_price = COALESCE(?, high_price),
                        low_price = COALESCE(?, low_price),
                        volume = COALESCE(?, volume),
                        turnover = COALESCE(?, turnover),
                        change = COALESCE(?, change)
                    WHERE date = ? AND ticker = ?
                """
                
                for _, row in df.iterrows():
                    cursor.execute(update_sql, (
                        row['stock_name'] if pd.notna(row['stock_name']) else None,
                        safe_float(row.get('closing_price')),
                        safe_float(row.get('margin_balance_amount')),
                        safe_int(row.get('margin_balance_shares')),
                        safe_int(row.get('margin_prev_balance')),
                        safe_int(row.get('margin_buy_shares')),
                        safe_int(row.get('margin_sell_shares')),
                        safe_int(row.get('margin_cash_repay_shares')),
                        safe_float(row.get('margin_cost_est')),
                        safe_float(row.get('margin_ratio')),
                        safe_float(row.get('open_price')),  # 如果為 None，保留原值
                        safe_float(row.get('high_price')),
                        safe_float(row.get('low_price')),
                        safe_int(row.get('volume')),
                        safe_float(row.get('turnover')),
                        safe_float(row.get('change')),
                        date,
                        row['ticker']
                    ))
                
                conn.commit()
                print(f"[Info] 已更新 {len(df)} 筆資料到 SQLite（保留原始數據）")
            else:
                # 如果資料不存在，使用 INSERT
                df.to_sql('margin_data', conn, if_exists='append', index=False)
                conn.commit()
                print(f"[Info] 已儲存 {len(df)} 筆資料到 SQLite")
        except Exception as e:
            conn.rollback()
            print(f"[Error] SQLite 儲存失敗: {e}")
        finally:
            conn.close()
        
        # 儲存到 MySQL（如果啟用）
        if self.mysql_enabled:
            try:
                import pymysql
                mysql_conn = pymysql.connect(**self.mysql_config)
                mysql_cursor = mysql_conn.cursor()
                
                # 檢查資料是否已存在
                mysql_cursor.execute("SELECT COUNT(*) FROM margin_data WHERE date = %s", (date,))
                exists = mysql_cursor.fetchone()[0] > 0
                
                if exists:
                    # 如果資料已存在，使用 UPDATE 只更新計算結果欄位，保留原始數據
                    update_sql = """
                        UPDATE margin_data SET
                            stock_name = %s,
                            closing_price = %s,
                            margin_balance_amount = %s,
                            margin_balance_shares = %s,
                            margin_prev_balance = %s,
                            margin_buy_shares = %s,
                            margin_sell_shares = %s,
                            margin_cash_repay_shares = %s,
                            margin_cost_est = %s,
                            margin_ratio = %s,
                            open_price = COALESCE(%s, open_price),
                            high_price = COALESCE(%s, high_price),
                            low_price = COALESCE(%s, low_price),
                            volume = COALESCE(%s, volume),
                            turnover = COALESCE(%s, turnover),
                            `change` = COALESCE(%s, `change`)
                        WHERE date = %s AND ticker = %s
                    """
                    
                    for _, row in df.iterrows():
                        mysql_cursor.execute(update_sql, (
                            row['stock_name'] if pd.notna(row['stock_name']) else None,
                            safe_float(row.get('closing_price')),
                            safe_float(row.get('margin_balance_amount')),
                            safe_int(row.get('margin_balance_shares')),
                            safe_int(row.get('margin_prev_balance')),
                            safe_int(row.get('margin_buy_shares')),
                            safe_int(row.get('margin_sell_shares')),
                            safe_int(row.get('margin_cash_repay_shares')),
                            safe_float(row.get('margin_cost_est')),
                            safe_float(row.get('margin_ratio')),
                            safe_float(row.get('open_price')),  # 如果為 None，保留原值
                            safe_float(row.get('high_price')),
                            safe_float(row.get('low_price')),
                            safe_int(row.get('volume')),
                            safe_float(row.get('turnover')),
                            safe_float(row.get('change')),
                            date,
                            row['ticker']
                        ))
                    
                    mysql_conn.commit()
                    print(f"[Info] 已更新 {len(df)} 筆資料到 MySQL（保留原始數據）")
                else:
                    # 如果資料不存在，使用 INSERT
                    insert_sql = """
                        INSERT INTO margin_data 
                        (date, ticker, stock_name, closing_price, open_price, high_price, low_price,
                         volume, turnover, `change`, margin_balance_amount,
                         margin_balance_shares, margin_prev_balance, margin_buy_shares,
                         margin_sell_shares, margin_cash_repay_shares, margin_cost_est, margin_ratio)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    values = []
                    for _, row in df.iterrows():
                        values.append((
                            row['date'],
                            row['ticker'],
                            row['stock_name'] if pd.notna(row['stock_name']) else None,
                            safe_float(row['closing_price']),
                            safe_float(row.get('open_price')),
                            safe_float(row.get('high_price')),
                            safe_float(row.get('low_price')),
                            safe_int(row.get('volume')),
                            safe_float(row.get('turnover')),
                            safe_float(row.get('change')),
                            safe_float(row['margin_balance_amount']),
                            safe_int(row['margin_balance_shares']),
                            safe_int(row['margin_prev_balance']),
                            safe_int(row['margin_buy_shares']),
                            safe_int(row['margin_sell_shares']),
                            safe_int(row['margin_cash_repay_shares']),
                            safe_float(row['margin_cost_est']),
                            safe_float(row['margin_ratio'])
                        ))
                    
                    mysql_cursor.executemany(insert_sql, values)
                    mysql_conn.commit()
                    print(f"[Info] 已儲存 {len(df)} 筆資料到 MySQL")
                
                mysql_cursor.close()
                mysql_conn.close()
            except ImportError:
                print("[Warning] pymysql 未安裝，無法寫入 MySQL。請執行: pip install pymysql")
            except Exception as e:
                print(f"[Warning] MySQL 儲存失敗: {e}，但 SQLite 資料已成功儲存")
                # 不影響 SQLite 的運作
    
    def get_top_n_by_ratio_change(self, n=10, days=10):
        """
        取得融資維持率跌幅最大的前N檔
        
        參數:
        - n: 要取幾檔
        - days: 計算過去幾天的平均
        """
        conn = sqlite3.connect(self.db_path)
        
        query = f"""
        WITH avg_ratio AS (
            SELECT 
                ticker,
                stock_name,
                AVG(margin_ratio) as avg_margin_ratio
            FROM strategy_result
            WHERE date >= date('now', '-{days} days')
            GROUP BY ticker, stock_name
        ),
        latest AS (
            SELECT 
                ticker,
                stock_name,
                margin_ratio as latest_margin_ratio,
                date
            FROM strategy_result
            WHERE date = (SELECT MAX(date) FROM strategy_result)
        )
        SELECT 
            l.ticker,
            l.stock_name,
            l.latest_margin_ratio,
            a.avg_margin_ratio,
            (l.latest_margin_ratio - a.avg_margin_ratio) as ratio_change,
            ((l.latest_margin_ratio - a.avg_margin_ratio) / a.avg_margin_ratio * 100) as ratio_change_pct
        FROM latest l
        JOIN avg_ratio a ON l.ticker = a.ticker
        WHERE a.avg_margin_ratio > 0
        ORDER BY ratio_change_pct ASC
        LIMIT {n}
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
    
    def load_previous_costs(self, date):
        '''查詢 strategy_result 近一交易日的 margin_cost_est 欄位Dict，如果前一天沒有成本，則回溯更早的日期'''
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 先查詢前一天的成本
        cursor.execute("""
        SELECT ticker, margin_cost_est FROM strategy_result WHERE date = (SELECT MAX(date) FROM strategy_result WHERE date < ?)
        """, (date,))
        costs = {ticker: cost for ticker, cost in cursor.fetchall() if cost is not None}
        
        # 如果前一天沒有資料，對每個有前日餘額的股票，回溯找到最近一次的成本
        if not costs:
            # 取得當天所有有前日餘額的股票代號
            cursor.execute("""
            SELECT DISTINCT ticker FROM twse_margin_data 
            WHERE date = ? AND margin_prev_balance > 0
            """, (date,))
            tickers_with_prev_balance = [row[0] for row in cursor.fetchall()]
            
            # 對每個股票回溯找到最近一次的成本
            for ticker in tickers_with_prev_balance:
                cursor.execute("""
                SELECT margin_cost_est FROM strategy_result 
                WHERE ticker = ? AND date < ? AND margin_cost_est IS NOT NULL
                ORDER BY date DESC LIMIT 1
                """, (ticker, date))
                result = cursor.fetchone()
                if result and result[0] is not None:
                    costs[ticker] = result[0]
        
        conn.close()
        return costs
    
    def _find_first_margin_balance_date(self, ticker, before_date):
        """
        找到指定股票在指定日期之前最早出現融資餘額的日期
        
        參數:
        - ticker: 股票代號
        - before_date: 指定日期（YYYYMMDD）
        
        回傳:
        - 日期（YYYYMMDD），如果找不到則回傳 None
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 找到該股票最早出現融資餘額的日期
        cursor.execute("""
        SELECT date, margin_prev_balance, margin_balance_shares
        FROM twse_margin_data
        WHERE ticker = ? AND date < ? AND margin_balance_shares > 0
        ORDER BY date ASC
        LIMIT 1
        """, (ticker, before_date))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return result[0]  # 回傳日期
        return None
    
    def _calculate_cost_backward(self, ticker, from_date, to_date):
        """
        回溯計算某個股票從起始日期到目標日期的融資成本
        
        參數:
        - ticker: 股票代號
        - from_date: 起始日期（YYYYMMDD），這一天的前一天融資餘額應該為0
        - to_date: 目標日期（YYYYMMDD）
        
        回傳:
        - 目標日期的融資成本，如果計算失敗則回傳 None
        """
        import pandas_market_calendars as pmc
        
        # 取得從 from_date 到 to_date 的所有交易日
        cal = pmc.get_calendar('XTAI')
        from_date_obj = pd.Timestamp(from_date[:4] + '-' + from_date[4:6] + '-' + from_date[6:8])
        to_date_obj = pd.Timestamp(to_date[:4] + '-' + to_date[4:6] + '-' + to_date[6:8])
        trading_days = cal.valid_days(start_date=from_date_obj, end_date=to_date_obj)
        date_list = [day.strftime('%Y%m%d') for day in trading_days]
        
        if not date_list:
            return None
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 依序計算每一天的成本
        current_cost = None
        for calc_date in date_list:
            # 取得當天的融資融券資料
            cursor.execute("""
            SELECT margin_balance_shares, margin_prev_balance, margin_buy_shares,
                   margin_sell_shares, margin_cash_repay_shares
            FROM twse_margin_data
            WHERE ticker = ? AND date = ?
            """, (ticker, calc_date))
            
            margin_data = cursor.fetchone()
            if not margin_data:
                continue
            
            shares_today, prev_balance, buy, sell, cash_repay = margin_data
            
            # 取得當天的收盤價
            cursor.execute("""
            SELECT close FROM tw_stock_price_data
            WHERE ticker = ? AND date = ?
            """, (ticker, calc_date))
            
            price_data = cursor.fetchone()
            if not price_data or price_data[0] is None:
                continue
            
            price = price_data[0]
            
            # 計算成本（根據 CMoney 公式）
            if shares_today == 0:
                current_cost = 0.0
            elif prev_balance == 0:
                # 前一天沒有融資餘額，使用今日收盤價
                current_cost = price
            else:
                # 使用公式計算：((昨日餘額 - 今日資現償-資賣)×昨日融資成本+今日資買×今日收盤) / 今日資餘
                if current_cost is None:
                    # 如果 current_cost 還是 None，表示計算有問題
                    conn.close()
                    return None
                numerator = (prev_balance - cash_repay - sell) * current_cost + buy * price
                try:
                    current_cost = numerator / shares_today
                except ZeroDivisionError:
                    current_cost = 0.0
            
            # 如果已經計算到目標日期，回傳結果
            if calc_date == to_date:
                conn.close()
                return current_cost
        
        conn.close()
        return current_cost
    
    def get_existing_dates(self, days=15, table='twse_margin_data'):
        """
        取得資料庫中已有的日期列表（最近 N 個交易日）
        
        參數:
        - days: 要檢查的天數
        - table: 要查詢的表名（預設 'twse_margin_data'）
        
        回傳:
        - 日期列表（字串格式 YYYYMMDD）
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT DISTINCT date FROM {table} 
            ORDER BY date DESC 
            LIMIT ?
        """, (days,))
        existing_dates = [row[0] for row in cursor.fetchall()]
        conn.close()
        return existing_dates
    
    def get_missing_dates(self, target_days=15):
        """
        計算需要補抓的日期（考慮交易日）
        
        參數:
        - target_days: 目標天數（預設 15 天）
        
        回傳:
        - 需要補抓的日期列表（字串格式 YYYYMMDD）
        """
        import pandas_market_calendars as pmc
        
        # 取得資料庫中已有的日期
        existing_dates = set(self.get_existing_dates(target_days * 2))  # 多查一些以確保涵蓋
        
        # 取得最近 N 個交易日
        cal = pmc.get_calendar('XTAI')
        today = pd.Timestamp.now()
        end_date = today
        start_date = today - pd.Timedelta(days=target_days * 3)  # 往前多查一些，確保有足夠的交易日
        
        trading_days = cal.valid_days(start_date=start_date, end_date=end_date)
        trading_days_str = [day.strftime('%Y%m%d') for day in trading_days]
        
        # 找出缺少的日期（取最近 N 個交易日中缺少的）
        missing_dates = []
        for date_str in reversed(trading_days_str[-target_days:]):  # 從最近開始往前
            if date_str not in existing_dates:
                missing_dates.append(date_str)
        
        return missing_dates
    
    def batch_merge_and_save(self, margin_data_dict, price_cache):
        """
        批次合併融資融券資料和股價資料，並儲存到資料庫
        
        參數:
        - margin_data_dict: {date: (margin_df, actual_date, summary_info)}
        - price_cache: {date: DataFrame} 股價快取，DataFrame 包含 date, ticker, open, high, low, close, volume, turnover, change
        
        回傳:
        - 成功儲存的日期數量
        """
        success_count = 0
        
        for date, (margin_df, actual_date, summary_info) in margin_data_dict.items():
            try:
                # 從快取中取得該日期的股價資料（MI_INDEX API 格式）
                if actual_date not in price_cache:
                    print(f"[Warning] {date} 無法從快取取得股價資料，跳過")
                    continue
                
                df_all_stocks = price_cache[actual_date]
                
                # 轉換為合併用的格式
                price_records = []
                for _, row_price in df_all_stocks.iterrows():
                    ticker = row_price['ticker']
                    price_records.append({
                        'Code': ticker,
                        'ClosingPrice': row_price.get('close'),
                        'OpenPrice': row_price.get('open'),
                        'HighPrice': row_price.get('high'),
                        'LowPrice': row_price.get('low'),
                        'Volume': row_price.get('volume'),
                        'Turnover': row_price.get('turnover'),
                        'Change': row_price.get('change')
                    })
                
                if not price_records:
                    print(f"[Warning] {date} 無法從快取取得股價資料，跳過")
                    continue
                
                price_df = pd.DataFrame(price_records)
                
                # 合併資料
                price_clean = price_df.copy()
                price_clean['Code'] = price_clean['Code'].astype(str).str.strip()
                if 'ClosingPrice' in price_clean.columns:
                    price_clean['ClosingPrice'] = pd.to_numeric(
                        price_clean['ClosingPrice'].astype(str).str.replace(',', '').str.replace('--', '0'),
                        errors='coerce'
                    )
                
                merged = pd.merge(
                    margin_df,
                    price_clean[['Code', 'ClosingPrice', 'OpenPrice', 'HighPrice', 'LowPrice', 'Volume', 'Turnover', 'Change']],
                    left_on='代號',
                    right_on='Code',
                    how='inner'
                )
                
                if merged.empty:
                    print(f"[Warning] {date} 合併後資料為空，跳過")
                    continue
                
                # 儲存到第一張表（證交所融資融券資料）
                self.save_twse_margin_data(margin_df, actual_date)
                
                # 準備股價資料（轉換為 tw_stock_price_data 格式）
                stock_records = []
                for _, row in merged.iterrows():
                    ticker = row['代號']
                    stock_records.append({
                        'ticker': ticker,
                        'open': row.get('OpenPrice', None),
                        'high': row.get('HighPrice', None),
                        'low': row.get('LowPrice', None),
                        'close': row.get('ClosingPrice', None),
                        'volume': row.get('Volume', None),
                        'turnover': row.get('Turnover', None),
                        'change': row.get('Change', None)
                    })
                
                stock_df = pd.DataFrame(stock_records)
                
                # 儲存到第二張表（證交所股價資料）
                if not stock_df.empty:
                    self.save_tw_stock_price_data(stock_df, actual_date)
                
                success_count += 1
                
            except Exception as e:
                print(f"[Error] {date} 合併儲存時發生錯誤: {e}")
                continue
        
        return success_count
    
    def fetch_and_save_data_only(self, date=None):
        """
        只取得資料並儲存，不計算維持率（用於第一步驟：資料取得）
        
        參數:
        - date: 要取得的日期（YYYYMMDD）
        
        回傳:
        - 是否成功
        """
        requested_date = self.resolve_trade_date(date)
        
        # 1. 取得融資融券資料
        margin_df, actual_date, summary_info = self.fetch_margin_data(date)
        if margin_df is None or actual_date is None:
            return False
        
        time.sleep(5)  # 禮貌休息
        
        # 2. 取得股價資料（歷史資料需要傳入股票代號列表）
        tickers = margin_df['代號'].unique().tolist()
        price_df = self.fetch_stock_price(date=actual_date, tickers=tickers)
        if price_df is None:
            return False
        
        # 3. 合併資料（只合併，不計算維持率）
        price_clean = price_df.copy()
        price_clean['Code'] = price_clean['Code'].astype(str).str.strip()
        if 'ClosingPrice' in price_clean.columns:
            price_clean['ClosingPrice'] = pd.to_numeric(
                price_clean['ClosingPrice'].astype(str).str.replace(',', '').str.replace('--', '0'),
                errors='coerce'
            )
        
        merged = pd.merge(
            margin_df,
            price_clean[['Code', 'ClosingPrice']],
            left_on='代號',
            right_on='Code',
            how='inner'
        )
        
        if merged.empty:
            return False
        
        # 4. 準備資料（不計算維持率，只儲存原始資料）
        records = []
        for _, row in merged.iterrows():
            ticker = row['代號']
            stock_name = row['名稱']
            price = row['ClosingPrice']
            shares_today = row['融資今日餘額']
            prev_balance = row['融資前日餘額']
            buy = row['融資買進']
            sell = row['融資賣出']
            cash_repay = row['融資現金償還']
            
            records.append({
                'ticker': ticker,
                'stock_name': stock_name,
                'closing_price': price,
                'margin_balance_amount': None,
                'margin_balance_shares': shares_today,
                'margin_prev_balance': prev_balance,
                'margin_buy_shares': buy,
                'margin_sell_shares': sell,
                'margin_cash_repay_shares': cash_repay,
                'margin_cost_est': None,  # 不計算
                'margin_ratio': None      # 不計算
            })
        
        result = pd.DataFrame(records)
        
        # 5. 儲存到資料庫（不計算維持率）
        self.save_to_database(result, actual_date)
        
        return True
    
    def fetch_specific_date_data(self, date, retry_times=3, retry_delay=5):
        """
        取得指定日期的原始資料（融資融券資料和股價資料）
        
        參數:
        - date: 日期（YYYYMMDD）
        - retry_times: 每個步驟失敗時的重試次數（預設 3 次）
        - retry_delay: 重試前的等待時間（秒，預設 5 秒）
        
        回傳:
        - 是否成功
        """
        print(f"\n{'='*60}")
        print(f"取得指定日期的原始資料: {date}")
        print(f"{'='*60}\n")
        
        # 步驟 1: 取得融資融券資料
        print(f"[步驟1] 取得 {date} 的融資融券資料...")
        success_margin = False
        for attempt in range(1, retry_times + 1):
            try:
                margin_df, actual_date, summary_info = self.fetch_margin_data(date=date)
                if margin_df is not None and actual_date is not None:
                    # 儲存到第一張表（證交所融資融券資料）
                    self.save_twse_margin_data(margin_df, actual_date)
                    success_margin = True
                    print(f"[Info] {date} 融資融券資料取得並儲存成功（{len(margin_df)} 檔股票）")
                    break
                else:
                    if attempt < retry_times:
                        print(f"[Warning] {date} 融資融券資料取得失敗，{retry_delay} 秒後重試（第 {attempt}/{retry_times} 次）...")
                        time.sleep(retry_delay)
                    else:
                        print(f"[Error] {date} 融資融券資料取得失敗，已重試 {retry_times} 次")
            except Exception as e:
                if attempt < retry_times:
                    print(f"[Error] {date} 融資融券資料取得時發生錯誤: {e}")
                    print(f"[Info] {retry_delay} 秒後重試（第 {attempt}/{retry_times} 次）...")
                    time.sleep(retry_delay)
                else:
                    print(f"[Error] {date} 融資融券資料取得時發生錯誤: {e}")
                    print(f"[Error] 已重試 {retry_times} 次")
        
        if not success_margin:
            print(f"[Error] {date} 融資融券資料取得失敗，中止")
            return False
        
        time.sleep(3)  # 禮貌休息
        
        # 步驟 2: 取得股價資料（使用 MI_INDEX API，一次取得所有個股）
        print(f"\n[步驟2] 取得 {date} 的股價資料...")
        success_price = False
        for attempt in range(1, retry_times + 1):
            try:
                df_all_stocks = self.fetch_all_stocks_daily_data_from_twse(date)
                
                if not df_all_stocks.empty:
                    # 儲存到第二張表（證交所股價資料）
                    self.save_tw_stock_price_data(df_all_stocks, date)
                    success_price = True
                    print(f"[Info] {date} 股價資料取得並儲存成功（{len(df_all_stocks)} 檔股票）")
                    break
                else:
                    if attempt < retry_times:
                        print(f"[Warning] {date} 股價資料取得失敗，{retry_delay} 秒後重試（第 {attempt}/{retry_times} 次）...")
                        time.sleep(retry_delay)
                    else:
                        print(f"[Error] {date} 股價資料取得失敗，已重試 {retry_times} 次")
                        
            except Exception as e:
                if attempt < retry_times:
                    print(f"[Error] {date} 股價資料取得時發生錯誤: {e}")
                    print(f"[Info] {retry_delay} 秒後重試（第 {attempt}/{retry_times} 次）...")
                    time.sleep(retry_delay)
                else:
                    print(f"[Error] {date} 股價資料取得時發生錯誤: {e}")
                    print(f"[Error] 已重試 {retry_times} 次")
        
        if not success_price:
            print(f"[Error] {date} 股價資料取得失敗，中止")
            return False
        
        print(f"\n{'='*60}")
        print(f"{date} 原始資料取得完成")
        print(f"{'='*60}")
        print(f"融資融券資料: 成功")
        print(f"股價資料: 成功")
        print(f"\n提示: 使用 --rolling 參數可滾動計算融資成本和維持率")
        print(f"範例: python margin_ratio_calculator.py --rolling 60")
        
        return True
    
    def batch_fetch_margin_data_only(self, days=15, start_date=None, retry_times=3, retry_delay=5):
        """
        步驟 1: 僅取得證交所的融資融券資料並儲存（不需要股價資料）
        
        參數:
        - days: 要更新的天數（預設 15 天）
        - start_date: 起始日期（YYYYMMDD），如果為 None，則從今天往前推
        - retry_times: 每個日期失敗時的重試次數（預設 3 次）
        - retry_delay: 重試前的等待時間（秒，預設 5 秒）
        
        回傳:
        - 更新結果統計
        """
        print(f"\n{'='*60}")
        print(f"步驟 1: 取得證交所融資融券資料（目標：{days} 個交易日）")
        print(f"{'='*60}\n")
        
        # 檢查缺少的日期
        missing_dates = self.get_missing_dates(target_days=days)
        
        if not missing_dates:
            print(f"[Info] 資料庫中已有最近 {days} 個交易日的資料，無需更新")
            return {'success': 0, 'failed': 0, 'skipped': days}
        
        print(f"[Info] 需要補抓 {len(missing_dates)} 個交易日的融資融券資料")
        print(f"[Info] 日期列表: {', '.join(missing_dates)}\n")
        
        success_count = 0
        failed_dates = []
        
        for i, date in enumerate(missing_dates, 1):
            print(f"[{i}/{len(missing_dates)}] 正在取得 {date} 的融資融券資料...")
            
            # 重試邏輯
            success = False
            for attempt in range(1, retry_times + 1):
                try:
                    margin_df, actual_date, summary_info = self.fetch_margin_data(date=date)
                    if margin_df is not None and actual_date is not None:
                        # 儲存到第一張表（證交所融資融券資料）
                        self.save_twse_margin_data(margin_df, actual_date)
                        success = True
                        success_count += 1
                        print(f"[Info] {date} 融資融券資料取得並儲存成功")
                        break
                    else:
                        if attempt < retry_times:
                            print(f"[Warning] {date} 融資融券資料取得失敗，{retry_delay} 秒後重試（第 {attempt}/{retry_times} 次）...")
                            time.sleep(retry_delay)
                        else:
                            print(f"[Error] {date} 融資融券資料取得失敗，已重試 {retry_times} 次，跳過此日期")
                except Exception as e:
                    if attempt < retry_times:
                        print(f"[Error] {date} 融資融券資料取得時發生錯誤: {e}")
                        print(f"[Info] {retry_delay} 秒後重試（第 {attempt}/{retry_times} 次）...")
                        time.sleep(retry_delay)
                    else:
                        print(f"[Error] {date} 融資融券資料取得時發生錯誤: {e}")
                        print(f"[Error] 已重試 {retry_times} 次，跳過此日期")
            
            if not success:
                failed_dates.append(date)
            
            # 禮貌休息
            if i < len(missing_dates):
                time.sleep(3)
        
        print(f"\n{'='*60}")
        print("步驟 1 完成：證交所融資融券資料取得")
        print(f"{'='*60}")
        print(f"成功: {success_count} 天")
        print(f"失敗: {len(failed_dates)} 天")
        print(f"跳過: {days - len(missing_dates)} 天（已有資料）")
        print(f"\n提示: 使用 --fetch-prices 參數可取得證交所的股價資料")
        print(f"範例: python margin_ratio_calculator.py --fetch-prices {days}")
        
        return {
            'success': success_count,
            'failed': len(failed_dates),
            'skipped': days - len(missing_dates),
            'total': days
        }
    
    def batch_fetch_stock_prices_only(self, days=15, start_date=None, retry_times=3, retry_delay=5):
        """
        步驟 2: 僅取得證交所的股價資料並更新資料庫（使用 MI_INDEX API，一次取得所有個股）
        
        參數:
        - days: 要更新的天數（預設 15 天）
        - start_date: 起始日期（YYYYMMDD），如果為 None，則從今天往前推
        - retry_times: 每個日期失敗時的重試次數（預設 3 次）
        - retry_delay: 重試前的等待時間（秒，預設 5 秒）
        
        回傳:
        - 更新結果統計
        """
        print(f"\n{'='*60}")
        print(f"步驟 2: 取得證交所股價資料（目標：{days} 個交易日）")
        print(f"{'='*60}\n")
        
        # 檢查資料庫中已有的日期（從第一張表）- 查詢所有已有的日期，不限於 15 天
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT date FROM twse_margin_data ORDER BY date DESC")
        all_existing_dates = set([row[0] for row in cursor.fetchall()])
        conn.close()
        
        if not all_existing_dates:
            print("[Error] 資料庫中沒有融資融券資料，請先執行 --fetch-margin")
            return {'success': 0, 'failed': 0, 'skipped': 0, 'total': 0}
        
        print(f"[Info] 資料庫中已有 {len(all_existing_dates)} 個交易日的融資融券資料")
        
        # 檢查哪些日期已經有股價資料
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT date FROM tw_stock_price_data")
        existing_price_dates = set([row[0] for row in cursor.fetchall()])
        conn.close()
        
        print(f"[Info] 資料庫中已有 {len(existing_price_dates)} 個交易日的股價資料")
        
        # 取得需要更新的日期範圍（最近 N 個交易日）
        import pandas_market_calendars as pmc
        cal = pmc.get_calendar('XTAI')
        today = pd.Timestamp.now()
        start_date_range = today - pd.Timedelta(days=days * 2)
        trading_days = cal.valid_days(start_date=start_date_range, end_date=today)
        trading_days_str = [day.strftime('%Y%m%d') for day in trading_days][-days:]
        
        # 只處理資料庫中已有的日期，且限制在指定的 days 範圍內
        # 並且過濾出需要更新的日期（有融資融券資料但沒有股價資料的日期）
        dates_to_update = [
            d for d in trading_days_str 
            if d in all_existing_dates and d not in existing_price_dates
        ]
        
        if not dates_to_update:
            print(f"[Info] 沒有需要更新股價資料的日期")
            return {'success': 0, 'failed': 0, 'skipped': days, 'total': days}
        
        print(f"[Info] 需要更新 {len(dates_to_update)} 個交易日的股價資料")
        print(f"[Info] 日期列表: {', '.join(dates_to_update)}\n")
        
        # 按日期批次取得股價資料（使用 MI_INDEX API，一次取得所有個股）
        success_count = 0
        failed_dates = []
        
        for i, date in enumerate(dates_to_update, 1):
            print(f"[{i}/{len(dates_to_update)}] 正在取得 {date} 的所有個股資料...")
            
            # 重試邏輯
            success = False
            for attempt in range(1, retry_times + 1):
                try:
                    df_all_stocks = self.fetch_all_stocks_daily_data_from_twse(date)
                    
                    if not df_all_stocks.empty:
                        # 儲存到第二張表（證交所股價資料）
                        self.save_tw_stock_price_data(df_all_stocks, date)
                        success = True
                        success_count += 1
                        print(f"[Info] {date} 成功取得 {len(df_all_stocks)} 檔個股資料")
                        break
                    else:
                        if attempt < retry_times:
                            print(f"[Warning] {date} 無法取得資料，{retry_delay} 秒後重試（第 {attempt}/{retry_times} 次）...")
                            time.sleep(retry_delay)
                        else:
                            print(f"[Error] {date} 無法取得資料，已重試 {retry_times} 次，跳過此日期")
                            
                except Exception as e:
                    if attempt < retry_times:
                        print(f"[Error] {date} 取得資料時發生錯誤: {e}")
                        print(f"[Info] {retry_delay} 秒後重試（第 {attempt}/{retry_times} 次）...")
                        time.sleep(retry_delay)
                    else:
                        print(f"[Error] {date} 取得資料時發生錯誤: {e}")
                        print(f"[Error] 已重試 {retry_times} 次，跳過此日期")
            
            if not success:
                failed_dates.append(date)
            
            # 禮貌休息：每個日期請求後休息 5 秒
            if i < len(dates_to_update):
                time.sleep(5)
        
        print(f"\n{'='*60}")
        print("步驟 2 完成：證交所股價資料取得")
        print(f"{'='*60}")
        print(f"成功: {success_count} 天")
        print(f"失敗: {len(failed_dates)} 天")
        if failed_dates:
            print(f"失敗日期: {', '.join(failed_dates)}")
        print(f"\n提示: 使用 --rolling 參數可滾動計算融資成本和維持率")
        print(f"範例: python margin_ratio_calculator.py --rolling {days}")
        
        return {
            'success': success_count,
            'failed': len(failed_dates),
            'skipped': 0,
            'total': len(dates_to_update)
        }
    
    def batch_update(self, days=15, start_date=None, retry_times=3, retry_delay=5):
        """
        批次更新多天資料（優化版：先取得所有融資融券資料，再批次取得股價）
        
        參數:
        - days: 要更新的天數（預設 15 天）
        - start_date: 起始日期（YYYYMMDD），如果為 None，則從今天往前推
        - retry_times: 每個日期失敗時的重試次數（預設 3 次）
        - retry_delay: 重試前的等待時間（秒，預設 5 秒）
        
        回傳:
        - 更新結果統計
        """
        print(f"\n{'='*60}")
        print(f"開始批次更新（目標：{days} 個交易日）")
        print(f"{'='*60}\n")
        
        # 檢查缺少的日期
        missing_dates = self.get_missing_dates(target_days=days)
        
        if not missing_dates:
            print(f"[Info] 資料庫中已有最近 {days} 個交易日的資料，無需更新")
            return {'success': 0, 'failed': 0, 'skipped': days}
        
        print(f"[Info] 需要補抓 {len(missing_dates)} 個交易日的資料")
        print(f"[Info] 日期列表: {', '.join(missing_dates)}\n")
        
        # ===== 階段 1: 先取得所有日期的融資融券資料（不取得股價）=====
        print(f"[Info] 階段 1: 取得所有日期的融資融券資料...")
        margin_data_dict = {}  # {date: (margin_df, actual_date, summary_info)}
        all_tickers_set = set()
        failed_dates = []
        
        for i, date in enumerate(missing_dates, 1):
            print(f"[{i}/{len(missing_dates)}] 正在取得 {date} 的融資融券資料...")
            
            # 重試邏輯（只取得融資融券資料，不取得股價）
            success = False
            for attempt in range(1, retry_times + 1):
                try:
                    margin_df, actual_date, summary_info = self.fetch_margin_data(date=date)
                    if margin_df is not None and actual_date is not None:
                        margin_data_dict[date] = (margin_df, actual_date, summary_info)
                        # 收集所有股票代號
                        all_tickers_set.update(margin_df['代號'].unique().tolist())
                        success = True
                        print(f"[Info] {date} 融資融券資料取得成功")
                        break
                    else:
                        if attempt < retry_times:
                            print(f"[Warning] {date} 融資融券資料取得失敗，{retry_delay} 秒後重試（第 {attempt}/{retry_times} 次）...")
                            time.sleep(retry_delay)
                        else:
                            print(f"[Error] {date} 融資融券資料取得失敗，已重試 {retry_times} 次，跳過此日期")
                except Exception as e:
                    if attempt < retry_times:
                        print(f"[Error] {date} 融資融券資料取得時發生錯誤: {e}")
                        print(f"[Info] {retry_delay} 秒後重試（第 {attempt}/{retry_times} 次）...")
                        time.sleep(retry_delay)
                    else:
                        print(f"[Error] {date} 融資融券資料取得時發生錯誤: {e}")
                        print(f"[Error] 已重試 {retry_times} 次，跳過此日期")
            
            if not success:
                failed_dates.append(date)
            
            # 禮貌休息
            if i < len(missing_dates):
                time.sleep(5)
        
        if not margin_data_dict:
            print(f"\n[Error] 無法取得任何融資融券資料，停止運行")
            return {
                'success': 0,
                'failed': len(missing_dates),
                'skipped': days - len(missing_dates),
                'total': days,
                'all_failed': True
            }
        
        print(f"\n[Info] 階段 1 完成：成功取得 {len(margin_data_dict)} 個日期的融資融券資料")
        print(f"[Info] 收集到 {len(all_tickers_set)} 檔股票\n")
        
        # ===== 階段 2: 計算需要的日期範圍 =====
        if not margin_data_dict:
            print("[Error] 沒有融資融券資料，無法繼續")
            return {'success': 0, 'failed': len(missing_dates), 'skipped': days - len(missing_dates)}
        
        # 取得日期範圍
        all_dates = [actual_date for _, (_, actual_date, _) in margin_data_dict.items()]
        start_date_range = min(all_dates)
        end_date_range = max(all_dates)
        
        print(f"[Info] 階段 2: 需要取得股價資料的日期範圍: {start_date_range} 到 {end_date_range}\n")
        
        # ===== 階段 3: 使用證交所 MI_INDEX API 批次取得所有需要的股價資料（按日期取得）=====
        print(f"[Info] 階段 3: 使用證交所 MI_INDEX API 批次取得股價資料（按日期取得所有個股）...")
        
        # 按日期取得所有個股資料
        price_cache = {}  # {date: DataFrame}
        
        for i, date in enumerate(all_dates, 1):
            print(f"[{i}/{len(all_dates)}] 正在取得 {date} 的所有個股資料...")
            
            try:
                df_all_stocks = self.fetch_all_stocks_daily_data_from_twse(date)
                if not df_all_stocks.empty:
                    price_cache[date] = df_all_stocks
                    print(f"[Info] {date} 成功取得 {len(df_all_stocks)} 檔個股資料")
                else:
                    print(f"[Warning] {date} 無法取得個股資料")
            except Exception as e:
                print(f"[Error] {date} 取得個股資料時發生錯誤: {e}")
            
            # 禮貌休息：每個日期請求後休息 3 秒
            if i < len(all_dates):
                time.sleep(3)
        
        print(f"[Info] 階段 3 完成：成功取得 {len(price_cache)} 個日期的股價資料\n")
        
        # ===== 階段 4: 合併資料並儲存 =====
        print(f"[Info] 階段 4: 合併資料並儲存到資料庫...")
        success_count = self.batch_merge_and_save(margin_data_dict, price_cache)
        
        print(f"\n{'='*60}")
        print("批次更新完成")
        print(f"{'='*60}")
        print(f"成功: {success_count} 天")
        print(f"失敗: {len(failed_dates)} 天（融資融券資料取得失敗）")
        print(f"跳過: {days - len(missing_dates)} 天（已有資料）")
        print(f"\n優化效果:")
        print(f"  - 股價 API 請求次數: {len(all_dates)} 次（使用 MI_INDEX API，一次取得所有個股）")
        if len(missing_dates) > 0:
            print(f"  - 舊方式需要: {len(missing_dates)} 天 × 多檔股票 × 多個月 = 數千次")
            print(f"  - 新方式只需: {len(all_dates)} 次（大幅減少 API 請求）")
        
        return {
            'success': success_count,
            'failed': len(failed_dates),
            'skipped': days - len(missing_dates),
            'total': days,
            'all_failed': False
        }
    
    def get_historical_data_range(self, days=60):
        """
        取得指定天數的歷史交易日列表（用於滾動計算）
        
        參數:
        - days: 要取得的天數（預設 60 天，約兩個月）
        
        回傳:
        - 日期列表（從最早到最新排序，字串格式 YYYYMMDD）
        """
        import pandas_market_calendars as pmc
        
        cal = pmc.get_calendar('XTAI')
        today = pd.Timestamp.now()
        start_date = today - pd.Timedelta(days=days * 2)  # 多查一些確保有足夠交易日
        
        trading_days = cal.valid_days(start_date=start_date, end_date=today)
        trading_days_str = [day.strftime('%Y%m%d') for day in trading_days]
        
        # 只取最近 N 個交易日
        return trading_days_str[-days:]
    
    def get_price_from_database(self, date):
        """
        從資料庫讀取指定日期的股價資料
        
        參數:
        - date: 日期（YYYYMMDD）
        
        回傳:
        - DataFrame 包含 Code, ClosingPrice（如果資料庫中有資料）
        - None（如果資料庫中沒有資料）
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 查詢該日期的股價資料（從第二張表，至少要有 close）
        cursor.execute("""
            SELECT DISTINCT ticker, close 
            FROM tw_stock_price_data 
            WHERE date = ? AND close IS NOT NULL
        """, (date,))
        
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            return None
        
        # 轉換為 DataFrame（格式與 API 回傳一致）
        records = []
        for ticker, closing_price in rows:
            records.append({
                'Code': ticker,
                'ClosingPrice': closing_price
            })
        
        return pd.DataFrame(records)
    
    def get_raw_data_from_database(self, date):
        """
        從資料庫讀取指定日期的原始股價數據（從第二張表 tw_stock_price_data）
        
        參數:
        - date: 日期（YYYYMMDD）
        
        回傳:
        - DataFrame 包含 ticker, open_price, high_price, low_price, volume, turnover, change
        - 如果資料庫中沒有資料，回傳空的 DataFrame
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 查詢該日期的原始股價資料（從第二張表）
        cursor.execute("""
            SELECT DISTINCT ticker, open, high, low, 
                   volume, turnover, change
            FROM tw_stock_price_data 
            WHERE date = ?
        """, (date,))
        
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            return pd.DataFrame()
        
        # 轉換為 DataFrame（欄位名稱統一為 open_price, high_price 等）
        records = []
        for row in rows:
            records.append({
                'ticker': row[0],
                'open_price': row[1],
                'high_price': row[2],
                'low_price': row[3],
                'volume': row[4],
                'turnover': row[5],
                'change': row[6]
            })
        
        return pd.DataFrame(records)
    
    def rolling_calculate_all_dates(self, days=60, force_recalculate=False):
        """
        滾動計算指定天數內所有日期的融資成本和維持率
        
        參數:
        - days: 要計算的天數（預設 60 天，約兩個月）
        - force_recalculate: 是否強制重新計算（即使已有資料）
        
        回傳:
        - 計算結果統計
        """
        print(f"\n{'='*60}")
        print(f"開始滾動計算融資成本和維持率（目標：{days} 個交易日）")
        print(f"{'='*60}\n")
        
        # 取得要計算的日期列表（從最早到最新）
        date_list = self.get_historical_data_range(days=days)
        
        if not date_list:
            print("[Error] 無法取得交易日列表")
            return None
        
        print(f"[Info] 將計算 {len(date_list)} 個交易日的資料")
        print(f"[Info] 日期範圍: {date_list[0]} 到 {date_list[-1]}\n")
        print(f"[Info] 將直接從資料庫讀取已儲存的股價資料（步驟 1 已取得）\n")
        
        success_count = 0
        failed_count = 0
        skipped_count = 0
        
        # 步驟 2: 依日期順序計算（從最早到最新）
        for i, date in enumerate(date_list, 1):
            print(f"\n[{i}/{len(date_list)}] 正在計算 {date}...")
            
            try:
                # 檢查是否有原始資料（從第一張表和第二張表）
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM twse_margin_data WHERE date = ?", (date,))
                has_margin_data = cursor.fetchone()[0] > 0
                cursor.execute("SELECT COUNT(*) FROM tw_stock_price_data WHERE date = ?", (date,))
                has_stock_data = cursor.fetchone()[0] > 0
                conn.close()
                
                # 如果沒有原始資料，先取得資料
                if not has_margin_data or not has_stock_data:
                    print(f"[Warning] {date} 缺少原始資料（融資融券: {has_margin_data}, 股價: {has_stock_data}），請先執行 --fetch-margin 和 --fetch-prices")
                    failed_count += 1
                    continue
                
                # 從第一張表讀取融資融券資料
                conn = sqlite3.connect(self.db_path)
                margin_query = """
                    SELECT ticker, stock_name, margin_balance_shares, margin_prev_balance,
                           margin_buy_shares, margin_sell_shares, margin_cash_repay_shares
                    FROM twse_margin_data
                    WHERE date = ?
                """
                margin_df = pd.read_sql_query(margin_query, conn, params=(date,))
                conn.close()
                
                if margin_df.empty:
                    print(f"[Warning] {date} 無法從資料庫讀取融資融券資料，跳過")
                    failed_count += 1
                    continue
                
                # 轉換為 calculate_margin_ratio 需要的格式（中文欄位名）
                margin_df['代號'] = margin_df['ticker']
                margin_df['名稱'] = margin_df['stock_name']
                margin_df['融資今日餘額'] = margin_df['margin_balance_shares']
                margin_df['融資前日餘額'] = margin_df['margin_prev_balance']
                margin_df['融資買進'] = margin_df['margin_buy_shares']
                margin_df['融資賣出'] = margin_df['margin_sell_shares']
                margin_df['融資現金償還'] = margin_df['margin_cash_repay_shares']
                
                # 從第二張表讀取股價資料
                price_df = self.get_price_from_database(date)
                if price_df is None:
                    print(f"[Warning] {date} 無法從資料庫讀取股價資料，跳過")
                    failed_count += 1
                    continue
                
                print(f"[Info] {date} 從資料庫讀取資料成功（融資融券: {len(margin_df)} 檔, 股價: {len(price_df)} 檔）")
                
                # 計算融資成本和維持率（會自動使用前一日成本）
                result = self.calculate_margin_ratio(margin_df, price_df, date, summary_info=None)
                
                if result is not None and not result.empty:
                    # 儲存到第三張表（策略結果表）
                    self.save_strategy_result(result, date)
                    success_count += 1
                    print(f"[Info] {date} 計算完成，成功計算 {len(result)} 檔股票")
                else:
                    failed_count += 1
                    print(f"[Warning] {date} 計算結果為空")
                    
            except Exception as e:
                failed_count += 1
                print(f"[Error] {date} 計算時發生錯誤: {e}")
            
        
        print(f"\n{'='*60}")
        print("滾動計算完成")
        print(f"{'='*60}")
        print(f"成功: {success_count} 天")
        print(f"失敗: {failed_count} 天")
        print(f"跳過: {skipped_count} 天（已有資料）")
        
        # 匯出回溯計算失敗的警告到 CSV
        if self.backward_calc_warnings:
            warnings_df = pd.DataFrame(self.backward_calc_warnings)
            csv_filename = f'backward_calc_warnings_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            warnings_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            print(f"\n[Info] 回溯計算失敗警告已匯出至: {csv_filename}")
            print(f"[Info] 共記錄 {len(self.backward_calc_warnings)} 筆回溯計算失敗")
            
            # 顯示統計資訊
            print(f"\n回溯計算失敗統計：")
            print(f"  總筆數: {len(self.backward_calc_warnings)}")
            if 'warning_type' in warnings_df.columns:
                print(f"  警告類型分布：")
                type_counts = warnings_df['warning_type'].value_counts()
                for warning_type, count in type_counts.items():
                    print(f"    {warning_type}: {count} 筆")
            if 'ticker' in warnings_df.columns:
                print(f"  涉及股票數: {warnings_df['ticker'].nunique()}")
                print(f"  涉及日期數: {warnings_df['date'].nunique()}")
            
            # 清空列表，準備下次使用
            self.backward_calc_warnings = []
        else:
            print(f"\n[Info] 沒有回溯計算失敗的記錄")
        
        return {
            'success': success_count,
            'failed': failed_count,
            'skipped': skipped_count,
            'total': len(date_list)
        }
    
    def generate_strategy_table(self, date=None):
        """
        產生量化交易用的結果表（第三張表）
        
        參數:
        - date: 日期（YYYYMMDD），如果為 None 則使用最新日期
        
        回傳:
        - DataFrame 包含所有量化交易需要的欄位（中英並行欄位名）
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 取得目標日期（從第三張表）
        if date is None:
            cursor.execute("SELECT MAX(date) FROM strategy_result")
            date = cursor.fetchone()[0]
            if date is None:
                print("[Warning] 資料庫中沒有資料")
                conn.close()
                return pd.DataFrame()
        
        # 直接從第三張表讀取資料（移動平均已經計算好了）
        query = """
            SELECT 
                ticker,
                stock_name,
                margin_ratio,
                avg_10day_ratio,
                volume,
                avg_10day_volume,
                (close_price - open_price) as close_minus_open,
                margin_balance_shares,
                avg_5day_balance_95
            FROM strategy_result
            WHERE date = ?
            AND margin_ratio IS NOT NULL
            ORDER BY ticker
        """
        
        cursor.execute(query, (date,))
        
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            return pd.DataFrame()
        
        # 轉換為 DataFrame
        records = []
        for row in rows:
            records.append({
                'stock_name': row[1],
                'ticker': row[0],
                'margin_ratio': row[2],
                'avg_10day_ratio': row[3],
                'volume': row[4],
                'avg_10day_volume': row[5],
                'close_minus_open': row[6],
                'margin_balance_shares': row[7],
                'avg_5day_balance_95': row[8]
            })
        
        df = pd.DataFrame(records)
        
        # 設定中英並行欄位名（使用 MultiIndex）
        df.columns = pd.MultiIndex.from_tuples([
            ('stock_name', '個股名稱'),
            ('ticker', '代號'),
            ('margin_ratio', '當日融資維持率'),
            ('avg_10day_ratio', '10日融資維持率移動平均'),
            ('volume', '當日成交量'),
            ('avg_10day_volume', '10日成交量移動平均'),
            ('close_minus_open', '收盤價-開盤價'),
            ('margin_balance_shares', '當日融資餘額'),
            ('avg_5day_balance_95', '前5日平均融資餘額×0.95')
        ])
        
        return df
    
    def get_strategy_signals(self, top_n=20):
        """
        取得符合策略條件的個股（融資維持率異常低檔 + 融資餘額穩定）
        
        策略條件:
        1. 當日融資維持率 < 過去10日移動平均值（異常低檔）
        2. 當日融資餘額 > 前5日平均融資餘額 × 0.95（融資餘額變動不大）
        
        參數:
        - top_n: 回傳前N名（按維持率落差排序）
        
        回傳:
        - DataFrame 包含符合條件的個股和排名
        """
        conn = sqlite3.connect(self.db_path)
        
        # 取得最新日期（從第三張表）
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(date) FROM strategy_result")
        latest_date = cursor.fetchone()[0]
        
        if latest_date is None:
            print("[Warning] 資料庫中沒有資料")
            conn.close()
            return pd.DataFrame()
        
        # 直接從第三張表讀取資料（移動平均已經計算好了）
        query = """
            SELECT 
                ticker,
                stock_name,
                close_price as closing_price,
                margin_balance_shares as today_balance,
                margin_ratio as today_ratio,
                avg_10day_ratio,
                (avg_5day_balance_95 / 0.95) as avg_5day_balance,
                (margin_ratio - avg_10day_ratio) as ratio_diff,
                ((margin_ratio - avg_10day_ratio) / avg_10day_ratio * 100) as ratio_diff_pct
            FROM strategy_result
            WHERE date = ?
            AND margin_ratio IS NOT NULL
            AND avg_10day_ratio IS NOT NULL
            AND avg_5day_balance_95 IS NOT NULL
            AND margin_ratio < avg_10day_ratio
            AND margin_balance_shares > avg_5day_balance_95
            ORDER BY ratio_diff ASC
            LIMIT ?
        """
        
        df = pd.read_sql_query(query, conn, params=(latest_date, top_n))
        
        conn.close()
        
        # 加入排名
        if not df.empty:
            df['排名'] = range(1, len(df) + 1)
            # 重新排列欄位順序
            df = df[['排名', 'ticker', 'stock_name', 'closing_price', 
                     'today_balance', 'today_ratio', 'avg_10day_ratio', 
                     'avg_5day_balance', 'ratio_diff', 'ratio_diff_pct']]
        
        return df
    
    def get_10day_avg_margin_ratio(self, ticker=None, top_n=None):
        """
        取得個股的10天平均融資維持率（含排名）
        
        參數:
        - ticker: 股票代號，如果為 None 則回傳所有股票
        - top_n: 只回傳前N名（None 表示全部）
        
        回傳:
        - DataFrame 包含10天平均維持率和排名
        """
        conn = sqlite3.connect(self.db_path)
        
        # 取得最新日期（從第三張表）
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(date) FROM strategy_result")
        latest_date = cursor.fetchone()[0]
        
        if latest_date is None:
            print("[Warning] 資料庫中沒有資料")
            conn.close()
            return pd.DataFrame()
        
        # 計算10個交易日前的日期（使用交易日曆）
        import pandas_market_calendars as pmc
        cal = pmc.get_calendar('XTAI')
        latest_dt = pd.Timestamp(latest_date)
        start_dt = latest_dt - pd.Timedelta(days=20)  # 往前多查一些確保有足夠交易日
        
        trading_days = cal.valid_days(start_date=start_dt, end_date=latest_dt)
        if len(trading_days) >= 10:
            # 取倒數第10個交易日
            target_date = trading_days[-10].strftime('%Y%m%d')
        else:
            # 如果交易日不足10天，使用最早日期
            target_date = trading_days[0].strftime('%Y%m%d') if len(trading_days) > 0 else latest_date
        
        if ticker:
            query = """
                SELECT 
                    ticker,
                    stock_name,
                    AVG(margin_ratio) as avg_10day_ratio,
                    MIN(margin_ratio) as min_10day_ratio,
                    MAX(margin_ratio) as max_10day_ratio,
                    COUNT(*) as days_count
                FROM strategy_result
                WHERE ticker = ?
                AND date >= ?
                AND date <= ?
                AND margin_ratio IS NOT NULL
                GROUP BY ticker, stock_name
            """
            df = pd.read_sql_query(query, conn, params=(ticker, target_date, latest_date))
        else:
            query = """
                SELECT 
                    ticker,
                    stock_name,
                    AVG(margin_ratio) as avg_10day_ratio,
                    MIN(margin_ratio) as min_10day_ratio,
                    MAX(margin_ratio) as max_10day_ratio,
                    COUNT(*) as days_count
                FROM strategy_result
                WHERE date >= ?
                AND date <= ?
                AND margin_ratio IS NOT NULL
                GROUP BY ticker, stock_name
                HAVING days_count >= 5
                ORDER BY avg_10day_ratio ASC
            """
            df = pd.read_sql_query(query, conn, params=(target_date, latest_date))
        
        conn.close()
        
        # 加入排名
        if not df.empty:
            df['排名'] = df['avg_10day_ratio'].rank(method='min', ascending=True).astype(int)
            # 重新排列欄位順序
            df = df[['排名', 'ticker', 'stock_name', 'avg_10day_ratio', 
                     'min_10day_ratio', 'max_10day_ratio', 'days_count']]
            
            # 如果指定 top_n，只回傳前N名
            if top_n:
                df = df.head(top_n)
        
        return df
    
    def run_daily_update(self, date=None):
        """執行每日更新流程"""
        requested_date = self.resolve_trade_date(date)
        
        print(f"\n{'='*60}")
        print(f"開始執行 {requested_date} 的資料更新")
        print(f"{'='*60}\n")
        
        # 1. 取得融資融券資料
        margin_df, actual_date, summary_info = self.fetch_margin_data(date)
        time.sleep(3)  # 避免請求過快
        
        if margin_df is None or actual_date is None:
            print("[Error] 融資融券資料取得失敗，中止更新")
            return None
        
        if date is None and actual_date != requested_date:
            print(f"[Info] 自動改用最近交易日 {actual_date} 的資料")  # 當日尚未更新時改抓最近工作日
        
        # 2. 取得股價資料（改用 MI_INDEX API 取得完整資料）
        print(f"[Info] 正在從證交所 MI_INDEX API 取得 {actual_date} 的完整個股資料...")
        time.sleep(5)  # 禮貌休息，避免請求過快
        
        # 使用 fetch_all_stocks_daily_data_from_twse 取得完整資料（包含開盤價、最高價、最低價、收盤價、成交量等）
        price_df_full = self.fetch_all_stocks_daily_data_from_twse(actual_date)
        
        if price_df_full is None or price_df_full.empty:
            print("[Error] 股價資料取得失敗，中止更新")
            return None
        
        print(f"[Info] 成功取得 {len(price_df_full)} 檔個股的完整資料")
        
        # 3. 儲存原始資料到第一張表和第二張表
        self.save_twse_margin_data(margin_df, actual_date)
        
        # 直接儲存完整股價資料到第二張表（tw_stock_price_data）
        # price_df_full 已經是正確格式（date, ticker, open, high, low, close, volume, turnover, change）
        self.save_tw_stock_price_data(price_df_full, actual_date)
        
        # 4. 準備計算用的股價資料格式（轉換為 fetch_stock_price 的回傳格式：Code, ClosingPrice）
        # 供 calculate_margin_ratio 使用
        price_df = price_df_full[['ticker', 'close']].copy()
        price_df.columns = ['Code', 'ClosingPrice']
        
        # 5. 計算融資維持率
        result = self.calculate_margin_ratio(margin_df, price_df, actual_date, summary_info)
        
        if result is not None:
            # 6. 儲存計算結果到第三張表
            self.save_strategy_result(result, actual_date)
            
            # 7. 顯示統計資訊
            print(f"\n{'='*60}")  # 分隔線
            print("資料統計")  # 顯示統計標題
            print(f"{'='*60}")  # 分隔線
            print(f"融資維持率平均: {result['margin_ratio'].mean():.2f}%")
            print(f"融資維持率中位數: {result['margin_ratio'].median():.2f}%")
            print(f"融資維持率最低: {result['margin_ratio'].min():.2f}%")
            print(f"融資維持率最高: {result['margin_ratio'].max():.2f}%")
            
            # 8. 顯示維持率最低的前10檔
            print(f"\n{'='*60}")  # 分隔線
            print("融資維持率最低的前10檔 (風險較高)")  # 提示使用者注意低維持率標的
            print(f"{'='*60}")  # 分隔線
            lowest_10 = result.nsmallest(10, 'margin_ratio')
            print(lowest_10[['ticker', 'stock_name', 'closing_price', 'margin_ratio']].to_string(index=False))
            
            return result
        
        return None

# ===== 使用範例 =====
if __name__ == "__main__":
    import sys
    
    # 方式1: 只使用 SQLite（預設）
    # calculator = MarginRatioCalculator()
    
    # 方式2: 同時使用 SQLite 和 MySQL（雙寫模式）
    mysql_config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'my_password',
        'database': 'taiwan_stock'
    }
    calculator = MarginRatioCalculator(mysql_config=mysql_config, config_path='config.ini')
    
    try:
        # 檢查命令列參數，決定執行模式
        if len(sys.argv) > 1:
            if sys.argv[1] == '--fetch-margin':
                # 步驟 1: 僅取得證交所融資融券資料
                days = int(sys.argv[2]) if len(sys.argv) > 2 else 15
                print(f"步驟 1: 取得證交所融資融券資料（目標 {days} 個交易日）")
                result = calculator.batch_fetch_margin_data_only(days=days)
            elif sys.argv[1] == '--fetch-prices':
                # 步驟 2: 僅取得證交所股價資料
                days = int(sys.argv[2]) if len(sys.argv) > 2 else 15
                print(f"步驟 2: 取得證交所股價資料（目標 {days} 個交易日）")
                result = calculator.batch_fetch_stock_prices_only(days=days)
            elif sys.argv[1] == '--fetch-date':
                # 取得指定日期的原始資料（融資融券資料和股價資料）
                if len(sys.argv) < 3:
                    print("[Error] 請指定日期（格式: YYYYMMDD）")
                    print("範例: python margin_ratio_calculator.py --fetch-date 20231222")
                    sys.exit(1)
                date = sys.argv[2]
                if len(date) != 8 or not date.isdigit():
                    print("[Error] 日期格式錯誤，請使用 YYYYMMDD 格式（例如: 20231222）")
                    sys.exit(1)
                success = calculator.fetch_specific_date_data(date=date)
                if not success:
                    sys.exit(1)
            elif sys.argv[1] == '--batch':
                # 批次更新模式：補抓多天資料
                days = int(sys.argv[2]) if len(sys.argv) > 2 else 15
                print(f"批次更新模式：目標 {days} 個交易日")
                result = calculator.batch_update(days=days)
                
                if result and not result.get('all_failed', False):
                    print("\n批次更新完成!")
                    print("\n提示: 使用 --rolling 參數可滾動計算融資成本和維持率")
                    print("範例: python margin_ratio_calculator.py --rolling 60")
            elif sys.argv[1] == '--rolling':
                # 滾動計算模式：從歷史資料開始計算
                days = int(sys.argv[2]) if len(sys.argv) > 2 else 60
                force = '--force' in sys.argv
                print(f"滾動計算模式：目標 {days} 個交易日")
                if force:
                    print("[Info] 強制重新計算模式（將覆蓋現有資料）")
                result = calculator.rolling_calculate_all_dates(days=days, force_recalculate=force)
                
                if result:
                    print("\n滾動計算完成!")
                    print("\n提示: 使用 --query-10day 參數可查詢10天平均維持率")
                    print("範例: python margin_ratio_calculator.py --query-10day")
            elif sys.argv[1] == '--query-10day':
                # 查詢10天平均維持率或策略信號
                strategy = '--strategy' in sys.argv
                top_n = None
                ticker = None
                
                # 解析參數
                for i, arg in enumerate(sys.argv[2:], 2):
                    if arg == '--strategy':
                        strategy = True
                    elif arg == '--top' and i + 1 < len(sys.argv):
                        top_n = int(sys.argv[i + 1])
                    elif not arg.startswith('--'):
                        ticker = arg
                
                if strategy:
                    # 查詢符合策略條件的個股
                    print("查詢符合策略條件的個股...")
                    print("策略條件:")
                    print("1. 當日融資維持率 < 過去10日移動平均值（異常低檔）")
                    print("2. 當日融資餘額 > 前5日平均融資餘額 × 0.95（融資餘額穩定）")
                    print()
                    df = calculator.get_strategy_signals(top_n=top_n or 20)
                    if not df.empty:
                        print(f"\n符合策略條件的個股（前 {len(df)} 名，按維持率落差排名）:")
                        print(df.to_string(index=False))
                    else:
                        print("[Warning] 沒有找到符合條件的個股")
                else:
                    # 查詢10天平均維持率
                    if ticker:
                        print(f"查詢股票 {ticker} 的10天平均融資維持率...")
                    else:
                        print("查詢所有股票的10天平均融資維持率...")
                    
                    df = calculator.get_10day_avg_margin_ratio(ticker=ticker, top_n=top_n)
                    if not df.empty:
                        print("\n10天平均融資維持率:")
                        print(df.to_string(index=False))
                    else:
                        print("[Warning] 沒有找到資料，請先執行滾動計算")
                        print("範例: python margin_ratio_calculator.py --rolling 60")
            elif sys.argv[1] == '--strategy-table':
                # 產生量化交易用的結果表（第三張表）
                date = None
                if len(sys.argv) > 2 and not sys.argv[2].startswith('--'):
                    date = sys.argv[2]
                
                print("產生量化交易用的結果表...")
                df = calculator.generate_strategy_table(date=date)
                if not df.empty:
                    print(f"\n量化交易結果表（共 {len(df)} 檔股票）:")
                    print(df.to_string(index=False))
                else:
                    print("[Warning] 沒有找到資料，請先執行滾動計算")
                    print("範例: python margin_ratio_calculator.py --rolling 60")
            else:
                # 單日更新模式：只更新今天（或最近交易日）
                print("開始更新今日融資維持率資料...")
                result = calculator.run_daily_update()
                
                if result is not None:
                    print("\n更新完成!")
                    print("\n提示: 使用 --rolling 參數可滾動計算歷史資料")
                    print("範例: python margin_ratio_calculator.py --rolling 60")
        else:
            # 單日更新模式：只更新今天（或最近交易日）
            print("開始更新今日融資維持率資料...")
            result = calculator.run_daily_update()
            
            if result is not None:
                print("\n更新完成!")
                print("\n使用說明:")
                print("1. --batch <天數>              : 批次更新多天資料（只抓資料，不計算維持率）")
                print("2. --fetch-date <日期>          : 取得指定日期的原始資料（融資融券+股價）")
                print("3. --rolling <天數>            : 滾動計算融資成本和維持率（從歷史開始）")
                print("4. --rolling <天數> --force    : 強制重新計算（覆蓋現有資料）")
                print("5. --query-10day [代號]        : 查詢10天平均維持率")
                print("6. --query-10day --strategy    : 查詢符合策略條件的個股（含排名）")
                print("7. --query-10day --strategy --top N : 查詢前N名符合策略條件的個股")
                print("8. --strategy-table [日期]     : 產生量化交易用的結果表（第三張表）")
                print("\n建議流程:")
                print("步驟1: python margin_ratio_calculator.py --batch 60")
                print("        （只取得資料，不計算維持率）")
                print("步驟2: python margin_ratio_calculator.py --rolling 60")
                print("        （滾動計算所有日期的融資成本和維持率）")
                print("步驟3: python margin_ratio_calculator.py --strategy-table")
                print("        （產生量化交易用的結果表，包含所有需要的欄位）")
    
    # 確保登出玉山證券 API（如果之前有登入的話）
    finally:
        calculator.esun_logout()