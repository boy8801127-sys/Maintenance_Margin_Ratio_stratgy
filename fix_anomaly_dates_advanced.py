"""
進階異常日期修復工具
對於證交所 API 當天資料錯誤的情況，使用前後幾天的股價來修正
"""

import sqlite3
import pandas as pd
import numpy as np
import time
import os
import sys
import pandas_market_calendars as pmc

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

# 匯入計算器
from margin_ratio_calculator import MarginRatioCalculator


def get_adjacent_prices(ticker, target_date, db_path, days_before=5, days_after=5):
    """
    取得前後幾天的股價來推估正確股價
    
    參數:
    - ticker: 股票代號
    - target_date: 目標日期（YYYYMMDD）
    - db_path: 資料庫路徑
    - days_before: 往前查幾天
    - days_after: 往後查幾天
    
    回傳:
    - (close_price, open_price) 或 (None, None)
    """
    cal = pmc.get_calendar('XTAI')
    target_date_obj = pd.Timestamp(target_date[:4] + '-' + target_date[4:6] + '-' + target_date[6:8])
    
    # 取得前後幾天的交易日
    start_date = target_date_obj - pd.Timedelta(days=days_before * 2)
    end_date = target_date_obj + pd.Timedelta(days=days_after * 2)
    trading_days = cal.valid_days(start_date=start_date, end_date=end_date)
    trading_days_str = [day.strftime('%Y%m%d') for day in trading_days]
    
    if target_date not in trading_days_str:
        return None, None
    
    target_idx = trading_days_str.index(target_date)
    
    # 往前找
    prices_before = []
    for i in range(max(0, target_idx - days_before), target_idx):
        date = trading_days_str[i]
        try:
            conn = sqlite3.connect(db_path, timeout=10)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT close, open 
                FROM tw_stock_price_data 
                WHERE ticker = ? AND date = ?
            """, (ticker, date))
            result = cursor.fetchone()
            conn.close()
            if result and result[0] is not None:
                prices_before.append((result[0], result[1]))
        except Exception as e:
            print(f"    [Warning] 查詢 {date} 的 {ticker} 股價時發生錯誤: {e}")
            continue
    
    # 往後找
    prices_after = []
    for i in range(target_idx + 1, min(len(trading_days_str), target_idx + days_after + 1)):
        date = trading_days_str[i]
        try:
            conn = sqlite3.connect(db_path, timeout=10)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT close, open 
                FROM tw_stock_price_data 
                WHERE ticker = ? AND date = ?
            """, (ticker, date))
            result = cursor.fetchone()
            conn.close()
            if result and result[0] is not None:
                prices_after.append((result[0], result[1]))
        except Exception as e:
            print(f"    [Warning] 查詢 {date} 的 {ticker} 股價時發生錯誤: {e}")
            continue
    
    # 合併前後價格
    all_prices = prices_before + prices_after
    
    if not all_prices:
        return None, None
    
    # 計算中位數（排除異常值）
    close_prices = [p[0] for p in all_prices if p[0] is not None and p[0] > 0]
    open_prices = [p[1] for p in all_prices if p[1] is not None and p[1] > 0]
    
    if not close_prices:
        return None, None
    
    # 如果價格差異太大，可能是異常值，使用中位數
    close_median = np.median(close_prices)
    open_median = np.median(open_prices) if open_prices else close_median
    
    return close_median, open_median


def fix_anomaly_date_advanced(date, calculator, retry_times=3, retry_delay=5):
    """
    進階修復單一異常日期的資料（包含股價修正）
    """
    print(f"\n{'='*80}")
    print(f"進階修復日期: {date}")
    print(f"{'='*80}")
    
    # 步驟1: 重新取得股價資料
    print(f"\n[步驟1] 重新取得 {date} 的股價資料...")
    success = False
    
    for attempt in range(1, retry_times + 1):
        try:
            df_all_stocks = calculator.fetch_all_stocks_daily_data_from_twse(date)
            
            if not df_all_stocks.empty:
                # 檢查並修正異常股價
                print(f"  [Info] 檢查異常股價...")
                fixed_count = 0
                
                # 檢查3661等已知異常股票
                known_anomalies = ['3661']  # 可以擴展更多
                
                for ticker in known_anomalies:
                    df_ticker = df_all_stocks[df_all_stocks['ticker'] == ticker]
                    if not df_ticker.empty:
                        close_price = df_ticker.iloc[0]['close']
                        if close_price is not None and close_price < 100:
                            print(f"  [Warning] {ticker} 股價異常: {close_price}，嘗試從前後幾天推估...")
                            fixed_close, fixed_open = get_adjacent_prices(ticker, date, calculator.db_path)
                            
                            if fixed_close is not None and fixed_close > 100:
                                # 更新股價
                                idx = df_all_stocks[df_all_stocks['ticker'] == ticker].index[0]
                                df_all_stocks.at[idx, 'close'] = fixed_close
                                if fixed_open is not None:
                                    df_all_stocks.at[idx, 'open'] = fixed_open
                                print(f"  [Info] {ticker} 股價已修正為: {fixed_close:.2f}")
                                fixed_count += 1
                            else:
                                print(f"  [Warning] {ticker} 無法從前後幾天推估正確股價")
                
                # 儲存到第二張表（證交所股價資料）
                calculator.save_tw_stock_price_data(df_all_stocks, date)
                print(f"  [Info] 成功取得 {len(df_all_stocks)} 檔個股資料（修正 {fixed_count} 檔異常股價）")
                success = True
                break
            else:
                if attempt < retry_times:
                    print(f"  [Warning] 無法取得資料，{retry_delay} 秒後重試（第 {attempt}/{retry_times} 次）...")
                    time.sleep(retry_delay)
                else:
                    print(f"  [Error] 無法取得資料，已重試 {retry_times} 次")
                    
        except Exception as e:
            if attempt < retry_times:
                print(f"  [Error] 取得資料時發生錯誤: {e}")
                print(f"  [Info] {retry_delay} 秒後重試（第 {attempt}/{retry_times} 次）...")
                time.sleep(retry_delay)
            else:
                print(f"  [Error] 取得資料時發生錯誤: {e}")
                print(f"  [Error] 已重試 {retry_times} 次")
    
    if not success:
        print(f"  [Error] {date} 股價資料取得失敗，跳過此日期")
        return False
    
    time.sleep(3)  # 禮貌休息
    
    # 步驟2-5: 與基本修復相同
    print(f"\n[步驟2] 從資料庫讀取 {date} 的融資融券資料...")
    conn = sqlite3.connect(calculator.db_path)
    
    margin_query = """
        SELECT ticker, stock_name, margin_balance_shares, margin_prev_balance,
               margin_buy_shares, margin_sell_shares, margin_cash_repay_shares
        FROM twse_margin_data
        WHERE date = ?
    """
    margin_df = pd.read_sql_query(margin_query, conn, params=(date,))
    conn.close()
    
    if margin_df.empty:
        print(f"  [Error] {date} 無法從資料庫讀取融資融券資料，跳過")
        return False
    
    print(f"  [Info] 讀取到 {len(margin_df)} 檔股票的融資融券資料")
    
    # 轉換格式
    margin_df['代號'] = margin_df['ticker']
    margin_df['名稱'] = margin_df['stock_name']
    margin_df['融資今日餘額'] = margin_df['margin_balance_shares']
    margin_df['融資前日餘額'] = margin_df['margin_prev_balance']
    margin_df['融資買進'] = margin_df['margin_buy_shares']
    margin_df['融資賣出'] = margin_df['margin_sell_shares']
    margin_df['融資現金償還'] = margin_df['margin_cash_repay_shares']
    
    # 步驟3: 從資料庫讀取股價資料
    print(f"\n[步驟3] 從資料庫讀取 {date} 的股價資料...")
    price_df = calculator.get_price_from_database(date)
    
    if price_df is None:
        print(f"  [Error] {date} 無法從資料庫讀取股價資料，跳過")
        return False
    
    print(f"  [Info] 讀取到 {len(price_df)} 檔股票的股價資料")
    
    # 檢查關鍵股票的股價
    price_3661 = price_df[price_df['Code'] == '3661']
    if not price_3661.empty:
        close_3661 = price_3661.iloc[0]['ClosingPrice']
        if close_3661 is not None:
            if close_3661 < 100:
                print(f"  [Warning] 3661 股價仍然異常: {close_3661}，嘗試直接修正...")
                fixed_close, fixed_open = get_adjacent_prices('3661', date, calculator.db_path)
                if fixed_close is not None and fixed_close > 100:
                    # 直接更新資料庫（加入重試機制）
                    max_retries = 5
                    for retry in range(max_retries):
                        try:
                            conn = sqlite3.connect(calculator.db_path, timeout=10)
                            cursor = conn.cursor()
                            cursor.execute("""
                                UPDATE tw_stock_price_data 
                                SET close = ?, open = ?
                                WHERE ticker = '3661' AND date = ?
                            """, (fixed_close, fixed_open if fixed_open else fixed_close, date))
                            conn.commit()
                            conn.close()
                            print(f"  [Info] 3661 股價已直接修正為: {fixed_close:.2f}")
                            # 重新讀取
                            price_df = calculator.get_price_from_database(date)
                            break
                        except sqlite3.OperationalError as e:
                            if "locked" in str(e).lower() and retry < max_retries - 1:
                                print(f"    [Warning] 資料庫鎖定，{2} 秒後重試（第 {retry + 1}/{max_retries} 次）...")
                                time.sleep(2)
                            else:
                                print(f"    [Error] 無法更新資料庫: {e}")
                                break
                        except Exception as e:
                            print(f"    [Error] 更新資料庫時發生錯誤: {e}")
                            break
            else:
                print(f"  [Info] 3661 股價正常: {close_3661}")
    
    # 步驟4: 重新計算維持率
    print(f"\n[步驟4] 重新計算 {date} 的融資維持率...")
    result = calculator.calculate_margin_ratio(margin_df, price_df, date, summary_info=None)
    
    if result is not None and not result.empty:
        # 步驟5: 更新資料庫
        print(f"\n[步驟5] 更新資料庫...")
        calculator.save_strategy_result(result, date)
        print(f"  [Info] 成功計算並更新 {len(result)} 檔股票的維持率")
        
        # 顯示統計
        print(f"\n[統計] {date} 修復後的統計：")
        print(f"  平均維持率: {result['margin_ratio'].mean():.2f}%")
        print(f"  中位數維持率: {result['margin_ratio'].median():.2f}%")
        print(f"  最低維持率: {result['margin_ratio'].min():.2f}%")
        print(f"  最高維持率: {result['margin_ratio'].max():.2f}%")
        print(f"  股票數量: {len(result)} 檔")
        
        # 計算平均數和中位數差異
        avg_ratio = result['margin_ratio'].mean()
        median_ratio = result['margin_ratio'].median()
        diff_pct = abs((avg_ratio - median_ratio) / median_ratio * 100) if median_ratio > 0 else 0
        print(f"  平均數與中位數差異: {diff_pct:.2f}%")
        
        return True
    else:
        print(f"  [Error] {date} 計算結果為空")
        return False


def main():
    """主程式"""
    print("=" * 80)
    print("進階異常日期資料修復工具")
    print("=" * 80)
    
    # 異常日期列表（已知股價異常的日期）
    anomaly_dates = [
        '20231222',  # 3661 股價異常
        '20200615',  # 3661 股價異常
        '20201117',  # 3661 股價異常
        '20200327',  # 3661 股價異常
    ]
    
    print(f"\n將修復以下 {len(anomaly_dates)} 個異常日期（使用前後幾天股價推估）：")
    for date in anomaly_dates:
        print(f"  - {date}")
    
    # 初始化計算器
    print("\n初始化計算器...")
    calculator = MarginRatioCalculator()
    
    # 修復每個異常日期
    success_count = 0
    failed_dates = []
    
    for i, date in enumerate(anomaly_dates, 1):
        print(f"\n[{i}/{len(anomaly_dates)}] 處理日期: {date}")
        
        try:
            success = fix_anomaly_date_advanced(date, calculator)
            if success:
                success_count += 1
            else:
                failed_dates.append(date)
        except Exception as e:
            print(f"  [Error] {date} 處理時發生錯誤: {e}")
            failed_dates.append(date)
        
        # 禮貌休息
        if i < len(anomaly_dates):
            time.sleep(5)
    
    # 總結
    print("\n" + "=" * 80)
    print("進階修復完成")
    print("=" * 80)
    print(f"成功: {success_count} 個日期")
    print(f"失敗: {len(failed_dates)} 個日期")
    if failed_dates:
        print(f"失敗日期: {', '.join(failed_dates)}")
    
    print("\n建議：")
    print("1. 重新執行 find_anomaly_dates.py 確認異常日期是否已修復")
    print("2. 重新執行 interactive_chart_generator.py 產生更新後的圖表")


if __name__ == '__main__':
    main()

