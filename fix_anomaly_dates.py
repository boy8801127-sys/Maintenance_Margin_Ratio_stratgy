"""
修復異常日期資料
重新取得並重新計算異常日期的股價資料和維持率
"""

import sqlite3
import pandas as pd
import time
import os
import sys

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


def fix_anomaly_date(date, calculator, retry_times=3, retry_delay=5):
    """
    修復單一異常日期的資料
    
    參數:
    - date: 日期（YYYYMMDD）
    - calculator: MarginRatioCalculator 實例
    - retry_times: 重試次數
    - retry_delay: 重試延遲（秒）
    """
    print(f"\n{'='*80}")
    print(f"修復日期: {date}")
    print(f"{'='*80}")
    
    # 步驟1: 重新取得股價資料
    print(f"\n[步驟1] 重新取得 {date} 的股價資料...")
    success = False
    
    for attempt in range(1, retry_times + 1):
        try:
            df_all_stocks = calculator.fetch_all_stocks_daily_data_from_twse(date)
            
            if not df_all_stocks.empty:
                # 檢查是否有異常股價（例如3661應該是3000+，不應該是90）
                # 檢查3661的股價
                df_3661 = df_all_stocks[df_all_stocks['ticker'] == '3661']
                if not df_3661.empty:
                    close_price = df_3661.iloc[0]['close']
                    if close_price is not None and close_price < 100:
                        print(f"  [Warning] 3661 股價異常: {close_price}（應該是3000+）")
                        if attempt < retry_times:
                            print(f"  [Info] {retry_delay} 秒後重試（第 {attempt}/{retry_times} 次）...")
                            time.sleep(retry_delay)
                            continue
                
                # 儲存到第二張表（證交所股價資料）
                calculator.save_tw_stock_price_data(df_all_stocks, date)
                print(f"  [Info] 成功取得 {len(df_all_stocks)} 檔個股資料")
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
    
    # 步驟2: 從資料庫讀取融資融券資料
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
    
    # 轉換為 calculate_margin_ratio 需要的格式（中文欄位名）
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
    
    # 檢查關鍵股票的股價是否正常
    price_3661 = price_df[price_df['Code'] == '3661']
    if not price_3661.empty:
        close_3661 = price_3661.iloc[0]['ClosingPrice']
        if close_3661 is not None:
            if close_3661 < 100:
                print(f"  [Warning] 3661 股價仍然異常: {close_3661}（應該是3000+）")
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
        
        return True
    else:
        print(f"  [Error] {date} 計算結果為空")
        return False


def main():
    """主程式"""
    print("=" * 80)
    print("異常日期資料修復工具")
    print("=" * 80)
    
    # 異常日期列表（從檢測結果）
    anomaly_dates = [
        '20231222',  # 差異15.4%
        '20200617',  # 差異7.6%
        '20200615',  # 差異7.1%
        '20201117',  # 差異6.7%
        '20210531',  # 差異6.4%
        '20200327',  # 差異6.1%
    ]
    
    print(f"\n將修復以下 {len(anomaly_dates)} 個異常日期：")
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
            success = fix_anomaly_date(date, calculator)
            if success:
                success_count += 1
            else:
                failed_dates.append(date)
        except Exception as e:
            print(f"  [Error] {date} 處理時發生錯誤: {e}")
            failed_dates.append(date)
        
        # 禮貌休息（最後一個日期不需要休息）
        if i < len(anomaly_dates):
            time.sleep(5)
    
    # 總結
    print("\n" + "=" * 80)
    print("修復完成")
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

