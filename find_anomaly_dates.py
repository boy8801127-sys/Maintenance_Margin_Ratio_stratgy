"""
異常日期檢測工具
透過平均數和中位數的差異來找出可能有問題的日期
如果平均數和中位數相差超過5%，可能是數據異常
"""

import sqlite3
import pandas as pd
import numpy as np
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


def find_anomaly_dates(db_path='taiwan_stock.db', threshold=5.0, diff_threshold=5.0, start_date='20190101', end_date='20251117'):
    """
    找出平均數和中位數相差超過閾值的異常日期
    
    參數:
    - db_path: 資料庫路徑
    - threshold: 差異百分比閾值（預設5%）
    - diff_threshold: 差異絕對值閾值（預設5.0%）
    - start_date: 開始日期
    - end_date: 結束日期
    """
    conn = sqlite3.connect(db_path)
    
    print("=" * 80)
    print(f"異常日期檢測（平均數與中位數差異 > {threshold}% 或差異絕對值 > {diff_threshold}%）")
    print("=" * 80)
    
    # 取得所有日期的平均數和極端維持率統計
    query_avg = """
    SELECT 
        date,
        COUNT(*) as stock_count,
        AVG(margin_ratio) as avg_ratio,
        COUNT(CASE WHEN margin_ratio < 50 THEN 1 END) as stocks_below_50,
        COUNT(CASE WHEN margin_ratio > 300 THEN 1 END) as stocks_above_300
    FROM strategy_result
    WHERE date >= ? AND date <= ?
        AND margin_ratio IS NOT NULL
        AND margin_ratio > 0
        AND margin_balance_shares > 0
    GROUP BY date
    ORDER BY date
    """
    
    df_avg = pd.read_sql_query(query_avg, conn, params=(start_date, end_date))
    
    if df_avg.empty:
        print("[Error] 沒有找到資料")
        conn.close()
        return pd.DataFrame(), pd.DataFrame()
    
    print(f"\n[Info] 正在計算所有日期的中位數...")
    print(f"[Info] 總共 {len(df_avg)} 個交易日")
    
    # 計算每個日期的中位數
    median_data = []
    total_dates = len(df_avg)
    
    for idx, date in enumerate(df_avg['date']):
        if (idx + 1) % 100 == 0:
            print(f"  進度: {idx + 1}/{total_dates} ({((idx+1)/total_dates*100):.1f}%)")
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT margin_ratio 
            FROM strategy_result 
            WHERE date = ? 
              AND margin_ratio IS NOT NULL 
              AND margin_ratio > 0
              AND margin_balance_shares > 0
            ORDER BY margin_ratio
        """, (date,))
        ratios = [row[0] for row in cursor.fetchall()]
        if ratios:
            median = np.median(ratios)
            median_data.append(median)
        else:
            median_data.append(None)
    
    df_avg['median_ratio'] = median_data
    
    # 計算差異百分比和絕對值差異
    df_avg['diff'] = df_avg['avg_ratio'] - df_avg['median_ratio']
    # 只計算 median_ratio 不為 None 且 > 0 的情況
    mask = (df_avg['median_ratio'].notna()) & (df_avg['median_ratio'] > 0)
    df_avg['diff_pct'] = None
    df_avg.loc[mask, 'diff_pct'] = (df_avg.loc[mask, 'diff'] / df_avg.loc[mask, 'median_ratio'] * 100).abs()
    df_avg['diff_abs'] = df_avg['diff'].abs()
    
    # 計算極端維持率個股的數量和比例
    df_avg['extreme_count'] = df_avg['stocks_below_50'] + df_avg['stocks_above_300']
    df_avg['extreme_ratio'] = (df_avg['extreme_count'] / df_avg['stock_count'] * 100)
    
    # 找出異常日期（差異百分比 > threshold 或 差異絕對值 > diff_threshold）
    anomaly_mask = (
        (df_avg['diff_pct'].notna()) & (df_avg['diff_pct'] > threshold)
    ) | (
        (df_avg['diff_abs'].notna()) & (df_avg['diff_abs'] > diff_threshold)
    )
    anomaly_dates = df_avg[anomaly_mask].copy()
    anomaly_dates = anomaly_dates.sort_values('diff_pct', ascending=False, na_position='last')
    
    conn.close()
    
    return df_avg, anomaly_dates


def check_specific_dates(dates, db_path='taiwan_stock.db'):
    """
    檢查特定日期的平均數和中位數差異，以及極端維持率個股數量
    
    參數:
    - dates: 日期列表（例如: ['20200922', '20211130']）
    - db_path: 資料庫路徑
    """
    conn = sqlite3.connect(db_path)
    
    print("=" * 80)
    print("檢查特定日期")
    print("=" * 80)
    
    results = []
    
    for date in dates:
        print(f"\n檢查日期: {date}")
        
        # 取得平均數和極端維持率統計
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                COUNT(*) as stock_count,
                AVG(margin_ratio) as avg_ratio,
                COUNT(CASE WHEN margin_ratio < 50 THEN 1 END) as stocks_below_50,
                COUNT(CASE WHEN margin_ratio > 300 THEN 1 END) as stocks_above_300
            FROM strategy_result
            WHERE date = ?
              AND margin_ratio IS NOT NULL
              AND margin_ratio > 0
              AND margin_balance_shares > 0
        """, (date,))
        row = cursor.fetchone()
        
        if row and row[0] > 0:
            stock_count, avg_ratio, stocks_below_50, stocks_above_300 = row
            
            # 計算中位數
            cursor.execute("""
                SELECT margin_ratio 
                FROM strategy_result 
                WHERE date = ? 
                  AND margin_ratio IS NOT NULL 
                  AND margin_ratio > 0
                  AND margin_balance_shares > 0
                ORDER BY margin_ratio
            """, (date,))
            ratios = [r[0] for r in cursor.fetchall()]
            
            if ratios:
                median_ratio = np.median(ratios)
                diff = avg_ratio - median_ratio
                diff_abs = abs(diff)
                diff_pct = abs((diff / median_ratio * 100)) if median_ratio > 0 else None
                extreme_count = stocks_below_50 + stocks_above_300
                extreme_ratio = (extreme_count / stock_count * 100) if stock_count > 0 else 0
                
                result = {
                    'date': date,
                    'stock_count': stock_count,
                    'avg_ratio': avg_ratio,
                    'median_ratio': median_ratio,
                    'diff': diff,
                    'diff_abs': diff_abs,
                    'diff_pct': diff_pct,
                    'stocks_below_50': stocks_below_50,
                    'stocks_above_300': stocks_above_300,
                    'extreme_count': extreme_count,
                    'extreme_ratio': extreme_ratio
                }
                results.append(result)
                
                print(f"  股票數量: {stock_count}")
                print(f"  平均維持率: {avg_ratio:.2f}%")
                print(f"  中位數維持率: {median_ratio:.2f}%")
                print(f"  差異: {diff:.2f}%")
                print(f"  差異絕對值: {diff_abs:.2f}%")
                if diff_pct is not None:
                    print(f"  差異百分比: {diff_pct:.2f}%")
                else:
                    print(f"  差異百分比: 無法計算（中位數為 0）")
                
                print(f"\n  極端維持率個股統計：")
                print(f"    < 50% 維持率: {stocks_below_50} 檔 ({stocks_below_50/stock_count*100:.2f}%)")
                print(f"    > 300% 維持率: {stocks_above_300} 檔 ({stocks_above_300/stock_count*100:.2f}%)")
                print(f"    極端個股總數: {extreme_count} 檔 ({extreme_ratio:.2f}%)")
                
                # 判斷是否異常
                warnings = []
                if diff_pct is not None and diff_pct > 5.0:
                    warnings.append(f"差異百分比超過 5% ({diff_pct:.2f}%)")
                if diff_abs > 5.0:
                    warnings.append(f"差異絕對值超過 5% ({diff_abs:.2f}%)")
                if extreme_ratio > 5.0:
                    warnings.append(f"極端維持率個股比例超過 5% ({extreme_ratio:.2f}%)")
                
                if warnings:
                    print(f"\n  [Warning] 可能是異常日期！")
                    for warning in warnings:
                        print(f"    - {warning}")
            else:
                print(f"  [Warning] 沒有找到有效的維持率資料")
        else:
            print(f"  [Warning] 沒有找到該日期的資料")
    
    conn.close()
    
    if results:
        df_results = pd.DataFrame(results)
        return df_results
    else:
        return pd.DataFrame()


def analyze_anomaly_date(date, db_path='taiwan_stock.db'):
    """
    分析單一異常日期的詳細資訊
    """
    conn = sqlite3.connect(db_path)
    
    print(f"\n{'='*80}")
    print(f"詳細分析日期: {date}")
    print(f"{'='*80}")
    
    # 1. 基本統計
    query1 = """
    SELECT 
        COUNT(*) as stock_count,
        AVG(margin_ratio) as avg_ratio,
        MIN(margin_ratio) as min_ratio,
        MAX(margin_ratio) as max_ratio,
        COUNT(CASE WHEN margin_ratio < 50 THEN 1 END) as stocks_below_50,
        COUNT(CASE WHEN margin_ratio < 100 THEN 1 END) as stocks_below_100,
        COUNT(CASE WHEN margin_ratio > 300 THEN 1 END) as stocks_above_300,
        COUNT(CASE WHEN margin_ratio > 500 THEN 1 END) as stocks_above_500
    FROM strategy_result
    WHERE date = ?
      AND margin_ratio IS NOT NULL
      AND margin_balance_shares > 0
    """
    
    df1 = pd.read_sql_query(query1, conn, params=(date,))
    if not df1.empty:
        print("\n[1] 基本統計：")
        print(df1.to_string(index=False))
    
    # 2. 異常維持率的股票（< 50% 或 > 300%）
    query2 = """
    SELECT 
        ticker,
        stock_name,
        margin_ratio,
        close_price,
        open_price,
        margin_cost_est,
        margin_balance_shares
    FROM strategy_result
    WHERE date = ?
      AND margin_ratio IS NOT NULL
      AND margin_balance_shares > 0
      AND (margin_ratio < 50 OR margin_ratio > 300)
    ORDER BY margin_ratio ASC
    LIMIT 30
    """
    
    df2 = pd.read_sql_query(query2, conn, params=(date,))
    if not df2.empty:
        print(f"\n[2] 異常維持率的股票（< 50% 或 > 300%），共 {len(df2)} 檔：")
        print(df2.to_string(index=False))
    
    # 3. 檢查股價異常
    query3 = """
    SELECT 
        s.ticker,
        s.stock_name,
        s.close_price,
        s.open_price,
        p.close as price_close,
        p.open as price_open
    FROM strategy_result s
    LEFT JOIN tw_stock_price_data p ON s.date = p.date AND s.ticker = p.ticker
    WHERE s.date = ?
      AND s.margin_ratio IS NOT NULL
      AND s.margin_balance_shares > 0
      AND (s.close_price < 10 OR s.close_price > 10000 OR p.close < 10 OR p.close > 10000)
    LIMIT 20
    """
    
    df3 = pd.read_sql_query(query3, conn, params=(date,))
    if not df3.empty:
        print("\n[3] 股價異常的股票（< 10 或 > 10000）：")
        print(df3.to_string(index=False))
    
    # 4. 與前後幾天的比較
    try:
        import pandas_market_calendars as pmc
        cal = pmc.get_calendar('XTAI')
        date_obj = pd.Timestamp(date[:4] + '-' + date[4:6] + '-' + date[6:8])
        
        trading_days = cal.valid_days(start_date=date_obj - pd.Timedelta(days=10), end_date=date_obj + pd.Timedelta(days=10))
        trading_days_str = [day.strftime('%Y%m%d') for day in trading_days]
        
        if date in trading_days_str:
            date_idx = trading_days_str.index(date)
            start_idx = max(0, date_idx - 2)
            end_idx = min(len(trading_days_str), date_idx + 3)
            dates_to_compare = trading_days_str[start_idx:end_idx]
            
            print("\n[4] 與前後幾天的股票數量比較：")
            for d in dates_to_compare:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM strategy_result 
                    WHERE date = ? 
                      AND margin_ratio IS NOT NULL 
                      AND margin_balance_shares > 0
                """, (d,))
                count = cursor.fetchone()[0]
                marker = " <-- 異常日期" if d == date else ""
                print(f"  {d}: {count} 檔股票{marker}")
    except Exception as e:
        print(f"\n[Warning] 無法比較前後幾天: {e}")
    
    conn.close()


def main():
    """主程式"""
    import argparse
    
    parser = argparse.ArgumentParser(description='異常日期檢測工具')
    parser.add_argument('--check-dates', nargs='+', 
                       help='檢查特定日期（例如: --check-dates 20200922 20211130）')
    parser.add_argument('--threshold', type=float, default=5.0,
                       help='差異百分比閾值（預設 5.0）')
    parser.add_argument('--diff-threshold', type=float, default=5.0,
                       help='差異絕對值閾值（預設 5.0）')
    parser.add_argument('--start-date', default='20200101',
                       help='開始日期（預設: 20200101）')
    parser.add_argument('--end-date', default='20251117',
                       help='結束日期（預設: 20251117）')
    parser.add_argument('--db-path', default='taiwan_stock.db',
                       help='資料庫路徑（預設: taiwan_stock.db）')
    
    args = parser.parse_args()
    
    # 如果指定了要檢查的日期，只檢查這些日期
    if args.check_dates:
        print(f"檢查特定日期: {', '.join(args.check_dates)}")
        df_results = check_specific_dates(args.check_dates, db_path=args.db_path)
        
        if not df_results.empty:
            print("\n" + "=" * 80)
            print("檢查結果：")
            print("=" * 80)
            display_cols = ['date', 'stock_count', 'avg_ratio', 'median_ratio', 'diff', 'diff_abs', 'diff_pct', 
                          'stocks_below_50', 'stocks_above_300', 'extreme_count', 'extreme_ratio']
            # 只顯示存在的欄位
            available_cols = [col for col in display_cols if col in df_results.columns]
            print(df_results[available_cols].to_string(index=False))
            
            # 找出異常日期（差異百分比 > threshold 或 差異絕對值 > diff_threshold）
            anomaly_results = df_results[
                ((df_results['diff_pct'].notna()) & (df_results['diff_pct'] > args.threshold)) |
                ((df_results['diff_abs'].notna()) & (df_results['diff_abs'] > args.diff_threshold))
            ]
            if not anomaly_results.empty:
                print(f"\n[Warning] 發現 {len(anomaly_results)} 個異常日期：")
                for _, row in anomaly_results.iterrows():
                    print(f"\n詳細分析 {row['date']}：")
                    analyze_anomaly_date(row['date'], db_path=args.db_path)
        else:
            print("\n[Warning] 沒有找到這些日期的資料")
        
        # 重要：使用 --check-dates 時，執行完後直接返回，不執行完整檢測
        print("\n" + "=" * 80)
        print("檢測完成")
        print("=" * 80)
        print("\n建議：")
        print("1. 檢查異常日期的詳細分析結果")
        print("2. 如果確認是數據問題，可以使用以下命令重新取得這些日期的資料：")
        print("   python margin_ratio_calculator.py --fetch-date <日期>")
        return
    
    # 找出所有異常日期
    df_all, anomaly_dates = find_anomaly_dates(
        threshold=args.threshold,
        diff_threshold=args.diff_threshold,
        start_date=args.start_date,
        end_date=args.end_date,
        db_path=args.db_path
    )
    
    if anomaly_dates.empty:
        print(f"\n[Info] 沒有發現異常日期（平均數與中位數差異都 < {args.threshold}% 且差異絕對值 < {args.diff_threshold}%）")
    else:
        print(f"\n[Warning] 發現 {len(anomaly_dates)} 個異常日期：")
        print("\n異常日期列表（按差異大小排序）：")
        print("-" * 80)
        display_cols = ['date', 'stock_count', 'avg_ratio', 'median_ratio', 'diff', 'diff_abs', 'diff_pct', 
                      'stocks_below_50', 'stocks_above_300', 'extreme_count', 'extreme_ratio']
        # 只顯示存在的欄位
        available_cols = [col for col in display_cols if col in anomaly_dates.columns]
        print(anomaly_dates[available_cols].to_string(index=False))
        
        # 儲存異常日期列表
        output_file = 'anomaly_dates.csv'
        anomaly_dates.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n[Info] 異常日期列表已儲存至: {output_file}")
        
        # 詳細分析前10個最異常的日期
        print("\n" + "=" * 80)
        print("詳細分析前10個最異常的日期：")
        print("=" * 80)
        
        for idx, row in anomaly_dates.head(10).iterrows():
            analyze_anomaly_date(row['date'], db_path=args.db_path)
    
    print("\n" + "=" * 80)
    print("檢測完成")
    print("=" * 80)
    print("\n建議：")
    print("1. 檢查異常日期的詳細分析結果")
    print("2. 如果確認是數據問題，可以使用以下命令重新取得這些日期的資料：")
    print("   python margin_ratio_calculator.py --fetch-date <日期>")


if __name__ == '__main__':
    main()