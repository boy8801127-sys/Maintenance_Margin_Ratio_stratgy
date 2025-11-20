"""
資料匯出工具 - 供 Orange 機器學習使用
將資料庫中的融資維持率數據匯出為 CSV 格式
"""

import sqlite3
import pandas as pd
import os
import sys
from datetime import datetime

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


def export_for_ml(db_path='taiwan_stock.db', start_date='20200101', end_date='20251117', 
                  output_file=None, include_features=True):
    """
    匯出資料供機器學習使用
    
    參數:
    - db_path: 資料庫路徑
    - start_date: 開始日期（YYYYMMDD）
    - end_date: 結束日期（YYYYMMDD）
    - output_file: 輸出檔案名稱（如果為 None，則自動產生）
    - include_features: 是否包含特徵工程（如：計算前一日維持率、價格變化等）
    """
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"資料庫檔案不存在: {db_path}")
    
    print("=" * 80)
    print("融資維持率資料匯出工具（供 Orange 機器學習使用）")
    print("=" * 80)
    
    conn = sqlite3.connect(db_path)
    
    # 基本查詢：取得所有相關資料
    query = """
    SELECT 
        date,
        ticker,
        stock_name,
        margin_ratio,
        margin_cost_est,
        margin_balance_amount,
        margin_balance_shares,
        avg_10day_ratio,
        volume,
        avg_10day_volume,
        open_price,
        close_price,
        avg_5day_balance_95
    FROM strategy_result
    WHERE date >= ? AND date <= ?
        AND margin_ratio IS NOT NULL
        AND margin_ratio > 0
        AND margin_balance_shares > 0
    ORDER BY ticker, date
    """
    
    print(f"\n[1/3] 讀取資料庫資料（{start_date} - {end_date}）...")
    df = pd.read_sql_query(query, conn, params=(start_date, end_date))
    
    if df.empty:
        print("[Error] 沒有找到資料")
        conn.close()
        return None
    
    print(f"[Info] 讀取了 {len(df):,} 筆資料")
    print(f"[Info] 涵蓋 {df['ticker'].nunique()} 檔股票")
    print(f"[Info] 日期範圍: {df['date'].min()} 至 {df['date'].max()}")
    
    # 特徵工程（可選）
    if include_features:
        print("\n[2/3] 進行特徵工程...")
        
        # 轉換日期為 datetime
        df['date_dt'] = pd.to_datetime(df['date'], format='%Y%m%d')
        
        # 按股票排序並計算特徵
        df = df.sort_values(['ticker', 'date_dt']).reset_index(drop=True)
        
        # 計算前一日維持率（作為特徵）
        df['prev_margin_ratio'] = df.groupby('ticker')['margin_ratio'].shift(1)
        
        # 計算維持率變化
        df['margin_ratio_change'] = df.groupby('ticker')['margin_ratio'].diff()
        
        # 計算維持率變化百分比
        df['margin_ratio_change_pct'] = df['margin_ratio'] / df['prev_margin_ratio'] - 1
        
        # 計算前一日收盤價
        df['prev_close_price'] = df.groupby('ticker')['close_price'].shift(1)
        
        # 計算價格變化
        df['price_change'] = df.groupby('ticker')['close_price'].diff()
        df['price_change_pct'] = df['close_price'] / df['prev_close_price'] - 1
        
        # 計算前一日成交量
        df['prev_volume'] = df.groupby('ticker')['volume'].shift(1)
        
        # 計算成交量變化
        df['volume_change_pct'] = df['volume'] / df['prev_volume'] - 1
        
        # 計算融資餘額變化
        df['prev_margin_balance'] = df.groupby('ticker')['margin_balance_shares'].shift(1)
        df['margin_balance_change_pct'] = df['margin_balance_shares'] / df['prev_margin_balance'] - 1
        
        # 計算是否接近追繳線（維持率 < 130%）
        df['near_margin_call'] = (df['margin_ratio'] < 130).astype(int)
        
        # 計算是否接近斷頭線（維持率 < 120%）
        df['near_liquidation'] = (df['margin_ratio'] < 120).astype(int)
        
        # 計算風險等級（分類標籤）
        def get_risk_level(ratio):
            if ratio < 120:
                return '極高風險'  # 斷頭風險
            elif ratio < 130:
                return '高風險'    # 追繳風險
            elif ratio < 150:
                return '中風險'    # 需要注意
            elif ratio < 200:
                return '低風險'    # 正常
            else:
                return '極低風險'  # 非常安全
        
        df['risk_level'] = df['margin_ratio'].apply(get_risk_level)
        
        # 計算維持率是否會下降（目標變數，預測未來3日是否會下降10%以上）
        df['future_margin_ratio'] = df.groupby('ticker')['margin_ratio'].shift(-3)
        df['will_drop_10pct'] = ((df['margin_ratio'] - df['future_margin_ratio']) / df['margin_ratio'] > 0.1).astype(int)
        df['will_drop_10pct'] = df['will_drop_10pct'].fillna(0).astype(int)
        
        # 移除日期欄位（機器學習不需要，或可保留年份、月份等）
        df['year'] = df['date_dt'].dt.year
        df['month'] = df['date_dt'].dt.month
        df['day'] = df['date_dt'].dt.day
        df['day_of_week'] = df['date_dt'].dt.dayofweek  # 0=週一, 6=週日
        
        # 移除 datetime 欄位（保留原始 date 字串）
        df = df.drop('date_dt', axis=1)
        
        print("[Info] 特徵工程完成")
        print(f"[Info] 新增了以下特徵：")
        print("  - prev_margin_ratio: 前一日維持率")
        print("  - margin_ratio_change: 維持率變化")
        print("  - price_change_pct: 價格變化百分比")
        print("  - volume_change_pct: 成交量變化百分比")
        print("  - near_margin_call: 是否接近追繳線（130%）")
        print("  - near_liquidation: 是否接近斷頭線（120%）")
        print("  - risk_level: 風險等級（分類標籤）")
        print("  - will_drop_10pct: 未來3日是否會下降10%以上（預測目標）")
    
    # 移除包含 NaN 的列（如果有特徵工程的話）
    if include_features:
        original_len = len(df)
        df = df.dropna(subset=['margin_ratio', 'close_price', 'volume'])
        removed = original_len - len(df)
        if removed > 0:
            print(f"[Info] 移除了 {removed} 筆包含缺失值的資料")
    
    # 產生輸出檔案名稱
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f'orange_ml_data_{start_date}_{end_date}_{timestamp}.csv'
    
    print(f"\n[3/3] 匯出資料至: {output_file}")
    
    # 匯出為 CSV（使用 UTF-8-sig 編碼，確保 Excel 和 Orange 都能正確讀取中文）
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"[Info] 匯出完成！")
    print(f"[Info] 共匯出 {len(df):,} 筆資料，{len(df.columns)} 個欄位")
    print(f"\n欄位列表：")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i:2d}. {col}")
    
    conn.close()
    
    print("\n" + "=" * 80)
    print("下一步：")
    print("1. 安裝 Orange（如果還沒安裝）")
    print("2. 開啟 Orange，使用 File → Open 讀取此 CSV 檔案")
    print("3. 參考 Orange_使用指南.md 進行機器學習分析")
    print("=" * 80)
    
    return output_file


def export_single_stock(db_path='taiwan_stock.db', ticker='2330', 
                       start_date='20200101', end_date='20251117', output_file=None):
    """
    匯出單一股票的資料
    """
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"資料庫檔案不存在: {db_path}")
    
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT 
        date,
        ticker,
        stock_name,
        margin_ratio,
        margin_cost_est,
        margin_balance_amount,
        margin_balance_shares,
        avg_10day_ratio,
        volume,
        avg_10day_volume,
        open_price,
        close_price,
        avg_5day_balance_95
    FROM strategy_result
    WHERE ticker = ? AND date >= ? AND date <= ?
        AND margin_ratio IS NOT NULL
        AND margin_ratio > 0
        AND margin_balance_shares > 0
    ORDER BY date
    """
    
    print(f"\n[Info] 匯出 {ticker} 的資料...")
    df = pd.read_sql_query(query, conn, params=(ticker, start_date, end_date))
    conn.close()
    
    if df.empty:
        print(f"[Error] 沒有找到 {ticker} 的資料")
        return None
    
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f'orange_ml_data_{ticker}_{start_date}_{end_date}_{timestamp}.csv'
    
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"[Info] 已匯出至: {output_file}")
    print(f"[Info] 共 {len(df):,} 筆資料")
    
    return output_file


def main():
    """主程式"""
    import argparse
    
    parser = argparse.ArgumentParser(description='匯出資料供 Orange 機器學習使用')
    parser.add_argument('--db', default='taiwan_stock.db', help='資料庫路徑')
    parser.add_argument('--start-date', default='20200101', help='開始日期 (YYYYMMDD)')
    parser.add_argument('--end-date', default='20251117', help='結束日期 (YYYYMMDD)')
    parser.add_argument('--ticker', help='只匯出特定股票代號（例如: 2330）')
    parser.add_argument('--output', help='輸出檔案名稱（如果為 None，則自動產生）')
    parser.add_argument('--no-features', action='store_true', help='不進行特徵工程')
    
    args = parser.parse_args()
    
    if args.ticker:
        export_single_stock(
            db_path=args.db,
            ticker=args.ticker,
            start_date=args.start_date,
            end_date=args.end_date,
            output_file=args.output
        )
    else:
        export_for_ml(
            db_path=args.db,
            start_date=args.start_date,
            end_date=args.end_date,
            output_file=args.output,
            include_features=not args.no_features
        )


if __name__ == '__main__':
    main()