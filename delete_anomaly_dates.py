"""
刪除異常日期原始資料工具
只刪除 tw_stock_price_data 和 twse_margin_data 表中的異常日期資料
不刪除 strategy_result 表的資料（由用戶自行決定是否刪除）
"""

import sqlite3
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


def delete_anomaly_dates(db_path='taiwan_stock.db', dates=None):
    """
    刪除異常日期的原始資料
    
    參數:
    - db_path: 資料庫路徑
    - dates: 要刪除的日期列表（必須提供，不能為 None）
    """
    if dates is None or len(dates) == 0:
        print("[Error] 沒有指定要刪除的日期")
        return
    
    print("=" * 80)
    print("刪除異常日期原始資料工具")
    print("=" * 80)
    print(f"\n將刪除以下 {len(dates)} 個異常日期的原始資料：")
    for date in dates:
        print(f"  - {date}")
    
    print("\n注意：只會刪除 tw_stock_price_data 和 twse_margin_data 表的資料")
    print("      strategy_result 表的資料不會被刪除（請自行決定是否刪除）")
    
    # 確認
    response = input("\n確定要執行嗎？(yes/no): ")
    if response.lower() != 'yes':
        print("已取消操作")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 1. 刪除 tw_stock_price_data 表中的異常日期資料
        print("\n[步驟1] 刪除 tw_stock_price_data 表中的異常日期資料...")
        deleted_count_price = 0
        for date in dates:
            cursor.execute("DELETE FROM tw_stock_price_data WHERE date = ?", (date,))
            count = cursor.rowcount
            deleted_count_price += count
            if count > 0:
                print(f"  [Info] {date}: 刪除 {count} 筆股價資料")
            else:
                print(f"  [Warning] {date}: 沒有找到股價資料")
        
        print(f"  [Info] 總共刪除 {deleted_count_price} 筆股價資料")
        
        # 2. 刪除 twse_margin_data 表中的異常日期資料
        print("\n[步驟2] 刪除 twse_margin_data 表中的異常日期資料...")
        deleted_count_margin = 0
        for date in dates:
            cursor.execute("DELETE FROM twse_margin_data WHERE date = ?", (date,))
            count = cursor.rowcount
            deleted_count_margin += count
            if count > 0:
                print(f"  [Info] {date}: 刪除 {count} 筆融資融券資料")
            else:
                print(f"  [Warning] {date}: 沒有找到融資融券資料")
        
        print(f"  [Info] 總共刪除 {deleted_count_margin} 筆融資融券資料")
        
        conn.commit()
        print("\n" + "=" * 80)
        print("刪除完成")
        print("=" * 80)
        print(f"已刪除股價資料: {deleted_count_price} 筆")
        print(f"已刪除融資融券資料: {deleted_count_margin} 筆")
        
        print("\n建議：")
        print("1. 使用以下命令重新取得異常日期的正確資料：")
        for date in dates:
            print(f"   python margin_ratio_calculator.py --fetch-date {date}")
        print("\n2. 確認資料正確後，刪除 strategy_result 表的資料：")
        print("   python delete_strategy_result.py")
        print("\n3. 重新執行滾動計算：")
        print("   python margin_ratio_calculator.py --rolling 60")
        
    except Exception as e:
        conn.rollback()
        print(f"\n[Error] 刪除資料時發生錯誤: {e}")
        print("已回滾所有變更")
    finally:
        conn.close()


def get_dates_from_user():
    """
    互動式取得要刪除的日期列表
    
    回傳:
    - 日期列表（字串列表，格式: ['20200922', '20211130']）
    - 如果沒有輸入有效日期，回傳空列表
    """
    print("=" * 80)
    print("刪除異常日期原始資料工具（互動式）")
    print("=" * 80)
    print("\n請輸入要刪除的日期（格式: YYYYMMDD，例如: 20200922）")
    print("可以輸入多個日期，用空格或逗號分隔")
    print("輸入 'q' 或 'quit' 結束輸入")
    print("\n範例：")
    print("  20200922 20211130")
    print("  或")
    print("  20200922,20211130")
    
    dates = []
    
    while True:
        user_input = input("\n請輸入日期（或 'q' 結束）: ").strip()
        
        if user_input.lower() in ['q', 'quit', 'exit']:
            break
        
        if not user_input:
            print("  [Warning] 請輸入日期")
            continue
        
        # 處理多個日期（支援空格或逗號分隔）
        input_dates = []
        if ',' in user_input:
            input_dates = [d.strip() for d in user_input.split(',')]
        else:
            input_dates = user_input.split()
        
        # 驗證日期格式
        valid_dates = []
        for date_str in input_dates:
            date_str = date_str.strip()
            if len(date_str) == 8 and date_str.isdigit():
                # 進一步驗證日期合理性（年份應該在合理範圍內）
                year = int(date_str[:4])
                month = int(date_str[4:6])
                day = int(date_str[6:8])
                
                if 2000 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                    valid_dates.append(date_str)
                    if date_str not in dates:
                        dates.append(date_str)
                        print(f"  [Info] 已加入日期: {date_str}")
                    else:
                        print(f"  [Warning] {date_str} 已經在列表中，跳過")
                else:
                    print(f"  [Warning] {date_str} 日期不合理（年份應在 2000-2100，月份應在 1-12，日期應在 1-31），跳過")
            else:
                print(f"  [Warning] {date_str} 格式錯誤（應為 YYYYMMDD，例如: 20200922），跳過")
        
        if valid_dates:
            # 詢問是否繼續輸入
            continue_input = input("\n是否繼續輸入其他日期？(y/n): ").strip().lower()
            if continue_input not in ['y', 'yes', '是']:
                break
    
    return dates


def main():
    """主程式"""
    import argparse
    
    parser = argparse.ArgumentParser(description='刪除異常日期原始資料工具')
    parser.add_argument('--dates', nargs='+', 
                       help='要刪除的日期列表（例如: --dates 20200922 20211130）')
    parser.add_argument('--db-path', default='taiwan_stock.db',
                       help='資料庫路徑（預設: taiwan_stock.db）')
    parser.add_argument('--interactive', action='store_true',
                       help='互動式模式（詢問要刪除哪些日期）')
    
    args = parser.parse_args()
    
    # 決定要刪除的日期
    dates_to_delete = None
    
    if args.interactive or (not args.dates):
        # 互動式模式：詢問用戶要刪除哪些日期
        dates_to_delete = get_dates_from_user()
        
        if not dates_to_delete:
            print("\n[Error] 沒有輸入任何有效日期，已取消操作")
            print("[Info] 請重新執行並輸入正確格式的日期（YYYYMMDD）")
            return
        
        # 顯示要刪除的日期並確認
        print("\n" + "=" * 80)
        print("確認要刪除的日期：")
        print("=" * 80)
        print(f"\n共 {len(dates_to_delete)} 個日期：")
        for i, date in enumerate(dates_to_delete, 1):
            print(f"  {i}. {date}")
        
        # 第二次確認
        print("\n" + "=" * 80)
        confirm = input("\n請再次確認，確定要刪除以上日期的原始資料嗎？(yes/no): ").strip().lower()
        if confirm not in ['yes', 'y', '是']:
            print("已取消操作")
            return
    elif args.dates:
        # 命令列模式：使用 --dates 參數
        # 驗證日期格式
        valid_dates = []
        for date_str in args.dates:
            date_str = date_str.strip()
            if len(date_str) == 8 and date_str.isdigit():
                # 進一步驗證日期合理性
                year = int(date_str[:4])
                month = int(date_str[4:6])
                day = int(date_str[6:8])
                
                if 2000 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                    valid_dates.append(date_str)
                else:
                    print(f"[Error] {date_str} 日期不合理（年份應在 2000-2100，月份應在 1-12，日期應在 1-31）")
            else:
                print(f"[Error] {date_str} 格式錯誤（應為 YYYYMMDD，例如: 20200922）")
        
        if not valid_dates:
            print("\n[Error] 沒有有效的日期，已取消操作")
            print("[Info] 請使用正確格式的日期（YYYYMMDD）")
            return
        
        dates_to_delete = valid_dates
    
    # 執行刪除
    if dates_to_delete:
        delete_anomaly_dates(db_path=args.db_path, dates=dates_to_delete)
    else:
        print("[Error] 沒有指定要刪除的日期")


if __name__ == '__main__':
    main()