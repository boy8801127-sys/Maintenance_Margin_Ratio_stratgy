"""
刪除 strategy_result 表的所有資料
支援 SQLite 和 MySQL 兩個資料庫
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


def delete_strategy_result(db_path='taiwan_stock.db', mysql_config=None):
    """
    刪除 strategy_result 表的所有資料
    
    參數:
    - db_path: SQLite 資料庫路徑
    - mysql_config: MySQL 連接設定（字典），如果為 None 則只處理 SQLite
    """
    print("=" * 80)
    print("刪除 strategy_result 表的所有資料")
    print("=" * 80)
    
    # 確認
    print("\n[警告] 此操作將刪除 strategy_result 表的所有資料")
    print("       此操作無法復原！")
    response = input("\n確定要執行嗎？(yes/no): ")
    if response.lower() != 'yes':
        print("已取消操作")
        return
    
    # 1. 刪除 SQLite 資料庫中的資料
    print("\n[步驟1] 刪除 SQLite 資料庫中的 strategy_result 資料...")
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 先查詢資料筆數
        cursor.execute("SELECT COUNT(*) FROM strategy_result")
        count = cursor.fetchone()[0]
        print(f"  [Info] SQLite 資料庫中有 {count} 筆資料")
        
        if count > 0:
            # 刪除所有資料
            cursor.execute("DELETE FROM strategy_result")
            conn.commit()
            print(f"  [Info] 已刪除 SQLite 資料庫中的 {count} 筆資料")
        else:
            print(f"  [Info] SQLite 資料庫中沒有資料")
        
        conn.close()
        print(f"  [Info] SQLite 資料庫處理完成")
        
    except Exception as e:
        print(f"  [Error] SQLite 資料庫處理失敗: {e}")
    
    # 2. 刪除 MySQL 資料庫中的資料（如果啟用）
    if mysql_config is not None:
        print("\n[步驟2] 刪除 MySQL 資料庫中的 strategy_result 資料...")
        try:
            import pymysql
            mysql_conn = pymysql.connect(**mysql_config)
            mysql_cursor = mysql_conn.cursor()
            
            # 先查詢資料筆數
            mysql_cursor.execute("SELECT COUNT(*) FROM strategy_result")
            count = mysql_cursor.fetchone()[0]
            print(f"  [Info] MySQL 資料庫中有 {count} 筆資料")
            
            if count > 0:
                # 刪除所有資料
                mysql_cursor.execute("DELETE FROM strategy_result")
                mysql_conn.commit()
                print(f"  [Info] 已刪除 MySQL 資料庫中的 {count} 筆資料")
            else:
                print(f"  [Info] MySQL 資料庫中沒有資料")
            
            mysql_cursor.close()
            mysql_conn.close()
            print(f"  [Info] MySQL 資料庫處理完成")
            
        except ImportError:
            print(f"  [Warning] pymysql 未安裝，無法處理 MySQL 資料庫")
            print(f"           請執行: pip install pymysql")
        except Exception as e:
            print(f"  [Error] MySQL 資料庫處理失敗: {e}")
    else:
        print("\n[步驟2] 跳過 MySQL 資料庫（未設定）")
    
    print("\n" + "=" * 80)
    print("刪除完成")
    print("=" * 80)
    print("\n建議：")
    print("1. 確認原始資料（tw_stock_price_data 和 twse_margin_data）已正確更新")
    print("2. 重新執行滾動計算：")
    print("   python margin_ratio_calculator.py --rolling 60")


def main():
    """主程式"""
    import argparse
    from configparser import ConfigParser
    
    parser = argparse.ArgumentParser(description='刪除 strategy_result 表的所有資料')
    parser.add_argument('--db-path', default='taiwan_stock.db',
                       help='SQLite 資料庫路徑（預設: taiwan_stock.db）')
    parser.add_argument('--mysql-config', type=str,
                       help='MySQL 設定檔路徑（例如: mysql_config.ini）')
    
    args = parser.parse_args()
    
    # 讀取 MySQL 設定（如果提供）
    mysql_config = None
    if args.mysql_config and os.path.exists(args.mysql_config):
        try:
            config = ConfigParser()
            config.read(args.mysql_config)
            mysql_config = {
                'host': config.get('mysql', 'host'),
                'port': config.getint('mysql', 'port'),
                'user': config.get('mysql', 'user'),
                'password': config.get('mysql', 'password'),
                'database': config.get('mysql', 'database')
            }
            print(f"[Info] 已讀取 MySQL 設定檔: {args.mysql_config}")
        except Exception as e:
            print(f"[Warning] 無法讀取 MySQL 設定檔: {e}")
            print("[Info] 將只處理 SQLite 資料庫")
    else:
        # 如果沒有提供 MySQL 設定檔，檢查 margin_ratio_calculator.py 中是否有預設設定
        # 這裡可以從 margin_ratio_calculator.py 讀取 MySQL 設定
        # 但為了簡化，我們先只處理 SQLite
        pass
    
    delete_strategy_result(db_path=args.db_path, mysql_config=mysql_config)


if __name__ == '__main__':
    main()