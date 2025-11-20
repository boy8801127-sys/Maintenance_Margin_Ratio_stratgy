"""
台股融資維持率分析系統 - 主入口檔案
統整所有功能模組，提供統一的命令列介面
"""

import os
import sys
import subprocess

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


def print_header():
    """顯示標題"""
    print("=" * 80)
    print("台股融資維持率分析系統")
    print("=" * 80)
    print()


def print_menu():
    """顯示主選單"""
    print("\n【主要功能模組】")
    print()
    print("1. 資料取得與計算 (margin_ratio_calculator.py)")
    print("   - 從證交所 API 取得融資融券資料和股價資料")
    print("   - 計算融資維持率")
    print("   - 滾動計算歷史資料")
    print()
    print("2. 互動式圖表產生 (interactive_chart_generator.py)")
    print("   - 產生大盤整體融資維持率統計圖表")
    print("   - 產生個股融資維持率與相關數據圖表")
    print()
    print("3. 異常日期檢測 (find_anomaly_dates.py)")
    print("   - 找出平均數與中位數差異過大的異常日期")
    print("   - 檢查特定日期是否異常")
    print()
    print("4. 異常日期處理 (delete_anomaly_dates.py)")
    print("   - 刪除異常日期的原始資料")
    print()
    print("5. 資料修復 (fix_anomaly_dates_advanced.py)")
    print("   - 修復異常日期的股價資料")
    print()
    print("6. 策略結果管理 (delete_strategy_result.py)")
    print("   - 刪除 strategy_result 表的所有資料")
    print()
    print("7. 資料匯出 (for_orange.py)")
    print("   - 匯出資料供 Orange 機器學習使用")
    print()
    print("8. 策略回測 (margin_ratio_backtest.py)")
    print("   - 執行融資維持率策略回測")
    print("   - 產生績效報告和圖表")
    print()
    print("0. 退出")
    print()


def show_calculator_help():
    """顯示 margin_ratio_calculator.py 的使用說明"""
    print("\n" + "=" * 80)
    print("資料取得與計算 (margin_ratio_calculator.py)")
    print("=" * 80)
    print("\n【主要功能】")
    print("1. 單日更新：更新今日融資維持率資料")
    print("2. 批次更新：補抓多天資料（只抓資料，不計算維持率）")
    print("3. 滾動計算：從歷史資料開始計算融資成本和維持率")
    print("4. 查詢功能：查詢10天平均維持率或策略信號")
    print()
    print("【使用指令】")
    print("  python margin_ratio_calculator.py                    # 單日更新")
    print("  python margin_ratio_calculator.py --batch 60          # 批次更新60天")
    print("  python margin_ratio_calculator.py --fetch-date 20231222  # 取得指定日期資料")
    print("  python margin_ratio_calculator.py --rolling 60        # 滾動計算60天")
    print("  python margin_ratio_calculator.py --rolling 60 --force # 強制重新計算")
    print("  python margin_ratio_calculator.py --query-10day       # 查詢10天平均維持率")
    print("  python margin_ratio_calculator.py --query-10day --strategy  # 查詢策略信號")
    print("  python margin_ratio_calculator.py --strategy-table    # 產生策略結果表")
    print()
    print("【建議流程】")
    print("  步驟1: python margin_ratio_calculator.py --batch 60")
    print("  步驟2: python margin_ratio_calculator.py --rolling 60")
    print("  步驟3: python margin_ratio_calculator.py --strategy-table")
    print()


def show_chart_generator_help():
    """顯示 interactive_chart_generator.py 的使用說明"""
    print("\n" + "=" * 80)
    print("互動式圖表產生 (interactive_chart_generator.py)")
    print("=" * 80)
    print("\n【主要功能】")
    print("1. 大盤整體融資維持率統計圖表")
    print("   - 平均維持率、中位數維持率")
    print("   - 每日有融資餘額的股票數量")
    print()
    print("2. 個股融資維持率與相關數據圖表")
    print("   - 融資維持率（含10日平均）")
    print("   - 融資餘額（股數）")
    print("   - 收盤價")
    print("   - 成交量")
    print()
    print("【使用指令】")
    print("  python interactive_chart_generator.py")
    print()
    print("【說明】")
    print("  執行後會以互動方式詢問要產生大盤或個股圖表")
    print("  大盤圖表：顯示整體市場統計")
    print("  個股圖表：需要輸入股票代號和日期範圍")
    print()


def show_find_anomaly_help():
    """顯示 find_anomaly_dates.py 的使用說明"""
    print("\n" + "=" * 80)
    print("異常日期檢測 (find_anomaly_dates.py)")
    print("=" * 80)
    print("\n【主要功能】")
    print("  找出平均數與中位數差異過大的異常日期")
    print("  檢測條件：")
    print("    - 平均數與中位數差異百分比 > 閾值")
    print("    - 平均數與中位數差異絕對值 > 閾值")
    print("    - 極端維持率股票比例過高")
    print()
    print("【使用指令】")
    print("  python find_anomaly_dates.py                          # 完整掃描")
    print("  python find_anomaly_dates.py --threshold 5.0          # 設定差異百分比閾值")
    print("  python find_anomaly_dates.py --diff-threshold 5.0      # 設定差異絕對值閾值")
    print("  python find_anomaly_dates.py --start-date 20200101    # 設定開始日期")
    print("  python find_anomaly_dates.py --end-date 20251117     # 設定結束日期")
    print("  python find_anomaly_dates.py --check-dates 20231222 20200922  # 檢查特定日期")
    print()


def show_delete_anomaly_help():
    """顯示 delete_anomaly_dates.py 的使用說明"""
    print("\n" + "=" * 80)
    print("異常日期處理 (delete_anomaly_dates.py)")
    print("=" * 80)
    print("\n【主要功能】")
    print("  刪除異常日期的原始資料（tw_stock_price_data 和 twse_margin_data）")
    print("  注意：不會刪除 strategy_result 表的資料")
    print()
    print("【使用指令】")
    print("  python delete_anomaly_dates.py")
    print()
    print("【說明】")
    print("  執行後會以互動方式詢問要刪除哪些日期")
    print("  輸入日期格式：YYYYMMDD（例如：20231222）")
    print("  可以輸入多個日期，用空白或逗號分隔")
    print("  會進行二次確認後才執行刪除")
    print()


def show_fix_anomaly_help():
    """顯示 fix_anomaly_dates_advanced.py 的使用說明"""
    print("\n" + "=" * 80)
    print("資料修復 (fix_anomaly_dates_advanced.py)")
    print("=" * 80)
    print("\n【主要功能】")
    print("  修復異常日期的股價資料")
    print("  使用前後交易日的資料來修正異常值")
    print()
    print("【使用指令】")
    print("  python fix_anomaly_dates_advanced.py")
    print()
    print("【說明】")
    print("  執行後會自動檢測並修復異常日期的股價資料")
    print("  建議在刪除異常日期資料後，重新取得資料，再執行此工具")
    print()


def show_delete_strategy_result_help():
    """顯示 delete_strategy_result.py 的使用說明"""
    print("\n" + "=" * 80)
    print("策略結果管理 (delete_strategy_result.py)")
    print("=" * 80)
    print("\n【主要功能】")
    print("  刪除 strategy_result 表的所有資料")
    print("  支援 SQLite 和 MySQL 兩個資料庫")
    print()
    print("【使用指令】")
    print("  python delete_strategy_result.py")
    print()
    print("【說明】")
    print("  執行後會要求確認，確認後才會刪除資料")
    print("  此操作無法復原，請謹慎使用")
    print("  建議在重新計算維持率前執行，清除舊資料")
    print()


def show_export_help():
    """顯示 for_orange.py 的使用說明"""
    print("\n" + "=" * 80)
    print("資料匯出 (for_orange.py)")
    print("=" * 80)
    print("\n【主要功能】")
    print("  匯出資料供 Orange 機器學習使用")
    print("  包含特徵工程（計算前一日數值、變化率等）")
    print()
    print("【使用指令】")
    print("  python for_orange.py                                  # 匯出所有股票資料")
    print("  python for_orange.py --start-date 20200101            # 設定開始日期")
    print("  python for_orange.py --end-date 20251117              # 設定結束日期")
    print("  python for_orange.py --ticker 2330                    # 只匯出特定股票")
    print("  python for_orange.py --no-features                    # 不進行特徵工程")
    print("  python for_orange.py --output my_data.csv             # 指定輸出檔名")
    print()


def show_backtest_help():
    """顯示 margin_ratio_backtest.py 的使用說明"""
    print("\n" + "=" * 80)
    print("策略回測 (margin_ratio_backtest.py)")
    print("=" * 80)
    print("\n【主要功能】")
    print("  執行融資維持率策略回測")
    print("  策略條件：")
    print("    1. 融資維持率 < 過去10日移動平均值（進入異常低檔狀態）")
    print("    2. 先篩選融資維持率跌幅前10名的股票")
    print("    3. 再通過三項濾網：")
    print("       - 成交量 < 過去10日平均量（籌碼面趨於穩定，散戶已出場）")
    print("       - 當日為紅K（收盤價 > 開盤價）")
    print("       - 融資餘額 > 前5日平均融資餘額 × 0.95")
    print()
    print("  操作規則：")
    print("    - 符合條件後，隔日開盤價買進（市價單）")
    print("    - 如果15日內有新訊號，再次買進加碼（更新進場日期和加權平均成本）")
    print("    - 買進時同時掛停損單（-10%），如果當日最低價觸及停損價格則觸發")
    print("    - 每次使用 1/10 的現金")
    print("    - 停利 +40%，停損 -10%（可選擇停用）")
    print("    - 持有15個交易日或達停損/停利即出場")
    print("    - 回測結束時保留持倉（不賣出）")
    print()
    print("【使用指令】")
    print("  python margin_ratio_backtest.py                      # 預設回測（2020-2025）")
    print("  python margin_ratio_backtest.py --start-date 20200101 # 設定開始日期")
    print("  python margin_ratio_backtest.py --end-date 20251117   # 設定結束日期")
    print("  python margin_ratio_backtest.py --capital 2000000     # 設定初始資金（預設100萬）")
    print("  python margin_ratio_backtest.py --no-take-profit     # 停用停利（無止盈）")
    print("  python margin_ratio_backtest.py --no-stop-loss       # 停用停損（無止損）")
    print()
    print("【輸出結果】")
    print("  - 回測報告（總報酬率、勝率、夏普比率等）")
    print("  - 交易記錄 CSV 檔案")
    print("  - 績效圖表 PNG 檔案")
    print()


def get_backtest_params():
    """
    互動式取得回測參數
    
    回傳: (start_date, end_date, capital, no_take_profit, no_stop_loss)
    """
    print("\n" + "=" * 80)
    print("設定回測參數")
    print("=" * 80)
    
    # 開始日期
    print("\n【開始日期】")
    start_date = input("請輸入開始日期（格式: YYYYMMDD，例如: 20200101，直接按 Enter 使用預設值 20200101）: ").strip()
    if not start_date:
        start_date = '20200101'
    else:
        # 驗證日期格式
        if len(start_date) != 8 or not start_date.isdigit():
            print("[Warning] 日期格式錯誤，使用預設值 20200101")
            start_date = '20200101'
        else:
            year = int(start_date[:4])
            month = int(start_date[4:6])
            day = int(start_date[6:8])
            if not (2000 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31):
                print("[Warning] 日期不合理，使用預設值 20200101")
                start_date = '20200101'
    
    # 結束日期
    print("\n【結束日期】")
    end_date = input("請輸入結束日期（格式: YYYYMMDD，例如: 20251117，直接按 Enter 使用預設值 20251117）: ").strip()
    if not end_date:
        end_date = '20251117'
    else:
        # 驗證日期格式
        if len(end_date) != 8 or not end_date.isdigit():
            print("[Warning] 日期格式錯誤，使用預設值 20251117")
            end_date = '20251117'
        else:
            year = int(end_date[:4])
            month = int(end_date[4:6])
            day = int(end_date[6:8])
            if not (2000 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31):
                print("[Warning] 日期不合理，使用預設值 20251117")
                end_date = '20251117'
            elif end_date < start_date:
                print("[Warning] 結束日期必須大於或等於開始日期，使用預設值 20251117")
                end_date = '20251117'
    
    # 初始資金
    print("\n【初始資金】")
    capital_input = input("請輸入初始資金（新台幣，例如: 1000000，直接按 Enter 使用預設值 1000000）: ").strip()
    if not capital_input:
        capital = 1000000
    else:
        try:
            capital = float(capital_input)
            if capital <= 0:
                print("[Warning] 初始資金必須大於 0，使用預設值 1000000")
                capital = 1000000
        except ValueError:
            print("[Warning] 輸入格式錯誤，使用預設值 1000000")
            capital = 1000000
    
    # 停利設定
    print("\n【停利設定】")
    take_profit_input = input("是否啟用停利（+40%）？(y/n，預設 y): ").strip().lower()
    no_take_profit = (take_profit_input == 'n')
    
    # 停損設定
    print("\n【停損設定】")
    stop_loss_input = input("是否啟用停損（-10%）？(y/n，預設 y): ").strip().lower()
    no_stop_loss = (stop_loss_input == 'n')
    
    # 顯示設定摘要
    print("\n" + "=" * 80)
    print("回測參數設定摘要")
    print("=" * 80)
    print(f"開始日期: {start_date}")
    print(f"結束日期: {end_date}")
    print(f"初始資金: NT$ {capital:,.0f}")
    print(f"停利: {'未啟用' if no_take_profit else '啟用 (+40%)'}")
    print(f"停損: {'未啟用' if no_stop_loss else '啟用 (-10%)'}")
    print(f"持有期: 15 個交易日")
    print("=" * 80)
    
    return start_date, end_date, capital, no_take_profit, no_stop_loss


def run_command(script_name, args=None):
    """執行 Python 腳本"""
    cmd = [sys.executable, script_name]
    if args:
        cmd.extend(args)
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"\n[Error] 執行失敗: {e}")
    except KeyboardInterrupt:
        print("\n[Info] 已取消執行")
    except Exception as e:
        print(f"\n[Error] 發生錯誤: {e}")


def main():
    """主程式"""
    print_header()
    
    while True:
        print_menu()
        choice = input("請選擇功能 (輸入數字): ").strip()
        
        if choice == '0':
            print("\n感謝使用！")
            break
        elif choice == '1':
            show_calculator_help()
            run = input("\n是否要執行？(y/n): ").strip().lower()
            if run == 'y':
                print("\n請輸入指令（例如: --batch 60 或 --rolling 60）")
                print("直接按 Enter 執行單日更新")
                args_input = input("指令: ").strip()
                args = args_input.split() if args_input else []
                run_command('margin_ratio_calculator.py', args)
        elif choice == '2':
            show_chart_generator_help()
            run = input("\n是否要執行？(y/n): ").strip().lower()
            if run == 'y':
                run_command('interactive_chart_generator.py')
        elif choice == '3':
            show_find_anomaly_help()
            run = input("\n是否要執行？(y/n): ").strip().lower()
            if run == 'y':
                print("\n請輸入指令參數（直接按 Enter 使用預設值）")
                args_input = input("指令: ").strip()
                args = args_input.split() if args_input else []
                run_command('find_anomaly_dates.py', args)
        elif choice == '4':
            show_delete_anomaly_help()
            run = input("\n是否要執行？(y/n): ").strip().lower()
            if run == 'y':
                run_command('delete_anomaly_dates.py')
        elif choice == '5':
            show_fix_anomaly_help()
            run = input("\n是否要執行？(y/n): ").strip().lower()
            if run == 'y':
                run_command('fix_anomaly_dates_advanced.py')
        elif choice == '6':
            show_delete_strategy_result_help()
            run = input("\n是否要執行？(y/n): ").strip().lower()
            if run == 'y':
                run_command('delete_strategy_result.py')
        elif choice == '7':
            show_export_help()
            run = input("\n是否要執行？(y/n): ").strip().lower()
            if run == 'y':
                print("\n請輸入指令參數（直接按 Enter 使用預設值）")
                args_input = input("指令: ").strip()
                args = args_input.split() if args_input else []
                run_command('for_orange.py', args)
        elif choice == '8':
            show_backtest_help()
            run = input("\n是否要執行回測？(y/n): ").strip().lower()
            if run == 'y':
                # 互動式取得參數
                start_date, end_date, capital, no_take_profit, no_stop_loss = get_backtest_params()
                
                # 確認執行
                confirm = input("\n確定要開始回測嗎？(y/n): ").strip().lower()
                if confirm == 'y':
                    args = [
                        '--start-date', start_date,
                        '--end-date', end_date,
                        '--capital', str(int(capital))
                    ]
                    if no_take_profit:
                        args.append('--no-take-profit')
                    if no_stop_loss:
                        args.append('--no-stop-loss')
                    
                    print("\n開始執行回測...")
                    print("=" * 80)
                    run_command('margin_ratio_backtest.py', args)
                else:
                    print("已取消回測")
        else:
            print("\n[Warning] 無效的選項，請重新選擇")
        
        input("\n按 Enter 繼續...")


if __name__ == '__main__':
    main()

