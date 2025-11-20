"""
台股融資維持率互動式圖表產生器
使用 Plotly 產生可互動的 HTML 圖表
"""

import sqlite3
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
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


class InteractiveChartGenerator:
    """互動式圖表產生器"""
    
    def __init__(self, db_path='taiwan_stock.db'):
        self.db_path = db_path
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"資料庫檔案不存在: {db_path}")
    
    def get_daily_statistics(self, start_date='20190701', end_date='20251117'):
        """取得每日統計資料（排除融資餘額為0的股票）"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            date,
            COUNT(*) as stock_count,
            AVG(margin_ratio) as avg_ratio
        FROM strategy_result
        WHERE date >= ? AND date <= ?
            AND margin_ratio IS NOT NULL
            AND margin_ratio > 0
            AND margin_balance_shares > 0
        GROUP BY date
        ORDER BY date
        """
        
        df = pd.read_sql_query(query, conn, params=(start_date, end_date))
        conn.close()
        
        if df.empty:
            return df
        
        # 轉換日期格式
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        
        # 計算中位數（排除融資餘額為0的股票）
        print("[Info] 正在計算每日中位數...")
        conn = sqlite3.connect(self.db_path)
        median_data = []
        total_dates = len(df)
        for idx, date in enumerate(df['date'].dt.strftime('%Y%m%d')):
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
        conn.close()
        
        df['median_ratio'] = median_data
        
        return df
    
    def get_stock_data(self, ticker, start_date='20200101', end_date='20251117'):
        """
        取得個股的詳細資料
        
        參數:
        - ticker: 股票代號（例如: '2330'）
        - start_date: 開始日期（YYYYMMDD）
        - end_date: 結束日期（YYYYMMDD）
        
        回傳:
        - DataFrame 包含日期、維持率、融資餘額、股價、成交量等資訊
        """
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            s.date,
            s.ticker,
            s.stock_name,
            s.margin_ratio,
            s.margin_balance_shares,
            s.margin_balance_amount,
            s.margin_cost_est,
            s.close_price,
            s.open_price,
            s.volume,
            s.avg_10day_ratio
        FROM strategy_result s
        WHERE s.ticker = ? 
          AND s.date >= ? 
          AND s.date <= ?
          AND s.margin_ratio IS NOT NULL
        ORDER BY s.date
        """
        
        df = pd.read_sql_query(query, conn, params=(ticker, start_date, end_date))
        conn.close()
        
        if df.empty:
            return df
        
        # 轉換日期格式
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        
        # 取得股票名稱（如果有的話）
        if 'stock_name' in df.columns and not df['stock_name'].isna().all():
            stock_name = df['stock_name'].iloc[0]
        else:
            stock_name = ticker
        
        print(f"[Info] 取得 {ticker} {stock_name} 的資料，共 {len(df)} 筆")
        
        return df
    
    def create_interactive_chart(self, df, output_path='interactive_margin_ratio_chart.html'):
        """建立互動式圖表（只顯示平均和中位數維持率）"""
        if df.empty:
            print("[Error] 沒有資料可以繪圖")
            return
        
        # 建立子圖（2個圖表，垂直排列）
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=('台股整體融資維持率統計 (2019/7/1 - 2025/11/17)', 
                          '每日有融資餘額的股票數量'),
            vertical_spacing=0.12,
            row_heights=[0.7, 0.3]
        )
        
        # 第一個圖：維持率統計（只顯示平均和中位數）
        # 平均維持率
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df['avg_ratio'],
                mode='lines',
                name='平均維持率',
                line=dict(color='blue', width=2),
                hovertemplate='日期: %{x}<br>平均維持率: %{y:.2f}%<extra></extra>'
            ),
            row=1, col=1
        )
        
        # 中位數維持率
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df['median_ratio'],
                mode='lines',
                name='中位數維持率',
                line=dict(color='green', width=2, dash='dash'),
                hovertemplate='日期: %{x}<br>中位數維持率: %{y:.2f}%<extra></extra>'
            ),
            row=1, col=1
        )
        
        # 參考線：130%（追繳線）和 120%（斷頭線）
        fig.add_hline(
            y=130, 
            line_dash="dot", 
            line_color="orange",
            annotation_text="維持率 130% (追繳)",
            annotation_position="right",
            row=1, col=1
        )
        
        fig.add_hline(
            y=120, 
            line_dash="dot", 
            line_color="red",
            annotation_text="維持率 120% (斷頭)",
            annotation_position="right",
            row=1, col=1
        )
        
        # 第二個圖：股票數量
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df['stock_count'],
                mode='lines',
                name='有融資餘額的股票數量',
                line=dict(color='darkblue', width=2),
                hovertemplate='日期: %{x}<br>股票數量: %{y:,.0f} 檔<extra></extra>',
                showlegend=False
            ),
            row=2, col=1
        )
        
        # 更新佈局
        fig.update_layout(
            height=900,
            title_text="台股整體融資維持率互動式圖表（排除融資餘額為0的股票）",
            title_x=0.5,
            hovermode='x unified',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        # 更新 X 軸
        fig.update_xaxes(title_text="日期", row=2, col=1)
        
        # 更新 Y 軸
        fig.update_yaxes(title_text="維持率 (%)", row=1, col=1)
        fig.update_yaxes(title_text="股票數量", row=2, col=1)
        
        # 儲存為 HTML
        fig.write_html(output_path)
        print(f"[Info] 互動式圖表已儲存至: {output_path}")
        print(f"[Info] 請用瀏覽器開啟此檔案查看互動圖表")
        
        return fig
    
    def create_stock_chart(self, df, ticker, stock_name, start_date, end_date, output_path=None):
        """
        建立個股的互動式圖表
        
        參數:
        - df: 個股資料 DataFrame
        - ticker: 股票代號
        - stock_name: 股票名稱
        - start_date: 開始日期
        - end_date: 結束日期
        - output_path: 輸出檔案路徑（如果為 None，則自動產生）
        """
        if df.empty:
            print("[Error] 沒有資料可以繪圖")
            return None
        
        if output_path is None:
            output_path = f'interactive_stock_{ticker}_{start_date}_{end_date}.html'
        
        # 建立子圖（4個圖表，垂直排列）
        fig = make_subplots(
            rows=4, cols=1,
            subplot_titles=(
                f'{ticker} {stock_name} - 融資維持率',
                f'{ticker} {stock_name} - 融資餘額（股數）',
                f'{ticker} {stock_name} - 收盤價',
                f'{ticker} {stock_name} - 成交量'
            ),
            vertical_spacing=0.08,
            row_heights=[0.3, 0.25, 0.25, 0.2],
            specs=[[{"secondary_y": False}],
                   [{"secondary_y": False}],
                   [{"secondary_y": False}],
                   [{"secondary_y": False}]]
        )
        
        # 第一個圖：融資維持率
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df['margin_ratio'],
                mode='lines',
                name='融資維持率',
                line=dict(color='rgb(0, 100, 200)', width=2.5),
                hovertemplate='日期: %{x}<br>維持率: %{y:.2f}%<extra></extra>'
            ),
            row=1, col=1
        )
        
        # 如果有10日平均維持率，也畫出來
        if 'avg_10day_ratio' in df.columns and df['avg_10day_ratio'].notna().any():
            fig.add_trace(
                go.Scatter(
                    x=df['date'],
                    y=df['avg_10day_ratio'],
                    mode='lines',
                    name='10日平均維持率',
                    line=dict(color='rgb(255, 140, 0)', width=2, dash='dot'),
                    hovertemplate='日期: %{x}<br>10日平均維持率: %{y:.2f}%<extra></extra>'
                ),
                row=1, col=1
            )
        
        # 參考線：130%（追繳線）和 120%（斷頭線）
        fig.add_hline(
            y=130, 
            line_dash="dot", 
            line_color="rgb(255, 165, 0)",
            line_width=2,
            annotation_text="130% (追繳)",
            annotation_position="right",
            row=1, col=1
        )
        
        fig.add_hline(
            y=120, 
            line_dash="dot", 
            line_color="rgb(220, 20, 60)",
            line_width=2,
            annotation_text="120%",
            annotation_position="right",
            row=1, col=1
        )
        
        # 第二個圖：融資餘額（股數）
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df['margin_balance_shares'],
                mode='lines',
                name='融資餘額（股數）',
                line=dict(color='rgb(0, 150, 0)', width=2.5),
                hovertemplate='日期: %{x}<br>融資餘額: %{y:,.0f} 股<extra></extra>',
                showlegend=False
            ),
            row=2, col=1
        )
        
        # 第三個圖：收盤價
        if 'close_price' in df.columns and df['close_price'].notna().any():
            fig.add_trace(
                go.Scatter(
                    x=df['date'],
                    y=df['close_price'],
                    mode='lines',
                    name='收盤價',
                    line=dict(color='rgb(128, 0, 128)', width=2.5),
                    hovertemplate='日期: %{x}<br>收盤價: %{y:.2f} 元<extra></extra>',
                    showlegend=False
                ),
                row=3, col=1
            )
        
        # 第四個圖：成交量（改用深色，確保在白色背景上清晰可見）
        if 'volume' in df.columns and df['volume'].notna().any():
            fig.add_trace(
                go.Bar(
                    x=df['date'],
                    y=df['volume'],
                    name='成交量',
                    marker_color='rgb(70, 130, 180)',
                    marker_line_color='rgb(50, 100, 150)',
                    marker_line_width=0.5,
                    hovertemplate='日期: %{x}<br>成交量: %{y:,.0f} 股<extra></extra>',
                    showlegend=False
                ),
                row=4, col=1
            )
        
        # 更新佈局
        date_range_str = f"{start_date[:4]}/{start_date[4:6]}/{start_date[6:8]} - {end_date[:4]}/{end_date[4:6]}/{end_date[6:8]}"
        fig.update_layout(
            height=1200,
            title_text=f"{ticker} {stock_name} - 融資維持率與相關數據 ({date_range_str})",
            title_x=0.5,
            hovermode='x unified',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            plot_bgcolor='white',
            paper_bgcolor='white'
        )
        
        # 更新 X 軸
        fig.update_xaxes(title_text="日期", row=4, col=1)
        
        # 更新 Y 軸
        fig.update_yaxes(title_text="維持率 (%)", row=1, col=1)
        fig.update_yaxes(title_text="融資餘額（股數）", row=2, col=1)
        fig.update_yaxes(title_text="收盤價（元）", row=3, col=1)
        fig.update_yaxes(title_text="成交量（股）", row=4, col=1)
        
        # 儲存為 HTML
        fig.write_html(output_path)
        print(f"[Info] 個股互動式圖表已儲存至: {output_path}")
        print(f"[Info] 請用瀏覽器開啟此檔案查看互動圖表")
        
        return fig
    
    def create_stock_comparison_chart(self, ticker_list, start_date='20200101', end_date='20251117'):
        """建立多檔股票比較的互動圖表"""
        conn = sqlite3.connect(self.db_path)
        
        fig = go.Figure()
        
        for ticker in ticker_list:
            query = """
            SELECT 
                date,
                margin_ratio,
                stock_name
            FROM strategy_result
            WHERE ticker = ? 
              AND date >= ? 
              AND date <= ?
              AND margin_ratio IS NOT NULL
              AND margin_ratio > 0
              AND margin_balance_shares > 0
            ORDER BY date
            """
            
            df = pd.read_sql_query(query, conn, params=(ticker, start_date, end_date))
            
            if not df.empty:
                df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
                stock_name = df['stock_name'].iloc[0] if 'stock_name' in df.columns else ticker
                
                fig.add_trace(
                    go.Scatter(
                        x=df['date'],
                        y=df['margin_ratio'],
                        mode='lines',
                        name=f"{ticker} {stock_name}",
                        hovertemplate=f'{ticker} {stock_name}<br>日期: %{{x}}<br>維持率: %{{y:.2f}}%<extra></extra>'
                    )
                )
        
        conn.close()
        
        fig.update_layout(
            title="多檔股票維持率比較",
            xaxis_title="日期",
            yaxis_title="維持率 (%)",
            hovermode='x unified',
            height=600
        )
        
        output_path = 'interactive_stock_comparison.html'
        fig.write_html(output_path)
        print(f"[Info] 股票比較圖表已儲存至: {output_path}")
        
        return fig


def get_user_input():
    """
    取得用戶輸入（選擇大盤或個股）
    
    回傳:
    - ('market', None, None, None, None) 或 ('stock', ticker, stock_name, start_date, end_date)
    """
    print("=" * 80)
    print("台股融資維持率互動式圖表產生器")
    print("=" * 80)
    print("\n請選擇要產生的圖表類型：")
    print("1. 大盤整體融資維持率統計")
    print("2. 個股融資維持率與相關數據")
    
    while True:
        choice = input("\n請輸入選項 (1 或 2): ").strip()
        
        if choice == '1':
            return ('market', None, None, None, None)
        elif choice == '2':
            # 取得股票代號
            while True:
                ticker = input("\n請輸入股票代號（例如: 2330）: ").strip()
                if ticker and len(ticker) == 4 and ticker.isdigit():
                    break
                else:
                    print("  [Warning] 股票代號格式錯誤，應為4位數字（例如: 2330）")
            
            # 取得日期範圍
            while True:
                start_date = input("請輸入開始日期（格式: YYYYMMDD，例如: 20200101）: ").strip()
                if len(start_date) == 8 and start_date.isdigit():
                    year = int(start_date[:4])
                    month = int(start_date[4:6])
                    day = int(start_date[6:8])
                    if 2000 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                        break
                    else:
                        print("  [Warning] 日期不合理，請重新輸入")
                else:
                    print("  [Warning] 日期格式錯誤，應為 YYYYMMDD（例如: 20200101）")
            
            while True:
                end_date = input("請輸入結束日期（格式: YYYYMMDD，例如: 20251117）: ").strip()
                if len(end_date) == 8 and end_date.isdigit():
                    year = int(end_date[:4])
                    month = int(end_date[4:6])
                    day = int(end_date[6:8])
                    if 2000 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                        if end_date >= start_date:
                            break
                        else:
                            print("  [Warning] 結束日期必須大於或等於開始日期")
                    else:
                        print("  [Warning] 日期不合理，請重新輸入")
                else:
                    print("  [Warning] 日期格式錯誤，應為 YYYYMMDD（例如: 20251117）")
            
            # 從資料庫取得股票名稱
            conn = sqlite3.connect('taiwan_stock.db')
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT stock_name 
                FROM strategy_result 
                WHERE ticker = ? 
                LIMIT 1
            """, (ticker,))
            result = cursor.fetchone()
            stock_name = result[0] if result else ticker
            conn.close()
            
            return ('stock', ticker, stock_name, start_date, end_date)
        else:
            print("  [Warning] 請輸入 1 或 2")


def main():
    """主程式"""
    generator = InteractiveChartGenerator()
    
    # 取得用戶選擇
    chart_type, ticker, stock_name, start_date, end_date = get_user_input()
    
    if chart_type == 'market':
        # 大盤整體統計
        print("\n[1/2] 取得每日統計資料...")
        daily_stats = generator.get_daily_statistics(
            start_date='20200101',
            end_date='20251117'
        )
        
        if not daily_stats.empty:
            # 建立互動式圖表
            print("\n[2/2] 產生互動式圖表...")
            generator.create_interactive_chart(daily_stats, 'interactive_margin_ratio_chart.html')
            
            print("\n" + "=" * 80)
            print("圖表產生完成！")
            print("=" * 80)
            print("\n使用說明：")
            print("1. 用瀏覽器開啟 interactive_margin_ratio_chart.html")
            print("2. 可以透過滑鼠進行以下操作：")
            print("   - 縮放：滾輪或拖曳選取區域")
            print("   - 平移：按住滑鼠左鍵拖曳")
            print("   - 查看資料：將滑鼠移到圖表上")
            print("   - 切換顯示：點擊圖例中的項目")
            print("   - 下載圖片：點擊右上角的相機圖示")
            print("   - 重置縮放：雙擊圖表")
        else:
            print("[Error] 沒有資料可以繪圖")
    
    elif chart_type == 'stock':
        # 個股統計
        print(f"\n[1/2] 取得 {ticker} {stock_name} 的資料...")
        stock_data = generator.get_stock_data(ticker, start_date, end_date)
        
        if not stock_data.empty:
            # 建立個股互動式圖表
            print("\n[2/2] 產生個股互動式圖表...")
            generator.create_stock_chart(
                stock_data, 
                ticker, 
                stock_name, 
                start_date, 
                end_date,
                output_path=f'interactive_stock_{ticker}_{start_date}_{end_date}.html'
            )
            
            print("\n" + "=" * 80)
            print("個股圖表產生完成！")
            print("=" * 80)
            print(f"\n檔案名稱: interactive_stock_{ticker}_{start_date}_{end_date}.html")
            print("\n使用說明：")
            print("1. 用瀏覽器開啟上述 HTML 檔案")
            print("2. 圖表包含以下資訊：")
            print("   - 融資維持率（含10日平均）")
            print("   - 融資餘額（股數）")
            print("   - 收盤價")
            print("   - 成交量")
            print("3. 可以透過滑鼠進行以下操作：")
            print("   - 縮放：滾輪或拖曳選取區域")
            print("   - 平移：按住滑鼠左鍵拖曳")
            print("   - 查看資料：將滑鼠移到圖表上")
            print("   - 切換顯示：點擊圖例中的項目")
            print("   - 下載圖片：點擊右上角的相機圖示")
            print("   - 重置縮放：雙擊圖表")
        else:
            print(f"[Error] 沒有找到 {ticker} 在 {start_date} 到 {end_date} 之間的資料")


if __name__ == '__main__':
    main()