# 台股融資維持率量化策略分析系統

## 系統簡介

本系統參考自TEJ台灣經濟新報:<市場恐慌還是機會？從融資維持率看穿轉折訊號>，用於分析台股融資維持率，包含資料取得、計算、視覺化、異常檢測、策略回測等功能。
詳細網址:https://www.tejwin.com/insight/tquant-%E8%9E%8D%E8%B3%87%E7%B6%AD%E6%8C%81%E7%8E%87/

## 策略執行結果與建議

詳細比對後，即重現台灣經濟新報的策略進行方式，但跑出的實際回測數據與台灣經濟新報差距過大，以及策略內容說詞與程式碼矛盾(網站提到:當日成交量 < 過去10日平均量：代表籌碼面已經趨於穩定，融資散戶已出場；但程式碼中實際執行為:當日成交量 > 過去10日平均量)，故此策略先行放棄。


經回溯後，發現報酬率與台灣經濟新報提供的數據差距龐大，報酬率相對低落許多，原因可分為幾點:


1. 受限資料取得(僅使用日K測試)。
2. 該策略內容說詞與程式碼矛盾，無法判斷實際執行方式。
3. 該策略回測數據假設不止盈止損，假設依照網站提出的進出場條件(40%止盈，10%止損)，經五年回測後報酬為負兩成餘。


現世界經濟局勢中,熱錢高度集中少數個股，想'撿便宜'去購買個股恐難以實踐營利，但超出基本面的股價也如履薄冰，恐有慢牛急熊的隱藏危機，故此策略在此時期不適合使用，暫行放棄為宜。

## 快速開始

### 方式一：使用主入口檔案（推薦）

```bash
python main.py
```

這會顯示互動式選單，可以選擇不同的功能模組。

### 方式二：直接執行各功能模組

各模組的詳細說明請參考下方「功能模組說明」。

## 功能模組說明

### 1. 資料取得與計算 (`margin_ratio_calculator.py`)

**功能**：從證交所 API 取得融資融券資料和股價資料，並計算融資維持率。

**主要指令**：
```bash
# 單日更新（更新今日資料）
python margin_ratio_calculator.py

# 批次更新（補抓多天資料，只抓資料不計算）
python margin_ratio_calculator.py --batch 60

# 取得指定日期資料
python margin_ratio_calculator.py --fetch-date 20231222

# 滾動計算（計算融資成本和維持率）
python margin_ratio_calculator.py --rolling 60

# 強制重新計算
python margin_ratio_calculator.py --rolling 60 --force

# 查詢10天平均維持率
python margin_ratio_calculator.py --query-10day

# 查詢策略信號
python margin_ratio_calculator.py --query-10day --strategy

# 產生策略結果表
python margin_ratio_calculator.py --strategy-table
```

**建議流程**：
1. `python margin_ratio_calculator.py --batch 60` - 先取得資料
2. `python margin_ratio_calculator.py --rolling 60` - 再計算維持率
3. `python margin_ratio_calculator.py --strategy-table` - 產生策略表

---

### 2. 互動式圖表產生 (`interactive_chart_generator.py`)

**功能**：產生互動式 HTML 圖表，可查看大盤整體統計或個股詳細資料。

**使用方式**：
```bash
python interactive_chart_generator.py
```

執行後會詢問：
- 選項 1：大盤整體融資維持率統計（平均、中位數、股票數量）
- 選項 2：個股融資維持率與相關數據（需要輸入股票代號和日期範圍）

**輸出檔案**：
- `interactive_margin_ratio_chart.html` - 大盤圖表
- `interactive_stock_{ticker}_{start_date}_{end_date}.html` - 個股圖表

---

### 3. 異常日期檢測 (`find_anomaly_dates.py`)

**功能**：找出平均數與中位數差異過大的異常日期。

**使用方式**：
```bash
# 完整掃描
python find_anomaly_dates.py

# 自訂參數
python find_anomaly_dates.py --threshold 5.0 --diff-threshold 5.0

# 檢查特定日期
python find_anomaly_dates.py --check-dates 20231222 20200922
```

**檢測條件**：
- 平均數與中位數差異百分比 > 閾值（預設 5%）
- 平均數與中位數差異絕對值 > 閾值（預設 5%）
- 極端維持率股票比例過高

---

### 4. 異常日期處理 (`delete_anomaly_dates.py`)

**功能**：刪除異常日期的原始資料（`tw_stock_price_data` 和 `twse_margin_data` 表）。

**使用方式**：
```bash
python delete_anomaly_dates.py
```

執行後會以互動方式詢問要刪除哪些日期，輸入格式：`YYYYMMDD`（例如：`20231222`），可以輸入多個日期，用空白或逗號分隔。

**注意**：只會刪除原始資料表，不會刪除 `strategy_result` 表的資料。

---

### 5. 資料修復 (`fix_anomaly_dates_advanced.py`)

**功能**：修復異常日期的股價資料，使用前後交易日的資料來修正異常值。

**使用方式**：
```bash
python fix_anomaly_dates_advanced.py
```

**建議流程**：
1. 使用 `delete_anomaly_dates.py` 刪除異常日期資料
2. 使用 `margin_ratio_calculator.py --fetch-date` 重新取得資料
3. 使用 `fix_anomaly_dates_advanced.py` 修復資料

---

### 6. 策略結果管理 (`delete_strategy_result.py`)

**功能**：刪除 `strategy_result` 表的所有資料（支援 SQLite 和 MySQL）。

**使用方式**：
```bash
python delete_strategy_result.py
```

**注意**：此操作無法復原，執行前會要求確認。建議在重新計算維持率前執行，清除舊資料。

---

### 7. 資料匯出 (`for_orange.py`)

**功能**：匯出資料供 Orange 機器學習使用，包含特徵工程。

**使用方式**：
```bash
# 匯出所有股票資料
python for_orange.py

# 自訂日期範圍
python for_orange.py --start-date 20200101 --end-date 20251117

# 只匯出特定股票
python for_orange.py --ticker 2330

# 不進行特徵工程
python for_orange.py --no-features

# 指定輸出檔名
python for_orange.py --output my_data.csv
```

**輸出內容**：
- 原始資料欄位
- 特徵工程欄位（前一日數值、變化率、風險等級等）

---

### 8. 策略回測 (`margin_ratio_backtest.py`)

**功能**：執行融資維持率策略回測，產生績效報告和圖表。

**策略條件**：
1. 融資維持率 < 過去10日移動平均值
2. 成交量 < 過去10日平均量
3. 當日為紅K（收盤價 > 開盤價）
4. 融資餘額 > 前5日平均融資餘額 × 0.95

**操作規則**：
- 符合條件後，隔日開盤價買進
- 每次使用 1/10 的現金
- 停利 +40%，停損 -10%
- 持有15個交易日或達停損/停利即出場

**使用方式**：
```bash
# 預設回測（2020-2025）
python margin_ratio_backtest.py

# 自訂日期範圍
python margin_ratio_backtest.py --start-date 20200101 --end-date 20251117

# 自訂初始資金（預設100萬）
python margin_ratio_backtest.py --capital 2000000
```

**輸出結果**：
- 回測報告（總報酬率、勝率、夏普比率、最大回落等）
- 交易記錄 CSV 檔案（`backtest_trades_YYYYMMDD_HHMMSS.csv`）
- 績效圖表 PNG 檔案（`backtest_performance_YYYYMMDD_HHMMSS.png`）

---

## 系統架構

```
taiwan_stock.db (SQLite 資料庫)
├── twse_margin_data          # 證交所融資融券原始資料
├── tw_stock_price_data       # 證交所股價原始資料
└── strategy_result           # 策略結果表（計算後的資料）
    ├── margin_ratio          # 融資維持率
    ├── avg_10day_ratio       # 10日平均維持率
    ├── avg_10day_volume       # 10日平均成交量
    ├── avg_5day_balance_95   # 前5日平均融資餘額×0.95
    └── ... (其他欄位)
```

## 依賴套件

所有依賴套件已列在 `requirements.txt` 中：

```bash
pip install -r requirements.txt
```

主要套件：
- `pandas` - 資料處理
- `numpy` - 數值計算
- `requests` - HTTP 請求
- `matplotlib` - 圖表繪製
- `plotly` - 互動式圖表
- `sqlite3` - 資料庫（Python 內建）
- `pymysql` - MySQL 連接（可選）

## 常見問題

### Q: 如何開始使用？

A: 建議流程：
1. 執行 `python margin_ratio_calculator.py --batch 60` 取得資料
2. 執行 `python margin_ratio_calculator.py --rolling 60` 計算維持率
3. 執行 `python interactive_chart_generator.py` 查看圖表
4. 執行 `python margin_ratio_backtest.py` 進行回測

### Q: 資料庫檔案在哪裡？

A: 預設資料庫檔案為 `taiwan_stock.db`，位於專案根目錄。

### Q: 如何處理異常日期？

A: 
1. 使用 `find_anomaly_dates.py` 找出異常日期
2. 使用 `delete_anomaly_dates.py` 刪除異常日期資料
3. 使用 `margin_ratio_calculator.py --fetch-date` 重新取得資料
4. 使用 `delete_strategy_result.py` 清除舊的計算結果
5. 使用 `margin_ratio_calculator.py --rolling` 重新計算

### Q: 回測結果如何解讀？

A: 回測報告包含：
- **總報酬率**：整體獲利/虧損百分比
- **勝率**：獲利交易佔總交易的比例
- **夏普比率**：風險調整後的報酬率（>1 較好）
- **最大回落**：從最高點的最大跌幅（負值越小越好）

### Q: 如何匯出資料給 Orange 使用？

A: 執行 `python for_orange.py`，會產生 CSV 檔案，然後在 Orange 中開啟即可。

## 授權

本專案僅供學習和研究使用。

## 聯絡方式

如有問題或建議，請透過 GitHub Issues 提出。

