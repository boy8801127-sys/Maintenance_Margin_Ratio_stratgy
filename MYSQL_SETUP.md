# MySQL 雙寫模式設定說明

## 功能說明

本程式支援同時寫入 SQLite 和 MySQL 資料庫（雙寫模式），讓你可以：
- 在開發階段使用 SQLite（輕量、快速）
- 在生產環境使用 MySQL（功能完整、支援多使用者）
- 同時使用兩者，確保資料備份

## 安裝需求

確保已安裝 `pymysql` 套件：

```bash
pip install pymysql
```

或使用 requirements.txt：

```bash
pip install -r requirements.txt
```

## MySQL 資料庫準備

### 1. 建立資料庫

在 MySQL 中建立資料庫（可選，程式會自動建立表格）：

```sql
CREATE DATABASE taiwan_stock CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

### 2. 建立使用者（可選）

```sql
CREATE USER 'stock_user'@'localhost' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON taiwan_stock.* TO 'stock_user'@'localhost';
FLUSH PRIVILEGES;
```

## 使用方式

### 方式 1: 只使用 SQLite（預設）

```python
from margin_ratio_calculator import MarginRatioCalculator

calculator = MarginRatioCalculator()
calculator.run_daily_update()
```

### 方式 2: 同時使用 SQLite 和 MySQL（雙寫模式）

```python
from margin_ratio_calculator import MarginRatioCalculator

# 設定 MySQL 連接資訊
mysql_config = {
    'host': 'localhost',      # MySQL 主機位置
    'port': 3306,             # MySQL 連接埠（預設 3306）
    'user': 'root',           # MySQL 使用者名稱
    'password': 'your_password',  # MySQL 密碼
    'database': 'taiwan_stock'    # 資料庫名稱
}

# 建立計算器實例（啟用 MySQL）
calculator = MarginRatioCalculator(mysql_config=mysql_config)

# 執行每日更新（會同時寫入 SQLite 和 MySQL）
calculator.run_daily_update()
```

## 錯誤處理

程式設計為**錯誤隔離**模式：
- 如果 MySQL 連接失敗，**不會影響 SQLite 的運作**
- SQLite 資料會正常儲存
- 只會顯示警告訊息，程式繼續執行

## 資料庫表格結構

MySQL 表格會自動建立，結構如下：

```sql
CREATE TABLE margin_data (
    date VARCHAR(8) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    stock_name VARCHAR(100),
    closing_price DECIMAL(10, 2),
    margin_balance_amount DECIMAL(15, 2),
    margin_balance_shares INT,
    margin_prev_balance INT,
    margin_buy_shares INT,
    margin_sell_shares INT,
    margin_cash_repay_shares INT,
    margin_cost_est DECIMAL(10, 4),
    margin_ratio DECIMAL(10, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, ticker)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

## 查詢資料

### 使用 MySQL Workbench 查詢

```sql
-- 查看最新一日的資料
SELECT * FROM margin_data 
WHERE date = (SELECT MAX(date) FROM margin_data)
ORDER BY margin_ratio ASC
LIMIT 20;

-- 查看特定股票的歷史資料
SELECT date, ticker, stock_name, closing_price, 
       margin_balance_shares, margin_cost_est, margin_ratio
FROM margin_data
WHERE ticker = '2330'
ORDER BY date DESC;

-- 查看維持率最低的前10檔（最新一日）
SELECT ticker, stock_name, closing_price, margin_ratio
FROM margin_data
WHERE date = (SELECT MAX(date) FROM margin_data)
  AND margin_ratio IS NOT NULL
ORDER BY margin_ratio ASC
LIMIT 10;
```

## 注意事項

1. **首次執行**：程式會自動建立 MySQL 表格，不需要手動建立
2. **資料同步**：每次執行 `run_daily_update()` 時，會同時更新 SQLite 和 MySQL
3. **重複執行**：如果同一天執行多次，會先刪除當日資料再插入新資料（避免重複）
4. **編碼設定**：MySQL 表格使用 `utf8mb4` 編碼，支援完整的中文字元

## 疑難排解

### 問題 1: 連接失敗

**錯誤訊息**：`MySQL 初始化失敗: ...`

**解決方法**：
- 確認 MySQL 服務是否啟動
- 檢查連接資訊（host、port、user、password）是否正確
- 確認資料庫是否存在（或讓程式自動建立）

### 問題 2: 權限不足

**錯誤訊息**：`Access denied for user ...`

**解決方法**：
- 確認使用者有建立表格的權限
- 確認使用者有對指定資料庫的讀寫權限

### 問題 3: 編碼問題

**錯誤訊息**：中文顯示為亂碼

**解決方法**：
- 確認 MySQL 資料庫使用 `utf8mb4` 編碼
- 確認 MySQL Workbench 連接時使用 UTF-8 編碼

