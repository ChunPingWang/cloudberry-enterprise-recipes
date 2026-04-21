-------------------------------------------------------------------------------
-- 10-external-tables.sql
-- 對應教學：第 12 章 — 外部資料表與資料載入
-- 用途：示範 COPY、gpfdist 外部表、Web Table 等載入方式
-- 執行方式：psql -d cloudberry_poc -f 10-external-tables.sql
-- 注意：gpfdist 相關示範需在容器內啟動 gpfdist 服務
-------------------------------------------------------------------------------

\echo '============================================'
\echo '  第 12 章：外部資料表與資料載入'
\echo '============================================'

-- =============================================
-- 12.1 準備示範資料（用 COPY TO 產生 CSV）
-- =============================================
\echo ''
\echo '>>> 12.1 準備示範資料'

-- 先確保有 sales_ao 表
DROP TABLE IF EXISTS demo_export_data;
CREATE TABLE demo_export_data (
    sale_id     BIGINT,
    sale_date   DATE,
    product_id  INT,
    customer_id INT,
    amount      DECIMAL(15,2)
) DISTRIBUTED BY (sale_id);

INSERT INTO demo_export_data
SELECT g, '2024-01-01'::DATE + (g % 365),
       (random() * 500)::INT,
       (random() * 10000)::INT,
       ROUND((random() * 1000)::NUMERIC, 2)
FROM generate_series(1, 5000) g;

-- 匯出 CSV（供後續 COPY FROM 示範）
COPY demo_export_data TO '/tmp/demo_sales.csv' WITH (FORMAT CSV, HEADER);
\echo '  已匯出 /tmp/demo_sales.csv（5,000 筆）'

-- =============================================
-- 12.2 COPY FROM 匯入
-- =============================================
\echo ''
\echo '>>> 12.2 COPY FROM 匯入（小量資料專用）'

DROP TABLE IF EXISTS sales_copy_test;
CREATE TABLE sales_copy_test (LIKE demo_export_data) DISTRIBUTED BY (sale_id);

COPY sales_copy_test FROM '/tmp/demo_sales.csv' WITH (FORMAT CSV, HEADER);

SELECT COUNT(*) AS imported_rows FROM sales_copy_test;
\echo '  COPY FROM 匯入完成。'
\echo '  ⚠️ COPY 只走 Coordinator，100 萬行以上建議改用 gpfdist'

-- =============================================
-- 12.3 gpfdist 外部表語法示範（語法參考）
-- =============================================
\echo ''
\echo '>>> 12.3 gpfdist 外部表（語法參考）'
\echo '  gpfdist 是 Cloudberry 原生的分散式 HTTP 檔案伺服器'
\echo '  每個 Segment 直接向 gpfdist 拉取資料 → 真正並行載入'
\echo ''

\echo '--- 啟動 gpfdist（在資料來源主機上執行）---'
\echo '  gpfdist -d /data/csv_files -p 8081 -l /tmp/gpfdist.log &'
\echo ''

-- 以下為語法參考，需啟動 gpfdist 後才能真正執行
\echo '--- 建立外部表語法 ---'
\echo 'CREATE EXTERNAL TABLE ext_sales_csv ('
\echo '    sale_id     BIGINT,'
\echo '    sale_date   DATE,'
\echo '    product_id  INT,'
\echo '    customer_id INT,'
\echo '    amount      DECIMAL(15,2)'
\echo ')'
\echo 'LOCATION (''gpfdist://etl-server:8081/sales_*.csv'')'
\echo 'FORMAT ''CSV'' (HEADER DELIMITER '','')'
\echo 'ENCODING ''UTF8'''
\echo 'LOG ERRORS'
\echo 'SEGMENT REJECT LIMIT 1000 ROWS;'
\echo ''

-- =============================================
-- 12.4 在 Sandbox 中模擬外部表（使用 file:// protocol）
-- =============================================
\echo ''
\echo '>>> 12.4 Sandbox 模擬：用 file:// protocol 讀取本地檔案'

-- 先產生多個小檔案
COPY (SELECT * FROM demo_export_data WHERE sale_id <= 1000)
    TO '/tmp/demo_sales_part1.csv' WITH (FORMAT CSV, HEADER);
COPY (SELECT * FROM demo_export_data WHERE sale_id > 1000 AND sale_id <= 2000)
    TO '/tmp/demo_sales_part2.csv' WITH (FORMAT CSV, HEADER);

\echo '  已產生 /tmp/demo_sales_part1.csv 和 part2.csv'

-- =============================================
-- 12.5 COPY 完整語法選項
-- =============================================
\echo ''
\echo '>>> 12.5 COPY 完整語法示範'

DROP TABLE IF EXISTS sales_copy_options;
CREATE TABLE sales_copy_options (
    sale_id     BIGINT,
    sale_date   DATE,
    product_id  INT,
    customer_id INT,
    amount      DECIMAL(15,2)
) DISTRIBUTED BY (sale_id);

COPY sales_copy_options
FROM '/tmp/demo_sales.csv'
WITH (
    FORMAT     CSV,
    HEADER     TRUE,
    DELIMITER  ',',
    NULL       '',
    ENCODING   'UTF8',
    QUOTE      '"',
    ESCAPE     '"'
);

SELECT COUNT(*) AS rows_loaded FROM sales_copy_options;

-- =============================================
-- 12.6 使用 Pipe 匯入壓縮檔
-- =============================================
\echo ''
\echo '>>> 12.6 壓縮檔匯入（Shell 操作）'
\echo '  gunzip -c /data/sales.csv.gz | psql -c "\\copy sales FROM STDIN CSV HEADER"'
\echo '  → 適合從客戶端本機載入壓縮檔'

-- 清理
DROP TABLE IF EXISTS sales_copy_test;
DROP TABLE IF EXISTS sales_copy_options;

\echo ''
\echo '>>> 外部資料表示範完成！'
\echo '  效能提示：'
\echo '  - 小量資料（< 1GB）→ COPY'
\echo '  - 中大量資料 → gpfdist（並行載入）'
\echo '  - 超大量 → gpfdist 多實例 + gpload'
\echo '  - S3/HDFS → s3:// 或 pxf:// protocol'
