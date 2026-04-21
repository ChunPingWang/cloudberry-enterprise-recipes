-------------------------------------------------------------------------------
-- 14-bulk-import.sql
-- 對應教學：第 17 章 — 大量資料匯入完全指南
-- 用途：實作 COPY、gpfdist 語法、gpload 設定、S3/PXF 語法參考
-- 執行方式：psql -d cloudberry_poc -f 14-bulk-import.sql
-------------------------------------------------------------------------------

\echo '============================================'
\echo '  第 17 章：大量資料匯入完全指南'
\echo '============================================'

-- =============================================
-- 匯入方式選型決策樹
-- =============================================
\echo ''
\echo '>>> 匯入方式選型決策樹'
\echo '  小型（< 1 GB）→ COPY / \\copy'
\echo '  中型（1~100 GB）→ gpfdist 並行'
\echo '  大型（100 GB+）→ gpfdist 多實例 / s3:// / pxf://'
\echo '  即時串流 → Kafka FDW'

-- =============================================
-- 17.1 準備目標表
-- =============================================
\echo ''
\echo '>>> 17.1 準備目標表'

DROP TABLE IF EXISTS import_target CASCADE;
CREATE TABLE import_target (
    sale_id     BIGINT,
    sale_date   DATE,
    product_id  INT,
    customer_id INT,
    amount      DECIMAL(15,2),
    currency    VARCHAR(3) DEFAULT 'USD'
) DISTRIBUTED BY (sale_id);

-- 產生測試 CSV 檔
DROP TABLE IF EXISTS import_source CASCADE;
CREATE TABLE import_source (
    sale_id     BIGINT,
    sale_date   DATE,
    product_id  INT,
    customer_id INT,
    amount      DECIMAL(15,2),
    currency    VARCHAR(3)
) DISTRIBUTED BY (sale_id);

INSERT INTO import_source
SELECT g, '2024-01-01'::DATE + (g % 365),
       (random() * 500)::INT, (random() * 10000)::INT,
       ROUND((random() * 1000)::NUMERIC, 2),
       CASE (g % 3) WHEN 0 THEN 'USD' WHEN 1 THEN 'EUR' ELSE 'TWD' END
FROM generate_series(1, 50000) g;

COPY import_source TO '/tmp/bulk_import_test.csv' WITH (FORMAT CSV, HEADER);
\echo '  已產生測試檔: /tmp/bulk_import_test.csv（50,000 筆）'

-- =============================================
-- 17.2 方式一：COPY FROM（小量資料）
-- =============================================
\echo ''
\echo '>>> 17.2 COPY FROM 匯入'

\timing on

COPY import_target
FROM '/tmp/bulk_import_test.csv'
WITH (
    FORMAT     CSV,
    HEADER     TRUE,
    DELIMITER  ',',
    NULL       '',
    ENCODING   'UTF8'
);

\timing off

SELECT COUNT(*) AS copy_imported_rows FROM import_target;
TRUNCATE import_target;

-- =============================================
-- 17.3 方式二：COPY 完整語法選項
-- =============================================
\echo ''
\echo '>>> 17.3 COPY 完整語法（含 FORCE_NULL）'

COPY import_target (sale_id, sale_date, product_id, customer_id, amount, currency)
FROM '/tmp/bulk_import_test.csv'
WITH (
    FORMAT     CSV,
    HEADER     TRUE,
    DELIMITER  ',',
    NULL       '',
    ENCODING   'UTF8',
    QUOTE      '"',
    ESCAPE     '"',
    FORCE_NULL (amount)    -- 空值強制為 NULL
);

SELECT COUNT(*) AS imported FROM import_target;
\echo '  COPY 完整語法匯入完成。'

TRUNCATE import_target;

-- =============================================
-- 17.4 gpfdist 外部表語法參考
-- =============================================
\echo ''
\echo '>>> 17.4 gpfdist 外部表語法（參考）'
\echo ''
\echo '  --- 啟動 gpfdist ---'
\echo '  gpfdist -d /data/csv_files -p 8081 -l /tmp/gpfdist.log &'
\echo ''
\echo '  --- 單一 gpfdist 實例 ---'
\echo '  CREATE EXTERNAL TABLE ext_sales_csv ('
\echo '      sale_id BIGINT, sale_date DATE, amount DECIMAL(15,2)'
\echo '  )'
\echo '  LOCATION (''gpfdist://etl-server:8081/sales_*.csv'')'
\echo '  FORMAT ''CSV'' (HEADER DELIMITER '','')'
\echo '  ENCODING ''UTF8'''
\echo '  LOG ERRORS'
\echo '  SEGMENT REJECT LIMIT 1000 ROWS;'
\echo ''
\echo '  --- 多 gpfdist 實例（提高吞吐）---'
\echo '  LOCATION ('
\echo '      ''gpfdist://etl-server-1:8081/sales_*.csv'','
\echo '      ''gpfdist://etl-server-2:8082/sales_*.csv'''
\echo '  )'
\echo ''
\echo '  --- 壓縮檔自動解壓 ---'
\echo '  LOCATION (''gpfdist://server:8081/sales_*.csv.gz'')'
\echo '  → gpfdist 自動偵測 .gz .bz2 並解壓'

-- =============================================
-- 17.5 gpload YAML 設定參考
-- =============================================
\echo ''
\echo '>>> 17.5 gpload 自動化批次載入（YAML 設定參考）'
\echo ''
\echo '  # /etc/gpload/sales_load.yaml'
\echo '  VERSION: 1.0.0.1'
\echo '  DATABASE: datawarehouse'
\echo '  USER: etl_user'
\echo '  GPLOAD:'
\echo '    INPUT:'
\echo '      - SOURCE:'
\echo '          PORT: 8081'
\echo '          FILE: [/data/sales/*.csv]'
\echo '        FORMAT: CSV'
\echo '        HEADER: true'
\echo '        ERROR_LIMIT: 2000'
\echo '        LOG_ERRORS: true'
\echo '    OUTPUT:'
\echo '      - TABLE: sales_fact'
\echo '        MODE: INSERT'
\echo ''
\echo '  執行: gpload -f /etc/gpload/sales_load.yaml'

-- =============================================
-- 17.6 S3 匯入語法參考
-- =============================================
\echo ''
\echo '>>> 17.6 S3 匯入語法（參考）'
\echo ''
\echo '  CREATE EXTERNAL TABLE ext_s3_sales (...)'
\echo '  LOCATION ('
\echo '      ''s3://my-bucket/sales/year=2024/*.csv'
\echo '       config=/etc/cloudberry/s3/s3.conf'
\echo '       region=ap-northeast-1'''
\echo '  )'
\echo '  FORMAT ''CSV'' (HEADER);'

-- =============================================
-- 17.7 PXF 匯入語法參考
-- =============================================
\echo ''
\echo '>>> 17.7 PXF 匯入語法（Parquet/ORC/Hive）'
\echo ''
\echo '  -- HDFS Parquet'
\echo '  LOCATION (''pxf://data/warehouse/sales/?PROFILE=hdfs:parquet'')'
\echo ''
\echo '  -- S3 ORC'
\echo '  LOCATION (''pxf://s3a://bucket/events/?PROFILE=s3:orc&SERVER=s3default'')'
\echo ''
\echo '  -- Hive'
\echo '  LOCATION (''pxf://default.customers?PROFILE=hive'')'

-- =============================================
-- 17.8 Web Table 動態資料來源
-- =============================================
\echo ''
\echo '>>> 17.8 外部 Web Table（Command-based）'
\echo ''
\echo '  -- 每個 Segment 各執行一次指令'
\echo '  CREATE EXTERNAL WEB TABLE ext_api_data (...)'
\echo '  EXECUTE ''python3 /scripts/fetch.py --segment=$GP_SEGMENT_ID'''
\echo '  ON ALL SEGMENTS'
\echo '  FORMAT ''CSV'' (HEADER);'
\echo ''
\echo '  -- 只在 Coordinator 執行（API 呼叫）'
\echo '  CREATE EXTERNAL WEB TABLE ext_rates (...)'
\echo '  EXECUTE ''curl -s https://api.example.com | python3 parse.py'''
\echo '  ON COORDINATOR'
\echo '  FORMAT ''CSV'' (HEADER);'

-- 清理
DROP TABLE IF EXISTS import_source CASCADE;

\echo ''
\echo '>>> 大量資料匯入指南完成！'
