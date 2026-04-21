-------------------------------------------------------------------------------
-- 15-bulk-export.sql
-- 對應教學：第 18 章 — 大量資料匯出完全指南
-- 用途：COPY TO、可寫外部表、gpbackup 語法參考
-- 執行方式：psql -d cloudberry_poc -f 15-bulk-export.sql
-------------------------------------------------------------------------------

\echo '============================================'
\echo '  第 18 章：大量資料匯出完全指南'
\echo '============================================'

-- =============================================
-- 18.1 COPY TO 基本匯出
-- =============================================
\echo ''
\echo '>>> 18.1 COPY TO 匯出'

-- 完整匯出
COPY sales_fact TO '/tmp/export_all.csv' WITH (FORMAT CSV, HEADER);
\echo '  已匯出: /tmp/export_all.csv'

-- 帶過濾條件匯出
COPY (
    SELECT sale_id, sale_date, amount
    FROM sales_fact
    WHERE sale_date = '2024-03-15'
      AND amount > 0
)
TO '/tmp/export_filtered.csv'
WITH (
    FORMAT    CSV,
    HEADER    TRUE,
    DELIMITER ',',
    NULL      '',
    ENCODING  'UTF8'
);
\echo '  已匯出: /tmp/export_filtered.csv（過濾條件匯出）'

-- 帶 JOIN 匯出
COPY (
    SELECT s.sale_id, s.sale_date,
           r.region_name,
           s.amount
    FROM sales_fact s
    JOIN dim_region r ON s.region_id = r.region_id
    WHERE s.sale_date = '2024-03-15'
)
TO '/tmp/export_with_join.csv'
WITH (FORMAT CSV, HEADER, DELIMITER '|');
\echo '  已匯出: /tmp/export_with_join.csv（Pipe 分隔）'

-- =============================================
-- 18.2 驗證匯出結果
-- =============================================
\echo ''
\echo '>>> 18.2 驗證匯出結果'

-- 用 COPY FROM 重新匯入驗證
DROP TABLE IF EXISTS export_verify;
CREATE TABLE export_verify (
    sale_id BIGINT,
    sale_date DATE,
    amount DECIMAL(15,2)
) DISTRIBUTED BY (sale_id);

COPY export_verify FROM '/tmp/export_filtered.csv' WITH (FORMAT CSV, HEADER);

\echo '  匯出驗證：'
SELECT
    (SELECT COUNT(*) FROM sales_fact WHERE sale_date = '2024-03-15' AND amount > 0) AS original_count,
    (SELECT COUNT(*) FROM export_verify) AS exported_count,
    CASE
        WHEN (SELECT COUNT(*) FROM sales_fact WHERE sale_date = '2024-03-15' AND amount > 0)
           = (SELECT COUNT(*) FROM export_verify)
        THEN '✓ 筆數一致'
        ELSE '✗ 筆數不一致'
    END AS verification;

DROP TABLE IF EXISTS export_verify;

-- =============================================
-- 18.3 可寫外部表（並行匯出）語法參考
-- =============================================
\echo ''
\echo '>>> 18.3 可寫外部表（Writable External Table）— 並行匯出'
\echo ''
\echo '  架構示意：'
\echo '  Segment 0 ──→ 寫出 /export/part0_*.csv'
\echo '  Segment 1 ──→ 寫出 /export/part1_*.csv'
\echo '  Segment 2 ──→ 寫出 /export/part2_*.csv'
\echo ''
\echo '  --- 啟動 gpfdist（在目標主機上）---'
\echo '  gpfdist -d /data/export -p 8081 &'
\echo ''
\echo '  --- 建立可寫外部表 ---'
\echo '  CREATE WRITABLE EXTERNAL TABLE ext_export_sales ('
\echo '      sale_id BIGINT, sale_date DATE, amount DECIMAL(15,2)'
\echo '  )'
\echo '  LOCATION (''gpfdist://export-server:8081/sales_%t.csv'')'
\echo '  FORMAT ''CSV'' (HEADER DELIMITER '','')'
\echo '  DISTRIBUTED BY (sale_id);'
\echo ''
\echo '  --- 並行匯出 ---'
\echo '  INSERT INTO ext_export_sales'
\echo '  SELECT sale_id, sale_date, amount FROM sales_fact;'

-- =============================================
-- 18.4 匯出到 S3 語法參考
-- =============================================
\echo ''
\echo '>>> 18.4 匯出到 S3（語法參考）'
\echo ''
\echo '  CREATE WRITABLE EXTERNAL TABLE ext_s3_export (...)'
\echo '  LOCATION ('
\echo '      ''s3://my-bucket/export/sales/'
\echo '       config=/etc/cloudberry/s3/s3.conf'''
\echo '  )'
\echo '  FORMAT ''CSV'' (HEADER)'
\echo '  DISTRIBUTED RANDOMLY;'

-- =============================================
-- 18.5 gpbackup / gprestore 語法參考
-- =============================================
\echo ''
\echo '>>> 18.5 gpbackup / gprestore（語法參考）'
\echo ''
\echo '  --- 完整備份 ---'
\echo '  gpbackup --dbname cloudberry_poc --backup-dir /backup --jobs 4 --with-stats'
\echo ''
\echo '  --- 只備份 Schema ---'
\echo '  gpbackup --dbname cloudberry_poc --backup-dir /backup --metadata-only'
\echo ''
\echo '  --- 只備份指定表格 ---'
\echo '  gpbackup --dbname cloudberry_poc --backup-dir /backup \'
\echo '           --include-table public.sales_fact'
\echo ''
\echo '  --- 還原 ---'
\echo '  gprestore --backup-dir /backup --timestamp 20240315120000 --create-db --jobs 4'
\echo ''
\echo '  --- 還原指定表格 ---'
\echo '  gprestore --backup-dir /backup --timestamp 20240315120000 \'
\echo '            --include-table public.sales_fact'

-- =============================================
-- 18.6 分批匯出腳本參考
-- =============================================
\echo ''
\echo '>>> 18.6 分批匯出（Shell 腳本參考）'
\echo ''
\echo '  #!/bin/bash'
\echo '  for year in $(seq 2020 2024); do'
\echo '    for month in $(seq -w 1 12); do'
\echo '      psql -c "\\copy ('
\echo '        SELECT * FROM sales_fact'
\echo '        WHERE sale_date >= ''${year}-${month}-01'''
\echo '          AND sale_date < ''${year}-${month}-01''::DATE + INTERVAL ''1 month'''
\echo '      ) TO ''/export/sales_${year}${month}.csv'' CSV HEADER"'
\echo '    done'
\echo '  done'

-- =============================================
-- 18.7 效能調優參數
-- =============================================
\echo ''
\echo '>>> 18.7 匯出效能調優'
\echo ''
\echo '  -- 提高記憶體'
\echo '  SET work_mem = ''1GB'';'
\echo ''
\echo '  -- 匯出後更新統計'
\echo '  ANALYZE sales_fact;'

\echo ''
\echo '>>> 大量資料匯出指南完成！'
