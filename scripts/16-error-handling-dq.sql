-------------------------------------------------------------------------------
-- 16-error-handling-dq.sql
-- 對應教學：第 19 章 — 錯誤處理與資料品質管控
-- 用途：錯誤隔離模式、兩段式載入、資料品質檢查 SP
-- 執行方式：psql -d cloudberry_poc -f 16-error-handling-dq.sql
-------------------------------------------------------------------------------

\echo '============================================'
\echo '  第 19 章：錯誤處理與資料品質管控'
\echo '============================================'

-- =============================================
-- 19.1 錯誤隔離模式語法參考
-- =============================================
\echo ''
\echo '>>> 19.1 錯誤隔離模式'
\echo ''
\echo '  模式一：固定行數限制'
\echo '  SEGMENT REJECT LIMIT 500 ROWS;'
\echo ''
\echo '  模式二：百分比限制'
\echo '  SEGMENT REJECT LIMIT 2 PERCENT;'
\echo ''
\echo '  模式三：持久化錯誤日誌'
\echo '  LOG ERRORS PERSISTENTLY'
\echo '  SEGMENT REJECT LIMIT 1000 ROWS;'
\echo ''
\echo '  查看錯誤：'
\echo '  SELECT * FROM gp_read_error_log(''外部表名'');'
\echo '  清除錯誤：'
\echo '  SELECT gp_truncate_error_log(''外部表名'');'

-- =============================================
-- 19.2 兩段式載入（Two-Phase Loading）模擬
-- =============================================
\echo ''
\echo '>>> 19.2 兩段式載入模擬'

-- 建立包含「髒資料」的暫存表（模擬外部資料）
DROP TABLE IF EXISTS raw_import_data CASCADE;
CREATE TABLE raw_import_data (
    sale_id_raw     TEXT,
    sale_date_raw   TEXT,
    product_id_raw  TEXT,
    amount_raw      TEXT,
    currency_raw    TEXT
) DISTRIBUTED RANDOMLY;

-- 插入正常資料
INSERT INTO raw_import_data VALUES
    ('1', '2024-03-15', '100', '250.50', 'USD'),
    ('2', '2024-03-15', '200', '130.00', 'EUR'),
    ('3', '2024-03-15', '300', '999.99', 'TWD'),
    ('4', '03/15/2024', '400', '50.25', 'USD'),   -- MM/DD/YYYY 格式
    ('5', '20240315', '500', '75.00', 'EUR');       -- YYYYMMDD 格式

-- 插入有問題的資料
INSERT INTO raw_import_data VALUES
    ('abc', '2024-03-15', '100', '250.50', 'USD'),  -- sale_id 非數字
    ('6', 'not-a-date', '100', '250.50', 'USD'),     -- 日期格式錯誤
    ('7', '2024-03-15', 'xyz', '250.50', 'USD'),     -- product_id 非數字
    ('8', '2024-03-15', '100', '$1,250.50', 'USD'),  -- 金額含符號
    ('9', '2024-03-15', '100', '-50.00', 'USD'),     -- 負數金額
    (NULL, '2024-03-15', '100', '100.00', 'USD'),    -- NULL sale_id
    ('10', '2024-03-15', '100', '', '');              -- 空值

\echo '  已建立含正常+髒資料的暫存表（12 筆，含 7 筆問題資料）'

-- Phase 2：驗證 + 轉換
DROP TABLE IF EXISTS clean_import_target CASCADE;
CREATE TABLE clean_import_target (
    sale_id     BIGINT,
    sale_date   DATE,
    product_id  INT,
    amount      DECIMAL(15,2),
    currency    VARCHAR(3)
) DISTRIBUTED BY (sale_id);

\echo ''
\echo '  Phase 2：驗證 + 轉換插入'

INSERT INTO clean_import_target (sale_id, sale_date, product_id, amount, currency)
SELECT
    sale_id_raw::BIGINT,
    -- 處理多種日期格式
    CASE
        WHEN sale_date_raw ~ '^\d{4}-\d{2}-\d{2}$'
            THEN sale_date_raw::DATE
        WHEN sale_date_raw ~ '^\d{2}/\d{2}/\d{4}$'
            THEN TO_DATE(sale_date_raw, 'MM/DD/YYYY')
        WHEN sale_date_raw ~ '^\d{8}$'
            THEN TO_DATE(sale_date_raw, 'YYYYMMDD')
        ELSE NULL
    END,
    product_id_raw::INT,
    -- 清理金額
    CASE
        WHEN REGEXP_REPLACE(amount_raw, '[,$¥€£]', '', 'g') ~ '^\-?\d+\.?\d*$'
            THEN REGEXP_REPLACE(amount_raw, '[,$¥€£]', '', 'g')::DECIMAL(15,2)
        ELSE NULL
    END,
    COALESCE(NULLIF(currency_raw, ''), 'USD')
FROM raw_import_data
WHERE sale_id_raw  ~ '^\d+$'           -- sale_id 必須是數字
  AND product_id_raw ~ '^\d+$'         -- product_id 必須是數字
  AND sale_date_raw IS NOT NULL;

SELECT COUNT(*) AS clean_rows FROM clean_import_target;

\echo ''
\echo '  載入成功的資料：'
SELECT * FROM clean_import_target ORDER BY sale_id;

-- 被過濾掉的資料
\echo ''
\echo '  被過濾掉的問題資料：'
SELECT
    sale_id_raw,
    sale_date_raw,
    product_id_raw,
    amount_raw,
    CASE
        WHEN sale_id_raw IS NULL OR sale_id_raw !~ '^\d+$' THEN 'sale_id 非數字'
        WHEN product_id_raw !~ '^\d+$' THEN 'product_id 非數字'
        WHEN sale_date_raw IS NULL THEN '日期為空'
        ELSE '其他'
    END AS rejection_reason
FROM raw_import_data
WHERE sale_id_raw IS NULL
   OR sale_id_raw !~ '^\d+$'
   OR product_id_raw !~ '^\d+$'
   OR sale_date_raw IS NULL;

-- =============================================
-- 19.3 資料品質監控 Stored Procedure
-- =============================================
\echo ''
\echo '>>> 19.3 資料品質監控 SP'

DROP TABLE IF EXISTS dq_check_results CASCADE;
CREATE TABLE dq_check_results (
    check_id     SERIAL,
    check_time   TIMESTAMP DEFAULT NOW(),
    table_name   TEXT,
    check_name   TEXT,
    total_rows   BIGINT,
    failed_rows  BIGINT,
    fail_pct     DECIMAL(6,3),
    status       TEXT,
    detail       TEXT
) DISTRIBUTED BY (check_id);

CREATE OR REPLACE PROCEDURE sp_run_dq_checks(p_table_name TEXT)
LANGUAGE plpgsql AS $$
DECLARE
    v_total     BIGINT;
    v_failed    BIGINT;
    v_pct       DECIMAL(6,3);
BEGIN
    EXECUTE format('SELECT COUNT(*) FROM %I', p_table_name) INTO v_total;

    -- 檢查 1：NULL 主鍵
    EXECUTE format(
        'SELECT COUNT(*) FROM %I WHERE sale_id IS NULL', p_table_name
    ) INTO v_failed;
    v_pct := ROUND(v_failed * 100.0 / NULLIF(v_total, 0), 3);
    INSERT INTO dq_check_results (table_name, check_name, total_rows, failed_rows, fail_pct, status)
    VALUES (p_table_name, 'NULL_PRIMARY_KEY', v_total, v_failed, v_pct,
            CASE WHEN v_failed = 0 THEN 'PASS' ELSE 'FAIL' END);

    -- 檢查 2：負數金額
    EXECUTE format(
        'SELECT COUNT(*) FROM %I WHERE amount < 0', p_table_name
    ) INTO v_failed;
    v_pct := ROUND(v_failed * 100.0 / NULLIF(v_total, 0), 3);
    INSERT INTO dq_check_results (table_name, check_name, total_rows, failed_rows, fail_pct, status)
    VALUES (p_table_name, 'NEGATIVE_AMOUNT', v_total, v_failed, v_pct,
            CASE WHEN v_pct <= 0.01 THEN 'PASS'
                 WHEN v_pct <= 1.0  THEN 'WARN'
                 ELSE 'FAIL' END);

    -- 檢查 3：未來日期
    EXECUTE format(
        'SELECT COUNT(*) FROM %I WHERE sale_date > CURRENT_DATE', p_table_name
    ) INTO v_failed;
    INSERT INTO dq_check_results (table_name, check_name, total_rows, failed_rows, fail_pct, status)
    VALUES (p_table_name, 'FUTURE_DATE', v_total, v_failed,
            ROUND(v_failed * 100.0 / NULLIF(v_total, 0), 3),
            CASE WHEN v_failed = 0 THEN 'PASS' ELSE 'WARN' END);

    -- 檢查 4：重複主鍵
    EXECUTE format(
        'SELECT COUNT(*) FROM (
            SELECT sale_id FROM %I GROUP BY sale_id HAVING COUNT(*) > 1
        ) dup', p_table_name
    ) INTO v_failed;
    INSERT INTO dq_check_results (table_name, check_name, total_rows, failed_rows, fail_pct, status)
    VALUES (p_table_name, 'DUPLICATE_KEY', v_total, v_failed,
            ROUND(v_failed * 100.0 / NULLIF(v_total, 0), 3),
            CASE WHEN v_failed = 0 THEN 'PASS' ELSE 'FAIL' END);

    -- 檢查 5：NULL 金額
    EXECUTE format(
        'SELECT COUNT(*) FROM %I WHERE amount IS NULL', p_table_name
    ) INTO v_failed;
    INSERT INTO dq_check_results (table_name, check_name, total_rows, failed_rows, fail_pct, status)
    VALUES (p_table_name, 'NULL_AMOUNT', v_total, v_failed,
            ROUND(v_failed * 100.0 / NULLIF(v_total, 0), 3),
            CASE WHEN v_pct <= 1.0 THEN 'PASS' ELSE 'WARN' END);

    RAISE NOTICE '資料品質檢查完成 - %：共 % 筆', p_table_name, v_total;
END;
$$;

-- 對 sales_fact 執行品質檢查
CALL sp_run_dq_checks('sales_fact');

\echo ''
\echo '  資料品質檢查結果（sales_fact）：'
SELECT check_name, total_rows, failed_rows, fail_pct, status
FROM dq_check_results
WHERE table_name = 'sales_fact'
ORDER BY check_id;

-- 對 clean_import_target 執行品質檢查
CALL sp_run_dq_checks('clean_import_target');

\echo ''
\echo '  資料品質檢查結果（clean_import_target）：'
SELECT check_name, total_rows, failed_rows, fail_pct, status
FROM dq_check_results
WHERE table_name = 'clean_import_target'
ORDER BY check_id;

-- 清理
DROP TABLE IF EXISTS raw_import_data CASCADE;
DROP TABLE IF EXISTS clean_import_target CASCADE;

\echo ''
\echo '>>> 錯誤處理與資料品質管控完成！'
\echo '  關鍵要點：'
\echo '  1. 外部表使用 LOG ERRORS + SEGMENT REJECT LIMIT 隔離錯誤'
\echo '  2. 複雜資料使用兩段式載入（全 TEXT → 驗證轉換）'
\echo '  3. 建立 DQ 檢查 SP 定期監控資料品質'
