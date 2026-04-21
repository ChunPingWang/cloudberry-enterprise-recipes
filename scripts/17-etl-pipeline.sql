-------------------------------------------------------------------------------
-- 17-etl-pipeline.sql
-- 對應教學：第 20 章 — 企業級 ETL 匯入匯出 SP 整合範例
-- 用途：完整 ETL Pipeline SP、Pipeline 設定管理、增量載入
-- 執行方式：psql -d cloudberry_poc -f 17-etl-pipeline.sql
-------------------------------------------------------------------------------

\echo '============================================'
\echo '  第 20 章：企業級 ETL Pipeline'
\echo '============================================'

-- =============================================
-- 20.1 ETL 基礎設施
-- =============================================
\echo ''
\echo '>>> 20.1 建立 ETL 基礎設施'

-- 批次日誌表
DROP TABLE IF EXISTS etl_batch_log CASCADE;
CREATE TABLE etl_batch_log (
    batch_id      SERIAL,
    batch_date    DATE,
    status        VARCHAR(20),
    started_at    TIMESTAMP,
    finished_at   TIMESTAMP,
    inserted_rows BIGINT DEFAULT 0,
    updated_rows  BIGINT DEFAULT 0,
    rejected_rows BIGINT DEFAULT 0,
    duration_sec  DECIMAL(10,2),
    error_msg     TEXT
) DISTRIBUTED BY (batch_id);

-- 清洗暫存表
DROP TABLE IF EXISTS sales_cleansed CASCADE;
CREATE TABLE sales_cleansed (
    sale_id     BIGINT,
    sale_date   DATE,
    product_id  INT,
    customer_id INT,
    amount      DECIMAL(15,2),
    currency    VARCHAR(3),
    batch_id    BIGINT
) DISTRIBUTED BY (sale_id);

-- 每日彙總表
DROP TABLE IF EXISTS sales_daily_summary CASCADE;
CREATE TABLE sales_daily_summary (
    sale_date       DATE PRIMARY KEY,
    total_amount    DECIMAL(20,2),
    total_orders    BIGINT,
    last_updated    TIMESTAMP
) DISTRIBUTED BY (sale_date);

-- Pipeline 設定表
DROP TABLE IF EXISTS etl_pipeline_config CASCADE;
CREATE TABLE etl_pipeline_config (
    pipeline_id     SERIAL,
    pipeline_name   TEXT,
    source_table    TEXT,
    target_table    TEXT,
    schedule_expr   TEXT,
    is_active       BOOLEAN DEFAULT TRUE,
    last_run_at     TIMESTAMP,
    last_run_status TEXT
) DISTRIBUTED BY (pipeline_id);

\echo '  ETL 基礎設施建立完成。'

-- =============================================
-- 20.2 完整的增量載入 SP
-- =============================================
\echo ''
\echo '>>> 20.2 完整的增量 ETL Stored Procedure'

CREATE OR REPLACE PROCEDURE sp_etl_incremental_load(
    p_batch_date        DATE,
    p_enable_logging    BOOLEAN DEFAULT TRUE
)
LANGUAGE plpgsql AS $$
DECLARE
    v_batch_id      BIGINT;
    v_inserted_rows BIGINT := 0;
    v_updated_rows  BIGINT := 0;
    v_start_ts      TIMESTAMP := clock_timestamp();
BEGIN
    -- Step 1: 建立批次記錄
    IF p_enable_logging THEN
        INSERT INTO etl_batch_log (batch_date, status, started_at)
        VALUES (p_batch_date, 'RUNNING', v_start_ts)
        RETURNING batch_id INTO v_batch_id;
    END IF;

    RAISE NOTICE '[ETL] 批次 % 開始，日期: %', v_batch_id, p_batch_date;

    -- Step 2: 資料品質過濾（Stage → Cleansed）
    DELETE FROM sales_cleansed WHERE batch_id = v_batch_id;

    INSERT INTO sales_cleansed (
        sale_id, sale_date, product_id, customer_id, amount, currency, batch_id
    )
    SELECT
        sale_id, sale_date, product_id, customer_id,
        CASE WHEN amount < 0 THEN NULL ELSE amount END,
        COALESCE(currency, 'USD'),
        v_batch_id
    FROM sales_staging
    WHERE sale_date = p_batch_date
      AND sale_id IS NOT NULL
      AND product_id IS NOT NULL
      AND customer_id IS NOT NULL;

    GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;
    RAISE NOTICE '[ETL] 清洗完成：% 筆通過品質檢查', v_inserted_rows;

    -- Step 3: Upsert 到事實表
    -- 先 UPDATE 已存在的行
    UPDATE sales_fact f
    SET amount     = c.amount,
        updated_at = NOW(),
        batch_id   = v_batch_id::TEXT
    FROM sales_cleansed c
    WHERE f.sale_id  = c.sale_id
      AND c.batch_id = v_batch_id;

    GET DIAGNOSTICS v_updated_rows = ROW_COUNT;

    -- 再 INSERT 新增的行
    INSERT INTO sales_fact (sale_id, sale_date, product_id, customer_id, region_id, amount, batch_id)
    SELECT c.sale_id, c.sale_date, c.product_id, c.customer_id,
           COALESCE(c.customer_id % 5 + 1, 1),  -- 模擬 region_id
           c.amount, v_batch_id::TEXT
    FROM sales_cleansed c
    WHERE c.batch_id = v_batch_id
      AND NOT EXISTS (
          SELECT 1 FROM sales_fact f WHERE f.sale_id = c.sale_id
      );

    -- Step 4: 更新每日彙總
    INSERT INTO sales_daily_summary (sale_date, total_amount, total_orders, last_updated)
    SELECT
        p_batch_date,
        SUM(amount),
        COUNT(*),
        NOW()
    FROM sales_fact
    WHERE sale_date = p_batch_date
    ON CONFLICT (sale_date)
    DO UPDATE SET
        total_amount = EXCLUDED.total_amount,
        total_orders = EXCLUDED.total_orders,
        last_updated = EXCLUDED.last_updated;

    -- Step 5: 清理暫存
    DELETE FROM sales_cleansed WHERE batch_id = v_batch_id;

    -- Step 6: 更新批次紀錄
    IF p_enable_logging THEN
        UPDATE etl_batch_log
        SET status        = 'SUCCESS',
            finished_at   = clock_timestamp(),
            inserted_rows = v_inserted_rows,
            updated_rows  = v_updated_rows,
            duration_sec  = ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - v_start_ts))::NUMERIC, 2)
        WHERE batch_id = v_batch_id;
    END IF;

    RAISE NOTICE '[ETL] 批次 % 完成 | 插入: % | 更新: % | 耗時: %s',
        v_batch_id, v_inserted_rows, v_updated_rows,
        ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - v_start_ts))::NUMERIC, 2);

END;
$$;

-- =============================================
-- 20.3 執行 ETL Pipeline
-- =============================================
\echo ''
\echo '>>> 20.3 執行 ETL Pipeline'

-- 確保暫存表有資料
INSERT INTO sales_staging
SELECT g + 50000, '2024-04-01'::DATE,
       (random() * 500)::INT, (random() * 10000)::INT,
       (random() * 4 + 1)::INT,
       ROUND((random() * 800)::NUMERIC, 2), 'USD'
FROM generate_series(1, 5000) g
WHERE NOT EXISTS (
    SELECT 1 FROM sales_staging s WHERE s.sale_id = g + 50000
);

\timing on
CALL sp_etl_incremental_load('2024-04-01');
\timing off

-- 查看批次日誌
\echo ''
\echo '  批次執行日誌：'
SELECT batch_id, batch_date, status, inserted_rows, updated_rows,
       duration_sec, started_at, finished_at
FROM etl_batch_log
ORDER BY batch_id DESC
LIMIT 5;

-- 查看每日彙總
\echo ''
\echo '  每日銷售彙總：'
SELECT * FROM sales_daily_summary ORDER BY sale_date DESC LIMIT 5;

-- =============================================
-- 20.4 Pipeline 設定管理
-- =============================================
\echo ''
\echo '>>> 20.4 Pipeline 設定管理'

INSERT INTO etl_pipeline_config (pipeline_name, source_table, target_table, schedule_expr)
SELECT 'sales_daily', 'sales_staging', 'sales_fact', '0 1 * * *'
WHERE NOT EXISTS (SELECT 1 FROM etl_pipeline_config WHERE pipeline_name = 'sales_daily');

INSERT INTO etl_pipeline_config (pipeline_name, source_table, target_table, schedule_expr)
SELECT 'orders_daily', 'orders_staging', 'orders_fact', '0 2 * * *'
WHERE NOT EXISTS (SELECT 1 FROM etl_pipeline_config WHERE pipeline_name = 'orders_daily');

\echo '  Pipeline 設定：'
SELECT pipeline_name, source_table, target_table, schedule_expr, is_active
FROM etl_pipeline_config;

-- =============================================
-- 20.5 pg_cron 排程參考
-- =============================================
\echo ''
\echo '>>> 20.5 pg_cron 排程語法（參考）'
\echo ''
\echo '  -- 安裝 pg_cron'
\echo '  CREATE EXTENSION IF NOT EXISTS pg_cron;'
\echo ''
\echo '  -- 每天 01:00 執行銷售 ETL'
\echo '  SELECT cron.schedule(''daily_sales'', ''0 1 * * *'','
\echo '      $$CALL sp_etl_incremental_load(CURRENT_DATE - 1);$$);'
\echo ''
\echo '  -- 查看排程狀態'
\echo '  SELECT * FROM cron.job;'
\echo ''
\echo '  -- 查看執行歷史'
\echo '  SELECT * FROM cron.job_run_details ORDER BY start_time DESC;'

-- =============================================
-- 20.6 匯出報表 SP
-- =============================================
\echo ''
\echo '>>> 20.6 匯出月報表 SP'

CREATE OR REPLACE PROCEDURE sp_export_monthly_summary(
    p_year  INT,
    p_month INT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_file_path TEXT;
    v_row_count BIGINT;
BEGIN
    v_file_path := format('/tmp/monthly_report_%s%s.csv', p_year, LPAD(p_month::TEXT, 2, '0'));

    -- 匯出彙總報表到 CSV
    EXECUTE format(
        'COPY (
            SELECT
                r.region_name,
                COUNT(*) AS order_count,
                SUM(s.amount) AS total_sales,
                ROUND(AVG(s.amount), 2) AS avg_order
            FROM sales_fact s
            JOIN dim_region r ON s.region_id = r.region_id
            WHERE EXTRACT(YEAR FROM s.sale_date) = %s
              AND EXTRACT(MONTH FROM s.sale_date) = %s
            GROUP BY r.region_name
            ORDER BY total_sales DESC
        ) TO %L WITH (FORMAT CSV, HEADER)',
        p_year, p_month, v_file_path
    );

    RAISE NOTICE '月報表已匯出至 %', v_file_path;
END;
$$;

CALL sp_export_monthly_summary(2024, 3);

-- 驗證匯出
\echo '  匯出的月報表內容：'
CREATE TEMP TABLE tmp_report (
    region_name TEXT,
    order_count BIGINT,
    total_sales DECIMAL(15,2),
    avg_order DECIMAL(15,2)
);
COPY tmp_report FROM '/tmp/monthly_report_202403.csv' WITH (FORMAT CSV, HEADER);
SELECT * FROM tmp_report ORDER BY total_sales DESC;
DROP TABLE tmp_report;

-- =============================================
-- 20.7 效能調優參數
-- =============================================
\echo ''
\echo '>>> 20.7 ETL 效能調優提示'
\echo ''
\echo '  -- 大量載入前暫時關閉觸發器'
\echo '  SET session_replication_role = replica;'
\echo ''
\echo '  -- 提高載入記憶體'
\echo '  SET work_mem = ''1GB'';'
\echo ''
\echo '  -- 載入後重建統計'
\echo '  ANALYZE sales_fact;'
\echo ''
\echo '  -- 載入後清理 AO 表'
\echo '  VACUUM sales_fact;'

\echo ''
\echo '>>> 企業級 ETL Pipeline 示範完成！'
\echo '  完整流程：'
\echo '  1. 外部資料 → 暫存表（gpfdist 並行載入）'
\echo '  2. 暫存表 → 清洗表（品質過濾）'
\echo '  3. 清洗表 → 事實表（Upsert）'
\echo '  4. 事實表 → 彙總表（聚合更新）'
\echo '  5. 彙總表 → 匯出報表（COPY TO / 可寫外部表）'
