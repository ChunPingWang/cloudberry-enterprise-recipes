-------------------------------------------------------------------------------
-- 11-mpp-stored-procedures.sql
-- 對應教學：第 13 章 — 分散式 MPP Stored Procedure 撰寫技巧
-- 用途：實作 MPP 友好的 SP、Co-located Join、反模式比較
-- 執行方式：psql -d cloudberry_poc -f 11-mpp-stored-procedures.sql
-------------------------------------------------------------------------------

\echo '============================================'
\echo '  第 13 章：分散式 MPP Stored Procedure'
\echo '============================================'

-- =============================================
-- 準備基礎表
-- =============================================
\echo ''
\echo '>>> 準備基礎表'

-- ETL 載入記錄表
DROP TABLE IF EXISTS etl_load_log CASCADE;
CREATE TABLE etl_load_log (
    log_id      SERIAL,
    load_date   DATE,
    table_name  TEXT,
    row_count   BIGINT,
    start_time  TIMESTAMP,
    end_time    TIMESTAMP,
    status      VARCHAR(20),
    error_msg   TEXT
) DISTRIBUTED BY (log_id);

-- 銷售事實表
DROP TABLE IF EXISTS sales_fact CASCADE;
CREATE TABLE sales_fact (
    sale_id     BIGINT,
    sale_date   DATE,
    product_id  INT,
    customer_id INT,
    region_id   INT,
    amount      DECIMAL(15,2),
    batch_id    TEXT,
    updated_at  TIMESTAMP DEFAULT NOW()
) DISTRIBUTED BY (sale_id);

-- 銷售暫存表（模擬外部資料來源）
DROP TABLE IF EXISTS sales_staging CASCADE;
CREATE TABLE sales_staging (
    sale_id     BIGINT,
    sale_date   DATE,
    product_id  INT,
    customer_id INT,
    region_id   INT,
    amount      DECIMAL(15,2),
    currency    VARCHAR(3) DEFAULT 'USD'
) DISTRIBUTED BY (sale_id);

-- 區域維度表
DROP TABLE IF EXISTS dim_region CASCADE;
CREATE TABLE dim_region (
    region_id   INT,
    region_name VARCHAR(100)
) DISTRIBUTED REPLICATED;

INSERT INTO dim_region VALUES
    (1, 'Asia'), (2, 'Europe'), (3, 'Americas'), (4, 'Africa'), (5, 'Oceania');

-- 插入暫存資料
INSERT INTO sales_staging
SELECT g, '2024-03-15'::DATE,
       (random() * 500)::INT, (random() * 10000)::INT,
       (random() * 4 + 1)::INT,
       ROUND((random() * 1000)::NUMERIC, 2), 'USD'
FROM generate_series(1, 10000) g;

\echo '  基礎表建立完成。'

-- =============================================
-- 13.1 執行模式查詢
-- =============================================
\echo ''
\echo '>>> 13.1 查看函數執行位置'
\echo '  proexeclocation: a=ANY, c=COORDINATOR, s=ALL SEGMENTS'

SELECT proname, provolatile, proexeclocation
FROM pg_proc
WHERE proname IN ('version', 'now', 'random')
ORDER BY proname;

-- =============================================
-- 13.2 基礎 SP：每日銷售載入
-- =============================================
\echo ''
\echo '>>> 13.2 基礎 SP：每日銷售載入'

CREATE OR REPLACE PROCEDURE sp_load_daily_sales(
    p_load_date     DATE,
    p_source_table  TEXT DEFAULT 'sales_staging'
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_row_count     BIGINT;
    v_start_time    TIMESTAMP := clock_timestamp();
    v_end_time      TIMESTAMP;
BEGIN
    RAISE NOTICE '開始載入 % 的銷售資料...', p_load_date;

    -- 刪除當日舊資料（DML → 分發至所有 Segment 並行）
    DELETE FROM sales_fact WHERE sale_date = p_load_date;

    -- 插入新資料（MPP 並行執行）
    INSERT INTO sales_fact (sale_id, sale_date, product_id, customer_id, region_id, amount)
    SELECT sale_id, sale_date, product_id, customer_id, region_id, amount
    FROM sales_staging
    WHERE sale_date = p_load_date;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_end_time := clock_timestamp();

    -- 記錄載入日誌
    INSERT INTO etl_load_log (load_date, table_name, row_count, start_time, end_time, status)
    VALUES (p_load_date, 'sales_fact', v_row_count, v_start_time, v_end_time, 'SUCCESS');

    RAISE NOTICE '載入完成：% 筆，耗時 % 秒',
        v_row_count,
        ROUND(EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC, 3);
END;
$$;

-- 執行
CALL sp_load_daily_sales('2024-03-15');

-- 驗證
SELECT COUNT(*) AS loaded_rows FROM sales_fact WHERE sale_date = '2024-03-15';
SELECT * FROM etl_load_log ORDER BY log_id DESC LIMIT 3;

-- =============================================
-- 13.3 正確 vs 錯誤：SET-BASED 操作
-- =============================================
\echo ''
\echo '>>> 13.3 SET-BASED 操作（正確做法）'

-- 建立彙總表
DROP TABLE IF EXISTS sales_summary CASCADE;
CREATE TABLE sales_summary (
    region_id    INT,
    total_amount DECIMAL(20,2),
    record_count BIGINT,
    updated_at   TIMESTAMP DEFAULT NOW()
) DISTRIBUTED BY (region_id);

-- 初始化
INSERT INTO sales_summary (region_id, total_amount, record_count)
SELECT region_id, 0, 0 FROM dim_region;

-- 正確的 SET-BASED SP
CREATE OR REPLACE PROCEDURE sp_update_summary_correct()
LANGUAGE plpgsql AS $$
BEGIN
    -- 整個 UPDATE 會被分發至所有 Segment 並行執行
    UPDATE sales_summary s
    SET total_amount = agg.sum_amount,
        record_count = agg.cnt,
        updated_at   = NOW()
    FROM (
        SELECT
            region_id,
            SUM(amount) AS sum_amount,
            COUNT(*)    AS cnt
        FROM sales_fact
        GROUP BY region_id
    ) agg
    WHERE s.region_id = agg.region_id;

    COMMIT;
    RAISE NOTICE '彙總表更新完成（SET-BASED，MPP 並行）';
END;
$$;

CALL sp_update_summary_correct();

\echo '  彙總結果：'
SELECT s.region_id, r.region_name, s.total_amount, s.record_count
FROM sales_summary s
JOIN dim_region r ON s.region_id = r.region_id
ORDER BY s.total_amount DESC;

-- =============================================
-- 13.4 Co-located Join 最佳化
-- =============================================
\echo ''
\echo '>>> 13.4 Co-located Join 最佳化'
\echo '  兩個表用相同分佈鍵 JOIN → 無需 Motion'

DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS order_items CASCADE;

CREATE TABLE orders (
    order_id    BIGINT,
    customer_id INT,
    order_date  DATE
) DISTRIBUTED BY (order_id);

CREATE TABLE order_items (
    item_id     BIGINT,
    order_id    BIGINT,
    product_id  INT,
    qty         INT,
    unit_price  DECIMAL(10,2)
) DISTRIBUTED BY (order_id);  -- 與 orders 相同分佈鍵！

-- 插入資料
INSERT INTO orders
SELECT g, (random() * 5000)::INT, '2024-01-01'::DATE + (g % 365)
FROM generate_series(1, 10000) g;

INSERT INTO order_items
SELECT g, (g / 3 + 1), (random() * 200)::INT,
       (random() * 10 + 1)::INT,
       ROUND((random() * 100)::NUMERIC, 2)
FROM generate_series(1, 30000) g;

ANALYZE orders;
ANALYZE order_items;

-- Co-located Join 函數
CREATE OR REPLACE FUNCTION fn_get_order_total(p_order_id BIGINT)
RETURNS DECIMAL(15,2)
LANGUAGE sql STABLE AS $$
    SELECT COALESCE(SUM(qty * unit_price), 0)
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.order_id = p_order_id;
$$;

\echo '  測試 Co-located Join：'
SELECT fn_get_order_total(100) AS order_100_total;

\echo ''
\echo '  EXPLAIN 確認無 Redistribute Motion：'
EXPLAIN
SELECT o.order_id, SUM(oi.qty * oi.unit_price)
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_id = 100
GROUP BY o.order_id;

-- =============================================
-- 13.5 聚合 + WINDOW 函數
-- =============================================
\echo ''
\echo '>>> 13.5 聚合函數與 WINDOW 函數'

CREATE OR REPLACE FUNCTION fn_monthly_sales_analysis(
    p_year  INT,
    p_month INT
)
RETURNS TABLE (
    region_name  TEXT,
    total_sales  DECIMAL(15,2),
    order_count  BIGINT,
    avg_order    DECIMAL(15,2),
    rank_by_sales INT
)
LANGUAGE sql STABLE AS $$
    WITH monthly_agg AS (
        SELECT
            r.region_name,
            SUM(s.amount)   AS total_sales,
            COUNT(*)        AS order_count,
            AVG(s.amount)   AS avg_order
        FROM sales_fact s
        JOIN dim_region r ON s.region_id = r.region_id
        WHERE EXTRACT(YEAR  FROM s.sale_date) = p_year
          AND EXTRACT(MONTH FROM s.sale_date) = p_month
        GROUP BY r.region_name
    )
    SELECT
        region_name,
        total_sales,
        order_count,
        ROUND(avg_order, 2),
        RANK() OVER (ORDER BY total_sales DESC)::INT
    FROM monthly_agg
    ORDER BY total_sales DESC;
$$;

\echo '  2024 年 3 月銷售分析：'
SELECT * FROM fn_monthly_sales_analysis(2024, 3);

-- =============================================
-- 13.6 反模式對照
-- =============================================
\echo ''
\echo '>>> 13.6 反模式對照'
\echo ''
\echo '  ❌ 反模式 1：CURSOR/LOOP 逐行處理'
\echo '     → 破壞 MPP 並行性，改用 SET-BASED 操作'
\echo ''
\echo '  ❌ 反模式 2：VOLATILE 函數用在 WHERE'
\echo '     SELECT * FROM t WHERE date > get_cutoff();'
\echo '     → 每個 Segment 各呼叫一次'
\echo '     ✅ 正確：SELECT * FROM t WHERE date > (SELECT get_cutoff());'
\echo ''
\echo '  ❌ 反模式 3：FUNCTION 內執行 DDL'
\echo '     → DDL 需要全叢集鎖，改用 PROCEDURE（支援 COMMIT）'
\echo ''
\echo '  ❌ 反模式 4：跨節點 dblink'
\echo '     → 改用 PXF 或 External Table'

\echo ''
\echo '>>> MPP Stored Procedure 示範完成！'
\echo '  黃金守則：'
\echo '  1. SET-BASED 優先，LOOP/CURSOR 最後'
\echo '  2. 善用 Co-located Join（分佈鍵一致）'
\echo '  3. 使用 PROCEDURE（可 COMMIT）做 ETL'
\echo '  4. 大量 DML 後執行 ANALYZE'
