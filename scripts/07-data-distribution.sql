-------------------------------------------------------------------------------
-- 07-data-distribution.sql
-- 對應教學：第 9 章 — 資料分佈策略
-- 用途：實作三種分佈模式，診斷資料傾斜
-- 執行方式：psql -d cloudberry_poc -f 07-data-distribution.sql
-------------------------------------------------------------------------------

\echo '============================================'
\echo '  第 9 章：資料分佈策略'
\echo '============================================'

-- =============================================
-- 9.1 Hash 分佈（推薦，高基數欄位）
-- =============================================
\echo ''
\echo '>>> 9.1 Hash 分佈 — 最常用的模式'

DROP TABLE IF EXISTS orders_hash;
CREATE TABLE orders_hash (
    order_id    BIGINT,
    customer_id INT,
    amount      DECIMAL(15,2),
    order_date  DATE
) DISTRIBUTED BY (order_id);

INSERT INTO orders_hash
SELECT
    g AS order_id,
    (random() * 10000)::INT AS customer_id,
    ROUND((random() * 500)::NUMERIC, 2) AS amount,
    '2024-01-01'::DATE + (g % 365) AS order_date
FROM generate_series(1, 50000) g;

\echo '  已建立 orders_hash 表（DISTRIBUTED BY order_id），插入 50,000 筆'

-- 查看各 Segment 資料分佈
\echo ''
\echo '  各 Segment 資料分佈（Hash 分佈 - 應均勻）：'
SELECT
    gp_segment_id,
    COUNT(*) AS row_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM orders_hash
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- =============================================
-- 9.2 隨機分佈（無明顯 Join Key）
-- =============================================
\echo ''
\echo '>>> 9.2 隨機分佈 — 適合日誌/事件類資料'

DROP TABLE IF EXISTS log_events_random;
CREATE TABLE log_events_random (
    event_time  TIMESTAMP,
    event_type  VARCHAR(50),
    payload     TEXT
) DISTRIBUTED RANDOMLY;

INSERT INTO log_events_random
SELECT
    NOW() - (random() * INTERVAL '365 days'),
    CASE (g % 5)
        WHEN 0 THEN 'LOGIN'
        WHEN 1 THEN 'LOGOUT'
        WHEN 2 THEN 'PAGE_VIEW'
        WHEN 3 THEN 'PURCHASE'
        WHEN 4 THEN 'ERROR'
    END,
    'payload_' || g
FROM generate_series(1, 50000) g;

\echo '  已建立 log_events_random 表（DISTRIBUTED RANDOMLY），插入 50,000 筆'

\echo ''
\echo '  各 Segment 資料分佈（隨機分佈 - 應均勻）：'
SELECT
    gp_segment_id,
    COUNT(*) AS row_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM log_events_random
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- =============================================
-- 9.3 複製分佈（小型維度表）
-- =============================================
\echo ''
\echo '>>> 9.3 複製分佈 — 小型維度表專用'

DROP TABLE IF EXISTS dim_region_replicated;
CREATE TABLE dim_region_replicated (
    region_id   INT,
    region_name VARCHAR(100)
) DISTRIBUTED REPLICATED;

INSERT INTO dim_region_replicated VALUES
    (1, 'Asia'),
    (2, 'Europe'),
    (3, 'Americas'),
    (4, 'Africa'),
    (5, 'Oceania');

\echo '  已建立 dim_region_replicated 表（DISTRIBUTED REPLICATED），5 筆'

\echo ''
\echo '  REPLICATED 表每個 Segment 都有完整副本：'
SELECT COUNT(*) AS total_rows FROM dim_region_replicated;
\echo '  （REPLICATED 表不支援 gp_segment_id 查詢，因為每個 Segment 都有完整資料）'

-- =============================================
-- 9.4 資料傾斜（Skew）示範
-- =============================================
\echo ''
\echo '>>> 9.4 資料傾斜（Skew）示範'
\echo '  故意用低基數欄位做分佈鍵 → 造成傾斜'

DROP TABLE IF EXISTS orders_skewed;
CREATE TABLE orders_skewed (
    order_id    BIGINT,
    gender      CHAR(1),    -- 只有 M/F，非常差的分佈鍵！
    amount      DECIMAL(15,2)
) DISTRIBUTED BY (gender);

INSERT INTO orders_skewed
SELECT
    g,
    CASE WHEN random() > 0.5 THEN 'M' ELSE 'F' END,
    ROUND((random() * 500)::NUMERIC, 2)
FROM generate_series(1, 50000) g;

\echo '  已建立 orders_skewed 表（DISTRIBUTED BY gender → 只有 M/F）'

\echo ''
\echo '  各 Segment 資料分佈（傾斜！）：'
SELECT
    gp_segment_id,
    COUNT(*) AS row_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM orders_skewed
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- =============================================
-- 9.5 傾斜率計算
-- =============================================
\echo ''
\echo '>>> 9.5 傾斜率計算'
\echo '  傾斜率 > 20% 應考慮更換分佈鍵'

\echo ''
\echo '--- orders_hash 的傾斜率（好的分佈）---'
WITH segment_counts AS (
    SELECT gp_segment_id, COUNT(*) AS cnt
    FROM orders_hash
    GROUP BY gp_segment_id
)
SELECT
    MAX(cnt) AS max_rows,
    MIN(cnt) AS min_rows,
    ROUND(AVG(cnt)) AS avg_rows,
    ROUND((MAX(cnt) - MIN(cnt)) * 100.0 / NULLIF(AVG(cnt), 0), 2) AS skew_pct
FROM segment_counts;

\echo ''
\echo '--- orders_skewed 的傾斜率（差的分佈）---'
WITH segment_counts AS (
    SELECT gp_segment_id, COUNT(*) AS cnt
    FROM orders_skewed
    GROUP BY gp_segment_id
)
SELECT
    MAX(cnt) AS max_rows,
    MIN(cnt) AS min_rows,
    ROUND(AVG(cnt)) AS avg_rows,
    ROUND((MAX(cnt) - MIN(cnt)) * 100.0 / NULLIF(AVG(cnt), 0), 2) AS skew_pct
FROM segment_counts;

-- =============================================
-- 9.6 修改分佈鍵
-- =============================================
\echo ''
\echo '>>> 9.6 修改分佈鍵（修正傾斜）'

ALTER TABLE orders_skewed SET DISTRIBUTED BY (order_id);

\echo '  已將 orders_skewed 的分佈鍵從 gender 改為 order_id'

\echo ''
\echo '  修正後的傾斜率：'
WITH segment_counts AS (
    SELECT gp_segment_id, COUNT(*) AS cnt
    FROM orders_skewed
    GROUP BY gp_segment_id
)
SELECT
    MAX(cnt) AS max_rows,
    MIN(cnt) AS min_rows,
    ROUND(AVG(cnt)) AS avg_rows,
    ROUND((MAX(cnt) - MIN(cnt)) * 100.0 / NULLIF(AVG(cnt), 0), 2) AS skew_pct
FROM segment_counts;

\echo ''
\echo '>>> 資料分佈策略示範完成！'
\echo '  關鍵原則：'
\echo '  1. 選擇高基數欄位（接近主鍵）作為分佈鍵'
\echo '  2. 常用於 JOIN 的欄位優先考慮'
\echo '  3. 避免 NULL 值多或選擇性低的欄位'
\echo '  4. 傾斜率 > 20% 需更換分佈鍵'
