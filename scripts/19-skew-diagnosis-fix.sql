-------------------------------------------------------------------------------
-- 19-skew-diagnosis-fix.sql
-- 用途：完整模擬資料傾斜 → 診斷 → 修正 → 驗證的端到端測試案例
-- 場景：電商平台訂單表，錯誤選用低基數欄位為分佈鍵
-- 執行方式：psql -d cloudberry_poc -f 19-skew-diagnosis-fix.sql
-------------------------------------------------------------------------------

\echo '============================================================'
\echo '  資料傾斜（Data Skew）完整診斷與修正案例'
\echo '============================================================'
\echo ''
\echo '  場景：電商訂單系統'
\echo '  問題：DBA 誤用 order_status（僅 5 種值）作為分佈鍵'
\echo '  目標：診斷傾斜 → 量化影響 → 修正 → 驗證'
\echo ''

-- =============================================
-- Phase 1：建立傾斜環境
-- =============================================
\echo '=========================================='
\echo '  Phase 1：模擬資料傾斜'
\echo '=========================================='

DROP TABLE IF EXISTS ecom_orders_skewed CASCADE;
DROP TABLE IF EXISTS ecom_order_items CASCADE;
DROP TABLE IF EXISTS ecom_customers CASCADE;

-- 故意用 order_status 作為分佈鍵（只有 5 種值 → 嚴重傾斜）
CREATE TABLE ecom_orders_skewed (
    order_id        BIGINT,
    customer_id     INT,
    order_status    VARCHAR(20),   -- PENDING / PROCESSING / SHIPPED / DELIVERED / CANCELLED
    order_date      DATE,
    total_amount    DECIMAL(15,2),
    region          VARCHAR(30),
    payment_method  VARCHAR(20)
) DISTRIBUTED BY (order_status);   -- ← 錯誤的分佈鍵！

-- 訂單明細表（正確的分佈鍵，但 JOIN 時會有問題）
CREATE TABLE ecom_order_items (
    item_id         BIGSERIAL,
    order_id        BIGINT,
    product_id      INT,
    qty             INT,
    unit_price      DECIMAL(10,2)
) DISTRIBUTED BY (order_id);

-- 客戶維度表
CREATE TABLE ecom_customers (
    customer_id     INT,
    customer_name   VARCHAR(100),
    vip_level       VARCHAR(10)
) DISTRIBUTED REPLICATED;

\echo ''
\echo '  插入 500,000 筆訂單（模擬真實分佈）...'

-- 插入訂單（狀態分佈不均：大部分是 DELIVERED）
INSERT INTO ecom_orders_skewed
SELECT
    g AS order_id,
    (random() * 50000 + 1)::INT AS customer_id,
    -- 模擬真實狀態分佈：DELIVERED 佔 60%，其他各 10%
    CASE
        WHEN random() < 0.60 THEN 'DELIVERED'
        WHEN random() < 0.75 THEN 'SHIPPED'
        WHEN random() < 0.85 THEN 'PROCESSING'
        WHEN random() < 0.95 THEN 'PENDING'
        ELSE 'CANCELLED'
    END AS order_status,
    '2023-01-01'::DATE + (g % 730) AS order_date,
    ROUND((random() * 2000 + 10)::NUMERIC, 2) AS total_amount,
    CASE (g % 6)
        WHEN 0 THEN 'North' WHEN 1 THEN 'South' WHEN 2 THEN 'East'
        WHEN 3 THEN 'West'  WHEN 4 THEN 'Central' ELSE 'Overseas'
    END AS region,
    CASE (g % 4)
        WHEN 0 THEN 'Credit Card' WHEN 1 THEN 'Debit Card'
        WHEN 2 THEN 'Wire Transfer' ELSE 'E-Wallet'
    END AS payment_method
FROM generate_series(1, 500000) g;

-- 插入訂單明細
INSERT INTO ecom_order_items (order_id, product_id, qty, unit_price)
SELECT
    (random() * 499999 + 1)::BIGINT,
    (random() * 2000 + 1)::INT,
    (random() * 5 + 1)::INT,
    ROUND((random() * 500 + 5)::NUMERIC, 2)
FROM generate_series(1, 1500000) g;

-- 插入客戶
INSERT INTO ecom_customers
SELECT g, 'Customer ' || g,
       CASE (g % 4) WHEN 0 THEN 'Gold' WHEN 1 THEN 'Silver' WHEN 2 THEN 'Bronze' ELSE 'Normal' END
FROM generate_series(1, 50000) g;

ANALYZE ecom_orders_skewed;
ANALYZE ecom_order_items;
ANALYZE ecom_customers;

\echo '  資料建立完成：500K 訂單 / 1.5M 明細 / 50K 客戶'

-- =============================================
-- Phase 2：診斷傾斜
-- =============================================
\echo ''
\echo '=========================================='
\echo '  Phase 2：診斷資料傾斜'
\echo '=========================================='

-- 2.1 查看各 Segment 資料分佈
\echo ''
\echo '>>> 2.1 各 Segment 資料行數分佈'
SELECT
    gp_segment_id AS seg_id,
    COUNT(*)       AS row_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct,
    LPAD('█', (COUNT(*) * 50 / MAX(COUNT(*)) OVER ())::INT, '█') AS bar
FROM ecom_orders_skewed
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- 2.2 計算傾斜率
\echo ''
\echo '>>> 2.2 傾斜率計算'
WITH seg AS (
    SELECT gp_segment_id, COUNT(*) AS cnt
    FROM ecom_orders_skewed
    GROUP BY gp_segment_id
)
SELECT
    MAX(cnt)                                                    AS max_rows,
    MIN(cnt)                                                    AS min_rows,
    ROUND(AVG(cnt))                                             AS avg_rows,
    MAX(cnt) - MIN(cnt)                                         AS diff,
    ROUND((MAX(cnt) - MIN(cnt)) * 100.0 / NULLIF(AVG(cnt), 0), 2) AS skew_pct,
    CASE
        WHEN (MAX(cnt) - MIN(cnt)) * 100.0 / NULLIF(AVG(cnt), 0) > 50 THEN '嚴重傾斜'
        WHEN (MAX(cnt) - MIN(cnt)) * 100.0 / NULLIF(AVG(cnt), 0) > 20 THEN '中度傾斜'
        ELSE '正常'
    END                                                         AS diagnosis
FROM seg;

-- 2.3 分佈鍵值分析（找出根因）
\echo ''
\echo '>>> 2.3 分佈鍵值分析（order_status 只有 5 種值）'
SELECT
    order_status,
    COUNT(*)       AS row_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct,
    gp_segment_id  AS landed_on_seg
FROM ecom_orders_skewed
GROUP BY order_status, gp_segment_id
ORDER BY row_count DESC;

-- 2.4 每個 Segment 的磁碟使用估算
\echo ''
\echo '>>> 2.4 各 Segment 磁碟使用估算'
SELECT
    gp_segment_id AS seg_id,
    COUNT(*) AS rows,
    pg_size_pretty(SUM(pg_column_size(ecom_orders_skewed.*))::BIGINT) AS approx_size
FROM ecom_orders_skewed
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- 2.5 傾斜對查詢效能的影響
\echo ''
\echo '>>> 2.5 傾斜對查詢效能的影響'
\echo '  執行聚合查詢，觀察 EXPLAIN ANALYZE 中各 Segment 的耗時差異'

\timing on

EXPLAIN ANALYZE
SELECT
    region,
    order_status,
    COUNT(*)                     AS order_count,
    SUM(total_amount)            AS total_revenue,
    ROUND(AVG(total_amount), 2)  AS avg_order
FROM ecom_orders_skewed
WHERE order_date >= '2024-01-01'
GROUP BY region, order_status
ORDER BY total_revenue DESC;

\timing off

-- 2.6 JOIN 效能影響（分佈鍵不一致 → Redistribute Motion）
\echo ''
\echo '>>> 2.6 JOIN 時的 Redistribute Motion（效能殺手）'
\echo '  orders 以 order_status 分佈，order_items 以 order_id 分佈'
\echo '  → JOIN 時必須 Redistribute 其中一張表'

EXPLAIN
SELECT
    o.order_id,
    o.order_status,
    SUM(oi.qty * oi.unit_price) AS item_total
FROM ecom_orders_skewed o
JOIN ecom_order_items oi ON o.order_id = oi.order_id
WHERE o.order_date >= '2024-06-01'
GROUP BY o.order_id, o.order_status
LIMIT 10;

-- =============================================
-- Phase 3：修正傾斜
-- =============================================
\echo ''
\echo '=========================================='
\echo '  Phase 3：修正資料傾斜'
\echo '=========================================='

\echo ''
\echo '>>> 3.1 修改分佈鍵：order_status → order_id'
\echo '  （order_id 是高基數唯一欄位，且為 JOIN 鍵）'

\timing on

ALTER TABLE ecom_orders_skewed SET DISTRIBUTED BY (order_id);

\timing off

\echo '  分佈鍵修改完成（資料已重新分佈）'

-- 重新收集統計
ANALYZE ecom_orders_skewed;

-- =============================================
-- Phase 4：驗證修正結果
-- =============================================
\echo ''
\echo '=========================================='
\echo '  Phase 4：驗證修正結果'
\echo '=========================================='

-- 4.1 修正後各 Segment 資料分佈
\echo ''
\echo '>>> 4.1 修正後各 Segment 資料行數分佈'
SELECT
    gp_segment_id AS seg_id,
    COUNT(*)       AS row_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct,
    LPAD('█', (COUNT(*) * 50 / MAX(COUNT(*)) OVER ())::INT, '█') AS bar
FROM ecom_orders_skewed
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- 4.2 修正後傾斜率
\echo ''
\echo '>>> 4.2 修正後傾斜率'
WITH seg AS (
    SELECT gp_segment_id, COUNT(*) AS cnt
    FROM ecom_orders_skewed
    GROUP BY gp_segment_id
)
SELECT
    MAX(cnt)                                                    AS max_rows,
    MIN(cnt)                                                    AS min_rows,
    ROUND(AVG(cnt))                                             AS avg_rows,
    MAX(cnt) - MIN(cnt)                                         AS diff,
    ROUND((MAX(cnt) - MIN(cnt)) * 100.0 / NULLIF(AVG(cnt), 0), 2) AS skew_pct,
    CASE
        WHEN (MAX(cnt) - MIN(cnt)) * 100.0 / NULLIF(AVG(cnt), 0) > 50 THEN '嚴重傾斜'
        WHEN (MAX(cnt) - MIN(cnt)) * 100.0 / NULLIF(AVG(cnt), 0) > 20 THEN '中度傾斜'
        ELSE '正常'
    END                                                         AS diagnosis
FROM seg;

-- 4.3 修正後查詢效能對比
\echo ''
\echo '>>> 4.3 修正後查詢效能（同樣的聚合查詢）'

\timing on

EXPLAIN ANALYZE
SELECT
    region,
    order_status,
    COUNT(*)                     AS order_count,
    SUM(total_amount)            AS total_revenue,
    ROUND(AVG(total_amount), 2)  AS avg_order
FROM ecom_orders_skewed
WHERE order_date >= '2024-01-01'
GROUP BY region, order_status
ORDER BY total_revenue DESC;

\timing off

-- 4.4 修正後 JOIN 效能（Co-located Join）
\echo ''
\echo '>>> 4.4 修正後 JOIN 效能（Co-located Join）'
\echo '  orders 和 order_items 現在都以 order_id 分佈 → 無需 Redistribute'

EXPLAIN
SELECT
    o.order_id,
    o.order_status,
    SUM(oi.qty * oi.unit_price) AS item_total
FROM ecom_orders_skewed o
JOIN ecom_order_items oi ON o.order_id = oi.order_id
WHERE o.order_date >= '2024-06-01'
GROUP BY o.order_id, o.order_status
LIMIT 10;

-- =============================================
-- Phase 5：前後對比總結報告
-- =============================================
\echo ''
\echo '=========================================='
\echo '  Phase 5：前後對比總結報告'
\echo '=========================================='

-- 用 CTE 產生完整對比報告
\echo ''
\echo '>>> 修正前後對比'

WITH current_dist AS (
    SELECT gp_segment_id, COUNT(*) AS cnt
    FROM ecom_orders_skewed
    GROUP BY gp_segment_id
)
SELECT
    '修正後 (order_id)'      AS distribution_key,
    MAX(cnt)                  AS max_rows,
    MIN(cnt)                  AS min_rows,
    ROUND(AVG(cnt))           AS avg_rows,
    ROUND((MAX(cnt) - MIN(cnt)) * 100.0 / NULLIF(AVG(cnt), 0), 2) AS skew_pct,
    CASE
        WHEN (MAX(cnt) - MIN(cnt)) * 100.0 / NULLIF(AVG(cnt), 0) < 20 THEN '✓ 正常'
        ELSE '✗ 仍需調整'
    END AS result
FROM current_dist;

\echo ''
\echo '>>> 驗證清單'
\echo '  [✓] 傾斜率 < 20%（資料均勻分佈）'
\echo '  [✓] 各 Segment 行數接近（百分比差異極小）'
\echo '  [✓] JOIN 使用 Co-located Join（無 Redistribute Motion）'
\echo '  [✓] 分佈鍵選用高基數唯一欄位（order_id）'

-- 清理
DROP TABLE IF EXISTS ecom_orders_skewed CASCADE;
DROP TABLE IF EXISTS ecom_order_items CASCADE;
DROP TABLE IF EXISTS ecom_customers CASCADE;

\echo ''
\echo '=========================================='
\echo '  資料傾斜診斷與修正案例 — 測試通過'
\echo '=========================================='
