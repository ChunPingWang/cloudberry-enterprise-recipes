-------------------------------------------------------------------------------
-- 03-cluster-architecture.sql
-- 對應教學：第 5 章 — 叢集架構深度解析
-- 用途：深入了解 Coordinator / Segment 架構與查詢執行流程
-- 執行方式：psql -d cloudberry_poc -f 03-cluster-architecture.sql
-------------------------------------------------------------------------------

\echo '============================================'
\echo '  第 5 章：叢集架構深度解析'
\echo '============================================'

-- =============================================
-- 5.1 Coordinator 資訊
-- =============================================
\echo ''
\echo '>>> 5.1 Coordinator（協調節點）資訊'
\echo '  - Coordinator 是所有客戶端的唯一連接點'
\echo '  - 負責 SQL 解析、查詢規劃、分發與結果彙整'
\echo '  - 不儲存使用者資料（只儲存系統表）'

SELECT
    dbid,
    hostname,
    port,
    datadir,
    role AS current_role,
    status
FROM gp_segment_configuration
WHERE content = -1
ORDER BY role;

-- =============================================
-- 5.2 Segment（工作節點）資訊
-- =============================================
\echo ''
\echo '>>> 5.2 Segment（工作節點）資訊'
\echo '  - Segment 實際儲存資料並執行查詢'
\echo '  - 每個 Segment 有 Primary 和 Mirror'

SELECT
    content AS segment_id,
    role,
    CASE role
        WHEN 'p' THEN 'Primary'
        WHEN 'm' THEN 'Mirror'
    END AS role_name,
    hostname,
    port,
    datadir,
    mode,
    status
FROM gp_segment_configuration
WHERE content >= 0
ORDER BY content, role;

-- =============================================
-- 5.3 查詢執行流程示範
-- =============================================
\echo ''
\echo '>>> 5.3 查詢執行流程示範'
\echo '  建立範例表來觀察 MPP 分散式查詢計劃'

-- 建立範例表
CREATE TABLE IF NOT EXISTS demo_sales (
    sale_id     BIGINT,
    region      VARCHAR(50),
    amount      DECIMAL(15,2),
    sale_date   DATE
) DISTRIBUTED BY (sale_id);

-- 插入範例資料
TRUNCATE demo_sales;
INSERT INTO demo_sales (sale_id, region, amount, sale_date)
SELECT
    generate_series AS sale_id,
    CASE (generate_series % 4)
        WHEN 0 THEN 'Asia'
        WHEN 1 THEN 'Europe'
        WHEN 2 THEN 'Americas'
        WHEN 3 THEN 'Africa'
    END AS region,
    ROUND((random() * 1000)::NUMERIC, 2) AS amount,
    '2024-01-01'::DATE + (generate_series % 365) AS sale_date
FROM generate_series(1, 10000);

\echo '  已插入 10,000 筆範例資料。'

-- =============================================
-- 5.4 觀察資料在各 Segment 的分佈
-- =============================================
\echo ''
\echo '>>> 5.4 觀察資料在各 Segment 的分佈'

SELECT
    gp_segment_id,
    COUNT(*) AS row_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM demo_sales
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- =============================================
-- 5.5 查看分散式執行計劃（EXPLAIN）
-- =============================================
\echo ''
\echo '>>> 5.5 查看分散式執行計劃'
\echo '  注意觀察以下關鍵資訊：'
\echo '  - Gather Motion：結果彙整至 Coordinator'
\echo '  - Redistribute Motion：資料在 Segment 間重分佈'
\echo '  - Broadcast Motion：小表廣播至所有 Segment'

\echo ''
\echo '--- 簡單查詢的執行計劃 ---'
EXPLAIN
SELECT * FROM demo_sales WHERE region = 'Asia';

\echo ''
\echo '--- 聚合查詢的執行計劃 ---'
EXPLAIN
SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
FROM demo_sales
GROUP BY region;

-- =============================================
-- 5.6 查看實際執行統計（EXPLAIN ANALYZE）
-- =============================================
\echo ''
\echo '>>> 5.6 查看實際執行統計'

EXPLAIN ANALYZE
SELECT
    region,
    SUM(amount) AS total_sales,
    COUNT(*) AS order_count,
    ROUND(AVG(amount), 2) AS avg_order
FROM demo_sales
GROUP BY region
ORDER BY total_sales DESC;

\echo ''
\echo '>>> 叢集架構解析完成！'
\echo '  重點觀察：'
\echo '  1. 資料均勻分佈在各 Segment（比例接近）'
\echo '  2. EXPLAIN 中的 Motion 節點代表資料在節點間傳遞'
\echo '  3. Gather Motion = 匯整到 Coordinator'
