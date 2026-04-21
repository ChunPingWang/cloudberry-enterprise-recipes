-------------------------------------------------------------------------------
-- 08-table-types.sql
-- 對應教學：第 10 章 — 表格類型選擇指南
-- 用途：實作 Heap / AO / AOCO 三種表格類型與分區表
-- 執行方式：psql -d cloudberry_poc -f 08-table-types.sql
-------------------------------------------------------------------------------

\echo '============================================'
\echo '  第 10 章：表格類型選擇指南'
\echo '============================================'

-- =============================================
-- 10.1 Heap 表（預設，適合 OLTP）
-- =============================================
\echo ''
\echo '>>> 10.1 Heap 表 — 頻繁 UPDATE/DELETE 時使用'

DROP TABLE IF EXISTS txn_heap;
CREATE TABLE txn_heap (
    txn_id      BIGINT,
    amount      DECIMAL(15,2),
    status      VARCHAR(20),
    created_at  TIMESTAMP DEFAULT NOW()
) DISTRIBUTED BY (txn_id);

INSERT INTO txn_heap
SELECT g, ROUND((random() * 1000)::NUMERIC, 2),
       CASE (g % 3) WHEN 0 THEN 'PENDING' WHEN 1 THEN 'COMPLETED' ELSE 'FAILED' END,
       NOW() - (random() * INTERVAL '30 days')
FROM generate_series(1, 10000) g;

\echo '  Heap 表特點：支援完整 UPDATE/DELETE，無壓縮'
\echo '  已插入 10,000 筆'

-- 示範 UPDATE（Heap 表擅長的操作）
UPDATE txn_heap SET status = 'COMPLETED' WHERE status = 'PENDING' AND txn_id <= 100;
\echo '  已更新前 100 筆 PENDING → COMPLETED'

-- =============================================
-- 10.2 AO 表（行式壓縮，大型事實表推薦）
-- =============================================
\echo ''
\echo '>>> 10.2 AO 表 — 大型事實表、批量載入優先'

DROP TABLE IF EXISTS sales_ao;
CREATE TABLE sales_ao (
    sale_id     BIGINT,
    product_id  INT,
    customer_id INT,
    sale_date   DATE,
    amount      DECIMAL(15,2)
)
WITH (appendoptimized=true, compresslevel=5)
DISTRIBUTED BY (sale_id);

INSERT INTO sales_ao
SELECT g, (random() * 1000)::INT, (random() * 5000)::INT,
       '2024-01-01'::DATE + (g % 365),
       ROUND((random() * 500)::NUMERIC, 2)
FROM generate_series(1, 100000) g;

\echo '  AO 表特點：支援壓縮、適合批量載入、UPDATE/DELETE 有限支援'
\echo '  已插入 100,000 筆（壓縮等級 5）'

-- =============================================
-- 10.3 AOCO 表（列式壓縮，分析查詢最佳）
-- =============================================
\echo ''
\echo '>>> 10.3 AOCO 表 — 聚合分析、寬表首選'

DROP TABLE IF EXISTS clickstream_aoco;
CREATE TABLE clickstream_aoco (
    user_id     BIGINT,
    page_url    TEXT,
    click_time  TIMESTAMP,
    session_id  VARCHAR(64),
    device_type VARCHAR(20),
    browser     VARCHAR(30),
    country     VARCHAR(50),
    duration_ms INT
)
WITH (appendoptimized=true, orientation=column, compresslevel=5)
DISTRIBUTED BY (user_id);

INSERT INTO clickstream_aoco
SELECT
    (random() * 100000)::BIGINT,
    '/page/' || (g % 200),
    NOW() - (random() * INTERVAL '90 days'),
    md5(g::TEXT),
    CASE (g % 3) WHEN 0 THEN 'Desktop' WHEN 1 THEN 'Mobile' ELSE 'Tablet' END,
    CASE (g % 4) WHEN 0 THEN 'Chrome' WHEN 1 THEN 'Safari' WHEN 2 THEN 'Firefox' ELSE 'Edge' END,
    CASE (g % 5) WHEN 0 THEN 'Taiwan' WHEN 1 THEN 'Japan' WHEN 2 THEN 'USA' WHEN 3 THEN 'UK' ELSE 'Germany' END,
    (random() * 30000)::INT
FROM generate_series(1, 100000) g;

\echo '  AOCO 表特點：列式儲存+壓縮，聚合查詢只讀需要的欄位'
\echo '  已插入 100,000 筆（列式壓縮等級 5）'

-- =============================================
-- 10.4 比較查詢效能
-- =============================================
\echo ''
\echo '>>> 10.4 比較查詢效能'
\echo '  對 AOCO 表做聚合分析（只需讀取少數欄位）：'

\timing on

SELECT device_type, country,
       COUNT(*) AS clicks,
       ROUND(AVG(duration_ms)) AS avg_duration
FROM clickstream_aoco
GROUP BY device_type, country
ORDER BY clicks DESC
LIMIT 10;

\timing off

-- =============================================
-- 10.5 查看表格屬性
-- =============================================
\echo ''
\echo '>>> 10.5 查看表格屬性'

\echo '--- Heap 表 ---'
\d+ txn_heap

\echo '--- AO 表 ---'
\d+ sales_ao

\echo '--- AOCO 表 ---'
\d+ clickstream_aoco

-- =============================================
-- 10.6 分區表（Partition Table）
-- =============================================
\echo ''
\echo '>>> 10.6 分區表 — 按時間範圍分區'

DROP TABLE IF EXISTS sales_partitioned;
CREATE TABLE sales_partitioned (
    sale_id     BIGINT,
    sale_date   DATE,
    product_id  INT,
    amount      DECIMAL(15,2)
)
WITH (appendoptimized=true, orientation=column, compresslevel=5)
DISTRIBUTED BY (sale_id)
PARTITION BY RANGE (sale_date)
(
    START ('2024-01-01') END ('2025-01-01') EVERY (INTERVAL '1 month'),
    DEFAULT PARTITION other
);

INSERT INTO sales_partitioned
SELECT g, '2024-01-01'::DATE + (g % 365),
       (random() * 500)::INT,
       ROUND((random() * 1000)::NUMERIC, 2)
FROM generate_series(1, 120000) g;

\echo '  已建立按月分區表（12 個月分區 + DEFAULT）'
\echo '  已插入 120,000 筆'

-- 查看分區定義（Cloudberry 3.x 使用 pg_inherits + pg_class）
\echo ''
\echo '  分區清單：'
SELECT
    c.relname AS partition_name,
    pg_get_expr(c.relpartbound, c.oid) AS partition_bound
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_class p ON p.oid = i.inhparent
WHERE p.relname = 'sales_partitioned'
ORDER BY c.relname;

-- 分區裁剪示範
\echo ''
\echo '  分區裁剪效果（EXPLAIN）：'
\echo '  查詢只掃描 2024-03 分區，其他分區被跳過'
EXPLAIN
SELECT SUM(amount) FROM sales_partitioned
WHERE sale_date BETWEEN '2024-03-01' AND '2024-03-31';

\echo ''
\echo '>>> 表格類型示範完成！'
\echo '  選型指南：'
\echo '  - 小表 + 頻繁更新 → Heap'
\echo '  - 大型事實表 + 批量載入 → AO'
\echo '  - 寬表 + 聚合分析 → AOCO（列式）'
\echo '  - 超大表 + 時間查詢 → AOCO + 分區'
