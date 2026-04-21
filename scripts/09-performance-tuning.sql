-------------------------------------------------------------------------------
-- 09-performance-tuning.sql
-- 對應教學：第 11 章 — 效能調優與工作負載管理
-- 用途：Resource Queue、GUC 參數調優、EXPLAIN 分析
-- 執行方式：psql -d cloudberry_poc -f 09-performance-tuning.sql
-------------------------------------------------------------------------------

\echo '============================================'
\echo '  第 11 章：效能調優與工作負載管理'
\echo '============================================'

-- =============================================
-- 11.1 Resource Queue 資源隔離
-- =============================================
\echo ''
\echo '>>> 11.1 Resource Queue 資源隔離'
\echo '  用途：限制不同使用者/角色的查詢並行度與記憶體'

-- 建立高優先 Queue（批次作業用）
-- 先嘗試清理（忽略不存在的錯誤）
DO $$ BEGIN EXECUTE 'DROP RESOURCE QUEUE batch_queue'; EXCEPTION WHEN OTHERS THEN NULL; END $$;
CREATE RESOURCE QUEUE batch_queue
    WITH (
        ACTIVE_STATEMENTS = 5,
        PRIORITY = HIGH
    );
\echo '  已建立 batch_queue（HIGH 優先，最多 5 個並行查詢）'

-- 建立低優先 Queue（分析師探索用）
DO $$ BEGIN EXECUTE 'DROP RESOURCE QUEUE analyst_queue'; EXCEPTION WHEN OTHERS THEN NULL; END $$;
CREATE RESOURCE QUEUE analyst_queue
    WITH (
        ACTIVE_STATEMENTS = 10,
        PRIORITY = LOW
    );
\echo '  已建立 analyst_queue（LOW 優先，最多 10 個並行查詢）'

-- 建立使用者並指派 Queue
DROP USER IF EXISTS batch_user;
CREATE USER batch_user WITH PASSWORD 'batch_pass';
ALTER ROLE batch_user RESOURCE QUEUE batch_queue;

DROP USER IF EXISTS analyst_user;
CREATE USER analyst_user WITH PASSWORD 'analyst_pass';
ALTER ROLE analyst_user RESOURCE QUEUE analyst_queue;

\echo '  已建立並指派使用者：'
\echo '    batch_user   → batch_queue (HIGH)'
\echo '    analyst_user → analyst_queue (LOW)'

-- 查看 Queue 設定
\echo ''
\echo '  目前 Resource Queue 設定：'
SELECT
    rsqname AS queue_name,
    rsqcountlimit AS max_active,
    rsqcostlimit AS max_cost
FROM pg_resqueue
ORDER BY rsqname;

-- =============================================
-- 11.2 重要 GUC 參數
-- =============================================
\echo ''
\echo '>>> 11.2 重要 GUC 參數'

\echo '--- work_mem（影響 Sort/Hash Join）---'
SHOW work_mem;

\echo '--- shared_buffers ---'
SHOW shared_buffers;

\echo '--- gp_vmem_protect_limit（Segment 記憶體上限）---'
SHOW gp_vmem_protect_limit;

\echo '--- max_connections ---'
SHOW max_connections;

\echo '--- statement_timeout ---'
SHOW statement_timeout;

-- Session 級別調整示範
\echo ''
\echo '  Session 級別暫時調大 work_mem：'
SET work_mem = '256MB';
SHOW work_mem;

-- 恢復
RESET work_mem;

-- =============================================
-- 11.3 EXPLAIN 計劃分析（進階）
-- =============================================
\echo ''
\echo '>>> 11.3 EXPLAIN 計劃分析'

-- 先確保有足夠資料
\echo '  更新統計資訊...'
ANALYZE sales_ao;
ANALYZE clickstream_aoco;
ANALYZE dim_region_replicated;

-- 完整執行計劃
\echo ''
\echo '--- 簡單全表聚合 ---'
EXPLAIN
SELECT device_type, COUNT(*), AVG(duration_ms)
FROM clickstream_aoco
GROUP BY device_type;

-- 帶 JOIN 的查詢
\echo ''
\echo '--- JOIN 查詢（注意 Motion 類型）---'
\echo '  REPLICATED 表 JOIN 不會產生 Redistribute Motion'

-- 建立一個臨時維度表供 JOIN
DROP TABLE IF EXISTS dim_product_tmp;
CREATE TABLE dim_product_tmp (
    product_id INT,
    product_name VARCHAR(100),
    category VARCHAR(50)
) DISTRIBUTED REPLICATED;

INSERT INTO dim_product_tmp
SELECT g, 'Product ' || g,
       CASE (g % 5) WHEN 0 THEN 'Electronics' WHEN 1 THEN 'Clothing'
                     WHEN 2 THEN 'Food' WHEN 3 THEN 'Books' ELSE 'Others' END
FROM generate_series(1, 1000) g;

ANALYZE dim_product_tmp;

\echo ''
EXPLAIN ANALYZE
SELECT
    p.category,
    SUM(s.amount) AS total_sales,
    COUNT(*) AS order_count
FROM sales_ao s
JOIN dim_product_tmp p ON s.product_id = p.product_id
GROUP BY p.category
ORDER BY total_sales DESC;

-- =============================================
-- 11.4 效能調優清單
-- =============================================
\echo ''
\echo '>>> 11.4 效能調優清單'
\echo ''
\echo '  □ 分佈鍵無資料傾斜（SKEW < 20%）'
\echo '  □ JOIN 的表使用相同分佈鍵（Co-located）'
\echo '  □ 統計資訊是最新的（ANALYZE 後查詢計劃才準確）'
\echo '  □ 分區裁剪有效（WHERE 條件包含分區鍵）'
\echo '  □ EXPLAIN 中無多餘的 Redistribute Motion'
\echo '  □ Resource Queue 設有適當 MEMORY_LIMIT'
\echo '  □ AO/AOCO 表定期 VACUUM'
\echo '  □ work_mem 設置合理'

-- 清理
DROP USER IF EXISTS batch_user;
DROP USER IF EXISTS analyst_user;
DO $$ BEGIN EXECUTE 'DROP RESOURCE QUEUE batch_queue'; EXCEPTION WHEN OTHERS THEN NULL; END $$;
DO $$ BEGIN EXECUTE 'DROP RESOURCE QUEUE analyst_queue'; EXCEPTION WHEN OTHERS THEN NULL; END $$;

\echo ''
\echo '>>> 效能調優示範完成！'
