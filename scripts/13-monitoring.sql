-------------------------------------------------------------------------------
-- 13-monitoring.sql
-- 對應教學：第 15 章 — 監控與診斷
-- 用途：查詢監控、鎖偵測、效能分析、Segment 健康檢查
-- 執行方式：psql -d cloudberry_poc -f 13-monitoring.sql
-------------------------------------------------------------------------------

\echo '============================================'
\echo '  第 15 章：監控與診斷'
\echo '============================================'

-- =============================================
-- 15.1 查詢監控
-- =============================================
\echo ''
\echo '>>> 15.1 目前進行中的查詢'

SELECT
    pid,
    usename,
    datname,
    state,
    now() - query_start AS duration,
    LEFT(query, 80) AS query_preview
FROM pg_stat_activity
WHERE state != 'idle'
ORDER BY duration DESC
LIMIT 20;

-- =============================================
-- 15.2 長時間執行的查詢
-- =============================================
\echo ''
\echo '>>> 15.2 長時間執行的查詢（> 1 分鐘）'

SELECT
    pid,
    usename,
    now() - query_start AS duration,
    wait_event_type,
    wait_event,
    LEFT(query, 100) AS query_preview
FROM pg_stat_activity
WHERE state = 'active'
  AND query_start < now() - INTERVAL '1 minute'
ORDER BY duration DESC;

-- =============================================
-- 15.3 鎖偵測
-- =============================================
\echo ''
\echo '>>> 15.3 目前的鎖定狀態'

SELECT
    l.locktype,
    l.relation::regclass AS table_name,
    l.mode,
    l.granted,
    a.pid,
    a.usename,
    LEFT(a.query, 60) AS query_preview
FROM pg_locks l
JOIN pg_stat_activity a ON l.pid = a.pid
WHERE l.relation IS NOT NULL
  AND a.query NOT LIKE '%pg_locks%'
ORDER BY l.relation, l.mode
LIMIT 20;

-- =============================================
-- 15.4 資料庫大小
-- =============================================
\echo ''
\echo '>>> 15.4 資料庫大小'

SELECT
    datname,
    pg_size_pretty(pg_database_size(datname)) AS size
FROM pg_database
WHERE datistemplate = FALSE
ORDER BY pg_database_size(datname) DESC;

-- =============================================
-- 15.5 各表格大小
-- =============================================
\echo ''
\echo '>>> 15.5 各表格大小（前 20 名）'

SELECT
    schemaname || '.' || tablename AS full_name,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) AS total_size,
    pg_size_pretty(pg_relation_size(schemaname || '.' || tablename)) AS data_size
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'gp_toolkit')
ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC
LIMIT 20;

-- =============================================
-- 15.6 各 Segment 資料分佈監控
-- =============================================
\echo ''
\echo '>>> 15.6 各 Segment 資料分佈（銷售事實表）'

SELECT
    gp_segment_id AS seg_id,
    COUNT(*) AS rows,
    pg_size_pretty(SUM(pg_column_size(sales_fact.*))::BIGINT) AS approx_size
FROM sales_fact
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- =============================================
-- 15.7 Segment 狀態檢查
-- =============================================
\echo ''
\echo '>>> 15.7 Segment 狀態檢查'

SELECT
    content AS seg_id,
    CASE WHEN role = 'p' THEN 'Primary' ELSE 'Mirror' END AS role,
    hostname,
    port,
    CASE status WHEN 'u' THEN '正常' WHEN 'd' THEN '異常' END AS status,
    CASE mode
        WHEN 's' THEN '同步'
        WHEN 'c' THEN 'Change Tracking'
        WHEN 'r' THEN '恢復中'
        WHEN 'n' THEN 'N/A'
    END AS mode,
    CASE WHEN role = preferred_role THEN '是' ELSE '否（已 failover）' END AS original_role
FROM gp_segment_configuration
ORDER BY content, role;

-- =============================================
-- 15.8 連線統計
-- =============================================
\echo ''
\echo '>>> 15.8 連線統計'

SELECT
    datname,
    usename,
    COUNT(*) AS connections,
    COUNT(*) FILTER (WHERE state = 'active') AS active,
    COUNT(*) FILTER (WHERE state = 'idle') AS idle,
    COUNT(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_txn
FROM pg_stat_activity
GROUP BY datname, usename
ORDER BY connections DESC;

-- =============================================
-- 15.9 統計資訊新鮮度
-- =============================================
\echo ''
\echo '>>> 15.9 統計資訊新鮮度（最後 ANALYZE 時間）'

SELECT
    schemaname,
    relname AS table_name,
    n_live_tup AS live_rows,
    n_dead_tup AS dead_rows,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
WHERE n_live_tup > 0
ORDER BY last_analyze ASC NULLS FIRST
LIMIT 15;

-- =============================================
-- 15.10 VACUUM 建議
-- =============================================
\echo ''
\echo '>>> 15.10 需要 VACUUM 的表格'

SELECT
    schemaname,
    relname AS table_name,
    n_live_tup AS live_rows,
    n_dead_tup AS dead_rows,
    ROUND(n_dead_tup * 100.0 / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS dead_pct,
    last_vacuum,
    last_autovacuum
FROM pg_stat_user_tables
WHERE n_dead_tup > 0
ORDER BY n_dead_tup DESC
LIMIT 10;

\echo ''
\echo '>>> 監控與診斷完成！'
\echo '  常用監控指令摘要：'
\echo '  - pg_stat_activity → 查詢監控'
\echo '  - pg_locks → 鎖偵測'
\echo '  - gp_segment_configuration → Segment 狀態'
\echo '  - pg_stat_user_tables → 統計資訊'
\echo '  - 容器內：gpstate -s / gpstate -e'
