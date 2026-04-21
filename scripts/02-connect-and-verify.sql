-------------------------------------------------------------------------------
-- 02-connect-and-verify.sql
-- 對應教學：第 4 章 — 連線與基礎操作
-- 用途：驗證 Sandbox 是否正常啟動，並練習基礎 SQL 操作
-- 執行方式：
--   docker exec -it cbdb-cdw /bin/bash
--   psql -f /path/to/02-connect-and-verify.sql
--   或逐行複製到 psql 中執行
-------------------------------------------------------------------------------

-- =============================================
-- 4.1 確認資料庫版本
-- =============================================
\echo '>>> 4.1 確認資料庫版本'
SELECT VERSION();

-- =============================================
-- 4.2 查看所有 Segment 狀態
-- =============================================
\echo ''
\echo '>>> 4.2 查看所有 Segment 狀態'
SELECT
    dbid,
    content,
    role,
    preferred_role,
    mode,
    status,
    port,
    hostname
FROM gp_segment_configuration
ORDER BY content, role;

-- =============================================
-- 4.3 Segment 狀態欄位說明
-- =============================================
\echo ''
\echo '>>> 4.3 Segment 狀態解讀'
\echo '  role=p → Primary（主要節點）'
\echo '  role=m → Mirror（鏡像節點）'
\echo '  mode=s → 同步中（正常狀態）'
\echo '  mode=c → Change tracking（Mirror 異常）'
\echo '  mode=n → Not in sync（Coordinator 正常值）'
\echo '  status=u → Up（正常）'
\echo '  status=d → Down（異常）'

-- =============================================
-- 4.4 快速健康檢查
-- =============================================
\echo ''
\echo '>>> 4.4 快速健康檢查'

-- 檢查是否有異常 Segment
SELECT
    CASE
        WHEN COUNT(*) FILTER (WHERE status = 'd') = 0
            THEN '✓ 所有 Segment 狀態正常'
        ELSE '✗ 發現 ' || COUNT(*) FILTER (WHERE status = 'd') || ' 個異常 Segment'
    END AS health_check
FROM gp_segment_configuration;

-- 檢查 Primary/Mirror 配對
SELECT
    content,
    COUNT(*) FILTER (WHERE role = 'p') AS primary_count,
    COUNT(*) FILTER (WHERE role = 'm') AS mirror_count,
    CASE
        WHEN COUNT(*) FILTER (WHERE role = 'p') = 1
         AND COUNT(*) FILTER (WHERE role = 'm') >= 1
            THEN '正常'
        ELSE '需檢查'
    END AS pair_status
FROM gp_segment_configuration
WHERE content >= 0
GROUP BY content
ORDER BY content;

-- =============================================
-- 4.5 基礎操作練習
-- =============================================
\echo ''
\echo '>>> 4.5 基礎操作練習'

-- 列出所有資料庫
\echo '--- 現有資料庫 ---'
SELECT datname, pg_size_pretty(pg_database_size(datname)) AS size
FROM pg_database
WHERE datistemplate = FALSE
ORDER BY datname;

-- 列出目前使用者
\echo ''
\echo '--- 目前使用者 ---'
SELECT current_user, current_database(), inet_server_addr(), inet_server_port();

-- 列出所有 Schema
\echo ''
\echo '--- 現有 Schema ---'
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name NOT LIKE 'pg_%'
  AND schema_name != 'information_schema'
ORDER BY schema_name;

-- =============================================
-- 4.6 建立測試資料庫（後續教學使用）
-- =============================================
\echo ''
\echo '>>> 4.6 建立測試資料庫'

-- 如果已存在就先刪除
DROP DATABASE IF EXISTS cloudberry_poc;
CREATE DATABASE cloudberry_poc;

\echo '  資料庫 cloudberry_poc 已建立。'
\echo ''
\echo '  後續操作請先切換到 poc 資料庫：'
\echo '    psql -d cloudberry_poc'
\echo ''
\echo '>>> 連線驗證全部完成！'
