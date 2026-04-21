-------------------------------------------------------------------------------
-- 20-pg-vs-cloudberry-sp.sql
-- 用途：系統性對比 PostgreSQL 與 Cloudberry 在 Stored Procedure 上的關鍵差異
-- 場景：同樣的商業邏輯，在單節點 PG 能跑，在 MPP 環境下需要改寫
-- 執行方式：psql -d cloudberry_poc -f 20-pg-vs-cloudberry-sp.sql
-------------------------------------------------------------------------------

\echo '============================================================'
\echo '  PostgreSQL vs Cloudberry：Stored Procedure 關鍵差異'
\echo '============================================================'
\echo ''
\echo '  Cloudberry 相容 PostgreSQL 語法，但 MPP 分散式架構'
\echo '  對 SP 的執行模型、交易管理、效能特性有根本性差異。'
\echo '  以下���一展示差異，並提供 MPP 正確寫法。'

-- =============================================
-- 準備測試資料
-- =============================================
\echo ''
\echo '=========================================='
\echo '  準備測試資料'
\echo '=========================================='

DROP TABLE IF EXISTS sp_demo_orders CASCADE;
DROP TABLE IF EXISTS sp_demo_items CASCADE;
DROP TABLE IF EXISTS sp_demo_summary CASCADE;
DROP TABLE IF EXISTS sp_demo_audit_log CASCADE;
DROP TABLE IF EXISTS sp_demo_dim_status CASCADE;

CREATE TABLE sp_demo_orders (
    order_id     BIGINT,
    customer_id  INT,
    status       VARCHAR(20),
    amount       DECIMAL(15,2),
    order_date   DATE,
    processed    BOOLEAN DEFAULT FALSE
) DISTRIBUTED BY (order_id);

CREATE TABLE sp_demo_items (
    item_id      BIGSERIAL,
    order_id     BIGINT,
    product_id   INT,
    qty          INT,
    unit_price   DECIMAL(10,2)
) DISTRIBUTED BY (order_id);    -- 與 orders 相同分佈鍵

CREATE TABLE sp_demo_summary (
    summary_date DATE,
    total_amount DECIMAL(20,2),
    order_count  BIGINT,
    updated_at   TIMESTAMP DEFAULT NOW()
) DISTRIBUTED BY (summary_date);

CREATE TABLE sp_demo_audit_log (
    log_id       BIGSERIAL,
    action       TEXT,
    detail       TEXT,
    created_at   TIMESTAMP DEFAULT NOW()
) DISTRIBUTED BY (log_id);

-- 狀態維度表（小表 → REPLICATED）
CREATE TABLE sp_demo_dim_status (
    status_code  VARCHAR(20),
    status_name  VARCHAR(50),
    is_final     BOOLEAN
) DISTRIBUTED REPLICATED;

INSERT INTO sp_demo_dim_status VALUES
    ('PENDING',    '待處理',   FALSE),
    ('PROCESSING', '處理中',   FALSE),
    ('SHIPPED',    '已出貨',   FALSE),
    ('DELIVERED',  '已送達',   TRUE),
    ('CANCELLED',  '已取消',   TRUE);

INSERT INTO sp_demo_orders
SELECT g, (random() * 10000)::INT,
       CASE (g % 5)
           WHEN 0 THEN 'PENDING' WHEN 1 THEN 'PROCESSING'
           WHEN 2 THEN 'SHIPPED' WHEN 3 THEN 'DELIVERED' ELSE 'CANCELLED'
       END,
       ROUND((random() * 1000 + 10)::NUMERIC, 2),
       '2024-01-01'::DATE + (g % 365),
       FALSE
FROM generate_series(1, 200000) g;

INSERT INTO sp_demo_items (order_id, product_id, qty, unit_price)
SELECT (random() * 199999 + 1)::BIGINT, (random() * 500)::INT,
       (random() * 5 + 1)::INT, ROUND((random() * 200 + 5)::NUMERIC, 2)
FROM generate_series(1, 600000) g;

ANALYZE sp_demo_orders;
ANALYZE sp_demo_items;

\echo '  已建立 200K 訂單 / 600K ��細'

-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  差異 1：CURSOR 逐行處理 vs SET-BASED 操作                    ║
-- ╚═══════════════════════════════════════════════════════════════╝
\echo ''
\echo '=========================================='
\echo '  差異 1：CURSOR 逐行 vs SET-BASED'
\echo '=========================================='
\echo ''
\echo '  PostgreSQL：CURSOR 逐行處理雖慢但可接受'
\echo '  Cloudberry：CURSOR 每次 FETCH 都從各 Segment 拉一行'
\echo '              → 完全破壞 MPP 並行性，效能災難'

-- ❌ PG 風格：CURSOR 逐行更新（在 Cloudberry 上極慢）
\echo ''
\echo '--- ❌ PG 風格：CURSOR 逐行更新 ---'

CREATE OR REPLACE FUNCTION fn_cursor_row_by_row(p_limit INT DEFAULT 1000)
RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE
    rec    RECORD;
    v_cnt  BIGINT := 0;
    cur    CURSOR FOR
           SELECT order_id FROM sp_demo_orders
           WHERE processed = FALSE
           LIMIT p_limit;
BEGIN
    OPEN cur;
    LOOP
        FETCH cur INTO rec;
        EXIT WHEN NOT FOUND;
        -- 每次 FETCH：Coordinator 從某個 Segment 拉 1 行
        -- 每次 UPDATE：再分發到對應 Segment
        UPDATE sp_demo_orders SET processed = TRUE
        WHERE order_id = rec.order_id;
        v_cnt := v_cnt + 1;
    END LOOP;
    CLOSE cur;
    RETURN v_cnt;
END;
$$;

\timing on
SELECT fn_cursor_row_by_row(1000) AS rows_updated_cursor;
\timing off

-- 還原
UPDATE sp_demo_orders SET processed = FALSE WHERE processed = TRUE;

-- ✅ Cloudberry 正確���法：SET-BASED 批量更新
\echo ''
\echo '--- ✅ Cloudberry 寫法：SET-BASED 批量更新 ---'

CREATE OR REPLACE FUNCTION fn_set_based_update(p_limit INT DEFAULT 1000)
RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE
    v_cnt BIGINT;
BEGIN
    -- 整個 UPDATE 被分發到所有 Segment 並行執行
    WITH target AS (
        SELECT order_id FROM sp_demo_orders
        WHERE processed = FALSE
        LIMIT p_limit
    )
    UPDATE sp_demo_orders o
    SET processed = TRUE
    FROM target t
    WHERE o.order_id = t.order_id;

    GET DIAGNOSTICS v_cnt = ROW_COUNT;
    RETURN v_cnt;
END;
$$;

\timing on
SELECT fn_set_based_update(1000) AS rows_updated_set_based;
\timing off

-- 還原
UPDATE sp_demo_orders SET processed = FALSE WHERE processed = TRUE;

\echo ''
\echo '  結論：SET-BASED 比 CURSOR 快數十倍以上'
\echo '  原因：CURSOR 序列化處理，SET-BASED 所有 Segment 並行'

-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  差異 2：COMMIT 在 EXCEPTION block 中的行為                   ║
-- ╚═══════════════════════════════════════════════════════════════╝
\echo ''
\echo '=========================================='
\echo '  差異 2：COMMIT 與 EXCEPTION block'
\echo '=========================================='
\echo ''
\echo '  PostgreSQL：EXCEPTION block 中可��� COMMIT'
\echo '  Cloudberry：EXCEPTION 建立 subtransaction，'
\echo '              其中不能 COMMIT → 報錯'

-- ❌ PG 風格：EXCEPTION block 中 COMMIT
\echo ''
\echo '--- ❌ PG 風格：COMMIT 在 EXCEPTION block 中 ---'

CREATE OR REPLACE PROCEDURE sp_pg_style_with_exception()
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO sp_demo_audit_log (action, detail)
    VALUES ('TEST', 'PG style with EXCEPTION');

    COMMIT;    -- ← 在 Cloudberry 會失敗！

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '錯誤: %', SQLERRM;
END;
$$;

\echo '  呼叫 sp_pg_style_with_exception()...'
DO $$
BEGIN
    CALL sp_pg_style_with_exception();
    RAISE NOTICE '  → 執行成功（不應該到這裡）';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '  → 預期的錯誤: %', SQLERRM;
END;
$$;

-- ✅ Cloudberry 正��寫法 A：移除 EXCEPTION，讓 PROCEDURE 自動 COMMIT
\echo ''
\echo '--- ✅ Cloudberry 寫法 A：無 EXCEPTION，自動 COMMIT ---'

CREATE OR REPLACE PROCEDURE sp_cloudberry_no_exception()
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO sp_demo_audit_log (action, detail)
    VALUES ('TEST', 'Cloudberry style - no exception');

    -- PROCEDURE 結束時自動 COMMIT
    RAISE NOTICE '  → 執行成功（自動 COMMIT）';
END;
$$;

CALL sp_cloudberry_no_exception();

-- ✅ Cloudberry 正確寫法 B：EXCEPTION 中不做 COMMIT，只記錄錯誤
\echo ''
\echo '--- ✅ Cloudberry 寫法 B：EXCEPTION 中只 RAISE，不 COMMIT ---'

CREATE OR REPLACE PROCEDURE sp_cloudberry_safe_exception()
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO sp_demo_audit_log (action, detail)
    VALUES ('TEST', 'Cloudberry safe exception');

    -- 模擬錯誤
    -- PERFORM 1/0;

    RAISE NOTICE '  → 執行成功';
EXCEPTION WHEN OTHERS THEN
    -- 不做 COMMIT！只 re-raise 或記錄
    RAISE NOTICE '  → 錯誤: %（交易自動 ROLLBACK）', SQLERRM;
    RAISE;   -- re-raise 讓呼叫方處理
END;
$$;

CALL sp_cloudberry_safe_exception();

\echo ''
\echo '  驗證 audit_log 有記錄：'
SELECT action, detail, created_at FROM sp_demo_audit_log ORDER BY log_id DESC LIMIT 3;

\echo ''
\echo '  結論：'
\echo '  - Cloudberry PROCEDURE 結束時自動 COMMIT'
\echo '  - EXCEPTION block 中不能 COMMIT（subtransaction 限制）'
\echo '  - 如需錯誤處理，在 EXCEPTION 中只做 RAISE，不做 DML + COMMIT'

-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  差異 3：VOLATILE 函數在 WHERE 條件中的行為                   ║
-- ╚═══════════════════════════════════════════════════════════════╝
\echo ''
\echo '=========================================='
\echo '  差異 3：VOLATILE 函數在 WHERE 中'
\echo '=========================================='
\echo ''
\echo '  PostgreSQL：WHERE sale_date > get_cutoff() → 呼叫 1 次'
\echo '  Cloudberry：每個 Segment 各呼叫 1 次 → N 次呼叫'
\echo '              若函數有副作用（INSERT 等）→ 結果不可預期'

-- 建立一個有副作用的 VOLATILE 函數
CREATE OR REPLACE FUNCTION fn_volatile_with_side_effect()
RETURNS DATE
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    -- 副作用：每次呼叫都寫入日誌
    INSERT INTO sp_demo_audit_log (action, detail)
    VALUES ('VOLATILE_CALL', 'Called at ' || clock_timestamp());
    RETURN '2024-06-01'::DATE;
END;
$$;

-- 清空日誌
TRUNCATE sp_demo_audit_log;

-- ❌ PG 風格：直接在 WHERE 中呼叫
\echo ''
\echo '--- ❌ PG 風格：VOLATILE 函數直接在 WHERE 中 ---'

SELECT COUNT(*) AS order_count
FROM sp_demo_orders
WHERE order_date > fn_volatile_with_side_effect();

\echo ''
\echo '  VOLATILE 函數被呼叫了幾次？'
SELECT COUNT(*) AS call_count FROM sp_demo_audit_log;
\echo '  → 在 3 Segment 叢集中，被呼叫 3 次以上（非 1 次）！'

-- ✅ Cloudberry 正確寫法：先用 subquery 取值
TRUNCATE sp_demo_audit_log;

\echo ''
\echo '--- ✅ Cloudberry 寫法：先用 subquery 取值 ---'

SELECT COUNT(*) AS order_count
FROM sp_demo_orders
WHERE order_date > (SELECT fn_volatile_with_side_effect());

\echo ''
\echo '  VOLATILE 函數被呼叫了幾次？'
SELECT COUNT(*) AS call_count FROM sp_demo_audit_log;
\echo '  → 只呼叫 1 次（在 Coordinator 的 subquery 中）'

\echo ''
\echo '  結論：VOLATILE 函數放在 subquery 中，確保只執行一次'

-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  差異 4：JOIN 效能 — 分佈鍵決定一切                           ║
-- ╚═══════════════════════════════════════════════════════════════╝
\echo ''
\echo '=========================================='
\echo '  差異 4：JOIN 效能取決於分佈鍵'
\echo '=========================================='
\echo ''
\echo '  PostgreSQL：JOIN 效能只取決於索引和資料量'
\echo '  Cloudberry：分佈鍵不一致 → Redistribute Motion（網路傳輸）'
\echo '              分佈鍵一致   → Co-located Join（本地 JOIN，最快）'

-- 4A：Co-located Join（兩表都以 order_id 分佈）
\echo ''
\echo '--- 4A：Co-located Join（同分佈鍵）---'
\echo '  orders DISTRIBUTED BY (order_id)'
\echo '  items  DISTRIBUTED BY (order_id)'
\echo '  → 同一 order_id 的資料在同一 Segment → 本地 JOIN'

EXPLAIN
SELECT o.order_id, o.amount, SUM(i.qty * i.unit_price) AS item_total
FROM sp_demo_orders o
JOIN sp_demo_items i ON o.order_id = i.order_id
WHERE o.order_date = '2024-06-15'
GROUP BY o.order_id, o.amount;

-- 4B：模擬分佈鍵不一致的情況
\echo ''
\echo '--- 4B：Redistribute Motion（不同分佈鍵）---'
\echo '  如果 items 以 product_id 分佈（與 orders 不同）'
\echo '  → JOIN 時必須 Redistribute 其中一張表'

DROP TABLE IF EXISTS sp_demo_items_bad CASCADE;
CREATE TABLE sp_demo_items_bad (
    item_id      BIGSERIAL,
    order_id     BIGINT,
    product_id   INT,
    qty          INT,
    unit_price   DECIMAL(10,2)
) DISTRIBUTED BY (product_id);    -- ← 錯誤：與 orders 不同分佈鍵

INSERT INTO sp_demo_items_bad
SELECT item_id, order_id, product_id, qty, unit_price FROM sp_demo_items;
ANALYZE sp_demo_items_bad;

EXPLAIN
SELECT o.order_id, o.amount, SUM(i.qty * i.unit_price) AS item_total
FROM sp_demo_orders o
JOIN sp_demo_items_bad i ON o.order_id = i.order_id
WHERE o.order_date = '2024-06-15'
GROUP BY o.order_id, o.amount;

\echo ''
\echo '  對比觀察：'
\echo '  - Co-located：EXPLAIN 中無 Redistribute Motion'
\echo '  - 不一致：EXPLAIN 中出現 Redistribute Motion 3:3'
\echo '  → Redistribute = 跨網路傳輸，大表時效能差距可達 10x 以上'

-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  差異 5：EXECUTE ON — Cloudberry 獨有功能                     ║
-- ╚═══════════════════════════════════════════════════════════════╝
\echo ''
\echo '=========================================='
\echo '  差異 5：EXECUTE ON（Cloudberry 獨有）'
\echo '=========================================='
\echo ''
\echo '  PostgreSQL：函數只在單一節點執行，無此概念'
\echo '  Cloudberry：可控制函數在 Coordinator / All Segments / Any 執行'

-- 查看現有函數的執行位置
\echo ''
\echo '--- 函數執行位置 ---'
SELECT
    p.proname AS function_name,
    CASE p.provolatile
        WHEN 'v' THEN 'VOLATILE'
        WHEN 's' THEN 'STABLE'
        WHEN 'i' THEN 'IMMUTABLE'
    END AS volatility,
    CASE p.proexeclocation
        WHEN 'a' THEN 'ANY（Planner 決定）'
        WHEN 'c' THEN 'COORDINATOR ONLY'
        WHEN 's' THEN 'ALL SEGMENTS'
    END AS exec_location
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname = 'public'
  AND p.proname LIKE 'fn_%'
ORDER BY p.proname;

-- EXECUTE ON COORDINATOR：只在 Coordinator 執行（適合呼叫外部 API、讀取設定）
\echo ''
\echo '--- EXECUTE ON COORDINATOR ---'

-- EXECUTE ON COORDINATOR 僅支援 set-returning function
CREATE OR REPLACE FUNCTION fn_coordinator_only()
RETURNS TABLE (info TEXT)
EXECUTE ON COORDINATOR
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY SELECT ('I run only on Coordinator, PID=' || pg_backend_pid())::TEXT;
END;
$$;

SELECT * FROM fn_coordinator_only();

-- EXECUTE ON ALL SEGMENTS：每個 Segment 各執行一次（適合收集本地統計）
\echo ''
\echo '--- EXECUTE ON ALL SEGMENTS ---'

CREATE OR REPLACE FUNCTION fn_all_segments()
RETURNS TABLE (seg_id INT, local_table_count BIGINT)
EXECUTE ON ALL SEGMENTS
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT
        gp_execution_segment()::INT,
        COUNT(*)::BIGINT
    FROM pg_class
    WHERE relkind = 'r' AND relname NOT LIKE 'pg_%';
END;
$$;

\echo '  每個 Segment 的本地使用者表數量：'
SELECT * FROM fn_all_segments() ORDER BY seg_id;

-- EXECUTE ON INITPLAN：在 Coordinator subquery 中執行（避免多次呼叫）
\echo ''
\echo '--- EXECUTE ON INITPLAN（透過 subquery 實現）---'

CREATE OR REPLACE FUNCTION fn_get_config_value(p_key TEXT)
RETURNS TEXT
LANGUAGE plpgsql STABLE AS $$
BEGIN
    -- 模擬讀取設定表（只需在 Coordinator 執行一次）
    RETURN CASE p_key
        WHEN 'cutoff_date' THEN '2024-06-01'
        WHEN 'min_amount'  THEN '100'
        ELSE NULL
    END;
END;
$$;

\echo '  ❌ 直接在 WHERE 中 → 每 Segment 各呼叫一次：'
EXPLAIN SELECT COUNT(*) FROM sp_demo_orders
WHERE order_date > fn_get_config_value('cutoff_date')::DATE;

\echo ''
\echo '  ✅ 放在 subquery → Coordinator 只呼叫一次：'
EXPLAIN SELECT COUNT(*) FROM sp_demo_orders
WHERE order_date > (SELECT fn_get_config_value('cutoff_date')::DATE);

-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  差異 6：TEMP TABLE 與 DDL 在 FUNCTION 中的限制               ║
-- ╚═══════════════════════════════════════════════════════════════╝
\echo ''
\echo '=========================================='
\echo '  差異 6：TEMP TABLE / DDL 在 FUNCTION 中'
\echo '=========================================='
\echo ''
\echo '  PostgreSQL：FUNCTION 中可自由 CREATE TEMP TABLE'
\echo '  Cloudberry：FUNCTION 中執行 DDL 需要全叢集鎖'
\echo '              → 並行度下降，可能造成鎖等待'
\echo '              → 改用 PROCEDURE 或在外部預建暫存表'

-- ✅ Cloudberry 推薦：用 PROCEDURE + COMMIT 做需要 DDL 的操作
\echo ''
\echo '--- ✅ 用 PROCEDURE 處理需要暫存表的 ETL ---'

CREATE OR REPLACE PROCEDURE sp_etl_with_temp_table(p_date DATE)
LANGUAGE plpgsql AS $$
BEGIN
    -- PROCEDURE 中可以安全執行 DDL + COMMIT
    DROP TABLE IF EXISTS tmp_daily_calc;
    CREATE TEMP TABLE tmp_daily_calc AS
    SELECT
        order_id,
        amount,
        amount * 0.05 AS tax
    FROM sp_demo_orders
    WHERE order_date = p_date
    DISTRIBUTED BY (order_id);

    -- 用暫存表做後續計算
    INSERT INTO sp_demo_summary (summary_date, total_amount, order_count)
    SELECT p_date, SUM(amount + tax), COUNT(*)
    FROM tmp_daily_calc;

    DROP TABLE IF EXISTS tmp_daily_calc;

    RAISE NOTICE '  → 日期 % 彙總完成', p_date;
END;
$$;

CALL sp_etl_with_temp_table('2024-06-15');

\echo '  彙總結果：'
SELECT * FROM sp_demo_summary WHERE summary_date = '2024-06-15';

-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  差異 7：REPLICATED 表在 SP 中的優勢                          ║
-- ╚═══════════════════════════════════════════════════════════════╝
\echo ''
\echo '=========================================='
\echo '  差異 7：REPLICATED 表消除 Motion'
\echo '=========================================='
\echo ''
\echo '  PostgreSQL：無此概念（單節點無資料分佈問題）'
\echo '  Cloudberry：小型維度表用 REPLICATED → 每 Segment 本地有副本'
\echo '              → JOIN 時不需要 Broadcast 或 Redistribute'

\echo ''
\echo '--- REPLICATED 維度表 JOIN 事實表 ---'
EXPLAIN
SELECT o.order_id, o.amount, d.status_name, d.is_final
FROM sp_demo_orders o
JOIN sp_demo_dim_status d ON o.status = d.status_code
WHERE o.order_date = '2024-06-15';

\echo ''
\echo '  觀察：EXPLAIN 中沒有 Broadcast 或 Redistribute Motion'
\echo '  原因：dim_status 是 REPLICATED，每個 Segment 本地就有完整資料'

-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  總結對比表                                                    ║
-- ╚═══════════════════════════════════════════════════════════════╝
\echo ''
\echo '=========================================='
\echo '  總結：PostgreSQL vs Cloudberry SP 差異'
\echo '=========================================='
\echo ''
\echo '  ┌──────────────────────┬──────────────────────┬──────────────────────────────┐'
\echo '  │ 差異點                │ PostgreSQL（單節點）  │ Cloudberry（MPP）             │'
\echo '  ├──────────────────────┼──────────────────────┼──────────────────────────────┤'
\echo '  │ 1. CURSOR 逐行       │ 慢但可用             │ 效能災難（破壞並行）          │'
\echo '  │ 2. COMMIT+EXCEPTION  │ 可以                 │ 不行（subtransaction 限制）   │'
\echo '  │ 3. VOLATILE in WHERE │ 呼叫 1 次            │ 每 Segment 各呼叫 1 次        │'
\echo '  │ 4. JOIN 效能         │ 看索引               │ 看分佈鍵（Co-located 最快）   │'
\echo '  │ 5. EXECUTE ON        │ 無此功能             │ 控制 Coordinator/Segments     │'
\echo '  │ 6. DDL in FUNCTION   │ 自由使用             │ 全叢集鎖，改用 PROCEDURE      │'
\echo '  │ 7. REPLICATED 表     │ 無此概念             │ 小表複製，消除 JOIN Motion    │'
\echo '  └──────────────────────┴──────────────────────┴──────────────────────────────┘'
\echo ''
\echo '  MPP SP 黃金守則：'
\echo '  1. SET-BASED 優先，永遠不要 CURSOR 逐行'
\echo '  2. EXCEPTION 中不做 COMMIT / DML'
\echo '  3. VOLATILE 函數放 subquery，確保只呼叫一次'
\echo '  4. JOIN 的表用相同分佈鍵（Co-located）'
\echo '  5. 小型維度表用 DISTRIBUTED REPLICATED'
\echo '  6. 需要 DDL 的 ETL 用 PROCEDURE，不用 FUNCTION'

-- 清理
DROP TABLE IF EXISTS sp_demo_orders CASCADE;
DROP TABLE IF EXISTS sp_demo_items CASCADE;
DROP TABLE IF EXISTS sp_demo_items_bad CASCADE;
DROP TABLE IF EXISTS sp_demo_summary CASCADE;
DROP TABLE IF EXISTS sp_demo_audit_log CASCADE;
DROP TABLE IF EXISTS sp_demo_dim_status CASCADE;
DROP FUNCTION IF EXISTS fn_cursor_row_by_row(INT);
DROP FUNCTION IF EXISTS fn_set_based_update(INT);
DROP FUNCTION IF EXISTS fn_volatile_with_side_effect();
DROP FUNCTION IF EXISTS fn_coordinator_only();
DROP FUNCTION IF EXISTS fn_all_segments();
DROP FUNCTION IF EXISTS fn_get_config_value(TEXT);
DROP PROCEDURE IF EXISTS sp_pg_style_with_exception();
DROP PROCEDURE IF EXISTS sp_cloudberry_no_exception();
DROP PROCEDURE IF EXISTS sp_cloudberry_safe_exception();
DROP PROCEDURE IF EXISTS sp_etl_with_temp_table(DATE);

\echo ''
\echo '=========================================='
\echo '  PostgreSQL vs Cloudberry SP 差異測試通過'
\echo '=========================================='
