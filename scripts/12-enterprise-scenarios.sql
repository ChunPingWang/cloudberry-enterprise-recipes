-------------------------------------------------------------------------------
-- 12-enterprise-scenarios.sql
-- 對應教學：第 14 章 — 企業場景實戰範例
-- 用途：銀行帳務對帳、零售庫存預警等企業級 SP
-- 執行方式：psql -d cloudberry_poc -f 12-enterprise-scenarios.sql
-------------------------------------------------------------------------------

\echo '============================================'
\echo '  第 14 章：企業場景實戰範例'
\echo '============================================'

-- =============================================
-- 14.1 銀行場景：每日帳務對帳
-- =============================================
\echo ''
\echo '>>> 14.1 銀行場景：每日帳務對帳'

-- 建立帳戶餘額表
DROP TABLE IF EXISTS account_balances CASCADE;
CREATE TABLE account_balances (
    account_id      BIGINT,
    balance_date    DATE,
    end_balance     DECIMAL(20,4)
) DISTRIBUTED BY (account_id);

-- 建立每日交易表
DROP TABLE IF EXISTS daily_transactions CASCADE;
CREATE TABLE daily_transactions (
    txn_id          BIGSERIAL,
    account_id      BIGINT,
    txn_date        DATE,
    credit_amount   DECIMAL(20,4) DEFAULT 0,
    debit_amount    DECIMAL(20,4) DEFAULT 0
) DISTRIBUTED BY (account_id);

-- 建立核心系統快照表（模擬外部系統）
DROP TABLE IF EXISTS core_system_balance_snapshot CASCADE;
CREATE TABLE core_system_balance_snapshot (
    account_id      BIGINT,
    snapshot_date   DATE,
    balance         DECIMAL(20,4)
) DISTRIBUTED BY (account_id);

-- 建立對帳結果表
DROP TABLE IF EXISTS daily_reconciliation CASCADE;
CREATE TABLE daily_reconciliation (
    recon_date      DATE,
    account_id      BIGINT,
    expected_bal    DECIMAL(20,4),
    actual_bal      DECIMAL(20,4),
    diff            DECIMAL(20,4),
    status          VARCHAR(20),
    created_at      TIMESTAMP DEFAULT NOW()
)
WITH (appendoptimized=true, orientation=column, compresslevel=5)
DISTRIBUTED BY (account_id);

-- 插入模擬資料
INSERT INTO account_balances
SELECT g, '2024-03-14'::DATE,
       ROUND((random() * 100000)::NUMERIC, 4)
FROM generate_series(1, 5000) g;

INSERT INTO daily_transactions (account_id, txn_date, credit_amount, debit_amount)
SELECT
    (random() * 4999 + 1)::BIGINT,
    '2024-03-15'::DATE,
    CASE WHEN random() > 0.5 THEN ROUND((random() * 5000)::NUMERIC, 4) ELSE 0 END,
    CASE WHEN random() <= 0.5 THEN ROUND((random() * 3000)::NUMERIC, 4) ELSE 0 END
FROM generate_series(1, 20000) g;

-- 模擬核心系統快照（大部分與預期一致，少數有差異）
INSERT INTO core_system_balance_snapshot
SELECT
    ab.account_id,
    '2024-03-15'::DATE,
    ab.end_balance + COALESCE(t.today_net, 0)
    + CASE WHEN random() > 0.98 THEN (random() * 100 - 50) ELSE 0 END  -- 2% 故意加入差異
FROM account_balances ab
LEFT JOIN (
    SELECT account_id, SUM(credit_amount - debit_amount) AS today_net
    FROM daily_transactions
    WHERE txn_date = '2024-03-15'
    GROUP BY account_id
) t ON ab.account_id = t.account_id
WHERE ab.balance_date = '2024-03-14';

\echo '  模擬資料建立完成：5,000 帳戶、20,000 筆交易'

-- 建立對帳 SP
CREATE OR REPLACE PROCEDURE sp_daily_recon(p_recon_date DATE)
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM daily_reconciliation WHERE recon_date = p_recon_date;

    -- 批量對帳（MPP 並行比對）
    INSERT INTO daily_reconciliation
        (recon_date, account_id, expected_bal, actual_bal, diff, status)
    SELECT
        p_recon_date,
        COALESCE(l.account_id, r.account_id),
        COALESCE(l.expected_balance, 0),
        COALESCE(r.actual_balance, 0),
        COALESCE(r.actual_balance, 0) - COALESCE(l.expected_balance, 0),
        CASE
            WHEN l.account_id IS NULL THEN 'MISSING'
            WHEN ABS(COALESCE(r.actual_balance, 0) - l.expected_balance) < 0.01 THEN 'MATCH'
            ELSE 'MISMATCH'
        END
    FROM (
        SELECT account_id,
               end_balance + COALESCE(today_net, 0) AS expected_balance
        FROM account_balances ab
        LEFT JOIN (
            SELECT account_id, SUM(credit_amount - debit_amount) AS today_net
            FROM daily_transactions
            WHERE txn_date = p_recon_date
            GROUP BY account_id
        ) t USING (account_id)
        WHERE balance_date = p_recon_date - 1
    ) l
    FULL OUTER JOIN (
        SELECT account_id, balance AS actual_balance
        FROM core_system_balance_snapshot
        WHERE snapshot_date = p_recon_date
    ) r USING (account_id);

    RAISE NOTICE '對帳日期: % | MATCH: % | MISMATCH: % | MISSING: %',
        p_recon_date,
        (SELECT COUNT(*) FROM daily_reconciliation WHERE recon_date = p_recon_date AND status = 'MATCH'),
        (SELECT COUNT(*) FROM daily_reconciliation WHERE recon_date = p_recon_date AND status = 'MISMATCH'),
        (SELECT COUNT(*) FROM daily_reconciliation WHERE recon_date = p_recon_date AND status = 'MISSING');
END;
$$;

-- 執行對帳
CALL sp_daily_recon('2024-03-15');

-- 查看對帳摘要
\echo '  對帳摘要：'
SELECT status, COUNT(*) AS count
FROM daily_reconciliation
WHERE recon_date = '2024-03-15'
GROUP BY status
ORDER BY status;

-- 查看 MISMATCH 帳戶（最大差異前 10 筆）
\echo ''
\echo '  差異最大的前 10 筆：'
SELECT account_id, expected_bal, actual_bal,
       ROUND(diff, 4) AS diff
FROM daily_reconciliation
WHERE recon_date = '2024-03-15' AND status = 'MISMATCH'
ORDER BY ABS(diff) DESC
LIMIT 10;

-- =============================================
-- 14.2 零售場景：庫存預警
-- =============================================
\echo ''
\echo '>>> 14.2 零售場景：庫存預警'

-- 產品維度表
DROP TABLE IF EXISTS dim_product CASCADE;
CREATE TABLE dim_product (
    product_id   INT,
    product_name VARCHAR(100),
    category     VARCHAR(50)
) DISTRIBUTED REPLICATED;

INSERT INTO dim_product
SELECT g, 'Product ' || g,
       CASE (g % 5) WHEN 0 THEN 'Electronics' WHEN 1 THEN 'Clothing'
                     WHEN 2 THEN 'Food' WHEN 3 THEN 'Books' ELSE 'Others' END
FROM generate_series(1, 500) g;

-- 庫存表
DROP TABLE IF EXISTS inventory CASCADE;
CREATE TABLE inventory (
    product_id  INT,
    stock_qty   INT,
    updated_at  TIMESTAMP DEFAULT NOW()
) DISTRIBUTED BY (product_id);

INSERT INTO inventory
SELECT g, (random() * 500)::INT, NOW()
FROM generate_series(1, 500) g;

-- 庫存預警函數
CREATE OR REPLACE FUNCTION fn_inventory_alert(p_days_ahead INT DEFAULT 7)
RETURNS TABLE (
    product_id      INT,
    product_name    TEXT,
    current_stock   INT,
    daily_avg_sales DECIMAL(10,2),
    days_remaining  DECIMAL(10,1),
    alert_level     TEXT
)
LANGUAGE sql STABLE AS $$
    WITH recent_sales AS (
        SELECT
            product_id,
            SUM(1)::DECIMAL / GREATEST(COUNT(DISTINCT sale_date), 1) AS daily_avg
        FROM sales_fact
        WHERE sale_date >= CURRENT_DATE - 30
        GROUP BY product_id
    )
    SELECT
        p.product_id,
        p.product_name::TEXT,
        i.stock_qty,
        ROUND(COALESCE(rs.daily_avg, 0), 2),
        ROUND(i.stock_qty / NULLIF(rs.daily_avg, 0), 1),
        CASE
            WHEN i.stock_qty / NULLIF(rs.daily_avg, 0) <= p_days_ahead     THEN 'CRITICAL'
            WHEN i.stock_qty / NULLIF(rs.daily_avg, 0) <= p_days_ahead * 2 THEN 'WARNING'
            ELSE 'OK'
        END
    FROM dim_product p
    JOIN inventory i ON p.product_id = i.product_id
    LEFT JOIN recent_sales rs ON p.product_id = rs.product_id
    WHERE rs.daily_avg > 0
      AND i.stock_qty < rs.daily_avg * p_days_ahead * 3
    ORDER BY 5 ASC NULLS LAST;  -- days_remaining 欄位位置
$$;

\echo '  庫存預警（7 天內可能缺貨的商品）：'
SELECT * FROM fn_inventory_alert(7) LIMIT 15;

-- =============================================
-- 14.3 分散式帳戶轉帳
-- =============================================
\echo ''
\echo '>>> 14.3 分散式帳戶轉帳'

DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS transaction_log CASCADE;

CREATE TABLE accounts (
    account_id  BIGINT,
    balance     DECIMAL(20,4),
    updated_at  TIMESTAMP DEFAULT NOW()
) DISTRIBUTED BY (account_id);

CREATE TABLE transaction_log (
    from_account BIGINT,
    to_account   BIGINT,
    amount       DECIMAL(15,2),
    txn_time     TIMESTAMP DEFAULT NOW()
) DISTRIBUTED BY (from_account);

-- 建立帳戶
INSERT INTO accounts VALUES (1001, 50000.00, NOW()), (1002, 30000.00, NOW());

-- 轉帳 SP（使用兩階段提交）
CREATE OR REPLACE PROCEDURE sp_transfer_balance(
    p_from_account  BIGINT,
    p_to_account    BIGINT,
    p_amount        DECIMAL(15,2)
)
LANGUAGE plpgsql AS $$
DECLARE
    v_from_balance DECIMAL(15,2);
BEGIN
    SELECT balance INTO v_from_balance
    FROM accounts
    WHERE account_id = p_from_account
    FOR UPDATE;

    IF v_from_balance < p_amount THEN
        RAISE EXCEPTION '餘額不足：帳戶 % 餘額 % < 轉帳金額 %',
            p_from_account, v_from_balance, p_amount;
    END IF;

    UPDATE accounts SET balance = balance - p_amount, updated_at = NOW()
    WHERE account_id = p_from_account;

    UPDATE accounts SET balance = balance + p_amount, updated_at = NOW()
    WHERE account_id = p_to_account;

    INSERT INTO transaction_log (from_account, to_account, amount, txn_time)
    VALUES (p_from_account, p_to_account, p_amount, NOW());

    RAISE NOTICE '轉帳成功：% → % 金額 %', p_from_account, p_to_account, p_amount;
END;
$$;

\echo '  轉帳前餘額：'
SELECT * FROM accounts ORDER BY account_id;

CALL sp_transfer_balance(1001, 1002, 5000.00);

\echo '  轉帳後餘額：'
SELECT * FROM accounts ORDER BY account_id;

\echo '  轉帳記錄：'
SELECT * FROM transaction_log;

\echo ''
\echo '>>> 企業場景實戰完成！'
