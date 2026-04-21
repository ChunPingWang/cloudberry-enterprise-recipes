-------------------------------------------------------------------------------
-- 18-cleanup.sql
-- 用途：清理所有 PoC 腳本建立的物件
-- 執行方式：psql -d cloudberry_poc -f 18-cleanup.sql
-- ⚠️  此腳本會刪除所有測試資料！
-------------------------------------------------------------------------------

\echo '============================================'
\echo '  清理所有 PoC 測試物件'
\echo '============================================'

-- 刪除 Procedures
DROP PROCEDURE IF EXISTS sp_load_daily_sales(DATE, TEXT);
DROP PROCEDURE IF EXISTS sp_update_summary_correct();
DROP PROCEDURE IF EXISTS sp_transfer_balance(BIGINT, BIGINT, DECIMAL);
DROP PROCEDURE IF EXISTS sp_daily_recon(DATE);
DROP PROCEDURE IF EXISTS sp_etl_incremental_load(DATE, BOOLEAN);
DROP PROCEDURE IF EXISTS sp_export_monthly_summary(INT, INT);
DROP PROCEDURE IF EXISTS sp_run_dq_checks(TEXT);

-- 刪除 Functions
DROP FUNCTION IF EXISTS fn_get_order_total(BIGINT);
DROP FUNCTION IF EXISTS fn_monthly_sales_analysis(INT, INT);
DROP FUNCTION IF EXISTS fn_inventory_alert(INT);

-- 刪除表格
DROP TABLE IF EXISTS demo_sales CASCADE;
DROP TABLE IF EXISTS orders_hash CASCADE;
DROP TABLE IF EXISTS log_events_random CASCADE;
DROP TABLE IF EXISTS dim_region_replicated CASCADE;
DROP TABLE IF EXISTS orders_skewed CASCADE;
DROP TABLE IF EXISTS txn_heap CASCADE;
DROP TABLE IF EXISTS sales_ao CASCADE;
DROP TABLE IF EXISTS clickstream_aoco CASCADE;
DROP TABLE IF EXISTS sales_partitioned CASCADE;
DROP TABLE IF EXISTS dim_product_tmp CASCADE;
DROP TABLE IF EXISTS demo_export_data CASCADE;
DROP TABLE IF EXISTS import_target CASCADE;
DROP TABLE IF EXISTS sales_fact CASCADE;
DROP TABLE IF EXISTS sales_staging CASCADE;
DROP TABLE IF EXISTS sales_cleansed CASCADE;
DROP TABLE IF EXISTS sales_daily_summary CASCADE;
DROP TABLE IF EXISTS sales_summary CASCADE;
DROP TABLE IF EXISTS dim_region CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS inventory CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS transaction_log CASCADE;
DROP TABLE IF EXISTS account_balances CASCADE;
DROP TABLE IF EXISTS daily_transactions CASCADE;
DROP TABLE IF EXISTS core_system_balance_snapshot CASCADE;
DROP TABLE IF EXISTS daily_reconciliation CASCADE;
DROP TABLE IF EXISTS etl_load_log CASCADE;
DROP TABLE IF EXISTS etl_batch_log CASCADE;
DROP TABLE IF EXISTS etl_pipeline_config CASCADE;
DROP TABLE IF EXISTS dq_check_results CASCADE;

\echo ''
\echo '  所有測試物件已清理完畢。'
\echo '  如要刪除整個資料庫：'
\echo '    psql -d gpadmin -c "DROP DATABASE cloudberry_poc;"'
