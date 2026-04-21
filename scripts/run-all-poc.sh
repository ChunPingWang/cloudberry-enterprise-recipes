#!/bin/bash
###############################################################################
# run-all-poc.sh
# 用途：按順序執行所有 SQL PoC 腳本
# 執行方式：
#   在容器內：bash run-all-poc.sh
#   在容器外：docker exec -it cbdb-cdw bash -c "cd /scripts && bash run-all-poc.sh"
#
# 前提：
#   1. Sandbox 已啟動（docker exec -it cbdb-cdw /bin/bash）
#   2. 已建立 cloudberry_poc 資料庫（02 腳本會自動建立）
###############################################################################

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DB="cloudberry_poc"
HOST="${PGHOST:-localhost}"
PORT="${PGPORT:-5432}"
USER="${PGUSER:-gpadmin}"
LOG_DIR="/tmp/poc_logs"

mkdir -p "$LOG_DIR"

echo "============================================"
echo "  Apache Cloudberry PoC 全部執行"
echo "============================================"
echo "  資料庫: $DB"
echo "  日誌目錄: $LOG_DIR"
echo ""

# 記錄開始時間
START_TIME=$(date +%s)

run_sql() {
    local file="$1"
    local desc="$2"
    local db="${3:-$DB}"
    local log_file="$LOG_DIR/$(basename "$file" .sql).log"

    echo -n "  執行 $desc ... "

    if psql -h "$HOST" -p "$PORT" -U "$USER" -d "$db" \
            -f "$SCRIPT_DIR/$file" > "$log_file" 2>&1; then
        echo "✓ 完成"
    else
        echo "✗ 失敗（查看日誌: $log_file）"
    fi
}

# --- Step 1: 建立 PoC 資料庫 ---
echo "[Phase 1] 環境準備"
echo -n "  建立 cloudberry_poc 資料庫 ... "
psql -h "$HOST" -p "$PORT" -U "$USER" -d gpadmin \
    -c "DROP DATABASE IF EXISTS cloudberry_poc;" > /dev/null 2>&1
psql -h "$HOST" -p "$PORT" -U "$USER" -d gpadmin \
    -c "CREATE DATABASE cloudberry_poc;" > /dev/null 2>&1
echo "✓"
echo ""

# --- Step 2: 按章節執行 SQL 腳本 ---
echo "[Phase 2] 逐章節執行 PoC 腳本"
echo ""

run_sql "03-cluster-architecture.sql"   "Ch.5  叢集架構深度解析"
run_sql "07-data-distribution.sql"      "Ch.9  資料分佈策略"
run_sql "08-table-types.sql"            "Ch.10 表格類型選擇"
run_sql "09-performance-tuning.sql"     "Ch.11 效能調優"
run_sql "10-external-tables.sql"        "Ch.12 外部資料表"
run_sql "11-mpp-stored-procedures.sql"  "Ch.13 MPP Stored Procedures"
run_sql "12-enterprise-scenarios.sql"   "Ch.14 企業場景實戰"
run_sql "13-monitoring.sql"             "Ch.15 監控與診斷"
run_sql "14-bulk-import.sql"            "Ch.17 大量資料匯入"
run_sql "15-bulk-export.sql"            "Ch.18 大量資料匯出"
run_sql "16-error-handling-dq.sql"      "Ch.19 錯誤處理與資料品質"
run_sql "17-etl-pipeline.sql"           "Ch.20 企業級 ETL Pipeline"

echo ""

# --- Step 3: 摘要 ---
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo "============================================"
echo "  全部執行完成！"
echo "  總耗時: ${ELAPSED} 秒"
echo "  日誌目錄: $LOG_DIR"
echo "============================================"
echo ""
echo "  各腳本日誌："
ls -la "$LOG_DIR"/*.log 2>/dev/null | awk '{print "    " $NF}'
echo ""
echo "  快速驗證（在 psql 中執行）："
echo "    psql -d cloudberry_poc"
echo "    SELECT * FROM sales_daily_summary;"
echo "    SELECT * FROM etl_batch_log ORDER BY batch_id DESC LIMIT 5;"
echo "    SELECT * FROM dq_check_results ORDER BY check_id DESC LIMIT 10;"
