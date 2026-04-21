# Apache Cloudberry Sandbox PoC 驗證腳本

配合 `apache-cloudberry-sandbox-guide.md` 教學指南的完整 PoC 驗證腳本集。

## 快速開始

```bash
# 1. 檢查本機環境
bash scripts/00-prerequisites-check.sh

# 2. 安裝 Sandbox
bash scripts/01-sandbox-setup.sh

# 3. 進入容器
docker exec -it cbdb-cdw /bin/bash

# 4. 建立 PoC 資料庫並驗證連線
psql -f scripts/02-connect-and-verify.sql

# 5. 逐章執行或一鍵全跑
bash scripts/run-all-poc.sh
```

## 腳本清單

| 腳本 | 對應章節 | 類型 | 說明 |
|------|----------|------|------|
| `00-prerequisites-check.sh` | Ch.2 | Shell | 環境前置檢查 |
| `01-sandbox-setup.sh` | Ch.3 | Shell | Sandbox 安裝指引 |
| `02-connect-and-verify.sql` | Ch.4 | SQL | 連線驗證、建立 PoC DB |
| `03-cluster-architecture.sql` | Ch.5 | SQL | 叢集架構、EXPLAIN 分析 |
| `04-admin-tools.sh` | Ch.6 | Shell | 管理工具速查（參考） |
| `05-lifecycle-management.sh` | Ch.7 | Shell | 生命週期管理（參考） |
| `06-fault-tolerance.sh` | Ch.8 | Shell | 容錯與故障恢復（參考） |
| `07-data-distribution.sql` | Ch.9 | SQL | 三種分佈模式、傾斜診斷 |
| `08-table-types.sql` | Ch.10 | SQL | Heap/AO/AOCO/分區表 |
| `09-performance-tuning.sql` | Ch.11 | SQL | Resource Queue、GUC 調優 |
| `10-external-tables.sql` | Ch.12 | SQL | COPY、gpfdist 語法 |
| `11-mpp-stored-procedures.sql` | Ch.13 | SQL | MPP SP、Co-located Join |
| `12-enterprise-scenarios.sql` | Ch.14 | SQL | 銀行對帳、庫存預警、轉帳 |
| `13-monitoring.sql` | Ch.15 | SQL | 查詢監控、鎖偵測 |
| `14-bulk-import.sql` | Ch.17 | SQL | 大量匯入方式比較 |
| `15-bulk-export.sql` | Ch.18 | SQL | 大量匯出方式比較 |
| `16-error-handling-dq.sql` | Ch.19 | SQL | 錯誤隔離、DQ 檢查 SP |
| `17-etl-pipeline.sql` | Ch.20 | SQL | 企業級 ETL Pipeline |
| `18-cleanup.sql` | - | SQL | 清理所有測試物件 |
| `run-all-poc.sh` | - | Shell | 一鍵執行全部 SQL 腳本 |

## 執行方式

### SQL 腳本（在容器內 psql 執行）

```bash
# 單獨執行某章
psql -d cloudberry_poc -f scripts/07-data-distribution.sql

# 一鍵全跑
bash scripts/run-all-poc.sh
```

### Shell 腳本

- `00` / `01`：在本機執行（Docker 主機）
- `04` / `05` / `06`：在容器內執行，僅供閱讀參考

## 注意事項

- SQL 腳本之間有依賴關係，建議按編號順序執行
- `11-mpp-stored-procedures.sql` 會建立 `sales_fact`、`dim_region` 等基礎表，後續腳本依賴這些表
- Shell 腳本（04~06）部分指令會影響叢集狀態，標記為「參考」，請勿直接 `bash` 執行
- 清理：執行 `18-cleanup.sql` 或直接 `DROP DATABASE cloudberry_poc`
