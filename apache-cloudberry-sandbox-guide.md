# Apache Cloudberry Sandbox 完整教學指南
## 從入門到分散式 MPP Stored Procedure 進階開發

> **適用版本**：Apache Cloudberry 2.x（Incubating）  
> **環境**：Docker Sandbox（`apache/cloudberry` devops/sandbox）  
> **目標讀者**：資料工程師、DBA、架構師、企業應用開發人員

---

## 目錄

1. [Apache Cloudberry 簡介](#1-apache-cloudberry-簡介)
2. [環境前置需求](#2-環境前置需求)
3. [Sandbox 安裝與建置](#3-sandbox-安裝與建置)
4. [連線與基礎操作](#4-連線與基礎操作)
5. [叢集架構深度解析](#5-叢集架構深度解析)
6. [常用管理工具](#6-常用管理工具)
7. [叢集生命週期管理](#7-叢集生命週期管理)
8. [容錯與故障恢復](#8-容錯與故障恢復)
9. [資料分佈策略](#9-資料分佈策略)
10. [表格類型選擇指南](#10-表格類型選擇指南)
11. [進階用法：效能調優與工作負載管理](#11-進階用法效能調優與工作負載管理)
12. [進階用法：外部資料表與資料載入](#12-進階用法外部資料表與資料載入)
13. [分散式 MPP Stored Procedure 撰寫技巧](#13-分散式-mpp-stored-procedure-撰寫技巧)
14. [企業場景實戰範例](#14-企業場景實戰範例)
15. [監控與診斷](#15-監控與診斷)
16. [最佳實踐總結](#16-最佳實踐總結)
17. [大量資料匯入完全指南](#17-大量資料匯入完全指南)
18. [大量資料匯出完全指南](#18-大量資料匯出完全指南)
19. [錯誤處理與資料品質管控](#19-錯誤處理與資料品質管控)
20. [企業級 ETL 匯入匯出 SP 整合範例](#20-企業級-etl-匯入匯出-sp-整合範例)

---

## 1. Apache Cloudberry 簡介

Apache Cloudberry（孵化中）是一款以 PostgreSQL 和 Greenplum 為基礎發展的開源 **MPP（Massively Parallel Processing，大規模並行處理）資料倉儲引擎**，專為高效能大規模資料分析設計。

### 核心特性

| 特性 | 說明 |
|------|------|
| MPP 平行處理 | 查詢自動拆分至所有 Segment 節點並行執行 |
| 相容 PostgreSQL | 支援標準 SQL、PL/pgSQL、PL/Python 等擴展 |
| 多種儲存格式 | Heap、AO（Append-Optimized）、AOCO（列式壓縮） |
| 內建容錯機制 | FTS 故障偵測、Mirror 同步複寫、WAL Replication |
| 彈性擴充 | 支援線上 Expand 增加節點 |
| 工作負載管理 | Resource Queue 資源隔離與優先權控制 |

### 與傳統資料庫的關鍵差異

```
傳統 RDBMS（單節點）：
  Client → 單一 DB → 單一執行緒/少量並行

Apache Cloudberry MPP：
  Client → Coordinator（調度節點）
                ├── Segment 0（Primary + Mirror）
                ├── Segment 1（Primary + Mirror）
                ├── Segment 2（Primary + Mirror）
                └── Segment N（Primary + Mirror）
  每個 Segment 獨立執行查詢的一部分，結果匯回 Coordinator
```

---

## 2. 環境前置需求

### 最低硬體需求（Sandbox 開發環境）

| 資源 | 最低 | 建議 |
|------|------|------|
| CPU | 2 核心 | 4 核心以上 |
| 記憶體 | 4 GB | 8 GB 以上 |
| 磁碟空間 | 20 GB | 50 GB 以上 |

### 軟體需求

```bash
# 確認 Docker 已安裝並運行
docker --version        # >= 20.x
docker compose version  # >= 2.x

# 確認其他依賴
git --version
ssh -V
```

### 支援平台

- Linux（Ubuntu 20.04+、CentOS 8+、Rocky Linux 8/9）
- macOS（Apple Silicon M1/M2 支援）
- Windows（需透過 WSL2 + Docker Desktop）

> **企業環境注意**：若公司有 Docker Proxy 設定，需先在 Docker Desktop → Settings → Proxies 設定代理。

---

## 3. Sandbox 安裝與建置

### 3.1 取得原始碼

```bash
git clone https://github.com/apache/cloudberry.git
cd cloudberry/devops/sandbox
```

### 3.2 部署選項說明

```
./run.sh -h   # 查看所有選項
```

| 指令 | 說明 | 適用場景 |
|------|------|----------|
| `./run.sh -c local` | 使用當前本地原始碼編譯（單容器）| **開發者推薦** |
| `./run.sh -c local -m` | 使用本地原始碼（多容器叢集）| 分散式功能測試 |
| `./run.sh -c 2.0.0` | 使用指定 Release 版本（單容器）| 穩定版測試 |
| `./run.sh -c 2.0.0 -m` | 使用指定 Release 版本（多容器）| 生產環境模擬 |
| `./run.sh -c main` | 使用最新 main 分支（單容器）| 最新功能體驗 |

### 3.3 建置單容器 Sandbox（推薦初學者）

```bash
cd cloudberry/devops/sandbox

# 方式一：使用本地程式碼（最快）
./run.sh -c local

# 方式二：使用指定版本
./run.sh -c 2.0.0
```

建置過程約需 10~20 分鐘，完成後可看到 `Deployment Successful` 訊息。

### 3.4 建置多容器叢集（分散式測試推薦）

```bash
# 多容器部署（包含 Coordinator + 多個 Segment 節點）
./run.sh -c local -m

# 監控初始化進度
docker logs cbdb-cdw -f
# 等待出現 "Deployment Successful"
```

**多容器架構圖：**
```
┌─────────────────────────────────────────────────┐
│  Docker Network: cloudberry_network              │
│                                                  │
│  cbdb-cdw  (Coordinator + Standby)               │
│  cbdb-sdw1 (Segment Host 1 - Primary)            │
│  cbdb-sdw2 (Segment Host 2 - Primary)            │
│  cbdb-sdw3 (Segment Host 3 - Mirror)             │
└─────────────────────────────────────────────────┘
```

---

## 4. 連線與基礎操作

### 4.1 進入容器

```bash
# 連入 Coordinator 容器
docker exec -it cbdb-cdw /bin/bash

# 成功後看到提示符
[gpadmin@cdw /]$
```

### 4.2 連線資料庫

```bash
# 以預設 gpadmin 連線
[gpadmin@cdw ~]$ psql

# 指定資料庫
[gpadmin@cdw ~]$ psql -d mydb

# 從外部連線（需映射 Port）
psql -h localhost -p 5432 -U gpadmin -d gpadmin
```

### 4.3 確認版本與叢集狀態

```sql
-- 確認資料庫版本
SELECT VERSION();

-- 查看所有 Segment 狀態
SELECT * FROM gp_segment_configuration;

-- 快速健康檢查
SELECT dbid, content, role, preferred_role, mode, status, port, hostname
FROM gp_segment_configuration
ORDER BY content;
```

**輸出範例解讀：**

```
 dbid | content | role | preferred_role | mode | status | port  | hostname
------+---------+------+----------------+------+--------+-------+---------
    1 |      -1 | p    | p              | n    | u      |  5432 | cdw      ← Coordinator
    2 |       0 | p    | p              | s    | u      | 40000 | cdw      ← Segment 0 Primary
    3 |       0 | m    | m              | s    | u      | 41000 | cdw      ← Segment 0 Mirror
    4 |       1 | p    | p              | s    | u      | 40001 | cdw      ← Segment 1 Primary
    5 |       1 | m    | m              | s    | u      | 41001 | cdw      ← Segment 1 Mirror
```

| 欄位 | 說明 |
|------|------|
| `role = p` | 目前為 Primary |
| `role = m` | 目前為 Mirror |
| `mode = s` | 同步中（正常）|
| `mode = c` | Change tracking（Primary 正常，Mirror 異常）|
| `mode = r` | 恢復中 |
| `status = u` | 節點正常 |
| `status = d` | 節點異常 |

---

## 5. 叢集架構深度解析

### 5.1 Coordinator（協調節點）

- **角色**：所有客戶端的唯一連接點，負責 SQL 解析、查詢規劃、分發與結果彙整
- **不儲存使用者資料**（只儲存 Catalog/系統表）
- 監聽 Port：預設 `5432`
- 資料目錄：`/data0/database/coordinator/gpseg-1`

### 5.2 Segment（工作節點）

- **角色**：實際儲存資料並執行查詢計劃的分片
- 每個 Segment 有 Primary 和 Mirror 兩個實例
- 監聽 Port：從 `40000` 開始（Primary），`41000` 開始（Mirror）
- 資料目錄：`/data0/database/primary/gpsegN`

### 5.3 查詢執行流程

```
1. Client 送出 SQL → Coordinator
2. Coordinator 解析 SQL，生成分散式執行計劃
3. 計劃被拆分為多個 Slice，推送至各 Segment
4. 各 Segment 並行執行各自的 Slice
5. 中間結果透過 Interconnect（網路）在 Segment 間傳遞
6. 最終結果彙整至 Coordinator 回傳給 Client
```

### 5.4 查看執行計劃

```sql
-- 查看分散式執行計劃
EXPLAIN SELECT * FROM sales WHERE region = 'Asia';

-- 查看實際執行統計
EXPLAIN ANALYZE SELECT sum(amount) FROM sales GROUP BY region;
```

---

## 6. 常用管理工具

### 6.1 工具速查表

| 工具 | 功能 | 常用參數 |
|------|------|---------|
| `gpstart` | 啟動叢集 | `-a`（非互動式）、`-R`（受限模式）|
| `gpstop` | 停止叢集 | `-a`（非互動式）、`-M fast`（快速）、`-u`（重載設定）|
| `gpstate` | 查看狀態 | `-s`（詳細）、`-e`（只顯示錯誤）、`-f`（查詢 FTS）|
| `gpconfig` | 設定參數 | `-c <param> -v <value>`、`-r <param>`、`--show <param>` |
| `gprecoverseg` | 恢復 Segment | `-r`（rebalance）、`-F`（全量恢復）|
| `gpexpand` | 擴充叢集 | `-i <config>`、`-d <duration>`、`-c`（清理）|
| `gpaddmirrors` | 新增 Mirror | （互動式）|
| `gpcheckperf` | 效能測試 | `-r d`（I/O）、`-r n`（網路）、`-r s`（記憶體頻寬）|

### 6.2 常用操作範例

```bash
# 查看所有 GUC 參數
gpconfig -l

# 修改 work_mem（所有節點）
gpconfig -c work_mem -v '512MB'

# 只修改 Coordinator
gpconfig -c work_mem -v '512MB' --coordinatoronly

# 重載設定（不需重啟）
gpstop -u

# 查看目前參數值
gpconfig --show work_mem

# 效能測試
gpcheckperf -f hostfile -r ds -D -d /data0/database
```

---

## 7. 叢集生命週期管理

### 7.1 Docker 容器管理

```bash
# === 單容器操作 ===

# 停止（保留資料）
docker stop cbdb-cdw

# 刪除（移除資料）
docker rm -f cbdb-cdw

# 重新啟動停止的容器
docker start cbdb-cdw

# === 多容器操作 ===

# 停止（保留資料）
docker compose -f docker-compose-rockylinux9.yml stop

# 完全清除（含資料）
docker compose -f docker-compose-rockylinux9.yml down -v

# 重新啟動
docker compose -f docker-compose-rockylinux9.yml start
```

### 7.2 資料庫啟停（容器重啟後）

```bash
# 進入容器後手動啟動資料庫
docker exec -it cbdb-cdw /bin/bash
gpstart -a

# 停止資料庫（在容器內）
gpstop -a         # 等待連線結束（Smart 模式）
gpstop -M fast    # 快速停止
gpstop -M immediate  # 強制停止（危險，只在緊急時使用）
```

### 7.3 查看叢集狀態

```bash
gpstate             # 基本狀態
gpstate -s          # 詳細 Segment 狀態
gpstate -e          # 顯示錯誤狀態的 Segment
gpstate -f          # FTS 狀態

# 在 SQL 中查看
psql -c "SELECT * FROM gp_segment_configuration ORDER BY content;"
```

---

## 8. 容錯與故障恢復

### 8.1 Mirror 同步機制

```
正常狀態：Primary (u/s) ←→ Mirror (u/s)   ← 雙方同步

Mirror 故障：Primary (u/c) ←→ Mirror (d/s) ← Primary 切換至 change tracking 模式
Primary 故障：Mirror (d/s) → Mirror 升格為 Primary (u/c)
```

### 8.2 Segment 恢復步驟

```bash
# 步驟 1：確認哪個 Segment 異常
gpstate -e

# 步驟 2：查看設定歷史
psql -c "SELECT * FROM gp_configuration_history ORDER BY time DESC LIMIT 20;"

# 步驟 3：執行恢復
gprecoverseg           # 增量恢復（快）
gprecoverseg -F        # 全量恢復（慢，但更可靠）

# 步驟 4：觀察恢復進度
gpstate -e
gpstate -s | grep -i recovery

# 步驟 5：恢復後 rebalance（讓 Primary/Mirror 回到原始角色）
gprecoverseg -r

# 步驟 6：確認狀態
psql -c "SELECT * FROM gp_segment_configuration;"
```

### 8.3 Standby Coordinator 管理

```bash
# 建立 Standby Coordinator（scdw 為 standby 主機名）
gpinitstandby -s scdw

# 重新同步 Standby
gpinitstandby -n

# 移除 Standby
gpinitstandby -r

# 升格 Standby 為 Coordinator（在 standby 主機執行）
gpactivatestandby
```

---

## 9. 資料分佈策略

這是 MPP 效能調優的**最關鍵**環節，錯誤的分佈鍵會導致嚴重的資料傾斜。

### 9.1 三種分佈模式

```sql
-- 1. Hash 分佈（推薦：高基數、唯一性高的欄位）
CREATE TABLE orders (
    order_id    BIGINT,
    customer_id INT,
    amount      DECIMAL(15,2),
    order_date  DATE
) DISTRIBUTED BY (order_id);

-- 2. 隨機分佈（適合：無明顯 Join Key 的資料）
CREATE TABLE log_events (
    event_time  TIMESTAMP,
    event_type  VARCHAR(50),
    payload     TEXT
) DISTRIBUTED RANDOMLY;

-- 3. 複製分佈（只適合：小型維度表）
CREATE TABLE dim_region (
    region_id   INT,
    region_name VARCHAR(100)
) DISTRIBUTED REPLICATED;
```

### 9.2 資料傾斜診斷

```sql
-- 查看各 Segment 資料量分佈（偵測傾斜）
SELECT gp_segment_id, COUNT(*) AS row_count
FROM orders
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- 計算傾斜率（>20% 需考慮更換分佈鍵）
WITH segment_counts AS (
    SELECT gp_segment_id, COUNT(*) AS cnt
    FROM orders
    GROUP BY gp_segment_id
)
SELECT
    MAX(cnt) AS max_rows,
    MIN(cnt) AS min_rows,
    AVG(cnt) AS avg_rows,
    ROUND((MAX(cnt) - MIN(cnt)) * 100.0 / NULLIF(AVG(cnt), 0), 2) AS skew_pct
FROM segment_counts;
```

### 9.3 選擇分佈鍵的原則

```
選擇分佈鍵準則（優先順序）：
1. 唯一性高（接近主鍵）→ 保證均勻分佈
2. 常用於 JOIN 的欄位 → 讓 Co-located Join 成為可能
3. 避免 NULL 值多的欄位
4. 避免選擇性低的欄位（如 gender = M/F）

企業場景舉例：
- 訂單表：DISTRIBUTED BY (order_id)
- 客戶交易：DISTRIBUTED BY (customer_id)  ← 與客戶表同鍵，利於 Join
- 日誌表：DISTRIBUTED RANDOMLY
- 國家代碼表（10筆）：DISTRIBUTED REPLICATED
```

### 9.4 修改分佈鍵

```sql
-- 修改分佈鍵（會重新分佈資料，耗時）
ALTER TABLE orders SET DISTRIBUTED BY (customer_id);

-- 重新分佈資料（不改鍵，只重新均衡）
ALTER TABLE orders SET WITH (reorganize=true) DISTRIBUTED BY (order_id);
```

---

## 10. 表格類型選擇指南

### 10.1 三種表格類型比較

| 類型 | 適用場景 | 壓縮 | UPDATE/DELETE | 推薦 |
|------|----------|------|---------------|------|
| Heap | OLTP 類型、頻繁更新 | ✗ | ✓ | 小型查詢表 |
| AO（行式）| 大型事實表、批量載入 | ✓ | 有限支援 | **大型事實表** |
| AOCO（列式）| 聚合分析、寬表 | ✓✓ | 有限支援 | **分析查詢** |

### 10.2 建立語法範例

```sql
-- Heap 表（預設）
CREATE TABLE transactions_heap (
    txn_id      BIGINT,
    amount      DECIMAL(15,2),
    status      VARCHAR(20)
) DISTRIBUTED BY (txn_id);

-- AO 表（含壓縮）
CREATE TABLE sales_fact (
    sale_id     BIGINT,
    product_id  INT,
    customer_id INT,
    sale_date   DATE,
    amount      DECIMAL(15,2)
)
WITH (appendoptimized=true, compresslevel=5)
DISTRIBUTED BY (sale_id);

-- AOCO 表（列式壓縮，適合分析）
CREATE TABLE clickstream (
    user_id     BIGINT,
    page_url    TEXT,
    click_time  TIMESTAMP,
    session_id  VARCHAR(64),
    device_type VARCHAR(20)
)
WITH (appendoptimized=true, orientation=column, compresslevel=5)
DISTRIBUTED BY (user_id);

-- 查看表格屬性
\d+ sales_fact
```

### 10.3 分區表（Partition Table）

```sql
-- 按時間範圍分區（企業最常用模式）
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
    START ('2023-01-01') END ('2024-01-01') EVERY (INTERVAL '1 month'),
    DEFAULT PARTITION other
);

-- 查看分區定義
SELECT * FROM pg_partitions WHERE tablename = 'sales_partitioned';

-- 新增分區
ALTER TABLE sales_partitioned
ADD PARTITION p2024_q1
START ('2024-01-01') END ('2024-04-01');
```

---

## 11. 進階用法：效能調優與工作負載管理

### 11.1 Resource Queue 資源隔離

```sql
-- 建立高優先 Queue（用於批次作業）
CREATE RESOURCE QUEUE batch_queue
    WITH (
        ACTIVE_STATEMENTS = 5,
        PRIORITY = HIGH,
        MEMORY_LIMIT = '4GB'
    );

-- 建立低優先 Queue（用於探索性查詢）
CREATE RESOURCE QUEUE analyst_queue
    WITH (
        ACTIVE_STATEMENTS = 10,
        PRIORITY = LOW,
        MEMORY_LIMIT = '1GB',
        MAX_COST = 1e10     -- 限制大查詢
    );

-- 建立使用者並指派 Queue
CREATE USER batch_user WITH PASSWORD 'batch_pass';
ALTER ROLE batch_user RESOURCE QUEUE batch_queue;

CREATE USER analyst_user WITH PASSWORD 'analyst_pass';
ALTER ROLE analyst_user RESOURCE QUEUE analyst_queue;

-- 查看 Queue 狀態
SELECT * FROM gp_toolkit.gp_resqueue_status;

-- 查看等待中的查詢
SELECT * FROM gp_toolkit.gp_locks_on_resqueue;
```

### 11.2 重要 GUC 參數調優

```sql
-- 工作記憶體（影響 Sort/Hash Join）
SHOW work_mem;
SET work_mem = '512MB';  -- Session 級別

-- 平行 Slice 記憶體限制
SHOW gp_vmem_protect_limit;

-- 統計資訊採樣率
ALTER TABLE sales_fact SET (autovacuum_analyze_scale_factor = 0);
ANALYZE sales_fact;

-- 查看查詢記憶體使用
SELECT * FROM gp_toolkit.gp_resqueue_priority_statement;
```

### 11.3 EXPLAIN 計劃分析

```sql
-- 完整執行計劃（含實際時間）
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    p.product_name,
    SUM(s.amount) AS total_sales
FROM sales_fact s
JOIN dim_product p ON s.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_sales DESC
LIMIT 20;

-- 找出 Motion（資料重分佈）節點——這是效能瓶頸所在
-- Redistribute Motion：資料在 Segment 間重分佈（耗資源）
-- Broadcast Motion：小表廣播至所有 Segment（可接受）
-- Gather Motion：結果彙整至 Coordinator

-- 統計資訊更新
ANALYZE sales_fact;
```

---

## 12. 進階用法：外部資料表與資料載入

### 12.1 gpfdist 外部資料表（分散式高速載入）

```bash
# 在資料來源主機上啟動 gpfdist（每個 Segment 會並行讀取）
gpfdist -d /data/csv_files -p 8080 -l /tmp/gpfdist.log &
```

```sql
-- 建立外部資料表定義
CREATE EXTERNAL TABLE ext_sales_staging (
    sale_id     BIGINT,
    sale_date   DATE,
    product_id  INT,
    customer_id INT,
    amount      DECIMAL(15,2)
)
LOCATION ('gpfdist://data-server:8080/sales_*.csv')
FORMAT 'CSV' (HEADER DELIMITER ',')
ENCODING 'UTF8'
LOG ERRORS SEGMENT REJECT LIMIT 100 ROWS;

-- 並行載入資料（所有 Segment 同時從 gpfdist 讀取）
INSERT INTO sales_fact SELECT * FROM ext_sales_staging;

-- 查看載入錯誤
SELECT * FROM gp_read_error_log('ext_sales_staging');
```

### 12.2 COPY 指令載入

```bash
# 從 Coordinator 載入（適合小資料量）
psql -c "\COPY sales_fact FROM '/tmp/sales.csv' CSV HEADER"
```

### 12.3 PXF（Platform Extension Framework）

```sql
-- 連接 HDFS 外部資料（需 PXF 安裝）
CREATE EXTERNAL TABLE hdfs_sales (
    sale_id     BIGINT,
    amount      DECIMAL(15,2)
)
LOCATION ('pxf://data/warehouse/sales?PROFILE=hdfs:parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
```

---

## 13. 分散式 MPP Stored Procedure 撰寫技巧

> 這是與傳統單節點資料庫**最大的差異**。在 MPP 環境中撰寫 Stored Procedure 需要深刻理解資料分佈、Motion 機制以及 Segment 執行模型。

### 13.1 執行模式：Coordinator-only vs. Distributed

```sql
-- ❌ 錯誤觀念：預設所有 SP 都在每個 Segment 執行
-- ✅ 正確理解：
--   - SET SESSION AUTHORIZATION / DDL → 只在 Coordinator 執行
--   - DML (SELECT/INSERT/UPDATE/DELETE) → 計劃分發至各 Segment
--   - VOLATILE function with EXECUTE ON COORDINATOR → 僅限 Coordinator

-- 確認函數在哪裡執行
SELECT provolatile, proexeclocation
FROM pg_proc
WHERE proname = 'your_function_name';
-- proexeclocation:
--   'a' = ANY（預設，Planner 決定）
--   'c' = COORDINATOR（只在 Coordinator）
--   's' = ALL SEGMENTS（每個 Segment 都執行）
```

### 13.2 基礎：PL/pgSQL Stored Procedure 模板

```sql
-- 標準 MPP 友好的 Stored Procedure 模板
CREATE OR REPLACE PROCEDURE sp_load_daily_sales(
    p_load_date     DATE,
    p_source_table  TEXT DEFAULT 'ext_sales_staging'
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_row_count     BIGINT;
    v_start_time    TIMESTAMP := clock_timestamp();
    v_end_time      TIMESTAMP;
BEGIN
    -- 1. 記錄開始（在 Coordinator 執行）
    RAISE NOTICE 'Starting daily sales load for date: %', p_load_date;

    -- 2. 刪除當日舊資料（DML → 分發至所有 Segment）
    DELETE FROM sales_fact
    WHERE sale_date = p_load_date;

    -- 3. 插入新資料（利用並行執行）
    INSERT INTO sales_fact (sale_id, sale_date, product_id, customer_id, amount)
    SELECT
        sale_id,
        sale_date,
        product_id,
        customer_id,
        amount
    FROM ext_sales_staging
    WHERE sale_date = p_load_date;

    -- 4. 取得影響筆數（正確方式：用聚合函數，避免 Row 級操作）
    GET DIAGNOSTICS v_row_count = ROW_COUNT;

    -- 5. 更新載入記錄
    v_end_time := clock_timestamp();

    INSERT INTO etl_load_log (load_date, table_name, row_count, start_time, end_time, status)
    VALUES (p_load_date, 'sales_fact', v_row_count, v_start_time, v_end_time, 'SUCCESS');

    RAISE NOTICE 'Loaded % rows in % seconds',
        v_row_count,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time));

    COMMIT;

EXCEPTION WHEN OTHERS THEN
    -- 記錄錯誤
    INSERT INTO etl_load_log (load_date, table_name, row_count, start_time, end_time, status, error_msg)
    VALUES (p_load_date, 'sales_fact', 0, v_start_time, clock_timestamp(), 'FAILED', SQLERRM);

    RAISE EXCEPTION 'Daily load failed: %', SQLERRM;
END;
$$;

-- 呼叫方式
CALL sp_load_daily_sales('2024-01-15');
```

### 13.3 關鍵技巧：避免在 Segment 上執行 Coordinator-only 操作

```sql
-- ❌ 錯誤：在 SP 內部用 LOOP 逐行處理（破壞 MPP 並行性）
CREATE OR REPLACE FUNCTION bad_row_by_row_update()
RETURNS VOID AS $$
DECLARE
    rec RECORD;
BEGIN
    -- 這會強迫 Coordinator 逐行處理，完全放棄 MPP 優勢
    FOR rec IN SELECT sale_id, amount FROM sales_fact LOOP
        UPDATE sales_summary SET total = total + rec.amount
        WHERE id = 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- ✅ 正確：使用 SET-BASED 操作，讓 MPP 並行執行
CREATE OR REPLACE PROCEDURE sp_update_summary_correct()
LANGUAGE plpgsql AS $$
BEGIN
    -- 整個 UPDATE 會被分發至所有 Segment 並行執行
    UPDATE sales_summary s
    SET total_amount = agg.sum_amount,
        record_count = agg.cnt,
        updated_at   = NOW()
    FROM (
        SELECT
            region_id,
            SUM(amount)   AS sum_amount,
            COUNT(*)      AS cnt
        FROM sales_fact
        WHERE sale_date >= CURRENT_DATE - 30
        GROUP BY region_id
    ) agg
    WHERE s.region_id = agg.region_id;

    COMMIT;
END;
$$;
```

### 13.4 Segment-local 函數（EXECUTE ON ALL SEGMENTS）

```sql
-- 在每個 Segment 上各自執行的函數（用於 Segment-level 維護）
CREATE OR REPLACE FUNCTION fn_get_segment_stats()
RETURNS TABLE (
    segment_id  INT,
    table_name  TEXT,
    row_count   BIGINT,
    disk_bytes  BIGINT
)
EXECUTE ON ALL SEGMENTS  -- 每個 Segment 獨立執行
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT
        gp_execution_segment() AS segment_id,   -- 當前 Segment ID
        c.relname::TEXT,
        c.reltuples::BIGINT,
        pg_total_relation_size(c.oid) AS disk_bytes
    FROM pg_class c
    WHERE c.relkind = 'r'
      AND c.relname NOT LIKE 'pg_%'
    ORDER BY disk_bytes DESC
    LIMIT 10;
END;
$$;

-- 呼叫：結果會包含所有 Segment 的資料
SELECT * FROM fn_get_segment_stats() ORDER BY segment_id, disk_bytes DESC;
```

### 13.5 分散式交易管理

```sql
-- MPP 使用兩階段提交（2PC）處理分散式交易
-- 在 SP 中正確管理交易

CREATE OR REPLACE PROCEDURE sp_transfer_balance(
    p_from_account  BIGINT,
    p_to_account    BIGINT,
    p_amount        DECIMAL(15,2)
)
LANGUAGE plpgsql AS $$
DECLARE
    v_from_balance  DECIMAL(15,2);
BEGIN
    -- 在 MPP 中，這個 SELECT FOR UPDATE 會鎖定對應 Segment 上的行
    SELECT balance INTO v_from_balance
    FROM accounts
    WHERE account_id = p_from_account
    FOR UPDATE;

    IF v_from_balance < p_amount THEN
        RAISE EXCEPTION '餘額不足：帳戶 % 餘額 % < 轉帳金額 %',
            p_from_account, v_from_balance, p_amount;
    END IF;

    -- 更新來源帳戶（只更新對應 Segment 上的資料）
    UPDATE accounts
    SET balance    = balance - p_amount,
        updated_at = NOW()
    WHERE account_id = p_from_account;

    -- 更新目標帳戶（可能在不同 Segment）
    UPDATE accounts
    SET balance    = balance + p_amount,
        updated_at = NOW()
    WHERE account_id = p_to_account;

    -- 記錄交易
    INSERT INTO transaction_log (from_account, to_account, amount, txn_time)
    VALUES (p_from_account, p_to_account, p_amount, NOW());

    -- 兩階段提交：Coordinator 協調所有 Segment 一致提交
    COMMIT;

EXCEPTION WHEN OTHERS THEN
    ROLLBACK;
    RAISE;
END;
$$;
```

### 13.6 Co-located Join 最佳化

```sql
-- Co-located Join：兩個表用相同分佈鍵 JOIN → 無需 Motion，最快

-- 前提：orders 和 order_items 都以 order_id 分佈
CREATE TABLE orders (
    order_id    BIGINT,
    customer_id INT,
    order_date  DATE
) DISTRIBUTED BY (order_id);

CREATE TABLE order_items (
    item_id     BIGINT,
    order_id    BIGINT,   -- ← 與 orders 相同分佈鍵
    product_id  INT,
    qty         INT,
    unit_price  DECIMAL(10,2)
) DISTRIBUTED BY (order_id);  -- ← 必須相同！

-- 這個 SP 的 JOIN 不會產生 Redistribute Motion
CREATE OR REPLACE FUNCTION fn_get_order_total(p_order_id BIGINT)
RETURNS DECIMAL(15,2)
LANGUAGE sql STABLE AS $$
    SELECT SUM(qty * unit_price)
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id  -- Co-located！
    WHERE o.order_id = p_order_id;
$$;
```

### 13.7 聚合函數與 WINDOW 函數

```sql
-- 在 MPP 中，聚合分兩個階段：
-- 1. Partial Aggregate：在各 Segment 本地聚合
-- 2. Final Aggregate：在 Coordinator（或 Gather 節點）合併

CREATE OR REPLACE FUNCTION fn_monthly_sales_analysis(
    p_year  INT,
    p_month INT
)
RETURNS TABLE (
    region_name  TEXT,
    total_sales  DECIMAL(15,2),
    order_count  BIGINT,
    avg_order    DECIMAL(15,2),
    rank_by_sales INT
)
LANGUAGE sql STABLE AS $$
    WITH monthly_agg AS (
        SELECT
            r.region_name,
            SUM(s.amount)   AS total_sales,
            COUNT(*)        AS order_count,
            AVG(s.amount)   AS avg_order
        FROM sales_fact s
        JOIN dim_region r ON s.region_id = r.region_id  -- REPLICATED 表，無 Motion
        WHERE EXTRACT(YEAR  FROM s.sale_date) = p_year
          AND EXTRACT(MONTH FROM s.sale_date) = p_month
        GROUP BY r.region_name
        -- MPP：GROUP BY 在各 Segment 並行聚合後匯整
    )
    SELECT
        region_name,
        total_sales,
        order_count,
        ROUND(avg_order, 2),
        RANK() OVER (ORDER BY total_sales DESC)::INT AS rank_by_sales
    FROM monthly_agg
    ORDER BY total_sales DESC;
$$;
```

### 13.8 PL/Python 分散式函數（進階）

```sql
-- 安裝 PL/Python（Sandbox 已預裝）
CREATE EXTENSION IF NOT EXISTS plpython3u;

-- ⚠️ EXECUTE ON ALL SEGMENTS 搭配 PL/Python
-- 每個 Segment 上都會有一個 Python 解譯器實例

CREATE OR REPLACE FUNCTION fn_parse_json_payload(payload TEXT)
RETURNS TABLE (key TEXT, value TEXT)
EXECUTE ON ALL SEGMENTS    -- 在各 Segment 本地執行，無需 Motion
LANGUAGE plpython3u AS $$
    import json

    if not payload:
        return

    try:
        data = json.loads(payload)
        for k, v in data.items():
            yield (str(k), str(v))
    except json.JSONDecodeError:
        yield ('error', f'Invalid JSON: {payload[:50]}')
$$;

-- 使用：每個 Segment 在本地解析 JSON，完全並行
SELECT gp_segment_id, key, value
FROM clickstream,
     LATERAL fn_parse_json_payload(clickstream.event_payload)
LIMIT 100;
```

### 13.9 批次 ETL Procedure（企業實戰模板）

```sql
-- 完整的 ETL Stored Procedure，適合銀行/製造業/零售業資料倉儲

CREATE OR REPLACE PROCEDURE sp_etl_incremental_load(
    p_batch_date        DATE,
    p_batch_size        INT      DEFAULT 1000000,
    p_enable_logging    BOOLEAN  DEFAULT TRUE
)
LANGUAGE plpgsql AS $$
DECLARE
    v_batch_id      BIGINT;
    v_inserted_rows BIGINT  := 0;
    v_updated_rows  BIGINT  := 0;
    v_rejected_rows BIGINT  := 0;
    v_start_ts      TIMESTAMP := clock_timestamp();
BEGIN
    -- ── 1. 建立批次記錄 ──────────────────────────────
    IF p_enable_logging THEN
        INSERT INTO etl_batch_log (batch_date, status, started_at)
        VALUES (p_batch_date, 'RUNNING', v_start_ts)
        RETURNING batch_id INTO v_batch_id;
    END IF;

    -- ── 2. 資料品質過濾（Stage → Cleansed）────────────
    -- 利用 MPP 並行處理，直接 INSERT INTO ... SELECT
    INSERT INTO sales_cleansed (
        sale_id, sale_date, product_id, customer_id,
        amount, currency, batch_id
    )
    SELECT
        sale_id,
        sale_date,
        product_id,
        customer_id,
        CASE WHEN amount < 0 THEN NULL ELSE amount END,
        COALESCE(currency, 'USD'),
        v_batch_id
    FROM sales_staging
    WHERE sale_date       = p_batch_date
      AND sale_id         IS NOT NULL
      AND product_id      IS NOT NULL
      AND customer_id     IS NOT NULL;

    GET DIAGNOSTICS v_inserted_rows = ROW_COUNT;

    -- ── 3. Upsert 到事實表（MERGE 模式）───────────────
    -- MPP MERGE：先 UPDATE 再 INSERT（避免鎖表）
    UPDATE sales_fact f
    SET amount      = c.amount,
        updated_at  = NOW(),
        batch_id    = v_batch_id
    FROM sales_cleansed c
    WHERE f.sale_id   = c.sale_id
      AND c.batch_id  = v_batch_id;

    GET DIAGNOSTICS v_updated_rows = ROW_COUNT;

    -- 插入新增的行
    INSERT INTO sales_fact (sale_id, sale_date, product_id, customer_id, amount, batch_id)
    SELECT c.sale_id, c.sale_date, c.product_id, c.customer_id, c.amount, c.batch_id
    FROM sales_cleansed c
    WHERE c.batch_id = v_batch_id
      AND NOT EXISTS (
          SELECT 1 FROM sales_fact f WHERE f.sale_id = c.sale_id
      );

    -- ── 4. 更新彙整層（Summary Tables）───────────────
    -- 這個 UPDATE 利用 Co-located Join（如果 sales_fact 和 sales_daily_summary
    -- 都以 sale_date 的 hash 分佈，則無 Redistribute Motion）
    INSERT INTO sales_daily_summary (sale_date, total_amount, total_orders, last_updated)
    SELECT
        p_batch_date,
        SUM(amount),
        COUNT(*),
        NOW()
    FROM sales_fact
    WHERE sale_date = p_batch_date
    ON CONFLICT (sale_date)
    DO UPDATE SET
        total_amount  = EXCLUDED.total_amount,
        total_orders  = EXCLUDED.total_orders,
        last_updated  = EXCLUDED.last_updated;

    -- ── 5. 清理暫存資料 ──────────────────────────────
    DELETE FROM sales_staging WHERE sale_date = p_batch_date;
    DELETE FROM sales_cleansed WHERE batch_id = v_batch_id;

    -- ── 6. 更新批次紀錄 ──────────────────────────────
    IF p_enable_logging THEN
        UPDATE etl_batch_log
        SET status        = 'SUCCESS',
            finished_at   = clock_timestamp(),
            inserted_rows = v_inserted_rows,
            updated_rows  = v_updated_rows,
            rejected_rows = v_rejected_rows,
            duration_sec  = EXTRACT(EPOCH FROM (clock_timestamp() - v_start_ts))
        WHERE batch_id = v_batch_id;
    END IF;

    COMMIT;

    RAISE NOTICE '[ETL] Batch % 完成 | 日期: % | 插入: % 行 | 更新: % 行 | 耗時: % 秒',
        v_batch_id, p_batch_date, v_inserted_rows, v_updated_rows,
        ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - v_start_ts))::NUMERIC, 2);

EXCEPTION WHEN OTHERS THEN
    IF p_enable_logging AND v_batch_id IS NOT NULL THEN
        UPDATE etl_batch_log
        SET status      = 'FAILED',
            finished_at = clock_timestamp(),
            error_msg   = SQLERRM
        WHERE batch_id  = v_batch_id;
        COMMIT;
    END IF;

    RAISE EXCEPTION '[ETL] 批次 % 失敗，日期: %，原因: %',
        v_batch_id, p_batch_date, SQLERRM;
END;
$$;
```

### 13.10 常見 MPP Stored Procedure 反模式

```sql
-- ❌ 反模式 1：使用 CURSOR 逐行處理
-- 在 MPP 環境中，CURSOR 強制 Coordinator 序列化處理所有行
CREATE OR REPLACE PROCEDURE bad_cursor_loop()
LANGUAGE plpgsql AS $$
DECLARE
    cur CURSOR FOR SELECT sale_id FROM sales_fact;
    v_id BIGINT;
BEGIN
    OPEN cur;
    LOOP
        FETCH cur INTO v_id;
        EXIT WHEN NOT FOUND;
        -- 每次 FETCH 都從各 Segment 取一行 → 效能極差
        UPDATE sales_fact SET processed = TRUE WHERE sale_id = v_id;
    END LOOP;
    CLOSE cur;
END;
$$;

-- ✅ 正確：用 SET-BASED UPDATE
UPDATE sales_fact SET processed = TRUE;  -- 所有 Segment 並行執行


-- ❌ 反模式 2：VOLATILE 函數用在 WHERE 條件（導致 Segment-level 多次呼叫）
SELECT * FROM sales_fact WHERE sale_date > get_cutoff_date();  -- 每 Segment 呼叫一次

-- ✅ 正確：先算出值再用
SELECT * FROM sales_fact WHERE sale_date > (SELECT get_cutoff_date());


-- ❌ 反模式 3：在 FUNCTION 內執行 DDL（MPP 中 DDL 需要全叢集鎖）
CREATE OR REPLACE FUNCTION bad_ddl_in_function()
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    CREATE TEMP TABLE tmp_result AS SELECT ...;  -- DDL in function = 危險
END;
$$;

-- ✅ 正確：使用 PROCEDURE（支援 COMMIT）或在呼叫方預建暫存表


-- ❌ 反模式 4：跨節點的 dblink（效能殺手）
-- ✅ 正確：使用 PXF 或 External Table 整合外部資料源
```

---

## 14. 企業場景實戰範例

### 14.1 銀行：每日帳務對帳 SP

```sql
-- 建立對帳輔助表
CREATE TABLE daily_reconciliation (
    recon_date      DATE,
    account_id      BIGINT,
    expected_bal    DECIMAL(20,4),
    actual_bal      DECIMAL(20,4),
    diff            DECIMAL(20,4),
    status          VARCHAR(20),  -- 'MATCH', 'MISMATCH', 'MISSING'
    created_at      TIMESTAMP DEFAULT NOW()
)
WITH (appendoptimized=true, orientation=column, compresslevel=5)
DISTRIBUTED BY (account_id);

CREATE OR REPLACE PROCEDURE sp_daily_recon(p_recon_date DATE)
LANGUAGE plpgsql AS $$
BEGIN
    -- 清除舊資料
    DELETE FROM daily_reconciliation WHERE recon_date = p_recon_date;

    -- 批量對帳（MPP 並行比對）
    INSERT INTO daily_reconciliation
        (recon_date, account_id, expected_bal, actual_bal, diff, status)
    SELECT
        p_recon_date,
        COALESCE(l.account_id, r.account_id),
        COALESCE(l.expected_balance, 0),
        COALESCE(r.actual_balance,  0),
        COALESCE(r.actual_balance, 0) - COALESCE(l.expected_balance, 0),
        CASE
            WHEN l.account_id IS NULL THEN 'MISSING'
            WHEN ABS(COALESCE(r.actual_balance, 0) - l.expected_balance) < 0.01 THEN 'MATCH'
            ELSE 'MISMATCH'
        END
    FROM (
        -- 預期餘額（來自昨日結餘 + 今日交易）
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
        -- 實際餘額（來自核心系統）
        SELECT account_id, balance AS actual_balance
        FROM core_system_balance_snapshot
        WHERE snapshot_date = p_recon_date
    ) r USING (account_id);

    COMMIT;

    -- 輸出對帳摘要
    RAISE NOTICE '對帳日期: % | 比對 MATCH: % | MISMATCH: % | MISSING: %',
        p_recon_date,
        (SELECT COUNT(*) FROM daily_reconciliation WHERE recon_date = p_recon_date AND status = 'MATCH'),
        (SELECT COUNT(*) FROM daily_reconciliation WHERE recon_date = p_recon_date AND status = 'MISMATCH'),
        (SELECT COUNT(*) FROM daily_reconciliation WHERE recon_date = p_recon_date AND status = 'MISSING');
END;
$$;
```

### 14.2 零售：商品銷售排行 + 庫存預警

```sql
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
            SUM(qty)::DECIMAL / GREATEST(COUNT(DISTINCT sale_date), 1) AS daily_avg
        FROM sales_fact
        WHERE sale_date >= CURRENT_DATE - 30
        GROUP BY product_id
    )
    SELECT
        p.product_id,
        p.product_name,
        i.stock_qty,
        ROUND(COALESCE(rs.daily_avg, 0), 2),
        ROUND(i.stock_qty / NULLIF(rs.daily_avg, 0), 1),
        CASE
            WHEN i.stock_qty / NULLIF(rs.daily_avg, 0) <= p_days_ahead     THEN 'CRITICAL'
            WHEN i.stock_qty / NULLIF(rs.daily_avg, 0) <= p_days_ahead * 2 THEN 'WARNING'
            ELSE 'OK'
        END AS alert_level
    FROM dim_product p
    JOIN inventory i       ON p.product_id = i.product_id
    LEFT JOIN recent_sales rs ON p.product_id = rs.product_id
    WHERE i.stock_qty < rs.daily_avg * p_days_ahead * 3  -- 只回傳庫存偏低的商品
    ORDER BY days_remaining ASC NULLS LAST;
$$;
```

---

## 15. 監控與診斷

### 15.1 查詢監控

```sql
-- 查看目前進行中的查詢
SELECT
    pid,
    now() - pg_stat_activity.query_start AS duration,
    query,
    state,
    wait_event_type,
    wait_event
FROM pg_stat_activity
WHERE state != 'idle'
  AND query_start < now() - INTERVAL '5 minutes'
ORDER BY duration DESC;

-- 查看等待中的鎖
SELECT
    blocked.pid,
    blocked.query AS blocked_query,
    blocking.pid AS blocking_pid,
    blocking.query AS blocking_query
FROM pg_catalog.pg_locks AS blocked_locks
JOIN pg_catalog.pg_stat_activity AS blocked  ON blocked.pid  = blocked_locks.pid
JOIN pg_catalog.pg_locks         AS block_locks ON block_locks.locktype  = blocked_locks.locktype
JOIN pg_catalog.pg_stat_activity AS blocking  ON blocking.pid = block_locks.pid
WHERE NOT blocked_locks.granted;

-- 終止長時間運行的查詢
SELECT pg_cancel_backend(pid)
FROM pg_stat_activity
WHERE state = 'active'
  AND query_start < now() - INTERVAL '30 minutes';
```

### 15.2 效能分析

```sql
-- 查看最耗資源的查詢
SELECT * FROM gp_toolkit.gp_resqueue_priority_statement
ORDER BY importance DESC;

-- 查看各 Segment 的磁碟使用
SELECT * FROM gp_toolkit.gp_disk_free ORDER BY dfsegment;

-- 查看表格膨脹（bloat）
SELECT schemaname, tablename, bloat_ratio
FROM gp_toolkit.gp_bloat_expected_pages
WHERE bloat_ratio > 3
ORDER BY bloat_ratio DESC;

-- VACUUM 分析（回收空間）
VACUUM ANALYZE sales_fact;
```

### 15.3 Segment 健康檢查

```bash
# 完整叢集健康報告
gpstate -s

# 查詢設定歷史（FTS 事件）
psql -c "SELECT * FROM gp_configuration_history ORDER BY time DESC LIMIT 20;"

# 查看每個 Segment 的日誌
# Coordinator 日誌
tail -f /data0/database/coordinator/gpseg-1/log/*.csv

# Segment 日誌
tail -f /data0/database/primary/gpseg0/log/*.csv
```

---

## 16. 最佳實踐總結

### 16.1 資料建模原則

```
✅ 事實表：
   - 使用 AO 或 AOCO 表
   - 選擇高基數欄位作為分佈鍵
   - 按時間範圍分區
   - 避免過多索引（MPP 全表掃描往往更快）

✅ 維度表：
   - 小表（< 100萬行）：DISTRIBUTED REPLICATED
   - 大型維度表：與關聯事實表使用相同分佈鍵

✅ 暫存表：
   - 使用 CREATE TEMP TABLE ... AS SELECT（MPP 並行建立）
   - 明確指定 DISTRIBUTED BY（否則繼承來源表）
```

### 16.2 Stored Procedure 撰寫黃金守則

```
1. SET-BASED 優先，LOOP/CURSOR 最後
2. 善用 Co-located Join（分佈鍵一致）
3. 避免不必要的 Motion（關注 EXPLAIN 的 Redistribute/Broadcast）
4. 使用 PROCEDURE（可 COMMIT）而非 FUNCTION 做 ETL
5. 大量 DML 後執行 VACUUM ANALYZE
6. 錯誤處理要記錄到日誌表（避免失敗無從追蹤）
7. 批次處理：PROCEDURE 而非 FUNCTION，支援中間 COMMIT
8. 避免在 Segment-level Function 內執行 DDL
```

### 16.3 效能調優清單

```
□ 確認分佈鍵無資料傾斜（SKEW < 20%）
□ JOIN 的兩個表使用相同分佈鍵（Co-located）
□ 統計資訊是最新的（ANALYZE 後才有好的執行計劃）
□ 分區裁剪（Partition Pruning）有效工作（WHERE 條件含分區鍵）
□ EXPLAIN 計劃中無多餘的 Redistribute Motion
□ Resource Queue 有適當的 MEMORY_LIMIT
□ AO/AOCO 表定期執行 VACUUM（清理 dead tuple bitmap）
□ work_mem 設置合理（影響 Hash Join / Sort 記憶體使用）
```

---

## 附錄：快速指令參考

```bash
# === Docker 操作 ===
docker exec -it cbdb-cdw /bin/bash      # 進入容器
docker logs cbdb-cdw -f                  # 監看日誌

# === 叢集管理 ===
gpstart -a                               # 啟動叢集
gpstop -a                                # 停止叢集
gpstate -s                               # 查看狀態
gprecoverseg                             # 恢復 Segment
gprecoverseg -r                          # Rebalance

# === 常用 SQL ===
\l                                       # 列出所有資料庫
\dt                                      # 列出所有表格
\d+ tablename                            # 查看表格詳情
\timing                                  # 開啟查詢計時
EXPLAIN ANALYZE <query>;                 # 查看執行計劃

# === 日誌位置 ===
# Coordinator: /data0/database/coordinator/gpseg-1/log/
# Segment 0:   /data0/database/primary/gpseg0/log/
# Admin 工具:  ~/gpAdminLogs/
```

---

---

## 17. 大量資料匯入完全指南

Apache Cloudberry 的資料載入核心概念是：將外部資料先轉換為外部表（External Table）或外部資料表（Foreign Table），再透過 `SELECT` 或 `INSERT INTO ... SELECT` 將資料讀入內部表。

### 17.1 匯入方式選型決策樹

```
資料量級別？
├── 小型（< 1 GB）
│   └── COPY / \copy → 簡單直接，Coordinator 序列處理
│
├── 中型（1 GB ~ 100 GB）
│   ├── 本地網路 → gpfdist（並行，推薦）
│   └── 已在容器內 → file:// protocol（超級使用者）
│
├── 大型（100 GB ~ TB 級）
│   ├── 本地/內網 → gpfdist 多實例 + gpload 自動化
│   ├── S3/物件儲存 → s3:// protocol
│   └── Hadoop/HDFS → pxf:// protocol（Parquet/ORC）
│
└── 即時串流
    └── Kafka FDW（持續消費 Kafka Topic）
```

各載入方式對應的資料來源、格式與是否支援並行的完整比較：COPY 不支援並行；file:// protocol、gpfdist、gpfdists、gpload、s3://、pxf:// 均支援並行載入。

### 17.2 方式一：COPY / `\copy`（小量資料）

`COPY FROM` 是非並行的：資料透過 Coordinator 單一進程載入，只建議用於非常小量的資料檔案。`\copy` 是 psql 的客戶端指令，等同於 `COPY FROM STDIN`，檔案在 psql 客戶端所在主機上讀取。

```bash
# ── 基本匯入（從 Coordinator 主機上的檔案）──
psql -c "COPY sales_fact FROM '/tmp/sales_2024.csv' WITH (FORMAT CSV, HEADER);"

# ── 從客戶端本機讀取（\copy，不需把檔案複製到伺服器）──
psql -c "\copy sales_fact FROM '/local/path/sales.csv' CSV HEADER"

# ── 匯出：COPY TO（資料寫出到檔案）──
psql -c "COPY (SELECT * FROM sales_fact WHERE sale_date='2024-01-01')
         TO '/tmp/export_20240101.csv' WITH (FORMAT CSV, HEADER);"

# ── 壓縮 Pipe 匯入 ──
gunzip -c /data/sales.csv.gz | psql -c "\copy sales_fact FROM STDIN CSV HEADER"
```

```sql
-- COPY 完整語法選項
COPY sales_fact (sale_id, sale_date, amount)
FROM '/data/sales.csv'
WITH (
    FORMAT     CSV,
    HEADER     TRUE,
    DELIMITER  ',',
    NULL       '',          -- 空字串視為 NULL
    ENCODING   'UTF8',
    QUOTE      '"',
    ESCAPE     '"',
    FORCE_NULL (amount)     -- 強制 amount 空值為 NULL
);
```

> ⚠️ **限制**：COPY 只走 Coordinator，100 萬行以上建議改用 gpfdist。

---

### 17.3 方式二：gpfdist 並行載入（中大量資料，最常用）

`gpfdist` 是 Cloudberry 原生的**分散式 HTTP 檔案伺服器**，每個 Segment 會直接向 gpfdist 拉取各自分片的資料，實現真正的並行載入。

#### 架構示意

```
 ┌─────────────────────────────────────────────────────────┐
 │  資料來源主機（可以是多台）                              │
 │                                                          │
 │  gpfdist :8081  /data/sales_part*.csv  ─────────┐       │
 │  gpfdist :8082  /data/orders_part*.csv ─────────┤       │
 └────────────────────────────────────────────────────┐    │
                                                      ↓    │
 ┌─────────────────────────────────────────────────────────┐
 │  Apache Cloudberry MPP Cluster                          │
 │                                                         │
 │  Coordinator ─ 分配任務                                 │
 │  Segment 0 ←── 並行拉取 /sales_part0*.csv              │
 │  Segment 1 ←── 並行拉取 /sales_part1*.csv              │
 │  Segment 2 ←── 並行拉取 /sales_part2*.csv              │
 └─────────────────────────────────────────────────────────┘
```

#### 步驟 1：啟動 gpfdist 服務

```bash
# 在資料來源主機上執行（可後台執行）
gpfdist -d /data/csv_files \
        -p 8081 \
        -l /tmp/gpfdist_8081.log \
        -t 30 \         # 連線逾時秒數
        --ssl &         # 啟用 SSL（生產環境建議）

# 多目錄多 Port（提高吞吐）
gpfdist -d /data/part1 -p 8081 -l /tmp/gpfdist1.log &
gpfdist -d /data/part2 -p 8082 -l /tmp/gpfdist2.log &

# 確認是否正常啟動
curl http://data-server:8081/  # 應回傳檔案清單
```

#### 步驟 2：建立外部表

```sql
-- ── 單一 gpfdist 實例 ──
CREATE EXTERNAL TABLE ext_sales_csv (
    sale_id     BIGINT,
    sale_date   DATE,
    product_id  INT,
    customer_id INT,
    amount      DECIMAL(15,2),
    currency    VARCHAR(3)
)
LOCATION ('gpfdist://etl-server:8081/sales_*.csv')
FORMAT 'CSV' (
    HEADER
    DELIMITER ','
    NULL ''
    QUOTE '"'
)
ENCODING 'UTF8'
LOG ERRORS
SEGMENT REJECT LIMIT 1000 ROWS;   -- 每個 Segment 最多容忍 1000 行錯誤

-- ── 多 gpfdist 實例（提高吞吐）──
CREATE EXTERNAL TABLE ext_sales_multi (
    sale_id     BIGINT,
    sale_date   DATE,
    amount      DECIMAL(15,2)
)
LOCATION (
    'gpfdist://etl-server-1:8081/sales_*.csv',
    'gpfdist://etl-server-2:8082/sales_*.csv'
)
FORMAT 'CSV' (HEADER)
ENCODING 'UTF8';

-- ── Pipe 格式（壓縮自動解壓）──
CREATE EXTERNAL TABLE ext_sales_gz (
    sale_id   BIGINT,
    sale_date DATE,
    amount    DECIMAL(15,2)
)
LOCATION ('gpfdist://etl-server:8081/sales_*.csv.gz')
FORMAT 'CSV' (HEADER)
ENCODING 'UTF8';
-- gpfdist 自動偵測 .gz .bz2 並解壓，無需額外設定
```

#### 步驟 3：執行並行載入

```sql
-- 並行匯入（所有 Segment 同時從 gpfdist 拉取）
INSERT INTO sales_fact
SELECT * FROM ext_sales_csv;

-- 帶條件過濾的匯入
INSERT INTO sales_fact (sale_id, sale_date, product_id, customer_id, amount)
SELECT
    sale_id,
    sale_date::DATE,
    product_id,
    customer_id,
    NULLIF(amount, 0)
FROM ext_sales_csv
WHERE sale_date BETWEEN '2024-01-01' AND '2024-12-31'
  AND amount IS NOT NULL;

-- 查看載入過程中的錯誤行
SELECT cmdtime, filename, linenum, errmsg, rawdata
FROM gp_read_error_log('ext_sales_csv')
ORDER BY cmdtime DESC
LIMIT 50;

-- 清除錯誤日誌
SELECT gp_truncate_error_log('ext_sales_csv');
```

---

### 17.4 方式三：gpload 自動化批次載入

`gpload` 是封裝 gpfdist 的上層工具，透過 YAML 設定檔驅動，適合**排程自動化 ETL**。

```yaml
# /etc/gpload/sales_load.yaml
VERSION: 1.0.0.1
DATABASE: datawarehouse
USER: etl_user
PASSWORD: secret
HOST: cdw
PORT: 5432

GPLOAD:
  INPUT:
    - SOURCE:
        LOCAL_HOSTNAME:
          - etl-server-1
          - etl-server-2
        PORT: 8081
        FILE:
          - /data/sales/daily/*.csv
          - /data/sales/incremental/*.csv
      FORMAT: CSV
      COLUMNS:
        - sale_id:     bigint
        - sale_date:   date
        - product_id:  integer
        - customer_id: integer
        - amount:      decimal(15,2)
      DELIMITER: ','
      HEADER: true
      NULL_AS: ''
      ENCODING: UTF-8
      ERROR_LIMIT: 2000           # 最大錯誤行數
      LOG_ERRORS: true

  OUTPUT:
    - TABLE: sales_fact
      MODE: INSERT                # INSERT / UPDATE / MERGE

  PRELOAD:
    - TRUNCATE: false
    - REUSE_TABLES: true          # 重用外部表，加速多次執行

  SQL:
    - BEFORE: "SET work_mem = '512MB';"
    - AFTER: "ANALYZE sales_fact;"
```

```bash
# 執行載入
gpload -f /etc/gpload/sales_load.yaml -l /logs/gpload_$(date +%Y%m%d).log

# 測試模式（不實際執行，只驗證設定）
gpload -f /etc/gpload/sales_load.yaml --explain

# 排程：加入 crontab
# 每天凌晨 2 點執行
0 2 * * * gpload -f /etc/gpload/sales_load.yaml >> /logs/gpload_cron.log 2>&1
```

---

### 17.5 方式四：file:// Protocol（Segment 本地檔案）

此方式讓各 Segment 直接讀取**自己主機上的本地檔案**，適合資料已預先分片存放在各 Segment 主機的情境。

```sql
-- ⚠️ 只有超級使用者可使用 file:// protocol
-- 每個 Segment 讀取自己主機上的同名路徑

CREATE EXTERNAL TABLE ext_local_sales (
    sale_id   BIGINT,
    sale_date DATE,
    amount    DECIMAL(15,2)
)
LOCATION (
    'file://sdw1:40000/data/segment0/sales.csv',
    'file://sdw2:40001/data/segment1/sales.csv',
    'file://sdw3:40002/data/segment2/sales.csv'
)
FORMAT 'CSV' (HEADER DELIMITER ',');
```

---

### 17.6 方式五：S3 物件儲存匯入

```sql
-- 設定 S3 存取（或使用 IAM Role）
CREATE OR REPLACE FUNCTION read_from_s3()
RETURNS void AS $$
BEGIN
    EXECUTE format(
        'ALTER SYSTEM SET s3.accessid = %L',
        'YOUR_ACCESS_KEY_ID'
    );
    EXECUTE format(
        'ALTER SYSTEM SET s3.secret = %L',
        'YOUR_SECRET_ACCESS_KEY'
    );
    PERFORM pg_reload_conf();
END;
$$ LANGUAGE plpgsql;

-- 從 S3 建立外部表
CREATE EXTERNAL TABLE ext_s3_sales (
    sale_id     BIGINT,
    sale_date   DATE,
    product_id  INT,
    amount      DECIMAL(15,2)
)
LOCATION (
    's3://my-dw-bucket/sales/year=2024/month=01/*.csv
     config=/etc/cloudberry/s3/s3.conf
     region=ap-northeast-1'
)
FORMAT 'CSV' (HEADER);

-- 從 S3 載入（並行讀取，所有 Segment 同時存取 S3）
INSERT INTO sales_fact SELECT * FROM ext_s3_sales;
```

```ini
# /etc/cloudberry/s3/s3.conf
[default]
secret = YOUR_SECRET_ACCESS_KEY
accessid = YOUR_ACCESS_KEY_ID
threadnum = 4           # 每個 Segment 的並行執行緒數
chunksize = 67108864    # 64 MB 分片大小
```

---

### 17.7 方式六：PXF 載入 Parquet / ORC（大數據格式）

```sql
-- 從 HDFS 讀取 Parquet 檔案
CREATE EXTERNAL TABLE ext_hdfs_sales (
    sale_id     BIGINT,
    sale_date   DATE,
    product_id  INT,
    amount      DECIMAL(15,2)
)
LOCATION ('pxf://data/warehouse/sales/year=2024?PROFILE=hdfs:parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

-- 從 S3 讀取 ORC 格式
CREATE EXTERNAL TABLE ext_s3_orc_events (
    event_id    BIGINT,
    event_time  TIMESTAMP,
    user_id     BIGINT,
    event_type  TEXT
)
LOCATION ('pxf://s3a://my-bucket/events/?PROFILE=s3:orc&SERVER=s3default')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

-- 從 Hive 讀取（透過 PXF）
CREATE EXTERNAL TABLE ext_hive_customers (
    customer_id INT,
    name        TEXT,
    region      TEXT
)
LOCATION ('pxf://default.customers?PROFILE=hive')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
```

---

### 17.8 方式七：Kafka FDW 即時串流匯入

```sql
-- 安裝 Kafka FDW 擴展
CREATE EXTENSION kafka_fdw;

-- 建立 Kafka 伺服器連線
CREATE SERVER kafka_server
FOREIGN DATA WRAPPER kafka_fdw
OPTIONS (
    brokers 'kafka-broker1:9092,kafka-broker2:9092',
    topic   'sales_events'
);

-- 建立使用者對映
CREATE USER MAPPING FOR etl_user
SERVER kafka_server
OPTIONS (sasl_username 'kafka_user', sasl_password 'secret');

-- 建立 Kafka 外部表（持續讀取）
CREATE FOREIGN TABLE kafka_sales_stream (
    sale_id     BIGINT    OPTIONS (kafka_key 'true'),
    sale_date   DATE,
    product_id  INT,
    amount      DECIMAL(15,2),
    _partition  INT       OPTIONS (kafka_partition 'true'),
    _offset     BIGINT    OPTIONS (kafka_offset 'true')
)
SERVER kafka_server
OPTIONS (
    format        'json',
    batch_size    '10000',
    offset_commit 'true'
);

-- 消費並寫入（可在 pg_cron 排程定期呼叫）
INSERT INTO sales_fact (sale_id, sale_date, product_id, amount)
SELECT sale_id, sale_date, product_id, amount
FROM kafka_sales_stream
LIMIT 100000;  -- 每次消費最多 10 萬筆
```

---

### 17.9 外部 Web Table（Command-based 動態資料來源）

```sql
-- 透過 Shell Command 動態產生資料（每個 Segment 各執行一次）
CREATE EXTERNAL WEB TABLE ext_api_data (
    record_id   BIGINT,
    value       TEXT,
    timestamp   TIMESTAMP
)
EXECUTE 'python3 /scripts/fetch_api.py --segment=$GP_SEGMENT_ID'
ON ALL SEGMENTS
FORMAT 'CSV' (HEADER DELIMITER ',');

-- 透過 curl 拉取 API 資料
CREATE EXTERNAL WEB TABLE ext_exchange_rates (
    currency    VARCHAR(3),
    rate        DECIMAL(10,6),
    updated_at  TIMESTAMP
)
EXECUTE 'curl -s "https://api.exchangerate.io/latest?base=USD" | python3 /scripts/parse_rates.py'
ON COORDINATOR              -- API 只在 Coordinator 呼叫一次
FORMAT 'CSV' (HEADER);
```

---

## 18. 大量資料匯出完全指南

### 18.1 匯出方式比較

| 方式 | 並行 | 適用場景 | 輸出位置 |
|------|------|----------|----------|
| `COPY TO` | ✗ | 小量、單檔 | Coordinator 本機 |
| `\copy` (psql) | ✗ | 客戶端本機 | 客戶端主機 |
| 可寫外部表 + gpfdist | ✓ | 中大量、分片輸出 | 遠端主機多檔 |
| `COPY TO PROGRAM` | ✗ | 管道壓縮輸出 | Shell 管道 |
| `pg_dump / gpbackup` | ✓ | 完整備份還原 | 本機或 S3 |
| PXF 可寫外部表 | ✓ | 輸出到 S3/HDFS | 物件儲存 |

---

### 18.2 COPY TO 匯出

```bash
# ── 基本 CSV 匯出（在伺服器端執行）──
psql -c "COPY sales_fact TO '/tmp/sales_export.csv' WITH (FORMAT CSV, HEADER);"

# ── 客戶端本機輸出（\copy）──
psql -c "\copy (SELECT * FROM sales_fact WHERE sale_date='2024-01-01')
         TO '/local/export/sales_20240101.csv' CSV HEADER"

# ── 壓縮輸出 Pipe ──
psql -c "COPY sales_fact TO STDOUT CSV HEADER" | gzip > /backup/sales.csv.gz

# ── 指定欄位 + 過濾 ──
psql -c "COPY (
    SELECT s.sale_id, s.sale_date, p.product_name, s.amount
    FROM sales_fact s
    JOIN dim_product p ON s.product_id = p.product_id
    WHERE s.sale_date >= '2024-01-01'
) TO '/tmp/sales_with_product.csv' WITH (FORMAT CSV, HEADER, DELIMITER '|');"
```

```sql
-- COPY TO 完整語法
COPY (
    SELECT sale_id, sale_date, amount, currency
    FROM sales_fact
    WHERE sale_date BETWEEN '2024-01-01' AND '2024-03-31'
      AND amount > 0
)
TO '/data/export/q1_2024.csv'
WITH (
    FORMAT    CSV,
    HEADER    TRUE,
    DELIMITER ',',
    NULL      '',          -- NULL 輸出為空字串
    ENCODING  'UTF8',
    QUOTE     '"',
    FORCE_QUOTE (amount)   -- 數值欄位也加引號
);
```

---

### 18.3 可寫外部表（Writable External Table）並行匯出

這是處理大型資料匯出的**核心方法**。Cloudberry 每個 Segment 同時將各自的資料片段寫出，實現真正的並行匯出。

#### 架構示意

```
 ┌─────────────────────────────────────────────────────────┐
 │  Apache Cloudberry Cluster                              │
 │  Segment 0 ──→ 寫出 /export/part0_*.csv               │
 │  Segment 1 ──→ 寫出 /export/part1_*.csv               │
 │  Segment 2 ──→ 寫出 /export/part2_*.csv  ─────→ 目標主機│
 └─────────────────────────────────────────────────────────┘
```

```bash
# 在目標主機上啟動 gpfdist（接收模式）
gpfdist -d /data/export -p 8081 -l /tmp/gpfdist_export.log &
```

```sql
-- 建立可寫外部表（Writable External Table）
CREATE WRITABLE EXTERNAL TABLE ext_export_sales (
    sale_id     BIGINT,
    sale_date   DATE,
    product_id  INT,
    customer_id INT,
    amount      DECIMAL(15,2)
)
LOCATION ('gpfdist://export-server:8081/sales_export_%t.csv')
-- %t = 時間戳記，避免不同 Segment 覆蓋同一檔案
FORMAT 'CSV' (
    HEADER
    DELIMITER ','
    NULL ''
)
ENCODING 'UTF8'
DISTRIBUTED BY (sale_id);   -- 決定哪個 Segment 負責哪些資料

-- 執行並行匯出
INSERT INTO ext_export_sales
SELECT sale_id, sale_date, product_id, customer_id, amount
FROM sales_fact
WHERE sale_date BETWEEN '2024-01-01' AND '2024-03-31';
-- 所有 Segment 同時寫出各自的分片！
```

#### 帶壓縮的並行匯出

```sql
-- 輸出到 gzip 壓縮檔（gpfdist 自動壓縮）
CREATE WRITABLE EXTERNAL TABLE ext_export_compressed (
    sale_id   BIGINT,
    sale_date DATE,
    amount    DECIMAL(15,2)
)
LOCATION ('gpfdist://export-server:8081/sales_%t.csv.gz')
FORMAT 'CSV' (HEADER)
ENCODING 'UTF8';

INSERT INTO ext_export_compressed SELECT sale_id, sale_date, amount FROM sales_fact;
```

---

### 18.4 匯出到 S3（可寫 S3 外部表）

```sql
-- 建立可寫 S3 外部表
CREATE WRITABLE EXTERNAL TABLE ext_s3_export_sales (
    sale_id     BIGINT,
    sale_date   DATE,
    amount      DECIMAL(15,2)
)
LOCATION (
    's3://my-dw-bucket/export/sales/year=2024/
     config=/etc/cloudberry/s3/s3.conf
     region=ap-northeast-1
     newline=LF'
)
FORMAT 'CSV' (HEADER)
DISTRIBUTED RANDOMLY;

-- 並行匯出到 S3
INSERT INTO ext_s3_export_sales
SELECT sale_id, sale_date, amount
FROM sales_fact
WHERE EXTRACT(YEAR FROM sale_date) = 2024;
```

---

### 18.5 匯出到 PXF（Parquet 格式寫入 HDFS/S3）

```sql
-- 建立可寫 PXF 外部表（Parquet 格式，適合下游 Spark/Hive 使用）
CREATE WRITABLE EXTERNAL TABLE ext_pxf_parquet_sales (
    sale_id     BIGINT,
    sale_date   DATE,
    product_id  INT,
    amount      DECIMAL(15,2)
)
LOCATION ('pxf://data/export/sales_parquet/?PROFILE=hdfs:parquet&SERVER=hdfsdefault')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');

-- 匯出（所有 Segment 並行寫入 HDFS）
INSERT INTO ext_pxf_parquet_sales
SELECT sale_id, sale_date, product_id, amount
FROM sales_fact;
```

---

### 18.6 gpbackup / gprestore 完整備份還原

```bash
# ── 完整備份（含所有 Schema + 資料）──
gpbackup \
    --dbname datawarehouse \
    --backup-dir /backup/cloudberry \
    --jobs 4 \                    # 並行備份 Job 數
    --with-stats                  # 備份統計資訊

# ── 只備份 Schema（不含資料）──
gpbackup \
    --dbname datawarehouse \
    --backup-dir /backup/cloudberry \
    --metadata-only

# ── 只備份指定 Schema ──
gpbackup \
    --dbname datawarehouse \
    --backup-dir /backup/cloudberry \
    --include-schema sales_schema \
    --include-schema product_schema

# ── 只備份指定表格 ──
gpbackup \
    --dbname datawarehouse \
    --backup-dir /backup/cloudberry \
    --include-table sales_schema.sales_fact \
    --include-table product_schema.dim_product

# ── 備份到 S3 ──
gpbackup \
    --dbname datawarehouse \
    --plugin-config /etc/gpbackup/s3_plugin.yaml \
    --jobs 8

# ── 還原（還原指定時間戳的備份）──
gprestore \
    --backup-dir /backup/cloudberry \
    --timestamp 20240115120000 \
    --create-db \                 # 如果目標 DB 不存在，自動建立
    --jobs 4

# ── 只還原指定表格 ──
gprestore \
    --backup-dir /backup/cloudberry \
    --timestamp 20240115120000 \
    --include-table sales_schema.sales_fact

# ── 還原到不同資料庫 ──
gprestore \
    --backup-dir /backup/cloudberry \
    --timestamp 20240115120000 \
    --redirect-db datawarehouse_restore \
    --jobs 4
```

```yaml
# /etc/gpbackup/s3_plugin.yaml（S3 備份設定）
executablepath: /usr/local/cloudberry/bin/gpbackup_s3_plugin
options:
  region: ap-northeast-1
  aws_access_key_id: YOUR_ACCESS_KEY
  aws_secret_access_key: YOUR_SECRET_KEY
  bucket: my-backup-bucket
  folder: cloudberry/backups
```

---

### 18.7 pg_dump / pg_restore（Schema 層級備份）

```bash
# ── 備份單一資料庫（純文字 SQL）──
pg_dump -h localhost -p 5432 -U gpadmin \
        -Fp \                             # Plain text 格式
        -f /backup/datawarehouse.sql \
        datawarehouse

# ── 備份為自訂壓縮格式（支援並行還原）──
pg_dump -h localhost -p 5432 -U gpadmin \
        -Fc \                             # Custom（壓縮）格式
        -f /backup/datawarehouse.dump \
        datawarehouse

# ── 只備份 Schema 定義（不含資料）──
pg_dump -h localhost -p 5432 -U gpadmin \
        -s \                              # Schema-only
        -f /backup/datawarehouse_schema.sql \
        datawarehouse

# ── 還原 ──
pg_restore -h localhost -p 5432 -U gpadmin \
           -d datawarehouse_new \
           -j 4 \                         # 並行 Job 數
           /backup/datawarehouse.dump
```

---

### 18.8 大型資料分批匯出腳本

```bash
#!/bin/bash
# 按月分批匯出，適合超大型歷史資料遷移

EXPORT_DIR="/data/export/sales"
START_YEAR=2020
END_YEAR=2024

mkdir -p $EXPORT_DIR

for year in $(seq $START_YEAR $END_YEAR); do
    for month in $(seq -w 1 12); do
        echo "匯出 ${year}-${month} 的資料..."

        psql -h localhost -U gpadmin -d datawarehouse -c \
        "\copy (
            SELECT sale_id, sale_date, product_id, customer_id, amount
            FROM sales_fact
            WHERE sale_date >= '${year}-${month}-01'
              AND sale_date <  '${year}-${month}-01'::DATE + INTERVAL '1 month'
        ) TO '${EXPORT_DIR}/sales_${year}${month}.csv.gz'
         WITH (FORMAT CSV, HEADER)"

        echo "  完成：${EXPORT_DIR}/sales_${year}${month}.csv.gz"
    done
done
echo "全部匯出完成。"
```

---

## 19. 錯誤處理與資料品質管控

### 19.1 錯誤隔離模式設計

Apache Cloudberry 預設行為是：若外部表資料含有任何錯誤，整個載入作業失敗、不載入任何資料。啟用錯誤處理後，可以載入格式正確的資料，同時隔離有問題的行。支援兩種模式：單行錯誤隔離（跳過問題行繼續處理）以及錯誤日誌（記錄詳細錯誤資訊供後續分析）。

```sql
-- ── 模式一：固定行數限制（每個 Segment 最多容忍 N 行錯誤）──
CREATE EXTERNAL TABLE ext_sales_tolerant (
    sale_id   BIGINT,
    sale_date DATE,
    amount    DECIMAL(15,2)
)
LOCATION ('gpfdist://etl-server:8081/sales/*.csv')
FORMAT 'CSV' (HEADER)
LOG ERRORS
SEGMENT REJECT LIMIT 500 ROWS;    -- 每個 Segment 最多 500 行錯誤

-- ── 模式二：百分比限制（更靈活）──
CREATE EXTERNAL TABLE ext_sales_pct (
    sale_id   BIGINT,
    sale_date DATE,
    amount    DECIMAL(15,2)
)
LOCATION ('gpfdist://etl-server:8081/sales/*.csv')
FORMAT 'CSV' (HEADER)
LOG ERRORS
SEGMENT REJECT LIMIT 2 PERCENT;   -- 每個 Segment 最多 2% 錯誤行

-- ── 模式三：持久化錯誤日誌（跨 Session 保留）──
CREATE EXTERNAL TABLE ext_sales_persistent_log (
    sale_id   BIGINT,
    sale_date DATE,
    amount    DECIMAL(15,2)
)
LOCATION ('gpfdist://etl-server:8081/sales/*.csv')
FORMAT 'CSV' (HEADER)
LOG ERRORS PERSISTENTLY
SEGMENT REJECT LIMIT 1000 ROWS;
```

### 19.2 錯誤日誌查詢與分析

錯誤日誌包含以下欄位：`cmdtime`（錯誤時間戳記）、`relname`（外部表名稱）、`filename`（來源檔案）、`linenum`（行號）、`bytenum`（位元組位置）、`errmsg`（錯誤訊息）、`rawdata`（造成錯誤的原始資料）。

```sql
-- 查詢所有錯誤
SELECT cmdtime, filename, linenum, errmsg, rawdata
FROM gp_read_error_log('ext_sales_tolerant')
ORDER BY cmdtime DESC;

-- 錯誤類型統計（找出最多的錯誤原因）
SELECT
    errmsg,
    COUNT(*)          AS error_count,
    MIN(linenum)      AS first_line,
    MAX(linenum)      AS last_line,
    COUNT(DISTINCT filename) AS affected_files
FROM gp_read_error_log('ext_sales_tolerant')
GROUP BY errmsg
ORDER BY error_count DESC;

-- 找出特定欄位的轉型錯誤
SELECT filename, linenum, errmsg, rawdata
FROM gp_read_error_log('ext_sales_tolerant')
WHERE errmsg LIKE '%invalid input syntax for type%'
   OR errmsg LIKE '%date/time field value out of range%'
ORDER BY filename, linenum;

-- 清除錯誤日誌
SELECT gp_truncate_error_log('ext_sales_tolerant');
```

### 19.3 兩段式載入策略（Two-Phase Loading）

對於複雜資料，建議使用暫存表的兩段式載入方式：第一階段以寬鬆型別（全部 text）載入，第二階段驗證後轉換並插入最終表，能有效隔離資料品質問題。

```sql
-- ── Phase 1：全 text 型別外部表，最寬鬆接收 ──
CREATE EXTERNAL TABLE ext_raw_sales (
    sale_id_raw     TEXT,
    sale_date_raw   TEXT,
    product_id_raw  TEXT,
    amount_raw      TEXT,
    currency_raw    TEXT
)
LOCATION ('gpfdist://etl-server:8081/sales/*.csv')
FORMAT 'CSV' (HEADER)
LOG ERRORS
SEGMENT REJECT LIMIT 20 PERCENT;

-- ── Phase 2：驗證 + 轉換插入正式表 ──
INSERT INTO sales_fact (sale_id, sale_date, product_id, amount, currency)
SELECT
    sale_id_raw::BIGINT,
    CASE
        WHEN sale_date_raw ~ '^\d{4}-\d{2}-\d{2}$'
            THEN sale_date_raw::DATE
        WHEN sale_date_raw ~ '^\d{2}/\d{2}/\d{4}$'
            THEN TO_DATE(sale_date_raw, 'MM/DD/YYYY')
        ELSE NULL
    END,
    product_id_raw::INT,
    CASE
        WHEN amount_raw ~ '^\d+\.?\d*$' THEN amount_raw::DECIMAL(15,2)
        ELSE NULL
    END,
    COALESCE(NULLIF(currency_raw, ''), 'USD')
FROM ext_raw_sales
WHERE sale_id_raw  ~ '^\d+$'
  AND product_id_raw ~ '^\d+$'
  AND amount_raw   ~ '^\d+\.?\d*$'
  AND sale_date_raw IS NOT NULL;
```

### 19.4 常見錯誤類型解決方案

常見資料載入錯誤包括：資料型別轉換錯誤（數值欄位含非數字）、日期格式不一致、欄位數量不符、字元編碼問題。

```sql
-- ── 錯誤 1：日期格式不一致 ──
-- 解法：用 text 暫存，事後用 CASE 解析
CASE
    WHEN date_str ~ '^\d{8}$'               -- 20240115
        THEN TO_DATE(date_str, 'YYYYMMDD')
    WHEN date_str ~ '^\d{4}/\d{2}/\d{2}$'  -- 2024/01/15
        THEN TO_DATE(date_str, 'YYYY/MM/DD')
    WHEN date_str ~ '^\d{2}-\d{2}-\d{4}$'  -- 15-01-2024
        THEN TO_DATE(date_str, 'DD-MM-YYYY')
    ELSE NULL
END AS parsed_date

-- ── 錯誤 2：金額含千分位符或貨幣符號 ──
-- 解法：用 regexp_replace 清理
REGEXP_REPLACE(amount_str, '[,$¥€£]', '', 'g')::DECIMAL(15,2)

-- ── 錯誤 3：欄位多或少 ──
-- 解法：增加 extra_col1~N 吸收多餘欄位
CREATE EXTERNAL TABLE ext_flexible (
    col1 TEXT, col2 TEXT, col3 TEXT, col4 TEXT,
    extra1 TEXT, extra2 TEXT, extra3 TEXT  -- 吸收多餘欄位
)
LOCATION ('gpfdist://server:8081/*.csv')
FORMAT 'CSV' (HEADER)
LOG ERRORS SEGMENT REJECT LIMIT 10 PERCENT;

-- ── 錯誤 4：Big5/GBK 編碼 ──
CREATE EXTERNAL TABLE ext_big5_data (...)
LOCATION ('gpfdist://server:8081/*.txt')
FORMAT 'CSV' (HEADER DELIMITER '|')
ENCODING 'BIG5';           -- 直接指定 BIG5，DB 端自動轉 UTF8
```

### 19.5 資料品質監控 Stored Procedure

```sql
-- 建立資料品質監控主表
CREATE TABLE dq_check_results (
    check_id     SERIAL,
    check_time   TIMESTAMP DEFAULT NOW(),
    table_name   TEXT,
    check_name   TEXT,
    total_rows   BIGINT,
    failed_rows  BIGINT,
    fail_pct     DECIMAL(6,3),
    status       TEXT,   -- 'PASS', 'WARN', 'FAIL'
    detail       TEXT
)
DISTRIBUTED BY (check_id);

CREATE OR REPLACE PROCEDURE sp_run_dq_checks(p_table_name TEXT)
LANGUAGE plpgsql AS $$
DECLARE
    v_total     BIGINT;
    v_failed    BIGINT;
    v_pct       DECIMAL(6,3);
BEGIN
    -- 取得總筆數
    EXECUTE format('SELECT COUNT(*) FROM %I', p_table_name) INTO v_total;

    -- 檢查 1：NULL 主鍵
    EXECUTE format(
        'SELECT COUNT(*) FROM %I WHERE sale_id IS NULL', p_table_name
    ) INTO v_failed;
    v_pct := ROUND(v_failed * 100.0 / NULLIF(v_total, 0), 3);
    INSERT INTO dq_check_results (table_name, check_name, total_rows, failed_rows, fail_pct, status)
    VALUES (p_table_name, 'NULL_PRIMARY_KEY', v_total, v_failed, v_pct,
            CASE WHEN v_failed = 0 THEN 'PASS' ELSE 'FAIL' END);

    -- 檢查 2：負數金額
    EXECUTE format(
        'SELECT COUNT(*) FROM %I WHERE amount < 0', p_table_name
    ) INTO v_failed;
    v_pct := ROUND(v_failed * 100.0 / NULLIF(v_total, 0), 3);
    INSERT INTO dq_check_results (table_name, check_name, total_rows, failed_rows, fail_pct, status)
    VALUES (p_table_name, 'NEGATIVE_AMOUNT', v_total, v_failed, v_pct,
            CASE WHEN v_pct <= 0.01 THEN 'PASS'
                 WHEN v_pct <= 1.0  THEN 'WARN'
                 ELSE 'FAIL' END);

    -- 檢查 3：未來日期
    EXECUTE format(
        'SELECT COUNT(*) FROM %I WHERE sale_date > CURRENT_DATE', p_table_name
    ) INTO v_failed;
    INSERT INTO dq_check_results (table_name, check_name, total_rows, failed_rows, fail_pct, status)
    VALUES (p_table_name, 'FUTURE_DATE', v_total, v_failed,
            ROUND(v_failed * 100.0 / NULLIF(v_total, 0), 3),
            CASE WHEN v_failed = 0 THEN 'PASS' ELSE 'WARN' END);

    COMMIT;

    -- 輸出摘要
    RAISE NOTICE '資料品質報告 - %：共 % 筆，檢查完成。', p_table_name, v_total;
END;
$$;

-- 執行資料品質檢查
CALL sp_run_dq_checks('sales_fact');

-- 查看結果
SELECT check_name, total_rows, failed_rows, fail_pct, status
FROM dq_check_results
WHERE table_name = 'sales_fact'
  AND check_time >= NOW() - INTERVAL '1 hour'
ORDER BY check_time DESC;
```

---

## 20. 企業級 ETL 匯入匯出 SP 整合範例

### 20.1 完整的每日 ETL Pipeline SP

```sql
-- 建立 Pipeline 設定表
CREATE TABLE etl_pipeline_config (
    pipeline_id     SERIAL PRIMARY KEY,
    pipeline_name   TEXT UNIQUE,
    source_table    TEXT,
    target_table    TEXT,
    gpfdist_url     TEXT,
    schedule_expr   TEXT,       -- cron 表達式
    is_active       BOOLEAN DEFAULT TRUE,
    last_run_at     TIMESTAMP,
    last_run_status TEXT
) DISTRIBUTED BY (pipeline_id);

-- 通用 ETL Pipeline 執行器
CREATE OR REPLACE PROCEDURE sp_run_etl_pipeline(
    p_pipeline_name TEXT,
    p_run_date      DATE DEFAULT CURRENT_DATE
)
LANGUAGE plpgsql AS $$
DECLARE
    v_cfg           RECORD;
    v_start_ts      TIMESTAMP := clock_timestamp();
    v_rows_loaded   BIGINT    := 0;
    v_rows_rejected BIGINT    := 0;
    v_batch_id      TEXT      := p_pipeline_name || '_' || TO_CHAR(p_run_date, 'YYYYMMDD');
BEGIN
    -- 取得 Pipeline 設定
    SELECT * INTO v_cfg
    FROM etl_pipeline_config
    WHERE pipeline_name = p_pipeline_name AND is_active = TRUE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Pipeline % 不存在或已停用', p_pipeline_name;
    END IF;

    RAISE NOTICE '[%] Pipeline 開始執行，日期: %', p_pipeline_name, p_run_date;

    -- ── Step 1：清除當日舊資料（冪等性保障）──────────────
    EXECUTE format(
        'DELETE FROM %I WHERE load_date = %L',
        v_cfg.target_table, p_run_date
    );

    -- ── Step 2：從外部表並行載入 ────────────────────────
    EXECUTE format(
        'INSERT INTO %I SELECT *, %L AS load_date, %L AS batch_id
         FROM %I
         WHERE extract(epoch FROM load_timestamp::TIMESTAMP)
               BETWEEN extract(epoch FROM %L::TIMESTAMP)
               AND extract(epoch FROM (%L::DATE + 1)::TIMESTAMP)',
        v_cfg.target_table,
        p_run_date,
        v_batch_id,
        v_cfg.source_table,
        p_run_date,
        p_run_date
    );
    GET DIAGNOSTICS v_rows_loaded = ROW_COUNT;

    -- ── Step 3：查詢並記錄錯誤行 ────────────────────────
    EXECUTE format(
        'SELECT COUNT(*) FROM gp_read_error_log(%L)',
        v_cfg.source_table
    ) INTO v_rows_rejected;

    -- ── Step 4：更新 Pipeline 狀態 ──────────────────────
    UPDATE etl_pipeline_config
    SET last_run_at     = clock_timestamp(),
        last_run_status = 'SUCCESS'
    WHERE pipeline_name = p_pipeline_name;

    -- ── Step 5：更新統計資訊（影響後續查詢計劃）─────────
    EXECUTE format('ANALYZE %I', v_cfg.target_table);

    COMMIT;

    RAISE NOTICE '[%] 完成 | 載入: % 筆 | 拒絕: % 筆 | 耗時: %s 秒',
        p_pipeline_name,
        v_rows_loaded,
        v_rows_rejected,
        ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - v_start_ts))::NUMERIC, 2);

EXCEPTION WHEN OTHERS THEN
    UPDATE etl_pipeline_config
    SET last_run_at     = clock_timestamp(),
        last_run_status = 'FAILED: ' || SQLERRM
    WHERE pipeline_name = p_pipeline_name;
    COMMIT;
    RAISE;
END;
$$;
```

### 20.2 並行多 Pipeline 協調器

```sql
-- 使用 pg_cron 排程（需安裝 pg_cron extension）
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- 每天 01:00 執行銷售資料載入
SELECT cron.schedule(
    'daily_sales_etl',
    '0 1 * * *',
    $$CALL sp_run_etl_pipeline('sales_daily');$$
);

-- 每天 02:00 執行訂單資料載入
SELECT cron.schedule(
    'daily_orders_etl',
    '0 2 * * *',
    $$CALL sp_run_etl_pipeline('orders_daily');$$
);

-- 查看排程狀態
SELECT jobname, schedule, command, active, last_run_started_at
FROM cron.job
ORDER BY jobname;

-- 查看執行歷史
SELECT jobname, start_time, end_time, status, return_message
FROM cron.job_run_details
ORDER BY start_time DESC
LIMIT 20;
```

### 20.3 匯出 SP：定時生成報表到 S3

```sql
CREATE OR REPLACE PROCEDURE sp_export_monthly_report(
    p_year  INT,
    p_month INT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_writable_table  TEXT := 'ext_export_monthly_' || p_year || LPAD(p_month::TEXT, 2, '0');
    v_s3_path         TEXT := format('s3://my-reports-bucket/monthly/%s/%s/',
                                      p_year, LPAD(p_month::TEXT, 2, '0'));
BEGIN
    -- 動態建立可寫外部表
    EXECUTE format(
        'CREATE WRITABLE EXTERNAL TABLE %I (
             region_name   TEXT,
             product_name  TEXT,
             total_sales   DECIMAL(15,2),
             order_count   BIGINT,
             avg_order     DECIMAL(15,2)
         )
         LOCATION (%L)
         FORMAT ''CSV'' (HEADER DELIMITER '','')',
        v_writable_table,
        v_s3_path || 'monthly_report_%t.csv config=/etc/cloudberry/s3/s3.conf'
    );

    -- 並行匯出報表資料
    EXECUTE format(
        'INSERT INTO %I
         SELECT
             r.region_name,
             p.product_name,
             SUM(s.amount)        AS total_sales,
             COUNT(*)             AS order_count,
             ROUND(AVG(s.amount), 2) AS avg_order
         FROM sales_fact s
         JOIN dim_region  r ON s.region_id  = r.region_id
         JOIN dim_product p ON s.product_id = p.product_id
         WHERE EXTRACT(YEAR  FROM s.sale_date) = %s
           AND EXTRACT(MONTH FROM s.sale_date) = %s
         GROUP BY r.region_name, p.product_name
         ORDER BY total_sales DESC',
        v_writable_table, p_year, p_month
    );

    -- 清除暫時外部表
    EXECUTE format('DROP EXTERNAL TABLE %I', v_writable_table);

    COMMIT;
    RAISE NOTICE '報表已匯出至 %', v_s3_path;
END;
$$;

-- 呼叫
CALL sp_export_monthly_report(2024, 3);
```

---

### 附錄補充：匯入匯出效能調優參數

```sql
-- 大量載入前，暫時關閉觸發器（如有）
SET session_replication_role = replica;

-- 提高並行載入的記憶體
SET work_mem = '1GB';

-- 提高 gpfdist 的並行度（參數設定）
gpconfig -c gp_external_max_segs -v 64   -- 每個外部表最多使用 64 個 Segment

-- 載入後重建統計資訊
ANALYZE sales_fact;

-- 載入後 VACUUM（清理 AO 表 dead tuple bitmap）
VACUUM sales_fact;

-- 查看外部表載入進度（透過 gp_toolkit）
SELECT * FROM gp_toolkit.gp_resqueue_status;

-- 監控當前載入 Session 的資料流
SELECT pid, query_start, state, query
FROM pg_stat_activity
WHERE query LIKE '%ext_sales%'
ORDER BY query_start;
```

---

*本文件基於 Apache Cloudberry 2.x（Incubating）官方文件整理，結合企業實務場景撰寫。*  
*最後更新：2026 年 4 月 | 參考來源：https://github.com/apache/cloudberry/tree/main/devops/sandbox*
