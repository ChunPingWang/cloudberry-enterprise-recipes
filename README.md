# Apache Cloudberry Enterprise Recipes

> **Apache Cloudberry MPP 資料倉儲 — 從架構原理到企業實戰的完整 PoC 驗證**

本專案提供 **21 個可執行腳本**，涵蓋 Apache Cloudberry 的核心架構概念、資料分佈策略、效能調優、企業級 ETL Pipeline，以及完整的資料傾斜診斷與修正案例。所有腳本均已在 **Cloudberry 3.0.0-devel Sandbox** 上實測通過。

---

## 目錄

- [Apache Cloudberry 架構簡介](#apache-cloudberry-架構簡介)
- [PoC 目標與範圍](#poc-目標與範圍)
- [環境需求](#環境需求)
- [快速開始](#快速開始)
- [驗證案例詳細說明](#驗證案例詳細說明)
  - [Phase 1：環境建置與連線驗證](#phase-1環境建置與連線驗證)
  - [Phase 2：叢集架構與管理](#phase-2叢集架構與管理)
  - [Phase 3：資料建模核心概念](#phase-3資料建模核心概念)
  - [Phase 4：效能調優與外部資料](#phase-4效能調優與外部資料)
  - [Phase 5：MPP Stored Procedure 與企業實戰](#phase-5mpp-stored-procedure-與企業實戰)
  - [Phase 6：資料匯入匯出與品質管控](#phase-6資料匯入匯出與品質管控)
  - [Phase 7：端到端資料傾斜修正案例](#phase-7端到端資料傾斜修正案例)
- [測試結果總覽](#測試結果總覽)
- [參考資源](#參考資源)

---

## Apache Cloudberry 架構簡介

Apache Cloudberry（Incubating）是以 PostgreSQL 和 Greenplum 為基礎發展的開源 **MPP（Massively Parallel Processing）資料倉儲引擎**，專為 PB 級大規模資料分析設計。

### 核心架構

```
                        ┌──────────────────────┐
                        │      Client App      │
                        └──────────┬───────────┘
                                   │ SQL
                        ┌──────────▼───────────┐
                        │     Coordinator       │
                        │  ┌─────────────────┐  │
                        │  │ Parser / Planner │  │  ← SQL 解析、查詢規劃
                        │  │   Optimizer      │  │  ← GPORCA 分散式優化器
                        │  │   Dispatcher     │  │  ← 將計劃分發至各 Segment
                        │  └─────────────────┘  │
                        │  System Catalog only  │  ← 不儲存使用者資料
                        └──────────┬───────────┘
                     ┌─────────────┼─────────────┐
                     │             │             │
              ┌──���───▼──────┐ ┌───▼──────┐ ┌───▼──────┐
              │ Segment 0   │ │ Segment 1│ │ Segment N│
              │┌───────────┐│ │┌────────┐│ │┌────────┐│
              ││  Primary  ││ ││ Primary││ ││ Primary││  ← 實際儲存資料
              │└─────┬─────┘│ │└───┬────┘│ │└───┬────┘│    並行執行查詢
              │┌─────▼─────┐│ │┌───▼────┐│ │┌───▼────┐│
              ││  Mirror   ││ ││ Mirror ││ ││ Mirror ││  ← 同步複寫（容錯）
              │└───────────┘│ │└────────┘│ │└────────┘│
              └──���──────────┘ └──────────┘ └──────────┘
                     │             │             │
              ───────┴─────────────┴─────────────┴───────
                          Interconnect（網路層）
                    Segment 間的資料交換通道（Motion）
```

### 核心概念

| 概念 | 說明 |
|------|------|
| **Coordinator** | 所有客戶端的唯一入口。負責 SQL 解析、查詢規劃與結果彙整，不儲存使用者資料 |
| **Segment** | 實際儲存資料並執行查詢的工作節點，每個 Segment 由 Primary + Mirror 組成 |
| **Distribution Key** | 決定資料如何分片到各 Segment 的關鍵欄位。選擇不當會造成資料傾斜 |
| **Motion** | Segment 間的資料傳遞操作。`Redistribute` = 重分佈、`Broadcast` = 廣播、`Gather` = 匯整 |
| **Co-located Join** | 兩張表使用相同分佈鍵 JOIN 時，資料在同一 Segment 上直接 JOIN，無需 Motion |
| **GPORCA** | Cloudberry 的分散式查詢優化器，能產生更優的 MPP 執行計劃 |
| **FTS** | Fault Tolerance Service，自動偵測 Segment 故障並觸發 Mirror 接管 |

### 查詢執行流程

```
1. Client 送出 SQL → Coordinator
2. Coordinator 解析 SQL，GPORCA 生成分散式執行計劃
3. 計劃被拆分為多個 Slice，推送至各 Segment
4. 各 Segment 並行執行各自的 Slice
5. 中間結果透過 Interconnect 在 Segment 間傳遞（Motion）
6. 最終結果彙整至 Coordinator 回傳給 Client
```

### 三種資料分佈模式

```
┌─────────────────┬───────────────────────────────────────────────────┐
│  Hash 分佈       │  DISTRIBUTED BY (order_id)                       │
│  （推薦）        │  資料按欄位 Hash 值分配到各 Segment               │
│                  │  適合：高基數欄位（主鍵、唯一鍵）                  │
├─────────────────┼───────────────────────────────────────────────────┤
│  隨機分佈        │  DISTRIBUTED RANDOMLY                             │
│                  │  資料隨機均勻分配                                  │
│                  │  適合：無明顯 JOIN Key 的日誌/事件表               │
├─────────────────┼───────────────────────────────────────────────────┤
│  複製分佈        │  DISTRIBUTED REPLICATED                           │
│                  │  每個 Segment 持有完整副本                         │
│                  │  適合：小型維度表（< 100 萬行）                    │
└─────────────────┴───────────────────────────────────────────────────┘
```

### 三種表格儲存類型

```
┌──────────┬─────────────────┬────────┬──────────────┬────────────────┐
│  類型     │  適用場景        │  壓縮  │ UPDATE/DELETE │  建議用途       │
├──────────┼─────────────────┼────────┼──────────────┼────────────────┤
│  Heap    │  頻繁更新的小表  │   ✗    │     完整支援  │  設定表、狀態表  │
│  AO      │  大型批量載入    │   ✓    │     有限支援  │  事實表          │
│  AOCO    │  聚合分析、寬表  │   ✓✓   │     有限支援  │  分析查詢        │
└──────────┴─────────────────┴────────┴──────────────┴────────────────┘
```

---

## PoC 目標與範圍

### 目標

1. **驗證 Cloudberry MPP 架構**的分散式查詢執行、資料分佈與容錯機制
2. **建立企業級 ETL Pipeline** 的 Stored Procedure 模板，驗證 MPP 環境下的最佳實踐
3. **量化資料傾斜的影響**，並完整展示診斷到修正的端到端流程
4. **提供可重複執行的腳本集**，作為初學者學習與團隊內部教育訓練素材

### 範圍

本 PoC 涵蓋 Cloudberry 教學指南（`apache-cloudberry-sandbox-guide.md`）全部 20 個章節：

| 領域 | 涵蓋內容 |
|------|----------|
| 基礎建置 | 環境檢查、Sandbox 安裝、連線驗證 |
| 叢集管理 | 架構解析、管理工具、生命週期、容錯恢復 |
| 資料建模 | 分佈策略、表格類型、分區表 |
| 效能調優 | Resource Queue、GUC 參數、EXPLAIN 分析 |
| 資料整合 | COPY、gpfdist、外部表、S3/PXF 語法 |
| SP 開發 | MPP 友好 SP、Co-located Join、反模式 |
| 企業實戰 | 銀行對帳、庫存預警、分散式轉帳 |
| ETL 管線 | 增量載入、品質檢查、匯出報表 |
| 傾斜治理 | 端到端傾斜模擬、診斷、修正、驗證 |

### 測試環境

```
平台：     macOS Darwin 25.3.0 (Apple Silicon)
容器：     Docker 29.4.0 / Single-container Sandbox
版本：     Apache Cloudberry 3.0.0-devel (PostgreSQL 14.4)
叢集配置： 1 Coordinator + 3 Primary Segments + 3 Mirror Segments
```

---

## 環境需求

| 資源 | 最低 | 建議 |
|------|------|------|
| CPU | 2 核心 | 4 核心+ |
| 記憶體 | 4 GB | 8 GB+ |
| 磁碟 | 20 GB | 50 GB+ |
| Docker | 20.x+ | 最新版 |
| Git | 2.x+ | 最新版 |

---

## 快速開始

```bash
# 1. Clone 本專案
git clone <repo-url>
cd cloudberry-enterprise-recipes

# 2. 檢查本機環境
bash scripts/00-prerequisites-check.sh

# 3. 安裝 Cloudberry Sandbox
bash scripts/01-sandbox-setup.sh
# 或手動：
cd ~/cloudberry/devops/sandbox && ./run.sh -c local

# 4. 進入容器並建立 PoC 資料庫
docker exec -it cbdb-cdw /bin/bash
source /usr/local/cloudberry-db/cloudberry-env.sh
psql -f scripts/02-connect-and-verify.sql

# 5. 逐章執行
psql -d cloudberry_poc -f scripts/03-cluster-architecture.sql

# 6. 或一鍵全跑
bash scripts/run-all-poc.sh

# 7. 清理
psql -d cloudberry_poc -f scripts/18-cleanup.sql
```

---

## 驗證案例詳細說明

### Phase 1：環境建置與連線驗證

#### `00-prerequisites-check.sh` — 環境前置檢查

自動偵測本機環境是否滿足 Sandbox 部署需求。

```
檢查項目：
├── Docker 版本與 Daemon 狀態
├── Docker Compose v2
├── Git / SSH
├── CPU 核心數（≥ 2）
├── 記憶體容量（≥ 4 GB）
└── 磁碟可用空間
```

**預期輸出：** 所有項目顯示 `[PASS]`，最終顯示「所有前置條件都已滿足」。

---

#### `01-sandbox-setup.sh` — Sandbox 安裝指引

提供三種部署模式的引導式安裝腳本：

| 模式 | 指令 | 說明 |
|------|------|------|
| 單容器 | `./01-sandbox-setup.sh single` | 初學者推薦，一個容器包含完整叢集 |
| 多容器 | `./01-sandbox-setup.sh multi` | 模擬真實分散式環境（4 個容器） |
| 指定版本 | `./01-sandbox-setup.sh version 2.0.0` | 使用特定 Release 版本 |

---

#### `02-connect-and-verify.sql` — 連線驗證與健康檢查

驗證 Sandbox 是否正常啟動，並建立後續腳本使用的 `cloudberry_poc` 資料庫。

**驗證內容：**

- 資料庫版本確認（`SELECT VERSION()`）
- 所有 Segment 狀態檢查（`gp_segment_configuration`）
- Primary/Mirror 配對完整性
- 異常 Segment 偵測
- 現有資料庫、Schema、使用者列表

**關鍵觀察點：** 所有 Segment 的 `status = u`（Up）且 `mode = s`（Synced）。

---

### Phase 2：叢集架構與管理

#### `03-cluster-architecture.sql` — 叢集架構深度解析

透過實際資料觀察 MPP 的分散式查詢執行機制。

```
驗證流程：
1. 查看 Coordinator 資訊（Port 5432、只存 Catalog）
2. 查看 Segment 清單（Primary + Mirror、Port 40000~50002）
3. 建立 demo_sales 表，插入 10,000 筆資料
4. 觀察資料在各 Segment 的分佈比例
5. EXPLAIN 查看分散式執行計劃
6. EXPLAIN ANALYZE 查看實際執行統計
```

**關鍵觀察點：**
- 資料均勻分佈在 3 個 Segment（各約 33%）
- 執行計劃中的 `Gather Motion 3:1` = 從 3 個 Segment 匯整到 Coordinator
- `Redistribute Motion 3:3` = 資料在 Segment 間重分佈（效能敏感）

---

#### `04-admin-tools.sh` — 管理工具速查（參考）

以文字說明形式列出所有叢集管理工具的用法。

| 工具 | 用途 | 風險等級 |
|------|------|----------|
| `gpstate` | 查看叢集狀態 | 安全 |
| `gpconfig` | 查看/修改 GUC 參數 | 低 |
| `gpstop -u` | 重載設定 | 低 |
| `gpstart` / `gpstop` | 啟停叢集 | 中 |
| `gprecoverseg` | 恢復故障 Segment | 中 |

---

#### `05-lifecycle-management.sh` — 叢集生命週期管理（參考）

涵蓋 Docker 容器操作（start/stop/rm）與資料庫啟停（gpstart/gpstop）的完整指令。

---

#### `06-fault-tolerance.sh` — 容錯與故障恢復（參考）

說明 Mirror 同步機制、Segment 故障模擬與恢復步驟、Standby Coordinator 管理。

```
故障恢復流程：
1. gpstate -e           → 確認異常 Segment
2. gprecoverseg         → 增量恢復
3. gprecoverseg -F      → 全量恢復（更可靠）
4. gprecoverseg -r      → Rebalance（恢復原始角色）
5. gpstate -e           → 確認恢復完成
```

---

### Phase 3：資料建模核心概念

#### `07-data-distribution.sql` — 資料分佈策略

實作三種分佈模式，並透過「故意傾斜」來展示錯誤選擇分佈鍵的後果。

```
驗證流程：
1. Hash 分佈   → orders_hash (DISTRIBUTED BY order_id)
   → 觀察各 Segment 各約 33%（均勻）

2. 隨機分佈   → log_events_random (DISTRIBUTED RANDOMLY)
   → 觀察各 Segment 各約 33%（均勻）

3. 複製分佈   → dim_region_replicated (DISTRIBUTED REPLICATED)
   → 每個 Segment 持有完整 5 筆

4. 故意傾斜   → orders_skewed (DISTRIBUTED BY gender)
   → 只有 M/F 兩值，資料集中在 2 個 Segment

5. 傾斜率計算  → skew_pct = (MAX - MIN) / AVG × 100
   → Hash: ~0.8%（正常）vs Skewed: 高（傾斜）

6. 修正傾斜   → ALTER TABLE SET DISTRIBUTED BY (order_id)
   → 傾斜率降至 ~0.8%
```

**判定標準：** 傾斜率 > 20% 需更換分佈鍵。

---

#### `08-table-types.sql` — 表格類型選擇指南

實作 Heap / AO / AOCO 三種表格，並建立按月分區表。

```
建立的表格：
├── txn_heap          Heap 表      10,000 筆   → 示範 UPDATE 能力
├── sales_ao          AO 行式表   100,000 筆   → zlib 壓縮等級 5
├── clickstream_aoco  AOCO 列式表 100,000 筆   → 列式壓縮，聚合最快
└── sales_partitioned 分區表      120,000 筆   → 按月分區（12 個月 + DEFAULT）
```

**關鍵觀察點：**
- `\d+` 輸出中的 `Access method` 顯示表格類型
- AOCO 表的 `\d+` 會顯示每欄位的壓縮方式
- 分區裁剪：EXPLAIN 顯示 `Number of partitions to scan: 1 (out of 13)`

---

### Phase 4：效能調優與外部資料

#### `09-performance-tuning.sql` — 效能調優與工作負載管理

建立 Resource Queue 實現資源隔離，調整 GUC 參數，分析 EXPLAIN 執行計劃。

```
驗證內容：
1. Resource Queue
   ├── batch_queue   (HIGH 優先，最多 5 並行)
   └── analyst_queue (LOW 優先，最多 10 並行)

2. GUC 參數檢視
   ├── work_mem           → 影響 Sort/Hash Join 記憶體
   ├── shared_buffers     → 共享緩衝區
   ├── gp_vmem_protect_limit → Segment 記憶體上限
   └── max_connections    → 最大連線數

3. EXPLAIN ANALYZE
   └── 觀察 REPLICATED 表 JOIN 無 Redistribute Motion
```

---

#### `10-external-tables.sql` — 外部資料表與資料載入

實作 COPY FROM/TO，並提供 gpfdist、gpload、S3、PXF 的語法參考。

```
實際執行的操作：
├── COPY TO → 匯出 5,000 筆到 /tmp/demo_sales.csv
├── COPY FROM → 從 CSV 匯入 5,000 筆
└── COPY FROM（完整語法）→ 含 HEADER / NULL / ENCODING / QUOTE

語法參考（需啟動對應服務）：
├── gpfdist   → 分散式 HTTP 檔案伺服器
├── gpload    → YAML 設定驅動的自動化載入
├── s3://     → S3 物件儲存
├── pxf://    → HDFS Parquet / ORC / Hive
└── Web Table → Command-based 動態資料來源
```

---

### Phase 5：MPP Stored Procedure 與企業實戰

#### `11-mpp-stored-procedures.sql` — 分散式 MPP SP 撰寫技巧

本腳本是後續所有腳本的**基礎依賴**，建立核心表格並實作 MPP 友好的 SP 模板。

```
建立的核心表格：
├── sales_fact      銷售事實表     (DISTRIBUTED BY sale_id)
├── sales_staging   暫存表         (DISTRIBUTED BY sale_id)
├── dim_region      區域維度表     (DISTRIBUTED REPLICATED)
├── etl_load_log    ETL 載入日誌
├── sales_summary   銷售彙總表
├── orders          訂單表         (DISTRIBUTED BY order_id)
└── order_items     訂單明細表     (DISTRIBUTED BY order_id) ← 與 orders 同鍵！

驗證的 SP / Function：
├── sp_load_daily_sales()      → 每日銷售載入（DELETE + INSERT 模式）
├── sp_update_summary_correct() → SET-BASED 彙總更新（vs 逐行反模式）
├── fn_get_order_total()        → Co-located Join（無 Redistribute）
└── fn_monthly_sales_analysis() → 聚合 + WINDOW 函數
```

**Co-located Join 驗證：** `orders` 和 `order_items` 都以 `order_id` 分佈，EXPLAIN 中不應出現 `Redistribute Motion`。

**反模式對照：**

| 反模式 | 問題 | 正確做法 |
|--------|------|----------|
| CURSOR/LOOP 逐行處理 | 破壞 MPP 並行性 | SET-BASED 操作 |
| VOLATILE 函數在 WHERE | 每 Segment 各呼叫一次 | 先用 subquery 取值 |
| FUNCTION 內執行 DDL | 全叢集鎖 | 改用 PROCEDURE |
| 跨節點 dblink | 效能殺手 | 改用 External Table |

---

#### `12-enterprise-scenarios.sql` — 企業場景實戰

三個完整的企業級 SP 案例。

**案例 1：銀行每日帳務對帳**

```
資料規模：5,000 帳戶 / 20,000 筆交易
流程：
  昨日結餘 + 今日交易 = 預期餘額
  預期餘額 vs 核心系統快照 = MATCH / MISMATCH / MISSING
  故意在 2% 帳戶注入差異，驗證偵測能力
輸出：對帳摘要（MATCH / MISMATCH / MISSING 統計）
```

**案例 2：零售庫存預警**

```
計算邏輯：
  過去 30 天平均日銷量 → 預估可銷售天數
  可銷售天數 ≤ 7 天 → CRITICAL
  可銷售天數 ≤ 14 天 → WARNING
```

**案例 3：分散式帳戶轉帳**

```
驗證重點：
  SELECT FOR UPDATE 鎖定 → 餘額檢查 → 扣款 + 入帳 → 記錄交易
  跨 Segment 的兩階段提交（2PC）保證原子性
  轉帳前後餘額一致性驗證
```

---

#### `13-monitoring.sql` — 監控與診斷

提供 DBA 日常監控的完整查詢集。

```
監控面向：
├── 進行中的查詢（pg_stat_activity）
├── 長時間查詢偵測（> 1 分鐘）
├── 鎖定狀態（pg_locks）
├── 資料庫 / 表格大小排行
├── 各 Segment 資料分佈
├── Segment 健康狀態 + Failover 偵測
├── 連線統計（active / idle / idle in txn）
├── 統計資訊新鮮度（最後 ANALYZE 時間）
└── 需要 VACUUM 的表格（dead tuple 比例）
```

---

### Phase 6：資料匯入匯出與品質管控

#### `14-bulk-import.sql` — 大量資料匯入完全指南

實作 COPY FROM 匯入，並提供所有匯入方式的語法參考。

```
決策樹：
  < 1 GB        → COPY FROM（簡單，走 Coordinator）
  1 ~ 100 GB    → gpfdist（並行，每 Segment 直接拉取���
  100 GB ~ TB   → gpfdist 多實例 / gpload / s3:// / pxf://
  即時串流       → Kafka FDW
```

---

#### `15-bulk-export.sql` — 大量資料匯出完全指南

實作 COPY TO 匯出，包含過濾條件匯出、JOIN 匯出、匯出驗證。

```
匯出驗證流程：
1. COPY TO 匯出到 CSV
2. COPY FROM 重新匯入到暫時表
3. 比對原始筆數 vs 匯出筆數 → 確認一致
```

---

#### `16-error-handling-dq.sql` — 錯誤處理與資料品質管控

模擬「髒資料」並實作兩段式載入與 DQ 檢查 SP。

**兩段式載入（Two-Phase Loading）：**

```
Phase 1: 全 TEXT 型別暫存表接收（最寬鬆）
  ↓
Phase 2: 驗證 + 轉換
  ├── sale_id 非數字     → 過濾
  ├── 日期格式不一致     → CASE 多格式解析（YYYY-MM-DD / MM/DD/YYYY / YYYYMMDD）
  ├── 金額含貨幣符號     → regexp_replace 清理
  ├── product_id 非數字  → 過濾
  └── 空值               → COALESCE 預設值
```

**DQ 檢查 SP（`sp_run_dq_checks`）：**

| 檢查項目 | 判定標準 |
|----------|----------|
| NULL 主鍵 | 0 筆 → PASS，> 0 → FAIL |
| 負數金額 | ��� 0.01% → PASS，≤ 1% → WARN，> 1% → FAIL |
| 未來日期 | 0 筆 → PASS，> 0 → WARN |
| 重複主鍵 | 0 筆 → PASS，> 0 → FAIL |
| NULL 金額 | ≤ 1% → PASS，> 1% → WARN |

---

#### `17-etl-pipeline.sql` — 企業級 ETL Pipeline

完整的增量 ETL Pipeline，包含批次管理、品質過濾、Upsert、彙總更新、匯出報表。

```
Pipeline 流程：
                    ┌──────────────┐
                    │ sales_staging│  ← 外部資料來源
                    └──────┬───────┘
                           │ Step 2: 品質過濾
                    ┌��─────▼───────┐
                    │sales_cleansed│  ← NULL 檢查、負值處理
                    └──────┬───────┘
                           │ Step 3: Upsert
                    ┌──────▼───────┐
                    │  sales_fact  │  ← UPDATE 已存在 + INSERT 新增
                    └──────┬───────┘
                           │ Step 4: 彙總
                    ┌──────▼───────┐
                    │ daily_summary│  ← ON CONFLICT DO UPDATE
                    └──────┬───────┘
                           │ Step 6: 匯出
                    ┌──────▼───────┐
                    │  CSV 報表     │  ← COPY TO
                    └──────────────┘

監控：etl_batch_log 記錄每次執行的狀態、筆數、耗時
```

---

### Phase 7：端到端資料���斜修正案例

#### `19-skew-diagnosis-fix.sql` — 資料傾斜完整診斷與修正

這是本 PoC 的**壓軸案例**，完整模擬電商訂單系統因錯誤分佈鍵導致傾斜的發現與修正過程。

**場景：** DBA 誤用 `order_status`（僅 5 種值）作為分佈鍵

**資料規模：** 500,000 筆訂單 / 1,500,000 筆明細 / 50,000 筆客戶

```
Phase 1 — 模擬傾斜
  └── DISTRIBUTED BY (order_status)
      └── DELIVERED 佔 60%，全部落在同一個 Segment

Phase 2 — 診斷
  ├── 2.1 各 Segment 行數分佈（含視覺化長條圖）
  │       seg_0: 61.37% ██████████████████████████████████████████████████
  │       seg_1: 38.63% ███████████████████████████████
  │
  ├── 2.2 傾斜率計算
  │       skew_pct = 45.48%（中度傾斜）
  │
  ├── 2.3 分佈鍵值分析（找出根因）
  │       DELIVERED: 299,851 筆 → 全在 seg_0
  │       SHIPPED:   150,377 筆 → 全在 seg_1
  │
  ├── 2.4 磁碟使用估算
  │       seg_0: 22 MB vs seg_1: 14 MB（差 57%）
  │
  ├── 2.5 聚合查詢效能影響（EXPLAIN ANALYZE）
  │
  └── 2.6 JOIN 效能影響
          → Redistribute Motion（orders 需重分佈才能 JOIN order_items）

Phase 3 — 修正
  └── ALTER TABLE SET DISTRIBUTED BY (order_id)
      └── 耗時 ~512ms（資料重新 Hash 分佈）

Phase 4 — 驗證
  ├── 4.1 修正後分佈
  │       seg_0: 33.32% █████████████████████████████████████████████████
  │       seg_1: 33.31% █████████████████████████████████████████████████
  │       seg_2: 33.37% ██████████████████████████████████████████████████
  │
  ├── 4.2 傾斜率
  │       skew_pct = 0.20%（正常）
  │
  ├── 4.3 查詢效能對比
  │
  └── 4.4 JOIN 效能
          → Co-located Join（無 Redistribute Motion）

Phase 5 — 前後對比報告
```

**實測結果：**

| 指標 | 修正前 | 修正後 |
|------|--------|--------|
| Segment 數量使用 | 2/3（1 個空閒） | 3/3（全部使用） |
| 分佈比例 | 61% / 39% / 0% | 33.32% / 33.31% / 33.37% |
| 傾斜率 | **45.48%** | **0.20%** |
| 最大/最小差 | 113,704 行 | 327 行 |
| JOIN 方式 | Redistribute Motion | Co-located Join |

---

## 測試結果總覽

所有 12 個 SQL 腳本均已在 Cloudberry 3.0.0-devel Sandbox 上實測通過：

```
✓ 03-cluster-architecture.sql     Ch.5  叢集架構深度解析
✓ 07-data-distribution.sql        Ch.9  資料分佈策略
✓ 08-table-types.sql              Ch.10 表格類型選擇
✓ 09-performance-tuning.sql       Ch.11 效能調優
✓ 10-external-tables.sql          Ch.12 外部資料表
✓ 11-mpp-stored-procedures.sql    Ch.13 MPP Stored Procedure
✓ 12-enterprise-scenarios.sql     Ch.14 企業場景實戰
✓ 13-monitoring.sql               Ch.15 監控與診斷
✓ 14-bulk-import.sql              Ch.17 大量匯入
✓ 15-bulk-export.sql              Ch.18 大量匯出
✓ 16-error-handling-dq.sql        Ch.19 錯誤處理與品質管控
✓ 17-etl-pipeline.sql             Ch.20 ETL Pipeline
✓ 19-skew-diagnosis-fix.sql       傾斜診斷與修正

結果：13/13 通過 | 0 錯誤
```

### 測試過程中發現並修正的 Cloudberry 3.x 相容性問題

| 問題 | 原因 | 修正方式 |
|------|------|----------|
| `REPLICATED` 表查詢 `gp_segment_id` 報錯 | 複製表不暴露 Segment ID | 改用 `COUNT(*)` 驗證 |
| `pg_partitions` 不存在 | Cloudberry 3.x 移除 | 改用 `pg_inherits + pg_class` |
| `DROP RESOURCE QUEUE IF EXISTS` 語法錯誤 | 不支援 IF EXISTS | 改用 `DO $$ EXCEPTION` |
| `COMMIT` 在 EXCEPTION block 中失敗 | subtransaction 限制 | 移除 EXCEPTION 中的 COMMIT |
| `UNIQUE` 與 `DISTRIBUTED BY` 不相容 | 唯一約束必須包含分佈鍵 | 移除不相容的約束 |
| SQL function ORDER BY 別名 | 不能引用 RETURNS TABLE 別名 | 改用位置編號 |

---

## 專案結構

```
cloudberry-enterprise-recipes/
├── README.md                              ← 本文件
├── apache-cloudberry-sandbox-guide.md     ← 完整教學指南（20 章）
└── scripts/
    ├── 00-prerequisites-check.sh          ← 環境前置檢查
    ├── 01-sandbox-setup.sh                ← Sandbox 安裝指引
    ├── 02-connect-and-verify.sql          ← 連線驗證
    ├── 03-cluster-architecture.sql        ← 叢集架構
    ├── 04-admin-tools.sh                  ← 管理工具（參考）
    ├── 05-lifecycle-management.sh         ← 生命週期（參考）
    ├── 06-fault-tolerance.sh              ← 容錯恢復（參考）
    ├── 07-data-distribution.sql           ← 資料分佈策略
    ├── 08-table-types.sql                 ← 表格類型
    ├── 09-performance-tuning.sql          ← 效能調優
    ├── 10-external-tables.sql             ← 外部資料表
    ├── 11-mpp-stored-procedures.sql       ← MPP SP（核心依賴）
    ├── 12-enterprise-scenarios.sql        ← 企業場景實戰
    ├── 13-monitoring.sql                  ← 監控與診斷
    ├── 14-bulk-import.sql                 ← 大量匯入
    ├── 15-bulk-export.sql                 ← 大量匯出
    ├── 16-error-handling-dq.sql           ← 錯誤處理與 DQ
    ├── 17-etl-pipeline.sql               ← ETL Pipeline
    ├── 18-cleanup.sql                     ← 清理測試物件
    ├── 19-skew-diagnosis-fix.sql          ← 傾斜診斷與修正
    ├── run-all-poc.sh                     ← 一鍵執行全部
    └── README.md                          ← 腳本索引
```

---

## 參考資源

- [Apache Cloudberry GitHub](https://github.com/apache/cloudberry)
- [Cloudberry Sandbox](https://github.com/apache/cloudberry/tree/main/devops/sandbox)
- [Cloudberry Documentation](https://cloudberry.apache.org/docs/)
- [PostgreSQL 14 Documentation](https://www.postgresql.org/docs/14/)

---

*本專案基於 Apache Cloudberry 3.0.0-devel（Incubating）實測驗證。*
*最後更新：2026 年 4 月*
