#!/bin/bash
###############################################################################
# 04-admin-tools.sh
# 對應教學：第 6 章 — 常用管理工具
# 用途：示範 gpstart/gpstop/gpstate/gpconfig 等管理工具
# 執行方式：在 Coordinator 容器內（docker exec -it cbdb-cdw /bin/bash）執行
# ⚠️  此腳本僅供閱讀參考，請勿直接 bash 執行（部分指令會影響叢集狀態）
###############################################################################

echo "============================================"
echo "  第 6 章：常用管理工具（參考示範）"
echo "============================================"
echo ""

# =============================================
# 6.1 工具速查表
# =============================================
cat << 'TOOLREF'
┌────────────────┬──────────────────────┬──────────────────────────────┐
│ 工具           │ 功能                 │ 常用參數                     │
├────────────────┼──────────────────────┼──────────────────────────────┤
│ gpstart        │ 啟動叢集             │ -a（非互動式）               │
│ gpstop         │ 停止叢集             │ -a（非互動）-M fast（快速）  │
│ gpstate        │ 查看狀態             │ -s（詳細）-e（只顯示錯誤）   │
│ gpconfig       │ 設定參數             │ -c <param> -v <value>        │
│ gprecoverseg   │ 恢復 Segment         │ -r（rebalance）-F（全量）    │
│ gpcheckperf    │ 效能測試             │ -r d（I/O）-r n（網路）      │
└────────────────┴──────────────────────┴──────────────────────────────┘
TOOLREF
echo ""

# =============================================
# 6.2 查看叢集狀態（安全，可直接執行）
# =============================================
echo "--- 6.2 查看叢集狀態 ---"
echo ""
echo "[可安全執行] gpstate"
gpstate 2>/dev/null || echo "  (需在容器內執行)"
echo ""

echo "[可安全執行] gpstate -s  (詳細狀態)"
# gpstate -s
echo "  → 取消註解以執行"
echo ""

echo "[可安全執行] gpstate -e  (只顯示錯誤)"
# gpstate -e
echo "  → 取消註解以執行"
echo ""

# =============================================
# 6.3 查看與修改 GUC 參數
# =============================================
echo "--- 6.3 查看與修改 GUC 參數 ---"
echo ""

echo "[可安全執行] 列出所有可設定參數："
echo "  gpconfig -l"
echo ""

echo "[可安全執行] 查看特定參數："
echo "  gpconfig --show work_mem"
echo "  gpconfig --show gp_vmem_protect_limit"
echo "  gpconfig --show max_connections"
echo ""

echo "[需謹慎] 修改參數（所有節點）："
echo "  gpconfig -c work_mem -v '512MB'"
echo "  gpstop -u    # 重載設定（不需重啟叢集）"
echo ""

echo "[需謹慎] 只修改 Coordinator："
echo "  gpconfig -c work_mem -v '512MB' --coordinatoronly"
echo ""

# =============================================
# 6.4 常用操作範例
# =============================================
echo "--- 6.4 常用操作範例（僅供參考）---"
echo ""

cat << 'EXAMPLES'
# --- 啟動叢集 ---
gpstart -a                  # 非互動式啟動（最常用）

# --- 停止叢集 ---
gpstop -a                   # Smart 模式（等待連線結束）
gpstop -M fast              # Fast 模式（立即停止，回滾進行中交易）
gpstop -M immediate         # Immediate 模式（危險！只在緊急時使用）

# --- 重載設定（不重啟）---
gpstop -u                   # 只重載 postgresql.conf 設定

# --- 效能測試 ---
gpcheckperf -f hostfile -r ds -D -d /data0/database

# --- 查看 GUC 參數 ---
gpconfig --show work_mem
gpconfig --show shared_buffers
gpconfig --show gp_segment_configuration

# --- 在 SQL 中查看參數 ---
psql -c "SHOW work_mem;"
psql -c "SHOW ALL;" | grep -i "work_mem"
EXAMPLES

echo ""
echo "============================================"
echo "  管理工具介紹完成"
echo "============================================"
echo "  ⚠️  重要提醒："
echo "  - gpstart/gpstop 會影響整個叢集"
echo "  - gpconfig 修改後需 gpstop -u 才能生效"
echo "  - 生產環境請勿使用 gpstop -M immediate"
