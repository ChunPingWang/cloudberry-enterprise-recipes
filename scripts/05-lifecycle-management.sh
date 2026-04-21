#!/bin/bash
###############################################################################
# 05-lifecycle-management.sh
# 對應教學：第 7 章 — 叢集生命週期管理
# 用途：示範 Docker 容器管理與資料庫啟停操作
# ⚠️  此腳本僅供閱讀參考，請根據需要逐步手動執行
###############################################################################

echo "============================================"
echo "  第 7 章：叢集生命週期管理"
echo "============================================"
echo ""

# =============================================
# 7.1 Docker 容器管理
# =============================================
echo "--- 7.1 Docker 容器管理 ---"
echo ""

cat << 'DOCKER_CMDS'
=== 單容器操作 ===

# 查看容器狀態
docker ps -a --filter name=cbdb

# 停止容器（保留資料）
docker stop cbdb-cdw

# 啟動已停止的容器
docker start cbdb-cdw

# 刪除容器（資料也會消失！）
docker rm -f cbdb-cdw

# 查看容器日誌
docker logs cbdb-cdw -f
docker logs cbdb-cdw --tail 100

=== 多容器操作（Docker Compose）===

# 停止所有容器（保留資料）
docker compose -f docker-compose-rockylinux9.yml stop

# 啟動所有容器
docker compose -f docker-compose-rockylinux9.yml start

# 完全清除（含資料卷）
docker compose -f docker-compose-rockylinux9.yml down -v

# 查看所有容器狀態
docker compose -f docker-compose-rockylinux9.yml ps
DOCKER_CMDS

echo ""

# =============================================
# 7.2 資料庫啟停（容器重啟後）
# =============================================
echo "--- 7.2 資料庫啟停 ---"
echo ""

cat << 'DB_CMDS'
# 進入容器
docker exec -it cbdb-cdw /bin/bash

# 啟動資料庫
gpstart -a                    # 非互動式啟動

# 確認啟動成功
gpstate                       # 查看基本狀態

# 停止資料庫
gpstop -a                     # Smart 模式（等待連線結束）
gpstop -M fast                # Fast 模式（立即停止）

# ⚠️  容器重啟後資料庫不會自動啟動
# 需要手動 docker exec 進入後 gpstart
DB_CMDS

echo ""

# =============================================
# 7.3 查看叢集狀態
# =============================================
echo "--- 7.3 查看叢集狀態 ---"
echo ""

cat << 'STATUS_CMDS'
# 在容器內執行
gpstate             # 基本狀態
gpstate -s          # 詳細 Segment 狀態
gpstate -e          # 顯示錯誤狀態的 Segment
gpstate -f          # FTS 狀態

# 用 SQL 查看
psql -c "SELECT * FROM gp_segment_configuration ORDER BY content;"
STATUS_CMDS

echo ""

# =============================================
# 7.4 快速重建 Sandbox
# =============================================
echo "--- 7.4 快速重建 Sandbox ---"
echo ""

cat << 'REBUILD'
# 如果 Sandbox 出問題，最快的方式是重建

# 1. 移除舊容器
docker rm -f cbdb-cdw
# 或（多容器）
docker compose -f docker-compose-rockylinux9.yml down -v

# 2. 重新建置
cd ~/cloudberry/devops/sandbox
./run.sh -c local          # 單容器
# 或
./run.sh -c local -m       # 多容器
REBUILD

echo ""
echo ">>> 生命週期管理指令已列出。"
echo ">>> 建議：先從 docker stop/start 練起，再嘗試 gpstart/gpstop。"
