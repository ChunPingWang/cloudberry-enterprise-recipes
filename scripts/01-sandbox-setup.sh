#!/bin/bash
###############################################################################
# 01-sandbox-setup.sh
# 對應教學：第 3 章 — Sandbox 安裝與建置
# 用途：自動化 clone 並啟動 Apache Cloudberry Sandbox
# 說明：提供單容器與多容器兩種模式
###############################################################################

set -e

CLOUDBERRY_DIR="${HOME}/cloudberry"
MODE="${1:-single}"   # 預設單容器；傳入 "multi" 則啟用多容器

echo "============================================"
echo "  Apache Cloudberry Sandbox 安裝"
echo "  模式: $MODE"
echo "============================================"
echo ""

# --- Step 1: 取得原始碼 ---
echo "[Step 1] 取得 Apache Cloudberry 原始碼..."

if [ -d "$CLOUDBERRY_DIR" ]; then
    echo "  目錄 $CLOUDBERRY_DIR 已存在，跳過 clone。"
    echo "  如需重新 clone，請先執行: rm -rf $CLOUDBERRY_DIR"
else
    git clone https://github.com/apache/cloudberry.git "$CLOUDBERRY_DIR"
    echo "  Clone 完成。"
fi

cd "$CLOUDBERRY_DIR/devops/sandbox"
echo "  工作目錄: $(pwd)"
echo ""

# --- Step 2: 選擇部署模式 ---
echo "[Step 2] 啟動 Sandbox..."

case "$MODE" in
    single)
        echo "  使用單容器模式（推薦初學者）"
        echo "  執行: ./run.sh -c local"
        echo ""
        echo "  ⚠️  建置約需 10~20 分鐘，請耐心等待..."
        echo "  完成後會看到 'Deployment Successful' 訊息。"
        echo ""
        # 取消註解以下行來實際執行：
        # ./run.sh -c local
        echo "  [模擬模式] 請手動執行以下指令開始建置："
        echo "  cd $CLOUDBERRY_DIR/devops/sandbox && ./run.sh -c local"
        ;;
    multi)
        echo "  使用多容器叢集模式（分散式功能測試）"
        echo "  執行: ./run.sh -c local -m"
        echo ""
        echo "  多容器架構："
        echo "  ┌─────────────────────────────────────────────┐"
        echo "  │  cbdb-cdw  (Coordinator + Standby)          │"
        echo "  │  cbdb-sdw1 (Segment Host 1 - Primary)       │"
        echo "  │  cbdb-sdw2 (Segment Host 2 - Primary)       │"
        echo "  │  cbdb-sdw3 (Segment Host 3 - Mirror)        │"
        echo "  └─────────────────────────────────────────────┘"
        echo ""
        echo "  ⚠️  建置約需 15~30 分鐘"
        echo ""
        # 取消註解以下行來實際執行：
        # ./run.sh -c local -m
        echo "  [模擬模式] 請手動執行以下指令開始建置："
        echo "  cd $CLOUDBERRY_DIR/devops/sandbox && ./run.sh -c local -m"
        ;;
    version)
        VERSION="${2:-2.0.0}"
        echo "  使用指定版本模式: $VERSION"
        echo ""
        # ./run.sh -c "$VERSION"
        echo "  [模擬模式] 請手動執行以下指令："
        echo "  cd $CLOUDBERRY_DIR/devops/sandbox && ./run.sh -c $VERSION"
        ;;
    *)
        echo "  未知模式: $MODE"
        echo "  用法: $0 [single|multi|version] [version_number]"
        echo "  範例:"
        echo "    $0 single          # 單容器（預設）"
        echo "    $0 multi           # 多容器叢集"
        echo "    $0 version 2.0.0   # 指定版本"
        exit 1
        ;;
esac

echo ""
echo "============================================"
echo "  建置完成後的驗證步驟"
echo "============================================"
echo ""
echo "  1. 進入容器:  docker exec -it cbdb-cdw /bin/bash"
echo "  2. 連線資料庫: psql"
echo "  3. 確認版本:   SELECT VERSION();"
echo "  4. 查看叢集:   SELECT * FROM gp_segment_configuration;"
echo ""
echo "  從外部連線:   psql -h localhost -p 5432 -U gpadmin -d gpadmin"
echo ""
echo "  下一步：執行 02-connect-and-verify.sql"
