#!/bin/bash
###############################################################################
# 00-prerequisites-check.sh
# 對應教學：第 2 章 — 環境前置需求
# 用途：檢查本機是否已安裝 Docker、Docker Compose、Git 等必要工具
###############################################################################

set -e

echo "============================================"
echo "  Apache Cloudberry Sandbox 環境前置檢查"
echo "============================================"
echo ""

PASS=0
FAIL=0

check_command() {
    local cmd="$1"
    local min_version="$2"
    local description="$3"

    if command -v "$cmd" &> /dev/null; then
        local version
        version=$($cmd --version 2>&1 | head -1)
        echo "[PASS] $description: $version"
        ((PASS++))
    else
        echo "[FAIL] $description: 未安裝 $cmd"
        ((FAIL++))
    fi
}

# --- 必要工具 ---
echo "--- 必要工具 ---"
check_command "docker"          "20.x"  "Docker"
check_command "git"             ""      "Git"

# Docker Compose（v2 以指令 docker compose 執行）
if docker compose version &> /dev/null; then
    echo "[PASS] Docker Compose: $(docker compose version 2>&1 | head -1)"
    ((PASS++))
else
    echo "[FAIL] Docker Compose: 未安裝（需要 v2+）"
    ((FAIL++))
fi

# SSH
if ssh -V 2>&1 | grep -q "OpenSSH"; then
    echo "[PASS] SSH: $(ssh -V 2>&1)"
    ((PASS++))
else
    echo "[FAIL] SSH: 未安裝"
    ((FAIL++))
fi

echo ""

# --- Docker 服務狀態 ---
echo "--- Docker 服務狀態 ---"
if docker info &> /dev/null; then
    echo "[PASS] Docker Daemon 正在運行"
    ((PASS++))
else
    echo "[FAIL] Docker Daemon 未啟動，請先啟動 Docker Desktop"
    ((FAIL++))
fi

echo ""

# --- 系統資源 ---
echo "--- 系統資源 ---"

# CPU 核心數
CPU_CORES=$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo "unknown")
if [ "$CPU_CORES" != "unknown" ] && [ "$CPU_CORES" -ge 2 ]; then
    echo "[PASS] CPU 核心數: $CPU_CORES（最低 2，建議 4+）"
    ((PASS++))
else
    echo "[WARN] CPU 核心數: $CPU_CORES（建議至少 2 核心）"
fi

# 記憶體
if command -v sysctl &> /dev/null; then
    MEM_BYTES=$(sysctl -n hw.memsize 2>/dev/null || echo 0)
    MEM_GB=$((MEM_BYTES / 1024 / 1024 / 1024))
elif [ -f /proc/meminfo ]; then
    MEM_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
    MEM_GB=$((MEM_KB / 1024 / 1024))
else
    MEM_GB=0
fi

if [ "$MEM_GB" -ge 4 ]; then
    echo "[PASS] 記憶體: ${MEM_GB} GB（最低 4 GB，建議 8 GB+）"
    ((PASS++))
else
    echo "[WARN] 記憶體: ${MEM_GB} GB（建議至少 4 GB）"
fi

# 磁碟空間
DISK_AVAIL=$(df -h . | tail -1 | awk '{print $4}')
echo "[INFO] 可用磁碟空間: $DISK_AVAIL（建議 20 GB+）"

echo ""

# --- 平台資訊 ---
echo "--- 平台資訊 ---"
echo "[INFO] 作業系統: $(uname -s) $(uname -m)"
echo "[INFO] 內核版本: $(uname -r)"

echo ""
echo "============================================"
echo "  檢查結果: $PASS 通過 / $FAIL 失敗"
echo "============================================"

if [ "$FAIL" -gt 0 ]; then
    echo ""
    echo "請先安裝缺少的工具再繼續。"
    exit 1
else
    echo ""
    echo "所有前置條件都已滿足，可以開始安裝 Sandbox！"
    echo "下一步：執行 ./01-sandbox-setup.sh"
fi
