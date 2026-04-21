#!/bin/bash
###############################################################################
# 06-fault-tolerance.sh
# 對應教學：第 8 章 — 容錯與故障恢復
# 用途：示範 Mirror 同步機制、Segment 恢復、Standby Coordinator
# ⚠️  部分操作會模擬故障，請在 Sandbox 環境中使用
###############################################################################

echo "============================================"
echo "  第 8 章：容錯與故障恢復"
echo "============================================"
echo ""

# =============================================
# 8.1 Mirror 同步機制
# =============================================
echo "--- 8.1 Mirror 同步機制 ---"
echo ""

cat << 'MIRROR_INFO'
正常狀態：
  Primary (status=u, mode=s) ←→ Mirror (status=u, mode=s)
  → 雙方資料同步，正常提供服務

Mirror 故障時：
  Primary (status=u, mode=c) ←→ Mirror (status=d)
  → Primary 切換至 change tracking 模式，繼續服務
  → 需執行 gprecoverseg 恢復 Mirror

Primary 故障時：
  Mirror 自動升格為 Primary (status=u, mode=c)
  → 服務不中斷！
  → 原 Primary 需要恢復後成為新的 Mirror
MIRROR_INFO

echo ""

# =============================================
# 8.2 檢查 Segment 健康狀態
# =============================================
echo "--- 8.2 檢查 Segment 健康狀態 ---"
echo ""
echo "以下指令需在容器內執行："
echo ""

cat << 'CHECK_CMDS'
# 步驟 1：確認哪個 Segment 異常
gpstate -e

# 步驟 2：用 SQL 查看詳細狀態
psql -c "
SELECT
    content AS seg_id,
    role,
    preferred_role,
    CASE WHEN role = preferred_role THEN '正常'
         ELSE '角色已切換'
    END AS role_status,
    mode,
    status,
    hostname,
    port
FROM gp_segment_configuration
WHERE content >= 0
ORDER BY content, role;
"

# 步驟 3：查看 FTS 設定歷史
psql -c "
SELECT time, dbid, desc
FROM gp_configuration_history
ORDER BY time DESC
LIMIT 20;
"
CHECK_CMDS

echo ""

# =============================================
# 8.3 Segment 恢復步驟
# =============================================
echo "--- 8.3 Segment 恢復步驟 ---"
echo ""

cat << 'RECOVERY'
# 步驟 1：確認異常 Segment
gpstate -e

# 步驟 2：執行增量恢復（快）
gprecoverseg

# 或全量恢復（慢，但更可靠）
gprecoverseg -F

# 步驟 3：觀察恢復進度
gpstate -e
gpstate -s | grep -i recovery

# 步驟 4：恢復後 rebalance（讓 Primary/Mirror 回到原始角色）
gprecoverseg -r

# 步驟 5：最終確認
psql -c "SELECT * FROM gp_segment_configuration ORDER BY content;"
RECOVERY

echo ""

# =============================================
# 8.4 模擬 Segment 故障（Sandbox 專用）
# =============================================
echo "--- 8.4 模擬 Segment 故障（⚠️ 僅限 Sandbox）---"
echo ""

cat << 'SIMULATE_FAULT'
# ⚠️ 以下操作會模擬故障，僅在 Sandbox 使用！

# 1. 找到一個 Primary Segment 的 PID
psql -c "
SELECT content, port, datadir
FROM gp_segment_configuration
WHERE role = 'p' AND content = 0;
"

# 2. 強制停止該 Segment（模擬故障）
#    在容器內找到 Segment 的 postgres 進程
pg_ctl stop -D /data0/database/primary/gpseg0 -m immediate

# 3. 觀察 FTS 偵測到故障（約 60 秒內）
watch -n 5 "psql -c \"SELECT content, role, preferred_role, mode, status \
FROM gp_segment_configuration WHERE content = 0;\""

# 4. Mirror 應該已升格為 Primary
psql -c "SELECT * FROM gp_segment_configuration WHERE content = 0;"

# 5. 恢復 Segment
gprecoverseg

# 6. 等待同步完成後 rebalance
gprecoverseg -r

# 7. 確認恢復正常
gpstate -e
SIMULATE_FAULT

echo ""

# =============================================
# 8.5 Standby Coordinator 管理
# =============================================
echo "--- 8.5 Standby Coordinator 管理 ---"
echo ""

cat << 'STANDBY'
# 建立 Standby Coordinator（需要 standby 主機）
gpinitstandby -s scdw         # scdw 是 standby 主機名

# 重新同步 Standby
gpinitstandby -n

# 移除 Standby
gpinitstandby -r

# 升格 Standby 為 Coordinator（在 standby 主機執行）
gpactivatestandby

# 確認 Standby 狀態
gpstate -f
STANDBY

echo ""
echo ">>> 容錯與故障恢復示範完成。"
echo ">>> 重點：gprecoverseg 是最常用的恢復指令。"
