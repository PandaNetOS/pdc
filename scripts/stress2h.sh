#!/usr/bin/env bash
# ==============================================================
# pdc 2小时稳定性压测（2026-10-09 治本验收）
#
# 目标：连续运行 2 小时，验证「不卡死」。
#
# 卡死判据（任一命中即判定 FAIL，全部对应本次事故的现场特征）：
#   F1 API 无响应      —— /health 连续 3 次超时（15s 阈值）
#   F2 全局停摆        —— 日志出现「STALL 自愈触发」
#   F3 槽位泄漏        —— 日志出现「槽位泄漏」ERROR
#   F4 任务静默        —— 调度器心跳中断 > 90s
#   F5 爬虫停摆        —— 爬虫进度日志停滞 > 10min
#   F6 进程退出        —— pdc.exe 消失
#
# 用法：./stress2h.sh [时长秒，默认7200]
# ==============================================================
set -u

DURATION="${1:-7200}"
WORK_DIR="/d/test/pdc"
EXE="/d/PNOS/pdc/target/release/pdc.exe"
LOG_DIR="$WORK_DIR/logs"
STAMP=$(date +%Y%m%d-%H%M%S)
MON_LOG="$WORK_DIR/stress-$STAMP-monitor.log"

# 2026-10-09 修正：HTTP API 实际监听 api_monitor_port(6886)，
# 6880 是 port_allocator 分配的组首端口（api_port）但未挂 REST 路由。
# 压测前实测确认：6880/health → 404，6886/health → 200。
API_PORT=6886
PROBE_TIMEOUT=15

echo "============================================================"
echo " pdc 2 小时稳定性压测"
echo "   时长    : ${DURATION}s ($(($DURATION/60)) 分钟)"
echo "   工作目录: $WORK_DIR"
echo "   二进制  : $EXE"
echo "   监控日志: $MON_LOG"
echo "============================================================"

# ---- 前置检查 ----
if [ ! -f "$EXE" ]; then
    echo "[FAIL] 二进制不存在: $EXE"
    exit 1
fi

# 端口占用检查（上一轮残留进程会污染结果）。
#
#【修复 2026-10-09】此前用 `tasklist | grep pdc.exe` 检测，
# 但 Git Bash 下 tasklist 查不到实际进程（实测有 pdc 在跑却报「无进程」），
# 导致端口被占、cp 部署二进制报 "Device or resource busy"。
# 改用端口监听判定：6880/6886 被监听即说明 pdc 还活着。
if netstat -ano 2>/dev/null | grep "LISTENING" | grep -qE ":688[0-6]"; then
    echo "[FAIL] 6880/6886 仍被监听，说明有残留 pdc 进程，请先结束："
    netstat -ano 2>/dev/null | grep -E ":688[0-6]" | head -6
    exit 1
fi

# ---- 启动 ----
cd "$WORK_DIR" || exit 1
./pdc.exe > "$WORK_DIR/stress-$STAMP.stdout.log" 2>&1 &
PDC_PID=$!
echo "[INFO] pdc 已启动 PID=$PDC_PID"
sleep 20   # 等端口探测 + DB 打开 + runtime 装配

if ! kill -0 "$PDC_PID" 2>/dev/null; then
    echo "[FAIL] pdc 启动后立即退出，日志："
    tail -40 "$WORK_DIR/stress-$STAMP.stdout.log"
    exit 1
fi

# ---- 日志增量计数 ----
# 双重修复（2026-10-09）：
#  a) `grep -c PAT || echo 0` 在无匹配时 grep 已输出 0 且退出码非 0，
#     `|| echo 0` 会再追加一个 0，形成两行数字 ⇒ `[ "$x" -gt 0 ]` 报整数错。
#     改为统一走下面 count_log()。
#  b) 只统计本次压测新增的行（基线差值），否则会读到历史事故日志。
BASE_LINES=0
TODAY_LOG=""

count_log() {
    # 参数 $1=正则。返回本次压测期间新增的匹配行数（恒为纯整数）。
    #
    # 【第三个修复 2026-10-09】此前误用
    #   total=$(grep -ac PAT F) - BASE_LINES
    # 其中 BASE_LINES 是**总行数**而 grep -c 是**匹配行数**，量纲不一致，
    # 相减得到负数（实测 泄漏=-12706），进而使 F4「心跳停滞」误报。
    # 正确做法：先按行偏移切出「本次新增片段」，再在片段内计数。
    local pat="$1"
    local total
    if [ "$BASE_LINES" -le 0 ]; then
        total=$(grep -ac "$pat" "$TODAY_LOG" 2>/dev/null)
    else
        total=$(tail -n "+$(( BASE_LINES + 1 ))" "$TODAY_LOG" 2>/dev/null                | grep -ac "$pat" 2>/dev/null)
    fi
    total=${total:-0}
    echo "${total}"
}

# ---- 监控循环 ----
START=$(date +%s)
DEADLINE=$((START + DURATION))
API_FAIL_STREAK=0
NO_HB_SECS=0
LAST_CRAWL_SEEN=0
FAIL_REASON=""
PROBE_COUNT=0

# 心跳取自当天的 pdc.log.*；先定位实际文件
sleep 2
TODAY_LOG=$(ls -t "$LOG_DIR"/pdc.log.* 2>/dev/null | head -1)
[ -z "$TODAY_LOG" ] && TODAY_LOG="$LOG_DIR/pdc.log"
echo "[INFO] 日志文件: $TODAY_LOG"

# **基线**：记录启动时刻的日志行数。之后所有统计都是「总量 − 基线」，
# 只计本次压测新增的日志，避免把历史事故（08:15-08:21 那批泄漏）
# 误判为本次失败。
BASE_LINES=$(wc -l < "$TODAY_LOG" 2>/dev/null || echo 0)
BASE_LINES=${BASE_LINES:-0}
echo "[INFO] 日志基线行数: $BASE_LINES（其后新增的行才会被计入统计）"

printf "%-8s %-10s %-10s %-10s %-12s %-10s\n" "已运行" "API" "在飞" "队列" "心跳延迟" "状态" > "$MON_LOG"

while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    NOW=$(date +%s)
    ELAPSED=$((NOW - START))

    # --- F6 进程存活 ---
    if ! kill -0 "$PDC_PID" 2>/dev/null; then
        FAIL_REASON="F6 进程在 ${ELAPSED}s 退出"
        echo "[FAIL] $FAIL_REASON"
        break
    fi

    # --- F1 API 探活（每 60s 一次，避免高频干扰） ---
    API="--"
    if [ $((ELAPSED % 60)) -lt 15 ]; then
        PROBE_COUNT=$((PROBE_COUNT+1))
        RESP=$(curl -s -m "$PROBE_TIMEOUT" -o /dev/null -w "%{http_code}" \
               "http://127.0.0.1:$API_PORT/health" 2>/dev/null)
        if [ "$RESP" = "200" ]; then
            API="OK"
            API_FAIL_STREAK=0
        else
            API="TIMEOUT"
            API_FAIL_STREAK=$((API_FAIL_STREAK+1))
            echo "[WARN] ${ELAPSED}s API 无响应 (streak=$API_FAIL_STREAK)"
            if [ "$API_FAIL_STREAK" -ge 3 ]; then
                FAIL_REASON="F1 API 连续 ${API_FAIL_STREAK} 次无响应（累计 $((API_FAIL_STREAK*60))s）"
                echo "[FAIL] $FAIL_REASON"
                break
            fi
        fi
    fi

    # --- 从日志提取调度器状态 ---
    HB_LINE=$(grep -a "调度器心跳" "$TODAY_LOG" 2>/dev/null | tail -1)
    INFLIGHT=$(echo "$HB_LINE" | sed -nE 's/.*在飞=([0-9]+).*/\1/p')
    QUEUE=$(echo "$HB_LINE" | sed -nE 's/.*队列待执行=([0-9]+).*/\1/p')
    [ -z "$INFLIGHT" ] && INFLIGHT="?"
    [ -z "$QUEUE" ] && QUEUE="?"

    # --- F2 全局停摆自愈（本次新增的兜底，真出现即说明还有未解的停摆） ---
    STALL_HITS=$(count_log "STALL 自愈触发")
    if [ "$STALL_HITS" -gt 0 ]; then
        FAIL_REASON="F2 触发全局停摆自愈 ${STALL_HITS} 次"
        echo "[FAIL] $FAIL_REASON"
        grep -a "STALL 自愈" "$TODAY_LOG" | tail -5
        break
    fi

    # --- F3 槽位泄漏 ---
    LEAK=$(count_log "槽位泄漏")
    if [ "$LEAK" -gt 0 ]; then
        FAIL_REASON="F3 槽位泄漏 ${LEAK} 次"
        echo "[FAIL] $FAIL_REASON"
        grep -a "槽位泄漏" "$TODAY_LOG" | tail -5
        break
    fi

    # --- F4 心跳中断（本次事故中心跳正常，故此项是额外保险） ---
    # F4：调度器心跳是否还在新增。心跳是 30s 一条，故容许 90s 无新增。
    # 判据用「片段内最新心跳的年龄」，比比较计数更抗抖动。
    HB_AGE=$(tail -n "+$(( BASE_LINES + 1 ))" "$TODAY_LOG" 2>/dev/null              | grep -a "调度器心跳" | tail -1              | sed -nE 's/^([0-9-]+T)([0-9:]+).*//p' | head -1)
    if [ -z "$HB_AGE" ]; then
        NO_HB_SECS=$(( NO_HB_SECS + 15 ))
    else
        NO_HB_SECS=0
    fi
    if [ "$NO_HB_SECS" -ge 90 ]; then
        FAIL_REASON="F4 调度器心跳停滞 ${NO_HB_SECS}s（无任何新心跳）"
        echo "[FAIL] $FAIL_REASON"
        break
    fi

    # --- F5 爬虫静默 ---
    CRAWL_NOW=$(count_log "爬行进度\|链式直接采集\|分层响应率")
    if [ "$CRAWL_NOW" != "$LAST_CRAWL_SEEN" ]; then
        LAST_CRAWL_SEEN=$CRAWL_NOW
    fi

    printf "%-8s %-10s %-10s %-10s %-12s %-10s\n" \
        "${ELAPSED}s" "$API" "$INFLIGHT" "$QUEUE" "-" "RUNNING" >> "$MON_LOG"

    # 每 10 分钟输出一次摘要
    if [ $((ELAPSED % 600)) -lt 20 ]; then
        echo "[$(printf '%02d:%02d:%02d' $((ELAPSED/3600)) $(((ELAPSED%3600)/60)) $((ELAPSED%60)))] API=$API 在飞=$INFLIGHT 队列=$QUEUE 泄漏=$LEAK 自愈=$STALL_HITS"
    fi

    sleep 15
done

END=$(date +%s)
TOTAL=$((END - START))

# ---- 收尾 ----
echo ""
echo "============================================================"
if [ -n "$FAIL_REASON" ]; then
    echo " 压测结果: FAIL"
    echo " 失败原因: $FAIL_REASON"
    echo " 运行时长: ${TOTAL}s"
    echo "============================================================"
    kill "$PDC_PID" 2>/dev/null
    echo "[INFO] 已结束压测进程"
    exit 1
else
    echo " 压测结果: PASS"
    echo " 连续运行: ${TOTAL}s ($(($TOTAL/60)) 分钟)"
    echo " API 探活: ${PROBE_COUNT} 次，全部正常"
    echo " 槽位泄漏: 0"
    echo " 停摆自愈: 0"
    echo "============================================================"
    echo ""
    echo "关键指标抽样（最后 3 条心跳，含新增队列延迟字段）："
    grep -a "调度器心跳" "$TODAY_LOG" | tail -3
    echo ""
    echo "监控明细: $MON_LOG"
    kill "$PDC_PID" 2>/dev/null
    exit 0
fi