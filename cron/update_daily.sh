#!/bin/bash
set -euo pipefail

# 简单版每日更新脚本
# 功能：调用main.py完成每日股票数据更新，支持Linux定时任务运行

# 配置项（根据实际情况修改）
PROJECT_ROOT="/opt/stock_down_load"          # 项目在Linux上的部署路径
PYTHON_PATH="${PROJECT_ROOT}/venv/bin/python" # Python路径（如果用系统Python就填python3）
LOCK_FILE="/tmp/stock_update.lock"            # 锁文件路径，防止重复执行
LOG_FILE="${PROJECT_ROOT}/cron/update_$(date +%Y%m%d).log" # 日志文件路径

# 日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "${LOG_FILE}"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# 检查是否已有进程在运行
check_lock() {
    if [ -f "${LOCK_FILE}" ]; then
        PID=$(cat "${LOCK_FILE}")
        if kill -0 "${PID}" 2>/dev/null; then
            log "ERROR: 已有更新进程在运行，PID: ${PID}，本次执行退出"
            exit 1
        else
            log "WARNING: 发现过期锁文件，已清理"
            rm -f "${LOCK_FILE}"
        fi
    fi
    echo $$ > "${LOCK_FILE}"
}

# 释放锁
release_lock() {
    rm -f "${LOCK_FILE}"
    log "INFO: 锁已释放"
}

# 主函数
main() {
    log "===== 开始执行每日数据更新 ====="

    # 检查项目路径
    if [ ! -d "${PROJECT_ROOT}" ]; then
        log "ERROR: 项目目录不存在: ${PROJECT_ROOT}"
        exit 1
    fi

    # 进入项目目录
    cd "${PROJECT_ROOT}" || {
        log "ERROR: 无法进入项目目录"
        exit 1
    }

    # 加载环境变量
    if [ -f ".env" ]; then
        set -a
        source .env
        set +a
        log "INFO: 环境变量加载完成"
    else
        log "ERROR: .env文件不存在，请先配置雪球Cookie"
        exit 1
    fi

    # 1. 更新自选股
    log "INFO: 开始更新自选股数据"
    if ! ${PYTHON_PATH} main.py quote-update >> "${LOG_FILE}" 2>&1; then
        log "WARNING: 自选股更新失败，继续执行其他任务"
    else
        log "INFO: 自选股更新完成"
    fi

    # 2. 执行日级任务（更新所有股票的行情、资金流、日K线等）
    log "INFO: 开始执行日级全量更新"
    if ! ${PYTHON_PATH} main.py task --type daily >> "${LOG_FILE}" 2>&1; then
        log "ERROR: 日级更新失败"
        exit 1
    else
        log "INFO: 日级更新完成"
    fi

    # 3. 数据质量校验
    log "INFO: 开始数据质量校验"
    if ! ${PYTHON_PATH} main.py validate-data --auto-fix >> "${LOG_FILE}" 2>&1; then
        log "WARNING: 数据校验发现问题，已尝试自动修复"
    else
        log "INFO: 数据校验完成，所有数据正常"
    fi

    # 4. 每周日执行周级更新
    if [ "$(date +%u)" -eq 7 ]; then
        log "INFO: 今天是周日，执行周级更新"
        if ! ${PYTHON_PATH} main.py task --type weekly >> "${LOG_FILE}" 2>&1; then
            log "WARNING: 周级更新失败"
        else
            log "INFO: 周级更新完成"
        fi
    fi

    # 5. 每月1号执行月级更新
    if [ "$(date +%d)" -eq 01 ]; then
        log "INFO: 今天是月初，执行月级更新"
        if ! ${PYTHON_PATH} main.py task --type monthly >> "${LOG_FILE}" 2>&1; then
            log "WARNING: 月级更新失败"
        else
            log "INFO: 月级更新完成"
        fi
    fi

    log "===== 所有更新任务执行完成 ====="
}

# 执行主流程
trap release_lock EXIT
check_lock
main "$@"
exit 0
