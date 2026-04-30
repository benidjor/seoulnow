#!/usr/bin/env bash
# 80% 초과 시 비-0 종료. cron 또는 수동 실행.
set -euo pipefail

THRESHOLD=${THRESHOLD:-80}

if [[ "$(uname)" == "Darwin" ]]; then
  total_bytes=$(sysctl -n hw.memsize)
  page_size=$(vm_stat | awk '/page size of/ {print $8}')
  free_pages=$(vm_stat | awk '/Pages free/ {gsub(/\./,"",$3); print $3}')
  inactive_pages=$(vm_stat | awk '/Pages inactive/ {gsub(/\./,"",$3); print $3}')
  free_bytes=$(( (free_pages + inactive_pages) * page_size ))
  used_bytes=$(( total_bytes - free_bytes ))
else
  total_bytes=$(awk '/MemTotal/ {print $2*1024}' /proc/meminfo)
  avail_bytes=$(awk '/MemAvailable/ {print $2*1024}' /proc/meminfo)
  used_bytes=$(( total_bytes - avail_bytes ))
fi

usage_pct=$(( used_bytes * 100 / total_bytes ))
echo "memory used: ${usage_pct}% (threshold ${THRESHOLD}%)"
if (( usage_pct > THRESHOLD )); then
  echo "ALERT: memory above threshold"
  exit 1
fi
