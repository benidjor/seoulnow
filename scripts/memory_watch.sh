#!/usr/bin/env bash
# 80% 초과 시 비-0 종료. cron 또는 수동 실행.
set -euo pipefail

THRESHOLD=${THRESHOLD:-80}

if [[ "$(uname)" == "Darwin" ]]; then
  total_bytes=$(sysctl -n hw.memsize)
  vm_stat_out=$(vm_stat)
  page_size=$(awk '/page size of/ {print $8}' <<< "$vm_stat_out")
  free_pages=$(awk '/Pages free/ {gsub(/\./,"",$3); print $3}' <<< "$vm_stat_out")
  inactive_pages=$(awk '/Pages inactive/ {gsub(/\./,"",$3); print $3}' <<< "$vm_stat_out")
  if [[ -z "$page_size" || -z "$free_pages" || -z "$inactive_pages" ]]; then
    echo "ERROR: vm_stat parse failed (page_size='${page_size}' free='${free_pages}' inactive='${inactive_pages}')"
    exit 2
  fi
  free_bytes=$(( (free_pages + inactive_pages) * page_size ))
  used_bytes=$(( total_bytes - free_bytes ))
else
  total_bytes=$(awk '/MemTotal/ {print $2*1024}' /proc/meminfo)
  avail_bytes=$(awk '/MemAvailable/ {print $2*1024}' /proc/meminfo)
  if [[ -z "$total_bytes" || -z "$avail_bytes" ]]; then
    echo "ERROR: /proc/meminfo parse failed"
    exit 2
  fi
  used_bytes=$(( total_bytes - avail_bytes ))
fi

usage_pct=$(( used_bytes * 100 / total_bytes ))
echo "memory used: ${usage_pct}% (threshold ${THRESHOLD}%)"
if (( usage_pct > THRESHOLD )); then
  echo "ALERT: memory above threshold"
  exit 1
fi
