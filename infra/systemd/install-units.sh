#!/usr/bin/env bash
# infra/systemd/*.service 를 /etc/systemd/system 에 설치 + enable.
# VM 에서 실행: bash infra/systemd/install-units.sh
set -euo pipefail

SRC="$(cd "$(dirname "$0")" && pwd)"
UNITS=(seoulnow-hotspot-producer seoulnow-bronze-silver seoulnow-silver-gold seoulnow-api)

for u in "${UNITS[@]}"; do
  sudo cp "$SRC/$u.service" "/etc/systemd/system/$u.service"
done

sudo systemctl daemon-reload
sudo systemctl enable "${UNITS[@]}"
echo "installed + enabled: ${UNITS[*]}"
echo "start: sudo systemctl start ${UNITS[*]}"
