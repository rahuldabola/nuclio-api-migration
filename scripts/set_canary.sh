#!/usr/bin/env bash
# Shift /v1 traffic between legacy and Nuclio without restarting anything.
#   scripts/set_canary.sh 0 100    # full cutover
#   scripts/set_canary.sh 100 0    # rollback
set -euo pipefail
cd "$(dirname "$0")/.."

legacy_weight=${1:?legacy weight}
nuclio_weight=${2:?nuclio weight}

rendered=$(mktemp)
awk -v lw="$legacy_weight" -v nw="$nuclio_weight" '
  /target: legacy:8000/                 { print; getline; sub(/weight: [0-9]+/, "weight: " lw); print; next }
  /target: nuclio-nuclio-quote-api:8080/ { print; getline; sub(/weight: [0-9]+/, "weight: " nw); print; next }
  { print }
' gateway/kong.yml > "$rendered"

# DB-less Kong swaps its whole config atomically through the admin API.
curl -fsS -X POST http://localhost:8001/config -F "config=@$rendered" > /dev/null
rm -f "$rendered"
echo "canary: legacy=$legacy_weight nuclio=$nuclio_weight"
