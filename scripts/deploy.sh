#!/usr/bin/env bash
# Bring up the full stack: infra -> Nuclio functions (nuctl, local Docker platform) -> Kong.
# Requires Docker and nuctl (https://github.com/nuclio/nuclio/releases) on PATH.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose up -d --build --wait redis kafka legacy
docker compose run --rm kafka-init

# Nuclio builds a function from one directory, so stage each function with the shared
# quotecore package next to its handler.
rm -rf build && mkdir -p build
for fn in quote_api quote_worker; do
  mkdir -p "build/$fn"
  cp "functions/$fn/main.py" "functions/$fn/function.yaml" "build/$fn/"
  cp -r quotecore "build/$fn/quotecore"
  find "build/$fn" -name __pycache__ -prune -exec rm -rf {} +
done

nuctl deploy --platform local --path build/quote_api --file build/quote_api/function.yaml
nuctl deploy --platform local --path build/quote_worker --file build/quote_worker/function.yaml
nuctl get function --platform local

docker compose --profile gateway up -d --wait kong
echo "Gateway ready on http://localhost:8080 (header: apikey: demo-key-change-me)"
