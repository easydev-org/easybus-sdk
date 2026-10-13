#!/usr/bin/env bash
# Generate Go types from the zod schemas in @abc-protocol/sdk.
#
#   zod (single source of truth)
#     -> zod-to-openapi -> openapi.json
#       -> oapi-codegen  -> types.gen.go
#
# Prereqs: npm workspace is installed (../sdk-ts), and oapi-codegen is on PATH
# (or set OAPI_CODEGEN to its path).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"          # /home/user/abc-protocol
SDK_TS="$ROOT/sdk-ts"
DST="$ROOT/sdk-go"                                # this repo
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

OAPI_CODEGEN="${OAPI_CODEGEN:-oapi-codegen}"

echo "[1/3] generating openapi.json from zod schemas..."
( cd "$SDK_TS" && npx tsx -e "
  import { generateOpenApi } from './packages/sdk/src/openapi.ts'
  const fs = require('node:fs')
  fs.writeFileSync('$TMP/openapi.json', JSON.stringify(generateOpenApi(), null, 2))
" )

echo "[2/3] generating Go types via oapi-codegen..."
cat > "$TMP/config.yaml" <<EOF
package: abcprotocol
generate:
  models: true
output: $DST/types.gen.go
output-options:
  skip-prune: true
EOF

"$OAPI_CODEGEN" -config "$TMP/config.yaml" "$TMP/openapi.json"

echo "[3/3] done -> $DST/types.gen.go"
gofmt -w "$DST/types.gen.go" || true