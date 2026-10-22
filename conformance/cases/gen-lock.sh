#!/bin/sh
# Regenerate cases.lock and copy the case files into each SDK.
#
# The lock is the SHA-256 of every case file, concatenated in sorted order. Each
# SDK's test recomputes it from its own COPY, so an SDK whose copy has drifted —
# hand-edited, or simply not refreshed after a case changed here — fails with a
# precise message instead of quietly asserting an older contract.
#
# Run this after an intentional change here, then copy the same lock into the SDKs
# (the script does both).
set -eu

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../.." && pwd)

# One hash over the sorted case files: the file NAME is part of the digest, so
# renaming a case is a change, and adding one cannot collide with editing another.
digest() {
  set -- "$1"/*.json
  cat "$@" | sha256sum | cut -d' ' -f1
}

sum=$(digest "$here")
printf '%s\n' "$sum" > "$here/cases.lock"
echo "cases.lock = $sum"

for sdk in easybus-sdk easybus-sdk-python easybus-sdk-rust easybus-sdk-typescript; do
  dst="$root/$sdk/conformance-cases"
  [ -d "$root/$sdk" ] || continue
  rm -rf "$dst"
  mkdir -p "$dst"
  cp "$here"/*.json "$dst"/
  cp "$here/cases.lock" "$dst"/
  echo "  copied -> $sdk/conformance-cases"
done
