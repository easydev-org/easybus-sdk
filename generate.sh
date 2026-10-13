#!/usr/bin/env bash
# Generate language bindings from the single source of truth.
#
#   proto/easybus/v1/*.proto   (the ONLY hand-written contract)
#     └── buf generate
#           ├── protoc-gen-go  -> gen/easybus/v1/*.pb.go   (this repo)
#           └── (later) protoc-gen-es / protoc-gen-python / protoc-gen-rust
#
# Adding a language = adding a plugin line to proto/buf.gen.yaml. The .proto is
# never edited per language.
#
# Prereqs: buf + protoc-gen-go on PATH, e.g.
#   go install github.com/bufbuild/buf/cmd/buf@latest
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
set -euo pipefail

cd "$(dirname "$0")/proto"

echo "[buf] linting..."
buf lint

echo "[buf] generating..."
buf generate

cd ..
echo "[gofmt] $(gofmt -l gen | wc -l) file(s) to format"
gofmt -w gen
echo "[done] -> gen/easybus/v1/"
