#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v codeql >/dev/null 2>&1; then
  echo "ERROR: codeql CLI not found in PATH"
  echo "Install from: https://docs.github.com/en/code-security/codeql-cli/getting-started-with-the-codeql-cli"
  exit 1
fi

mkdir -p .tmp/codeql

echo "==> Ensuring CodeQL standard query packs are available"
codeql pack download codeql/go-queries codeql/actions-queries

CODEQL_GO_RAM_MB="${CODEQL_GO_RAM_MB:-2048}"
CODEQL_ACTIONS_RAM_MB="${CODEQL_ACTIONS_RAM_MB:-1024}"
CODEQL_QUERY_STRATEGY="${CODEQL_QUERY_STRATEGY:-github}"

run_go() {
  echo "==> CodeQL (Go)"
  rm -rf .tmp/codeql/go-db
  codeql database create .tmp/codeql/go-db \
    --language=go \
    --ram="$CODEQL_GO_RAM_MB" \
    --command="go build ./..."

  if [[ "$CODEQL_QUERY_STRATEGY" == "security-and-quality" ]]; then
    codeql database analyze .tmp/codeql/go-db \
      codeql/go-queries:codeql-suites/go-security-and-quality.qls \
      --download \
      --ram="$CODEQL_GO_RAM_MB" \
      --format=sarifv2.1.0 \
      --sarif-category="/language:go" \
      --output .tmp/codeql/go.sarif
  else
    codeql database analyze .tmp/codeql/go-db \
      codeql/go-queries \
      --download \
      --ram="$CODEQL_GO_RAM_MB" \
      --format=sarifv2.1.0 \
      --sarif-category="/language:go" \
      --output .tmp/codeql/go.sarif
  fi
}

run_actions() {
  echo "==> CodeQL (Actions)"
  rm -rf .tmp/codeql/actions-db
  codeql database create .tmp/codeql/actions-db \
    --language=actions \
    --build-mode=none \
    --ram="$CODEQL_ACTIONS_RAM_MB"

  codeql database analyze .tmp/codeql/actions-db \
    codeql/actions-queries \
    --download \
    --ram="$CODEQL_ACTIONS_RAM_MB" \
    --format=sarifv2.1.0 \
    --sarif-category="/language:actions" \
    --output .tmp/codeql/actions.sarif
}

run_go
run_actions

echo ""
echo "CodeQL local run complete."
echo "SARIF outputs:"
echo "  .tmp/codeql/go.sarif"
echo "  .tmp/codeql/actions.sarif"
