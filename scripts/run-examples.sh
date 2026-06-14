#!/usr/bin/env bash
# Runs every examples/*.php against a running NATS server and reports pass / skip / fail. This doubles as
# a "the README examples still work" gate (each example mirrors a README snippet).
#
# Base examples use $NATS_URL (default nats://127.0.0.1:4222). The auth/WebSocket examples self-configure
# their own server via their own env vars (NATS_TOKEN_URL, NATS_USERPASS_URL, NATS_JWT_URL, NATS_NKEY_URL,
# NATS_TLS_URL, NATS_WS_URL), defaulting to the dev docker-compose ports.
#
# Quick start (dev docker-compose stack):
#   docker compose up -d                                  # full stack (token/userpass/jwt/tls/nkey/ws)
#   composer fixture:jwt                                  # JWT + creds fixtures (auth-jwt-nkey, auth-credentials-file)
#   NATS_URL=nats://127.0.0.1:14222 bash scripts/run-examples.sh
#
# For just the core examples, `docker compose up -d nats` is enough; the auth/WebSocket ones will then fail
# to connect (expected without the variant servers).
set -uo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

export NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
timeout_s="${EXAMPLE_TIMEOUT:-60}"

pass=0
skip=0
fail=0
failed=""

for file in examples/*.php; do
  name="$(basename "$file" .php)"
  out="$(timeout "$timeout_s" php "$file" 2>&1)"
  rc=$?
  summary="$(printf '%s\n' "$out" | grep -m1 -E '^(OK|SKIP)' || printf '%s' "$out" | tail -1)"

  if [ "$rc" -ne 0 ]; then
    fail=$((fail + 1))
    failed="$failed $name"
    printf 'FAIL  %-34s %s\n' "$name" "$(printf '%s\n' "$out" | tail -1)"
  elif printf '%s' "$summary" | grep -q '^SKIP'; then
    skip=$((skip + 1))
    printf 'SKIP  %-34s %s\n' "$name" "$summary"
  else
    pass=$((pass + 1))
    printf 'PASS  %-34s %s\n' "$name" "$summary"
  fi
done

echo
echo "examples: ${pass} passed, ${skip} skipped, ${fail} failed"

if [ -n "$failed" ]; then
  echo "failed:$failed"
  exit 1
fi
