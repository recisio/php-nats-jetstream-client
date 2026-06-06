#!/usr/bin/env bash
# Validates that build/nats/jwt.conf is a well-formed, self-consistent NATS JWT auth config.
#
# The JWT fixtures are generated with random nkeys (scripts/regenerate-jwt-fixture.sh), so the
# committed config can never be reproduced byte-for-byte. The previous version of this check
# regenerated the fixtures and diffed them, which therefore ALWAYS reported drift and blocked the
# BDD/e2e preflight. This check instead verifies the config is structurally valid and internally
# consistent (deterministic, no Docker/nsc required). Use `composer fixture:jwt` to regenerate a
# fresh, matching fixture set locally before running the JWT integration tests.
#
# Usage:
#   bash scripts/check-jwt-fixture.sh

set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
conf="$root_dir/build/nats/jwt.conf"

fail() {
  echo "JWT fixture check failed: $1" >&2
  exit 1
}

[[ -f "$conf" ]] || fail "missing $conf"

# Must declare an operator, a system account, and the in-memory resolver with a preload block.
grep -qE '^operator:[[:space:]]*"[^"]+"' "$conf" || fail "missing 'operator:' path in jwt.conf"
grep -qE '^resolver:[[:space:]]*MEMORY' "$conf" || fail "missing 'resolver: MEMORY' in jwt.conf"
grep -qE '^resolver_preload:[[:space:]]*\{' "$conf" || fail "missing 'resolver_preload:' block in jwt.conf"

# The configured system account public key must itself be one of the preloaded accounts.
sys_acc="$(awk '/^system_account:/ {print $2; exit}' "$conf")"
[[ -n "$sys_acc" ]] || fail "missing 'system_account:' in jwt.conf"
grep -qE "^[[:space:]]*${sys_acc}:[[:space:]]*\"ey" "$conf" \
  || fail "system_account ${sys_acc} has no matching JWT in resolver_preload"

# Expect at least two preloaded account JWTs (system account + application account). Account public
# keys are base32 nkeys; the JWT values begin with the base64url of '{"' i.e. "ey".
preloaded="$(grep -cE '^[[:space:]]*[A-Z2-7]{40,}:[[:space:]]*"ey' "$conf" || true)"
[[ "$preloaded" -ge 2 ]] || fail "expected >= 2 preloaded account JWTs in jwt.conf, found ${preloaded}"

echo "JWT fixture check passed: jwt.conf is well-formed and self-consistent (${preloaded} preloaded accounts)."
