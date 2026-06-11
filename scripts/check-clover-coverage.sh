#!/usr/bin/env bash
# Fails if Clover statement coverage is below a threshold, reusing an already-generated
# coverage.xml (no extra PHPUnit run). Use after `composer coverage:clover:all` in CI.
#
# Usage:
#   bash scripts/check-clover-coverage.sh [threshold] [coverage.xml]

set -euo pipefail

threshold="${1:-90}"
file="${2:-coverage.xml}"

if ! [[ "$threshold" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "Invalid threshold: $threshold" >&2
  exit 2
fi

if [[ ! -f "$file" ]]; then
  echo "Coverage file not found: $file (run 'composer coverage:clover:all' first)" >&2
  exit 2
fi

# The project-level totals are the last <metrics .../> element in the Clover report.
line="$(grep -oE '<metrics[^>]*>' "$file" | tail -1)"

# A leading space before `statements=` excludes `coveredstatements=` (preceded by 'd').
statements="$(printf '%s' "$line" | sed -nE 's/.*[[:space:]]statements="([0-9]+)".*/\1/p')"
covered="$(printf '%s' "$line" | sed -nE 's/.*coveredstatements="([0-9]+)".*/\1/p')"

if [[ -z "$statements" || -z "$covered" || "$statements" -eq 0 ]]; then
  echo "Could not parse statement coverage from $file" >&2
  exit 2
fi

percent="$(awk -v c="$covered" -v s="$statements" 'BEGIN { printf "%.2f", (c / s) * 100 }')"

if ! awk -v p="$percent" -v t="$threshold" 'BEGIN { exit (p + 0 < t + 0) ? 1 : 0 }'; then
  echo "Coverage check failed: statement coverage ${percent}% is below ${threshold}% (${covered}/${statements})"
  exit 1
fi

echo "Coverage check passed: statement coverage ${percent}% >= ${threshold}% (${covered}/${statements})"
