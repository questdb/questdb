#!/usr/bin/env bash
#
# Render a GitHub issue body for a failed pgwire fuzz workflow.

set -euo pipefail

artifact_dir="${ARTIFACT_DIR:-core/target/pgwire-fuzz/artifacts}"
run_url="${RUN_URL:-}"
target="${TARGET:-unknown}"
max_inline_bytes="${MAX_INLINE_BYTES:-8192}"
max_total_inline_bytes="${MAX_TOTAL_INLINE_BYTES:-32768}"

echo "Nightly pgwire fuzz failed for target ${target}."
echo
if [[ -n "$run_url" ]]; then
  echo "Run: ${run_url}"
  echo
fi

if [[ ! -d "$artifact_dir" ]]; then
  echo "No artifact directory was found at \`${artifact_dir}\`."
  exit 0
fi

mapfile -t artifacts < <(find "$artifact_dir" -maxdepth 1 -type f | sort)
if [[ ${#artifacts[@]} -eq 0 ]]; then
  echo "The artifact directory \`${artifact_dir}\` was empty."
  exit 0
fi

echo "Artifacts:"
for artifact in "${artifacts[@]}"; do
  size="$(wc -c < "$artifact" | tr -d ' ')"
  echo "- \`$(basename "$artifact")\` (${size} bytes)"
done

echo
echo "Small artifact excerpts:"

total_inline=0
for artifact in "${artifacts[@]}"; do
  size="$(wc -c < "$artifact" | tr -d ' ')"
  if (( size > max_inline_bytes || total_inline + size > max_total_inline_bytes )); then
    continue
  fi

  echo
  echo "#### $(basename "$artifact")"
  if LC_ALL=C grep -Iq . "$artifact"; then
    echo '```'
    sed -n '1,200p' "$artifact"
    echo '```'
  else
    echo '```base64'
    base64 "$artifact"
    echo '```'
  fi
  total_inline=$((total_inline + size))
done

if (( total_inline == 0 )); then
  echo
  echo "No artifacts were small enough to inline. See the workflow artifacts."
fi
