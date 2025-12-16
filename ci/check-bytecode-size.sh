#!/bin/bash
#
# Check bytecode size of methods in compiled Java classes.
# Fails if any method exceeds the specified threshold (default: 8000 bytes).
#
# Usage: ./check-bytecode-size.sh [--threshold N] --jar FILE
#

set -euo pipefail

THRESHOLD=8000
JAR_FILE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --threshold) THRESHOLD="$2"; shift 2 ;;
        --jar) JAR_FILE="$2"; shift 2 ;;
        *) echo "Usage: $0 [--threshold N] --jar FILE"; exit 1 ;;
    esac
done

[[ -z "$JAR_FILE" ]] && { echo "Error: --jar required"; exit 1; }
[[ ! -f "$JAR_FILE" ]] && { echo "Error: JAR not found: $JAR_FILE"; exit 1; }

echo "Checking bytecode size in: $JAR_FILE"
echo "Threshold: $THRESHOLD bytes"
echo ""

CLASSES=$(jar tf "$JAR_FILE" | grep '\.class$' | sed 's/\.class$//' | tr '/' '.')

# Simple approach: find the last bytecode offset before LineNumberTable for each method
VIOLATIONS=$(echo "$CLASSES" | xargs javap -v -cp "$JAR_FILE" 2>/dev/null | awk -v threshold="$THRESHOLD" '
/^public |^class |^interface |^enum / {
    for (i=1; i<=NF; i++) if ($i=="class" || $i=="interface" || $i=="enum") { class=$(i+1); break }
}
/^  [^ ].*[;]$/ { method=$0; gsub(/^  |;$/,"",method); offset=0 }
/^[[:space:]]*[0-9]+:/ { gsub(/:/,"",$1); offset=$1+0 }
/LineNumberTable/ {
    if (offset > threshold) print class "." method ": " offset " bytes"
    offset=0
}
')

if [[ -n "$VIOLATIONS" ]]; then
    echo "FAILED - methods over $THRESHOLD bytes:"
    echo "$VIOLATIONS"
    exit 1
else
    echo "PASSED"
    exit 0
fi
