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

# Exclusion list: methods expected to be large
# Format: "ClassName.methodName" or "ClassName.static {}" for static initializers
EXCLUSIONS=(
    "io.questdb.PropertyKey.static {}"
    "io.questdb.std.Decimal256.static {}"
    "io.questdb.griffin.engine.groupby.hyperloglog.BiasCorrectionData.static {}"
    "io.questdb.griffin.SqlCodeGenerator.static {}"
    "io.questdb.std.fastdouble.FastDoubleMath.static {}"
    "io.questdb.PropServerConfiguration.PropServerConfiguration"
)

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

# Build exclusion pattern for awk
EXCL_PATTERN=$(printf "%s\n" "${EXCLUSIONS[@]}" | paste -sd'|')

# Find methods over threshold, excluding known large methods
VIOLATIONS=$(echo "$CLASSES" | xargs javap -v -cp "$JAR_FILE" 2>/dev/null | awk -v threshold="$THRESHOLD" -v excl="$EXCL_PATTERN" '
BEGIN { split(excl, excludes, "|") }
/^public |^class |^interface |^enum / {
    for (i=1; i<=NF; i++) if ($i=="class" || $i=="interface" || $i=="enum") { class=$(i+1); break }
}
/^  [^ ].*[;]$/ {
    method=$0; gsub(/^  |;$/,"",method)
    # Extract simple method name
    if (method == "static {}") {
        simple = "static {}"
    } else {
        simple=method
        gsub(/^public |^private |^protected /, "", simple)
        sub(/\(.*/, "", simple)      # remove args
        n=split(simple, parts, " ")
        simple=parts[n]              # last word is method name
        sub(/.*\./, "", simple)      # remove package prefix (for constructors)
        if (simple == "{}") simple = class  # constructor
    }
    offset=0
}
/^[[:space:]]*[0-9]+:/ { gsub(/:/,"",$1); offset=$1+0 }
/LineNumberTable/ {
    if (offset > threshold) {
        key = class "." simple
        excluded = 0
        for (i in excludes) if (key == excludes[i]) { excluded = 1; break }
        if (!excluded) print class "." method ": " offset " bytes"
    }
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
