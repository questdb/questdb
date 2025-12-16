#!/bin/bash
#
# Check bytecode size of methods in compiled Java classes.
# Fails if any method exceeds the specified threshold (default: 8000 bytes).
#
# Usage: ./check-bytecode-size.sh [--threshold N] [--jar FILE]
#
# The JVM has a hard limit of 64KB (65535 bytes) for method bytecode,
# but methods over 8000 bytes typically cannot be JIT-compiled efficiently
# and may cause performance issues.
#

set -euo pipefail

THRESHOLD=8000
JAR_FILE=""

usage() {
    echo "Usage: $0 [--threshold N] --jar FILE"
    echo ""
    echo "Options:"
    echo "  --threshold N   Maximum allowed bytecode size (default: 8000)"
    echo "  --jar FILE      JAR file to check"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --threshold)
            THRESHOLD="$2"
            shift 2
            ;;
        --jar)
            JAR_FILE="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

if [[ -z "$JAR_FILE" ]]; then
    echo "Error: --jar must be specified"
    usage
fi

if [[ ! -f "$JAR_FILE" ]]; then
    echo "Error: JAR file not found: $JAR_FILE"
    exit 1
fi

echo "Checking bytecode size in JAR: $JAR_FILE"
echo "Threshold: $THRESHOLD bytes"
echo ""

# Extract all class names from the JAR and run javap on them all at once
# This is much faster than running javap on each class individually
CLASSES=$(jar tf "$JAR_FILE" | grep '\.class$' | sed 's/\.class$//' | tr '/' '.')

# Run javap with classpath set to the JAR and parse all output with awk
VIOLATIONS=$(echo "$CLASSES" | xargs javap -v -cp "$JAR_FILE" 2>/dev/null | awk -v threshold="$THRESHOLD" '
BEGIN {
    current_class = ""
    current_method = ""
    in_code = 0
    last_offset = 0
    violation_count = 0
}

# Match class declaration from Classfile line or class definition
/^Classfile / {
    # Reset for new class file
    current_class = ""
}

/^(public |protected |private |abstract |final |static )*(class|interface|enum) / {
    # Extract class name
    for (i = 1; i <= NF; i++) {
        if ($i == "class" || $i == "interface" || $i == "enum") {
            current_class = $(i+1)
            break
        }
    }
}

# Match method declaration (two leading spaces, ends with ;)
/^  [a-zA-Z].*\(.*\);$/ {
    current_method = $0
    gsub(/^[[:space:]]+/, "", current_method)
    gsub(/;$/, "", current_method)
    in_code = 0
    last_offset = 0
}

# Detect Code: block
/^[[:space:]]+Code:$/ {
    in_code = 1
    last_offset = 0
}

# Parse bytecode instruction offsets (format: "       N: instruction")
in_code && /^[[:space:]]+[0-9]+:/ {
    # Extract the offset number
    match($0, /^[[:space:]]+([0-9]+):/, arr)
    if (RSTART > 0) {
        sub(/^[[:space:]]+/, "")
        sub(/:.*/, "")
        last_offset = $0 + 0
    }
}

# Detect end of Code block
in_code && (/^[[:space:]]+LineNumberTable/ || /^[[:space:]]+LocalVariableTable/ || /^[[:space:]]+StackMapTable/ || /^[[:space:]]+Exception/ || /^[[:space:]]*$/ || /^  [a-zA-Z]/) {
    if (last_offset > 0) {
        # Approximate code size (last offset + estimated instruction size)
        code_size = last_offset + 3

        if (code_size > threshold) {
            violation_count++
            print current_class "." current_method ": " code_size " bytes"
        }
    }
    in_code = 0
}

END {
    exit (violation_count > 0 ? 1 : 0)
}
') || true

if [[ -n "$VIOLATIONS" ]]; then
    echo "========================================"
    echo "FAILED: Found methods exceeding bytecode size threshold of $THRESHOLD bytes"
    echo "========================================"
    echo ""
    echo "Violations:"
    echo "$VIOLATIONS"
    exit 1
else
    echo "PASSED: No methods exceed bytecode size threshold of $THRESHOLD bytes"
    exit 0
fi
