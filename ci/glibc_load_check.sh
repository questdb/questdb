#!/usr/bin/env bash
#
# Verify a committed, prebuilt native library still loads on an old glibc.
#
# Invoked inside an old-glibc container (amazonlinux:2 / almalinux:8) by the
# "C++ glibc check" stage in ci/test-hosted-pipeline.yml. It installs a Python
# interpreter and the C++ runtime, then dlopen()s the library: if the library
# was built against a newer glibc (or libstdc++) than the container provides,
# the load fails and so does this script. RTLD_LAZY defers undefined JNI
# symbols (bound against the JVM at runtime), so only the library's version
# requirements are exercised here.
#
# Usage: glibc_load_check.sh <lib-path-relative-to-/work>
set -euo pipefail

rel_lib="${1:?usage: glibc_load_check.sh <lib-path-relative-to-/work>}"
lib="/work/${rel_lib}"

if command -v dnf >/dev/null 2>&1; then
    dnf install -y python3 libstdc++ >/dev/null
else
    yum install -y python3 libstdc++ >/dev/null
fi

echo "container glibc: $(ldd --version | head -1)"
echo "checking: ${lib}"

python3 - "${lib}" <<'PY'
import ctypes
import os
import sys

lib = sys.argv[1]
try:
    ctypes.CDLL(lib, mode=os.RTLD_LAZY)
except OSError as e:
    sys.exit("FAIL: %s does not load on this glibc:\n  %s" % (lib, e))
print("OK: %s loads on this glibc" % lib)
PY
