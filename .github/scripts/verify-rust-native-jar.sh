#!/usr/bin/env bash
#
# Copyright (c) 2014-2019 Appsicle
# Copyright (c) 2019-2026 QuestDB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

if [[ "$#" -ne 2 ]]; then
    echo "usage: $0 <core-jar> <staged-native-root>" >&2
    exit 2
fi

python3 - "$1" "$2" "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" <<'PY'
import hashlib
import pathlib
import sys
import zipfile

jar_path = pathlib.Path(sys.argv[1])
staged_root = pathlib.Path(sys.argv[2])
sys.path.insert(0, sys.argv[3])
import native_arch  # noqa: E402
expected_paths = (
    "io/questdb/bin/linux-x86-64/libquestdbr.so",
    "io/questdb/bin/linux-aarch64/libquestdbr.so",
    "io/questdb/bin/darwin-aarch64/libquestdbr.dylib",
    "io/questdb/bin/windows-x86-64/questdbr.dll",
)


def fail(message):
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


if not jar_path.is_file():
    fail(f"core jar is not a file: {jar_path}")

sources = {}
for expected_path in expected_paths:
    source = staged_root / expected_path
    if not source.is_file() or source.is_symlink() or source.stat().st_size == 0:
        fail(f"staged Rust library is not a non-empty regular file: {expected_path}")
    architecture_error = native_arch.mismatch(source, source.parent.name)
    if architecture_error is not None:
        fail(architecture_error)
    sources[expected_path] = hashlib.sha256(source.read_bytes()).hexdigest()

try:
    with zipfile.ZipFile(jar_path) as archive:
        rust_entries = [
            entry
            for entry in archive.infolist()
            if not entry.is_dir() and "questdbr" in pathlib.PurePosixPath(entry.filename).name
        ]
        paths = [entry.filename for entry in rust_entries]
        if len(paths) != len(set(paths)):
            duplicate_paths = sorted(path for path in set(paths) if paths.count(path) > 1)
            fail("duplicate Rust entries: " + ", ".join(duplicate_paths))
        if set(paths) != set(expected_paths):
            missing = sorted(set(expected_paths) - set(paths))
            extra = sorted(set(paths) - set(expected_paths))
            details = []
            if missing:
                details.append("missing " + ", ".join(missing))
            if extra:
                details.append("unexpected " + ", ".join(extra))
            fail("Rust jar entries must exactly match the supported platforms: " + "; ".join(details))

        results = []
        for expected_path in expected_paths:
            entry = next(entry for entry in rust_entries if entry.filename == expected_path)
            if entry.file_size == 0:
                fail(f"empty Rust jar entry: {expected_path}")
            digest = hashlib.sha256(archive.read(entry)).hexdigest()
            if digest != sources[expected_path]:
                fail(f"checksum mismatch for Rust jar entry: {expected_path}")
            results.append((expected_path, digest))
except zipfile.BadZipFile as error:
    fail(f"invalid core jar: {error}")

for path, digest in sorted(results):
    print(f"{path} {digest}")
PY
