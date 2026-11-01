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

if [[ "$#" -ne 3 ]]; then
    echo "usage: $0 <runtime-archive.tar.gz> <linux-x86-64|windows-x86-64> <staged-native-root>" >&2
    exit 2
fi

python3 - "$1" "$2" "$3" <<'PY'
import hashlib
import pathlib
import sys
import tarfile

archive_path = pathlib.Path(sys.argv[1])
platform = sys.argv[2]
staged_root = pathlib.Path(sys.argv[3])
source_paths = {
    "linux-x86-64": "io/questdb/bin/linux-x86-64/libquestdbr.so",
    "windows-x86-64": "io/questdb/bin/windows-x86-64/questdbr.dll",
}


def fail(message):
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


if platform not in source_paths:
    fail(f"unsupported runtime platform: {platform}")
if not archive_path.is_file():
    fail(f"runtime archive is not a file: {archive_path}")

source_path = source_paths[platform]
source = staged_root / source_path
if not source.is_file() or source.is_symlink() or source.stat().st_size == 0:
    fail(f"staged Rust library is not a non-empty regular file: {source_path}")
source_digest = hashlib.sha256(source.read_bytes()).hexdigest()
expected_name = pathlib.PurePosixPath(source_path).name

try:
    with tarfile.open(archive_path, "r:gz") as archive:
        rust_members = [
            member
            for member in archive.getmembers()
            if member.isfile() and "questdbr" in pathlib.PurePosixPath(member.name).name
        ]
        if len(rust_members) != 1:
            fail(f"runtime archive must contain exactly one Rust library, got {len(rust_members)}")
        member = rust_members[0]
        member_path = pathlib.PurePosixPath(member.name)
        if len(member_path.parts) != 3 or member_path.parts[1] != "lib" or member_path.name != expected_name:
            fail(f"Rust runtime library must use <root>/lib/{expected_name}: {member.name}")
        if member.size == 0:
            fail(f"empty Rust runtime library: {member.name}")
        member_file = archive.extractfile(member)
        if member_file is None:
            fail(f"cannot read Rust runtime library: {member.name}")
        digest = hashlib.sha256(member_file.read()).hexdigest()
except (tarfile.TarError, OSError) as error:
    fail(f"invalid runtime archive: {error}")

if digest != source_digest:
    fail(f"runtime checksum mismatch for {member.name}")
print(f"{platform} {member.name} {digest}")
PY
