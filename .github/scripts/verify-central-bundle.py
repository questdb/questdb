#!/usr/bin/env python3
#
# Copyright (c) 2014-2019 Appsicle
# Copyright (c) 2019-2026 QuestDB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Verify the exact signed Maven Central bundle for the release core artifact."""

from __future__ import annotations

import argparse
import collections
import hashlib
import io
import pathlib
import sys
import xml.etree.ElementTree as element_tree
import zipfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import native_arch  # noqa: E402

RUST_NATIVE_PATHS = {
    "io/questdb/bin/linux-x86-64/libquestdbr.so",
    "io/questdb/bin/linux-aarch64/libquestdbr.so",
    "io/questdb/bin/darwin-aarch64/libquestdbr.dylib",
    "io/questdb/bin/windows-x86-64/questdbr.dll",
}
SIDECARS = (".asc", ".md5", ".sha1", ".sha256", ".sha512")


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def expected_entries(group_id: str, artifact_id: str, version: str) -> set[str]:
    directory = f"{group_id.replace('.', '/')}/{artifact_id}/{version}/"
    artifacts = (
        f"{artifact_id}-{version}.pom",
        f"{artifact_id}-{version}.jar",
        f"{artifact_id}-{version}-sources.jar",
        f"{artifact_id}-{version}-javadoc.jar",
        f"{artifact_id}-{version}.zip",
    )
    return {
        directory + artifact + suffix
        for artifact in artifacts
        for suffix in ("", *SIDECARS)
    }


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("bundle", type=pathlib.Path)
    parser.add_argument("main_jar", type=pathlib.Path)
    parser.add_argument("staged_native_root", type=pathlib.Path)
    parser.add_argument("--group-id", default="org.questdb")
    parser.add_argument("--artifact-id", default="questdb")
    parser.add_argument("--version", required=True)
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()
    if not arguments.bundle.is_file():
        fail(f"Central bundle is not a file: {arguments.bundle}")
    if not arguments.main_jar.is_file():
        fail(f"verified core jar is not a file: {arguments.main_jar}")

    directory = f"{arguments.group_id.replace('.', '/')}/{arguments.artifact_id}/{arguments.version}/"
    expected = expected_entries(arguments.group_id, arguments.artifact_id, arguments.version)
    main_jar_entry = directory + f"{arguments.artifact_id}-{arguments.version}.jar"
    pom_entry = directory + f"{arguments.artifact_id}-{arguments.version}.pom"

    try:
        with zipfile.ZipFile(arguments.bundle) as bundle:
            entries = [entry.filename for entry in bundle.infolist() if not entry.is_dir()]
            actual = collections.Counter(entries)
            expected_counter = collections.Counter(expected)
            if actual != expected_counter:
                missing = sorted((expected_counter - actual).elements())
                unexpected = sorted((actual - expected_counter).elements())
                duplicates = sorted(entry for entry, count in actual.items() if count > 1)
                fail(
                    "Central bundle entries differ from the exact signed allowlist: "
                    f"missing={missing}, unexpected={unexpected}, duplicates={duplicates}"
                )

            pom = element_tree.fromstring(bundle.read(pom_entry))
            if any("SNAPSHOT" in (node.text or "") for node in pom.iter()):
                fail("Central bundled POM contains a SNAPSHOT dependency")

            bundled_jar = bundle.read(main_jar_entry)
    except (OSError, element_tree.ParseError, zipfile.BadZipFile) as error:
        fail(f"invalid Central bundle: {error}")

    if bundled_jar != arguments.main_jar.read_bytes():
        fail("Central bundled core jar differs from the verified core jar")

    try:
        with zipfile.ZipFile(io.BytesIO(bundled_jar)) as jar:
            native_entries = [
                entry.filename
                for entry in jar.infolist()
                if "questdbr" in pathlib.PurePosixPath(entry.filename).name
            ]
            if collections.Counter(native_entries) != collections.Counter(RUST_NATIVE_PATHS):
                fail("Central bundled core jar does not contain exactly four Rust entries")
            for entry in native_entries:
                source = arguments.staged_native_root / entry
                if not source.is_file() or source.is_symlink() or source.stat().st_size == 0:
                    fail(f"staged Rust native is not a non-empty regular file: {entry}")
                architecture_error = native_arch.mismatch(source, source.parent.name)
                if architecture_error is not None:
                    fail(architecture_error)
                if hashlib.sha256(jar.read(entry)).digest() != hashlib.sha256(source.read_bytes()).digest():
                    fail(f"Central bundled native checksum mismatch: {entry}")
    except (OSError, zipfile.BadZipFile) as error:
        fail(f"invalid bundled core jar: {error}")

    print("Central bundle exact signed allowlist, POM, jar identity, and native checks passed")


if __name__ == "__main__":
    main()
