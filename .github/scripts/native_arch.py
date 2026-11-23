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

"""Check that a native library's binary format and CPU match its platform directory.

The release workflow downloads four Rust libraries into per-platform paths and
every later check compares bytes against that staged tree, so a library staged
under the wrong platform directory would pass checksum verification everywhere.
This module reads the object-file header and rejects such a mismatch.
"""

from __future__ import annotations

import argparse
import pathlib
import struct
import sys

# platform directory -> (object format, machine identifier)
PLATFORM_MACHINES: dict[str, tuple[str, int]] = {
    "linux-x86-64": ("elf", 0x3E),  # EM_X86_64
    "linux-aarch64": ("elf", 0xB7),  # EM_AARCH64
    "darwin-aarch64": ("macho", 0x0100000C),  # CPU_TYPE_ARM64
    "windows-x86-64": ("pe", 0x8664),  # IMAGE_FILE_MACHINE_AMD64
}

ELF_MAGIC = b"\x7fELF"
MACHO_64_LE_MAGIC = b"\xcf\xfa\xed\xfe"
PE_MAGIC = b"PE\0\0"


def describe(data: bytes) -> tuple[str, int] | None:
    """Return (format, machine) for a 64-bit ELF, Mach-O, or PE image, else None."""
    if data[:4] == ELF_MAGIC and len(data) >= 20 and data[4] == 2 and data[5] == 1:
        return "elf", struct.unpack_from("<H", data, 18)[0]
    if data[:4] == MACHO_64_LE_MAGIC and len(data) >= 8:
        return "macho", struct.unpack_from("<I", data, 4)[0]
    if data[:2] == b"MZ" and len(data) >= 0x40:
        pe_offset = struct.unpack_from("<I", data, 0x3C)[0]
        if len(data) >= pe_offset + 6 and data[pe_offset:pe_offset + 4] == PE_MAGIC:
            return "pe", struct.unpack_from("<H", data, pe_offset + 4)[0]
    return None


def mismatch(path: pathlib.Path, platform: str) -> str | None:
    """Return an error message when the file at path is not a library for platform."""
    expected = PLATFORM_MACHINES.get(platform)
    if expected is None:
        return f"unknown platform directory {platform!r} for {path}"
    with open(path, "rb") as handle:
        header = handle.read(4096)
    actual = describe(header)
    if actual is None:
        return f"{path} is not a 64-bit ELF, Mach-O, or PE image"
    if actual != expected:
        return (
            f"{path} is a {actual[0]} image for machine {actual[1]:#x}, "
            f"but {platform} requires a {expected[0]} image for machine {expected[1]:#x}"
        )
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("library", type=pathlib.Path, nargs="+", help="library whose parent directory names the platform")
    arguments = parser.parse_args()
    failures = [
        message
        for library in arguments.library
        for message in [mismatch(library, library.parent.name)]
        if message is not None
    ]
    for message in failures:
        print(f"ERROR: {message}", file=sys.stderr)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
