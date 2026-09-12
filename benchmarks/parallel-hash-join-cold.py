#!/usr/bin/env python3
#      ___                  _   ____  ____
#     / _ \ _   _  ___  ___| |_|  _ \| __ )
#    | | | | | | |/ _ \/ __| __| | | |  _ \
#    | |_| | |_| |  __/\__ \ |_| |_| | |_) |
#     \__\_\\__,_|\___||___/\__|____/|____/
#
#   Copyright (c) 2014-2019 Appsicle
#   Copyright (c) 2019-2026 QuestDB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

"""Evict only this runner's dataset and verify Linux page-cache residency without touching pages."""
import ctypes
import os
from pathlib import Path
import sys


def resident(fd, size):
    pages = (size + os.sysconf("SC_PAGE_SIZE") - 1) // os.sysconf("SC_PAGE_SIZE")
    vector = (ctypes.c_ubyte * pages)()
    address = libc.mmap(None, size, 1, 1, fd, 0)  # PROT_READ, MAP_SHARED; no prefault
    if address == ctypes.c_void_p(-1).value:
        raise OSError(ctypes.get_errno(), "mmap")
    try:
        if libc.mincore(address, size, vector) != 0:
            raise OSError(ctypes.get_errno(), "mincore")
        return pages, sum(value & 1 for value in vector)
    finally:
        if libc.munmap(address, size) != 0:
            raise OSError(ctypes.get_errno(), "munmap")


if __name__ == "__main__":
    root = Path(sys.argv[1]).resolve(strict=True)
    if not root.name.startswith("hash-join-v1-") or not (root / "fact_solar_readings").is_dir():
        raise ValueError("expected a retained HashJoinGroupByV1Benchmark data directory")
    libc = ctypes.CDLL(None, use_errno=True)
    libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_long]
    libc.mmap.restype = ctypes.c_void_p
    libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_ubyte)]
    libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    pages = before = after = files = 0
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError("unexpected symlink in benchmark data")
        if not path.is_file() or path.stat().st_size == 0:
            continue
        fd = os.open(path, os.O_RDONLY)
        try:
            size = os.fstat(fd).st_size
            count, cached = resident(fd, size)
            pages += count
            before += cached
            os.fsync(fd)  # DONTNEED cannot evict dirty pages.
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            after += resident(fd, size)[1]
            files += 1
        finally:
            os.close(fd)
    print(f"# cold_cache files={files} pages={pages} resident_before={before} resident_after={after}", flush=True)
    if pages == 0 or after / pages > 0.01:
        raise RuntimeError("more than 1% of dataset pages remain resident")
