#!/usr/bin/env python3
"""Page-cache residency of every regular file under a directory, via mmap + mincore.

    ./residency.py <database-root>

This is how the cold-restore cell's eviction was checked: the harness pauses after
--restart-cache=cold has fsynced and advised every file (a throwaway sleep, not committed),
and this script, run against the database root during the pause, reports what the kernel
still holds. Per top-level directory and in total; the checkpoint tree is the part that
matters, and it read 0 of 14.4 MB resident at 100,000 keys. Linux only, no root needed.
"""
import ctypes, ctypes.util, mmap, os, sys

libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_char_p]
libc.mmap.restype = ctypes.c_void_p
libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_long]
libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
PAGE = os.sysconf("SC_PAGE_SIZE")
PROT_NONE, MAP_SHARED = 0, 1

def residency(path):
    size = os.path.getsize(path)
    if size == 0:
        return 0, 0
    fd = os.open(path, os.O_RDONLY)
    try:
        addr = libc.mmap(None, size, PROT_NONE, MAP_SHARED, fd, 0)
        if addr == ctypes.c_void_p(-1).value or addr is None:
            return 0, 0
        pages = (size + PAGE - 1) // PAGE
        vec = ctypes.create_string_buffer(pages)
        if libc.mincore(addr, size, vec) != 0:
            libc.munmap(addr, size)
            return 0, 0
        resident = sum(b & 1 for b in vec.raw)
        libc.munmap(addr, size)
        return resident, pages
    finally:
        os.close(fd)

root = sys.argv[1]
per_dir = {}
total_res = total_pages = 0
for dirpath, _, files in os.walk(root):
    for name in files:
        p = os.path.join(dirpath, name)
        try:
            r, n = residency(p)
        except OSError:
            continue
        key = os.path.relpath(dirpath, root).split(os.sep)[0]
        a, b = per_dir.get(key, (0, 0))
        per_dir[key] = (a + r, b + n)
        total_res += r; total_pages += n
for key, (r, n) in sorted(per_dir.items()):
    if n:
        print(f"{key:40s} resident {r*PAGE/1e6:9.1f} MB of {n*PAGE/1e6:9.1f} MB  ({100.0*r/n:5.1f}%)")
print(f"{'TOTAL':40s} resident {total_res*PAGE/1e6:9.1f} MB of {total_pages*PAGE/1e6:9.1f} MB  ({100.0*total_res/max(1,total_pages):5.1f}%)")
