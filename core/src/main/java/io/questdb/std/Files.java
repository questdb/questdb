/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.std;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

public final class Files {

    public static final Charset UTF_8;
    public static final long PAGE_SIZE;
    public static final int DT_DIR = 4;

    public static final int MAP_RO = 1;
    public static final int MAP_RW = 2;
    public static final char SEPARATOR;

    static final AtomicLong OPEN_FILE_COUNT = new AtomicLong();

    public static native int copy(long from, long to);

    public static int copy(LPSZ from, LPSZ to) {
        return copy(from.address(), to.address());
    }

    static {
        Os.init();
        UTF_8 = StandardCharsets.UTF_8;
        PAGE_SIZE = getPageSize();
        SEPARATOR = Os.type == Os.WINDOWS ? '\\' : '/';
    }

    private Files() {
    } // Prevent construction.

    public native static long append(long fd, long address, long len);

    public static int close(long fd) {
        assert auditClose(fd);
        int res = close0(fd);
        if (res == 0) {
            OPEN_FILE_COUNT.decrementAndGet();
        }
        return res;
    }

    public static native boolean exists(long fd);

    public static boolean exists(LPSZ lpsz) {
        return lpsz != null && exists0(lpsz.address());
    }

    private static native boolean exists0(long lpsz);

    public native static void findClose(long findPtr);

    public static long findFirst(LPSZ lpsz) {
        return findFirst(lpsz.address());
    }

    public native static long findName(long findPtr);

    public native static int findNext(long findPtr);

    public native static int findType(long findPtr);

    public static long getLastModified(LPSZ lpsz) {
        return getLastModified(lpsz.address());
    }

    public static long getOpenFileCount() {
        return OPEN_FILE_COUNT.get();
    }

    public native static long getStdOutFd();

    public static boolean isDots(CharSequence name) {
        return Chars.equals(name, '.') || Chars.equals(name, "..");
    }

    public static long length(LPSZ lpsz) {
        return length0(lpsz.address());
    }

    public native static long length(long fd);

    public static native int lock(long fd);

    public static native int msync(long addr, long len, boolean async);

    public static native int fsync(long fd);

    public static int mkdir(LPSZ path, int mode) {
        return mkdir(path.address(), mode);
    }

    public static int mkdirs(LPSZ path, int mode) {
        try (Path pp = new Path()) {
            for (int i = 0, n = path.length(); i < n; i++) {
                char c = path.charAt(i);
                if (c == File.separatorChar) {

                    if (i == 2 && Os.type == Os.WINDOWS && path.charAt(1) == ':') {
                        pp.put(c);
                        continue;
                    }

                    pp.$();
                    if (pp.length() > 0 && !Files.exists(pp)) {
                        int r = Files.mkdir(pp, mode);
                        if (r != 0) {
                            return r;
                        }
                    }
                    pp.chopZ();
                }
                pp.put(c);
            }
        }
        return 0;
    }

    public static long mmap(long fd, long len, long offset, int flags) {
        long address = mmap0(fd, len, offset, flags);
        if (address != -1) {
            Unsafe.recordMemAlloc(len);
        }
        return address;
    }

    public static long mremap(long fd, long address, long previousSize, long newSize, long offset, int flags) {
        Unsafe.recordMemAlloc(-previousSize);
        address = mremap0(fd, address, previousSize, newSize, offset, flags);
        if (address != -1) {
            Unsafe.recordMemAlloc(newSize);
        }
        return address;
    }

    public static void munmap(long address, long len) {
        if (address != 0 && munmap0(address, len) != -1) {
            Unsafe.recordMemAlloc(-len);
        }
    }

    public static long openAppend(LPSZ lpsz) {
        long fd = openAppend(lpsz.address());
        if (fd != -1) {
            assert auditOpen(fd);
            bumpFileCount();
        }
        return fd;
    }

    public static long openRO(LPSZ lpsz) {
        long fd = openRO(lpsz.address());
        if (fd != -1) {
            assert auditOpen(fd);
            bumpFileCount();
        }
        return fd;
    }

    public static long openRW(LPSZ lpsz) {
        long fd = openRW(lpsz.address());
        if (fd != -1) {
            assert auditOpen(fd);
            bumpFileCount();
        }
        return fd;
    }

    public static void bumpFileCount() {
        OPEN_FILE_COUNT.incrementAndGet();
    }

    public native static long read(long fd, long address, long len, long offset);

    public static boolean remove(LPSZ lpsz) {
        return remove(lpsz.address());
    }

    public static boolean rename(LPSZ oldName, LPSZ newName) {
        return rename(oldName.address(), newName.address());
    }

    public static boolean rmdir(Path path) {
        long p = findFirst(path.address());
        int len = path.length();
        boolean clean = true;

        if (p > 0) {
            try {
                do {
                    long lpszName = findName(p);
                    path.trimTo(len).concat(lpszName).$();
                    if (findType(p) == DT_DIR) {
                        if (strcmp(lpszName, "..") || strcmp(lpszName, ".")) {
                            continue;
                        }

                        if (rmdir(path)) {
                            continue;
                        }

                        clean = false;
                    } else {
                        if (remove(path.address())) {
                            continue;
                        }

                        clean = false;

                    }
                } while (findNext(p) > 0);
            } finally {
                findClose(p);
            }
            return rmdir(path.trimTo(len).$().address()) && clean;
        }

        return false;
    }

    public native static long sequentialRead(long fd, long address, int len);

    public static boolean setLastModified(LPSZ lpsz, long millis) {
        return setLastModified(lpsz.address(), millis);
    }

    public static boolean touch(LPSZ lpsz) {
        long fd = openRW(lpsz);
        boolean result = fd > 0;
        if (result) {
            close(fd);
        }
        return result;
    }

    public native static boolean truncate(long fd, long size);

    public native static boolean allocate(long fd, long size);

    public native static long write(long fd, long address, long len, long offset);

    private native static int close0(long fd);

    private static boolean strcmp(long lpsz, CharSequence s) {
        int len = s.length();
        for (int i = 0; i < len; i++) {
            byte b = Unsafe.getUnsafe().getByte(lpsz + i);
            if (b == 0 || b != (byte) s.charAt(i)) {
                return false;
            }
        }
        return Unsafe.getUnsafe().getByte(lpsz + len) == 0;
    }

    private static native int munmap0(long address, long len);

    private static native long mremap0(long fd, long address, long previousSize, long newSize, long offset, int flags);

    private static native long mmap0(long fd, long len, long offset, int flags);

    private native static long getPageSize();

    private native static boolean remove(long lpsz);

    private native static boolean rmdir(long lpsz);

    private native static long getLastModified(long lpszName);

    private native static long length0(long lpszName);

    private native static int mkdir(long lpszPath, int mode);

    private native static long openRO(long lpszName);

    private native static long openRW(long lpszName);

    private native static long openAppend(long lpszName);

    private native static long findFirst(long lpszName);

    private native static boolean setLastModified(long lpszName, long millis);

    private static native boolean rename(long lpszOld, long lpszNew);

    private static LongHashSet openFds;

    public static synchronized boolean auditOpen(long fd) {
        if (null == openFds) {
            openFds = new LongHashSet();
        }
        if (fd < 0) {
            throw new IllegalStateException("Invalid fd " + fd);
        }
        if (openFds.contains(fd)) {
            throw new IllegalStateException("fd " + fd + " is already open");
        }
        openFds.add(fd);
        return true;
    }

    public static synchronized boolean auditClose(long fd) {
        if (fd < 0) {
            throw new IllegalStateException("Invalid fd " + fd);
        }
        if (openFds.remove(fd) == -1) {
            throw new IllegalStateException("fd " + fd + " is already closed!");
        }
        return true;
    }
}
