/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

public final class Files {

    public static final Charset UTF_8;
    public static final long PAGE_SIZE;
    public static final int DT_FILE = 8;
    public static final int DT_DIR = 4;
    public static final int MAP_RO = 1;
    public static final int MAP_RW = 2;
    public static final char SEPARATOR;
    public static final int POSIX_FADV_SEQUENTIAL;
    public static final int POSIX_FADV_RANDOM;
    public static final int POSIX_MADV_SEQUENTIAL;
    public static final int POSIX_MADV_RANDOM;
    public static final int FILES_RENAME_OK = 0;
    public static final int FILES_RENAME_ERR_EXDEV = 1;
    public static final int FILES_RENAME_ERR_OTHER = 2;
    static final AtomicLong OPEN_FILE_COUNT = new AtomicLong();
    private static LongHashSet openFds;
    public static final int WINDOWS_ERROR_FILE_EXISTS = 0x50;

    private Files() {
        // Prevent construction.
    }

    public native static boolean allocate(long fd, long size);

    public native static long append(long fd, long address, long len);

    public static synchronized boolean auditClose(long fd) {
        if (fd < 0) {
            throw new IllegalStateException("Invalid fd " + fd);
        }
        if (openFds.remove(fd) == -1) {
            throw new IllegalStateException("fd " + fd + " is already closed!");
        }
        return true;
    }

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

    public static long bumpFileCount(long fd) {
        if (fd != -1) {
            //noinspection AssertWithSideEffects
            assert auditOpen(fd);
            OPEN_FILE_COUNT.incrementAndGet();
        }
        return fd;
    }

    public static long ceilPageSize(long size) {
        return ((size + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;
    }

    public static int close(long fd) {
        assert auditClose(fd);
        int res = close0(fd);
        if (res == 0) {
            OPEN_FILE_COUNT.decrementAndGet();
        }
        return res;
    }

    public static native int copy(long from, long to);

    public static int copy(LPSZ from, LPSZ to) {
        return copy(from.address(), to.address());
    }

    /**
     * close(fd) should be used instead of this method in most cases
     * unless you don't need close sys call to happen.
     */
    public static void decrementFileCount(long fd) {
        assert auditClose(fd);
        OPEN_FILE_COUNT.decrementAndGet();
    }

    public static native boolean exists(long fd);

    public static boolean exists(LPSZ lpsz) {
        return lpsz != null && exists0(lpsz.address());
    }

    public static native long noop();

    public static void fadvise(long fd, long offset, long len, int advise) {
        if (Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64) {
            fadvise0(fd, offset, len, advise);
        }
    }

    public static void madvise(long address, long len, int advise) {
        if (Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64) {
            madvise0(address, len, advise);
        }
    }

    public static native void fadvise0(long fd, long offset, long len, int advise);

    public static native void madvise0(long address, long len, int advise);

    public native static void findClose(long findPtr);

    public static long findFirst(LPSZ lpsz) {
        return findFirst(lpsz.address());
    }

    public native static long findName(long findPtr);

    public native static int findNext(long findPtr);

    public native static int findType(long findPtr);

    public static long floorPageSize(long size) {
        return size - size % PAGE_SIZE;
    }

    public static native int fsync(long fd);

    /**
     * Detects if filesystem is supported by QuestDB. The function returns both FS magic and name. Both
     * can be presented to user even if file system is not supported.
     *
     * @param lpszName existing path on the file system. The name of the filesystem is written to this
     *                 address, therefore name should have at least 128 byte capacity
     * @return 0 when OS call failed, errno should be checked. Negative number is file system magic that is supported
     * positive number is magic that is not supported.
     */
    public static int getFileSystemStatus(LPSZ lpszName) {
        assert lpszName.capacity() > 127;
        return getFileSystemStatus(lpszName.address());
    }

    public static long getLastModified(LPSZ lpsz) {
        return getLastModified(lpsz.address());
    }

    public static String getOpenFdDebugInfo() {
        if (openFds != null) {
            return openFds.toString();
        }
        return null;
    }

    public static long getOpenFileCount() {
        return OPEN_FILE_COUNT.get();
    }

    public native static long getStdOutFd();

    public static native int hardLink(long lpszSrc, long lpszHardLink);

    public static int hardLink(LPSZ src, LPSZ hardLink) {
        return hardLink(src.address(), hardLink.address());
    }

    public static boolean isDir(long pUtf8NameZ, long type, StringSink nameSink) {
        if (type == DT_DIR) {
            nameSink.clear();
            Chars.utf8DecodeZ(pUtf8NameZ, nameSink);
            return notDots(nameSink);
        }
        return false;
    }

    public static boolean isDir(long pUtf8NameZ, long type) {
        return type == DT_DIR && notDots(pUtf8NameZ);
    }

    public static boolean isDots(CharSequence name) {
        return Chars.equals(name, '.') || Chars.equals(name, "..");
    }

    public static long length(LPSZ lpsz) {
        return length0(lpsz.address());
    }

    public native static long length(long fd);

    public static native int lock(long fd);

    public static int mkdir(Path path, int mode) {
        return mkdir(path.address(), mode);
    }

    public static int mkdirs(Path path, int mode) {
        for (int i = 0, n = path.length(); i < n; i++) {
            char c = path.charAt(i);
            if (c == Files.SEPARATOR) {

                // do not attempt to create '/' on linux or 'C:\' on Windows
                if ((i == 0 && Os.type != Os.WINDOWS) || (i == 2 && Os.type == Os.WINDOWS && path.charAt(1) == ':')) {
                    continue;
                }

                // replace separator we just found with \0
                // temporarily truncate path to the directory we need to create
                path.$at(i);
                if (path.length() > 0 && !Files.exists(path)) {
                    int r = Files.mkdir(path, mode);
                    if (r != 0) {
                        path.put(i, Files.SEPARATOR);
                        return r;
                    }
                }
                path.put(i, Files.SEPARATOR);
            }
        }
        return 0;
    }

    public static long mmap(long fd, long len, long offset, int flags, int memoryTag) {
        long address = mmap0(fd, len, offset, flags, 0);
        if (address != -1) {
            Unsafe.recordMemAlloc(len, memoryTag);
        }
        return address;
    }

    public static long mremap(long fd, long address, long previousSize, long newSize, long offset, int flags, int memoryTag) {
        Unsafe.recordMemAlloc(-previousSize, memoryTag);
        address = mremap0(fd, address, previousSize, newSize, offset, flags);
        if (address != -1) {
            Unsafe.recordMemAlloc(newSize, memoryTag);
        }
        return address;
    }

    public static native int msync(long addr, long len, boolean async);

    public static void munmap(long address, long len, int memoryTag) {
        if (address != 0 && munmap0(address, len) != -1) {
            Unsafe.recordMemAlloc(-len, memoryTag);
        }
    }

    public static boolean notDots(CharSequence value) {
        final int len = value.length();
        if (len > 2) {
            return true;
        }
        if (value.charAt(0) != '.') {
            return true;
        }
        return len == 2 && value.charAt(1) != '.';
    }

    public static boolean notDots(long pUtf8NameZ) {
        final byte b0 = Unsafe.getUnsafe().getByte(pUtf8NameZ);

        if (b0 != '.') {
            return true;
        }

        final byte b1 = Unsafe.getUnsafe().getByte(pUtf8NameZ + 1);
        return b1 != 0 && (b1 != '.' || Unsafe.getUnsafe().getByte(pUtf8NameZ + 2) != 0);
    }

    public static long openAppend(LPSZ lpsz) {
        return bumpFileCount(openAppend(lpsz.address()));
    }

    public static long openCleanRW(LPSZ lpsz, long size) {
        return bumpFileCount(openCleanRW(lpsz.address(), size));
    }

    public native static long openCleanRW(long lpszName, long size);

    public static long openRO(LPSZ lpsz) {
        return bumpFileCount(openRO(lpsz.address()));
    }

    public static long openRW(LPSZ lpsz) {
        return bumpFileCount(openRW(lpsz.address()));
    }

    public static long openRW(LPSZ lpsz, long opts) {
        return bumpFileCount(openRWOpts(lpsz.address(), opts));
    }

    public native static long read(long fd, long address, long len, long offset);

    public native static long readULong(long fd, long offset);

    public static boolean remove(LPSZ lpsz) {
        return remove(lpsz.address());
    }

    public static int rename(LPSZ oldName, LPSZ newName) {
        return rename(oldName.address(), newName.address());
    }

    public static int rmdir(Path path) {
        long p = findFirst(path.address());
        int len = path.length();
        int errno = -1;
        if (p > 0) {
            try {
                do {
                    long lpszName = findName(p);
                    path.trimTo(len).concat(lpszName).$();
                    if (findType(p) == DT_DIR) {
                        if (strcmp(lpszName, "..") || strcmp(lpszName, ".")) {
                            continue;
                        }

                        if ((errno = rmdir(path)) == 0) {
                            continue;
                        }

                    } else {
                        if ((remove(path.address()))) {
                            continue;
                        }
                        errno = Os.errno();
                    }
                    return errno;
                } while (findNext(p) > 0);
            } finally {
                findClose(p);
            }
            if (rmdir(path.trimTo(len).$().address())) {
                return 0;
            }
            return Os.errno();
        }

        return errno;
    }

    public static boolean setLastModified(LPSZ lpsz, long millis) {
        return setLastModified(lpsz.address(), millis);
    }

    public static native int sync();

    public static boolean touch(LPSZ lpsz) {
        long fd = openRW(lpsz);
        boolean result = fd > 0;
        if (result) {
            close(fd);
        }
        return result;
    }

    public native static boolean truncate(long fd, long size);

    public native static long write(long fd, long address, long len, long offset);

    private native static int getPosixFadvRandom();

    private native static int getPosixFadvSequential();

    private native static int getPosixMadvRandom();

    private native static int getPosixMadvSequential();

    private static native int getFileSystemStatus(long lpszName);

    private native static int close0(long fd);

    private static native boolean exists0(long lpsz);

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

    private static native long mmap0(long fd, long len, long offset, int flags, long baseAddress);

    private native static long getPageSize();

    private native static boolean remove(long lpsz);

    private native static boolean rmdir(long lpsz);

    private native static long getLastModified(long lpszName);

    private native static long length0(long lpszName);

    private native static int mkdir(long lpszPath, int mode);

    private native static long openRO(long lpszName);

    private native static long openRW(long lpszName);

    private native static long openRWOpts(long lpszName, long opts);

    private native static long openAppend(long lpszName);

    //caller must call findClose to free allocated struct 
    private native static long findFirst(long lpszName);

    private native static boolean setLastModified(long lpszName, long millis);

    private static native int rename(long lpszOld, long lpszNew);

    static {
        Os.init();
        UTF_8 = StandardCharsets.UTF_8;
        PAGE_SIZE = getPageSize();
        SEPARATOR = File.separatorChar;
        if (Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64) {
            POSIX_FADV_RANDOM = getPosixFadvRandom();
            POSIX_FADV_SEQUENTIAL = getPosixFadvSequential();
            POSIX_MADV_RANDOM = getPosixMadvRandom();
            POSIX_MADV_SEQUENTIAL = getPosixMadvSequential();
        } else {
            POSIX_FADV_SEQUENTIAL = -1;
            POSIX_FADV_RANDOM = -1;
            POSIX_MADV_SEQUENTIAL = -1;
            POSIX_MADV_RANDOM = -1;
        }
    }
}
