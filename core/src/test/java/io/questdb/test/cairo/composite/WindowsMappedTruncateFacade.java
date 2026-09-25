/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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


package io.questdb.test.cairo.composite;

import io.questdb.std.str.LPSZ;
import io.questdb.test.std.TestFilesFacadeImpl;

import java.util.HashMap;

/**
 * Copies the Windows rule that a file cannot be shortened while any view of it is still mapped: Windows fails
 * {@code SetEndOfFile} with {@code ERROR_USER_MAPPED_FILE}, where POSIX {@code ftruncate} succeeds. It lets the
 * other platforms reproduce what a Windows CI job sees.
 */
public class WindowsMappedTruncateFacade extends TestFilesFacadeImpl {
    public static final int ERROR_USER_MAPPED_FILE = 1224;
    private final HashMap<Long, String> fdPaths = new HashMap<>();
    private final HashMap<Long, Integer> mappedAddressRefs = new HashMap<>();
    private final HashMap<Long, String> mappedAddresses = new HashMap<>();
    private final HashMap<String, Integer> mappedCounts = new HashMap<>();
    private volatile boolean isTruncateRefused;
    private int refusedTruncateCount;

    @Override
    public synchronized boolean close(long fd) {
        fdPaths.remove(fd);
        return super.close(fd);
    }

    @Override
    public int errno() {
        if (isTruncateRefused) {
            isTruncateRefused = false;
            return ERROR_USER_MAPPED_FILE;
        }
        return super.errno();
    }

    public synchronized int getRefusedTruncateCount() {
        return refusedTruncateCount;
    }

    @Override
    public synchronized long mmap(long fd, long len, long offset, int flags, int memoryTag) {
        return trackMapping(fd, super.mmap(fd, len, offset, flags, memoryTag));
    }

    @Override
    public synchronized long mmapNoCache(long fd, long len, long offset, int flags, int memoryTag) {
        return trackMapping(fd, super.mmapNoCache(fd, len, offset, flags, memoryTag));
    }

    @Override
    public synchronized long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
        return moveMapping(fd, addr, super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag));
    }

    @Override
    public synchronized long mremapNoCache(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
        return moveMapping(fd, addr, super.mremapNoCache(fd, addr, previousSize, newSize, offset, mode, memoryTag));
    }

    @Override
    public synchronized void munmap(long address, long size, int memoryTag) {
        untrackMapping(address);
        super.munmap(address, size, memoryTag);
    }

    @Override
    public synchronized long openCleanRW(LPSZ name, long size) {
        return trackFd(name, super.openCleanRW(name, size));
    }

    @Override
    public synchronized long openRO(LPSZ name) {
        return trackFd(name, super.openRO(name));
    }

    @Override
    public synchronized long openRONoCache(LPSZ name) {
        return trackFd(name, super.openRONoCache(name));
    }

    @Override
    public synchronized long openRW(LPSZ name, int opts) {
        return trackFd(name, super.openRW(name, opts));
    }

    @Override
    public synchronized long openRWNoCache(LPSZ name, int opts) {
        return trackFd(name, super.openRWNoCache(name, opts));
    }

    @Override
    public synchronized boolean truncate(long fd, long size) {
        final String path = fdPaths.get(fd);
        if (path != null && mappedCounts.getOrDefault(path, 0) > 0 && size < length(fd)) {
            refusedTruncateCount++;
            isTruncateRefused = true;
            return false;
        }
        return super.truncate(fd, size);
    }

    private long moveMapping(long fd, long oldAddr, long newAddr) {
        if (newAddr != -1) {
            untrackMapping(oldAddr);
            trackMapping(fd, newAddr);
        }
        return newAddr;
    }

    private long trackFd(LPSZ name, long fd) {
        if (fd != -1) {
            fdPaths.put(fd, name.asAsciiCharSequence().toString());
        }
        return fd;
    }

    private long trackMapping(long fd, long addr) {
        final String path = fdPaths.get(fd);
        if (addr != -1 && path != null) {
            mappedAddresses.put(addr, path);
            mappedAddressRefs.merge(addr, 1, Integer::sum);
            mappedCounts.merge(path, 1, Integer::sum);
        }
        return addr;
    }

    private void untrackMapping(long addr) {
        final String path = mappedAddresses.get(addr);
        if (path != null) {
            if (mappedAddressRefs.merge(addr, -1, (a, b) -> a + b == 0 ? null : a + b) == null) {
                mappedAddresses.remove(addr);
            }
            mappedCounts.merge(path, -1, (a, b) -> a + b == 0 ? null : a + b);
        }
    }
}
