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

package io.questdb.test.cairo.wal;

import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Calls {@link #onFlush(String)} with the file's path before each {@code barrierFsync}, {@code fdatasync} and
 * synchronous {@code msync} of a file opened read-write whose name {@link #isTracked(LPSZ)} accepts. An msync is
 * attributed through the mapping that holds its address. A subclass that also hooks {@code fsync} calls
 * {@link #onFlush(long)} from its override.
 */
public abstract class WalFlushHookFacade extends TestFilesFacadeImpl {
    // mapping address -> {length, fd} of the tracked files
    private final ConcurrentSkipListMap<Long, long[]> mappings = new ConcurrentSkipListMap<>();
    private final Map<Long, String> paths = new ConcurrentHashMap<>();

    @Override
    public void barrierFsync(long fd) {
        onFlush(fd);
        super.barrierFsync(fd);
    }

    @Override
    public boolean close(long fd) {
        paths.remove(fd);
        return super.close(fd);
    }

    @Override
    public void fdatasync(long fd) {
        onFlush(fd);
        super.fdatasync(fd);
    }

    @Override
    public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
        final long addr = super.mmap(fd, len, offset, flags, memoryTag);
        if (paths.containsKey(fd)) {
            mappings.put(addr, new long[]{len, fd});
        }
        return addr;
    }

    @Override
    public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
        final long newAddr = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        if (paths.containsKey(fd)) {
            mappings.remove(addr);
            mappings.put(newAddr, new long[]{newSize, fd});
        }
        return newAddr;
    }

    @Override
    public void msync(long addr, long len, boolean async) {
        if (!async) {
            final Map.Entry<Long, long[]> mapping = mappings.floorEntry(addr);
            if (mapping != null && addr < mapping.getKey() + mapping.getValue()[0]) {
                onFlush(mapping.getValue()[1]);
            }
        }
        super.msync(addr, len, async);
    }

    @Override
    public void munmap(long address, long size, int memoryTag) {
        mappings.remove(address);
        super.munmap(address, size, memoryTag);
    }

    @Override
    public long openRW(LPSZ name, int opts) {
        final long fd = super.openRW(name, opts);
        if (fd > -1 && isTracked(name)) {
            paths.put(fd, Utf8s.stringFromUtf8BytesSafe(name));
        }
        return fd;
    }

    // Whether flushes of the file opened read-write under this name reach onFlush(String). Every file by default.
    protected boolean isTracked(LPSZ name) {
        return true;
    }

    // Calls onFlush(String) when fd is a tracked file.
    protected final void onFlush(long fd) {
        final String path = paths.get(fd);
        if (path != null) {
            onFlush(path);
        }
    }

    // Runs before the flush; a throw fails it.
    protected abstract void onFlush(String path);
}
