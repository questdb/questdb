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

import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Counts the flushes of WAL files while armed: {@code fsync}, {@code fdatasync}, {@code barrierFsync},
 * {@code fsyncAndClose}, a synchronous {@code msync} and {@code syncFileRange}, each once even when one routes
 * through another. A flush of a file in a {@code wal<N>} directory is writer-private; one under {@code txn_seq}
 * is the sequencer's. It tells the flushes the calling thread made while holding a role-switch lock
 * ({@link SuspensionScope#hasAnyRoleSwitchLock()}) from the others. An msync is attributed through the mapping
 * that holds its address.
 */
public class WalFlushCountingFacade extends TestFilesFacadeImpl {
    public static final int OTHER = 2;
    public static final int PRIVATE = 0;
    public static final int SEQUENCER = 1;
    private static final Pattern PRIVATE_PATH = Pattern.compile(".*[/\\\\]wal[0-9]+([/\\\\].*)?");
    private static final Pattern SEQUENCER_PATH = Pattern.compile(".*[/\\\\]txn_seq([/\\\\].*)?");
    private static final ThreadLocal<int[]> depth = ThreadLocal.withInitial(() -> new int[1]);
    // [kind][isUnderLock ? 1 : 0]
    private final int[][] counts = new int[3][2];
    private final Map<Long, String> fdPaths = new HashMap<>();
    private final List<String> log = new ArrayList<>();
    // mapping address -> {length, fd}
    private final TreeMap<Long, long[]> mappings = new TreeMap<>();
    private volatile boolean isArmed;

    public static String kindName(int kind) {
        return switch (kind) {
            case PRIVATE -> "private";
            case SEQUENCER -> "sequencer";
            default -> "other";
        };
    }

    public synchronized void arm() {
        for (int[] c : counts) {
            c[0] = 0;
            c[1] = 0;
        }
        log.clear();
        isArmed = true;
    }

    @Override
    public void barrierFsync(long fd) {
        countFd("barrierFsync", fd);
        depth.get()[0]++;
        try {
            super.barrierFsync(fd);
        } finally {
            depth.get()[0]--;
        }
    }

    @Override
    public boolean close(long fd) {
        synchronized (this) {
            fdPaths.remove(fd);
        }
        return super.close(fd);
    }

    public synchronized void disarm() {
        isArmed = false;
    }

    @Override
    public void fdatasync(long fd) {
        countFd("fdatasync", fd);
        depth.get()[0]++;
        try {
            super.fdatasync(fd);
        } finally {
            depth.get()[0]--;
        }
    }

    @Override
    public void fsync(long fd) {
        countFd("fsync", fd);
        depth.get()[0]++;
        try {
            super.fsync(fd);
        } finally {
            depth.get()[0]--;
        }
    }

    @Override
    public void fsyncAndClose(long fd) {
        countFd("fsyncAndClose", fd);
        synchronized (this) {
            fdPaths.remove(fd);
        }
        depth.get()[0]++;
        try {
            super.fsyncAndClose(fd);
        } finally {
            depth.get()[0]--;
        }
    }

    @Override
    public void fsyncDurable(long fd) {
        countFd("fsyncDurable", fd);
        depth.get()[0]++;
        try {
            super.fsyncDurable(fd);
        } finally {
            depth.get()[0]--;
        }
    }

    /**
     * The flushes of files of {@code kind} counted since {@link #arm()}, made under a role-switch lock or not.
     */
    public synchronized int getCount(int kind, boolean isUnderLock) {
        return counts[kind][isUnderLock ? 1 : 0];
    }

    public synchronized int getCount(int kind) {
        return counts[kind][0] + counts[kind][1];
    }

    /**
     * The flushes counted since {@link #arm()}, one line each: the call, whether under a role-switch lock, and
     * the file.
     */
    public synchronized String getLog() {
        return String.join("\n", log);
    }

    @Override
    public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
        final long addr = super.mmap(fd, len, offset, flags, memoryTag);
        if (addr > 0) {
            synchronized (this) {
                mappings.put(addr, new long[]{len, fd});
            }
        }
        return addr;
    }

    @Override
    public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
        final long newAddr = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        if (newAddr > 0) {
            synchronized (this) {
                mappings.remove(addr);
                mappings.put(newAddr, new long[]{newSize, fd});
            }
        }
        return newAddr;
    }

    @Override
    public void msync(long addr, long len, boolean async) {
        if (!async) {
            final long fd;
            synchronized (this) {
                final Map.Entry<Long, long[]> mapping = mappings.floorEntry(addr);
                fd = mapping != null && addr < mapping.getKey() + mapping.getValue()[0] ? mapping.getValue()[1] : -1;
            }
            countFd("msync", fd);
        }
        depth.get()[0]++;
        try {
            super.msync(addr, len, async);
        } finally {
            depth.get()[0]--;
        }
    }

    @Override
    public void munmap(long address, long size, int memoryTag) {
        synchronized (this) {
            mappings.remove(address);
        }
        super.munmap(address, size, memoryTag);
    }

    @Override
    public long openCleanRW(LPSZ name, long size) {
        return named(super.openCleanRW(name, size), name);
    }

    @Override
    public long openRO(LPSZ name) {
        return named(super.openRO(name), name);
    }

    @Override
    public long openRONoCache(LPSZ path) {
        return named(super.openRONoCache(path), path);
    }

    @Override
    public long openRW(LPSZ name, int opts) {
        return named(super.openRW(name, opts), name);
    }

    @Override
    public long openRWExisting(LPSZ name) {
        return named(super.openRWExisting(name), name);
    }

    @Override
    public long openRWNoCache(LPSZ name, int opts) {
        return named(super.openRWNoCache(name, opts), name);
    }

    @Override
    public int syncFileRange(long fd, long offset, long nbytes, int flags) {
        countFd("syncFileRange", fd);
        depth.get()[0]++;
        try {
            return super.syncFileRange(fd, offset, nbytes, flags);
        } finally {
            depth.get()[0]--;
        }
    }

    private static int kindOf(String path) {
        if (path == null) {
            return OTHER;
        }
        if (SEQUENCER_PATH.matcher(path).matches()) {
            return SEQUENCER;
        }
        return PRIVATE_PATH.matcher(path).matches() ? PRIVATE : OTHER;
    }

    private void countFd(String call, long fd) {
        if (!isArmed || depth.get()[0] > 0) {
            return;
        }
        final boolean isUnderLock = SuspensionScope.hasAnyRoleSwitchLock();
        synchronized (this) {
            final String path = fdPaths.get(fd);
            final int kind = kindOf(path);
            counts[kind][isUnderLock ? 1 : 0]++;
            log.add(call + (isUnderLock ? " locked " : " ") + path);
        }
    }

    private long named(long fd, LPSZ name) {
        if (fd > -1) {
            synchronized (this) {
                fdPaths.put(fd, Utf8s.stringFromUtf8BytesSafe(name));
            }
        }
        return fd;
    }
}
