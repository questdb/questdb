/*******************************************************************************
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

package io.questdb.test.std;

import io.questdb.std.str.LPSZ;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link TestFilesFacadeImpl} that ATTRIBUTES every durability barrier to the FILE it acted on, so a
 * test can assert "{@code _txn} of THIS table was msync'd N times during this commit" rather than merely
 * counting anonymous barriers.
 *
 * <p>Attribution works in two hops, both of which the facade observes:
 * <ol>
 *   <li>{@code openRW}/{@code openRO}/{@code openCleanRW} give fd -&gt; path;</li>
 *   <li>{@code mmap}/{@code mremap} give [address, address+len) -&gt; fd, hence -&gt; path.</li>
 * </ol>
 * {@code fsync}/{@code fdatasync}/{@code fsyncAndClose} are attributed directly by fd;
 * {@code msync(addr, len, ..)} is attributed by locating the mapping whose range contains {@code addr}.
 * {@code syncfs} is filesystem-wide and therefore counted separately under {@link #syncfsCount()} rather
 * than charged to any single file.
 *
 * <p>Counters are keyed by FULL path and queried by substring, so a test can scope an assertion to one
 * table's directory ({@code msyncCount("t1~1/_txn")}) and not be polluted by the {@code _txn} of
 * telemetry or sibling tables.
 *
 * <p>It deliberately does NOT suppress any real syscall -- it observes and delegates, so the system under
 * test behaves exactly as in production.
 */
public class SyncAttributingFilesFacade extends TestFilesFacadeImpl {

    private final List<String> barrierOrder = new ArrayList<>();
    private final Map<Long, String> fdToPath = new HashMap<>();
    private final Map<String, int[]> fsyncs = new HashMap<>();
    private final List<long[]> mappings = new ArrayList<>(); // {addrLo, addrHi, fd}
    private final Map<String, int[]> msyncs = new HashMap<>();
    private int syncfsCount;

    /**
     * Total msync + fsync/fdatasync barriers charged to files whose path contains {@code pathContains}.
     */
    public int barrierCount(String pathContains) {
        return msyncCount(pathContains) + fsyncCount(pathContains);
    }

    /**
     * Forget all counters (the fd/mapping tables describe live state and are kept). Call this immediately
     * before the operation under test so the assertion window is exact.
     */
    public synchronized void clearCounters() {
        msyncs.clear();
        fsyncs.clear();
        barrierOrder.clear();
        syncfsCount = 0;
    }

    /**
     * Every barrier since the last {@link #clearCounters()}, in the order it was issued.
     * <p>
     * Counters answer "how many"; ordering invariants ("the data is durable before the pointer that
     * names it") need "in what order", and a count cannot express that.
     */
    public synchronized List<String> barrierOrder() {
        return new ArrayList<>(barrierOrder);
    }

    /**
     * Every recorded barrier as {@code path xN}, one per line, msyncs then fsyncs. Not used by assertions --
     * it is the diagnostic to print when a count assertion fails, since the usual cause is that the path you
     * asserted on is spelled differently from the one the engine actually opened.
     */
    public synchronized String debugDump() {
        StringBuilder sb = new StringBuilder("\nMSYNC:\n");
        for (Map.Entry<String, int[]> e : msyncs.entrySet()) {
            sb.append("  ").append(e.getKey()).append(" x").append(e.getValue()[0]).append('\n');
        }
        sb.append("FSYNC:\n");
        for (Map.Entry<String, int[]> e : fsyncs.entrySet()) {
            sb.append("  ").append(e.getKey()).append(" x").append(e.getValue()[0]).append('\n');
        }
        return sb.toString();
    }

    @Override
    public void barrierFsync(long fd) {
        fdatasync(fd);
    }

    @Override
    public void fsyncDurable(long fd) {
        fsync(fd);
    }

    @Override
    public void fdatasync(long fd) {
        recordFsync(fd);
        super.fdatasync(fd);
    }

    @Override
    public void fsync(long fd) {
        recordFsync(fd);
        super.fsync(fd);
    }

    @Override
    public void fsyncAndClose(long fd) {
        recordFsync(fd);
        super.fsyncAndClose(fd);
    }

    public synchronized int fsyncCount(String pathContains) {
        return sum(fsyncs, pathContains);
    }

    @Override
    public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
        long addr = super.mmap(fd, len, offset, flags, memoryTag);
        if (addr > 0) {
            synchronized (this) {
                mappings.add(new long[]{addr, addr + len, fd});
            }
        }
        return addr;
    }

    @Override
    public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
        long newAddr = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        synchronized (this) {
            removeMapping(addr);
            if (newAddr > 0) {
                mappings.add(new long[]{newAddr, newAddr + newSize, fd});
            }
        }
        return newAddr;
    }

    @Override
    public void msync(long addr, long len, boolean async) {
        recordMsync(addr);
        super.msync(addr, len, async);
    }

    public synchronized int msyncCount(String pathContains) {
        return sum(msyncs, pathContains);
    }

    @Override
    public void munmap(long address, long size, int memoryTag) {
        synchronized (this) {
            removeMapping(address);
        }
        super.munmap(address, size, memoryTag);
    }

    @Override
    public long openCleanRW(LPSZ name, long size) {
        long fd = super.openCleanRW(name, size);
        remember(fd, name);
        return fd;
    }

    @Override
    public long openRO(LPSZ name) {
        long fd = super.openRO(name);
        remember(fd, name);
        return fd;
    }

    @Override
    public long openRW(LPSZ name, int opts) {
        long fd = super.openRW(name, opts);
        remember(fd, name);
        return fd;
    }

    /**
     * Number of filesystem-wide {@code syncfs} calls. Not charged to any file: one syncfs makes the whole
     * filesystem durable, so a test asserting "the epoch forced everything durable" checks this instead of
     * per-file barriers.
     */
    public synchronized int syncfsCount() {
        return syncfsCount;
    }

    @Override
    public void syncfs(long fd) {
        synchronized (this) {
            syncfsCount++;
        }
        super.syncfs(fd);
    }

    private static void bump(Map<String, int[]> into, String path) {
        int[] c = into.get(path);
        if (c == null) {
            into.put(path, new int[]{1});
        } else {
            c[0]++;
        }
    }

    private static int sum(Map<String, int[]> counters, String pathContains) {
        int total = 0;
        for (Map.Entry<String, int[]> e : counters.entrySet()) {
            if (e.getKey().contains(pathContains)) {
                total += e.getValue()[0];
            }
        }
        return total;
    }

    private synchronized String pathOfAddress(long addr) {
        for (int i = mappings.size() - 1; i >= 0; i--) {
            long[] m = mappings.get(i);
            if (addr >= m[0] && addr < m[1]) {
                return pathOfFd(m[2]);
            }
        }
        return "<unmapped>";
    }

    private String pathOfFd(long fd) {
        String path = fdToPath.get(fd);
        return path == null ? "<unknown>" : path;
    }

    private synchronized void recordFsync(long fd) {
        final String path = pathOfFd(fd);
        bump(fsyncs, path);
        barrierOrder.add(path);
    }

    private synchronized void recordMsync(long addr) {
        final String path = pathOfAddress(addr);
        bump(msyncs, path);
        barrierOrder.add(path);
    }

    /**
     * Decode an {@link LPSZ} path to a String by its BYTES.
     * <p>
     * Neither {@code name.toString()} nor {@code Utf8s.toString(name)} works here: {@code Path$PathLPSZ}
     * does not override {@code Object.toString()} and {@code Utf8s.toString} simply delegates to it, so both
     * yield an identity hash like {@code Path$PathLPSZ@6aca80d1}. Every counter would then be keyed by a
     * unique nonsense string and every {@code contains()} lookup would silently return 0 -- a harness that
     * reports "no barriers" for everything, which reads exactly like a passing "must not flush" assertion.
     * Test paths are ASCII temp dirs, so a byte-wise decode is exact.
     */
    private static String pathToString(LPSZ name) {
        final int n = name.size();
        final StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            sb.append((char) (name.byteAt(i) & 0xFF));
        }
        return sb.toString();
    }

    private synchronized void remember(long fd, LPSZ name) {
        if (fd > -1) {
            fdToPath.put(fd, pathToString(name));
        }
    }

    private void removeMapping(long addr) {
        for (int i = mappings.size() - 1; i >= 0; i--) {
            if (mappings.get(i)[0] == addr) {
                mappings.remove(i);
                return;
            }
        }
    }
}
