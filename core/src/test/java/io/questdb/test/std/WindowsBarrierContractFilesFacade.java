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

import io.questdb.cairo.CairoException;
import io.questdb.std.str.LPSZ;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Enforces, on Linux, the one durability rule that only Windows actually polices: a barrier must be
 * issued through a handle that has WRITE access.
 *
 * <p>On Windows {@code fsync} is {@code FlushFileBuffers}, which requires {@code GENERIC_WRITE} on the
 * handle and returns {@code ERROR_ACCESS_DENIED} (errno 5) for the {@code GENERIC_READ}-only handle
 * {@code openRO} produces. On POSIX an {@code O_RDONLY} fd fsyncs perfectly well. A barrier written the
 * wrong way round is therefore GREEN on every Linux leg and DEAD on Windows -- and since OSS PR CI is
 * Linux-only, nothing in this repository notices. That is exactly how the {@code data.parquet} barrier
 * shipped broken for two months and took Enterprise cold storage down with it on that platform.
 *
 * <p>DIRECTORIES are exempt, and the exemption is the whole reason this has to be a runtime check rather
 * than a source scan: a directory barrier is impossible on Windows and every such site in the engine is
 * already behind an {@code Os.isWindows()} / {@code isRestrictedFileSystem()} guard, while on Linux those
 * same sites are legitimate and load-bearing. Only at runtime, at {@code open} time, can a directory be
 * told from a file -- static text cannot.
 *
 * <p>{@code Os.isWindows()} is a runtime constant a test cannot fake, so this does not pretend to be
 * Windows. It models the single behaviour that differs, and nothing else.
 *
 * <p>Violations are RECORDED as well as thrown, so an assertion can name the offending path instead of
 * surfacing as a bare {@code [5] could not fsync [fd=...]} the way the Windows CI leg did. Call
 * {@link #assertNoReadOnlyFileBarrier()} after the workload.
 */
public class WindowsBarrierContractFilesFacade extends TestFilesFacadeImpl {

    private final Map<String, int[]> barriers = new HashMap<>();
    private final Map<Long, String> fdToPath = new HashMap<>();
    private final Set<Long> readOnlyFileFds = new HashSet<>();
    private final Set<String> violations = new TreeSet<>();

    /**
     * Total barriers charged to paths containing {@code pathContains}. Use it to prove the workload
     * actually reached the code under test: a run that issued NO barriers passes the violation check too,
     * and would keep passing with the barrier deleted.
     */
    public synchronized int barrierCount(String pathContains) {
        int total = 0;
        for (Map.Entry<String, int[]> e : barriers.entrySet()) {
            if (e.getKey().contains(pathContains)) {
                total += e.getValue()[0];
            }
        }
        return total;
    }

    @Override
    public void barrierFsync(long fd) {
        enforceWriteAccess(fd);
        super.barrierFsync(fd);
    }

    public synchronized void assertNoReadOnlyFileBarrier() {
        if (!violations.isEmpty()) {
            throw new AssertionError(
                    "durability barrier issued on a handle opened WITHOUT write access, which"
                            + " ERROR_ACCESS_DENIEDs on Windows (FlushFileBuffers needs GENERIC_WRITE): "
                            + violations + debugDump()
            );
        }
    }

    /**
     * Forget the counters. The fd table describes live open handles and is KEPT.
     */
    public synchronized void clearCounters() {
        barriers.clear();
        violations.clear();
    }

    @Override
    public boolean close(long fd) {
        forget(fd);
        return super.close(fd);
    }

    /**
     * Every barrier as {@code path xN}. Diagnostic only -- print it when a count assertion fails, since the
     * usual cause is that the path asserted on is spelled differently from the one the engine opened.
     */
    public synchronized String debugDump() {
        final StringBuilder sb = new StringBuilder("\nBARRIERS:\n");
        for (Map.Entry<String, int[]> e : barriers.entrySet()) {
            sb.append("  ").append(e.getKey()).append(" x").append(e.getValue()[0]).append('\n');
        }
        return sb.toString();
    }

    @Override
    public void fdatasync(long fd) {
        enforceWriteAccess(fd);
        super.fdatasync(fd);
    }

    @Override
    public void fsync(long fd) {
        enforceWriteAccess(fd);
        super.fsync(fd);
    }

    @Override
    public void fsyncAndClose(long fd) {
        try {
            enforceWriteAccess(fd);
        } catch (CairoException e) {
            // The real facade closes even when the barrier fails. Mirror that, or a rejection here also
            // leaks the fd and reports as a misleading memory-leak failure instead of the contract breach.
            super.close(fd);
            forget(fd);
            throw e;
        }
        forget(fd);
        super.fsyncAndClose(fd);
    }

    @Override
    public void fsyncDurable(long fd) {
        enforceWriteAccess(fd);
        super.fsyncDurable(fd);
    }

    @Override
    public long openCleanRW(LPSZ name, long size) {
        final long fd = super.openCleanRW(name, size);
        remember(fd, name, true);
        return fd;
    }

    @Override
    public long openAppend(LPSZ name) {
        final long fd = super.openAppend(name);
        remember(fd, name, true);
        return fd;
    }

    @Override
    public long openRO(LPSZ name) {
        final long fd = super.openRO(name);
        remember(fd, name, false);
        return fd;
    }

    @Override
    public long openRONoCache(LPSZ name) {
        final long fd = super.openRONoCache(name);
        remember(fd, name, false);
        return fd;
    }

    @Override
    public long openRW(LPSZ name, int opts) {
        final long fd = super.openRW(name, opts);
        remember(fd, name, true);
        return fd;
    }

    @Override
    public long openRWNoCache(LPSZ name, int opts) {
        final long fd = super.openRWNoCache(name, opts);
        remember(fd, name, true);
        return fd;
    }

    public synchronized int violationCount() {
        return violations.size();
    }

    private synchronized void bump(String path) {
        final int[] c = barriers.get(path);
        if (c == null) {
            barriers.put(path, new int[]{1});
        } else {
            c[0]++;
        }
    }

    private synchronized void enforceWriteAccess(long fd) {
        final String path = fdToPath.get(fd);
        if (path != null) {
            bump(path);
        }
        if (readOnlyFileFds.contains(fd)) {
            violations.add(path == null ? "<fd " + fd + '>' : path);
            // The exact shape Windows produces, so a failure here reads like the CI log it stands in for.
            throw CairoException.critical(5).put("could not fsync [fd=").put(fd).put(']');
        }
    }

    private synchronized void forget(long fd) {
        fdToPath.remove(fd);
        readOnlyFileFds.remove(fd);
    }

    /**
     * Decode by BYTES: {@code Path$PathLPSZ} does not override {@code toString()}, so the obvious
     * conversions yield an identity hash, every lookup silently misses, and the facade reports nothing --
     * which reads exactly like a clean run.
     */
    private static String pathToString(LPSZ name) {
        final int n = name.size();
        final StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            sb.append((char) (name.byteAt(i) & 0xFF));
        }
        return sb.toString();
    }

    private synchronized void remember(long fd, LPSZ name, boolean writable) {
        if (fd < 0) {
            return;
        }
        final String path = pathToString(name);
        fdToPath.put(fd, path);
        // isDirOrSoftLinkDir is evaluated HERE, while the path is still in hand: by barrier time only the fd
        // remains, and an fd cannot be asked whether it names a directory through this interface.
        if (writable || isDirOrSoftLinkDir(name)) {
            readOnlyFileFds.remove(fd);
        } else {
            readOnlyFileFds.add(fd);
        }
    }
}
