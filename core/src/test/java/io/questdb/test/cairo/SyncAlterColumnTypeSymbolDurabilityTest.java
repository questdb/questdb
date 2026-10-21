/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\__,_|\___||___/\__|____/|____/
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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Under cairo.commit.mode=sync, ALTER COLUMN TYPE ... SYMBOL must flush the files it writes before it returns:
 * the converted key column, the new symbol map and, for SYMBOL INDEX, the index of every partition.
 * The facade records the files of column "v" that the statement creates, and whether each one's content
 * (fsync of its fd, or msync of a mapping of it) and its directory entry (fsync of its parent directory)
 * were flushed. A simulated power loss then deletes each file whose name was never flushed and empties each
 * file whose content was never flushed. Every other file is kept as written, which favours the database.
 */
public class SyncAlterColumnTypeSymbolDurabilityTest extends AbstractCairoTest {
    private static final String ROWS = """
            ts\tv
            2024-01-01T00:00:00.000000Z\t1
            2024-01-02T00:00:00.000000Z\t2
            2024-01-03T00:00:00.000000Z\t3
            """;

    @Test
    public void testToSymbolIndexNonWal() throws Exception {
        assertSurvivesPowerLoss(false, "ALTER TABLE x ALTER COLUMN v TYPE SYMBOL INDEX", "SELECT ts, v FROM x WHERE v = '2'", false, """
                ts\tv
                2024-01-02T00:00:00.000000Z\t2
                """);
    }

    @Test
    public void testToSymbolNonWal() throws Exception {
        assertSurvivesPowerLoss(false, "ALTER TABLE x ALTER COLUMN v TYPE SYMBOL", "SELECT ts, v FROM x", true, ROWS);
    }

    @Test
    public void testToSymbolWal() throws Exception {
        assertSurvivesPowerLoss(true, "ALTER TABLE x ALTER COLUMN v TYPE SYMBOL", "SELECT ts, v FROM x", true, ROWS);
    }

    private void assertSurvivesPowerLoss(boolean wal, String ddl, String query, boolean expectSize, String expected) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        final PowerLossFacade ff = new PowerLossFacade("v.");
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY " + (wal ? "WAL" : "BYPASS WAL"));
            execute("INSERT INTO x VALUES ('2024-01-01', '1'), ('2024-01-02', '2'), ('2024-01-03', '3')");
            drainWalQueue();

            ff.arm();
            execute(ddl);
            drainWalQueue();
            assertRows(query, expectSize, expected);

            engine.releaseAllWriters();
            engine.releaseAllReaders();
            final List<String> lost = ff.powerLoss(engine.getConfiguration().getDbRoot());
            try {
                assertRows(query, expectSize, expected);
            } catch (Throwable th) {
                throw new AssertionError("after a power loss the table does not read back; files never flushed: " + lost
                        + "; error: " + th.getMessage(), th);
            }
            Assert.assertEquals("files never flushed", "[]", lost.toString());
        });
    }

    private void assertRows(String query, boolean expectSize, String expected) throws Exception {
        if (expectSize) {
            assertQuery(query).timestamp("ts").expectSize().returns(expected);
        } else {
            assertQuery(query).timestamp("ts").returns(expected);
        }
    }

    static final class PowerLossFacade extends TestFilesFacadeImpl {
        // created file path -> {contentFlushed, nameFlushed}
        private final Map<String, boolean[]> created = new HashMap<>();
        private final Map<Long, String> fdPaths = new HashMap<>();
        // mapping address -> {length, path}
        private final TreeMap<Long, Object[]> mappings = new TreeMap<>();
        private final String prefix;
        private volatile boolean armed;

        PowerLossFacade(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public synchronized boolean close(long fd) {
            fdPaths.remove(fd);
            return super.close(fd);
        }

        @Override
        public synchronized void fsync(long fd) {
            super.fsync(fd);
            flushed(fd);
        }

        @Override
        public synchronized void fsyncAndClose(long fd) {
            flushed(fd);
            fdPaths.remove(fd);
            super.fsyncAndClose(fd);
        }

        @Override
        public synchronized long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            final long addr = super.mmap(fd, len, offset, flags, memoryTag);
            track(addr, len, fdPaths.get(fd));
            return addr;
        }

        @Override
        public synchronized long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            mappings.remove(addr);
            final long newAddr = super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
            track(newAddr, newSize, fdPaths.get(fd));
            return newAddr;
        }

        @Override
        public synchronized void msync(long addr, long len, boolean async) {
            super.msync(addr, len, async);
            final Map.Entry<Long, Object[]> e = mappings.floorEntry(addr);
            if (e != null && addr < e.getKey() + (long) e.getValue()[0]) {
                final boolean[] state = created.get((String) e.getValue()[1]);
                if (state != null && !async) {
                    state[0] = true;
                }
            }
        }

        @Override
        public synchronized void munmap(long address, long size, int memoryTag) {
            mappings.remove(address);
            super.munmap(address, size, memoryTag);
        }

        @Override
        public synchronized long openAppend(LPSZ name) {
            final boolean existed = exists(name);
            return opened(super.openAppend(name), name, existed);
        }

        @Override
        public synchronized long openCleanRW(LPSZ name, long size) {
            final boolean existed = exists(name);
            return opened(super.openCleanRW(name, size), name, existed);
        }

        @Override
        public synchronized long openRO(LPSZ name) {
            return opened(super.openRO(name), name, true);
        }

        @Override
        public synchronized long openRONoCache(LPSZ path) {
            return opened(super.openRONoCache(path), path, true);
        }

        @Override
        public synchronized long openRW(LPSZ name, int opts) {
            final boolean existed = exists(name);
            return opened(super.openRW(name, opts), name, existed);
        }

        @Override
        public synchronized long openRWNoCache(LPSZ name, int opts) {
            final boolean existed = exists(name);
            return opened(super.openRWNoCache(name, opts), name, existed);
        }

        @Override
        public synchronized int rename(LPSZ from, LPSZ to) {
            final int rc = super.rename(from, to);
            final boolean[] state = created.remove(Utf8s.stringFromUtf8Bytes(from));
            if (state != null) {
                created.put(Utf8s.stringFromUtf8Bytes(to), new boolean[]{state[0], false});
            }
            return rc;
        }

        @Override
        public synchronized int sync() {
            for (boolean[] state : created.values()) {
                state[0] = state[1] = true;
            }
            return super.sync();
        }

        @Override
        public synchronized boolean touch(LPSZ path) {
            final boolean existed = exists(path);
            final boolean ok = super.touch(path);
            if (ok && !existed) {
                opened(Long.MIN_VALUE, path, false);
            }
            return ok;
        }

        synchronized void arm() {
            armed = true;
        }

        // Drops what was never flushed and returns the dropped files, relative to the database root.
        synchronized List<String> powerLoss(CharSequence dbRoot) throws Exception {
            armed = false;
            final List<String> lost = new ArrayList<>();
            for (Map.Entry<String, boolean[]> e : new TreeMap<>(created).entrySet()) {
                final File f = new File(e.getKey());
                if (!f.exists()) {
                    continue;
                }
                final String name = e.getKey().substring(dbRoot.length() + 1);
                if (!e.getValue()[1]) {
                    Assert.assertTrue(f.delete());
                    lost.add(name + (e.getValue()[0] ? " (name not flushed)" : " (name and content not flushed)"));
                } else if (!e.getValue()[0]) {
                    try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
                        raf.setLength(0);
                    }
                    lost.add(name + " (content not flushed)");
                }
            }
            created.clear();
            return lost;
        }

        private void flushed(long fd) {
            final String path = fdPaths.get(fd);
            if (path == null) {
                return;
            }
            final boolean[] state = created.get(path);
            if (state != null) {
                state[0] = true;
            }
            for (Map.Entry<String, boolean[]> e : created.entrySet()) {
                if (path.equals(new File(e.getKey()).getParent())) {
                    e.getValue()[1] = true;
                }
            }
        }

        private long opened(long fd, LPSZ name, boolean existed) {
            if (armed && fd != -1) {
                final String raw = Utf8s.stringFromUtf8Bytes(name);
                final String path = raw.endsWith(File.separator) ? raw.substring(0, raw.length() - 1) : raw;
                if (fd != Long.MIN_VALUE) {
                    fdPaths.put(fd, path);
                }
                final String fileName = new File(path).getName();
                if (!existed && fileName.startsWith(prefix) && !path.contains(File.separator + "wal")) {
                    created.put(path, new boolean[2]);
                }
            }
            return fd;
        }

        private void track(long addr, long len, String path) {
            if (path != null && addr > 0) {
                mappings.put(addr, new Object[]{len, path});
            }
        }
    }
}
