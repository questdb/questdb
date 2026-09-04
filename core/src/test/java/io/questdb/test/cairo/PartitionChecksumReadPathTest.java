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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionChecksumSidecar;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Verification performed when a native partition opens.
 * <p>
 * It is deliberately STRUCTURAL -- the sidecar's own trailer, plus each covered file's length against
 * the recorded one -- and never hashes blocks. Column files are mmap'd, so there is no read hook to
 * piggyback on and hashing here would make open cost O(bytes) on the query path. That cost bound is
 * itself a design decision, so it is pinned by a test.
 */
public class PartitionChecksumReadPathTest extends AbstractCairoTest {

    @Test
    public void testGrownColumnFileOpensCleanly() throws Exception {
        // A file longer than recorded is the normal "appended since the last generation" state.
        // Reading it as corruption would condemn every actively-written table.
        assertMemoryLeak(() -> {
            createSealed("r2");
            appendGarbageToFirstCoveredFile("r2", "2024-01-01", 64);
            engine.releaseInactive();
            Assert.assertEquals(165L, sumV("r2"));
        });
    }

    @Test
    public void testOpenDoesNotHashBlocks() throws Exception {
        // The cost bound IS the design decision. Without this a later change could quietly turn every
        // partition open into a full hash and nothing else would fail.
        final AtomicLong mapped = new AtomicLong();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (memoryTag == MemoryTag.MMAP_DEFAULT) {
                    mapped.addAndGet(len);
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("create table r4 (ts timestamp, v long) timestamp(ts) partition by day wal");
            for (int i = 0; i < 200; i++) {
                execute("insert into r4 values ('2024-01-01T00:00:0" + (i % 10) + ".0000" + (100 + i) + "Z', " + i + ")");
            }
            execute("insert into r4 values ('2024-01-02T00:00:00.000000Z', 999)");
            drainWalQueue();
            engine.releaseInactive();

            final long before = mapped.get();
            Assert.assertEquals(201L, rowCount("r4"));
            final long during = mapped.get() - before;
            Assert.assertTrue(
                    "partition open mapped " + during + " bytes with MMAP_DEFAULT; verification must be"
                            + " structural, not a block hash",
                    during < 64 * 1024
            );
        });
    }

    @Test
    public void testColumnOpenFailureStillPropagatesAndRetriesWithCoverageOn() throws Exception {
        // Verification runs at the head of openPartition0, BEFORE the columns are opened, and swallows
        // its own I/O failures by design (an unopenable sidecar degrades to ABSENT so detection never
        // costs ingestion). This pins the other half of that: a failure opening a real COLUMN file
        // must still propagate, and a retry must still succeed, with coverage enabled.
        //
        // TimeFrameCursorTest covers the same contract with coverage OFF, because it arms the fault by
        // COUNTING file ops and verification's own ops shifted that count. Here the fault is armed on
        // the PATH instead, so it lands on the column open whatever else the open path starts doing.
        final AtomicBoolean armed = new AtomicBoolean();
        final AtomicInteger fired = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (armed.get() && Utf8s.endsWithAscii(name, "2024-01-01" + File.separator + "v.d")) {
                    armed.set(false);
                    fired.incrementAndGet();
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            createSealed("r5");

            armed.set(true);
            try {
                sumV("r5");
                Assert.fail("a failed column open must not read as healthy");
            } catch (CairoException expected) {
                // The open failed, as armed.
            }
            Assert.assertEquals("the injection must have fired on the column open, not been consumed"
                    + " elsewhere", 1, fired.get());

            // Retry: the partition must open cleanly once the fault is gone, i.e. the failed open left
            // no half-open partition state behind.
            engine.releaseInactive();
            Assert.assertEquals(165L, sumV("r5"));
        });
    }

    @Test
    public void testTruncatedColumnFileIsDetectedOnOpen() throws Exception {
        // Truncation is the shape a torn write at the tail of a file takes, and the one corruption a
        // structural check catches for free.
        assertMemoryLeak(() -> {
            createSealed("r1");
            truncateFirstCoveredFile("r1", "2024-01-01");
            engine.releaseInactive();
            try {
                sumV("r1");
                Assert.fail("a truncated covered file must not read as healthy");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "shorter than recorded");
            }
        });
    }

    /**
     * The same truncation on a NON-WAL table must NOT be condemned.
     * <p>
     * {@code CommitMode.appliesColumnSync} documents that ADAPTIVE on a non-WAL table degrades to
     * nosync-grade apply durability: there is no durable WAL to replay and no epoch, so nothing orders
     * the sidecar against the columns it covers and nothing re-derives coverage afterwards. The
     * configured token still reads ADAPTIVE, so a check keyed on the token alone treats a short file
     * as corruption and takes the partition offline for behaving exactly as the mode allows.
     * <p>
     * Found by the adaptive soak, not by this suite: build 267126,
     * {@code FrameAppendFuzzTest#testSimple} on a {@code BYPASS WAL} table --
     * {@code new_col_4.d.31, recorded=44112, actual=8192}. The WAL sibling above still throws, which
     * is what keeps this pair honest: the relaxation is scoped to the tables that cannot order their
     * own coverage, not applied to every table that happens to be short.
     */
    @Test
    public void testTruncatedColumnFileOnNonWalTableReadsUnverified() throws Exception {
        assertMemoryLeak(() -> {
            createSealedNonWal("r6");
            truncateFirstCoveredFile("r6", "2024-01-01");
            engine.releaseInactive();
            // Must not throw. The row total is deliberately not asserted: the truncation removed real
            // bytes, so a short read is a correct outcome for a mode with no durability contract over
            // them. What must not happen is a corruption verdict.
            sumV("r6");
        });
    }

    @Test
    public void testUncoveredPartitionOpensCleanly() throws Exception {
        // Upgrade-on-write: a partition sealed by an older binary has no sidecar and must open normally.
        assertMemoryLeak(() -> {
            createSealed("r3");
            final File chk = new File(partitionDir("r3", "2024-01-01"), PartitionChecksumSidecar.FILE_NAME);
            Assert.assertTrue("precondition: the partition must be covered", chk.exists());
            Assert.assertTrue(chk.delete());
            engine.releaseInactive();
            Assert.assertEquals(165L, sumV("r3"));
        });
    }

    private void appendGarbageToFirstCoveredFile(String table, String partition, int count) {
        withFirstCoveredFile(table, partition, (ff, filePath) -> {
            final long fd = ff.openRW(filePath.$(), 0);
            final long buf = Unsafe.malloc(count, MemoryTag.NATIVE_DEFAULT);
            try {
                final long at = ff.length(filePath.$());
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) 0x5A);
                }
                ff.write(fd, buf, count, at);
            } finally {
                ff.close(fd);
                Unsafe.free(buf, count, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * 12 rows in 2024-01-01 so covered files are big enough to truncate, then a later partition seals it.
     */
    /**
     * As {@link #createSealed(String)} but BYPASS WAL, so the table takes the non-WAL apply path while
     * the configured commit mode is still the suite default (adaptive). No drainWalQueue: a non-WAL
     * table applies inline.
     */
    private void createSealedNonWal(String table) throws Exception {
        execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day bypass wal");
        for (int i = 0; i < 12; i++) {
            execute("insert into " + table + " values ('2024-01-01T0" + (i % 10) + ":00:0" + (i % 10)
                    + ".00000" + i + "Z', " + i + ")");
        }
        execute("insert into " + table + " values ('2024-01-02T00:00:00.000000Z', 99)");
        engine.releaseInactive();
    }

    private void createSealed(String table) throws Exception {
        execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal");
        for (int i = 0; i < 12; i++) {
            execute("insert into " + table + " values ('2024-01-01T0" + (i % 10) + ":00:0" + (i % 10)
                    + ".00000" + i + "Z', " + i + ")");
        }
        execute("insert into " + table + " values ('2024-01-02T00:00:00.000000Z', 99)");
        drainWalQueue();
        engine.releaseInactive();
    }

    /**
     * Reads actual column DATA. count() can be answered from partition metadata without opening a
     * partition at all, so a truncation test built on it would never reach the check.
     */
    private long sumV(String table) {
        try (io.questdb.cairo.sql.RecordCursorFactory f = select("select sum(v) from " + table)) {
            try (io.questdb.cairo.sql.RecordCursor c = f.getCursor(sqlExecutionContext)) {
                return c.hasNext() ? c.getRecord().getLong(0) : 0L;
            }
        } catch (io.questdb.griffin.SqlException e) {
            throw new RuntimeException(e);
        }
    }

    private File partitionDir(String tableName, String partitionName) {
        final File tableDir = new File(
                configuration.getDbRoot().toString(),
                engine.verifyTableName(tableName).getDirName()
        );
        final File[] candidates = tableDir.listFiles();
        Assert.assertNotNull(candidates);
        File best = null;
        for (File f : candidates) {
            if (f.isDirectory()
                    && (f.getName().equals(partitionName) || f.getName().startsWith(partitionName + "."))
                    && (best == null || f.getName().compareTo(best.getName()) > 0)) {
                best = f;
            }
        }
        Assert.assertNotNull("partition directory not found for " + partitionName, best);
        return best;
    }

    private long rowCount(String table) {
        try (io.questdb.cairo.sql.RecordCursorFactory f = select("select count() from " + table)) {
            try (io.questdb.cairo.sql.RecordCursor c = f.getCursor(sqlExecutionContext)) {
                return c.hasNext() ? c.getRecord().getLong(0) : 0L;
            }
        } catch (io.questdb.griffin.SqlException e) {
            throw new RuntimeException(e);
        }
    }

    private void truncateFirstCoveredFile(String table, String partition) {
        withFirstCoveredFile(table, partition, (ff, filePath) -> {
            final long fd = ff.openRW(filePath.$(), 0);
            try {
                final long len = ff.length(filePath.$());
                Assert.assertTrue("nothing to truncate", len > 8);
                Assert.assertTrue(ff.truncate(fd, len - 8));
            } finally {
                ff.close(fd);
            }
        });
    }

    private void withFirstCoveredFile(String table, String partition, FileAction action) {
        final File dir = partitionDir(table, partition);
        try (Path chk = new Path(); Path data = new Path();
             PartitionChecksumSidecar sidecar = new PartitionChecksumSidecar()) {
            chk.of(dir.getAbsolutePath()).concat(PartitionChecksumSidecar.FILE_NAME);
            sidecar.of(configuration.getFilesFacade(), chk, configuration.getPartitionChecksumBlockSize());
            Assert.assertTrue("precondition: the partition must cover something", sidecar.fileCount() > 0);
            data.of(dir.getAbsolutePath()).concat(sidecar.fileName(0));
            action.run(configuration.getFilesFacade(), data);
        }
    }

    @FunctionalInterface
    private interface FileAction {
        void run(FilesFacade ff, Path filePath);
    }
}
