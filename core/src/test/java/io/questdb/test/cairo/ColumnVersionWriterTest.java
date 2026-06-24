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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.ColumnVersionWriter;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.Formatter;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.ColumnVersionReader.BLOCK_SIZE_BYTES;
import static io.questdb.cairo.ColumnVersionReader.HEADER_SIZE;
import static io.questdb.cairo.ColumnVersionReader.OFFSET_OFFSET_A_64;
import static io.questdb.cairo.ColumnVersionReader.OFFSET_OFFSET_B_64;
import static io.questdb.cairo.ColumnVersionReader.OFFSET_SIZE_A_64;
import static io.questdb.cairo.ColumnVersionReader.OFFSET_SIZE_B_64;
import static io.questdb.cairo.ColumnVersionReader.OFFSET_VERSION_64;

public class ColumnVersionWriterTest extends AbstractCairoTest {

    // Representative area for the helper unit tests: 3 entries * 32 bytes = 96 bytes.
    private static final long CV_AREA_SIZE = 3L * BLOCK_SIZE_BYTES;

    // ---- _cv body-checksum unit tests (mirror the _txn helper tests) ----

    @Test
    public void testCvAreaChecksumChangesWhenAnyByteChanges() {
        long addr = Unsafe.malloc(CV_AREA_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            fillPattern(addr, CV_AREA_SIZE, 0x1234);
            long base = TableUtils.calculateCvAreaChecksum(addr, CV_AREA_SIZE);
            // Every byte of the whole area is covered (zero exclusions): flipping any one must change it.
            for (long off = 0; off < CV_AREA_SIZE; off++) {
                byte orig = Unsafe.getByte(addr + off);
                Unsafe.putByte(addr + off, (byte) (orig ^ 0x5a));
                Assert.assertNotEquals("checksum did not change for covered byte offset " + off, base, TableUtils.calculateCvAreaChecksum(addr, CV_AREA_SIZE));
                Unsafe.putByte(addr + off, orig);
            }
        } finally {
            Unsafe.free(addr, CV_AREA_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCvAreaChecksumIsDeterministicAndNeverZero() {
        long addr = Unsafe.malloc(CV_AREA_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int seed = 0; seed < 100_000; seed++) {
                fillPattern(addr, CV_AREA_SIZE, seed);
                long c1 = TableUtils.calculateCvAreaChecksum(addr, CV_AREA_SIZE);
                long c2 = TableUtils.calculateCvAreaChecksum(addr, CV_AREA_SIZE);
                Assert.assertEquals("checksum must be deterministic for seed " + seed, c1, c2);
                Assert.assertNotEquals("checksum returned 0 for seed " + seed, 0L, c1);
            }
            // The all-zero area (avalanche of 0) must still be remapped away from the 0 = "absent" sentinel.
            for (long off = 0; off < CV_AREA_SIZE; off++) {
                Unsafe.putByte(addr + off, (byte) 0);
            }
            Assert.assertNotEquals("all-zero area must not collide with the absent sentinel", 0L, TableUtils.calculateCvAreaChecksum(addr, CV_AREA_SIZE));
        } finally {
            Unsafe.free(addr, CV_AREA_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCvChecksumAbsentOldFormat() throws Exception {
        // An old-format _cv with a 40-byte header and NO trailing checksum long (file ends exactly at
        // offset+size). readSafe() must succeed and must NOT SIGBUS or throw or fall back: the EOF guard
        // sees the file is too short for a trailing long and skips the verify.
        assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (Path path = new Path()) {
                LPSZ cvPath = path.of(root).concat("_cv").$();
                long liveOffset;
                long liveSize;
                try (ColumnVersionWriter w = new ColumnVersionWriter(configuration, cvPath, true)) {
                    for (int i = 0; i < 5; i++) {
                        w.upsert(i, i, i + 1, i * 10L);
                    }
                    w.commit();
                    long version = peekLong(ff, cvPath, OFFSET_VERSION_64);
                    boolean areaA = (version & 1L) == 0;
                    liveOffset = peekLong(ff, cvPath, areaA ? OFFSET_OFFSET_A_64 : OFFSET_OFFSET_B_64);
                    liveSize = peekLong(ff, cvPath, areaA ? OFFSET_SIZE_A_64 : OFFSET_SIZE_B_64);
                }

                // Truncate the file to exactly offset+size, removing the trailing checksum long entirely:
                // this is byte-for-byte an old-format _cv (no trailing bytes after the last block).
                truncateFile(ff, cvPath, liveOffset + liveSize);
                Assert.assertEquals(liveOffset + liveSize, ff.length(cvPath));

                ColumnVersionReader.resetBodyChecksumFallbackCount();
                try (ColumnVersionReader r = new ColumnVersionReader().ofRO(ff, cvPath)) {
                    // Must not SIGBUS / throw, and must read the data back correctly.
                    r.readSafe(configuration.getMillisecondClock(), 1);
                    for (int i = 0; i < 5; i++) {
                        Assert.assertEquals(i * 10L, r.getColumnTopQuick(i, i));
                    }
                    Assert.assertEquals("absent checksum must not trigger fallback", 0L, ColumnVersionReader.getBodyChecksumFallbackCount());
                }
            }
        });
    }

    @Test
    public void testCvChecksumDetectsCorruption() throws Exception {
        // Single commit => only the live area is valid (the other area was never written / is empty).
        // Corrupting a covered byte of the live area, leaving its checksum stale, must NOT silently return
        // the wrong value: with no valid other area, readSafe() must throw.
        assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (Path path = new Path()) {
                LPSZ cvPath = path.of(root).concat("_cv").$();
                long liveOffset;
                try (ColumnVersionWriter w = new ColumnVersionWriter(configuration, cvPath, true)) {
                    for (int i = 0; i < 4; i++) {
                        w.upsert(i, i, i + 1, i * 100L);
                    }
                    w.commit();
                    long version = peekLong(ff, cvPath, OFFSET_VERSION_64);
                    boolean areaA = (version & 1L) == 0;
                    liveOffset = peekLong(ff, cvPath, areaA ? OFFSET_OFFSET_A_64 : OFFSET_OFFSET_B_64);
                }

                // Flip the first block's partition timestamp WITHOUT recomputing the trailing checksum.
                long orig = peekLong(ff, cvPath, liveOffset);
                pokeLong(ff, cvPath, liveOffset, orig ^ 0x5a5a_5a5aL);

                ColumnVersionReader.resetBodyChecksumFallbackCount();
                try (ColumnVersionReader r = new ColumnVersionReader().ofRO(ff, cvPath)) {
                    try {
                        r.readSafe(configuration.getMillisecondClock(), 1);
                        Assert.fail("expected CairoException - corrupt live area with no valid fallback");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "_cv checksum mismatch in both A and B areas");
                    }
                    Assert.assertEquals("exactly one fallback attempt expected", 1L, ColumnVersionReader.getBodyChecksumFallbackCount());
                }
            }
        });
    }

    @Test
    public void testCvChecksumFallbackAfterManyCommits() throws Exception {
        // THE KEY PLACEMENT TEST. Many commits with VARYING entry counts (grow then shrink, repeatedly) so
        // calculateWriteOffset both APPENDS new areas after the current one AND REUSES freed front space -
        // exercising the +8 footprint reservation in BOTH branches. After all that, corrupt ONLY the current
        // area; readSafe() must fall back to the OTHER (prior) area and return its exact, valid content. A
        // wrong +8 reservation would have let some commit clobber the prior area's data OR its trailing
        // checksum, so either the fallback content would be wrong or its checksum would fail (=> both-areas
        // throw). We assert the fallback content equals the prior area read straight off disk.
        assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (Path path = new Path()) {
                LPSZ cvPath = path.of(root).concat("_cv").$();

                final int cycles = 60;
                long priorOffset;
                long priorSize;
                long currentOffset;
                long currentSize;

                try (ColumnVersionWriter w = new ColumnVersionWriter(configuration, cvPath, true)) {
                    // Repeatedly grow the entry set up to a peak then shrink back down, committing on every
                    // step. The oscillating size makes calculateWriteOffset alternate between front-reuse
                    // (when the new, smaller area fits before the current one) and append (when it doesn't).
                    int gen = 0;
                    for (int cycle = 0; cycle < cycles; cycle++) {
                        int peak = 4 + (cycle % 13); // varies the peak so offsets don't settle into a pattern
                        // grow
                        for (int n = 1; n <= peak; n++) {
                            w.upsert(n, 0, gen++, n * 3L);
                            w.commit();
                        }
                        // shrink (keep partition 1 so the area never becomes empty - we want pure size
                        // oscillation between front-reuse and append, not the empty-area edge case)
                        for (int n = peak; n >= 2; n--) {
                            w.removePartition(n);
                            w.commit();
                        }
                    }
                    // A final pair of distinct commits so both A and B hold a real, checksummed area.
                    w.upsert(1, 0, gen++, 11L);
                    w.upsert(2, 0, gen++, 22L);
                    w.commit();
                    w.upsert(3, 0, gen++, 33L);
                    w.commit();

                    long version = peekLong(ff, cvPath, OFFSET_VERSION_64);
                    boolean areaA = (version & 1L) == 0;
                    currentOffset = peekLong(ff, cvPath, areaA ? OFFSET_OFFSET_A_64 : OFFSET_OFFSET_B_64);
                    currentSize = peekLong(ff, cvPath, areaA ? OFFSET_SIZE_A_64 : OFFSET_SIZE_B_64);
                    priorOffset = peekLong(ff, cvPath, areaA ? OFFSET_OFFSET_B_64 : OFFSET_OFFSET_A_64);
                    priorSize = peekLong(ff, cvPath, areaA ? OFFSET_SIZE_B_64 : OFFSET_SIZE_A_64);
                }

                // Invariant guaranteed by the +8 reservation: the two areas' full footprints
                // [offset, offset+size+8) must be disjoint. If this fails, the placement math is wrong.
                assertNoAreaOverlap(currentOffset, currentSize, priorOffset, priorSize);

                // Corrupt the current/live area's first block, leaving its trailing checksum stale.
                long origFirst = peekLong(ff, cvPath, currentOffset);
                pokeLong(ff, cvPath, currentOffset, origFirst ^ 0x33);

                ColumnVersionReader.resetBodyChecksumFallbackCount();
                try (ColumnVersionReader r = new ColumnVersionReader().ofRO(ff, cvPath)) {
                    r.readSafe(configuration.getMillisecondClock(), 1);
                    Assert.assertEquals("must fall back to the prior area exactly once", 1L, ColumnVersionReader.getBodyChecksumFallbackCount());
                    // The fallback exposed the prior area: its block count and exact content must equal the
                    // prior area read straight off disk (proving it was neither clobbered nor mis-sized).
                    Assert.assertEquals(
                            "fallback must expose the prior area's full block set",
                            (int) (priorSize / BLOCK_SIZE_BYTES) * ColumnVersionWriter.BLOCK_SIZE,
                            r.getCachedColumnVersionList().size()
                    );
                    assertReaderMatchesArea(ff, cvPath, priorOffset, priorSize, r);
                }
            }
        });
    }

    @Test
    public void testCvNoFalsePositiveUnderConcurrentCommits() throws Exception {
        // Concurrent writer (thousands of commits) + a reader looping readSafe(). Because the whole _cv area
        // is commit-immutable, the reader must NEVER see a stable-version checksum mismatch: the fallback
        // counter must stay 0 across the entire run.
        assertMemoryLeak(() -> {
            final int N = 10_000;
            final FilesFacade ff = configuration.getFilesFacade();
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = new ColumnVersionWriter(configuration, path.of(root).concat("_cv").$(), true);
                    ColumnVersionReader r = new ColumnVersionReader().ofRO(ff, path.$())
            ) {
                ColumnVersionReader.resetBodyChecksumFallbackCount();
                final CyclicBarrier barrier = new CyclicBarrier(2);
                final ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
                final AtomicInteger done = new AtomicInteger();

                Thread writer = new Thread(() -> {
                    final Rnd rnd = new Rnd();
                    try {
                        barrier.await();
                        for (int txn = 0; txn < N; txn++) {
                            int increment = rnd.nextInt(32);
                            for (int j = 0; j < increment; j++) {
                                w.upsert(rnd.nextLong(20), rnd.nextInt(10), txn, -1);
                            }
                            w.commit();
                        }
                    } catch (Throwable th) {
                        exceptions.add(th);
                    } finally {
                        done.incrementAndGet();
                    }
                });

                Thread reader = new Thread(() -> {
                    try {
                        barrier.await();
                        while (done.get() == 0) {
                            try {
                                // A generous timeout: we are not testing liveness here, only that a healthy
                                // concurrent commit never trips a body-checksum fallback.
                                r.readSafe(configuration.getMillisecondClock(), 5_000);
                            } catch (CairoException ex) {
                                if (Chars.contains(ex.getFlyweightMessage(), "timeout")) {
                                    continue;
                                }
                                throw ex;
                            }
                            Os.pause();
                        }
                    } catch (Throwable th) {
                        exceptions.add(th);
                    }
                });

                writer.start();
                reader.start();
                writer.join();
                reader.join();

                if (!exceptions.isEmpty()) {
                    Assert.fail(exceptions.poll().toString());
                }
                Assert.assertEquals("no false-positive fallback under healthy concurrent commits", 0L, ColumnVersionReader.getBodyChecksumFallbackCount());
            }
        });
    }

    @Test
    public void testCvRollbackVerifies() throws Exception {
        // rollback() re-exposes the prior area (version - 1). Its trailing checksum must still verify - proof
        // that the latest commit's placement did NOT clobber the prior area's data or checksum.
        assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (Path path = new Path()) {
                LPSZ cvPath = path.of(root).concat("_cv").$();
                try (ColumnVersionWriter w = new ColumnVersionWriter(configuration, cvPath, true)) {
                    // First committed state.
                    for (int i = 0; i < 6; i++) {
                        w.upsert(i + 1, i, 10 + i, 100L + i);
                    }
                    w.commit();
                    StringSink before = new StringSink();
                    before.put(w.toString());

                    // Second commit with a different shape (so the area placement actually moves).
                    w.upsert(99, 0, 999, 9999);
                    w.upsert(100, 1, 1000, 10000);
                    w.commit();

                    // Roll back to the prior committed area. readUnsafe() inside rollback() verifies-or-skips;
                    // a clobbered prior checksum would log an error but to be strict we re-verify via a fresh
                    // reader's readSafe() (which does the real checksum check) below.
                    w.rollback();
                    Assert.assertEquals("rollback must restore the prior committed state", before.toString(), w.toString());
                }

                // Independently confirm the now-current (post-rollback) area passes the strict readSafe check.
                ColumnVersionReader.resetBodyChecksumFallbackCount();
                try (ColumnVersionReader r = new ColumnVersionReader().ofRO(ff, cvPath)) {
                    r.readSafe(configuration.getMillisecondClock(), 1);
                    Assert.assertEquals("rollback area must verify cleanly (no fallback)", 0L, ColumnVersionReader.getBodyChecksumFallbackCount());
                    for (int i = 0; i < 6; i++) {
                        Assert.assertEquals(100L + i, r.getColumnTopQuick(i + 1, i));
                    }
                }
            }
        });
    }

    @Test
    public void testColumnAddRemove() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = new ColumnVersionWriter(configuration, path.of(root).concat("_cv").$(), true)
            ) {
                long partitionTimestamp = Micros.DAY_MICROS * 2;
                int columnIndex = 3;

                // Add column
                w.upsert(partitionTimestamp, columnIndex, 123, 987);
                w.upsertDefaultTxnName(columnIndex, 123, partitionTimestamp);

                // Verify
                Assert.assertEquals(0, w.getColumnTopQuick(partitionTimestamp, columnIndex + 1));
                Assert.assertEquals(partitionTimestamp, w.getColumnTopPartitionTimestamp(columnIndex));
                Assert.assertEquals(123, w.getColumnNameTxn(partitionTimestamp, columnIndex));
                Assert.assertEquals(987, w.getColumnTopQuick(partitionTimestamp, columnIndex));
                int recordIndex = w.getRecordIndex(partitionTimestamp, columnIndex);
                Assert.assertEquals(123, w.getColumnNameTxnByIndex(recordIndex));
                Assert.assertEquals(987, w.getColumnTopByIndex(recordIndex));

                // Remove non-existing column top
                w.removeColumnTop(partitionTimestamp, columnIndex + 1);
                Assert.assertEquals(0, w.getColumnTopQuick(partitionTimestamp, columnIndex + 1));

                Assert.assertEquals(partitionTimestamp, w.getColumnTopPartitionTimestamp(columnIndex));
                Assert.assertEquals(123, w.getColumnNameTxn(partitionTimestamp, columnIndex));
                Assert.assertEquals(987, w.getColumnTopQuick(partitionTimestamp, columnIndex));

                // Remove existing column top
                w.removeColumnTop(partitionTimestamp, columnIndex);

                Assert.assertEquals(partitionTimestamp, w.getColumnTopPartitionTimestamp(columnIndex));
                Assert.assertEquals(123, w.getColumnNameTxn(partitionTimestamp, columnIndex));
                Assert.assertEquals(0, w.getColumnTopQuick(partitionTimestamp, columnIndex));
            }
        });
    }

    @Test
    public void testClearResetsAllFields() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = createColumnVersionWriter(path);
                    ColumnVersionReader r = createColumnVersionReader(path)
            ) {
                // Write some data
                w.upsert(0, 0, 1, 100);
                w.upsert(0, 1, 2, 200);
                w.upsertDefaultTxnName(0, 1, 0);
                w.upsertDefaultTxnName(1, 2, 0);
                w.commit();

                // Read and verify data is populated
                r.readUnsafe();
                Assert.assertTrue("cachedColumnVersionList should not be empty", r.getCachedColumnVersionList().size() > 0);
                Assert.assertEquals(100, r.getColumnTopQuick(0, 0));
                Assert.assertEquals(200, r.getColumnTopQuick(0, 1));
                Assert.assertEquals(1, r.getDefaultColumnNameTxn(0));
                Assert.assertEquals(2, r.getDefaultColumnNameTxn(1));

                // Now clear
                r.clear();

                // Verify all fields are reset
                Assert.assertEquals("cachedColumnVersionList should be empty after clear", 0, r.getCachedColumnVersionList().size());
                Assert.assertEquals("version should be -1 after clear", -1, r.getVersion());
                // Lookups on cleared reader should return defaults
                Assert.assertEquals(0, r.getColumnTopQuick(0, 0));
                Assert.assertEquals(-1, r.getDefaultColumnNameTxn(0));
            }
        });
    }

    @Test
    public void testColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = createColumnVersionWriter(path);
                    ColumnVersionReader r = createColumnVersionReader(path)
            ) {
                for (int i = 0; i < 100; i += 2) {
                    w.upsert(i, i % 10, -1, i * 10L);
                }

                w.commit();

                r.readSafe(configuration.getMillisecondClock(), 1);
                for (int i = 0; i < 100; i++) {
                    long colTop = r.getColumnTopQuick(i, i % 10);
                    Assert.assertEquals(i % 2 == 0 ? i * 10 : 0, colTop);
                }

                TestUtils.assertEquals(w.getCachedColumnVersionList(), r.getCachedColumnVersionList());
            }
        });
    }

    @Test
    public void testColumnTopChangedInO3() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = createColumnVersionWriter(path)
            ) {
                long day1 = 0;
                long day2 = Micros.DAY_MICROS;
                long day3 = Micros.DAY_MICROS * 2;
                int columnIndex = 3;
                int columnIndex1 = 1;

                // Add column
                w.upsert(day3, columnIndex, 123, 987);
                w.upsertDefaultTxnName(columnIndex, 123, day3);

                // Simulate O3 write to day1, day2
                w.upsertColumnTop(day1, columnIndex, 15);
                w.upsertColumnTop(day2, columnIndex, 0);
                w.upsertColumnTop(day1, columnIndex1, 15);
                w.upsertColumnTop(day2, columnIndex1, 0);

                // Check column top, txn name
                Assert.assertEquals(123, w.getColumnNameTxn(day1, columnIndex));
                Assert.assertEquals(123, w.getColumnNameTxn(day2, columnIndex));
                Assert.assertEquals(123, w.getColumnNameTxn(day3, columnIndex));

                Assert.assertEquals(-1, w.getColumnNameTxn(day1, columnIndex1));
                Assert.assertEquals(-1, w.getColumnNameTxn(day2, columnIndex1));
                Assert.assertEquals(-1, w.getColumnNameTxn(day3, columnIndex1));

                // Check column top values
                Assert.assertEquals(15, w.getColumnTopQuick(day1, columnIndex));
                Assert.assertEquals(0, w.getColumnTopQuick(day2, columnIndex));
                Assert.assertEquals(987, w.getColumnTopQuick(day3, columnIndex));

                Assert.assertEquals(15, w.getColumnTopQuick(day1, columnIndex1));
                Assert.assertEquals(0, w.getColumnTopQuick(day2, columnIndex1));
                Assert.assertEquals(0, w.getColumnTopQuick(day3, columnIndex1));
            }
        });
    }

    @Test
    public void testColumnTruncate() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = createColumnVersionWriter(path);
                    ColumnVersionReader r = createColumnVersionReader(path)
            ) {
                Rnd rnd = TestUtils.generateRandom(LOG);
                int columnCount = 27;
                for (int i = 0; i < columnCount; i++) {
                    w.upsertDefaultTxnName(i, i, Micros.DAY_MICROS * i);
                    w.upsertColumnTop(Micros.DAY_MICROS * i, i, i * 100);
                }

                w.commit();
                w.truncate();
                r.readUnsafe();

                for (int i = 0; i < columnCount; i++) {
                    for (int j = 0; j < 100; j++) {
                        Assert.assertEquals(0, r.getColumnTop(rnd.nextLong(), i));
                        Assert.assertEquals(i, r.getDefaultColumnNameTxn(i));
                        final long ts = r.getColumnTopPartitionTimestamp(i);
                        Assert.assertTrue(ts == ColumnVersionReader.COL_TOP_DEFAULT_PARTITION || ts == ColumnVersionReader.SYMBOL_TABLE_VERSION_PARTITION);
                    }
                }
            }
        });
    }

    @Test
    public void testColumnVersionReaderReuse() throws Exception {
        assertMemoryLeak(() -> {
            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = createColumnVersionWriter(path);
                    ColumnVersionReader r = new ColumnVersionReader().ofRO(ff, path.$())
            ) {
                for (int i = 0; i < 100; i += 2) {
                    w.upsert(i, i % 10, -1, i * 10L);
                }

                w.commit();

                r.readSafe(configuration.getMillisecondClock(), 1);
                for (int i = 0; i < 100; i++) {
                    long colTop = r.getColumnTopQuick(i, i % 10);
                    Assert.assertEquals(i % 2 == 0 ? i * 10 : 0, colTop);
                }

                TestUtils.assertEquals(w.getCachedColumnVersionList(), r.getCachedColumnVersionList());

                r.ofRO(ff, path.$());
                r.readSafe(configuration.getMillisecondClock(), 1);
                TestUtils.assertEquals(w.getCachedColumnVersionList(), r.getCachedColumnVersionList());

                MemoryCMR mem = Vm.getCMRInstance();
                mem.of(ff, path.$(), 0, HEADER_SIZE, MemoryTag.MMAP_TABLE_READER);
                r.ofRO(mem);
                r.readSafe(configuration.getMillisecondClock(), 1);
                TestUtils.assertEquals(w.getCachedColumnVersionList(), r.getCachedColumnVersionList());
                mem.close();
            }
        });
    }

    @Test
    public void testFuzz() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd();
            final int N = 100_000;
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = createColumnVersionWriter(path);
                    ColumnVersionReader r = new ColumnVersionReader().ofRO(configuration.getFilesFacade(), path.$())
            ) {
                w.upsert(1, 2, 3, -1);

                for (int i = 0; i < N; i++) {
                    // increment from 0 to 4 columns
                    int increment = rnd.nextInt(32);

                    for (int j = 0; j < increment; j++) {
                        w.upsert(rnd.nextLong(20), rnd.nextInt(10), i, -1);
                    }

                    w.commit();
                    r.readSafe(configuration.getMillisecondClock(), 1);
                    Assert.assertTrue(w.getCachedColumnVersionList().size() > 0);
                    TestUtils.assertEquals(w.getCachedColumnVersionList(), r.getCachedColumnVersionList());
                    // assert list is ordered by (timestamp,column_index)

                    LongList list = r.getCachedColumnVersionList();
                    long prevTimestamp = -1;
                    long prevColumnIndex = -1;
                    for (int j = 0, n = list.size(); j < n; j += ColumnVersionWriter.BLOCK_SIZE) {
                        long timestamp = list.getQuick(j);
                        long columnIndex = list.getQuick(j + 1);

                        if (prevTimestamp < timestamp) {
                            prevTimestamp = timestamp;
                            prevColumnIndex = columnIndex;
                            continue;
                        }

                        if (prevTimestamp == timestamp) {
                            Assert.assertTrue(prevColumnIndex < columnIndex);
                            prevColumnIndex = columnIndex;
                            continue;
                        }

                        Assert.fail();
                    }
                }
            }
        });
    }

    @Test
    public void testFuzzConcurrent() throws Exception {
        testFuzzConcurrent(0);
    }

    @Test
    public void testFuzzWithTimeout() throws Exception {
        testFuzzConcurrent(5_000);
    }

    @Test
    public void testRemovePartitionColumns() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = createColumnVersionWriter(path);
                    ColumnVersionReader r = new ColumnVersionReader().ofRO(configuration.getFilesFacade(), path.$())
            ) {
                CVStringTable.setupColumnVersionWriter(w,
                        """
                                     pts  colIdx  colTxn  colTop
                                       0       2      -1      10
                                       0       3      -1      10
                                       0       5      -1      10
                                       1       0      -1      10
                                       1       2      -1      10
                                       2       2      -1      10
                                       2      11      -1      10
                                       2      15      -1      10
                                       3       0      -1      10
                                """
                );

                w.commit();
                w.removePartition(0);
                w.commit();

                String expected =
                        """
                                     pts  colIdx  colTxn  colTop
                                       1       0      -1      10
                                       1       2      -1      10
                                       2       2      -1      10
                                       2      11      -1      10
                                       2      15      -1      10
                                       3       0      -1      10
                                """;

                TestUtils.assertEquals(expected, CVStringTable.asTable(w.getCachedColumnVersionList()));
                r.readSafe(configuration.getMillisecondClock(), 1);
                TestUtils.assertEquals(expected, CVStringTable.asTable(r.getCachedColumnVersionList()));
            }
        });
    }

    @Test
    public void testToString() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = createColumnVersionWriter(path);
                    ColumnVersionReader r = createColumnVersionReader(path)
            ) {
                for (int i = 0; i < 3; i += 2) {
                    w.upsert(i, i % 10, -1, i * 10L);
                }
                final long defaultTs = MicrosFormatUtils.parseTimestamp("2024-02-24T00:00:00.000000Z");
                w.upsertDefaultTxnName(4, 123, defaultTs);

                w.commit();

                r.readSafe(configuration.getMillisecondClock(), 1);
                Assert.assertEquals("{[\n" +
                        "{columnIndex: 4, defaultNameTxn: 123, addedPartition: " + defaultTs + "},\n" +
                        "{columnIndex: 0, nameTxn: -1, partition: 0, columnTop: 0},\n" +
                        "{columnIndex: 2, nameTxn: -1, partition: 2, columnTop: 20}\n" +
                        "]}", r.toString());
            }
        });
    }

    @Test
    public void testUpsertPartition() throws Exception {
        assertUpsertPartitionFromSourceCV(
                """
                             pts  colIdx  colTxn  colTop
                               0       2      -1      10
                               0       3      -1      10
                               0       5      -1      10
                               1       0      -1      10
                               1       2      -1      10
                               2       2      -1      10
                               2      11      -1      10
                               2      15      -1      10
                               3       0      -1      10
                               4       7      -1      10
                        """,
                """
                             pts  colIdx  colTxn  colTop
                               0       2       3       1
                               0       3       1     101
                               1       0      -1      10
                               2       2       1     111
                               2      11       2       1
                               2      15       2    1001
                               3       0       3     110
                        """,
                """
                             pts  colIdx  colTxn  colTop
                               0       2      -1      10
                               0       3      -1      10
                               0       5      -1      10
                               1       0      -1      10
                               2       2      -1      10
                               2      11      -1      10
                               2      15      -1      10
                               3       0       3     110
                               4       7      -1      10
                        """,
                0, 2, 4
        );
    }

    @Test
    public void testUpsertPartitionDstContainsPartition() throws Exception {
        assertUpsertPartitionFromSourceCV(
                """
                             pts  colIdx  colTxn  colTop
                               2      11       0      99
                               2      12       1      17
                               3      11       1       8
                        """,
                """
                             pts  colIdx  colTxn  colTop
                               0       2       3       1
                               0       3       1     101
                               2      11       5      12
                               2      12       5      12
                        """,
                """
                             pts  colIdx  colTxn  colTop
                               0       2       3       1
                               0       3       1     101
                               2      11       0      99
                               2      12       1      17
                        """,
                2
        );
    }

    @Test
    public void testUpsertPartitionDstDoesNotContainPartition() throws Exception {
        assertUpsertPartitionFromSourceCV(
                """
                             pts  colIdx  colTxn  colTop
                               2      11       1      10
                        """,
                """
                             pts  colIdx  colTxn  colTop
                               0       2       3       1
                               0       3       1     101
                        """,
                """
                             pts  colIdx  colTxn  colTop
                               0       2       3       1
                               0       3       1     101
                               2      11       1      10
                        """, // Gets added
                2
        );
    }

    @Test
    public void testUpsertPartitionSrcDoesNotContainPartition() throws Exception {
        assertUpsertPartitionFromSourceCV(
                """
                             pts  colIdx  colTxn  colTop
                               2      11       1      10
                        """,
                """
                             pts  colIdx  colTxn  colTop
                               0       2       3       1
                               0       3       1     101
                        """,
                "     pts  colIdx  colTxn  colTop\n" + // No changes
                        "       0       2       3       1\n" +
                        "       0       3       1     101\n",
                0
        );
    }

    private static void assertUpsertPartitionFromSourceCV(
            String srcExpected,
            String dstExpected,
            String dstUpsertFromSrcExpected,
            long... partitionTimestamp
    ) throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    ColumnVersionWriter w1 = new ColumnVersionWriter(configuration, path.of(root).concat("_cv1").$(), true);
                    ColumnVersionWriter w2 = createColumnVersionWriter(path);
                    ColumnVersionReader r = new ColumnVersionReader().ofRO(configuration.getFilesFacade(), path.$())
            ) {
                CVStringTable.setupColumnVersionWriter(w1, srcExpected);
                CVStringTable.setupColumnVersionWriter(w2, dstExpected);
                for (long p : partitionTimestamp) {
                    w2.overrideColumnVersions(p, w1);
                }
                TestUtils.assertEquals(dstUpsertFromSrcExpected, CVStringTable.asTable(w2.getCachedColumnVersionList()));
                w2.commit();
                r.readSafe(configuration.getMillisecondClock(), 1);
                TestUtils.assertEquals(dstUpsertFromSrcExpected, CVStringTable.asTable(r.getCachedColumnVersionList()));
            }
        });
    }

    private static ColumnVersionReader createColumnVersionReader(Path path) {
        return new ColumnVersionReader().ofRO(TestFilesFacadeImpl.INSTANCE, path.$());
    }

    private static @NotNull ColumnVersionWriter createColumnVersionWriter(Path path) {
        return new ColumnVersionWriter(configuration, path.of(root).concat("_cv").$(), true);
    }

    private void testFuzzConcurrent(int spinLockTimeout) throws Exception {
        assertMemoryLeak(() -> {
            final int N = 10_000;
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = createColumnVersionWriter(path);
                    ColumnVersionReader r = new ColumnVersionReader().ofRO(configuration.getFilesFacade(), path.$())
            ) {
                CyclicBarrier barrier = new CyclicBarrier(2);
                ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
                AtomicLong done = new AtomicLong();

                Thread writer = new Thread(() -> {
                    Rnd rnd = new Rnd();
                    try {
                        barrier.await();
                        for (int txn = 0; txn < N; txn++) {
                            int increment = rnd.nextInt(32);
                            for (int j = 0; j < increment; j++) {
                                w.upsert(rnd.nextLong(20), rnd.nextInt(10), txn, -1);
                            }
                            LongList list = w.getCachedColumnVersionList();
                            for (int j = 0, n = list.size(); j < n; j += ColumnVersionWriter.BLOCK_SIZE) {
                                long timestamp = list.getQuick(j);
                                int index = (int) list.getQuick(j + 1);
                                w.upsert(timestamp, index, txn, -1);
                            }
                            w.commit();
                        }
                    } catch (Throwable th) {
                        exceptions.add(th);
                    } finally {
                        done.incrementAndGet();
                    }
                });

                Thread reader = new Thread(() -> {
                    try {
                        barrier.await();
                        while (done.get() == 0) {
                            try {
                                r.readSafe(configuration.getMillisecondClock(), spinLockTimeout);
                            } catch (CairoException ex) {
                                if (spinLockTimeout == 0 && Chars.contains(ex.getFlyweightMessage(), "timeout")) {
                                    continue;
                                }
                                throw ex;
                            }
                            long txn = -1;
                            LongList list = r.getCachedColumnVersionList();
                            long prevTimestamp = -1;
                            long prevColumnIndex = -1;

                            for (int i = 0, n = list.size(); i < n; i += ColumnVersionWriter.BLOCK_SIZE) {
                                long timestamp = list.getQuick(i);
                                long columnIndex = list.getQuick(i + 1);

                                if (prevTimestamp < timestamp) {
                                    prevTimestamp = timestamp;
                                    prevColumnIndex = columnIndex;
                                    continue;
                                } else {
                                    if (prevTimestamp == timestamp) {
                                        Assert.assertTrue(prevColumnIndex < columnIndex);
                                        prevColumnIndex = columnIndex;
                                    } else {
                                        Assert.fail();
                                    }
                                }

                                long txn2 = list.getQuick(i + 2);
                                if (txn == -1) {
                                    txn = txn2;
                                } else if (txn != txn2) {
                                    // All txn must be same.
                                    Assert.assertEquals("index " + i / ColumnVersionWriter.BLOCK_SIZE + ", version " + r.getVersion(), txn, txn2);
                                }
                            }
                        }
                    } catch (Throwable th) {
                        exceptions.add(th);
                    }
                });

                writer.start();
                reader.start();

                writer.join();
                reader.join();

                if (!exceptions.isEmpty()) {
                    Assert.fail(exceptions.poll().toString());
                }
            }
        });
    }

    // Deterministic, seed-dependent pseudo-random byte fill (xorshift): every byte position carries
    // entropy and distinct seeds produce distinct content. Mirrors TxnTest.fillPattern.
    private static void fillPattern(long addr, long size, int seed) {
        int x = seed | 1; // avoid the zero-stuck xorshift state
        for (long i = 0; i < size; i++) {
            x ^= x << 13;
            x ^= x >>> 17;
            x ^= x << 5;
            Unsafe.putByte(addr + i, (byte) x);
        }
    }

    // Asserts the two areas' FULL on-disk footprints [offset, offset+size+8) (data + trailing checksum
    // long) are disjoint - the invariant the +8 placement reservation must guarantee.
    private static void assertNoAreaOverlap(long offsetA, long sizeA, long offsetB, long sizeB) {
        long endA = offsetA + sizeA + Long.BYTES; // exclusive end including the trailing checksum
        long endB = offsetB + sizeB + Long.BYTES;
        boolean disjoint = endA <= offsetB || endB <= offsetA;
        Assert.assertTrue(
                "area footprints overlap: A=[" + offsetA + "," + endA + ") B=[" + offsetB + "," + endB + ")",
                disjoint
        );
    }

    // Reads `size` bytes of blocks from the on-disk area at `offset` and asserts they equal what the reader
    // loaded into its cached list (same order). Proves the fallback exposed the prior area byte-for-byte.
    private static void assertReaderMatchesArea(FilesFacade ff, LPSZ path, long offset, long size, ColumnVersionReader r) {
        int blocks = (int) (size / BLOCK_SIZE_BYTES);
        LongList list = r.getCachedColumnVersionList();
        Assert.assertEquals(blocks * ColumnVersionWriter.BLOCK_SIZE, list.size());
        for (int b = 0; b < blocks; b++) {
            long base = offset + (long) b * BLOCK_SIZE_BYTES;
            for (int w = 0; w < ColumnVersionWriter.BLOCK_SIZE; w++) {
                long onDisk = peekLong(ff, path, base + (long) w * Long.BYTES);
                long inReader = list.getQuick(b * ColumnVersionWriter.BLOCK_SIZE + w);
                Assert.assertEquals("block " + b + " word " + w + " mismatch vs prior area on disk", onDisk, inReader);
            }
        }
    }

    // Positional 8-byte read of the _cv file (no mmap, so it cannot truncate the file on close).
    private static long peekLong(FilesFacade ff, LPSZ path, long offset) {
        long fd = ff.openRO(path);
        Assert.assertTrue(fd > -1);
        long buf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals(Long.BYTES, ff.read(fd, buf, Long.BYTES, offset));
            return Unsafe.getLong(buf);
        } finally {
            Unsafe.free(buf, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    // Positional 8-byte write of the _cv file. Used to corrupt a committed area on disk WITHOUT recomputing
    // its trailing checksum and WITHOUT truncating the file (a writable mmap would truncate). Mirrors TxnTest.
    private static void pokeLong(FilesFacade ff, LPSZ path, long offset, long value) {
        long fd = ff.openRW(path, CairoConfiguration.O_NONE);
        Assert.assertTrue(fd > -1);
        long buf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putLong(buf, value);
            Assert.assertEquals(Long.BYTES, ff.write(fd, buf, Long.BYTES, offset));
            ff.fsync(fd);
        } finally {
            Unsafe.free(buf, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    // Truncates the _cv file to exactly `len` bytes (used to forge an old-format file with no trailing long).
    private static void truncateFile(FilesFacade ff, LPSZ path, long len) {
        long fd = ff.openRW(path, CairoConfiguration.O_NONE);
        Assert.assertTrue(fd > -1);
        try {
            Assert.assertTrue(ff.truncate(fd, len));
        } finally {
            ff.close(fd);
        }
    }

    private static abstract class CVStringTable {
        private static final Formatter strF = new Formatter(new SinkFormatterAdapter());

        private static long[] parseColumnVersionTable(String table) {
            String[] rows = table.split("\n");
            long[] values = new long[(rows.length - 1) * ColumnVersionWriter.BLOCK_SIZE]; // minus header
            for (int i = 1, k = 0; i < rows.length; i++) {
                String[] columns = rows[i].split("\\s+");
                assert columns.length == 5;
                for (int j = 1; j < columns.length; j++) {
                    values[k++] = Long.parseLong(columns[j]);
                }
            }
            assert values.length > 0 && values.length % ColumnVersionWriter.BLOCK_SIZE == 0;
            return values;
        }

        static String asTable(LongList cachedList) {
            sink.clear();
            strF.format("%8s%8s%8s%8s\n", "pts", "colIdx", "colTxn", "colTop");
            for (int i = 0; i < cachedList.size(); i++) {
                strF.format("%8d", cachedList.getQuick(i));
                if (i > 0 && (i + 1) % ColumnVersionWriter.BLOCK_SIZE == 0) {
                    sink.put('\n');
                }
            }
            return sink.toString();
        }

        static void setupColumnVersionWriter(ColumnVersionWriter w, String expectedTable) {
            long[] values = parseColumnVersionTable(expectedTable);
            for (int i = 0; i < values.length; i += ColumnVersionWriter.BLOCK_SIZE) {
                w.upsert(
                        values[i],
                        (int) values[i + ColumnVersionWriter.COLUMN_INDEX_OFFSET],
                        values[i + ColumnVersionWriter.COLUMN_NAME_TXN_OFFSET],
                        values[i + ColumnVersionWriter.COLUMN_TOP_OFFSET]);
            }
            TestUtils.assertEquals(expectedTable, asTable(w.getCachedColumnVersionList()));
        }

        private static final class SinkFormatterAdapter extends StringSink implements Appendable {
            @Override
            public Appendable append(CharSequence csq) {
                sink.put(csq);
                return this;
            }

            @Override
            public Appendable append(CharSequence csq, int start, int end) {
                sink.put(csq, start, end);
                return this;
            }

            @Override
            public Appendable append(char c) {
                sink.put(c);
                return this;
            }
        }
    }
}
