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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.BinaryTypeDriver;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.StringTypeDriver;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8SplitString;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.engine.TestBinarySequence;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for the N=2 in-memory tier infrastructure.
 * No live-view CREATE / refresh involved — the tier is exercised standalone
 * so allocations + CAS-based pin/swap can be audited without higher-layer
 * noise. All tests run under {@code assertMemoryLeak} to catch native-memory
 * regressions in the column buffers and the refcount block.
 */
public class LiveViewInMemoryTierTest extends AbstractCairoTest {

    private static final long PAGE_SIZE = 4096L;

    @Test
    public void testApproxRowSizeBytes() throws Exception {
        // approxRowSizeBytes() turns the byte-denominated IN MEMORY growth budget into a row
        // count for LiveViewRefreshJob's amortised-compaction gate: it sums the exact
        // fixed-width stride of every column and charges a nominal 32B for each var-size column
        // (whose true per-row size is data dependent). A wrong sum would skew the compaction
        // cadence, so pin the arithmetic for representative schemas. The buffers allocate their
        // per-column arenas at construction, so this runs under assertMemoryLeak.
        assertMemoryLeak(() -> {
            // Single fixed-width column: just its stride.
            try (LiveViewInMemoryBuffer buf = new LiveViewInMemoryBuffer(singleLongCol(), 0, PAGE_SIZE)) {
                Assert.assertEquals(Long.BYTES, buf.approxRowSizeBytes());
            }
            // All fixed-width: strides add up (TIMESTAMP 8 + LONG256 32 + UUID 16 + LONG128 16).
            try (LiveViewInMemoryBuffer buf = new LiveViewInMemoryBuffer(wideFixedWidthSchema(), 0, PAGE_SIZE)) {
                Assert.assertEquals(8 + 32 + 16 + 16, buf.approxRowSizeBytes());
            }
            // One var-size column charges the nominal 32B on top of the TIMESTAMP stride.
            try (LiveViewInMemoryBuffer buf = new LiveViewInMemoryBuffer(varcharSchema(), 0, PAGE_SIZE)) {
                Assert.assertEquals(8 + 32, buf.approxRowSizeBytes());
            }
            // Two var-size columns are each charged 32B (TIMESTAMP 8 + STRING 32 + BINARY 32).
            try (LiveViewInMemoryBuffer buf = new LiveViewInMemoryBuffer(strBinSchema(), 0, PAGE_SIZE)) {
                Assert.assertEquals(8 + 32 + 32, buf.approxRowSizeBytes());
            }
        });
    }

    @Test
    public void testCloseFreesAllNativeMemory() throws Exception {
        // The whole class runs under assertMemoryLeak; this test exists so the
        // failure mode (forgetting to free either column buffers or the refcount
        // block) surfaces at the smallest possible allocation footprint.
        assertMemoryLeak(() -> {
            IntList types = new IntList(1);
            types.add(ColumnType.LONG);
            try (LiveViewInMemoryTier tier = new LiveViewInMemoryTier(types, 0, PAGE_SIZE)) {
                LiveViewInMemoryBuffer slot = tier.tryAcquireWrite(1 - tier.getPublishedIdx());
                Assert.assertNotNull(slot);
                slot.putLong(0, 0, 42L);
                slot.setRowCount(1);
                tier.publishSwap(1 - tier.getPublishedIdx());
            }
        });
    }

    @Test
    public void testColumnTypeSupportGate() {
        // The tier stores every persisted (storable) column type: fixed-width, SYMBOL,
        // the variable-length STRING / BINARY / VARCHAR / ARRAY types, the wide
        // LONG256 / LONG128 / UUID, and every DECIMAL width. Only a non-persisted /
        // internal type - which an LV output can never carry - is rejected, so the
        // disk-only fallback the gate guards is now defensive. This unit check is the
        // replacement for the old integration tests that pinned the disk-only path on
        // a DECIMAL output column (now stored, so no persisted type disables the tier).
        // No native memory is allocated, so assertMemoryLeak is unnecessary.
        int[] supported = {
                ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.SHORT, ColumnType.CHAR,
                ColumnType.INT, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP,
                ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.STRING, ColumnType.SYMBOL,
                ColumnType.LONG256, ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT,
                ColumnType.GEOLONG, ColumnType.BINARY, ColumnType.UUID, ColumnType.LONG128,
                ColumnType.IPv4, ColumnType.VARCHAR, ColumnType.encodeArrayType(ColumnType.DOUBLE, 1),
                ColumnType.getDecimalType(2, 0), ColumnType.getDecimalType(4, 1),
                ColumnType.getDecimalType(9, 3), ColumnType.getDecimalType(18, 2),
                ColumnType.getDecimalType(38, 6), ColumnType.getDecimalType(60, 0),
        };
        IntList allSupported = new IntList(supported.length);
        for (int type : supported) {
            Assert.assertTrue(
                    "tier must store " + ColumnType.nameOf(type),
                    LiveViewInMemoryBuffer.isColumnTypeSupported(type)
            );
            allSupported.add(type);
        }
        Assert.assertTrue(LiveViewInMemoryBuffer.areColumnTypesSupported(allSupported));

        // A non-persisted type (INTERVAL) an LV output can never carry is the only kind
        // the gate rejects; areColumnTypesSupported turns false if any column is one.
        Assert.assertFalse(LiveViewInMemoryBuffer.isColumnTypeSupported(ColumnType.INTERVAL));
        IntList withUnsupported = new IntList(2);
        withUnsupported.add(ColumnType.TIMESTAMP);
        withUnsupported.add(ColumnType.INTERVAL);
        Assert.assertFalse(LiveViewInMemoryBuffer.areColumnTypesSupported(withUnsupported));
    }

    @Test
    public void testDoublePinStallsWriter() throws Exception {
        // When both slots are pinned by readers the
        // writer cannot make progress; tryAcquireWrite returns null and the
        // refresh worker trails.
        assertMemoryLeak(() -> {
            IntList types = singleLongCol();
            try (LiveViewInMemoryTier tier = new LiveViewInMemoryTier(types, 0, PAGE_SIZE)) {
                // Seed both slots: write into the non-published slot and swap so
                // both have been writer-touched. Both slots are then idle (rc=0).
                int writeIdx = 1 - tier.getPublishedIdx();
                LiveViewInMemoryBuffer slotA = tier.tryAcquireWrite(writeIdx);
                slotA.putLong(0, 0, 1L);
                slotA.setRowCount(1);
                tier.publishSwap(writeIdx);

                writeIdx = 1 - tier.getPublishedIdx();
                LiveViewInMemoryBuffer slotB = tier.tryAcquireWrite(writeIdx);
                slotB.putLong(0, 0, 2L);
                slotB.setRowCount(1);
                tier.publishSwap(writeIdx);

                // Pin the published slot from a reader.
                int readPin = tier.acquireRead();

                // Force the OTHER slot to also be reader-pinned by atomically
                // swapping publishedIdx via a write+publish cycle and pinning the
                // newly-published slot too.
                int otherIdx = 1 - readPin;
                LiveViewInMemoryBuffer otherWrite = tier.tryAcquireWrite(otherIdx);
                Assert.assertNotNull("seed write on other slot must succeed", otherWrite);
                tier.publishSwap(otherIdx);
                int otherPin = tier.acquireRead();
                Assert.assertEquals("second pin must land on the now-published slot", otherIdx, otherPin);

                // Both slots are now reader-pinned: try to acquire write on either.
                Assert.assertNull(
                        "tryAcquireWrite must return null on the published slot (pinned)",
                        tier.tryAcquireWrite(otherPin)
                );
                Assert.assertNull(
                        "tryAcquireWrite must return null on the older slot (still pinned)",
                        tier.tryAcquireWrite(readPin)
                );

                // Release one pin — writer can now make progress on that slot.
                tier.releaseRead(readPin);
                LiveViewInMemoryBuffer revived = tier.tryAcquireWrite(readPin);
                Assert.assertNotNull("writer must acquire the just-released slot", revived);
                revived.putLong(0, 0, 3L);
                revived.setRowCount(1);
                tier.publishSwap(readPin);

                // Clean up the lingering pin so close() doesn't observe a stale rc.
                tier.releaseRead(otherPin);
            }
        });
    }

    @Test
    public void testFootprintBytesToleratesConcurrentClose() throws Exception {
        // footprintBytes() / publishedRowCount() feed live_views() and run
        // lock-free off the catalogue cursor with no read pin, so a concurrent
        // DROP can free the tier mid-scan. freeNativeMemory() closes each buffer -
        // Misc.freeObjList(dataMem) then dataMem.clear() (Arrays.fill(null)) -
        // before nulling the slot reference, so a reader that captured the slot
        // non-null then walked the buffer used to NPE on a nulled dataMem / auxMem
        // entry. Race many trials of the monitoring reads against close(); a
        // regression in the buffer's null guards throws.
        assertMemoryLeak(() -> {
            final int trials = 100;
            for (int t = 0; t < trials; t++) {
                final LiveViewInMemoryTier tier = new LiveViewInMemoryTier(strBinSchema(), 0, PAGE_SIZE);
                int writeIdx = 1 - tier.getPublishedIdx();
                LiveViewInMemoryBuffer slot = tier.tryAcquireWrite(writeIdx);
                Assert.assertNotNull(slot);
                VarSizeRecord rec = new VarSizeRecord();
                final int rows = 16;
                for (int r = 0; r < rows; r++) {
                    rec.of((r + 1) * 1_000_000L, "row-" + r, new TestBinarySequence().of(bytesOf(r + 1, 8)));
                    slot.copyRowFromRecord(rec, r);
                }
                slot.setRowCount(rows);
                tier.publishSwap(writeIdx);

                final CountDownLatch start = new CountDownLatch(1);
                final AtomicReference<Throwable> error = new AtomicReference<>();
                final AtomicBoolean closed = new AtomicBoolean();
                Thread reader = new Thread(() -> {
                    try {
                        start.await();
                        while (!closed.get()) {
                            tier.footprintBytes();
                            tier.publishedRowCount();
                        }
                        // One more read after teardown, covering the fully-nulled slots.
                        tier.footprintBytes();
                        tier.publishedRowCount();
                    } catch (Throwable th) {
                        error.set(th);
                    }
                });
                reader.start();
                start.countDown();
                tier.close();
                closed.set(true);
                reader.join(TimeUnit.SECONDS.toMillis(10));
                Assert.assertFalse("reader thread must terminate", reader.isAlive());
                if (error.get() != null) {
                    throw new AssertionError(
                            "monitoring read threw during concurrent close",
                            error.get()
                    );
                }
            }
        });
    }

    @Test
    public void testReadAfterPublishSwap() throws Exception {
        assertMemoryLeak(() -> {
            IntList types = new IntList(2);
            types.add(ColumnType.TIMESTAMP);
            types.add(ColumnType.DOUBLE);
            try (LiveViewInMemoryTier tier = new LiveViewInMemoryTier(types, 0, PAGE_SIZE)) {
                int writeIdx = 1 - tier.getPublishedIdx();
                LiveViewInMemoryBuffer write = tier.tryAcquireWrite(writeIdx);
                Assert.assertNotNull(write);
                write.putTimestamp(0, 0, 1_000_000L);
                write.putDouble(0, 1, 3.14);
                write.putTimestamp(1, 0, 2_000_000L);
                write.putDouble(1, 1, 2.71);
                write.setRowCount(2);
                write.setSeamTs(1_000_000L);
                tier.publishSwap(writeIdx);

                int pinned = tier.acquireRead();
                Assert.assertEquals("acquireRead must observe the just-published slot", writeIdx, pinned);
                LiveViewInMemoryBuffer slot = tier.getSlot(pinned);
                Assert.assertEquals(2L, slot.rowCount());
                Assert.assertEquals(1_000_000L, slot.seamTs());
                Assert.assertEquals(1_000_000L, slot.getTimestamp(0, 0));
                Assert.assertEquals(3.14, slot.getDouble(0, 1), 1e-12);
                Assert.assertEquals(2_000_000L, slot.getTimestamp(1, 0));
                Assert.assertEquals(2.71, slot.getDouble(1, 1), 1e-12);
                tier.releaseRead(pinned);
            }
        });
    }

    @Test
    public void testReaderObservesWriterSentinelAndSpins() throws Exception {
        // Reader CAS-loop: rc < 0 on the PUBLISHED slot means a writer holds the
        // sentinel; the reader must spin until it is released. This is the fast-path
        // in-place append: the writer takes the sentinel on the published slot,
        // appends, then releases via releaseWriteWithoutPublish WITHOUT swapping. A
        // background reader entering acquireRead must block in the spin until that
        // release, then pin the published slot. (The old version of this test held the
        // sentinel on the NON-published slot, so acquireRead pinned the published slot
        // immediately and never exercised the spin.)
        assertMemoryLeak(() -> {
            IntList types = singleLongCol();
            try (LiveViewInMemoryTier tier = new LiveViewInMemoryTier(types, 0, PAGE_SIZE)) {
                int published = tier.getPublishedIdx();
                LiveViewInMemoryBuffer seed = tier.getSlot(published);
                seed.putLong(0, 0, 11L);
                seed.setRowCount(1);

                // Writer takes the sentinel on the PUBLISHED slot (fast-path append).
                LiveViewInMemoryBuffer write = tier.tryAcquireWrite(published);
                Assert.assertNotNull("writer must acquire the idle published slot", write);

                final CountDownLatch readerEntered = new CountDownLatch(1);
                final AtomicInteger pinnedResult = new AtomicInteger(Integer.MIN_VALUE);
                final AtomicLong observedValue = new AtomicLong(Long.MIN_VALUE);
                final AtomicReference<Throwable> error = new AtomicReference<>();
                Thread reader = new Thread(() -> {
                    try {
                        readerEntered.countDown();
                        // Spins here while the sentinel is held on the published slot.
                        int pin = tier.acquireRead();
                        observedValue.set(tier.getSlot(pin).getLong(0, 0));
                        pinnedResult.set(pin);
                        tier.releaseRead(pin);
                    } catch (Throwable th) {
                        error.set(th);
                    }
                }, "lv-tier-sentinel-reader");
                reader.start();

                // While the sentinel is held, acquireRead cannot return - it sees rc < 0
                // and spins. Confirm the reader stays blocked (no pin yet); the only way
                // pinnedResult could be set here is a bug that lets a read observe a slot
                // mid-write.
                Assert.assertTrue(readerEntered.await(10, TimeUnit.SECONDS));
                for (int i = 0; i < 10 && reader.isAlive(); i++) {
                    Assert.assertEquals(
                            "reader must spin while the writer sentinel is held",
                            Integer.MIN_VALUE,
                            pinnedResult.get()
                    );
                    Os.sleep(5);
                }

                // Append a row under the sentinel, then release without swapping. The
                // reader's spin now completes and pins the (same) published slot.
                write.putLong(1, 0, 22L);
                write.setRowCount(2);
                tier.releaseWriteWithoutPublish(published);

                reader.join(TimeUnit.SECONDS.toMillis(10));
                Assert.assertFalse("reader thread must terminate after the sentinel release", reader.isAlive());
                if (error.get() != null) {
                    throw new AssertionError("reader threw while spinning on the sentinel", error.get());
                }
                Assert.assertEquals("reader must pin the published slot", published, pinnedResult.get());
                Assert.assertEquals("reader must observe the published slot's rows", 11L, observedValue.get());
            }
        });
    }

    @Test
    public void testTryAcquireWriteFailsWhenSlotIsPinnedSucceedsOnOther() throws Exception {
        assertMemoryLeak(() -> {
            IntList types = singleLongCol();
            try (LiveViewInMemoryTier tier = new LiveViewInMemoryTier(types, 0, PAGE_SIZE)) {
                // Seed the published slot with a row so acquireRead returns something
                // sensible.
                int published = tier.getPublishedIdx();
                LiveViewInMemoryBuffer seed = tier.getSlot(published);
                seed.putLong(0, 0, 7L);
                seed.setRowCount(1);

                int pinned = tier.acquireRead();
                try {
                    Assert.assertNull(
                            "tryAcquireWrite must fail while a reader pins the slot",
                            tier.tryAcquireWrite(pinned)
                    );
                    // The OTHER slot has no readers — writer should succeed and
                    // publishSwap must release the sentinel cleanly.
                    int otherIdx = 1 - pinned;
                    LiveViewInMemoryBuffer otherWrite = tier.tryAcquireWrite(otherIdx);
                    Assert.assertNotNull(otherWrite);
                    otherWrite.putLong(0, 0, 8L);
                    otherWrite.setRowCount(1);
                    tier.publishSwap(otherIdx);
                } finally {
                    tier.releaseRead(pinned);
                }
            }
        });
    }

    @Test
    public void testWriterReleaseUnpinsReader() throws Exception {
        // Validates the acquire/release lifecycle from two threads. One thread
        // briefly holds a write sentinel + publishes; the other thread does
        // acquireRead -> read -> releaseRead and must observe the post-publish
        // contents.
        assertMemoryLeak(() -> {
            IntList types = singleLongCol();
            try (LiveViewInMemoryTier tier = new LiveViewInMemoryTier(types, 0, PAGE_SIZE)) {
                final CountDownLatch readerReady = new CountDownLatch(1);
                final AtomicReference<Long> observed = new AtomicReference<>();

                Thread reader = new Thread(() -> {
                    try {
                        readerReady.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    int pin = tier.acquireRead();
                    try {
                        if (tier.getSlot(pin).rowCount() > 0) {
                            observed.set(tier.getSlot(pin).getLong(0, 0));
                        }
                    } finally {
                        tier.releaseRead(pin);
                    }
                }, "lv-tier-test-reader");
                reader.start();

                int writeIdx = 1 - tier.getPublishedIdx();
                LiveViewInMemoryBuffer write = tier.tryAcquireWrite(writeIdx);
                Assert.assertNotNull(write);
                write.putLong(0, 0, 99L);
                write.setRowCount(1);
                tier.publishSwap(writeIdx);

                readerReady.countDown();
                reader.join(TimeUnit.SECONDS.toMillis(5));
                Assert.assertFalse("reader thread must complete", reader.isAlive());
                Assert.assertEquals(Long.valueOf(99L), observed.get());
            }
        });
    }

    @Test
    public void testAcquireReadAfterCloseReturnsMinusOne() throws Exception {
        // Once close() is observed, no new
        // cursor should be able to pin a slot. Returning -1 from acquireRead
        // lets the caller fall through to disk-only reads without segfaulting
        // on freed native memory.
        assertMemoryLeak(() -> {
            IntList types = singleLongCol();
            LiveViewInMemoryTier tier = new LiveViewInMemoryTier(types, 0, PAGE_SIZE);
            tier.close();
            // Native memory has been freed (no pins were active); subsequent
            // acquireRead must reject cleanly rather than read through the
            // freed refcount block.
            Assert.assertEquals(-1, tier.acquireRead());
            // close() is idempotent.
            tier.close();
        });
    }

    @Test
    public void testDeferredCloseFreesWhenLastPinReleases() throws Exception {
        // Modulo cursor pins: close() while a
        // cursor holds a pin must defer native memory free until the cursor
        // releases. Verifies the cursor-side releaseRead path remains safe
        // after the LV's DROP has marked the tier closed.
        assertMemoryLeak(() -> {
            IntList types = singleLongCol();
            LiveViewInMemoryTier tier = new LiveViewInMemoryTier(types, 0, PAGE_SIZE);
            try {
                // Seed and pin.
                int writeIdx = 1 - tier.getPublishedIdx();
                LiveViewInMemoryBuffer write = tier.tryAcquireWrite(writeIdx);
                Assert.assertNotNull(write);
                write.putLong(0, 0, 42L);
                write.setRowCount(1);
                tier.publishSwap(writeIdx);
                int pin = tier.acquireRead();
                Assert.assertTrue(pin >= 0);

                // Request close — cannot free yet because the pin is live.
                tier.close();
                // Reader still observes the pinned slot's contents — no UAF.
                Assert.assertEquals(42L, tier.getSlot(pin).getLong(0, 0));
                // New acquires reject post-close.
                Assert.assertEquals(-1, tier.acquireRead());

                // Last release drains the pin count and triggers the actual free.
                tier.releaseRead(pin);
            } catch (Throwable t) {
                // Defensive: ensure native memory does not leak if assertions fail.
                tier.close();
                throw t;
            }
            // assertMemoryLeak verifies the native refcount block + slot
            // buffers were ultimately freed.
        });
    }

    @Test
    public void testReleaseWriteWithoutPublishKeepsPriorSlotPublished() throws Exception {
        // A copy failure mid-swap must not flip publishedIdx,
        // otherwise readers would silently regress from N rows to 0 rows.
        // releaseWriteWithoutPublish drops the writer sentinel while leaving
        // publishedIdx pointing at the prior (still-populated) slot.
        assertMemoryLeak(() -> {
            IntList types = singleLongCol();
            try (LiveViewInMemoryTier tier = new LiveViewInMemoryTier(types, 0, PAGE_SIZE)) {
                // Seed the initially-published slot with one row.
                int initialPublished = tier.getPublishedIdx();
                LiveViewInMemoryBuffer pub = tier.getSlot(initialPublished);
                pub.putLong(0, 0, 11L);
                pub.setRowCount(1);

                // Acquire write on the OTHER slot, do nothing (simulating a
                // partial copy that threw), then release without publishing.
                int writeIdx = 1 - initialPublished;
                LiveViewInMemoryBuffer write = tier.tryAcquireWrite(writeIdx);
                Assert.assertNotNull(write);
                // Note: we deliberately do NOT call setRowCount — the slot's
                // contents are partial / zero-row.
                tier.releaseWriteWithoutPublish(writeIdx);

                // publishedIdx must still point at the original slot.
                Assert.assertEquals(initialPublished, tier.getPublishedIdx());
                int pin = tier.acquireRead();
                Assert.assertEquals("reader still sees the previously-published slot",
                        initialPublished, pin);
                Assert.assertEquals(1L, tier.getSlot(pin).rowCount());
                Assert.assertEquals(11L, tier.getSlot(pin).getLong(0, 0));
                tier.releaseRead(pin);

                // The write slot must be re-acquirable (sentinel cleared).
                LiveViewInMemoryBuffer second = tier.tryAcquireWrite(writeIdx);
                Assert.assertNotNull("write slot must be re-acquirable after release-without-publish",
                        second);
                second.putLong(0, 0, 22L);
                second.setRowCount(1);
                tier.publishSwap(writeIdx);
            }
        });
    }

    @Test
    public void testArrayRegionReallocMidFill() throws Exception {
        // Long, growing 1-D arrays force both the dataMem payload region and the
        // auxMem header region to realloc (moving their base addresses) mid-fill.
        // The BorrowedArray-decoded reads must still resolve against the relocated
        // blocks once the fill ends.
        assertMemoryLeak(() -> {
            IntList types = array1dSchema();
            try (
                    LiveViewInMemoryBuffer buf = new LiveViewInMemoryBuffer(types, 0, PAGE_SIZE);
                    DirectArray a = new DirectArray(configuration)
            ) {
                ArrayRecord rec = new ArrayRecord();
                final int rows = 8;
                for (int r = 0; r < rows; r++) {
                    // Each array spans several pages and grows per row, so the
                    // payload region reallocs multiple times during the fill.
                    set1d(a, doubleSeq(r, (int) PAGE_SIZE * (r + 1)));
                    rec.of((r + 1) * 1_000_000L, a, null);
                    buf.copyRowFromRecord(rec, r);
                }
                buf.setRowCount(rows);

                for (int r = 0; r < rows; r++) {
                    assertArray1dEquals(doubleSeq(r, (int) PAGE_SIZE * (r + 1)), buf.getArray(r, 1));
                }
            }
        });
    }

    @Test
    public void testArrayRoundTripThroughTier() throws Exception {
        // Write DOUBLE[] (1-D) and DOUBLE[][] (2-D) rows - including a NULL array
        // and an empty array - into a slot, publish, then read them back through the
        // buffer's per-column BorrowedArray after a reader pins the published slot.
        // Two array columns prove the per-column flyweight: an ArrayView from one
        // column survives a getArray on the other (the reuse contract).
        assertMemoryLeak(() -> {
            IntList types = array1d2dSchema();
            try (
                    LiveViewInMemoryTier tier = new LiveViewInMemoryTier(types, 0, PAGE_SIZE);
                    DirectArray a1 = new DirectArray(configuration);
                    DirectArray a2 = new DirectArray(configuration)
            ) {
                int writeIdx = 1 - tier.getPublishedIdx();
                LiveViewInMemoryBuffer slot = tier.tryAcquireWrite(writeIdx);
                Assert.assertNotNull(slot);
                ArrayRecord rec = new ArrayRecord();

                // row 0: 1-D [1,2,3] + 2-D [[10,20],[30,40]].
                set1d(a1, 1.0, 2.0, 3.0);
                set2d(a2, 2, 2, 10.0, 20.0, 30.0, 40.0);
                rec.of(1_000_000L, a1, a2);
                slot.copyRowFromRecord(rec, 0);

                // row 1: NULL 1-D + NULL 2-D.
                a1.ofNull();
                a2.ofNull();
                rec.of(2_000_000L, a1, a2);
                slot.copyRowFromRecord(rec, 1);

                // row 2: empty 1-D [] + single-element 2-D [[7]].
                set1d(a1);
                set2d(a2, 1, 1, 7.0);
                rec.of(3_000_000L, a1, a2);
                slot.copyRowFromRecord(rec, 2);
                slot.setRowCount(3);
                tier.publishSwap(writeIdx);

                int pin = tier.acquireRead();
                Assert.assertEquals(writeIdx, pin);
                LiveViewInMemoryBuffer r = tier.getSlot(pin);
                Assert.assertEquals(3, r.rowCount());

                // row 0: the 1-D and 2-D views are distinct per-column instances, so
                // holding both at once does not clobber either.
                ArrayView v1 = r.getArray(0, 1);
                ArrayView v2 = r.getArray(0, 2);
                assertArray1dEquals(new double[]{1.0, 2.0, 3.0}, v1);
                Assert.assertEquals(2, v2.getDimCount());
                Assert.assertEquals(2, v2.getDimLen(0));
                Assert.assertEquals(2, v2.getDimLen(1));
                Assert.assertEquals(10.0, v2.getDouble(0), 0.0);
                Assert.assertEquals(20.0, v2.getDouble(1), 0.0);
                Assert.assertEquals(30.0, v2.getDouble(2), 0.0);
                Assert.assertEquals(40.0, v2.getDouble(3), 0.0);

                // row 1: NULL arrays.
                Assert.assertTrue(r.getArray(1, 1).isNull());
                Assert.assertTrue(r.getArray(1, 2).isNull());

                // row 2: empty 1-D (not null) + 2-D [[7]].
                ArrayView e1 = r.getArray(2, 1);
                Assert.assertFalse(e1.isNull());
                Assert.assertTrue(e1.isEmpty());
                ArrayView e2 = r.getArray(2, 2);
                Assert.assertFalse(e2.isNull());
                Assert.assertEquals(7.0, e2.getDouble(0), 0.0);

                tier.releaseRead(pin);
            }
        });
    }

    @Test
    public void testArraySlowPathSwapPreservesValues() throws Exception {
        // The slow-path swap rebuilds a slot via copyRowFrom (buffer -> buffer),
        // re-appending each retained ARRAY through the driver. Skipping rows
        // (eviction) keeps the destination append dense, so the order assert holds
        // and the copied values round-trip across the normal / null cases.
        assertMemoryLeak(() -> {
            IntList types = array1dSchema();
            LiveViewInMemoryBuffer src = new LiveViewInMemoryBuffer(types, 0, PAGE_SIZE);
            LiveViewInMemoryBuffer dst = new LiveViewInMemoryBuffer(types, 0, PAGE_SIZE);
            try (DirectArray a = new DirectArray(configuration)) {
                ArrayRecord rec = new ArrayRecord();
                final int rows = 6;
                for (int r = 0; r < rows; r++) {
                    if (r % 3 == 0) {
                        a.ofNull();
                    } else {
                        set1d(a, doubleSeq(r, r + 1));
                    }
                    rec.of((r + 1) * 1_000_000L, a, null);
                    src.copyRowFromRecord(rec, r);
                }
                src.setRowCount(rows);

                // Retain only the odd src rows, densely, into dst (mimics eviction).
                long dstRow = 0;
                for (int r = 1; r < rows; r += 2) {
                    dst.copyRowFrom(src, r, dstRow);
                    dstRow++;
                }
                dst.setRowCount(dstRow);
                Assert.assertEquals(3, dst.rowCount());

                dstRow = 0;
                for (int r = 1; r < rows; r += 2) {
                    if (r % 3 == 0) {
                        Assert.assertTrue(dst.getArray(dstRow, 1).isNull());
                    } else {
                        assertArray1dEquals(doubleSeq(r, r + 1), dst.getArray(dstRow, 1));
                    }
                    dstRow++;
                }
            } finally {
                src.close();
                dst.close();
            }
        });
    }

    @Test
    public void testStringBinaryRegionReallocMidFill() throws Exception {
        // A value larger than the initial page forces the dataMem region to
        // realloc (moving its base address) mid-fill. The offset-based reads must
        // still resolve correctly against the relocated block once the fill ends.
        assertMemoryLeak(() -> {
            IntList types = strBinSchema();
            try (LiveViewInMemoryBuffer buf = new LiveViewInMemoryBuffer(types, 0, PAGE_SIZE)) {
                VarSizeRecord rec = new VarSizeRecord();
                final int rows = 8;
                for (int r = 0; r < rows; r++) {
                    // Each string is several pages long and grows per row, so the
                    // region reallocs multiple times during the fill.
                    String s = repeat((char) ('a' + r), (int) PAGE_SIZE * (r + 1));
                    byte[] b = bytesOf(r + 1, 3 * (int) PAGE_SIZE);
                    rec.of((r + 1) * 1_000_000L, s, new TestBinarySequence().of(b));
                    buf.copyRowFromRecord(rec, r);
                }
                buf.setRowCount(rows);

                for (int r = 0; r < rows; r++) {
                    String s = repeat((char) ('a' + r), (int) PAGE_SIZE * (r + 1));
                    byte[] b = bytesOf(r + 1, 3 * (int) PAGE_SIZE);
                    TestUtils.assertEquals(s, buf.getStrA(r, 1));
                    Assert.assertEquals(s.length(), buf.getStrLen(r, 1));
                    assertBinEquals(b, buf.getBin(r, 2));
                    Assert.assertEquals(b.length, buf.getBinLen(r, 2));
                }
            }
        });
    }

    @Test
    public void testStringBinaryRefillAfterResetPreservesValues() throws Exception {
        // reset() rewinds the STRING / BINARY aux cursor to 0, dropping the leading
        // terminator the layout is built on, so it has to put it back before the refill
        // appends over it. Nothing else catches a dropped seed: a first fill is fine
        // (construction seeds it) and the extents alone look plausible either way.
        //
        // Without the re-seed the refill's row 0 writes its END offset at entry 0, shifting
        // the vector down one entry, and aux[row] stops being row's start: every read then
        // resolves the PREVIOUS row's payload, and row 0 reads from wherever entry 0's
        // stale bytes point. Refill with values that differ per row - and shorter ones than
        // the first fill, so a mis-resolved read lands on live bytes rather than tripping a
        // bound and failing for the wrong reason.
        assertMemoryLeak(() -> {
            IntList types = strBinSchema(); // TIMESTAMP, STRING, BINARY
            try (LiveViewInMemoryBuffer buf = new LiveViewInMemoryBuffer(types, 0, PAGE_SIZE)) {
                VarSizeRecord rec = new VarSizeRecord();
                final int firstRows = 5;
                for (int r = 0; r < firstRows; r++) {
                    rec.of((r + 1) * 1_000_000L, "first-fill-" + r + repeat('p', 32), new TestBinarySequence().of(bytesOf(r, 64)));
                    buf.copyRowFromRecord(rec, r);
                }
                buf.setRowCount(firstRows);

                buf.reset();
                assertEmptyExtents(buf, types); // the terminator is back, the fill is gone

                final int rows = 4;
                for (int r = 0; r < rows; r++) {
                    rec.of((r + 1) * 2_000_000L, "b" + r, new TestBinarySequence().of(bytesOf(r + 3, r + 1)));
                    buf.copyRowFromRecord(rec, r);
                }
                buf.setRowCount(rows);

                for (int r = 0; r < rows; r++) {
                    TestUtils.assertEquals("b" + r, buf.getStrA(r, 1));
                    assertBinEquals(bytesOf(r + 3, r + 1), buf.getBin(r, 2));
                }
                // The refilled vector is the native one again, sized to the refill's rows
                // rather than carrying the first fill's entries.
                Assert.assertEquals(StringTypeDriver.INSTANCE.getAuxVectorSize(rows), buf.auxSize(1));
                Assert.assertEquals(BinaryTypeDriver.INSTANCE.getAuxVectorSize(rows), buf.auxSize(2));
            }
        });
    }

    @Test
    public void testStringBinaryRoundTripThroughTier() throws Exception {
        // Write STRING + BINARY rows (including NULL and a real, empty value) into
        // a slot, publish, then read them back through the buffer getters after a
        // reader pins the published slot.
        assertMemoryLeak(() -> {
            IntList types = strBinSchema();
            try (LiveViewInMemoryTier tier = new LiveViewInMemoryTier(types, 0, PAGE_SIZE)) {
                int writeIdx = 1 - tier.getPublishedIdx();
                LiveViewInMemoryBuffer slot = tier.tryAcquireWrite(writeIdx);
                Assert.assertNotNull(slot);
                VarSizeRecord rec = new VarSizeRecord();
                rec.of(1_000_000L, "hello", new TestBinarySequence().of(new byte[]{1, 2, 3}));
                slot.copyRowFromRecord(rec, 0);
                rec.of(2_000_000L, null, null); // NULL string + NULL binary
                slot.copyRowFromRecord(rec, 1);
                rec.of(3_000_000L, "", new TestBinarySequence().of(new byte[0])); // empties (distinct from null)
                slot.copyRowFromRecord(rec, 2);
                slot.setRowCount(3);
                tier.publishSwap(writeIdx);

                int pin = tier.acquireRead();
                Assert.assertEquals(writeIdx, pin);
                LiveViewInMemoryBuffer r = tier.getSlot(pin);
                Assert.assertEquals(3, r.rowCount());

                // row 0: normal values; A and B views are distinct instances.
                TestUtils.assertEquals("hello", r.getStrA(0, 1));
                TestUtils.assertEquals("hello", r.getStrB(0, 1));
                Assert.assertEquals(5, r.getStrLen(0, 1));
                assertBinEquals(new byte[]{1, 2, 3}, r.getBin(0, 2));
                Assert.assertEquals(3, r.getBinLen(0, 2));

                // row 1: NULL string + NULL binary.
                Assert.assertNull(r.getStrA(1, 1));
                Assert.assertEquals(TableUtils.NULL_LEN, r.getStrLen(1, 1));
                Assert.assertNull(r.getBin(1, 2));
                Assert.assertEquals(TableUtils.NULL_LEN, r.getBinLen(1, 2));

                // row 2: empty string + empty binary - distinct from null.
                TestUtils.assertEquals("", r.getStrA(2, 1));
                Assert.assertEquals(0, r.getStrLen(2, 1));
                Assert.assertNotNull(r.getBin(2, 2));
                assertBinEquals(new byte[0], r.getBin(2, 2));
                Assert.assertEquals(0, r.getBinLen(2, 2));

                tier.releaseRead(pin);
            }
        });
    }

    @Test
    public void testStringBinarySlowPathSwapPreservesValues() throws Exception {
        // The slow-path swap rebuilds a slot via copyRowFrom (buffer -> buffer),
        // re-appending each retained var-size value. Skipping rows (eviction)
        // keeps the destination append dense, so the order assert holds and the
        // copied values round-trip.
        assertMemoryLeak(() -> {
            IntList types = strBinSchema();
            LiveViewInMemoryBuffer src = new LiveViewInMemoryBuffer(types, 0, PAGE_SIZE);
            LiveViewInMemoryBuffer dst = new LiveViewInMemoryBuffer(types, 0, PAGE_SIZE);
            try {
                VarSizeRecord rec = new VarSizeRecord();
                final int rows = 6;
                for (int r = 0; r < rows; r++) {
                    String s = (r % 3 == 0) ? null : "v" + r + repeat('x', r);
                    byte[] b = (r % 4 == 0) ? null : bytesOf(r + 7, r);
                    rec.of((r + 1) * 1_000_000L, s, b == null ? null : new TestBinarySequence().of(b));
                    src.copyRowFromRecord(rec, r);
                }
                src.setRowCount(rows);

                // Retain only the odd src rows, densely, into dst (mimics eviction).
                long dstRow = 0;
                for (int r = 1; r < rows; r += 2) {
                    dst.copyRowFrom(src, r, dstRow);
                    dstRow++;
                }
                dst.setRowCount(dstRow);
                Assert.assertEquals(3, dst.rowCount());

                dstRow = 0;
                for (int r = 1; r < rows; r += 2) {
                    String s = (r % 3 == 0) ? null : "v" + r + repeat('x', r);
                    byte[] b = (r % 4 == 0) ? null : bytesOf(r + 7, r);
                    if (s == null) {
                        Assert.assertNull(dst.getStrA(dstRow, 1));
                    } else {
                        TestUtils.assertEquals(s, dst.getStrA(dstRow, 1));
                    }
                    if (b == null) {
                        Assert.assertNull(dst.getBin(dstRow, 2));
                    } else {
                        assertBinEquals(b, dst.getBin(dstRow, 2));
                    }
                    dstRow++;
                }
            } finally {
                src.close();
                dst.close();
            }
        });
    }

    @Test
    public void testVarcharRegionReallocMidFill() throws Exception {
        // Long, growing values force both the dataMem payload region and the
        // auxMem header region to realloc (moving their base addresses) mid-fill.
        // The driver-decoded reads must still resolve against the relocated blocks
        // once the fill ends.
        assertMemoryLeak(() -> {
            IntList types = varcharSchema();
            try (LiveViewInMemoryBuffer buf = new LiveViewInMemoryBuffer(types, 0, PAGE_SIZE)) {
                VarcharRecord rec = new VarcharRecord();
                final int rows = 8;
                for (int r = 0; r < rows; r++) {
                    // Each value is several pages long and grows per row, so the
                    // split-path payload region reallocs multiple times.
                    String s = repeat((char) ('a' + r), (int) PAGE_SIZE * (r + 1));
                    rec.of((r + 1) * 1_000_000L, new Utf8String(s));
                    buf.copyRowFromRecord(rec, r);
                }
                buf.setRowCount(rows);

                for (int r = 0; r < rows; r++) {
                    String s = repeat((char) ('a' + r), (int) PAGE_SIZE * (r + 1));
                    TestUtils.assertEquals(s, buf.getVarcharA(r, 1));
                    Assert.assertEquals(s.length(), buf.getVarcharSize(r, 1));
                }
            }
        });
    }

    @Test
    public void testVarcharRoundTripThroughTier() throws Exception {
        // Write VARCHAR rows (NULL, an empty value, a short fully-inlined value, and
        // a value that exceeds VARCHAR_MAX_BYTES_FULLY_INLINED so it spills into the
        // dataMem payload via the split path) into a slot, publish, then read them
        // back through the buffer getters after a reader pins the published slot.
        assertMemoryLeak(() -> {
            // A value long enough to force the split (non-inlined) path.
            String longValue = repeat('z', VarcharTypeDriver.VARCHAR_MAX_BYTES_FULLY_INLINED + 10);
            IntList types = varcharSchema();
            try (LiveViewInMemoryTier tier = new LiveViewInMemoryTier(types, 0, PAGE_SIZE)) {
                int writeIdx = 1 - tier.getPublishedIdx();
                LiveViewInMemoryBuffer slot = tier.tryAcquireWrite(writeIdx);
                Assert.assertNotNull(slot);
                VarcharRecord rec = new VarcharRecord();
                rec.of(1_000_000L, new Utf8String("hi")); // short, fully inlined
                slot.copyRowFromRecord(rec, 0);
                rec.of(2_000_000L, null); // NULL
                slot.copyRowFromRecord(rec, 1);
                rec.of(3_000_000L, new Utf8String("")); // empty - distinct from null
                slot.copyRowFromRecord(rec, 2);
                rec.of(4_000_000L, new Utf8String(longValue)); // split path
                slot.copyRowFromRecord(rec, 3);
                slot.setRowCount(4);
                tier.publishSwap(writeIdx);

                int pin = tier.acquireRead();
                Assert.assertEquals(writeIdx, pin);
                LiveViewInMemoryBuffer r = tier.getSlot(pin);
                Assert.assertEquals(4, r.rowCount());

                // row 0: short inlined value; A and B views are distinct instances.
                TestUtils.assertEquals("hi", r.getVarcharA(0, 1));
                TestUtils.assertEquals("hi", r.getVarcharB(0, 1));
                Assert.assertEquals(2, r.getVarcharSize(0, 1));

                // row 1: NULL.
                Assert.assertNull(r.getVarcharA(1, 1));
                Assert.assertNull(r.getVarcharB(1, 1));
                Assert.assertEquals(TableUtils.NULL_LEN, r.getVarcharSize(1, 1));

                // row 2: empty value - distinct from null.
                Assert.assertNotNull(r.getVarcharA(2, 1));
                TestUtils.assertEquals("", r.getVarcharA(2, 1));
                Assert.assertEquals(0, r.getVarcharSize(2, 1));

                // row 3: long value forced through the split (data-region) path.
                TestUtils.assertEquals(longValue, r.getVarcharA(3, 1));
                Assert.assertEquals(longValue.length(), r.getVarcharSize(3, 1));

                tier.releaseRead(pin);
            }
        });
    }

    @Test
    public void testVarcharSlowPathSwapPreservesValues() throws Exception {
        // The slow-path swap rebuilds a slot via copyRowFrom (buffer -> buffer),
        // re-appending each retained VARCHAR through the driver. Skipping rows
        // (eviction) keeps the destination append dense, so the order assert holds
        // and the copied values round-trip across the inlined / split / null cases.
        assertMemoryLeak(() -> {
            IntList types = varcharSchema();
            LiveViewInMemoryBuffer src = new LiveViewInMemoryBuffer(types, 0, PAGE_SIZE);
            LiveViewInMemoryBuffer dst = new LiveViewInMemoryBuffer(types, 0, PAGE_SIZE);
            try {
                VarcharRecord rec = new VarcharRecord();
                final int rows = 6;
                for (int r = 0; r < rows; r++) {
                    String s = varcharValue(r);
                    rec.of((r + 1) * 1_000_000L, s == null ? null : new Utf8String(s));
                    src.copyRowFromRecord(rec, r);
                }
                src.setRowCount(rows);

                // Retain only the odd src rows, densely, into dst (mimics eviction).
                long dstRow = 0;
                for (int r = 1; r < rows; r += 2) {
                    dst.copyRowFrom(src, r, dstRow);
                    dstRow++;
                }
                dst.setRowCount(dstRow);
                Assert.assertEquals(3, dst.rowCount());

                dstRow = 0;
                for (int r = 1; r < rows; r += 2) {
                    String s = varcharValue(r);
                    if (s == null) {
                        Assert.assertNull(dst.getVarcharA(dstRow, 1));
                    } else {
                        TestUtils.assertEquals(s, dst.getVarcharA(dstRow, 1));
                    }
                    dstRow++;
                }
            } finally {
                src.close();
                dst.close();
            }
        });
    }

    @Test
    public void testWideFixedWidthRoundTripThroughTier() throws Exception {
        // Write the wide fixed-width types Phase 4 added - LONG256 (32 B), UUID and
        // LONG128 (16 B each) - including a NULL row, into a slot, publish, then read
        // them back through the buffer getters after a reader pins the published slot.
        // Their aux region stays the NullMemory stub; the value lives wholly in dataMem
        // at an absolute offset (row << 5 for LONG256, row << 4 for UUID / LONG128).
        assertMemoryLeak(() -> {
            IntList types = wideFixedWidthSchema();
            try (LiveViewInMemoryTier tier = new LiveViewInMemoryTier(types, 0, PAGE_SIZE)) {
                int writeIdx = 1 - tier.getPublishedIdx();
                LiveViewInMemoryBuffer slot = tier.tryAcquireWrite(writeIdx);
                Assert.assertNotNull(slot);
                WideFixedRecord rec = new WideFixedRecord();

                // row 0: normal values.
                rec.of(1_000_000L, long256Of(1, 2, 3, 4), 0x1111L, 0x2222L, 0x3333L, 0x4444L);
                slot.copyRowFromRecord(rec, 0);
                // row 1: NULL for every wide column.
                rec.of(2_000_000L, Long256Impl.NULL_LONG256, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
                slot.copyRowFromRecord(rec, 1);
                // row 2: another distinct set, including extreme long values.
                rec.of(3_000_000L, long256Of(-1, Long.MAX_VALUE, Long.MIN_VALUE, 100), 0x5555L, 0x6666L, 0x7777L, 0x8888L);
                slot.copyRowFromRecord(rec, 2);
                slot.setRowCount(3);
                tier.publishSwap(writeIdx);

                int pin = tier.acquireRead();
                Assert.assertEquals(writeIdx, pin);
                LiveViewInMemoryBuffer r = tier.getSlot(pin);
                Assert.assertEquals(3, r.rowCount());

                // row 0: LONG256 A/B are distinct flyweights with equal value (holding
                // both at once does not clobber either); UUID + LONG128 round-trip lo/hi.
                Long256 a = r.getLong256A(0, 1);
                Long256 b = r.getLong256B(0, 1);
                Assert.assertNotSame(a, b);
                assertLong256Equals(1, 2, 3, 4, a);
                assertLong256Equals(1, 2, 3, 4, b);
                Assert.assertEquals(0x1111L, r.getLong128Lo(0, 2));
                Assert.assertEquals(0x2222L, r.getLong128Hi(0, 2));
                Assert.assertEquals(0x3333L, r.getLong128Lo(0, 3));
                Assert.assertEquals(0x4444L, r.getLong128Hi(0, 3));

                // row 1: NULL across the board.
                Assert.assertTrue(Long256Impl.isNull(r.getLong256A(1, 1)));
                Assert.assertEquals(Numbers.LONG_NULL, r.getLong128Lo(1, 2));
                Assert.assertEquals(Numbers.LONG_NULL, r.getLong128Hi(1, 2));
                Assert.assertEquals(Numbers.LONG_NULL, r.getLong128Lo(1, 3));
                Assert.assertEquals(Numbers.LONG_NULL, r.getLong128Hi(1, 3));

                // row 2: distinct values, no cross-row bleed.
                assertLong256Equals(-1, Long.MAX_VALUE, Long.MIN_VALUE, 100, r.getLong256A(2, 1));
                Assert.assertEquals(0x5555L, r.getLong128Lo(2, 2));
                Assert.assertEquals(0x6666L, r.getLong128Hi(2, 2));
                Assert.assertEquals(0x7777L, r.getLong128Lo(2, 3));
                Assert.assertEquals(0x8888L, r.getLong128Hi(2, 3));

                tier.releaseRead(pin);
            }
        });
    }

    @Test
    public void testWideFixedWidthSlowPathSwapPreservesValues() throws Exception {
        // The slow-path swap rebuilds a slot via copyRowFrom (buffer -> buffer),
        // re-writing each retained wide fixed-width value at its absolute offset.
        // Skipping rows (eviction) keeps the destination dense, so the LONG256 / UUID
        // / LONG128 values round-trip across the normal / null cases.
        assertMemoryLeak(() -> {
            IntList types = wideFixedWidthSchema();
            LiveViewInMemoryBuffer src = new LiveViewInMemoryBuffer(types, 0, PAGE_SIZE);
            LiveViewInMemoryBuffer dst = new LiveViewInMemoryBuffer(types, 0, PAGE_SIZE);
            try {
                WideFixedRecord rec = new WideFixedRecord();
                final int rows = 6;
                for (int r = 0; r < rows; r++) {
                    if (r % 3 == 0) {
                        rec.of((r + 1) * 1_000_000L, Long256Impl.NULL_LONG256, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
                    } else {
                        rec.of((r + 1) * 1_000_000L, long256Of(r, r + 1, r + 2, r + 3), 0x10L + r, 0x20L + r, 0x30L + r, 0x40L + r);
                    }
                    src.copyRowFromRecord(rec, r);
                }
                src.setRowCount(rows);

                // Retain only the odd src rows, densely, into dst (mimics eviction).
                long dstRow = 0;
                for (int r = 1; r < rows; r += 2) {
                    dst.copyRowFrom(src, r, dstRow);
                    dstRow++;
                }
                dst.setRowCount(dstRow);
                Assert.assertEquals(3, dst.rowCount());

                dstRow = 0;
                for (int r = 1; r < rows; r += 2) {
                    if (r % 3 == 0) {
                        Assert.assertTrue(Long256Impl.isNull(dst.getLong256A(dstRow, 1)));
                        Assert.assertEquals(Numbers.LONG_NULL, dst.getLong128Lo(dstRow, 2));
                        Assert.assertEquals(Numbers.LONG_NULL, dst.getLong128Hi(dstRow, 3));
                    } else {
                        assertLong256Equals(r, r + 1, r + 2, r + 3, dst.getLong256A(dstRow, 1));
                        Assert.assertEquals(0x10L + r, dst.getLong128Lo(dstRow, 2));
                        Assert.assertEquals(0x20L + r, dst.getLong128Hi(dstRow, 2));
                        Assert.assertEquals(0x30L + r, dst.getLong128Lo(dstRow, 3));
                        Assert.assertEquals(0x40L + r, dst.getLong128Hi(dstRow, 3));
                    }
                    dstRow++;
                }
            } finally {
                src.close();
                dst.close();
            }
        });
    }

    @Test
    public void testDecimalRoundTripThroughTier() throws Exception {
        // Write every DECIMAL width - DECIMAL8/16/32/64 in place at their natural byte
        // stride, the wide DECIMAL128 / DECIMAL256 at an absolute offset (row << 4 /
        // row << 5) - including a NULL row carrying each width's null sentinel, then
        // read them back through the buffer getters after a reader pins the published
        // slot. The aux region stays the NullMemory stub; the value lives wholly in
        // dataMem.
        assertMemoryLeak(() -> {
            IntList types = decimalSchema();
            try (LiveViewInMemoryTier tier = new LiveViewInMemoryTier(types, 0, PAGE_SIZE)) {
                int writeIdx = 1 - tier.getPublishedIdx();
                LiveViewInMemoryBuffer slot = tier.tryAcquireWrite(writeIdx);
                Assert.assertNotNull(slot);
                DecimalRecord rec = new DecimalRecord();

                // row 0: normal distinct values.
                rec.of(1_000_000L, (byte) 0x12, (short) 0x1234, 0x1234_5678, 0x1234_5678_9ABC_DEF0L,
                        0x1111L, 0x2222L, 0x3333L, 0x4444L, 0x5555L, 0x6666L);
                slot.copyRowFromRecord(rec, 0);
                // row 1: NULL for every decimal width.
                rec.of(2_000_000L, Decimals.DECIMAL8_NULL, Decimals.DECIMAL16_NULL, Decimals.DECIMAL32_NULL,
                        Decimals.DECIMAL64_NULL, Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL,
                        Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL, Decimals.DECIMAL256_LH_NULL,
                        Decimals.DECIMAL256_LL_NULL);
                slot.copyRowFromRecord(rec, 1);
                // row 2: distinct extremes, no cross-row bleed.
                rec.of(3_000_000L, (byte) -7, (short) -12_345, Integer.MAX_VALUE, 987_654_321_012L,
                        Long.MAX_VALUE, -1L, -1L, Long.MAX_VALUE, 7L, 100L);
                slot.copyRowFromRecord(rec, 2);
                slot.setRowCount(3);
                tier.publishSwap(writeIdx);

                int pin = tier.acquireRead();
                Assert.assertEquals(writeIdx, pin);
                LiveViewInMemoryBuffer r = tier.getSlot(pin);
                Assert.assertEquals(3, r.rowCount());

                Decimal128 d128 = new Decimal128();
                Decimal256 d256 = new Decimal256();

                // row 0.
                Assert.assertEquals((byte) 0x12, r.getDecimal8(0, 1));
                Assert.assertEquals((short) 0x1234, r.getDecimal16(0, 2));
                Assert.assertEquals(0x1234_5678, r.getDecimal32(0, 3));
                Assert.assertEquals(0x1234_5678_9ABC_DEF0L, r.getDecimal64(0, 4));
                r.getDecimal128(0, 5, d128);
                Assert.assertEquals(0x1111L, d128.getHigh());
                Assert.assertEquals(0x2222L, d128.getLow());
                r.getDecimal256(0, 6, d256);
                assertDecimal256Equals(0x3333L, 0x4444L, 0x5555L, 0x6666L, d256);

                // row 1: NULL across the board (per-width sentinels + isNull sinks).
                Assert.assertEquals(Decimals.DECIMAL8_NULL, r.getDecimal8(1, 1));
                Assert.assertEquals(Decimals.DECIMAL16_NULL, r.getDecimal16(1, 2));
                Assert.assertEquals(Decimals.DECIMAL32_NULL, r.getDecimal32(1, 3));
                Assert.assertEquals(Decimals.DECIMAL64_NULL, r.getDecimal64(1, 4));
                r.getDecimal128(1, 5, d128);
                Assert.assertTrue(d128.isNull());
                r.getDecimal256(1, 6, d256);
                Assert.assertTrue(d256.isNull());

                // row 2: distinct extremes, no cross-row bleed.
                Assert.assertEquals((byte) -7, r.getDecimal8(2, 1));
                Assert.assertEquals((short) -12_345, r.getDecimal16(2, 2));
                Assert.assertEquals(Integer.MAX_VALUE, r.getDecimal32(2, 3));
                Assert.assertEquals(987_654_321_012L, r.getDecimal64(2, 4));
                r.getDecimal128(2, 5, d128);
                Assert.assertEquals(Long.MAX_VALUE, d128.getHigh());
                Assert.assertEquals(-1L, d128.getLow());
                r.getDecimal256(2, 6, d256);
                assertDecimal256Equals(-1L, Long.MAX_VALUE, 7L, 100L, d256);

                tier.releaseRead(pin);
            }
        });
    }

    @Test
    public void testDecimalSlowPathSwapPreservesValues() throws Exception {
        // The slow-path swap rebuilds a slot via copyRowFrom (buffer -> buffer),
        // re-writing each retained decimal value at its absolute offset. Skipping rows
        // (eviction) keeps the destination dense, so every DECIMAL width round-trips
        // across the normal / null cases.
        assertMemoryLeak(() -> {
            IntList types = decimalSchema();
            LiveViewInMemoryBuffer src = new LiveViewInMemoryBuffer(types, 0, PAGE_SIZE);
            LiveViewInMemoryBuffer dst = new LiveViewInMemoryBuffer(types, 0, PAGE_SIZE);
            try {
                DecimalRecord rec = new DecimalRecord();
                final int rows = 6;
                for (int r = 0; r < rows; r++) {
                    if (r % 3 == 0) {
                        rec.of((r + 1) * 1_000_000L, Decimals.DECIMAL8_NULL, Decimals.DECIMAL16_NULL,
                                Decimals.DECIMAL32_NULL, Decimals.DECIMAL64_NULL, Decimals.DECIMAL128_HI_NULL,
                                Decimals.DECIMAL128_LO_NULL, Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                                Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL);
                    } else {
                        rec.of((r + 1) * 1_000_000L, (byte) (0x10 + r), (short) (0x2000 + r), 0x3000_0000 + r,
                                0x4000_0000_0000_0000L + r, 0x50L + r, 0x60L + r, 0x70L + r, 0x80L + r, 0x90L + r, 0xA0L + r);
                    }
                    src.copyRowFromRecord(rec, r);
                }
                src.setRowCount(rows);

                // Retain only the odd src rows, densely, into dst (mimics eviction).
                long dstRow = 0;
                for (int r = 1; r < rows; r += 2) {
                    dst.copyRowFrom(src, r, dstRow);
                    dstRow++;
                }
                dst.setRowCount(dstRow);
                Assert.assertEquals(3, dst.rowCount());

                Decimal128 d128 = new Decimal128();
                Decimal256 d256 = new Decimal256();
                dstRow = 0;
                for (int r = 1; r < rows; r += 2) {
                    if (r % 3 == 0) {
                        Assert.assertEquals(Decimals.DECIMAL8_NULL, dst.getDecimal8(dstRow, 1));
                        Assert.assertEquals(Decimals.DECIMAL64_NULL, dst.getDecimal64(dstRow, 4));
                        dst.getDecimal128(dstRow, 5, d128);
                        Assert.assertTrue(d128.isNull());
                        dst.getDecimal256(dstRow, 6, d256);
                        Assert.assertTrue(d256.isNull());
                    } else {
                        Assert.assertEquals((byte) (0x10 + r), dst.getDecimal8(dstRow, 1));
                        Assert.assertEquals((short) (0x2000 + r), dst.getDecimal16(dstRow, 2));
                        Assert.assertEquals(0x3000_0000 + r, dst.getDecimal32(dstRow, 3));
                        Assert.assertEquals(0x4000_0000_0000_0000L + r, dst.getDecimal64(dstRow, 4));
                        dst.getDecimal128(dstRow, 5, d128);
                        Assert.assertEquals(0x50L + r, d128.getHigh());
                        Assert.assertEquals(0x60L + r, d128.getLow());
                        dst.getDecimal256(dstRow, 6, d256);
                        assertDecimal256Equals(0x70L + r, 0x80L + r, 0x90L + r, 0xA0L + r, d256);
                    }
                    dstRow++;
                }
            } finally {
                src.close();
                dst.close();
            }
        });
    }

    @Test
    public void testGeoHashRoundTripThroughTier() throws Exception {
        // testColumnTypeSupportGate admits all four GEOHASH widths into the tier, so the
        // record -> buffer copier has to be able to read them. A GEOHASH column function
        // (GeoByteColumn and siblings) overrides only getGeoByte/Short/Int/Long and
        // inherits the plain-width getters from AbstractGeoHashFunction, which throw. The
        // copier used to reach for record.getLong/getInt/getShort/getByte for the GEO
        // cases, so every refresh cycle of a view projecting a GEOHASH column threw. The
        // throw was invisible end-to-end: handleRefreshFailure swallowed it and recomputed
        // the window from the applied base, which is exactly what a recompute oracle
        // compares against, so the view's answer stayed right while the incremental path
        // never ran. GeoRecord implements only the getGeo* getters, so a copier that
        // regresses to a plain getter fails here the same way it does in production.
        assertMemoryLeak(() -> {
            IntList types = geoSchema();
            try (LiveViewInMemoryTier tier = new LiveViewInMemoryTier(types, 0, PAGE_SIZE)) {
                int writeIdx = 1 - tier.getPublishedIdx();
                LiveViewInMemoryBuffer slot = tier.tryAcquireWrite(writeIdx);
                Assert.assertNotNull(slot);
                GeoRecord rec = new GeoRecord();

                // row 0: normal values, one per width.
                rec.of(1_000_000L, (byte) 0x0A, (short) 0x0BBB, 0x00CC_CCCC, 0x0000_00DD_DDDD_DDDDL);
                slot.copyRowFromRecord(rec, 0);
                // row 1: the per-width GEOHASH NULL sentinels, which are just byte patterns
                // the tier has to carry through verbatim.
                rec.of(2_000_000L, GeoHashes.BYTE_NULL, GeoHashes.SHORT_NULL, GeoHashes.INT_NULL, GeoHashes.NULL);
                slot.copyRowFromRecord(rec, 1);
                // row 2: another distinct set.
                rec.of(3_000_000L, (byte) 0x01, (short) 0x0222, 0x0033_3333, 0x0000_0044_4444_4444L);
                slot.copyRowFromRecord(rec, 2);
                slot.setRowCount(3);
                tier.publishSwap(writeIdx);

                int pin = tier.acquireRead();
                Assert.assertEquals(writeIdx, pin);
                LiveViewInMemoryBuffer r = tier.getSlot(pin);
                Assert.assertEquals(3, r.rowCount());

                // The buffer stores each GEOHASH at its plain width, so it reads back
                // through the plain-width buffer getters (LiveViewBufferRecord.getGeoByte
                // delegates to buffer.getByte, and so on).
                Assert.assertEquals(1_000_000L, r.getLong(0, 0));
                Assert.assertEquals((byte) 0x0A, r.getByte(0, 1));
                Assert.assertEquals((short) 0x0BBB, r.getShort(0, 2));
                Assert.assertEquals(0x00CC_CCCC, r.getInt(0, 3));
                Assert.assertEquals(0x0000_00DD_DDDD_DDDDL, r.getLong(0, 4));

                Assert.assertEquals(GeoHashes.BYTE_NULL, r.getByte(1, 1));
                Assert.assertEquals(GeoHashes.SHORT_NULL, r.getShort(1, 2));
                Assert.assertEquals(GeoHashes.INT_NULL, r.getInt(1, 3));
                Assert.assertEquals(GeoHashes.NULL, r.getLong(1, 4));

                Assert.assertEquals((byte) 0x01, r.getByte(2, 1));
                Assert.assertEquals((short) 0x0222, r.getShort(2, 2));
                Assert.assertEquals(0x0033_3333, r.getInt(2, 3));
                Assert.assertEquals(0x0000_0044_4444_4444L, r.getLong(2, 4));

                tier.releaseRead(pin);
            }
        });
    }

    @Test
    public void testFixedWidthRegionExposesAddressesGettersAgreeWith() throws Exception {
        // dataAddress/dataSize expose a column's region as the (address, used-extent)
        // pair a page frame column address wants. A fixed-width column's payload lives
        // wholly in the data region at row << shift, so a raw read at that stride must
        // return exactly what the buffer getter returns - the frame path would resolve
        // through the address while every other reader stays on the getter.
        assertMemoryLeak(() -> {
            IntList types = wideFixedWidthSchema(); // TIMESTAMP, LONG256, UUID, LONG128
            try (LiveViewInMemoryBuffer buf = new LiveViewInMemoryBuffer(types, 0, PAGE_SIZE)) {
                // Enough rows to push every region well past the initial page, so the
                // addresses read below are post-realloc ones.
                final int rows = 1_024;
                for (int r = 0; r < rows; r++) {
                    buf.putLong(r, 0, r * 1_000_000L);
                    buf.putLong256(r, 1, long256Of(r, r + 1L, r + 2L, r + 3L));
                    buf.putLong128(r, 2, r, ~r);
                    buf.putLong128(r, 3, r * 7L, r * 11L);
                }
                buf.setRowCount(rows);

                // The extent is rowCount * stride, not the append cursor: a fixed-width
                // write lands at an absolute offset and never advances the cursor, so a
                // dataSize wired to getAppendOffset() would report 0 for every column here.
                Assert.assertEquals((long) rows * Long.BYTES, buf.dataSize(0));
                Assert.assertEquals((long) rows * 32, buf.dataSize(1));
                Assert.assertEquals((long) rows * 16, buf.dataSize(2));
                Assert.assertEquals((long) rows * 16, buf.dataSize(3));

                // A fixed-width column has no aux vector: its auxMem slot is the shared
                // NullMemory stub, whose addressOf/getAppendOffset throw. Both accessors
                // must report the 0 sentinel instead of delegating to it.
                for (int c = 0, n = types.size(); c < n; c++) {
                    Assert.assertEquals("aux address, col " + c, 0, buf.auxAddress(c));
                    Assert.assertEquals("aux size, col " + c, 0, buf.auxSize(c));
                    Assert.assertNotEquals("data address, col " + c, 0, buf.dataAddress(c));
                }

                final long tsAddr = buf.dataAddress(0);
                final long uuidAddr = buf.dataAddress(2);
                for (int r = 0; r < rows; r++) {
                    Assert.assertEquals(buf.getLong(r, 0), Unsafe.getUnsafe().getLong(tsAddr + ((long) r << 3)));
                    // UUID / LONG128 store lo first and hi at + 8 - the same 16-byte
                    // layout PageFrameMemoryRecord.getLong128Lo/Hi read.
                    Assert.assertEquals(buf.getLong128Lo(r, 2), Unsafe.getUnsafe().getLong(uuidAddr + ((long) r << 4)));
                    Assert.assertEquals(buf.getLong128Hi(r, 2), Unsafe.getUnsafe().getLong(uuidAddr + ((long) r << 4) + Long.BYTES));
                }
            }
        });
    }

    @Test
    public void testRegionExtentsCollapseWhenEmptyAndAfterReset() throws Exception {
        // An unfilled region reports a 0 data extent (and a 0 data address, since nothing
        // is allocated yet), and reset() must take a filled buffer back to both. reset()
        // retains the allocated pages and rewinds only the var-size append cursors, so a
        // dataSize that reported the allocation rather than the written extent - or one
        // that ignored rowCount for a fixed-width column - would keep reporting the old
        // fill and hand a Phase 3 frame rows the slot no longer holds.
        //
        // The aux side of "empty" is type dependent: a STRING / BINARY column carries the
        // native layout's leading terminator, seeded at construction and re-seeded by
        // reset(), so its empty extent is one entry rather than zero and its region is
        // allocated from the start.
        assertMemoryLeak(() -> {
            IntList types = strBinSchema(); // TIMESTAMP, STRING, BINARY
            try (LiveViewInMemoryBuffer buf = new LiveViewInMemoryBuffer(types, 0, PAGE_SIZE)) {
                assertEmptyExtents(buf, types);
                // Nothing is allocated up front except the STRING / BINARY aux regions the
                // seed writes into (which assertEmptyExtents covers).
                for (int c = 0, n = types.size(); c < n; c++) {
                    Assert.assertEquals("empty data address, col " + c, 0, buf.dataAddress(c));
                }

                VarSizeRecord rec = new VarSizeRecord();
                final int rows = 4;
                for (int r = 0; r < rows; r++) {
                    rec.of((r + 1) * 1_000_000L, "v" + r, new TestBinarySequence().of(bytesOf(r, 8)));
                    buf.copyRowFromRecord(rec, r);
                }
                buf.setRowCount(rows);
                Assert.assertTrue(buf.dataSize(1) > 0);
                Assert.assertEquals((long) (rows + 1) * Long.BYTES, buf.auxSize(1));

                // A reset buffer reports exactly the extents a fresh one does - including a
                // STRING / BINARY column back at its bare terminator rather than at an empty
                // vector, since reset() rewinds the aux cursor to 0 and must re-seed.
                buf.reset();
                assertEmptyExtents(buf, types);
                // The pages survive reset() (the next refill reuses them), so the regions
                // stay allocated even though the extents collapsed.
                Assert.assertNotEquals(0, buf.dataAddress(1));
                Assert.assertNotEquals(0, buf.auxAddress(1));
            }
        });
    }

    @Test
    public void testStringBinaryAuxRegionMatchesNativeLayout() throws Exception {
        // The buffer's STRING / BINARY aux vector is the drivers' own N+1 model: a leading
        // 0, then one 8-byte END offset per row. So it matches getAuxVectorSize entry for
        // entry - and BinaryTypeDriver's with it, since it extends StringTypeDriver - which
        // is what makes every column here driver-readable and lets a Phase 3 frame cursor
        // size a data page through getDataVectorSizeAt like it would anywhere else.
        //
        // The vector's shape is the contract; auxSize's *extent* is deliberately one entry
        // wider than the N * 8 a native frame publishes (see testStringBinaryAuxSizeReportsNativeVector).
        assertMemoryLeak(() -> {
            IntList types = strBinSchema(); // TIMESTAMP, STRING, BINARY
            try (LiveViewInMemoryBuffer buf = new LiveViewInMemoryBuffer(types, 0, PAGE_SIZE)) {
                VarSizeRecord rec = new VarSizeRecord();
                final int rows = 6;
                for (int r = 0; r < rows; r++) {
                    rec.of((r + 1) * 1_000_000L, "s" + r, new TestBinarySequence().of(bytesOf(r, 4)));
                    buf.copyRowFromRecord(rec, r);
                }
                buf.setRowCount(rows);

                // The vector the drivers expect, exactly: N+1 entries for N rows.
                Assert.assertEquals(StringTypeDriver.INSTANCE.getAuxVectorSize(rows), buf.auxSize(1));
                Assert.assertEquals(BinaryTypeDriver.INSTANCE.getAuxVectorSize(rows), buf.auxSize(2));

                for (int col = 1; col <= 2; col++) {
                    final long auxAddr = buf.auxAddress(col);
                    final long dataSize = buf.dataSize(col);

                    // Entry 0 is the seeded terminator: row 0's payload starts at the base.
                    Assert.assertEquals("leading terminator, col " + col, 0, Unsafe.getUnsafe().getLong(auxAddr));
                    // Every entry ascends and stays inside the data extent, and the last one
                    // bounds the final row's payload exactly - the entry the pre-N+1 layout
                    // did not have at all.
                    long prev = -1;
                    for (int r = 0; r <= rows; r++) {
                        final long off = Unsafe.getUnsafe().getLong(auxAddr + ((long) r << 3));
                        Assert.assertTrue("offset ascends, col " + col + " entry " + r, off > prev);
                        Assert.assertTrue("offset within data extent, col " + col + " entry " + r, off <= dataSize);
                        prev = off;
                    }
                    Assert.assertEquals("terminator bounds the payload, col " + col, dataSize, prev);

                    // The driver agrees: sizing the data page off this aux now yields the
                    // same extent dataSize reports, which is the whole point of aligning.
                    final ColumnTypeDriver driver = ColumnType.getDriver(types.getQuick(col));
                    Assert.assertEquals(
                            "driver-sized data page, col " + col,
                            dataSize,
                            driver.getDataVectorSizeAt(auxAddr, rows - 1)
                    );
                }
            }
        });
    }

    @Test
    public void testStringBinaryAuxSizeReportsNativeVector() throws Exception {
        // auxSize means "used extent", so for STRING / BINARY it reports the whole N+1
        // vector - one entry MORE than the N * 8 a native page frame publishes for the same
        // rows (FwdTableReaderPageFrameCursor sets a frame's aux extent to (hi - lo) * 8).
        //
        // Pin that gap, because a frame is meant to publish this value as-is rather than
        // re-derive the native extent. It is inert: the extent is consumed only as a bounds
        // guard (every STRING / BINARY read in PageFrameMemoryRecord is
        // `if (auxPageLim < auxOffset + 8) throw`), nothing derives a row count from it, and
        // the frame's row range caps rowIndex at rowCount - 1 regardless. So the extra entry
        // only loosens the guard by one entry it can never reach.
        assertMemoryLeak(() -> {
            IntList types = strBinSchema(); // TIMESTAMP, STRING, BINARY
            try (LiveViewInMemoryBuffer buf = new LiveViewInMemoryBuffer(types, 0, PAGE_SIZE)) {
                VarSizeRecord rec = new VarSizeRecord();
                final int rows = 6;
                for (int r = 0; r < rows; r++) {
                    rec.of((r + 1) * 1_000_000L, "s" + r, new TestBinarySequence().of(bytesOf(r, 4)));
                    buf.copyRowFromRecord(rec, r);
                }
                buf.setRowCount(rows);

                final long framePublishes = (long) rows * Long.BYTES;
                Assert.assertEquals(framePublishes + Long.BYTES, buf.auxSize(1));
                Assert.assertEquals(framePublishes + Long.BYTES, buf.auxSize(2));
                // VARCHAR / ARRAY carry no terminator, so their extent is the native one
                // outright - the gap above is STRING / BINARY only.
                Assert.assertEquals(0, buf.auxSize(0)); // TIMESTAMP: fixed-width, no aux at all
            }
        });
    }

    @Test
    public void testVarcharRegionAddressesFeedDriverReads() throws Exception {
        // VARCHAR (like ARRAY) carries a self-describing 16-byte header per row and needs
        // no trailing terminator, so its regions match the native layout exactly. Prove it
        // by decoding every row straight from the raw (aux, data) addresses through the
        // same VarcharTypeDriver entry point a page frame uses, bounding both regions with
        // the reported extents rather than the allocated limits, and cross-checking against
        // the buffer getter.
        assertMemoryLeak(() -> {
            IntList types = varcharSchema(); // TIMESTAMP, VARCHAR
            try (LiveViewInMemoryBuffer buf = new LiveViewInMemoryBuffer(types, 0, PAGE_SIZE)) {
                // varcharValue mixes null, fully-inlined and split (payload-bearing) rows,
                // so the decode below covers every encoding branch.
                VarcharRecord rec = new VarcharRecord();
                final int rows = 32;
                for (int r = 0; r < rows; r++) {
                    String v = varcharValue(r);
                    rec.of((r + 1) * 1_000_000L, v == null ? null : new Utf8String(v));
                    buf.copyRowFromRecord(rec, r);
                }
                buf.setRowCount(rows);

                Assert.assertEquals((long) rows * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES, buf.auxSize(1));
                Assert.assertEquals(
                        buf.auxSize(1),
                        VarcharTypeDriver.INSTANCE.getAuxVectorSize(rows)
                );

                final long auxAddr = buf.auxAddress(1);
                final long dataAddr = buf.dataAddress(1);
                final long auxLim = auxAddr + buf.auxSize(1);
                final long dataLim = dataAddr + buf.dataSize(1);
                Utf8SplitString view = new Utf8SplitString();
                for (int r = 0; r < rows; r++) {
                    Utf8Sequence actual = VarcharTypeDriver.getSplitValue(auxAddr, auxLim, dataAddr, dataLim, r, view);
                    String expected = varcharValue(r);
                    if (expected == null) {
                        Assert.assertNull("row " + r, actual);
                    } else {
                        Assert.assertNotNull("row " + r, actual);
                        TestUtils.assertEquals(expected, actual);
                    }
                }
            }
        });
    }

    private static IntList array1d2dSchema() {
        IntList types = new IntList(3);
        types.add(ColumnType.TIMESTAMP);
        types.add(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
        types.add(ColumnType.encodeArrayType(ColumnType.DOUBLE, 2));
        return types;
    }

    private static IntList array1dSchema() {
        IntList types = new IntList(2);
        types.add(ColumnType.TIMESTAMP);
        types.add(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
        return types;
    }

    private static void assertArray1dEquals(double[] expected, ArrayView actual) {
        Assert.assertFalse(actual.isNull());
        Assert.assertEquals(1, actual.getDimCount());
        Assert.assertEquals(expected.length, actual.getDimLen(0));
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals("element " + i, expected[i], actual.getDouble(i), 0.0);
        }
    }

    private static void assertBinEquals(byte[] expected, BinarySequence actual) {
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.length, actual.length());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals("byte " + i, expected[i], actual.byteAt(i));
        }
    }

    private static void assertDecimal256Equals(long hh, long hl, long lh, long ll, Decimal256 actual) {
        Assert.assertEquals("hh", hh, actual.getHh());
        Assert.assertEquals("hl", hl, actual.getHl());
        Assert.assertEquals("lh", lh, actual.getLh());
        Assert.assertEquals("ll", ll, actual.getLl());
    }

    // Asserts the extents of a buffer that holds no rows - whether freshly constructed or
    // freshly reset(). The data extent is 0 for every column. The aux extent splits by
    // type: a fixed-width column has no aux vector at all, a VARCHAR / ARRAY column has an
    // empty one, and a STRING / BINARY column already carries its leading terminator, so
    // its extent is one entry and that entry reads 0.
    //
    // Addresses are deliberately out of scope: they distinguish the two states rather than
    // share them (a fresh buffer has allocated nothing but the STRING / BINARY aux seed; a
    // reset one keeps every page it allocated), so the test that cares asserts them itself.
    private static void assertEmptyExtents(LiveViewInMemoryBuffer buf, IntList types) {
        for (int c = 0, n = types.size(); c < n; c++) {
            Assert.assertEquals("empty data size, col " + c, 0, buf.dataSize(c));
            final int tag = ColumnType.tagOf(types.getQuick(c));
            if (tag == ColumnType.STRING || tag == ColumnType.BINARY) {
                Assert.assertEquals("seeded aux size, col " + c, Long.BYTES, buf.auxSize(c));
                Assert.assertNotEquals("seeded aux address, col " + c, 0, buf.auxAddress(c));
                Assert.assertEquals("seeded terminator, col " + c, 0, Unsafe.getUnsafe().getLong(buf.auxAddress(c)));
            } else {
                Assert.assertEquals("empty aux size, col " + c, 0, buf.auxSize(c));
                Assert.assertEquals("empty aux address, col " + c, 0, buf.auxAddress(c));
            }
        }
    }

    private static void assertLong256Equals(long l0, long l1, long l2, long l3, Long256 actual) {
        Assert.assertEquals("long0", l0, actual.getLong0());
        Assert.assertEquals("long1", l1, actual.getLong1());
        Assert.assertEquals("long2", l2, actual.getLong2());
        Assert.assertEquals("long3", l3, actual.getLong3());
    }

    private static byte[] bytesOf(int seed, int len) {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[i] = (byte) (seed + i);
        }
        return b;
    }

    private static double[] doubleSeq(int seed, int len) {
        double[] a = new double[len];
        for (int i = 0; i < len; i++) {
            a[i] = seed + i + 0.5;
        }
        return a;
    }

    private static Long256 long256Of(long l0, long l1, long l2, long l3) {
        Long256Impl v = new Long256Impl();
        v.setAll(l0, l1, l2, l3);
        return v;
    }

    private static String repeat(char c, int len) {
        char[] a = new char[len];
        java.util.Arrays.fill(a, c);
        return new String(a);
    }

    // Populates a 1-D DOUBLE[] in row-major order (empty when vals is empty).
    private static void set1d(DirectArray a, double... vals) {
        a.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
        a.setDimLen(0, vals.length);
        a.applyShape();
        MemoryA m = a.startMemoryA();
        for (double v : vals) {
            m.putDouble(v);
        }
    }

    // Populates a 2-D DOUBLE[][] of shape (d0, d1) in row-major order.
    private static void set2d(DirectArray a, int d0, int d1, double... vals) {
        a.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 2));
        a.setDimLen(0, d0);
        a.setDimLen(1, d1);
        a.applyShape();
        MemoryA m = a.startMemoryA();
        for (double v : vals) {
            m.putDouble(v);
        }
    }

    private static IntList singleLongCol() {
        IntList types = new IntList(1);
        types.add(ColumnType.LONG);
        return types;
    }

    private static IntList strBinSchema() {
        IntList types = new IntList(3);
        types.add(ColumnType.TIMESTAMP);
        types.add(ColumnType.STRING);
        types.add(ColumnType.BINARY);
        return types;
    }

    private static IntList varcharSchema() {
        IntList types = new IntList(2);
        types.add(ColumnType.TIMESTAMP);
        types.add(ColumnType.VARCHAR);
        return types;
    }

    // TIMESTAMP + the three wide fixed-width types Phase 4 added: LONG256 (col 1),
    // UUID (col 2) and LONG128 (col 3). UUID and LONG128 share the same 16-byte
    // lo/hi storage and accessors but are distinct column types.
    // Schema covering all six DECIMAL widths (one column each), keyed by the
    // precision that maps to each storage tag: DECIMAL8 (<=2), DECIMAL16 (<=4),
    // DECIMAL32 (<=9), DECIMAL64 (<=18), DECIMAL128 (<=38), DECIMAL256 (<=76).
    private static IntList decimalSchema() {
        IntList types = new IntList(7);
        types.add(ColumnType.TIMESTAMP);
        types.add(ColumnType.getDecimalType(2, 0));   // DECIMAL8
        types.add(ColumnType.getDecimalType(4, 1));   // DECIMAL16
        types.add(ColumnType.getDecimalType(9, 3));   // DECIMAL32
        types.add(ColumnType.getDecimalType(18, 2));  // DECIMAL64
        types.add(ColumnType.getDecimalType(38, 6));  // DECIMAL128
        types.add(ColumnType.getDecimalType(60, 0));  // DECIMAL256
        return types;
    }

    // TIMESTAMP + one column of each GEOHASH storage width: GEOBYTE (<=7 bits),
    // GEOSHORT (<=15), GEOINT (<=31) and GEOLONG (<=60). The bit counts mirror the
    // rnd_geohash(4/12/24/40) projection the end-to-end tier test uses.
    private static IntList geoSchema() {
        IntList types = new IntList(5);
        types.add(ColumnType.TIMESTAMP);
        types.add(ColumnType.getGeoHashTypeWithBits(4));
        types.add(ColumnType.getGeoHashTypeWithBits(12));
        types.add(ColumnType.getGeoHashTypeWithBits(24));
        types.add(ColumnType.getGeoHashTypeWithBits(40));
        return types;
    }

    private static IntList wideFixedWidthSchema() {
        IntList types = new IntList(4);
        types.add(ColumnType.TIMESTAMP);
        types.add(ColumnType.LONG256);
        types.add(ColumnType.UUID);
        types.add(ColumnType.LONG128);
        return types;
    }

    // Per-row VARCHAR value mixing the null, fully-inlined and split (data-region)
    // cases so a slow-path swap exercises every encoding branch.
    private static String varcharValue(int r) {
        if (r % 3 == 0) {
            return null;
        }
        // Odd rows are short (fully inlined); a longer row spills to the data region.
        return (r % 2 == 0) ? "v" + r : "v" + r + repeat('x', VarcharTypeDriver.VARCHAR_MAX_BYTES_FULLY_INLINED + r);
    }

    // Minimal single-row Record stub feeding copyRowFromRecord a TIMESTAMP (index
    // 0) and up to two ARRAY columns (a 1-D at index 1, a 2-D at index 2). Rebind
    // per row via of(); a null arr2d serves the 1-D-only schemas.
    private static final class ArrayRecord implements Record {
        private ArrayView arr1d;
        private ArrayView arr2d;
        private long ts;

        @Override
        public ArrayView getArray(int col, int columnType) {
            return col == 1 ? arr1d : arr2d;
        }

        @Override
        public long getTimestamp(int col) {
            return ts;
        }

        void of(long ts, ArrayView arr1d, ArrayView arr2d) {
            this.ts = ts;
            this.arr1d = arr1d;
            this.arr2d = arr2d;
        }
    }

    // Minimal single-row Record stub feeding copyRowFromRecord a TIMESTAMP (index 0)
    // and one column of each DECIMAL width (indexes 1..6): DECIMAL8, DECIMAL16,
    // DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256. The wide 128/256 values are stored
    // as their raw limbs and re-materialised into the caller's sink on read. Rebind
    // per row via of().
    private static final class DecimalRecord implements Record {
        private long d128Hi;
        private long d128Lo;
        private short d16;
        private long d256Hh;
        private long d256Hl;
        private long d256Lh;
        private long d256Ll;
        private int d32;
        private long d64;
        private byte d8;
        private long ts;

        @Override
        public void getDecimal128(int col, Decimal128 sink) {
            sink.ofRaw(d128Hi, d128Lo);
        }

        @Override
        public short getDecimal16(int col) {
            return d16;
        }

        @Override
        public void getDecimal256(int col, Decimal256 sink) {
            sink.ofRaw(d256Hh, d256Hl, d256Lh, d256Ll);
        }

        @Override
        public int getDecimal32(int col) {
            return d32;
        }

        @Override
        public long getDecimal64(int col) {
            return d64;
        }

        @Override
        public byte getDecimal8(int col) {
            return d8;
        }

        @Override
        public long getTimestamp(int col) {
            return ts;
        }

        void of(
                long ts,
                byte d8,
                short d16,
                int d32,
                long d64,
                long d128Hi,
                long d128Lo,
                long d256Hh,
                long d256Hl,
                long d256Lh,
                long d256Ll
        ) {
            this.ts = ts;
            this.d8 = d8;
            this.d16 = d16;
            this.d32 = d32;
            this.d64 = d64;
            this.d128Hi = d128Hi;
            this.d128Lo = d128Lo;
            this.d256Hh = d256Hh;
            this.d256Hl = d256Hl;
            this.d256Lh = d256Lh;
            this.d256Ll = d256Ll;
        }
    }

    // Minimal single-row Record stub feeding copyRowFromRecord a TIMESTAMP (col 0) and
    // the four GEOHASH widths (GEOBYTE 1, GEOSHORT 2, GEOINT 3, GEOLONG 4). It models a
    // real GEOHASH column function: GeoByteColumn and its siblings override ONLY the
    // getGeo* getters, inheriting the plain-width ones from AbstractGeoHashFunction,
    // which throw. So this stub deliberately leaves getByte/getShort/getInt/getLong
    // unimplemented - Record's defaults throw UnsupportedOperationException - and a
    // copier that reaches for a plain getter fails here exactly as it does in production.
    private static final class GeoRecord implements Record {
        private byte geoByte;
        private int geoInt;
        private long geoLong;
        private short geoShort;
        private long ts;

        @Override
        public byte getGeoByte(int col) {
            return geoByte;
        }

        @Override
        public int getGeoInt(int col) {
            return geoInt;
        }

        @Override
        public long getGeoLong(int col) {
            return geoLong;
        }

        @Override
        public short getGeoShort(int col) {
            return geoShort;
        }

        @Override
        public long getTimestamp(int col) {
            return ts;
        }

        void of(long ts, byte geoByte, short geoShort, int geoInt, long geoLong) {
            this.ts = ts;
            this.geoByte = geoByte;
            this.geoShort = geoShort;
            this.geoInt = geoInt;
            this.geoLong = geoLong;
        }
    }

    // Minimal single-row Record stub feeding copyRowFromRecord a TIMESTAMP, a
    // STRING, and a BINARY column (indexes 0, 1, 2). Rebind per row via of().
    private static final class VarSizeRecord implements Record {
        private BinarySequence bin;
        private CharSequence str;
        private long ts;

        @Override
        public BinarySequence getBin(int col) {
            return bin;
        }

        @Override
        public CharSequence getStrA(int col) {
            return str;
        }

        @Override
        public CharSequence getStrB(int col) {
            return str;
        }

        @Override
        public long getTimestamp(int col) {
            return ts;
        }

        void of(long ts, CharSequence str, BinarySequence bin) {
            this.ts = ts;
            this.str = str;
            this.bin = bin;
        }
    }

    // Minimal single-row Record stub feeding copyRowFromRecord a TIMESTAMP and a
    // VARCHAR column (indexes 0, 1). Rebind per row via of().
    private static final class VarcharRecord implements Record {
        private long ts;
        private Utf8Sequence varchar;

        @Override
        public long getTimestamp(int col) {
            return ts;
        }

        @Override
        public Utf8Sequence getVarcharA(int col) {
            return varchar;
        }

        @Override
        public Utf8Sequence getVarcharB(int col) {
            return varchar;
        }

        void of(long ts, Utf8Sequence varchar) {
            this.ts = ts;
            this.varchar = varchar;
        }
    }

    // Minimal single-row Record stub feeding copyRowFromRecord a TIMESTAMP (col 0), a
    // LONG256 (col 1), a UUID (col 2) and a LONG128 (col 3). UUID and LONG128 are read
    // through the same getLong128Lo / getLong128Hi accessors, dispatched on the column
    // index. Rebind per row via of().
    private static final class WideFixedRecord implements Record {
        private long l128Hi;
        private long l128Lo;
        private Long256 l256;
        private long ts;
        private long uuidHi;
        private long uuidLo;

        @Override
        public long getLong128Hi(int col) {
            return col == 2 ? uuidHi : l128Hi;
        }

        @Override
        public long getLong128Lo(int col) {
            return col == 2 ? uuidLo : l128Lo;
        }

        @Override
        public Long256 getLong256A(int col) {
            return l256;
        }

        @Override
        public long getTimestamp(int col) {
            return ts;
        }

        void of(long ts, Long256 l256, long uuidLo, long uuidHi, long l128Lo, long l128Hi) {
            this.ts = ts;
            this.l256 = l256;
            this.uuidLo = uuidLo;
            this.uuidHi = uuidHi;
            this.l128Lo = l128Lo;
            this.l128Hi = l128Hi;
        }
    }
}
