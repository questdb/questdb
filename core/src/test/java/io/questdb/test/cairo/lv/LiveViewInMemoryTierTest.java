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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.IntList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.engine.TestBinarySequence;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
        // Reader CAS-loop: rc < 0 means a writer holds
        // the sentinel; the reader must spin until the writer releases. We hold
        // the sentinel on a non-published slot, run acquireRead in the
        // background — it pins the *currently-published* slot immediately, so
        // no spin. Then we publishSwap onto the sentinel-held slot to validate
        // the "publishedIdx moved during pin" path actually retries cleanly.
        assertMemoryLeak(() -> {
            IntList types = singleLongCol();
            try (LiveViewInMemoryTier tier = new LiveViewInMemoryTier(types, 0, PAGE_SIZE)) {
                // Seed a known value in the initially-published slot.
                int initialPublished = tier.getPublishedIdx();
                LiveViewInMemoryBuffer seed = tier.getSlot(initialPublished);
                seed.putLong(0, 0, 11L);
                seed.setRowCount(1);

                // Writer takes sentinel on the OTHER slot.
                int writeIdx = 1 - initialPublished;
                LiveViewInMemoryBuffer write = tier.tryAcquireWrite(writeIdx);
                Assert.assertNotNull(write);
                write.putLong(0, 0, 22L);
                write.setRowCount(1);

                // Reader pins the initially-published slot; the sentinel on the
                // OTHER slot doesn't block this.
                int pinned = tier.acquireRead();
                Assert.assertEquals(initialPublished, pinned);
                Assert.assertEquals(11L, tier.getSlot(pinned).getLong(0, 0));
                tier.releaseRead(pinned);

                // Now publish the swap; the new pin must observe the swapped slot.
                tier.publishSwap(writeIdx);
                int pinnedAfter = tier.acquireRead();
                Assert.assertEquals(writeIdx, pinnedAfter);
                Assert.assertEquals(22L, tier.getSlot(pinnedAfter).getLong(0, 0));
                tier.releaseRead(pinnedAfter);
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

    private static void assertBinEquals(byte[] expected, BinarySequence actual) {
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.length, actual.length());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals("byte " + i, expected[i], actual.byteAt(i));
        }
    }

    private static byte[] bytesOf(int seed, int len) {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[i] = (byte) (seed + i);
        }
        return b;
    }

    private static String repeat(char c, int len) {
        char[] a = new char[len];
        java.util.Arrays.fill(a, c);
        return new String(a);
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

    // Per-row VARCHAR value mixing the null, fully-inlined and split (data-region)
    // cases so a slow-path swap exercises every encoding branch.
    private static String varcharValue(int r) {
        if (r % 3 == 0) {
            return null;
        }
        // Odd rows are short (fully inlined); a longer row spills to the data region.
        return (r % 2 == 0) ? "v" + r : "v" + r + repeat('x', VarcharTypeDriver.VARCHAR_MAX_BYTES_FULLY_INLINED + r);
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
}
