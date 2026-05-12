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
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.std.IntList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Phase 1b Commit 2: unit tests for the N=2 in-memory tier infrastructure.
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
        // RFC 123 §"Stall behavior": when both slots are pinned by readers the
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
        // RFC 123 §"In-memory tier" reader CAS-loop: rc < 0 means a writer holds
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

    private static IntList singleLongCol() {
        IntList types = new IntList(1);
        types.add(ColumnType.LONG);
        return types;
    }
}
