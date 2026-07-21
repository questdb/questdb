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

package io.questdb.test.cairo.wal.seq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Direct unit tests for the adaptive group-commit CONTIGUOUS DURABLE PREFIX frontier (CRITICAL 2). Under
 * {@code W > 0} several {@link io.questdb.cairo.wal.WalWriter}s of one table share one {@code SeqTxnTracker}
 * and flush their batches INDEPENDENTLY (no cross-writer barrier). The durable-ack frontier
 * ({@code localDurableSeqTxn}) must therefore advance only to the contiguous durable prefix across all
 * writers — {@code min(oldest-un-flushed seqTxn) - 1}, or {@code getSeqTxn()} when nothing is pending — never
 * to the flushing writer's own seqTxn (which is the CRIT-2 over-claim / silent acknowledged-data loss bug).
 *
 * <p>These drive {@code registerWriterPending} / {@code markWriterDurable} / {@code resetDurableFrontier}
 * directly, so the tracker's contiguity math is verified without the WAL plumbing.
 */
public class SeqTxnTrackerContiguousFrontierTest {

    /**
     * A DROPPED (distressed / crash-torn) writer that never flushed must LEAVE its pin: the frontier can
     * never advance past that writer's non-durable data, even as OTHER writers keep flushing higher txns.
     */
    @Test
    public void testDroppedWriterPinHoldsFrontierBehindForever() {
        final SeqTxnTracker tracker = newTracker();
        tracker.initTxns(0, 30, false); // max committed seqTxn = 30

        tracker.registerWriterPending(1, 10); // writer 1: oldest un-flushed = 10 (this one will be "dropped")
        tracker.registerWriterPending(2, 20); // writer 2
        tracker.registerWriterPending(3, 30); // writer 3

        // Writers 2 and 3 flush (in any order); writer 1 is dropped WITHOUT a flush (its pin is left in place).
        tracker.markWriterDurable(3);
        assertEquals("writer 1's hole at 10 bounds the prefix to 9", 9, tracker.getLocalDurableSeqTxn());
        tracker.markWriterDurable(2);
        assertEquals("writer 1 still un-flushed: frontier stuck at 9", 9, tracker.getLocalDurableSeqTxn());

        // Even a brand-new writer flushing a still-higher txn cannot lift the frontier past writer 1's hole.
        tracker.notifyOnCommit(40);
        tracker.registerWriterPending(4, 40);
        tracker.markWriterDurable(4);
        assertEquals("a dropped writer's un-flushed pin holds the durable frontier behind it",
                9, tracker.getLocalDurableSeqTxn());
    }

    /**
     * markWriterDurable for an unknown / already-removed walId is a harmless no-op that still recomputes the
     * contiguous prefix from the remaining pins (idempotent teardown safety).
     */
    @Test
    public void testMarkDurableUnknownWalIdIsSafeNoOp() {
        final SeqTxnTracker tracker = newTracker();
        tracker.initTxns(0, 5, false);
        tracker.registerWriterPending(1, 5);

        // No such walId: must not throw and must not advance past the pending writer 1.
        tracker.markWriterDurable(999);
        assertEquals("unknown walId must not advance past the pending writer", 4, tracker.getLocalDurableSeqTxn());

        tracker.markWriterDurable(1);
        assertEquals("after the real writer flushes the frontier reaches getSeqTxn()",
                tracker.getSeqTxn(), tracker.getLocalDurableSeqTxn());
        // Removing an already-removed walId is a clean no-op.
        tracker.markWriterDurable(1);
        assertEquals(tracker.getSeqTxn(), tracker.getLocalDurableSeqTxn());
    }

    /**
     * The frontier is MONOTONE: an out-of-order flush that computes a lower contiguous prefix than the current
     * frontier never moves it backward.
     */
    @Test
    public void testMonotoneFrontierNeverMovesBackward() {
        final SeqTxnTracker tracker = newTracker();
        tracker.initTxns(0, 10, false);

        // Writer 1 flushes a contiguous batch first -> frontier reaches getSeqTxn().
        tracker.registerWriterPending(1, 5);
        tracker.markWriterDurable(1);
        final long high = tracker.getLocalDurableSeqTxn();
        assertEquals(10, high);

        // A NEW pending writer appears at a lower oldest-un-flushed; a stray recompute must not drop the
        // already-published frontier (durable data stays durable).
        tracker.registerWriterPending(2, 8);
        tracker.markWriterDurable(999); // recompute with pin {2->8} present: min-1 = 7 < 10
        assertTrue("frontier must never regress below a previously published value",
                tracker.getLocalDurableSeqTxn() >= high);
    }

    /**
     * THE CORE CONTRACT. register(1,10)+register(2,11): writer 2 flushing OUT OF ORDER (before writer 1) must
     * leave the frontier at 9 (writer 1's txn 10 is the oldest hole), NOT at writer 2's own seqTxn 11. Once
     * writer 1 flushes too, the whole prefix is durable and the frontier jumps to getSeqTxn().
     */
    @Test
    public void testOutOfOrderFlushKeepsFrontierAtContiguousPrefix() {
        final SeqTxnTracker tracker = newTracker();
        tracker.initTxns(0, 11, false); // max committed seqTxn = 11

        tracker.registerWriterPending(1, 10); // writer 1: oldest un-flushed = 10
        tracker.registerWriterPending(2, 11); // writer 2: oldest un-flushed = 11

        // Writer 2 flushes FIRST (out of order). Its own txn 11 is durable, but 10 is still a hole.
        tracker.markWriterDurable(2);
        assertEquals("out-of-order flush must NOT over-claim: frontier = oldest hole (10) - 1",
                9, tracker.getLocalDurableSeqTxn());

        // Writer 1 flushes -> the prefix is now contiguous up to getSeqTxn().
        tracker.markWriterDurable(1);
        assertEquals("with nothing pending the frontier reaches the max committed seqTxn",
                tracker.getSeqTxn(), tracker.getLocalDurableSeqTxn());
        assertEquals(11, tracker.getLocalDurableSeqTxn());
    }

    /**
     * putIfAbsent: a writer's LATER commits within the same batch must not lower its recorded oldest
     * un-flushed seqTxn (the batch floor is set once, at batch start).
     */
    @Test
    public void testRegisterIsPutIfAbsentWithinBatch() {
        final SeqTxnTracker tracker = newTracker();
        tracker.initTxns(0, 15, false);

        tracker.registerWriterPending(1, 10); // batch floor
        tracker.registerWriterPending(1, 12); // later commit in the same batch — must NOT change the floor
        tracker.registerWriterPending(1, 15);
        tracker.registerWriterPending(2, 11);

        tracker.markWriterDurable(2);
        assertEquals("writer 1's floor stays at its batch start (10), so prefix is 9",
                9, tracker.getLocalDurableSeqTxn());
    }

    /**
     * resetDurableFrontier (recovery / reboot) clears all pins and resets the frontier to the uninitialised
     * -1, so a fresh writer that registers + flushes afterwards recomputes cleanly from an empty map.
     */
    @Test
    public void testResetDurableFrontierClearsPinsAndFrontier() {
        final SeqTxnTracker tracker = newTracker();
        tracker.initTxns(0, 11, false);

        tracker.registerWriterPending(1, 10);
        tracker.registerWriterPending(2, 11);
        tracker.markWriterDurable(2);
        assertEquals(9, tracker.getLocalDurableSeqTxn());

        tracker.resetDurableFrontier();
        assertEquals("reset returns the frontier to the uninitialised -1", -1, tracker.getLocalDurableSeqTxn());

        // The map is cleared too: a fresh writer flushing now sees an EMPTY map -> reaches getSeqTxn()
        // (the stale writer-1 pin must be gone, otherwise this would be stuck at 9).
        tracker.notifyOnCommit(20);
        tracker.registerWriterPending(5, 20);
        tracker.markWriterDurable(5);
        assertEquals("after reset the stale pins are gone; the frontier tracks getSeqTxn() again",
                tracker.getSeqTxn(), tracker.getLocalDurableSeqTxn());
    }

    /**
     * The single-writer case (the dominant shape): one writer registers a batch and flushes it; the frontier
     * simply reaches the max committed seqTxn.
     */
    @Test
    public void testSingleWriterFrontierReachesGetSeqTxn() {
        final SeqTxnTracker tracker = newTracker();
        tracker.initTxns(0, 7, false);

        tracker.registerWriterPending(1, 3);
        assertEquals("while pending the frontier lags", -1, tracker.getLocalDurableSeqTxn());
        tracker.markWriterDurable(1);
        assertEquals(tracker.getSeqTxn(), tracker.getLocalDurableSeqTxn());
        assertEquals(7, tracker.getLocalDurableSeqTxn());
    }

    @NotNull
    private static SeqTxnTracker newTracker() {
        return new SeqTxnTracker(new DefaultCairoConfiguration(null) {
            @Override
            public @NotNull MillisecondClock getMillisecondClock() {
                return MillisecondClockImpl.INSTANCE;
            }
        });
    }
}
