/*+*****************************************************************************
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *  Licensed under the Apache License, Version 2.0 (the "License");
 ******************************************************************************/
package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * The commit mode is read MORE THAN ONCE inside a single {@code commit0}, from a volatile that a peer
 * WalWriter on the same table can republish:
 *
 * <ol>
 *   <li>{@code syncIfRequired()} reads it to decide whether to DEFER the device flush;</li>
 *   <li>{@code TableSequencerImpl.nextTxn} reads it to decide whether to REGISTER the durable-ack pin;</li>
 *   <li>the guard after sequencing reads it to decide whether to RECORD the pending frontier.</li>
 * </ol>
 *
 * <p>Nothing forces the three to agree. {@code setCommitModeAtSeqTxn} does not defer a change to a txn
 * boundary -- its guard only stops an OLDER publish clobbering a newer one -- so the new mode is visible
 * to every reader immediately. And the publisher is not a distant thread:
 * {@code publishEffectiveCommitModeIfUnset}'s own comment notes a sequenced {@code SET PARAM commit_mode}
 * is published by a WalWriter, and several WalWriters sharing one tracker is the normal ILP shape.
 *
 * <p>This drives the ADAPTIVE -&gt; NOSYNC direction, which is the durability lie: the flush is DEFERRED at
 * (1), then no pin is registered at (2) and nothing is recorded at (3), so the deferred sequencer record
 * is never flushed by anyone -- yet a peer's {@code markWriterDurable}, finding an empty pin map, advances
 * the shared frontier to {@code getSeqTxn()} and swallows the un-flushed txn. That is the CRITICAL-2
 * over-claim reached by a different route than the one Task 1b closed.
 */
public class AdaptiveCommitModeFlipRaceTest extends AbstractCairoTest {

    @Test
    public void testModeFlipMidCommitMustNotOverClaimTheUnflushedTxn() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 50_000);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);

            try (WalWriter racer = engine.getWalWriter(tt)) {
                // Flip the published mode inside the window: after syncIfRequired() has DEFERRED, before
                // the sequencer decides whether to pin. This models a peer WalWriter publishing a
                // sequenced SET PARAM commit_mode at exactly the wrong moment.
                WalWriter.deferredCommitInterceptor = new WalWriter.DeferredCommitInterceptor() {
                    @Override
                    public void onDeferDecidedBeforeSequencing(int walId) {
                        tracker.setCommitModeAtSeqTxn(CommitMode.NOSYNC, tracker.getSeqTxn());
                    }

                    @Override
                    public void onSequencedBeforePin(int walId, long seqTxn) {
                    }
                };
                try {
                    TableWriterRow(racer);
                    racer.commit();
                } finally {
                    WalWriter.deferredCommitInterceptor = null;
                }

                final long sequenced = tracker.getSeqTxn();
                final long durable = tracker.getLocalDurableSeqTxn();

                // The commit deferred its device flush and then, seeing the flipped mode, recorded no
                // pending frontier and registered no pin. Nothing has flushed it, so it MUST NOT be
                // reported as locally durable.
                Assert.assertTrue(
                        "precondition: the racy commit must have been sequenced",
                        sequenced > 0
                );
                Assert.assertTrue(
                        "a commit whose device flush was DEFERRED and then orphaned by a mid-commit mode"
                                + " flip must not be reported durable: localDurableSeqTxn=" + durable
                                + " must stay below sequencedTxn=" + sequenced,
                        durable < sequenced
                );
            }
        });
    }

    /**
     * The direction that can LIE. Under NOSYNC {@code syncIfRequired0} returns immediately, so the
     * commit takes NO column barriers at all. If the mode flips to ADAPTIVE before the sequencer is
     * reached, the flipped reads at (2) and (3) register a pin and record a pending frontier, and the
     * later {@code flushPendingDurable} makes the SEQUENCER RECORD device-durable over data that was
     * never fsynced -- exactly what the fail-safe group-commit protocol exists to prevent:
     * "prevents a peer's later fdatasync of the shared sequencer from publishing a record whose
     * data/_event is still volatile".
     * <p>
     * The assertion is the implication, not the raw frontier: a txn may be reported locally durable
     * ONLY if its WAL column data was actually barriered. Written that way so it stays meaningful if
     * the flush timing changes.
     */
    @Test
    public void testModeFlipToAdaptiveMidCommitMustNotAckUnflushedData() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 50_000);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);

        final AdaptiveWalDurabilityTest.FdatasyncOrderFacade trackFf =
                new AdaptiveWalDurabilityTest.FdatasyncOrderFacade();
        assertMemoryLeak(trackFf, () -> {
            execute("create table z (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("z");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);

            trackFf.resetFdatasyncOrder();
            WalWriter.deferredCommitInterceptor = new WalWriter.DeferredCommitInterceptor() {
                @Override
                public void onDeferDecidedBeforeSequencing(int walId) {
                    // A peer WalWriter publishing a sequenced SET PARAM commit_mode='adaptive' lands
                    // here: syncIfRequired() has already skipped every barrier under NOSYNC.
                    tracker.setCommitModeAtSeqTxn(CommitMode.ADAPTIVE, tracker.getSeqTxn());
                }

                @Override
                public void onSequencedBeforePin(int walId, long seqTxn) {
                }
            };
            try {
                try (WalWriter racer = engine.getWalWriter(tt)) {
                    TableWriterRow(racer);
                    racer.commit();
                } // release flushes any pending deferred batch
            } finally {
                WalWriter.deferredCommitInterceptor = null;
            }

            Assert.assertEquals("precondition: the flip must have taken effect",
                    CommitMode.ADAPTIVE, tracker.getCommitMode());

            long columnBarriers = 0;
            for (String path : trackFf.getFdatasyncOrder()) {
                if (path.contains("wal") && path.endsWith(".d")) {
                    columnBarriers++;
                }
            }
            final long sequenced = tracker.getSeqTxn();
            final long durable = tracker.getLocalDurableSeqTxn();

            Assert.assertTrue("precondition: the racy commit must have been sequenced", sequenced > 0);
            // NO LIE: a txn may be reported locally durable only if its WAL column data was barriered.
            Assert.assertTrue(
                    "a txn may be reported locally durable ONLY if its WAL column data was barriered."
                            + " columnBarriers=" + columnBarriers + " durable=" + durable
                            + " sequenced=" + sequenced + " syncedPaths=" + trackFf.getFdatasyncOrder(),
                    columnBarriers > 0 || durable < sequenced
            );
            // NO STALL: the strengthen rule must also have taken the skipped barriers, so the frontier
            // catches up. Without it the sequencer's pin -- registered under the flipped mode and never
            // reaped, since orphanWriterPending runs only on the fdatasync-failure path -- would floor
            // the frontier at min(pin)-1 for this table until reboot. Safe, but a permanent stall.
            Assert.assertTrue(
                    "the commit's skipped barriers must have been taken once the flip was observed:"
                            + " columnBarriers=" + columnBarriers,
                    columnBarriers > 0
            );
            Assert.assertEquals(
                    "the durable frontier must not be left stranded below the sequenced txn by a pin"
                            + " that nothing will ever reap",
                    sequenced, durable
            );
        });
    }

    private static void TableWriterRow(WalWriter writer) {
        io.questdb.cairo.TableWriter.Row row = writer.newRow(0L);
        row.putLong(1, 1L);
        row.append();
    }
}
