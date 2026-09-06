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
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Randomised soak over the commit-mode FLIP WINDOW.
 * <p>
 * A sequenced {@code SET PARAM commit_mode} is published by a peer WalWriter, so the mode can change
 * WHILE another writer is inside {@code commit0} -- which reads it to decide whether to defer the device
 * flush, whether to strengthen, and whether to record the durable frontier. Hand-written tests pin the
 * three flip points I could think of ({@link AdaptiveCommitModeFlipRaceTest}); this one draws them.
 * <p>
 * Named *FuzzTest deliberately: the adaptive soak's ordinary-fuzz arm selects
 * {@code %regex[.*Fuzz.*class]}, so this is picked up with no pipeline change and draws a fresh seed
 * every iteration.
 * <p>
 * THE INVARIANT, checked after every commit: a txn reported locally durable must have had its WAL
 * column data barriered during that commit. Everything else about a flip is a policy choice --
 * withholding an ack is safe, granting a false one is not. Stalls are deliberately NOT asserted: the
 * barriers-taken guard may legitimately withhold a frontier advance, which is the safe side of the
 * trade.
 * <p>
 * <b>Measured detection.</b> With the {@code adaptiveBarriersTaken} guard removed, over 8 fresh seeds:
 * <pre>
 *   W drawn per seed          5/8 caught   (half the seeds never entered the W=0 regime)
 *   both W regimes every run  8/8 caught
 * </pre>
 * and 8/8 PASS with the guard restored, i.e. no seed produces a spurious failure. That second number
 * matters as much as the first: this runs in a continuous soak that pages on failure, and a test that
 * cries wolf on one seed in twenty teaches people to ignore the alert.
 * <p>
 * Hence W is covered by two methods rather than drawn. The data-&gt;sequencer ordering defect this series
 * fixed appears ONLY at {@code W=0}, where {@code sync0} fdatasyncs the sequencer inline instead of
 * deferring it; drawing W was the dominant source of detection variance.
 */
public class AdaptiveCommitModeFlipFuzzTest extends AbstractCairoTest {

    /**
     * {@code W=0}: the sequencer flushes INLINE inside getSequencerTxn, which is the regime where the
     * data-&gt;sequencer ordering defect lived. Run every iteration rather than drawn -- measured, drawing
     * W halved the seeds that reach this regime and dominated the detection variance.
     */
    @Test
    public void testFlipWindowNeverAcksUnbarrieredDataZeroWindow() throws Exception {
        fuzzFlipWindow(0);
    }

    /**
     * {@code W>0}: the sequencer flush is DEFERRED to the batched flushPendingDurable, which is the
     * shipped default's regime.
     */
    @Test
    public void testFlipWindowNeverAcksUnbarrieredDataDeferredWindow() throws Exception {
        fuzzFlipWindow(50_000);
    }

    private void fuzzFlipWindow(int windowUs) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final boolean startAdaptive = rnd.nextBoolean();
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, startAdaptive ? "adaptive" : "nosync");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, windowUs);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        LOG.info().$("flip fuzz [windowUs=").$(windowUs).$(", startAdaptive=").$(startAdaptive).I$();

        final AdaptiveWalDurabilityTest.FdatasyncOrderFacade trackFf =
                new AdaptiveWalDurabilityTest.FdatasyncOrderFacade();
        assertMemoryLeak(trackFf, () -> {
            execute("create table ff (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("ff");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);

            // Flip at a DRAWN seam, to a DRAWN mode. onSequencedBeforePin is included even though it
            // fires after every decision -- a flip there must be harmless, and asserting that is the
            // point of including it.
            WalWriter.deferredCommitInterceptor = new WalWriter.DeferredCommitInterceptor() {
                @Override
                public void onDeferDecidedBeforeSequencing(int walId) {
                    maybeFlip(0);
                }

                @Override
                public void onSequencedBeforePin(int walId, long seqTxn) {
                    maybeFlip(2);
                }

                @Override
                public void onStrengthenDecidedBeforeSequencing(int walId) {
                    maybeFlip(1);
                }

                private void maybeFlip(int seam) {
                    if (rnd.nextInt(3) == seam) {
                        tracker.setCommitModeAtSeqTxn(
                                rnd.nextBoolean() ? CommitMode.ADAPTIVE : CommitMode.NOSYNC,
                                tracker.getSeqTxn()
                        );
                    }
                }
            };

            try (WalWriter writer = engine.getWalWriter(tt)) {
                // Detection is probabilistic: a flip must land in a seam that matters, under a W that
                // matters. Measured with the barriers-taken guard removed, 30-100 commits caught it on
                // 5 of 8 seeds; the run costs under a second, so buy a higher per-iteration rate with
                // more commits rather than leaving it to the soak's iteration count.
                final int commits = 200 + rnd.nextInt(300);
                for (int i = 0; i < commits; i++) {
                    trackFf.resetFdatasyncOrder();

                    final io.questdb.cairo.TableWriter.Row row = writer.newRow(i * 1_000_000L);
                    row.putLong(1, i);
                    row.append();
                    writer.commit();

                    long columnBarriers = 0;
                    for (String path : trackFf.getFdatasyncOrder()) {
                        if (path.contains("wal") && path.endsWith(".d")) {
                            columnBarriers++;
                        }
                    }
                    final long sequenced = tracker.getSeqTxn();
                    final long durable = tracker.getLocalDurableSeqTxn();

                    Assert.assertTrue(
                            "commit " + i + " was reported locally durable but no WAL column barrier"
                                    + " covered it: durable=" + durable + " sequenced=" + sequenced
                                    + " columnBarriers=" + columnBarriers + " windowUs=" + windowUs
                                    + " mode=" + tracker.getCommitMode(),
                            durable < sequenced || columnBarriers > 0
                    );
                }
            } finally {
                WalWriter.deferredCommitInterceptor = null;
            }
        });
    }
}
