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

package io.questdb.test.cairo.o3;

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for partition COMPACTION (PARTITION_COMPACTION.md), which reclaims the dead space
 * merge-append leaves behind. Compaction is a different operation from squash: its unit is one
 * partition (one directory in this branch's model - there is no hardlink split to distinguish a
 * "physical" from a "logical" partition), and it is driven by waste rather than by piece count.
 * <p>
 * Ported from the enterprise `feat-partition-top-split` branch's acceptance suite. That repo's
 * composite partitions can be split across several directories sharing one logical partition
 * (hardlink splits); this branch has none of that; a composite partition here is exactly what the
 * reference repo calls a "folder" - one directory, one {@code _txn} entry, one {@code _geometry}
 * chain. See PARTITION_COMPACTION_state.md for the corrections this port required.
 * <p>
 * JOIN, MOVE-TAIL, MAKE-PLAIN and REWRITE are implemented (PARTITION_COMPACTION.md Sec.9 steps 0, 1, 2,
 * 3, 4, 5). MOVE-TAIL is ported onto this branch's classic-split machinery (a new sibling
 * {@code attachedPartitions} entry for the tail) rather than the reference's hardlink/{@code partitionTop}
 * scheme - see PARTITION_COMPACTION_state.md. MAKE-PLAIN reclaims a MOVE-TAIL'd front's leftover dead
 * space in place, gated on the same {@link io.questdb.cairo.TxnScoreboard} reader check REWRITE never
 * needs (it only ever appends). TRIM-FILES (also step 4, the file-shortening half of the reference's
 * in-place reclaim) is not implemented: a MAKE-PLAIN'd partition's files stay at their old, now-oversized
 * length until a later REWRITE copies it into a fresh, right-sized directory. REWRITE itself needs no
 * reader gate at all: it copies into a brand-new directory and leaves the old one for the ordinary purge
 * to remove once no reader still needs it, so a pinned reader's data stays correct with nothing to wait
 * for - see {@link #testRewriteLeavesAPinnedReadersDataIntact}.
 * <p>
 * JOIN has no dedicated test of its own here. {@code O3PartitionJob} folds list-and-file-adjacent pieces
 * INLINE, as part of the same commit that creates them (PARTITION_COMPACTION_state.md's "JOIN, inlined"),
 * so by the time a housekeeping pass reaches {@code TableWriter}'s own, separate JOIN sweep
 * ({@code foldContiguousPieces} / {@code foldFoldableFolders}), an ordinary sequence of merge-append
 * writes has never been observed to leave it anything to fold - the planner's own tail-extend
 * optimization (APPEND) and the fact that a relocated piece always lands at the physical tail keep
 * file-adjacency and list-adjacency from coinciding across separate commits. See the state doc for the
 * scenarios ruled out.
 */
public class O3PartitionCompactionTest extends AbstractCairoTest {

    @Before
    public void resetPassDays() {
        passDay = 0;
    }

    /** Days the housekeeping commits consume, never reused within a test. */
    private static int passDay;

    @Test
    public void testAgeTriggerCompactsAPartitionNothingHasWrittenTo() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            enableCompaction();
            // The age rule fires when a partition has not changed shape for this long AND it still has
            // waste or more than one piece.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "60m");
            // Waste/table-pressure alone must not be what fires here - only the age rule, once idle.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_PERCENT, "99");

            setCurrentMicros(parseMicros("2024-01-10T00:00:00.000000Z"));
            createDayTable("x", "2024-01-01", 20_000);
            // Two rewrites of the same stride, so the partition is left composite with dead rows.
            backdate("x", "2024-01-01T06:00:00", 200);
            backdate("x", "2024-01-01T06:00:00", 200);
            Assert.assertTrue("fixture produced no waste", deadRows("x") > 0);

            // Nothing writes to it for two hours of wall clock, then an unrelated commit gives
            // housekeeping a chance to run.
            setCurrentMicros(parseMicros("2024-01-10T02:00:00.000000Z"));
            append("x", "2024-01-05", 10);
            runCompactionPasses("x");

            Assert.assertEquals(
                    "the age rule did not compact a partition idle for two hours;" +
                            " dead rows still on disk: " + deadRows("x"),
                    0,
                    deadRows("x")
            );
        });
    }

    /**
     * A partition REWRITE has just emptied is not a compaction candidate: one piece at row 0, nothing
     * left to copy out. It must not be picked up again by the waste-ratio rule, or compaction loops
     * forever copying nothing. Detected here as repeated work rather than as a wrong result: the
     * physically-written-rows counter must stop moving once the copying is done.
     */
    @Test
    public void testCompactionDoesNotLoopOnAPartitionItAlreadyEmptied() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            enableCompaction();

            createDayTable("x", "2024-01-01", 20_000);
            // Three rewrites: each merge-append here rewrites the WHOLE partition (no pre-split cuts
            // it), so two rounds leave dead just under the live count - three pushes past the ratio.
            // The tight ratio is set only now, after the buildup: with it in effect throughout,
            // compaction reclaims the waste as it is created and there is nothing left to loop on.
            backdate("x", "2024-01-01T06:00:00", 200);
            backdate("x", "2024-01-01T06:00:00", 200);
            backdate("x", "2024-01-01T06:00:00", 200);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, "1");
            runCompactionPasses("x");

            // The partition must have been dealt with by now - this is what fails while compaction
            // is missing, and it is the precondition for the anti-loop check below to mean anything.
            Assert.assertEquals(
                    "compaction left dead rows behind, so the anti-loop check below is vacuous",
                    0,
                    deadRows("x")
            );

            final long afterFirstRound = physicallyWrittenRows();
            final long insertedByPasses = runCompactionPasses("x");
            final long written = physicallyWrittenRows() - afterFirstRound - insertedByPasses;

            Assert.assertEquals(
                    "compaction kept re-running on a partition with nothing left to move" +
                            " [rowsWrittenBySecondRound=" + written + ']',
                    0,
                    written
            );
        });
    }

    /**
     * MAKE-PLAIN's own success path: a successful MOVE-TAIL is immediately followed, in the same
     * housekeeping pass, by an attempt at MAKE-PLAIN on the front it just left behind - the front is
     * exactly MAKE-PLAIN's own eligible shape (one piece, row 0, dead space above it), and with no reader
     * in the way there is nothing to wait for. No bytes copied, {@code nameTxn} unchanged, proof this is
     * bookkeeping and not a REWRITE in disguise.
     */
    @Test
    public void testMakePlainReclaimsAMoveTailedFrontsDeadSpace() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            // Same fixture shape as testMoveTailCopiesTheTailNotTheWholePartition: a huge clean front
            // with a small, repeatedly-relocated stride pre-split into its own tail pieces, so MOVE-TAIL
            // (not REWRITE) is what the piece-count rule triggers.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

            createDayTable("x", "2024-01-01", 20_000);
            backdate("x", "2024-01-01T05:00:00", 200);
            backdate("x", "2024-01-01T05:00:00", 200);
            backdate("x", "2024-01-01T05:00:00", 200);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_MAX_PIECES, "2");

            final String expected = fingerprintOfDay("x", "2024-01-01");
            final long frontNameTxnBefore = frontNameTxnOfDay("x", "2024-01-01");

            enableCompaction();
            runCompactionPasses("x");

            Assert.assertFalse(
                    "MAKE-PLAIN did not follow MOVE-TAIL in the same housekeeping pass",
                    isComposite("x", "2024-01-01")
            );
            Assert.assertEquals(
                    "MAKE-PLAIN did not reclaim the dead space MOVE-TAIL left above the front's one piece",
                    0,
                    deadRowsOfDay("x", "2024-01-01")
            );
            Assert.assertEquals(
                    "MAKE-PLAIN rewrote the front - its nameTxn must stay the one MOVE-TAIL left behind",
                    frontNameTxnBefore,
                    frontNameTxnOfDay("x", "2024-01-01")
            );
            Assert.assertEquals("MAKE-PLAIN changed the data", expected, fingerprintOfDay("x", "2024-01-01"));
        });
    }

    /**
     * MAKE-PLAIN must wait for a reader still pinning the transaction its pre-MAKE-PLAIN geometry record
     * came from - unlike REWRITE and MOVE-TAIL, which never need this (see
     * {@link #testRewriteLeavesAPinnedReadersDataIntact}), MAKE-PLAIN reuses that record in place, so an
     * older reader resolving it would otherwise misread the reclaimed dead space as belonging to a piece
     * that is no longer there. This fixture's stride is pre-split into its own piece and MOVE-TAIL runs
     * first, so the front actually reaches MAKE-PLAIN's eligible shape instead of being reclaimed by
     * REWRITE before a pinned reader is even in the picture. The reader opens BEFORE MOVE-TAIL runs, so
     * it pins the pre-MOVE-TAIL transaction, not a later one that already sees the MAKE-PLAIN-eligible
     * shape and would have nothing to wait for.
     */
    @Test
    public void testMakePlainWaitsForAPinnedReaderThenReclaimsOnceItGoes() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            setCurrentMicros(parseMicros("2024-01-10T00:00:00.000000Z"));
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

            createDayTable("x", "2024-01-01", 20_000);
            backdate("x", "2024-01-01T05:00:00", 200);
            backdate("x", "2024-01-01T05:00:00", 200);
            backdate("x", "2024-01-01T05:00:00", 200);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_MAX_PIECES, "2");
            enableCompaction();

            final TableToken tt = engine.verifyTableName("x");
            final String before = fingerprintOfDay("x", "2024-01-01");

            try (TableReader pinned = engine.getReader(tt)) {
                Assert.assertNotNull(pinned);

                // MOVE-TAIL does not check the scoreboard, so it still fires with the reader pinned to
                // the pre-MOVE-TAIL transaction. The MAKE-PLAIN attempt chained onto its success is what
                // does check it, and declines here - the front is left reduced to its one piece with the
                // dead space still above it, not reclaimed.
                runCompactionPasses("x");
                Assert.assertTrue("fixture did not reach MOVE-TAIL", isComposite("x", "2024-01-01"));
                Assert.assertEquals(1, pieceCountOfDay("x", "2024-01-01"));
                final long deadBeforeMakePlain = deadRowsOfDay("x", "2024-01-01");
                Assert.assertTrue("MOVE-TAIL left no dead space to protect", deadBeforeMakePlain > 0);

                // The decline above started a one-minute backoff on the partition - clear it, so the
                // second decline below is proven to still be the reader check, not a leftover backoff.
                setCurrentMicros(currentMicros + 2 * Micros.MINUTE_MICROS);
                runCompactionPasses("x");

                Assert.assertEquals(
                        "the rows of the compacted day changed under compaction",
                        before,
                        fingerprintOfDay("x", "2024-01-01")
                );
                Assert.assertTrue(
                        "the partition was made non-composite while a reader still held the" +
                                " pre-MAKE-PLAIN transaction - its geometry record still lists the" +
                                " reclaimed dead space as live",
                        isComposite("x", "2024-01-01")
                );
                Assert.assertEquals(
                        "MAKE-PLAIN must not touch the partition while declined - dead rows changed" +
                                " with a reader still pinned",
                        deadBeforeMakePlain,
                        deadRowsOfDay("x", "2024-01-01")
                );
            }

            // Reader gone: the decline above also started a one-minute backoff on the partition (the
            // same bookkeeping any other declined compaction gets) - clear it before the retry, or this
            // pass would be suppressed for a reason that has nothing to do with the reader anymore.
            setCurrentMicros(currentMicros + 2 * Micros.MINUTE_MICROS);
            runCompactionPasses("x");
            Assert.assertFalse(
                    "the partition is still composite after the last reader went away;" +
                            " dead rows: " + deadRowsOfDay("x", "2024-01-01"),
                    isComposite("x", "2024-01-01")
            );
            Assert.assertEquals(0, deadRowsOfDay("x", "2024-01-01"));
            Assert.assertEquals("MAKE-PLAIN changed the data", before, fingerprintOfDay("x", "2024-01-01"));
        });
    }

    /**
     * MOVE-TAIL leaves the clean front's directory untouched and copies only the messy tail pieces into
     * a new sibling partition. Unlike REWRITE-only compaction, the day ends up as TWO {@code _txn}
     * entries and the front's own {@code nameTxn} is unchanged - proof the front was never rewritten,
     * not just that fewer bytes moved. The front's own composite state does not survive the same pass,
     * though: with no reader in the way, the MAKE-PLAIN attempt {@code runCompaction} chains onto a
     * successful MOVE-TAIL reclaims it immediately - see {@link #testMakePlainReclaimsAMoveTailedFrontsDeadSpace}
     * for that half in isolation.
     */
    @Test
    public void testMoveTailCopiesTheTailNotTheWholePartition() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            // The clean front is deliberately huge relative to the dead space one relocated 200-row
            // stride leaves behind, so the waste-ratio rule (dead > ratio*live) never fires here - the
            // piece-count rule is what selects this partition instead.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            // Small enough that the pre-split isolates the repeatedly-touched stride into its own piece
            // instead of merge-append relocating the whole partition - MOVE-TAIL needs a genuine clean
            // front left standing at row 0 to have anything to leave alone.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

            // 20k rows spanning 1s each (~5.5 hours from day start), and the churn is aimed at a 200-row
            // stride near the end of that span, so the clean front is the overwhelming majority of the
            // partition.
            createDayTable("x", "2024-01-01", 20_000);
            // Three rewrites: each one relocates only the pre-split-isolated stride, adding one more
            // piece each time. The piece-count limit is set only now, after the buildup, so it is the
            // OBSERVED pass that trips it (max.pieces=2), not the buildup itself.
            backdate("x", "2024-01-01T05:00:00", 200);
            backdate("x", "2024-01-01T05:00:00", 200);
            backdate("x", "2024-01-01T05:00:00", 200);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_MAX_PIECES, "2");

            final long deadBefore = deadRowsOfDay("x", "2024-01-01");
            Assert.assertTrue("fixture produced no waste", deadBefore > 0);
            final long partitionsBefore = partitionCountOfDay("x", "2024-01-01");
            final long frontNameTxnBefore = frontNameTxnOfDay("x", "2024-01-01");

            enableCompaction();
            final long writtenBefore = physicallyWrittenRows();
            final long insertedByPasses = runCompactionPasses("x");
            // Net of the rows the housekeeping commits wrote themselves.
            final long written = physicallyWrittenRows() - writtenBefore - insertedByPasses;

            Assert.assertTrue(
                    "MOVE-TAIL copied more than the tail [rowsWritten=" + written + ", partitionRows=20000]",
                    written > 0 && written < 5_000
            );
            Assert.assertEquals(
                    "MOVE-TAIL did not leave a new sibling partition behind for the day",
                    partitionsBefore + 1,
                    partitionCountOfDay("x", "2024-01-01")
            );
            Assert.assertEquals(
                    "the front partition's own nameTxn changed - it was rewritten, not left alone",
                    frontNameTxnBefore,
                    frontNameTxnOfDay("x", "2024-01-01")
            );
            // MOVE-TAIL itself leaves E untouched - it is the MAKE-PLAIN attempt chained onto its
            // success, not MOVE-TAIL, that reclaims the front's dead space. With no reader pinned in this
            // fixture, that reclaim happens in the very same housekeeping pass: the front ends up plain,
            // not composite, and its dead rows - both what MOVE-TAIL left behind and what it grew by
            // relocating the tail - drop to zero.
            Assert.assertFalse(
                    "MAKE-PLAIN did not follow MOVE-TAIL in the same housekeeping pass",
                    isComposite("x", "2024-01-01")
            );
            Assert.assertEquals(
                    "MAKE-PLAIN did not reclaim the front's dead rows",
                    0,
                    deadRowsOfDay("x", "2024-01-01")
            );
        });
    }

    /**
     * The piece-count rule is a backstop for a partition that has been cut many times. It must reduce
     * the pieces of one partition even when very little space is wasted.
     */
    @Test
    public void testPieceCountTriggerReducesTheNumberOfPieces() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);
            // Waste alone must not be what fires here.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");

            createDayTable("x", "2024-01-01", 40_000);
            // Six separated strides, so the pre-split cuts the day repeatedly. The piece-count limit is
            // set only now, after the buildup, so it is the OBSERVED pass that trips it, not the buildup
            // itself.
            backdate("x", "2024-01-01T02:00:00", 60);
            backdate("x", "2024-01-01T06:00:00", 60);
            backdate("x", "2024-01-01T10:00:00", 60);
            backdate("x", "2024-01-01T14:00:00", 60);
            backdate("x", "2024-01-01T18:00:00", 60);
            backdate("x", "2024-01-01T22:00:00", 60);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_MAX_PIECES, "4");

            final long piecesBefore = pieceCountOfDay("x", "2024-01-01");
            Assert.assertTrue(
                    "fixture did not produce enough pieces to trip the rule [pieces=" + piecesBefore + ']',
                    piecesBefore > 4
            );

            enableCompaction();
            runCompactionPasses("x");
            Assert.assertTrue(
                    "the piece-count rule left the partition above its limit" +
                            " [limit=4, pieces=" + pieceCountOfDay("x", "2024-01-01") + ']',
                    pieceCountOfDay("x", "2024-01-01") <= 4
            );
        });
    }

    /**
     * REWRITE copies live rows into a brand-new directory and leaves the old one for the ordinary purge
     * to remove once no reader still needs it (see {@code processPartitionRemoveCandidates0}'s scoreboard
     * check) - so a reader pinned to the pre-compaction transaction keeps seeing correct data, even
     * though the CURRENT state moves on to non-composite immediately, with nothing to wait for. Unlike
     * MAKE-PLAIN, REWRITE never reuses a pinned reader's already-resolved geometry record, so there is no
     * analogous reader-wait stage for it to go through.
     */
    @Test
    public void testRewriteLeavesAPinnedReadersDataIntact() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            enableCompaction();
            // A full-partition rewrite naturally crosses the table-wide dead-percent default (50%)
            // partway through the buildup below; keep the table-pressure rule out of the way so only
            // the waste-ratio rule (set after the buildup) is what fires.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_PERCENT, "99");

            createDayTable("x", "2024-01-01", 20_000);
            // Three rewrites: each merge-append here rewrites the WHOLE partition (no pre-split cuts
            // it), so two rounds leave dead just under the live count - three pushes past the ratio.
            // The tight ratio is set only now, after the buildup - see testWasteRatioTriggerReclaimsDeadRows.
            backdate("x", "2024-01-01T06:00:00", 200);
            backdate("x", "2024-01-01T06:00:00", 200);
            backdate("x", "2024-01-01T06:00:00", 200);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, "1");

            final TableToken tt = engine.verifyTableName("x");
            final String before = fingerprintOfDay("x", "2024-01-01");

            try (TableReader pinned = engine.getReader(tt)) {
                Assert.assertNotNull(pinned);
                runCompactionPasses("x");

                // The pinned reader must still see exactly what it saw when it opened, even though
                // REWRITE has already run underneath it and retired the directory it was reading from.
                Assert.assertEquals(
                        "the rows of the compacted day changed under compaction",
                        before,
                        fingerprintOfDay("x", "2024-01-01")
                );
            }
        });
    }

    /**
     * The rules are stated in dead rows and wasted bytes, and an operator has to be able to see both.
     * PARTITION_COMPACTION.md Sec.9 step 2 adds {@code deadRows} and {@code lastWriteTimestamp} to
     * {@code table_partitions()}; without them there is no way to observe why compaction did or did
     * not fire.
     */
    @Test
    public void testTablePartitionsReportsDeadRowsAndLastWriteTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            createDayTable("x", "2024-01-01", 20_000);
            backdate("x", "2024-01-01T06:00:00", 200);

            // Fails with "Invalid column" until the two columns exist.
            assertQuery("select count() from (select deadRows, lastWriteTimestamp" +
                    " from table_partitions('x') limit 1)")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n1\n");
        });
    }

    /**
     * The table-wide rule fires on the ratio of dead to live rows across the whole table and picks
     * the coldest partition. Here two days are made wasteful and the older one was written to first,
     * so it is the one that must be compacted first.
     */
    @Test
    public void testTablePressureTriggerCompactsTheColdestPartitionFirst() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();

            setCurrentMicros(parseMicros("2024-01-10T00:00:00.000000Z"));
            createDayTable("x", "2024-01-01", 4_000);
            append("x", "2024-01-02", 4_000);

            // The colder day: churned first, then left alone. The stride has to sit INSIDE the day's
            // data range - 4000 rows one second apart span 00:00:00 to 01:06:39 - or the apply writes
            // the batch as a piece of its own instead of merging, and leaves no dead rows at all.
            backdate("x", "2024-01-01T00:30:00", 400);
            backdate("x", "2024-01-01T00:30:00", 400);
            backdate("x", "2024-01-01T00:30:00", 400);

            setCurrentMicros(parseMicros("2024-01-10T00:30:00.000000Z"));
            backdate("x", "2024-01-02T00:30:00", 400);

            // Compaction comes on only now. With it on from the start, housekeeping runs on every
            // commit of the fixture above and reclaims the waste as it is created, so there is nothing
            // left to observe and no ordering to check.
            enableCompaction();
            // Per-partition rules must not be what fires - only the table-wide one. The fixture's total
            // waste is a few KB, well under the table-wide floor's 50MB default, so that floor must be
            // lowered too or the percentage check never even runs.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_MAX_PIECES, "1000000");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_PERCENT, "20");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_MIN_SIZE, "1");

            final long deadBefore = deadRows("x");
            Assert.assertTrue("fixture produced no waste", deadBefore > 0);

            setCurrentMicros(parseMicros("2024-01-10T01:00:00.000000Z"));
            runCompactionPasses("x");

            Assert.assertTrue(
                    "the table-wide rule did not reduce the table's dead rows" +
                            " [before=" + deadBefore + ", after=" + deadRows("x") + ']',
                    deadRows("x") < deadBefore
            );
            Assert.assertEquals(
                    "the coldest day still holds dead rows, so the wrong partition was picked",
                    0,
                    deadRowsOfDay("x", "2024-01-01")
            );
        });
    }

    /**
     * TRIM-FILES is not implemented in this pass, so once compaction has reclaimed a partition's waste on
     * the first pass, there is nothing left for a later pass to shrink further - unlike the reference
     * design's staged MAKE-PLAIN/TRIM-FILES, where the file shortening is a separate, later step. This
     * fixture's stride sits mid-partition (~25% front), below MOVE-TAIL's default
     * {@code prefix.min.percent} (50%), so REWRITE is what reclaims the waste here, not MOVE-TAIL. Expected
     * to fail on the final "disk actually fell further" assertion.
     */
    @Test
    public void testTrimFilesWaitsForAReaderThatMappedTheOldExtent() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            enableCompaction();
            // A full-partition rewrite naturally crosses the table-wide dead-percent default (50%)
            // partway through the buildup below; keep the table-pressure rule out of the way so only
            // the waste-ratio rule (set after the buildup) is what fires.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_PERCENT, "99");

            createDayTable("x", "2024-01-01", 20_000);
            // Three rewrites: each merge-append here rewrites the WHOLE partition (no pre-split cuts
            // it), so two rounds leave dead just under the live count - three pushes past the ratio.
            // The tight ratio is set only now, after the buildup - see testWasteRatioTriggerReclaimsDeadRows.
            backdate("x", "2024-01-01T06:00:00", 200);
            backdate("x", "2024-01-01T06:00:00", 200);
            backdate("x", "2024-01-01T06:00:00", 200);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, "1");

            final TableToken tt = engine.verifyTableName("x");
            // First pass: REWRITE reclaims the waste (no MOVE-TAIL instalment plan in this port).
            runCompactionPasses("x");

            final long diskAfterFirstPass = diskSizeOfDay("x", "2024-01-01");
            try (TableReader mapped = engine.getReader(tt)) {
                // Touch the partition so the reader really maps it.
                Assert.assertTrue(mapped.size() >= 0);
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, "x order by ts", "x order by ts", LOG);

                runCompactionPasses("x");
                Assert.assertEquals(
                        "disk moved while a reader still had the partition mapped",
                        diskAfterFirstPass,
                        diskSizeOfDay("x", "2024-01-01")
                );
            }

            runCompactionPasses("x");
            Assert.assertTrue(
                    "a later pass shortened the files further, which this port cannot do without" +
                            " TRIM-FILES [diskBefore=" + diskAfterFirstPass +
                            ", diskAfter=" + diskSizeOfDay("x", "2024-01-01") + ']',
                    diskSizeOfDay("x", "2024-01-01") < diskAfterFirstPass
            );
        });
    }

    /**
     * The waste-ratio rule: dead rows must exceed a multiple of the live rows AND a minimum size.
     * Repeated merge-appends of one narrow stride leave a copy of that stride dead each time.
     */
    @Test
    public void testWasteRatioTriggerReclaimsDeadRows() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            // A full-partition rewrite naturally crosses the table-wide dead-percent default (50%)
            // partway through the buildup below; keep the table-pressure rule out of the way so only
            // the waste-ratio rule (set after the buildup) is what fires.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_PERCENT, "99");

            // A small partition churned hard: each rewrite of the 400-row stride abandons the
            // previous copy, so dead rows climb past 3x the live rows. The tight ratio is set only now,
            // after the buildup: with it in effect throughout, compaction reclaims the waste as it is
            // created and there is nothing left to observe.
            createDayTable("x", "2024-01-01", 600);
            for (int i = 0; i < 8; i++) {
                backdate("x", "2024-01-01T06:00:00", 400);
            }
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, "1");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1");

            final long deadBefore = deadRows("x");
            final long live = liveRows("x");
            Assert.assertTrue(
                    "fixture did not reach the ratio [dead=" + deadBefore + ", live=" + live + ']',
                    deadBefore > live
            );

            final String expected = fingerprintOfDay("x", "2024-01-01");
            enableCompaction();
            runCompactionPasses("x");

            Assert.assertEquals(
                    "the waste-ratio rule did not reclaim the dead rows" +
                            " [before=" + deadBefore + ", after=" + deadRows("x") + ']',
                    0,
                    deadRows("x")
            );
            Assert.assertEquals(
                    "compaction changed the data",
                    expected,
                    fingerprintOfDay("x", "2024-01-01")
            );
        });
    }

    private static void append(String table, String day, int rows) throws Exception {
        execute("insert into " + table + " select cast(x as int) + 900000 i," +
                " timestamp_sequence('" + day + "', 60*1000000L) ts from long_sequence(" + rows + ")");
        drainWalQueue();
    }

    /**
     * One narrow backdated stride. With merge-append on this rewrites the piece that owns the stride
     * at the shared file tail, abandoning its previous copy - which is exactly the dead space
     * compaction exists to reclaim.
     */
    private static void backdate(String table, String ts, int rows) throws Exception {
        execute("insert into " + table + " select cast(x as int) + 500000 i," +
                " timestamp_sequence('" + ts + "', 1000000L) ts from long_sequence(" + rows + ")");
        drainWalQueue();
    }

    private static void createDayTable(String table, String day, int rows) throws Exception {
        execute("create table " + table + " as (select cast(x as int) i," +
                " timestamp_sequence('" + day + "', 1000000L) ts" +
                " from long_sequence(" + rows + ")) timestamp(ts) partition by DAY WAL");
        drainWalQueue();
    }

    private static long deadRows(String table) throws Exception {
        return scalar("select coalesce(sum(deadRows), 0) deadRows from table_partitions('" + table + "')");
    }

    private static long deadRowsOfDay(String table, String day) throws Exception {
        return scalar("select coalesce(sum(deadRows), 0) deadRows from table_partitions('" + table + "')" +
                " where name like '" + day + "%'");
    }

    /** Disk of one day only - the housekeeping commits land in other partitions. */
    private static long diskSizeOfDay(String table, String day) throws Exception {
        return scalar("select coalesce(sum(diskSize), 0) d from table_partitions('" + table + "')" +
                " where name like '" + day + "%'");
    }

    // Compaction is always on now; kept as a no-op so call sites still document intent.
    private static void enableCompaction() {
    }

    private static void enableMergeAppend() {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
    }

    /**
     * Content fingerprint of ONE day, so the housekeeping commits do not move it. Sums {@code i} by
     * walking the row cursor in Java rather than with a SQL {@code sum()} - a vectorized aggregate over
     * a composite partition that has accumulated dead space triggers a pre-existing bug in this
     * branch's composite read path (see PARTITION_COMPACTION_state.md, "found, not fixed"): the SIMD
     * kernel scans past the live piece into dead space, or crashes outright. Row-by-row iteration goes
     * through the ordinary per-piece frame cursor instead, which is the read path
     * {@code O3CompositePartitionTest} already exercises and is not what this class is testing.
     */
    private static String fingerprintOfDay(String table, String day) throws Exception {
        final String sql = "select i from " + table + " where ts in '" + day + "'";
        long count = 0;
        long sum = 0;
        try (RecordCursorFactory f = select(sql)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                while (c.hasNext()) {
                    count++;
                    sum += c.getRecord().getInt(0);
                }
            }
        }
        return count + "/" + sum;
    }

    /** The {@code nameTxn} of the day's OWN (first, front) partition - unchanged by MOVE-TAIL. */
    private static long frontNameTxnOfDay(String table, String day) throws Exception {
        final TableToken tt = engine.verifyTableName(table);
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(parseMicros(day + "T00:00:00.000000Z"));
            Assert.assertTrue("day has no partition", partitionIndex > -1);
            return txReader.getPartitionNameTxn(partitionIndex);
        }
    }

    /**
     * Whether the day's own partition is composite: more than one piece, or dead space above the live
     * rows, or rows starting above file row 0.
     */
    private static boolean isComposite(String table, String day) throws Exception {
        final TableToken tt = engine.verifyTableName(table);
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(parseMicros(day + "T00:00:00.000000Z"));
            return partitionIndex > -1 && txReader.isPartitionComposite(partitionIndex);
        }
    }

    private static long liveRows(String table) throws Exception {
        return scalar("select coalesce(sum(numRows), 0) live from table_partitions('" + table + "')");
    }

    /** A fresh day, well clear of every fixture's own partitions. */
    private static String nextPassDay() {
        return "2024-03-" + String.format("%02d", 1 + (passDay++ % 28));
    }

    private static long parseMicros(String ts) throws Exception {
        return MicrosTimestampDriver.floor(ts);
    }

    /** How many {@code table_partitions()} rows the day has - more than one after a classic split. */
    private static long partitionCountOfDay(String table, String day) throws Exception {
        return scalar("select count() from table_partitions('" + table + "') where name like '" + day + "%'");
    }

    private static long physicallyWrittenRows() {
        return node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
    }

    /** Pieces of one day's own partition, or 0 if the day has no partition at all. */
    private static long pieceCountOfDay(String table, String day) throws Exception {
        final TableToken tt = engine.verifyTableName(table);
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(parseMicros(day + "T00:00:00.000000Z"));
            return partitionIndex > -1 ? reader.getGeometry().getPieceCount(partitionIndex) : 0;
        }
    }

    /**
     * Compaction runs inside {@code TableWriter.housekeep}, which fires once per commit. A few small
     * unrelated commits give it several chances to act - one per step, since each step of a
     * compaction is its own transaction.
     */
    private static long runCompactionPasses(String table) throws Exception {
        for (int i = 0; i < 6; i++) {
            // A day of its own per commit, and a day never reused by a later call: re-inserting the
            // same timestamps would be an O3 write into an existing partition, which rewrites it and
            // charges those rows to physicallyWrittenRows - so the row accounting below would be wrong
            // by however much that merge amplified rather than by the 12 rows actually inserted.
            execute("insert into " + table + " select cast(x as int) + 800000 + " + (passDay * 10) + " i," +
                    " timestamp_sequence('" + nextPassDay() + "', 60*1000000L) ts from long_sequence(2)");
            drainWalQueue();
        }
        engine.releaseInactive();
        return 12; // 6 commits x 2 rows, all into partitions of their own
    }

    private static long scalar(String sql) throws Exception {
        try (RecordCursorFactory f = select(sql)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                Assert.assertTrue("query returned no row: " + sql, c.hasNext());
                return c.getRecord().getLong(0);
            }
        }
    }
}
