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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionCompactionPolicy;
import io.questdb.cairo.PartitionCompactionScanJob;
import io.questdb.cairo.PartitionGeometryFile;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

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
 * chain. See PARTITION_COMPACTION.md for the rules it applies.
 * <p>
 * JOIN, MOVE-TAIL, MAKE-PLAIN, TRIM-FILES and REWRITE are all implemented (PARTITION_COMPACTION.md Sec.9
 * steps 0, 1, 2, 3, 4, 5). MOVE-TAIL is ported onto this branch's classic-split machinery (a new sibling
 * {@code attachedPartitions} entry for the tail) rather than the reference's hardlink/{@code partitionTop}
 * scheme. MAKE-PLAIN reclaims a MOVE-TAIL'd front's leftover dead space in place, gated on the same
 * {@link io.questdb.cairo.TxnScoreboard} reader check REWRITE never needs (it only ever appends).
 * TRIM-FILES (also step 4, the file-shortening half of the reference's in-place reclaim) folds into that
 * same MAKE-PLAIN commit and needs no reader wait of its own - a reader maps a folder's column files only
 * as far as its highest live piece reaches ({@code TableReader#mappedRowCount}), never up to the folder's
 * physical extent {@code E}, so the dead space above the single piece at row 0 is bytes no reader can
 * resolve a row in. MAKE-PLAIN's own check is what clears the readers that could still resolve an EARLIER
 * shape, with pieces higher up the files. A running CHECKPOINT is the one thing left that blocks the trim,
 * and it blocks the whole of MAKE-PLAIN: a folder that cannot be trimmed must not be recorded as plain, or
 * its dead bytes are left with nothing to report or reclaim them. REWRITE itself needs no reader gate at
 * all: it copies into a brand-new directory and leaves the old one for the ordinary purge to remove once no
 * reader still needs it, so a pinned reader's data stays correct with nothing to wait for - see
 * {@link #testRewriteLeavesAPinnedReadersDataIntact}.
 * <p>
 * JOIN has no dedicated test of its own here. {@code O3PartitionJob} folds list-and-file-adjacent pieces
 * INLINE, as part of the same commit that creates them (PARTITION_COMPACTION.md's "JOIN, inlined"),
 * so by the time a housekeeping pass reaches {@code TableWriter}'s own, separate JOIN sweep
 * ({@code foldContiguousPieces} / {@code foldFoldableFolders}), an ordinary sequence of merge-append
 * writes has never been observed to leave it anything to fold - the planner's own tail-extend
 * optimization (APPEND) and the fact that a relocated piece always lands at the physical tail keep
 * file-adjacency and list-adjacency from coinciding across separate commits. See the state doc for the
 * scenarios ruled out.
 */
public class O3PartitionCompactionTest extends AbstractCairoTest {
    private static final int LAST_GEOMETRY_GENERATION = 15;
    private static final String SPLIT_DAY = "2024-01-01";
    private static final int SPLIT_DAY_ROWS = 20_000;
    private static final String TYPED_COLUMNS = """
            rnd_boolean() c_bool,
            (x % 100)::byte c_byte,
            (x % 1_000)::short c_short,
            rnd_char() c_char,
            rnd_int(0, 1_000, 4) c_int,
            rnd_long(0, 100_000, 4) c_long,
            rnd_float(4) c_float,
            rnd_double(4) c_double,
            (CASE WHEN x % 5 = 0 THEN NULL ELSE x * 1_000 END)::date c_date,
            (CASE WHEN x % 5 = 0 THEN NULL ELSE x * 1_000 END)::timestamp c_micros,
            (CASE WHEN x % 5 = 0 THEN NULL ELSE x * 1_000 END)::timestamp_ns c_nanos,
            rnd_str(2, 30, 4) c_string,
            rnd_varchar(1, 40, 4) c_varchar,
            rnd_bin(4, 64, 4) c_binary,
            rnd_uuid4(4) c_uuid,
            rnd_ipv4() c_ipv4,
            rnd_long256() c_long256,
            rnd_geohash(4) c_geobyte,
            rnd_geohash(12) c_geoshort,
            rnd_geohash(25) c_geoint,
            rnd_geohash(50) c_geolong,
            rnd_decimal(2, 1, 4) c_decimal8,
            rnd_decimal(4, 2, 4) c_decimal16,
            rnd_decimal(9, 3, 4) c_decimal32,
            rnd_decimal(18, 4, 4) c_decimal64,
            rnd_decimal(38, 6, 4) c_decimal128,
            rnd_decimal(76, 8, 4) c_decimal256,
            rnd_double_array(1, 4) c_array,
            rnd_symbol('a', 'b', 'c', NULL) c_symbol,
            rnd_symbol('a', 'b', 'c', NULL) c_indexed,
            rnd_symbol('a', 'b', 'c', NULL) c_covering,
            (x % 1_000)::int c_to_long,
            rnd_str(2, 30, 4) c_to_varchar,
            """;

    @Before
    public void resetPassDays() {
        passDay = 0;
    }

    /**
     * Days the housekeeping commits consume, never reused within a test.
     */
    private static int passDay;

    /**
     * A running CHECKPOINT must not let TRIM-FILES shorten a live partition's column files: backup sizes those
     * files by the physical row extent its manifest copied out of the checkpoint but reads them from the LIVE db
     * root, so a trim under a checkpoint leaves the upload asking for more rows than the file holds.
     * <p>
     * {@link io.questdb.cairo.DatabaseCheckpointAgent} pins its reader's transaction on the
     * {@link io.questdb.cairo.TxnScoreboard}. That pin sits at or above the geometry record MAKE-PLAIN retires, so
     * the check at the top of MAKE-PLAIN lets it through and MAKE-PLAIN commits its first half: E lowered to the
     * live rows, the partition still composite. The re-check between that commit and TRIM-FILES then finds the
     * pin below the commit while a checkpoint runs, and declines - the files stay long, which is what the
     * checkpoint's E asks for, and the halfway shape is one {@code isMakePlainShape} selects again. The second
     * half of the test releases the checkpoint and asserts the partition then goes all the way.
     * <p>
     * Fixture of {@link #testMakePlainWaitsForAPinnedReaderThenReclaimsOnceItGoes} run past the reader's exit;
     * without the checkpoint it trims ({@link #testMakePlainReclaimsAMoveTailedFrontsDeadSpace}). The checkpoint
     * that starts INSIDE MAKE-PLAIN is {@link #testACheckpointStartingMidMakePlainDefersTrimFiles}.
     */
    @Test
    public void testACheckpointDefersTrimFiles() throws Exception {
        // CHECKPOINT CREATE calls sync(), which Windows does not have.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            enableCompaction();
            letPreSplitCut();
            setCurrentMicros(parseMicros("2024-01-10T00:00:00.000000Z"));
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

            createDayTable("x", "2024-01-01", 20_000);
            buildMoveTailWaste("x");
            pinPieceCap(2);

            // The pinned reader gets the day to MAKE-PLAIN's eligible shape, then goes: MOVE-TAIL has run,
            // MAKE-PLAIN has declined, and the checkpoint below is the only thing left holding anything.
            try (TableReader pinned = engine.getReader(engine.verifyTableName("x"))) {
                Assert.assertNotNull(pinned);
                runCompactionPasses("x");
                Assert.assertTrue("fixture did not reach MOVE-TAIL", isComposite("x", "2024-01-01"));
                Assert.assertEquals(1, pieceCountOfDay("x", "2024-01-01"));
                Assert.assertTrue("MOVE-TAIL left no dead space to protect", deadRowsOfDay("x", "2024-01-01") > 0);
            }
            engine.releaseInactive();

            final String before = fingerprintOfDay("x", "2024-01-01");
            final long diskBefore = diskSizeOfDay("x", "2024-01-01");
            final long deadBefore = deadRowsOfDay("x", "2024-01-01");

            execute("checkpoint create");
            try {
                // Clear the decline's backoff, so what happens next is the checkpoint's doing.
                setCurrentMicros(currentMicros + 2 * Micros.MINUTE_MICROS);
                runCompactionPasses("x");

                assertMakePlainHalfwayUnderCheckpoint("x", "2024-01-01", deadBefore, diskBefore);
            } finally {
                execute("checkpoint release");
            }

            assertMakePlainCompletesAfterCheckpoint("x", "2024-01-01", before, diskBefore);
        });
    }

    /**
     * The checkpoint of {@link #testACheckpointDefersTrimFiles}, but started INSIDE MAKE-PLAIN: after its top
     * check ran and before its first commit. A flag check at the top alone would miss it - the checkpoint's reader
     * opens at the pre-lowering txn and its {@code _txn} copy carries the larger E, then TRIM-FILES shortens the
     * live files that E sizes. The re-check between the commit and TRIM-FILES must catch the checkpoint's pin.
     * <p>
     * The hook is the geometry file open of MAKE-PLAIN's first commit ({@code lowerEToLiveRows} publishes a
     * record), which is strictly after the top check and strictly before the commit. The checkpoint runs on its
     * own thread, as it would in production, and this thread waits for it.
     */
    @Test
    public void testACheckpointStartingMidMakePlainDefersTrimFiles() throws Exception {
        // CHECKPOINT CREATE calls sync(), which Windows does not have.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        final AtomicBoolean armed = new AtomicBoolean();
        final AtomicInteger fired = new AtomicInteger();
        final AtomicReference<Throwable> checkpointFailure = new AtomicReference<>();
        // openRW is synchronized in the parent; the checkpoint thread needs that monitor for its own opens, so
        // the hook runs after super returned, outside it.
        final WindowsMappedTruncateFacade ff = new WindowsMappedTruncateFacade() {
            @Override
            public long openRW(LPSZ name, int opts) {
                final long fd = super.openRW(name, opts);
                if (armed.get() && Utf8s.containsAscii(name, TableUtils.PARTITION_GEOMETRY_FILE_NAME + '.') && fired.getAndIncrement() == 0) {
                    final Thread checkpoint = new Thread(() -> {
                        try {
                            engine.checkpointCreate(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER, false);
                        } catch (Throwable e) {
                            checkpointFailure.set(e);
                        }
                    }, "checkpoint-mid-make-plain");
                    checkpoint.start();
                    try {
                        checkpoint.join();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                return fd;
            }
        };
        assertMemoryLeak(ff, () -> {
            enableMergeAppend();
            enableCompaction();
            letPreSplitCut();
            setCurrentMicros(parseMicros("2024-01-10T00:00:00.000000Z"));
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

            createDayTable("x", "2024-01-01", 20_000);
            buildMoveTailWaste("x");
            pinPieceCap(2);

            try (TableReader pinned = engine.getReader(engine.verifyTableName("x"))) {
                Assert.assertNotNull(pinned);
                runCompactionPasses("x");
                Assert.assertTrue("fixture did not reach MOVE-TAIL", isComposite("x", "2024-01-01"));
                Assert.assertEquals(1, pieceCountOfDay("x", "2024-01-01"));
                Assert.assertTrue("MOVE-TAIL left no dead space to protect", deadRowsOfDay("x", "2024-01-01") > 0);
            }
            engine.releaseInactive();

            final String before = fingerprintOfDay("x", "2024-01-01");
            final long diskBefore = diskSizeOfDay("x", "2024-01-01");
            final long deadBefore = deadRowsOfDay("x", "2024-01-01");

            // Clear the decline's backoff, then let MAKE-PLAIN run into the checkpoint.
            setCurrentMicros(currentMicros + 2 * Micros.MINUTE_MICROS);
            armed.set(true);
            try {
                runCompactionPasses("x");
            } finally {
                armed.set(false);
            }
            Assert.assertEquals("the hook did not fire: MAKE-PLAIN did not publish a geometry record", 1, fired.get());
            if (checkpointFailure.get() != null) {
                throw new AssertionError("checkpoint create failed", checkpointFailure.get());
            }
            try {
                assertMakePlainHalfwayUnderCheckpoint("x", "2024-01-01", deadBefore, diskBefore);
            } finally {
                execute("checkpoint release");
            }

            assertMakePlainCompletesAfterCheckpoint("x", "2024-01-01", before, diskBefore);
        });
    }

    @Test
    public void testAgeTriggerCompactsAPartitionNothingHasWrittenTo() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            enableCompaction();
            // The age rule fires when a partition has not changed shape for this long AND it still has
            // waste or more than one piece.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "60m");
            // Waste/table-pressure alone must not be what fires here - only the age rule, once idle.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD_PERCENT, "99");

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
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            enableCompaction();

            createDayTable("x", "2024-01-01", 20_000);
            // A piece floor above the partition's own row count, so the pre-split never cuts it.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 20_000);
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
     * The per-commit compaction passes start at {@code compositeFromIndex()} - the first partition at or
     * after the earliest composite one - rather than at index 0, so they skip the cold plain partitions
     * below it. This lays down a run of plain partitions BELOW the only composite one and asserts the
     * composite is still found and reclaimed: a watermark that started the pass too late would walk
     * straight past it and leave the dead space on disk.
     */
    @Test
    public void testCompactionSkipsColdPlainsButStillReclaimsALaterComposite() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            enableCompaction();
            // Isolate the AGE rule, exactly as testAgeTriggerCompactsAPartitionNothingHasWrittenTo does:
            // waste and table-pressure off, so only "idle AND still has waste" can fire.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "60m");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD_PERCENT, "99");

            setCurrentMicros(parseMicros("2024-01-20T00:00:00.000000Z"));
            // A run of plain partitions with no dead space, all below the composite one to come.
            createDayTable("x", "2024-01-01", 20_000);
            append("x", "2024-01-02", 50);
            append("x", "2024-01-03", 50);
            append("x", "2024-01-04", 50);
            append("x", "2024-01-05", 50);
            Assert.assertEquals("the plain run must start clean", 0, deadRows("x"));

            // Make a LATER day composite: fill 2024-01-06, then two rewrites of one stride leave dead rows.
            execute("insert into x select cast(x as int) + 300000 i," +
                    " timestamp_sequence('2024-01-06', 1000000L) ts from long_sequence(20000)");
            drainWalQueue();
            backdate("x", "2024-01-06T06:00:00", 200);
            backdate("x", "2024-01-06T06:00:00", 200);
            Assert.assertTrue("fixture produced no waste on the later day", deadRowsOfDay("x", "2024-01-06") > 0);
            Assert.assertEquals("only the composite day should carry waste", deadRowsOfDay("x", "2024-01-06"), deadRows("x"));

            // Nothing writes to it for two hours of wall clock, then unrelated commits give housekeeping
            // its chances.
            setCurrentMicros(parseMicros("2024-01-20T02:00:00.000000Z"));
            runCompactionPasses("x");

            Assert.assertEquals(
                    "the watermark-bounded compaction pass walked past the only composite partition and left" +
                            " its dead space unreclaimed; dead rows still on disk: " + deadRows("x"),
                    0,
                    deadRows("x")
            );
        });
    }

    /**
     * When the last composite partition is reclaimed, the composite watermark resets to Long.MAX_VALUE, and
     * a composite that appears later - at a different position - re-narrows it and is still picked up.
     * Guards the reset-and-re-narrow path of {@code minCompositePartitionTimestamp}.
     */
    @Test
    public void testCompactionFindsAFreshCompositeAfterAllPriorOnesDrained() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            enableCompaction();
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "60m");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD_PERCENT, "99");

            setCurrentMicros(parseMicros("2024-01-20T00:00:00.000000Z"));
            createDayTable("x", "2024-01-01", 20_000);
            backdate("x", "2024-01-01T06:00:00", 200);
            backdate("x", "2024-01-01T06:00:00", 200);
            Assert.assertTrue("fixture produced no waste", deadRows("x") > 0);

            // Drain the first composite: it goes back to plain, dropping the composite count to zero and
            // resetting the watermark.
            setCurrentMicros(parseMicros("2024-01-20T02:00:00.000000Z"));
            runCompactionPasses("x");
            Assert.assertEquals("first composite not drained", 0, deadRows("x"));
            Assert.assertFalse("2024-01-01 still composite", isComposite("x", "2024-01-01"));

            // A brand-new composite on a much later day, after the watermark has reset to Long.MAX_VALUE.
            execute("insert into x select cast(x as int) + 300000 i," +
                    " timestamp_sequence('2024-01-15', 1000000L) ts from long_sequence(20000)");
            drainWalQueue();
            backdate("x", "2024-01-15T06:00:00", 200);
            backdate("x", "2024-01-15T06:00:00", 200);
            Assert.assertTrue("no waste on the fresh composite", deadRowsOfDay("x", "2024-01-15") > 0);

            setCurrentMicros(currentMicros + Micros.HOUR_MICROS + 2 * Micros.MINUTE_MICROS);
            runCompactionPasses("x");
            Assert.assertEquals(
                    "the fresh composite that appeared after the watermark reset was never compacted;" +
                            " dead rows: " + deadRows("x"),
                    0,
                    deadRows("x")
            );
        });
    }

    /**
     * TRIM-FILES runs while a reader that resolves the partition's CURRENT, one-piece composite shape is
     * open - and that reader still reads correct rows out of the shortened files.
     * <p>
     * This is what {@link io.questdb.cairo.TableReader#mappedRowCount} buys: a reader maps a composite
     * partition's column files only as far as its highest live piece reaches, not up to {@code E}. The dead
     * space above the single piece at row 0 is bytes no reader can resolve a row in, so cutting them off
     * needs no wait for the readers of that shape - only MAKE-PLAIN's own check, which clears the readers
     * that could still resolve an EARLIER shape with pieces higher up the files.
     * <p>
     * The column is VAR-SIZE on purpose. Sizing the mapping by {@code E} does not merely map past the end
     * of the aux file - it reads the aux entry for the last mapped row to size the DATA mapping, and that
     * entry now sits in the region TRIM-FILES cut, so it reads 0 and the data vector is mapped empty.
     * <p>
     * The reader here opens AFTER MOVE-TAIL, so it pins a transaction at or past the geometry record
     * MAKE-PLAIN reuses - outside the range MAKE-PLAIN checks, which is the point:
     * {@link #testMakePlainWaitsForAPinnedReaderThenReclaimsOnceItGoes} covers the reader that IS inside it.
     */
    @Test
    public void testMakePlainTrimsUnderAReaderPinnedAfterMoveTail() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            enableCompaction();
            letPreSplitCut();
            setCurrentMicros(parseMicros("2024-01-10T00:00:00.000000Z"));
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

            execute("create table y as (select cast(x as int) i, ('v' || x) s," +
                    " timestamp_sequence('2024-01-01', 1000000L) ts from long_sequence(20000))" +
                    " timestamp(ts) partition by DAY WAL");
            drainWalQueue();
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1T");
            for (int i = 0; i < 5; i++) {
                execute("insert into y select cast(x as int) + 500000 i, ('b' || x) s," +
                        " timestamp_sequence('2024-01-01T05:00:00', 1000000L) ts from long_sequence(200)");
                drainWalQueue();
            }
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            pinPieceCap(2);

            // A first pinned reader gets the day to MAKE-PLAIN's eligible shape - MOVE-TAIL runs, MAKE-PLAIN
            // declines - and then goes.
            try (TableReader warmup = engine.getReader(engine.verifyTableName("y"))) {
                Assert.assertNotNull(warmup);
                runCompactionPassesVarSize("y");
                Assert.assertTrue("fixture did not reach MOVE-TAIL", isComposite("y", "2024-01-01"));
                Assert.assertEquals(1, pieceCountOfDay("y", "2024-01-01"));
                Assert.assertTrue("MOVE-TAIL left no dead space to protect", deadRowsOfDay("y", "2024-01-01") > 0);
            }
            engine.releaseInactive();

            final String before = fingerprintOfDayVarSize("y", "2024-01-01");
            final long diskBefore = diskSizeOfDay("y", "2024-01-01");

            // Clear the backoff the decline above started, so what happens next is not suppressed.
            setCurrentMicros(currentMicros + 2 * Micros.MINUTE_MICROS);

            long count = 0;
            long intSum = 0;
            long strLenSum = 0;
            final long dayHi = parseMicros("2024-01-02T00:00:00.000000Z");
            engine.releaseInactive();
            try (TableReader pinned = engine.getReader(engine.verifyTableName("y"))) {
                // Pinned but with nothing mapped yet: this reader maps the day only AFTER the trim below,
                // while still resolving the one-piece composite record.
                runCompactionPassesVarSize("y");
                // Still composite in this reader's view, with E above the live row count and the files now
                // cut to the live row count: mapping to E here faults on the bytes past the end of the file.
                Assert.assertTrue(pinned.getTxFile().isPartitionComposite(0));
                Assert.assertTrue(pinned.getPartitionPhysicalRowCount(0) > pinned.getTxFile().getPartitionSize(0));
                pinned.openPartition(0);
            }
            try (RecordCursorFactory f = select("select i, s, ts from y order by ts desc")) {
                try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(c.hasNext());
                    Assert.assertFalse(
                            "a reader pinned past the geometry record MAKE-PLAIN reuses must not hold it up;" +
                                    " dead rows: " + deadRowsOfDay("y", "2024-01-01"),
                            isComposite("y", "2024-01-01")
                    );
                    Assert.assertTrue(
                            "TRIM-FILES did not reclaim the dead bytes [before=" + diskBefore
                                    + ", after=" + diskSizeOfDay("y", "2024-01-01") + ']',
                            diskSizeOfDay("y", "2024-01-01") < diskBefore
                    );

                    do {
                        if (c.getRecord().getTimestamp(2) >= dayHi) {
                            continue;
                        }
                        count++;
                        intSum += c.getRecord().getInt(0);
                        final CharSequence str = c.getRecord().getStrA(1);
                        strLenSum += str == null ? 0 : str.length();
                    } while (c.hasNext());
                }
            }
            Assert.assertEquals(
                    "the cursor read different rows across the trim",
                    before,
                    count + "/" + intSum + "/" + strLenSum
            );
            Assert.assertEquals("MAKE-PLAIN changed the data", before, fingerprintOfDayVarSize("y", "2024-01-01"));
        });
    }

    /**
     * Windows refuses to shorten a file that is still mapped. MAKE-PLAIN first commits the partition as composite with
     * E lowered to its live rows, then trims, then commits it plain. A refused trim declines MAKE-PLAIN after the first
     * commit: the partition stays composite in that halfway shape, which the next attempt picks up and trims again.
     * The refusal lands on {@code ts.d}, after {@code i.d} is already cut, so the test also proves the cut file still
     * covers the lowered E.
     */
    @Test
    public void testMakePlainDeclinesWhenTrimFilesFailsThenRetries() throws Exception {
        final AtomicBoolean isTrimRefused = new AtomicBoolean(true);
        final AtomicInteger refusedTrims = new AtomicInteger();
        final LongHashSet frontTsFds = new LongHashSet();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean close(long fd) {
                synchronized (frontTsFds) {
                    frontTsFds.remove(fd);
                }
                return super.close(fd);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                final long fd = super.openRW(name, opts);
                if (fd > -1 && Utf8s.containsAscii(name, Files.SEPARATOR + "2024-01-01") && Utf8s.endsWithAscii(name, Files.SEPARATOR + "ts.d")) {
                    synchronized (frontTsFds) {
                        frontTsFds.add(fd);
                    }
                }
                return fd;
            }

            @Override
            public boolean truncate(long fd, long size) {
                final boolean isFrontTs;
                synchronized (frontTsFds) {
                    isFrontTs = frontTsFds.contains(fd);
                }
                if (isFrontTs && isTrimRefused.get() && size < length(fd)) {
                    refusedTrims.incrementAndGet();
                    return false;
                }
                return super.truncate(fd, size);
            }
        };
        assertMemoryLeak(ff, () -> {
            enableMergeAppend();
            enableCompaction();
            letPreSplitCut();
            setCurrentMicros(parseMicros("2024-01-10T00:00:00.000000Z"));
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

            createDayTable("x", "2024-01-01", 20_000);
            buildMoveTailWaste("x");
            pinPieceCap(2);
            final String before = fingerprintOfDay("x", "2024-01-01");

            runCompactionPasses("x");
            Assert.assertTrue("TRIM-FILES was never refused", refusedTrims.get() > 0);
            Assert.assertTrue("a refused TRIM-FILES must keep the partition composite", isComposite("x", "2024-01-01"));
            Assert.assertEquals(1, pieceCountOfDay("x", "2024-01-01"));
            Assert.assertEquals("E must be lowered to the live rows", 0, deadRowsOfDay("x", "2024-01-01"));
            Assert.assertEquals("the declined MAKE-PLAIN changed the data", before, fingerprintOfDay("x", "2024-01-01"));
            assertFrontFileCoversPhysicalRows("x", "2024-01-01", "i", Integer.BYTES);
            final long diskAfterDecline = diskSizeOfDay("x", "2024-01-01");

            // Clears the decline's one-minute backoff.
            isTrimRefused.set(false);
            setCurrentMicros(currentMicros + 2 * Micros.MINUTE_MICROS);
            runCompactionPasses("x");
            Assert.assertFalse(
                    "the retry left the partition composite; dead rows: " + deadRowsOfDay("x", "2024-01-01"),
                    isComposite("x", "2024-01-01")
            );
            Assert.assertEquals(0, deadRowsOfDay("x", "2024-01-01"));
            final long diskAfterRetry = diskSizeOfDay("x", "2024-01-01");
            Assert.assertTrue(
                    "the retry did not trim [diskAfterDecline=" + diskAfterDecline + ", diskAfterRetry=" + diskAfterRetry + ']',
                    diskAfterRetry < diskAfterDecline
            );
            Assert.assertEquals("MAKE-PLAIN changed the data", before, fingerprintOfDay("x", "2024-01-01"));
        });
    }

    /**
     * A reader pinned AFTER the current geometry record was published must keep reading that record for as
     * long as it lives, across MAKE-PLAIN and the composite commits that follow it into the SAME directory.
     * <p>
     * {@link io.questdb.cairo.TableReader} takes its transaction without resolving geometry and reads
     * {@code _geometry} only when a query first needs a piece, so such a reader holds nothing but the
     * {@code (generation, offset)} reference its {@code _txn} snapshot carries. MAKE-PLAIN retires that
     * generation and the next composite commit restarts the chain at generation 0, offset 0 in the unchanged
     * directory - so without a guard covering readers pinned past the record, the recreated chain writes over
     * the very bytes the pinned reader has still to read, and record validation (structure plus checksum,
     * never identity) accepts the newer record in its place.
     * <p>
     * The sibling {@link #testMakePlainTrimsUnderAReaderPinnedAfterMoveTail} pins the same kind of reader but
     * lets it go before anything recreates the chain, and
     * {@link #testPassiveReaderReloadsGeometryAfterCompositePlainCompositeRefReuse} reloads its reader onto
     * the recreated chain; this one holds the snapshot open across both.
     */
    @Test
    public void testMakePlainKeepsAPinnedReadersSnapshotAcrossGeometryRecreation() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            enableCompaction();
            letPreSplitCut();
            setCurrentMicros(parseMicros("2024-01-10T00:00:00.000000Z"));
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

            execute("CREATE TABLE y AS (SELECT x::INT i, ('v' || x) s," +
                    " timestamp_sequence('2024-01-01', 1000000L) ts FROM long_sequence(20000))" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1T");
            for (int i = 0; i < 5; i++) {
                execute("INSERT INTO y SELECT x::INT + 500000, 'b' || x," +
                        " timestamp_sequence('2024-01-01T05:00:00', 1000000L) FROM long_sequence(200)");
                drainWalQueue();
            }
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            pinPieceCap(2);

            // A first pinned reader gets the day to a composite shape MAKE-PLAIN can act on, then goes.
            try (TableReader warmup = engine.getReader(engine.verifyTableName("y"))) {
                Assert.assertNotNull(warmup);
                runCompactionPassesVarSize("y");
            }
            engine.releaseInactive();
            Assert.assertTrue("fixture did not leave the day composite", isComposite("y", "2024-01-01"));

            final String before = fingerprintOfDayVarSize("y", "2024-01-01");
            // Clear the backoff the decline above started, so what happens next is not suppressed.
            setCurrentMicros(currentMicros + 2 * Micros.MINUTE_MICROS);
            engine.releaseAllReaders();

            final long nameTxnBefore = frontNameTxnOfDay("y", "2024-01-01");
            try (TableReader pinned = engine.getReader(engine.verifyTableName("y"))) {
                // Pinned past the current geometry record's publishing txn, and with nothing resolved yet.
                Assert.assertTrue("fixture must pin a composite day", pinned.getTxFile().isPartitionComposite(0));
                final long pinnedGeometryRef = pinned.getTxFile().getGeometryRef(0);

                runCompactionPassesVarSize("y");
                Assert.assertFalse("fixture did not reach MAKE-PLAIN", isComposite("y", "2024-01-01"));
                enableCompaction();
                letPreSplitCut();
                // Backdated commits make the day composite again, in the directory the pinned reader is
                // still reading, and grow the recreated chain well past the pinned reference's own offset.
                for (int i = 0; i < 20; i++) {
                    execute("INSERT INTO y SELECT x::INT + 700000, 'new' || x," +
                            " timestamp_sequence('2024-01-01T01:00:00', 1000000L) FROM long_sequence(200)");
                    drainWalQueue();
                }
                Assert.assertTrue("fixture did not make the day composite again", isComposite("y", "2024-01-01"));
                Assert.assertEquals(
                        "fixture must recreate the chain in the SAME directory",
                        nameTxnBefore,
                        frontNameTxnOfDay("y", "2024-01-01")
                );

                try (TestTableReaderRecordCursor c = new TestTableReaderRecordCursor().of(pinned)) {
                    Assert.assertEquals(
                            "the pinned reader's snapshot changed across MAKE-PLAIN and geometry recreation",
                            before,
                            fingerprintOfDayVarSize(c, "2024-01-02")
                    );
                }
                Assert.assertFalse(
                        "the recreated chain grew back over an offset the pinned reader can still resolve",
                        geometryChainReaches("y", "2024-01-01", pinnedGeometryRef)
                );
            }
        });
    }

    /**
     * MAKE-PLAIN, then a split removal that makes the front composite again through the same writer, with the purge
     * of the retired generation still pending: the new chain must not reuse the retired generation.
     */
    @Test
    public void testSplitRemovalAfterMakePlainOpensTheChainOnAFreshGeneration() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            final int retiredGeneration = makeFrontPlainNextToMoveTailSibling();
            final long frontNameTxn = frontNameTxnOfDay("x", SPLIT_DAY);
            final long siblingMinTs = siblingMinTimestamp();

            removeSibling(siblingMinTs);
            drainWalQueueHoldingOffMakePlain();
            assertNotSuspended();
            Assert.assertEquals("the carve moved the front", frontNameTxn, frontNameTxnOfDay("x", SPLIT_DAY));
            assertFrontChainStartsAt(retiredGeneration + 1);

            assertPurgeRemovesTheRetiredGeneration(retiredGeneration, retiredGeneration + 1);
            insertIntoFront();
            assertRowsOfSplitDay(siblingMinTs, true);
        });
    }

    @Test
    public void testSplitRemovalAfterMakePlainRecoversFromAFailedCarve() throws Exception {
        final DirectoryRefusingFacade facade = new DirectoryRefusingFacade(SPLIT_DAY + "T");
        assertMemoryLeak(facade, () -> {
            final int retiredGeneration = makeFrontPlainNextToMoveTailSibling();
            final long siblingMinTs = siblingMinTimestamp();

            facade.isArmed = true;
            removeSibling(siblingMinTs);
            drainWalQueueOnOneWriter();
            facade.isArmed = false;
            Assert.assertEquals("the carve did not create its split directory once", 1, facade.refusedCount.get());
            assertSuspended("could not open");
            Assert.assertFalse("the failed carve left the front composite", isComposite("x", SPLIT_DAY));
            Assert.assertEquals("the failed carve changed the day's partitions", 2, partitionCountOfDay("x", SPLIT_DAY));
            Assert.assertFalse("the failed carve wrote a geometry record", hasFrontGeometryFile(retiredGeneration + 1));

            execute("alter table x resume wal");
            drainWalQueueHoldingOffMakePlain();
            assertNotSuspended();
            assertFrontChainStartsAt(retiredGeneration + 1);

            assertPurgeRemovesTheRetiredGeneration(retiredGeneration, retiredGeneration + 1);
            Assert.assertFalse("the carves left a file past the live generation", hasFrontGeometryFile(retiredGeneration + 2));
            insertIntoFront();
            assertRowsOfSplitDay(siblingMinTs, true);
        });
    }

    @Test
    public void testSplitRemovalAfterMakePlainRecoversFromAFailedPublish() throws Exception {
        final GeometryWriteFailingFacade facade = new GeometryWriteFailingFacade();
        assertMemoryLeak(facade, () -> {
            final int retiredGeneration = makeFrontPlainNextToMoveTailSibling();
            final long siblingMinTs = siblingMinTimestamp();
            final String rowsBefore = fingerprintOfDay("x", SPLIT_DAY);

            facade.isArmed = true;
            removeSibling(siblingMinTs);
            drainWalQueueOnOneWriter();
            facade.isArmed = false;
            assertSuspended("could not append partition geometry");
            Assert.assertFalse("the failed publish left the front composite", isComposite("x", SPLIT_DAY));
            Assert.assertEquals("the failed publish changed the day's partitions", 2, partitionCountOfDay("x", SPLIT_DAY));
            // not assertQuery(): its engine.clear() drops the pending purge
            Assert.assertEquals("the failed publish changed the day's rows", rowsBefore, fingerprintOfDay("x", SPLIT_DAY));

            execute("alter table x resume wal");
            drainWalQueueHoldingOffMakePlain();
            assertNotSuspended();
            // the failed write left a whole record in the generation after the retired one
            assertFrontChainStartsAt(retiredGeneration + 2);

            assertPurgeRemovesTheRetiredGeneration(retiredGeneration, retiredGeneration + 2);
            insertIntoFront();
            assertRowsOfSplitDay(siblingMinTs, true);
        });
    }

    @Test
    public void testSplitRemovalIndexesOnlyTheRowsOfTheNewDirectory() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            for (String include : new String[]{"", " include (i)"}) {
                for (boolean isMergeAppend : new boolean[]{false, true}) {
                    for (boolean isSingleTimestampParent : new boolean[]{false, true}) {
                        for (boolean isLastPartition : new boolean[]{false, true}) {
                            final String shape = "include=" + include + ", mergeAppend=" + isMergeAppend
                                    + ", singleTimestampParent=" + isSingleTimestampParent + ", last=" + isLastPartition;
                            createClassicSplitDay(include, isSingleTimestampParent, isLastPartition, false);
                            setMergeAppend(isMergeAppend);

                            removeSibling(siblingMinTimestamp());
                            drainWalQueue();
                            assertNotSuspended();
                            assertPostingIndexMatchesLastDirectoryOfSplitDay(shape);
                            // i = 1, 6, .., 296
                            assertQuery("select count() c, sum(i) total from x where s = 's1' and ts in '" + SPLIT_DAY + "'")
                                    .noRandomAccess()
                                    .expectSize()
                                    .returns("c\ttotal\n60\t8910\n");

                            execute("insert into x select 1_000, 's1', timestamp_sequence('2024-01-01T12:00:00', 1_000_000L) from long_sequence(5)");
                            drainWalQueue();
                            engine.releaseAllReaders();
                            engine.releaseAllWriters();
                            assertNotSuspended();
                            assertPostingIndexMatchesLastDirectoryOfSplitDay(shape);
                            assertQuery("select count() c, sum(i) total from x where s = 's1' and ts in '" + SPLIT_DAY + "'")
                                    .noRandomAccess()
                                    .expectSize()
                                    .returns("c\ttotal\n65\t13910\n");
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSplitRemovalKeepsTheRowsOfAReadOnlyCompositeParentReadOnly() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            buildCompositeFrontNextToMoveTailSibling();
            final long siblingMinTs = siblingMinTimestamp();
            setFirstPartitionOfSplitDayReadOnly();

            removeSibling(siblingMinTs);
            drainWalQueueHoldingOffMakePlain();
            assertNotSuspended();
            Assert.assertEquals("the carve left no split", 2, partitionCountOfDay("x", SPLIT_DAY));
            assertSplitDayReadOnly("composite parent");

            insertIntoFront();
            assertNotSuspended();
            assertRowsOfSplitDay(siblingMinTs, false);
        });
    }

    @Test
    public void testSplitRemovalKeepsTheRowsOfAReadOnlyParentReadOnly() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            for (boolean isDedup : new boolean[]{false, true}) {
                for (boolean isMergeAppend : new boolean[]{false, true}) {
                    for (boolean isSingleTimestampParent : new boolean[]{false, true}) {
                        for (boolean isLastPartition : new boolean[]{false, true}) {
                            final String shape = "dedup=" + isDedup + ", mergeAppend=" + isMergeAppend
                                    + ", singleTimestampParent=" + isSingleTimestampParent + ", last=" + isLastPartition;
                            createClassicSplitDay("", isSingleTimestampParent, isLastPartition, isDedup);
                            setMergeAppend(isMergeAppend);
                            setFirstPartitionOfSplitDayReadOnly();
                            final long siblingMinTs = siblingMinTimestamp();
                            final String snapshotBefore = snapshot("select count() c, sum(i) s from x");
                            // nothing in the day can precede a single timestamp at its start
                            final boolean hasOutOfOrderWrite = !isSingleTimestampParent || !isLastPartition;
                            final boolean isSuspendedByTheWrite = hasOutOfOrderWrite && isDedup;

                            try (TableReader pinned = engine.getReader(engine.verifyTableName("x"))) {
                                removeSibling(siblingMinTs);
                                drainWalQueue();
                                assertNotSuspended();
                                Assert.assertEquals(shape, isSingleTimestampParent ? 1 : 2, partitionCountOfDay("x", SPLIT_DAY));
                                assertSplitDayReadOnly(shape);
                                if (isSingleTimestampParent) {
                                    assertFrontStampedWithTheLastSeqTxn(shape);
                                }

                                if (hasOutOfOrderWrite) {
                                    // inside the range of the directory that a carve of the parent creates
                                    execute("insert into x values (1_000, 's1', '2024-01-01T00:04:58.500000Z')");
                                    drainWalQueue();
                                }
                                if (isSuspendedByTheWrite) {
                                    assertSuspended("cannot insert into read-only partition");
                                } else {
                                    assertNotSuspended();
                                    execute("insert into x values (7, 's2', '2024-01-09T00:00:00')");
                                    drainWalQueue();
                                    assertNotSuspended();
                                }
                                Assert.assertEquals(shape, snapshotBefore, countAndSum(pinned));
                            }
                            engine.releaseAllReaders();
                            engine.releaseAllWriters();
                            if (!isSuspendedByTheWrite) {
                                assertQuery("select count() c, sum(i) s from x where ts in '2024-01-09'")
                                        .noRandomAccess()
                                        .expectSize()
                                        .returns("c\ts\n1\t7\n");
                            }
                            // 1 + 2 + ... + 300
                            assertQuery("select count() c, sum(i) s from x where ts in '" + SPLIT_DAY + "'")
                                    .noRandomAccess()
                                    .expectSize()
                                    .returns("c\ts\n300\t45150\n");
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSplitRemovalOfACompositeParentKeepsItsCommittedGenerationWhenItFails() throws Exception {
        final DirectoryRefusingFacade facade = new DirectoryRefusingFacade(SPLIT_DAY + "T");
        assertMemoryLeak(facade, () -> {
            buildCompositeFrontNextToMoveTailSibling();
            final long siblingMinTs = siblingMinTimestamp();
            final long committedRef = frontGeometryRef();
            final int generation = TxReader.geometryGeneration(committedRef);
            final long committedLength = frontGeometryFileLength(generation);

            facade.isArmed = true;
            removeSibling(siblingMinTs);
            drainWalQueueOnOneWriter();
            facade.isArmed = false;
            Assert.assertEquals("the carve did not create its split directory once", 1, facade.refusedCount.get());
            assertSuspended("could not open");
            Assert.assertEquals("the failed carve moved the committed ref", committedRef, frontGeometryRef());
            Assert.assertEquals("the failed carve touched the committed generation", committedLength, frontGeometryFileLength(generation));
            Assert.assertFalse("the failed carve opened a generation", hasFrontGeometryFile(generation + 1));
            assertRowsOfSplitDay(Long.MAX_VALUE, false);

            execute("alter table x resume wal");
            drainWalQueueOnOneWriter();
            assertNotSuspended();
            insertIntoFront();
            assertRowsOfSplitDay(siblingMinTs, true);
        });
    }

    @Test
    public void testSplitRemovalPreservesEveryColumnType() throws Exception {
        assertSplitRemovalPreservesTypedFront("timestamp", false, false);
    }

    @Test
    public void testSplitRemovalPreservesEveryColumnTypeNanos() throws Exception {
        assertSplitRemovalPreservesTypedFront("timestamp_ns", false, false);
    }

    @Test
    public void testSplitRemovalOfASingleTimestampParentPublishesNoGeometry() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            // a classic split: merge-append would leave the parent composite
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
            execute("create table x (i int, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into x select x::int, '2024-01-01'::timestamp from long_sequence(8_000)");
            execute("insert into x select x::int, timestamp_sequence('2024-01-01T03:00:00', 1_000_000L) from long_sequence(200)");
            execute("insert into x select x::int, timestamp_sequence('2024-01-05', 1_000_000L) from long_sequence(2)");
            drainWalQueue();
            execute("insert into x select x::int, timestamp_sequence('2024-01-01T03:00:00', 1_000_000L) from long_sequence(10)");
            drainWalQueue();
            Assert.assertEquals("fixture did not split the day", 2, partitionCountOfDay("x", SPLIT_DAY));
            final long frontNameTxn = frontNameTxnOfDay("x", SPLIT_DAY);
            enableMergeAppend();

            removeSibling(siblingMinTimestamp());
            drainWalQueue();
            assertNotSuspended();
            Assert.assertEquals("the day kept a split", 1, partitionCountOfDay("x", SPLIT_DAY));
            Assert.assertNotEquals("the parent kept its directory", frontNameTxn, frontNameTxnOfDay("x", SPLIT_DAY));
            Assert.assertFalse("the parent became composite", isComposite("x", SPLIT_DAY));
            Assert.assertFalse("a geometry record was published", hasFrontGeometryFile(0));
            assertFrontStampedWithTheLastSeqTxn("writable parent");
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                Assert.assertFalse("a writable parent became read-only", reader.getTxFile().isPartitionReadOnly(0));
            }
            // 1 + 2 + ... + 8000
            assertQuery("select count() c, sum(i) s from x where ts in '" + SPLIT_DAY + "'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\ts\n8000\t32004000\n");
        });
    }

    @Test
    public void testSplitRemovalOfTheLastPartition() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            final int retiredGeneration = makeFrontPlainNextToMoveTailSibling();
            execute("alter table x drop partition where ts >= '2024-03-01'");
            drainWalQueueOnOneWriter();
            final long siblingMinTs = siblingMinTimestamp();

            removeSibling(siblingMinTs);
            drainWalQueueHoldingOffMakePlain();
            assertNotSuspended();
            assertFrontChainStartsAt(retiredGeneration + 1);

            insertIntoFront();
            assertRowsOfSplitDay(siblingMinTs, true);
        });
    }

    @Test
    public void testSplitRemovalRewritePreservesEveryColumnType() throws Exception {
        assertSplitRemovalPreservesTypedFront("timestamp", false, true);
    }

    @Test
    public void testSplitRemovalRewritePreservesEveryColumnTypeNanos() throws Exception {
        assertSplitRemovalPreservesTypedFront("timestamp_ns", false, true);
    }

    @Test
    public void testSplitRemovalRewritePreservesEveryColumnTypeOfTheLastPartition() throws Exception {
        assertSplitRemovalPreservesTypedFront("timestamp", true, true);
    }

    @Test
    public void testSplitRemovalRecoversFromAFailedReseal() throws Exception {
        final SymbolReadRefusingFacade facade = new SymbolReadRefusingFacade(SPLIT_DAY + "T000458");
        assertMemoryLeak(facade, () -> {
            for (String include : new String[]{"", " include (i)"}) {
                for (boolean isMergeAppend : new boolean[]{false, true}) {
                    for (boolean isLastPartition : new boolean[]{false, true}) {
                        final String shape = "include=" + include + ", mergeAppend=" + isMergeAppend + ", last=" + isLastPartition;
                        createClassicSplitDay(include, false, isLastPartition, false);
                        setMergeAppend(isMergeAppend);

                        facade.refusedCount.set(0);
                        facade.isArmed = true;
                        removeSibling(siblingMinTimestamp());
                        drainWalQueue();
                        facade.isArmed = false;
                        Assert.assertEquals(shape + ": the reseal did not read the symbol once", 1, facade.refusedCount.get());
                        assertSuspended("could not open");
                        // the parent's 300 rows and the split's 400
                        assertQuery("select count() c, sum(i) total from x where ts in '" + SPLIT_DAY + "'")
                                .noRandomAccess()
                                .expectSize()
                                .returns("c\ttotal\n700\t1571650\n");

                        execute("alter table x resume wal");
                        drainWalQueue();
                        assertNotSuspended();
                        assertPostingIndexMatchesLastDirectoryOfSplitDay(shape);
                        // 1 + 2 + ... + 300
                        assertQuery("select count() c, sum(i) total from x where ts in '" + SPLIT_DAY + "'")
                                .noRandomAccess()
                                .expectSize()
                                .returns("c\ttotal\n300\t45150\n");
                    }
                }
            }
        });
    }

    @Test
    public void testSplitRemovalRepeatedOverAReadOnlyParentKeepsItsRows() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            for (boolean isMergeAppend : new boolean[]{false, true}) {
                for (boolean isSingleTimestampParent : new boolean[]{false, true}) {
                    for (boolean isLastPartition : new boolean[]{false, true}) {
                        for (boolean isSameRange : new boolean[]{false, true}) {
                            createClassicSplitDay("", isSingleTimestampParent, isLastPartition, false);
                            setMergeAppend(isMergeAppend);
                            setFirstPartitionOfSplitDayReadOnly();
                            final long siblingMinTs = siblingMinTimestamp();
                            removeSibling(siblingMinTs);
                            drainWalQueue();
                            assertNotSuspended();

                            // either the same range again, or the one of the directory that now ends the day
                            removeSibling(isSameRange ? siblingMinTs : siblingMinTimestamp());
                            drainWalQueue();
                            assertNotSuspended();
                            assertSplitDayReadOnly("the repeated removal");
                            // 1 + 2 + ... + 300
                            assertQuery("select count() c, sum(i) s from x where ts in '" + SPLIT_DAY + "'")
                                    .noRandomAccess()
                                    .expectSize()
                                    .returns("c\ts\n300\t45150\n");

                            execute("insert into x values (7, 's2', '2024-01-09T00:00:00')");
                            drainWalQueue();
                            assertNotSuspended();
                            assertQuery("select count() c, sum(i) s from x where ts in '2024-01-09'")
                                    .noRandomAccess()
                                    .expectSize()
                                    .returns("c\ts\n1\t7\n");
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSplitRemovalRewriteKeepsReadOnlyAndStampsTheSeqTxn() throws Exception {
        assertSplitRemovalRewriteKeepsReadOnlyAndStampsTheSeqTxn(false);
    }

    @Test
    public void testSplitRemovalRewriteOfACompositeParentKeepsReadOnlyAndStampsTheSeqTxn() throws Exception {
        assertSplitRemovalRewriteKeepsReadOnlyAndStampsTheSeqTxn(true);
    }

    @Test
    public void testSplitRemovalCarvesACutCompositeParentOnlyIntoRoomForItsRecord() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            for (boolean isMergeAppend : new boolean[]{false, true}) {
                for (int roomForPieces = 1; roomForPieces < 3; roomForPieces++) {
                    final String shape = "isMergeAppend=" + isMergeAppend + ", room=" + roomForPieces;
                    dropTable();
                    buildCompositeFrontNextToMoveTailSibling();
                    final long frontNameTxn = frontNameTxnOfDay("x", SPLIT_DAY);
                    final long siblingMinTs = siblingMinTimestamp();
                    plantCutFront(roomForPieces);
                    node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, Boolean.toString(isMergeAppend));

                    removeSibling(siblingMinTs);
                    drainWalQueueOnOneWriter();
                    assertNotSuspended();
                    final boolean isRewritten = roomForPieces < 2;
                    Assert.assertEquals(shape, isRewritten, frontNameTxn != frontNameTxnOfDay("x", SPLIT_DAY));
                    Assert.assertEquals(shape, !isRewritten, isComposite("x", SPLIT_DAY));
                    assertRowsOfSplitDay(siblingMinTs, false);
                }
            }
        });
    }

    @Test
    public void testSquashPartitionsFoldsACutFront() throws Exception {
        assertSquashPartitionsFoldsTheFront(true);
    }

    @Test
    public void testSquashPartitionsFoldsAOnePieceFront() throws Exception {
        assertSquashPartitionsFoldsTheFront(false);
    }

    @Test
    public void testSplitRemovalRewriteRecoversFromAFailure() throws Exception {
        final DirectoryRefusingFacade facade = new DirectoryRefusingFacade(SPLIT_DAY + ".");
        assertMemoryLeak(facade, () -> {
            final int retiredGeneration = makeFrontPlainNextToMoveTailSibling();
            final long frontNameTxn = frontNameTxnOfDay("x", SPLIT_DAY);
            final long siblingMinTs = siblingMinTimestamp();
            holdEveryGenerationOfFront(retiredGeneration);

            facade.isArmed = true;
            removeSibling(siblingMinTs);
            drainWalQueueOnOneWriter();
            facade.isArmed = false;
            Assert.assertEquals("the rewrite did not create its directory once", 1, facade.refusedCount.get());
            assertSuspended("could not create directories");
            Assert.assertEquals("the failed rewrite moved the front", frontNameTxn, frontNameTxnOfDay("x", SPLIT_DAY));
            assertRowsOfSplitDay(Long.MAX_VALUE, false);

            execute("alter table x resume wal");
            drainWalQueueOnOneWriter();
            assertNotSuspended();
            Assert.assertNotEquals("the retry did not rewrite the front", frontNameTxn, frontNameTxnOfDay("x", SPLIT_DAY));
            insertIntoFront();
            assertRowsOfSplitDay(siblingMinTs, true);
        });
    }

    @Test
    public void testSplitRemovalWithMergeAppendOffCarvesDespiteEveryGenerationHeld() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            final int retiredGeneration = makeFrontPlainNextToMoveTailSibling();
            final long frontNameTxn = frontNameTxnOfDay("x", SPLIT_DAY);
            final long siblingMinTs = siblingMinTimestamp();
            holdEveryGenerationOfFront(retiredGeneration);
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");

            removeSibling(siblingMinTs);
            drainWalQueueOnOneWriter();
            assertNotSuspended();
            Assert.assertEquals("the carve rewrote the front", frontNameTxn, frontNameTxnOfDay("x", SPLIT_DAY));
            Assert.assertFalse("the carve recorded the front as composite", isComposite("x", SPLIT_DAY));
            Assert.assertEquals("the carve did not split the front's last line off", 2, partitionCountOfDay("x", SPLIT_DAY));

            insertIntoFront();
            assertRowsOfSplitDay(siblingMinTs, true);
        });
    }

    /**
     * An incremental backup treats a plain partition as unchanged while its name txn, row count and squash counter
     * all match the archived copy. MOVE-TAIL starts the tail sibling at its first row, not one past the front's last,
     * so rows timestamped in between still route to the front and append to it in place after MAKE-PLAIN trims it.
     * Refilling exactly the rows MOVE-TAIL took away returns the front to its original name txn and row count with
     * different rows, so MAKE-PLAIN must bump the squash counter or the backup reuses the stale archive.
     */
    @Test
    public void testMakePlainBumpsSquashCounterSoARefilledFrontLooksChanged() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            enableCompaction();
            letPreSplitCut();
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

            // 20000 rows one second apart: the front's last row before 05:00 is row 17999 at 04:59:59.
            createDayTable("x", "2024-01-01", 20_000);
            Assert.assertFalse(isComposite("x", "2024-01-01"));
            final long nameTxnBefore = frontNameTxnOfDay("x", "2024-01-01");
            final long squashBefore = frontSquashCountOfDay("x", "2024-01-01");

            buildMoveTailWaste("x");
            pinPieceCap(2);
            runCompactionPasses("x");
            Assert.assertFalse("MAKE-PLAIN did not follow MOVE-TAIL", isComposite("x", "2024-01-01"));
            Assert.assertEquals(18_000, frontRowCountOfDay("x", "2024-01-01"));

            // The gap between the front's last row and the tail's first: these rows land in the front, in place.
            execute("insert into x select cast(x as int) + 600000 i," +
                    " timestamp_sequence('2024-01-01T04:59:59.000001', 1L) ts from long_sequence(2000)");
            drainWalQueue();

            Assert.assertFalse(isComposite("x", "2024-01-01"));
            Assert.assertEquals("fixture no longer refills the front in place", nameTxnBefore, frontNameTxnOfDay("x", "2024-01-01"));
            Assert.assertEquals("fixture no longer refills the front in place", 20_000, frontRowCountOfDay("x", "2024-01-01"));
            Assert.assertNotEquals(
                    "front has the archived name txn and row count with different rows, but the same squash counter",
                    squashBefore,
                    frontSquashCountOfDay("x", "2024-01-01")
            );
        });
    }

    /**
     * MAKE-PLAIN's own success path: a successful MOVE-TAIL is immediately followed, in the same
     * housekeeping pass, by an attempt at MAKE-PLAIN on the front it just left behind - the front is
     * exactly MAKE-PLAIN's own eligible shape (one piece, row 0, dead space above it), and with no reader
     * in the way there is nothing to wait for. {@code nameTxn} unchanged is proof this is bookkeeping and
     * not a REWRITE in disguise; TRIM-FILES then physically shortens the front's files down to the live
     * row count, behind its own reader wait one transaction later - which clears immediately here, as
     * nothing is pinned.
     */
    @Test
    public void testJoinFoldsACutFrontOnlyIntoRoomForItsRecord() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            for (int roomForPieces = 0; roomForPieces < 2; roomForPieces++) {
                dropTable();
                buildCompositeFrontNextToMoveTailSibling();
                plantCutFront(roomForPieces);
                final long plantedRef = frontGeometryRef();

                runCompactionPassesOnOneWriter("x");
                assertNotSuspended();
                final boolean isJoined = roomForPieces == 1;
                Assert.assertEquals("room=" + roomForPieces, isJoined ? 1 : 2, pieceCountOfDay("x", SPLIT_DAY));
                Assert.assertEquals("room=" + roomForPieces, isJoined, plantedRef != frontGeometryRef());
                assertRowsOfSplitDay(Long.MAX_VALUE, false);
            }
        });
    }

    @Test
    public void testMakePlainStepsAsideForAFrontWithNoGenerationLeft() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            buildCompositeFrontNextToMoveTailSibling();
            final long frontNameTxn = frontNameTxnOfDay("x", SPLIT_DAY);
            NearFullGeometryFixture.plantCommittedRecord(engine, "x", parseMicros(SPLIT_DAY + "T00:00:00.000000Z"), LAST_GEOMETRY_GENERATION);
            final long plantedRef = frontGeometryRef();

            setCurrentMicros(currentMicros + 2 * Micros.MINUTE_MICROS);
            runCompactionPassesOnOneWriter("x");
            assertNotSuspended();
            Assert.assertEquals("MAKE-PLAIN published into the front", plantedRef, frontGeometryRef());
            Assert.assertEquals("compaction moved the front", frontNameTxn, frontNameTxnOfDay("x", SPLIT_DAY));
            assertRowsOfSplitDay(Long.MAX_VALUE, false);

            insertIntoFront();
            Assert.assertNotEquals("the write did not assemble a fresh front", frontNameTxn, frontNameTxnOfDay("x", SPLIT_DAY));
            Assert.assertFalse("the fresh front is composite", isComposite("x", SPLIT_DAY));
            assertRowsOfSplitDay(Long.MAX_VALUE, true);
        });
    }

    @Test
    public void testMakePlainReclaimsAMoveTailedFrontsDeadSpace() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            // Piece count off during the buildup below - the effective cap is liveRows / avg.rows.piece.lim
            // with no flat floor any more, so a small fixture could otherwise trip it well before the
            // explicit trigger set after the buildup.
            enableCompaction();
            // The fixture below needs a partition of several pieces, which only the pre-split makes.
            letPreSplitCut();
            // Same fixture shape as testMoveTailCopiesTheTailNotTheWholePartition: a huge clean front
            // with a small, repeatedly-relocated stride pre-split into its own tail pieces, so MOVE-TAIL
            // (not REWRITE) is what the piece-count rule triggers.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

            createDayTable("x", "2024-01-01", 20_000);
            buildMoveTailWaste("x");

            final String expected = fingerprintOfDay("x", "2024-01-01");
            final long frontNameTxnBefore = frontNameTxnOfDay("x", "2024-01-01");
            final long diskBefore = diskSizeOfFront("x", "2024-01-01");

            pinPieceCap(2);
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
            final long diskAfter = diskSizeOfFront("x", "2024-01-01");
            Assert.assertTrue(
                    "TRIM-FILES did not shrink the front's files after MAKE-PLAIN [diskBefore=" + diskBefore +
                            ", diskAfter=" + diskAfter + ']',
                    diskAfter < diskBefore
            );
        });
    }

    /**
     * Same shape and trigger as {@link #testMakePlainReclaimsAMoveTailedFrontsDeadSpace}, but with a
     * var-size column in the mix: TRIM-FILES has to shorten both the aux (offset) file and the data file
     * for it, and the data file's target length depends on reading an entry out of the aux file before
     * that file is itself shortened - the two-file coordination the fixed-column-only fixture above
     * cannot exercise.
     */
    @Test
    public void testMakePlainTrimsVarSizeColumnFiles() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            // Piece count off during the buildup below - the effective cap is liveRows / avg.rows.piece.lim
            // with no flat floor any more, so a small fixture could otherwise trip it well before the
            // explicit trigger set after the buildup.
            enableCompaction();
            // The fixture below needs a partition of several pieces, which only the pre-split makes.
            letPreSplitCut();
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

            execute("create table y as (select cast(x as int) i, ('v' || x) s," +
                    " timestamp_sequence('2024-01-01', 1000000L) ts from long_sequence(20000))" +
                    " timestamp(ts) partition by DAY WAL");
            drainWalQueue();
            for (int i = 0; i < 3; i++) {
                execute("insert into y select cast(x as int) + 500000 i, ('b' || x) s," +
                        " timestamp_sequence('2024-01-01T05:00:00', 1000000L) ts from long_sequence(200)");
                drainWalQueue();
            }

            final String expected = fingerprintOfDayVarSize("y", "2024-01-01");
            final long diskBefore = diskSizeOfFront("y", "2024-01-01");

            pinPieceCap(2);
            for (int i = 0; i < 6; i++) {
                execute("insert into y (i, s, ts) select cast(x as int) + 800000, null," +
                        " timestamp_sequence('" + nextPassDay() + "', 60*1000000L) from long_sequence(2)");
                drainWalQueue();
            }
            engine.releaseInactive();

            Assert.assertFalse(
                    "MAKE-PLAIN did not follow MOVE-TAIL in the same housekeeping pass",
                    isComposite("y", "2024-01-01")
            );
            Assert.assertEquals(
                    "MAKE-PLAIN did not reclaim the dead space MOVE-TAIL left above the front's one piece",
                    0,
                    deadRowsOfDay("y", "2024-01-01")
            );
            final long diskAfter = diskSizeOfFront("y", "2024-01-01");
            Assert.assertTrue(
                    "TRIM-FILES did not shrink the front's var-size column files after MAKE-PLAIN" +
                            " [diskBefore=" + diskBefore + ", diskAfter=" + diskAfter + ']',
                    diskAfter < diskBefore
            );
            Assert.assertEquals(
                    "TRIM-FILES corrupted the var-size column's data",
                    expected,
                    fingerprintOfDayVarSize("y", "2024-01-01")
            );
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
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            // Piece count off during the buildup below - the effective cap is liveRows / avg.rows.piece.lim
            // with no flat floor any more, so a small fixture could otherwise trip it well before the
            // explicit trigger set after the buildup.
            enableCompaction();
            // The fixture below needs a partition of several pieces, which only the pre-split makes.
            letPreSplitCut();
            setCurrentMicros(parseMicros("2024-01-10T00:00:00.000000Z"));
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

            createDayTable("x", "2024-01-01", 20_000);
            buildMoveTailWaste("x");
            pinPieceCap(2);

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
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            // Piece count off during the buildup below - the effective cap is liveRows / avg.rows.piece.lim
            // with no flat floor any more, so a small fixture could otherwise trip it well before the
            // explicit trigger set after the buildup.
            enableCompaction();
            // The fixture below needs a partition of several pieces, which only the pre-split makes.
            letPreSplitCut();
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
            // Build >10% waste without letting ingestion-driven MOVE-TAIL run during fixture construction.
            buildMoveTailWaste("x");

            final long deadBefore = deadRowsOfDay("x", "2024-01-01");
            Assert.assertTrue("fixture produced no waste", deadBefore > 0);
            final long partitionsBefore = partitionCountOfDay("x", "2024-01-01");
            final long frontNameTxnBefore = frontNameTxnOfDay("x", "2024-01-01");

            pinPieceCap(2);
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
     * MAKE-PLAIN's clampColumnTopsToLiveRows lowers the day's column tops in place (1000 to 264 here) under the
     * SAME column name txn. The reader's reconcileOpenPartitions0 keys its "reload in place" decision on
     * {@code ColumnVersionReader.getMaxPartitionVersion}, which folds name txns only, so it takes the
     * reloadColumnFiles path - which used to size the mapping by the top the column was opened with. The squash
     * then grows the day back to 1126 rows; the reader mapped 126 rows at file offset 0 and kept reading with top
     * 1000. reloadColumnFiles now refreshes each column's top from {@code _cv} whenever the column version moved.
     */
    @Test
    public void testLongLivedReaderSeesLateAddedColumnsAfterMakePlainAndSquash() throws Exception {
        assertMemoryLeak(() -> {
            enableMergeAppend();
            enableCompaction();
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");

            execute("create table x as (select cast(x as int) i, rnd_symbol('DE', null, 'EF', 'FG') sym2," +
                    " timestamp_sequence('2024-01-01', 1000000L) ts from long_sequence(1000))," +
                    " index(sym2 TYPE POSTING DELTA) timestamp(ts) partition by DAY WAL");
            drainWalQueue();
            // Still the last partition, so each top lands at the row count: 1000.
            execute("alter table x add column v long");
            execute("alter table x add column sym_top symbol index");
            execute("alter table x add column ip4 ipv4");
            execute("alter table x add column var_top varchar");
            drainWalQueue();
            // Rows past the original span, carrying the late-added columns.
            execute("insert into x (i, sym2, ts, v, sym_top, ip4, var_top) select cast(x as int) + 100000, 'DE'," +
                    " timestamp_sequence('2024-01-01T19:00:00', 1000000L), x, 'NGW', '1.2.3.4', 'abc' from long_sequence(59)");
            drainWalQueue();
            // A replace-range commit over [00:04:24, 00:13:13) with 529 rows: leaves a clean front [0, 264)
            // at row 0 and relocates the rest, the 59 rows above included, to the tail. Three times over,
            // so the dead space outweighs the tail and MOVE-TAIL is worth its copy.
            final TableToken xt = engine.verifyTableName("x");
            for (int round = 0; round < 3; round++) {
                try (WalWriter ww = engine.getWalWriter(xt)) {
                    final long lo = MicrosTimestampDriver.floor("2024-01-01T00:04:24.000000Z");
                    for (int k = 0; k < 529; k++) {
                        final TableWriter.Row r = ww.newRow(lo + k * 1_000_000L);
                        r.putInt(0, 200_000 + k);
                        r.append();
                    }
                    ww.commitWithParams(lo, MicrosTimestampDriver.floor("2024-01-01T00:13:13.000000Z"), WAL_DEDUP_MODE_REPLACE_RANGE);
                }
                drainWalQueue();
            }
            // More rows carrying the late-added columns, merged into the relocated tail.
            execute("insert into x (i, sym2, ts, v, sym_top, ip4, var_top) select cast(x as int) + 300000, 'EF'," +
                    " timestamp_sequence('2024-01-01T17:30:00', 1000000L), x + 1000, 'ABC', '5.6.7.8', 'xyz' from long_sequence(67)");
            drainWalQueue();
            // A later day, so 2024-01-01 is not the last partition when compaction reaches it.
            execute("insert into x (i, ts) select cast(x as int) + 400000, timestamp_sequence('2024-01-02', 60*1000000L) from long_sequence(5)");
            drainWalQueue();

            final String query = "select * from x where ts in '2024-01-01'";
            final String expected = snapshot(query);
            Assert.assertTrue("fixture produced no composite partition", isComposite("x", "2024-01-01"));

            pinPieceCap(1);
            // The pooled reader stays open across the whole compaction, as a live view's does: the base
            // drainWalQueue skips releaseInactive(), and a query after every commit reloads it in place.
            for (int i = 0; i < 3; i++) {
                execute("insert into x (i, ts) select cast(x as int) + 800000, timestamp_sequence('2024-03-0" + (i + 1) + "', 60*1000000L) from long_sequence(2)");
            }
            AbstractCairoTest.drainWalQueue();
            snapshot(query);
            for (int i = 0; i < 6; i++) {
                execute("insert into x (i, ts) select cast(x as int) + 900000, timestamp_sequence('" + nextPassDay() + "', 60*1000000L) from long_sequence(2)");
                AbstractCairoTest.drainWalQueue();
                snapshot(query);
            }
            Assert.assertFalse("MAKE-PLAIN did not run, the fixture proves nothing", isComposite("x", "2024-01-01"));

            final String actual = snapshot(query);
            try (TableReader r = engine.getReader(xt)) {
                final int vIdx = r.getMetadata().getColumnIndex("v");
                Assert.assertEquals(
                        "the long-lived reader kept the column top MAKE-PLAIN lowered",
                        r.getColumnVersionReader().getColumnTop(parseMicros("2024-01-01T00:00:00.000000Z"), vIdx),
                        r.getColumnTop(r.getColumnBase(0), vIdx)
                );
            }
            Assert.assertEquals("the long-lived reader lost the late-added columns' values", expected, actual);
            engine.releaseAllReaders();
            Assert.assertEquals("a fresh reader disagrees with the pre-compaction data", expected, snapshot(query));
        });
    }

    /**
     * A passive reader that already resolved one composite generation must forget it when reload observes
     * the same directory and geometry reference reused after the partition went plain and composite again.
     */
    @Test
    public void testPassiveReaderReloadsGeometryAfterCompositePlainCompositeRefReuse() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            enableCompaction();
            letPreSplitCut();
            setCurrentMicros(parseMicros("2024-01-10T00:00:00.000000Z"));
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

            createDayTable("x", "2024-01-01", 20_000);
            backdate("x", "2024-01-01T05:00:00", 200);
            final TableToken tableToken = engine.verifyTableName("x");
            try (TableReader reused = engine.getReader(tableToken)) {
                Assert.assertTrue(reused.getTxFile().isPartitionComposite(0));
                final long geometryRef = reused.getTxFile().getGeometryRef(0);
                final long nameTxn = reused.getTxFile().getPartitionNameTxn(0);
                final long oldWriterTxn = reused.getGeometry().getWriterTxn(0);

                reused.goPassive();
                buildMoveTailWaste("x");
                pinPieceCap(2);
                runCompactionPasses("x");
                Assert.assertFalse("fixture must reach plain", isComposite("x", "2024-01-01"));
                Assert.assertEquals("fixture must retain directory", nameTxn, frontNameTxnOfDay("x", "2024-01-01"));
                // The retired generation's file has to go before a later composite commit can legitimately
                // restart the chain at generation 0 and reuse the reference: PartitionGeometry.publish opens
                // a fresh chain on the first generation with NO file on disk, precisely so a recreation
                // cannot write over a record a pinned reader still resolves. This reader is passive, so it
                // holds nothing up and the purge job clears generation 0 for reuse.
                runColumnPurgeJob();

                enableCompaction();
                letPreSplitCut();
                backdate("x", "2024-01-01T01:00:00", 200);
                try (TableReader fresh = engine.getReader(tableToken)) {
                    Assert.assertTrue(fresh.getTxFile().isPartitionComposite(0));
                    Assert.assertEquals("fixture must reuse directory", nameTxn, fresh.getTxFile().getPartitionNameTxn(0));
                    Assert.assertEquals("fixture must reuse ref", geometryRef, fresh.getTxFile().getGeometryRef(0));
                    Assert.assertTrue("fresh reader must resolve a newer geometry", fresh.getGeometry().getWriterTxn(0) > oldWriterTxn);

                    reused.reload();
                    try (
                            TestTableReaderRecordCursor expected = new TestTableReaderRecordCursor().of(fresh);
                            TestTableReaderRecordCursor actual = new TestTableReaderRecordCursor().of(reused)
                    ) {
                        TestUtils.assertEquals(expected, fresh.getMetadata(), actual, reused.getMetadata(), true);
                    }
                }
            }
        });
    }

    /**
     * The piece-count rule is a backstop for a partition that has been cut many times. It must reduce
     * the pieces of one partition even when very little space is wasted.
     */
    @Test
    public void testPieceCountTriggerReducesTheNumberOfPieces() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            // Piece count off during the buildup below - the effective cap is liveRows / avg.rows.piece.lim
            // with no flat floor any more, so a small fixture could otherwise trip it well before the
            // explicit trigger set after the buildup.
            enableCompaction();
            // The fixture below needs a partition of several pieces, which only the pre-split makes.
            letPreSplitCut();
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

            final long piecesBefore = pieceCountOfDay("x", "2024-01-01");
            Assert.assertTrue(
                    "fixture did not produce enough pieces to trip the rule [pieces=" + piecesBefore + ']',
                    piecesBefore > 4
            );

            pinPieceCap(4);
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
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            enableCompaction();
            // A full-partition rewrite naturally crosses the table-wide dead-percent default (50%)
            // partway through the buildup below; keep the table-pressure rule out of the way so only
            // the waste-ratio rule (set after the buildup) is what fires.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD_PERCENT, "99");

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
     * The background sweep already picks this partition up - it is composite and idle, so it is a candidate
     * either way. What changes is what it does with it: the partition is already one piece at row 0 with
     * dead space above it, so MAKE-PLAIN and TRIM-FILES reach REWRITE's result IN PLACE, with no copy and
     * no new directory. The unchanged {@code nameTxn} below is what proves the sweep took that route.
     * <p>
     * This is the case the writer's own per-commit path cannot finish on its own: it deferred MAKE-PLAIN
     * for a reader, and then ingestion stopped, so nothing ever commits to that table again and the retry
     * never comes round.
     */
    @Test
    public void testTheCompactionSweepMakesAnIdlePartitionPlainInPlace() throws Exception {
        assertSweepMakesIdlePartitionPlainInPlace(false);
    }

    /**
     * {@link #testTheCompactionSweepMakesAnIdlePartitionPlainInPlace} on a frozen partition: the freeze keeps
     * writes out, not the sweep, and MAKE-PLAIN works on the partition's own {@code _txn} entry in place, so the
     * READ ONLY flag stays where it was.
     */
    @Test
    public void testTheCompactionSweepMakesAReadOnlyIdlePartitionPlainInPlaceAndKeepsItReadOnly() throws Exception {
        assertSweepMakesIdlePartitionPlainInPlace(true);
    }

    /**
     * The rules are stated in dead rows and wasted bytes, and an operator has to be able to see both.
     * PARTITION_COMPACTION.md Sec.9 step 2 adds {@code deadRows} and {@code lastWriteTimestamp} to
     * {@code table_partitions()}; without them there is no way to observe why compaction did or did
     * not fire.
     */
    @Test
    public void testTablePartitionsReportsDeadRowsAndLastWriteTimestamp() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
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
     * How many pieces a directory is made of, checked against the geometry the reader resolves
     * itself, so a drift between the function and the reader would show here.
     */
    @Test
    public void testTablePartitionsReportsPieceCount() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            // Let the pre-split cut the day on every stride, so it ends up as several pieces.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

            createDayTable("x", "2024-01-01", 40_000);
            append("x", "2024-01-02", 100);
            // Separated strides inside the day's range: each one cuts a piece off and rewrites the
            // owning piece at the file tail, leaving its old copy dead.
            backdate("x", "2024-01-01T02:00:00", 60);
            backdate("x", "2024-01-01T06:00:00", 60);
            backdate("x", "2024-01-01T10:00:00", 60);

            final long pieces = pieceCountOfDay("x", "2024-01-01");
            Assert.assertTrue("fixture must leave a composite day, pieces=" + pieces, pieces > 1);
            final long dead = deadRowsOfDay("x", "2024-01-01");
            Assert.assertTrue("fixture must leave dead rows", dead > 0);

            Assert.assertEquals(pieces, scalar("select pieceCount::long from table_partitions('x') where name = '2024-01-01'"));
            // The untouched day next to it is a single piece.
            assertQuery("select pieceCount from table_partitions('x') where name = '2024-01-02'")
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            pieceCount
                            1
                            """);
        });
    }


    /**
     * Waste-ratio candidates outrank every other tier, and the largest percentage wins within the tier.
     * The older partition below has waste too, so selecting by partition index would compact the wrong one.
     */
    @Test
    public void testWastePriorityCompactsTheHighestRatioFirst() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            enableCompaction();
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            setCurrentMicros(parseMicros("2024-01-10T00:00:00.000000Z"));

            createDayTable("x", "2024-01-01", 4_000);
            append("x", "2024-01-02", 4_000);
            backdate("x", "2024-01-01T00:30:00", 400);
            backdate("x", "2024-01-02T00:30:00", 400);
            backdate("x", "2024-01-02T00:30:00", 400);

            final long olderDeadBefore = deadRowsOfDay("x", "2024-01-01");
            final long newerDeadBefore = deadRowsOfDay("x", "2024-01-02");
            final long olderLive = scalar("SELECT coalesce(sum(numRows), 0) FROM table_partitions('x')" +
                    " WHERE name LIKE '2024-01-01%'");
            final long newerLive = scalar("SELECT coalesce(sum(numRows), 0) FROM table_partitions('x')" +
                    " WHERE name LIKE '2024-01-02%'");
            Assert.assertTrue("older partition has no waste", olderDeadBefore > 0);
            Assert.assertTrue(
                    "newer partition does not have the higher waste ratio",
                    newerDeadBefore * olderLive > olderDeadBefore * newerLive
            );

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_PRESSURE_DEAD_RATIO, "0.05");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, "0.1");
            append("x", nextPassDay(), 2);

            Assert.assertEquals("the lower-ratio partition was compacted first", olderDeadBefore, deadRowsOfDay("x", "2024-01-01"));
            Assert.assertEquals("the highest-ratio partition was not compacted first", 0, deadRowsOfDay("x", "2024-01-02"));
        });
    }

    /**
     * Piece-count candidates form the second priority tier, ordered by their piece count rather than their
     * partition index. Waste remains disabled so it cannot decide this fixture.
     */
    @Test
    public void testPiecePriorityCompactsTheHighestCountFirst() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            enableCompaction();
            letPreSplitCut();
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            setCurrentMicros(parseMicros("2024-01-10T00:00:00.000000Z"));

            createDayTable("x", "2024-01-01", 40_000);
            execute("INSERT INTO x SELECT cast(x AS int) + 900_000 i," +
                    " timestamp_sequence('2024-01-02', 1_000_000L) ts FROM long_sequence(40_000)");
            drainWalQueue();
            backdate("x", "2024-01-01T02:00:00", 60);
            backdate("x", "2024-01-01T06:00:00", 60);
            backdate("x", "2024-01-02T02:00:00", 60);
            backdate("x", "2024-01-02T06:00:00", 60);
            backdate("x", "2024-01-02T10:00:00", 60);

            final long olderPiecesBefore = pieceCountOfDay("x", "2024-01-01");
            final long newerPiecesBefore = pieceCountOfDay("x", "2024-01-02");
            Assert.assertTrue("older partition has no piece-count pressure", olderPiecesBefore > 1);
            Assert.assertTrue("newer partition does not have more pieces", newerPiecesBefore > olderPiecesBefore);

            pinPieceCap(1);
            append("x", nextPassDay(), 2);

            Assert.assertEquals(
                    "the lower-piece-count partition was compacted first",
                    olderPiecesBefore,
                    pieceCountOfDay("x", "2024-01-01")
            );
            Assert.assertTrue(
                    "the highest-piece-count partition was not compacted first",
                    pieceCountOfDay("x", "2024-01-02") < newerPiecesBefore
            );
        });
    }

    /**
     * The table-pressure percentage uses every live row visible to the user as its denominator, including
     * rows in plain partitions. Dead rows do not join that denominator: 100 live rows plus 50 dead rows is
     * 50% dead, not 33% of a 150-row physical extent.
     */
    @Test
    public void testTablePressurePercentageUsesVisibleTableRows() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            enableCompaction();
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, "10");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1");
            setCurrentMicros(parseMicros("2024-01-10T00:00:00.000000Z"));

            createDayTable("x", "2024-01-01", 4_000);
            backdate("x", "2024-01-01T00:30:00", 400);
            backdate("x", "2024-01-01T00:30:00", 400);
            backdate("x", "2024-01-01T00:30:00", 400);

            // Add enough plain rows to put the whole table below 50%, while the composite partition alone
            // remains above 50%. The old composite-only denominator would therefore compact this fixture.
            execute("INSERT INTO x SELECT cast(x AS int) + 900_000 i," +
                    " timestamp_sequence('2024-01-02', 1_000_000L) ts FROM long_sequence(100_000)");
            drainWalQueue();

            final long deadBefore = deadRows("x");
            final long compositeLive = scalar("SELECT coalesce(sum(numRows), 0) FROM table_partitions('x')" +
                    " WHERE name LIKE '2024-01-01%'");
            final long tableLive = liveRows("x");
            Assert.assertTrue("fixture does not cross 50% with the composite-only denominator", deadBefore > compositeLive);
            Assert.assertTrue("fixture is not below 50% of the table's visible rows", deadBefore * 100 < tableLive * 50);

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD_PERCENT, "50");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD, "1");
            runCompactionPasses("x");

            Assert.assertEquals(
                    "table pressure ignored live rows in plain partitions",
                    deadBefore,
                    deadRows("x")
            );
        });
    }

    /**
     * The table-wide rule picks the coldest composite partition. When every composite partition is hot -
     * written by one of the last {@link CairoConfiguration#getPartitionCompactionHotCommits()} commits -
     * "coldest" degenerates into "the one being written right now", and a REWRITE copies every live row
     * of a partition the next commit is about to dirty again. So the rule withholds REWRITE from a hot
     * partition. It withholds nothing else: JOIN, MOVE-TAIL and MAKE-PLAIN all still run, and on the
     * active partition they are what keeps its size down.
     * <p>
     * Same fixture as {@link #testTablePressureTriggerCompactsTheMostWastefulPartitionFirst}, minus its
     * cooling step: that step is the only difference, so what it asserts is exactly this rule.
     */
    @Test
    public void testTablePressureDoesNotRewriteAPartitionTheLastCommitsWrote() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();

            setCurrentMicros(parseMicros("2024-01-10T00:00:00.000000Z"));
            createDayTable("x", "2024-01-01", 4_000);
            append("x", "2024-01-02", 4_000);

            backdate("x", "2024-01-01T00:30:00", 400);
            backdate("x", "2024-01-01T00:30:00", 400);
            backdate("x", "2024-01-01T00:30:00", 400);

            setCurrentMicros(parseMicros("2024-01-10T00:30:00.000000Z"));
            backdate("x", "2024-01-02T00:30:00", 400);

            enableCompaction();
            // ...but this test is about the hot window itself, so it keeps the shipped one.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 10);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, "10");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD_PERCENT, "20");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD, "1");

            final long deadBefore = deadRows("x");
            Assert.assertTrue("fixture produced no waste", deadBefore > 0);

            setCurrentMicros(parseMicros("2024-01-10T01:00:00.000000Z"));
            // Fewer passes than the hot window is wide, so neither day leaves it.
            Assert.assertTrue(
                    "the fixture cannot show anything: 6 passes already clear the hot window",
                    6 < node1.getConfiguration().getPartitionCompactionHotCommits()
            );
            runCompactionPasses("x");

            Assert.assertEquals(
                    "the table-wide rule rewrote a partition the last commits had written",
                    deadBefore,
                    deadRows("x")
            );

            // ...and the same fixture, once the days fall out of the hot window, does get compacted -
            // so what held it back above was the hot rule and not a fixture that never qualified.
            coolPartitions("x");
            runCompactionPasses("x");
            Assert.assertTrue(
                    "the table-wide rule never fired even after the partitions cooled" +
                            " [before=" + deadBefore + ", after=" + deadRows("x") + ']',
                    deadRows("x") < deadBefore
            );
        });
    }

    /**
     * The table-wide rule fires on the ratio of dead to live rows across the whole table and picks
     * the coldest partition. Here two days are made wasteful and the older one was written to first,
     * so it is the one that must be compacted first.
     */
    @Test
    public void testTablePressureTriggerCompactsTheMostWastefulPartitionFirst() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
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

            // The table-wide rule declines a partition the last few commits wrote, so the fixture's two
            // wasteful days have to fall out of that window before the rule can see either of them. These
            // commits land in days of their own and add no waste; they only move the writer's txn on.
            coolPartitions("x");

            // Compaction comes on only now. With it on from the start, housekeeping runs on every
            // commit of the fixture above and reclaims the waste as it is created, so there is nothing
            // left to observe and no ordering to check.
            enableCompaction();
            // Per-partition rules must not be what fires - only the table-wide one. enableCompaction()
            // already keeps the piece-count rule off by default; the fixture's total waste is a few KB,
            // well under the table-wide floor's 50MB default, so that floor must be lowered too or the
            // percentage check never even runs. Keep the normal waste rule above this fixture's ratio.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_ROWS_RATIO, "10");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD_PERCENT, "20");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD, "1");

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
     * The waste-ratio rule: dead rows must exceed a multiple of the live rows AND a minimum size.
     * Repeated merge-appends of one narrow stride leave a copy of that stride dead each time.
     */
    @Test
    public void testWasteRatioTriggerReclaimsDeadRows() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            // Piece count off during the buildup below - the effective cap is liveRows / avg.rows.piece.lim
            // with no flat floor any more, and this fixture's live rows (3800) sit well under the default
            // 4096 divisor, so the piece-count rule would otherwise fire on the very first relocated piece.
            enableCompaction();
            // A full-partition rewrite naturally crosses the table-wide dead-percent default (50%)
            // partway through the buildup below; keep the table-pressure rule out of the way so only
            // the waste-ratio rule (set after the buildup) is what fires.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD_PERCENT, "99");

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

    /**
     * {@code housekeep} calls {@code runCompaction} after EVERY commit, on the WAL-apply throughput path,
     * and all three of its passes can only ever pick a composite partition. The gate that spares a table
     * with none of them is {@link TxReader#hasCompositePartitions()}, so pin what it answers: false while
     * every partition is plain, true the moment one goes composite, and false again once compaction has
     * folded it back.
     */
    @Test
    public void testHasCompositePartitionsTracksTheOnlyShapeCompactionCanPick() throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            enableCompaction();
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "60m");

            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_TABLE_DEAD_THRESHOLD_PERCENT, "99");

            setCurrentMicros(parseMicros("2024-01-10T00:00:00.000000Z"));
            createDayTable("hc", "2024-01-01", 20_000);
            Assert.assertFalse("a table of plain partitions has nothing compaction can pick", hasComposite("hc"));

            // Two rewrites of the same stride, so the partition is left composite with dead rows - one
            // alone gets folded back inline by the planner's own JOIN.
            backdate("hc", "2024-01-01T06:00:00", 200);
            backdate("hc", "2024-01-01T06:00:00", 200);
            Assert.assertTrue("the backdated strides should have left 2024-01-01 composite", hasComposite("hc"));

            setCurrentMicros(parseMicros("2024-01-10T02:00:00.000000Z"));
            append("hc", "2024-01-05", 10);
            runCompactionPasses("hc");
            Assert.assertFalse("compaction folded the last composite partition away", hasComposite("hc"));
        });
    }

    private static boolean hasComposite(String table) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(table))) {
            return reader.getTxFile().hasCompositePartitions();
        }
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
    private static void assertFrontFileCoversPhysicalRows(String table, String day, String column, int valueSize) throws Exception {
        final TableToken tt = engine.verifyTableName(table);
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final long partitionTs = parseMicros(day + "T00:00:00.000000Z");
            final int partitionIndex = txReader.getPartitionIndex(partitionTs);
            Assert.assertTrue("day has no partition", partitionIndex > -1);
            final long physicalRows = reader.getPartitionPhysicalRowCount(partitionIndex);
            final int columnIndex = reader.getMetadata().getColumnIndex(column);
            final long columnNameTxn = reader.getColumnVersionReader().getColumnNameTxn(partitionTs, reader.getMetadata().getWriterIndex(columnIndex));
            final Path path = Path.getThreadLocal(engine.getConfiguration().getDbRoot()).concat(tt);
            TableUtils.setPathForNativePartition(path, reader.getMetadata().getTimestampType(), reader.getPartitionedBy(), partitionTs, txReader.getPartitionNameTxn(partitionIndex));
            final long fileSize = engine.getConfiguration().getFilesFacade().length(TableUtils.dFile(path, column, columnNameTxn));
            Assert.assertTrue(
                    "the front's " + column + ".d is shorter than its physical rows [physicalRows=" + physicalRows + ", fileSize=" + fileSize + ']',
                    fileSize >= physicalRows * valueSize
            );
        }
    }

    private static void assertFrontStampedWithTheLastSeqTxn(String shape) throws Exception {
        try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(dayTimestamp(reader, SPLIT_DAY));
            Assert.assertEquals(shape, txReader.getSeqTxn(), txReader.getNativePartitionSeqTxn(partitionIndex));
        }
    }

    private static void assertMakePlainCompletesAfterCheckpoint(String table, String day, String before, long diskBefore) throws Exception {
        // Checkpoint gone: the same partition must now go all the way - plain AND trimmed.
        setCurrentMicros(currentMicros + 2 * Micros.MINUTE_MICROS);
        runCompactionPasses(table);
        Assert.assertFalse(
                "the partition is still composite after the checkpoint was released;" +
                        " dead rows: " + deadRowsOfDay(table, day),
                isComposite(table, day)
        );
        Assert.assertEquals(0, deadRowsOfDay(table, day));
        Assert.assertTrue(
                "the dead bytes the checkpoint deferred were never reclaimed [before=" + diskBefore
                        + ", after=" + diskSizeOfDay(table, day) + "]",
                diskSizeOfDay(table, day) < diskBefore
        );
        Assert.assertEquals("the deferred trim changed the data", before, fingerprintOfDay(table, day));
    }

    private static void assertMakePlainHalfwayUnderCheckpoint(String table, String day, long deadBefore, long diskBefore) throws Exception {
        Assert.assertTrue(
                "MAKE-PLAIN recorded the partition as plain while the checkpoint kept TRIM-FILES from" +
                        " running - its dead bytes are now reported by nothing and reclaimed by nothing",
                isComposite(table, day)
        );
        // MAKE-PLAIN's first commit may or may not have lowered E (the halfway state) - either way the dead rows
        // are no more than before, and the FILES must not have been touched.
        Assert.assertTrue(
                "dead rows grew under the checkpoint",
                deadRowsOfDay(table, day) <= deadBefore
        );
        // Not equal: the halfway commit appends a geometry record. A trim would cut thousands of rows' worth.
        final long diskAfter = diskSizeOfDay(table, day);
        Assert.assertTrue(
                "TRIM-FILES shortened a live column file while a checkpoint was running - a backup sizing" +
                        " that file by the manifest's physical row extent would fail its upload [before="
                        + diskBefore + ", after=" + diskAfter + ']',
                diskAfter >= diskBefore
        );
    }

    private static void assertSweepMakesIdlePartitionPlainInPlace(boolean isReadOnly) throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            enableMergeAppend();
            enableCompaction();
            letPreSplitCut();
            setCurrentMicros(parseMicros("2024-01-10T00:00:00.000000Z"));
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
            node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
            node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

            createDayTable("x", "2024-01-01", 20_000);
            buildMoveTailWaste("x");
            pinPieceCap(2);

            // A pinned reader gets the day to MAKE-PLAIN's shape - MOVE-TAIL runs, MAKE-PLAIN declines.
            try (TableReader pinned = engine.getReader(engine.verifyTableName("x"))) {
                Assert.assertNotNull(pinned);
                runCompactionPasses("x");
                Assert.assertTrue("fixture did not reach MOVE-TAIL", isComposite("x", "2024-01-01"));
                Assert.assertEquals(1, pieceCountOfDay("x", "2024-01-01"));
                Assert.assertTrue("MOVE-TAIL left no dead space to protect", deadRowsOfDay("x", "2024-01-01") > 0);
            }
            engine.releaseInactive();
            if (isReadOnly) {
                try (TableWriter writer = engine.getWriter(engine.verifyTableName("x"), "test-freeze")) {
                    final int frontIndex = writer.getTxWriter().getPartitionIndex(parseMicros("2024-01-01T00:00:00.000000Z"));
                    writer.getTxWriter().setPartitionReadOnly(frontIndex, true);
                    writer.bumpPartitionTableVersion();
                    writer.commit();
                }
                engine.releaseAllWriters();
            }

            final String before = fingerprintOfDay("x", "2024-01-01");
            final long diskBefore = diskSizeOfDay("x", "2024-01-01");
            final long deadBefore = deadRowsOfDay("x", "2024-01-01");
            final long nameTxnBefore = frontNameTxnOfDay("x", "2024-01-01");

            // Nothing writes to the table from here on, so the writer's own per-commit retry never runs
            // again - only the sweep can finish this off.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
            setCurrentMicros(parseMicros("2024-06-01T00:00:00.000000Z"));
            Assert.assertTrue("the writer already reclaimed it, the sweep has nothing to do",
                    deadRowsOfDay("x", "2024-01-01") == deadBefore);

            engine.releaseAllReaders();
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            Assert.assertFalse(
                    "the sweep left the idle partition composite; dead rows: " + deadRowsOfDay("x", "2024-01-01"),
                    isComposite("x", "2024-01-01")
            );
            Assert.assertEquals(0, deadRowsOfDay("x", "2024-01-01"));
            Assert.assertTrue(
                    "the sweep did not reclaim the dead bytes [before=" + diskBefore
                            + ", after=" + diskSizeOfDay("x", "2024-01-01") + ']',
                    diskSizeOfDay("x", "2024-01-01") < diskBefore
            );
            Assert.assertEquals(
                    "the sweep REWROTE the partition into a new directory instead of making it plain in place",
                    nameTxnBefore,
                    frontNameTxnOfDay("x", "2024-01-01")
            );
            Assert.assertEquals("the sweep changed the data", before, fingerprintOfDay("x", "2024-01-01"));
            Assert.assertEquals("the front's READ ONLY flag must survive MAKE-PLAIN", isReadOnly, isReadOnly("x", "2024-01-01"));
        });
    }

    private static void assertFrontChainStartsAt(int generation) throws Exception {
        Assert.assertTrue("the front is not composite", isComposite("x", SPLIT_DAY));
        final long ref = frontGeometryRef();
        Assert.assertEquals("the chain opened on the wrong generation", generation, TxReader.geometryGeneration(ref));
        Assert.assertEquals("a chain opens at offset 0", 0, TxReader.geometryOffset(ref));
    }

    private static void assertNotSuspended() {
        Assert.assertFalse("the table is suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
    }

    // Only the day's last directory: the one a carve shrinks keeps the entries of the rows that moved out.
    private static void assertPostingIndexMatchesLastDirectoryOfSplitDay(String shape) throws Exception {
        try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
            final TxReader txReader = reader.getTxFile();
            final long dayTs = dayTimestamp(reader, SPLIT_DAY);
            int partitionIndex = reader.getPartitionCount() - 1;
            while (txReader.getPartitionFloor(txReader.getPartitionTimestampByIndex(partitionIndex)) != dayTs) {
                partitionIndex--;
            }
            final long rowCount = reader.openPartition(partitionIndex);
            final int columnIndex = reader.getMetadata().getColumnIndex("s");
            final IndexReader indexReader = reader.getIndexReader(partitionIndex, columnIndex, IndexReader.DIR_FORWARD);
            long indexedRows = 0;
            for (int key = 0, keys = reader.getSymbolMapReader(columnIndex).getSymbolCount(); key <= keys; key++) {
                try (RowCursor cursor = indexReader.getCursor(key, 0, 100_000)) {
                    while (cursor.hasNext()) {
                        final long row = cursor.next();
                        Assert.assertTrue(shape + ": the index names row " + row + " of " + rowCount, row < rowCount);
                        indexedRows++;
                    }
                }
            }
            Assert.assertEquals(shape + ": the index holds the wrong rows", rowCount, indexedRows);
        }
    }

    private static void assertPurgeRemovesTheRetiredGeneration(int retiredGeneration, int liveGeneration) throws Exception {
        runColumnPurgeJob();
        Assert.assertFalse("the purge kept the retired generation", hasFrontGeometryFile(retiredGeneration));
        Assert.assertTrue("the purge deleted the live generation", hasFrontGeometryFile(liveGeneration));
    }

    private static void assertSplitDayReadOnly(String shape) throws Exception {
        try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
            final TxReader txReader = reader.getTxFile();
            final long dayTs = dayTimestamp(reader, SPLIT_DAY);
            for (int p = 0, n = txReader.getPartitionCount(); p < n; p++) {
                if (txReader.getPartitionFloor(txReader.getPartitionTimestampByIndex(p)) == dayTs) {
                    Assert.assertTrue(shape + ": partition " + p + " became writable", txReader.isPartitionReadOnly(p));
                }
            }
        }
    }

    private static void assertSuspended(String message) throws Exception {
        Assert.assertTrue("the table is not suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
        try (RecordCursorFactory factory = select("select errorMessage from wal_tables() where name = 'x'")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue("wal_tables() does not list the table", cursor.hasNext());
                TestUtils.assertContains(cursor.getRecord().getStrA(0), message);
            }
        }
    }

    private static void backdate(String table, String ts, int rows) throws Exception {
        execute("insert into " + table + " (i, ts) select cast(x as int) + 500000 i," +
                " timestamp_sequence('" + ts + "', 1000000L) ts from long_sequence(" + rows + ")");
        drainWalQueue();
    }

    private static void buildCompositeFrontNextToMoveTailSibling() throws Exception {
        buildCompositeFrontNextToMoveTailSibling("timestamp", "");
    }

    private static void buildCompositeFrontNextToMoveTailSibling(String timestampType, String extraColumns) throws Exception {
        enableMergeAppend();
        enableCompaction();
        letPreSplitCut();
        setCurrentMicros(parseMicros("2024-01-10T00:00:00.000000Z"));
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_DEAD_MIN_SIZE, "1T");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);

        createDayTable("x", SPLIT_DAY, SPLIT_DAY_ROWS, timestampType, extraColumns);
        buildMoveTailWaste("x");
        pinPieceCap(2);

        try (TableReader pinned = engine.getReader(engine.verifyTableName("x"))) {
            Assert.assertNotNull(pinned);
            runCompactionPasses("x");
        }
        Assert.assertTrue("fixture did not reach MOVE-TAIL", isComposite("x", SPLIT_DAY));
        Assert.assertEquals("MOVE-TAIL must leave the day as front + one sibling split", 2, partitionCountOfDay("x", SPLIT_DAY));
    }

    private static void buildMoveTailWaste(String table) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1T");
        for (int i = 0; i < 5; i++) {
            backdate(table, "2024-01-01T05:00:00", 200);
        }
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 512);
    }

    private static String countAndSum(TableReader reader) {
        long count = 0;
        long sum = 0;
        final int columnIndex = reader.getMetadata().getColumnIndex("i");
        try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
            while (cursor.hasNext()) {
                count++;
                sum += cursor.getRecord().getInt(columnIndex);
            }
        }
        return "c\ts\n" + count + '\t' + sum + '\n';
    }

    /**
     * Leaves SPLIT_DAY as a parent of 300 rows, i = 1..300, next to a classic split of 400.
     */
    private static void createClassicSplitDay(
            String include,
            boolean isSingleTimestampParent,
            boolean isLastPartition,
            boolean isDedup
    ) throws Exception {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        execute("drop table if exists x");
        drainWalQueue();
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 50);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 50);
        execute("create table x (i int, s symbol index type posting" + include + ", ts timestamp) timestamp(ts) partition by DAY WAL"
                + (isDedup ? " dedup upsert keys(ts, i)" : ""));
        final String parentTs = isSingleTimestampParent
                ? "'" + SPLIT_DAY + "'::timestamp"
                : "timestamp_sequence('" + SPLIT_DAY + "', 1_000_000L)";
        execute("insert into x select x::int, 's' || (x % 5), " + parentTs + " from long_sequence(300)");
        execute("insert into x select (2_000 + x)::int, 's1', timestamp_sequence('2024-01-01T03:00:00', 1_000_000L) from long_sequence(60)");
        if (!isLastPartition) {
            execute("insert into x select x::int, 's3', timestamp_sequence('2024-01-05', 1_000_000L) from long_sequence(2)");
        }
        drainWalQueue();
        execute("insert into x select (3_000 + x)::int, 's1', timestamp_sequence('2024-01-01T03:00:00', 1_000_000L) from long_sequence(10)");
        drainWalQueue();
        execute("insert into x select (4_000 + x)::int, 's1', timestamp_sequence('2024-01-01T05:00:00', 1_000_000L) from long_sequence(330)");
        drainWalQueue();
        Assert.assertEquals("fixture did not split the day", 2, partitionCountOfDay("x", SPLIT_DAY));
    }

    private static void createDayTable(String table, String day, int rows) throws Exception {
        createDayTable(table, day, rows, "timestamp", "");
    }

    private static void createDayTable(String table, String day, int rows, String timestampType, String extraColumns) throws Exception {
        execute("create table " + table + " as (select cast(x as int) i, " + extraColumns +
                " timestamp_sequence('" + day + "', 1000000L)::" + timestampType + " ts" +
                " from long_sequence(" + rows + ")) timestamp(ts) partition by DAY WAL");
        drainWalQueue();
    }

    /**
     * Moves the writer's txn past {@link CairoConfiguration#getPartitionCompactionHotCommits()} so the
     * partitions the fixture just wrote stop counting as hot. Each commit goes to a day of its own, so
     * none of them adds dead space of its own.
     */
    private static void coolPartitions(String table) throws Exception {
        for (int i = 0; i < node1.getConfiguration().getPartitionCompactionHotCommits() + 1; i++) {
            execute("insert into " + table + " select cast(x as int) + 700000 + " + (passDay * 10) + " i," +
                    " timestamp_sequence('" + nextPassDay() + "', 60*1000000L) ts from long_sequence(2)");
            drainWalQueue();
        }
    }

    private static long dayTimestamp(TableReader reader, String day) throws Exception {
        return ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType())
                .fromMicros(parseMicros(day + "T00:00:00.000000Z"));
    }

    private static long deadRows(String table) throws Exception {
        return scalar("select coalesce(sum(deadRows), 0) deadRows from table_partitions('" + table + "')");
    }

    private static long deadRowsOfDay(String table, String day) throws Exception {
        return scalar("select coalesce(sum(deadRows), 0) deadRows from table_partitions('" + table + "')" +
                " where name like '" + day + "%'");
    }

    /**
     * Disk of one day only - the housekeeping commits land in other partitions.
     */
    private static long diskSizeOfDay(String table, String day) throws Exception {
        return scalar("select coalesce(sum(diskSize), 0) d from table_partitions('" + table + "')" +
                " where name like '" + day + "%'");
    }

    /**
     * Disk of the day's own (front) partition directory only. {@link #diskSizeOfDay} also counts the fresh partition
     * MOVE-TAIL copies the tail into, and FrameFactory rounds that partition's files up to the OS page size - 64 KiB
     * on Windows, where the tail's page padding outweighs the dead bytes TRIM-FILES cuts from the front. Only
     * TRIM-FILES shortens the front's files, so a shrink measured here is its doing. The helper first closes idle
     * pooled readers and the writer, so it measures the files as they stand with nothing holding them open.
     */
    private static long diskSizeOfFront(String table, String day) throws Exception {
        engine.releaseInactive();
        final TableToken tt = engine.verifyTableName(table);
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final long partitionTs = parseMicros(day + "T00:00:00.000000Z");
            final int partitionIndex = txReader.getPartitionIndex(partitionTs);
            Assert.assertTrue("day has no partition", partitionIndex > -1);
            final Path path = Path.getThreadLocal(engine.getConfiguration().getDbRoot()).concat(tt);
            TableUtils.setPathForNativePartition(path, reader.getMetadata().getTimestampType(), reader.getPartitionedBy(), partitionTs, txReader.getPartitionNameTxn(partitionIndex));
            return engine.getConfiguration().getFilesFacade().getDirSize(path);
        }
    }

    /**
     * Hides {@link AbstractCairoTest#drainWalQueue()} so every one of this file's bare {@code drainWalQueue()}
     * calls also checks that {@code getPartitionPhysicalRowCount} still matches what the writer actually put
     * on disk - see {@link TestUtils#assertPhysicalRowCountsMatchFiles}. It first closes idle pooled readers, which
     * keep partition files mapped, and {@link WindowsMappedTruncateFacade} refuses TRIM-FILES on a mapped file.
     */
    protected static void drainWalQueue() {
        engine.releaseInactive();
        AbstractCairoTest.drainWalQueue();
        TestUtils.assertPhysicalRowCountsMatchFiles(engine);
    }

    private static void drainWalQueueHoldingOffMakePlain() {
        try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
            Assert.assertNotNull(reader);
            AbstractCairoTest.drainWalQueue();
        }
        TestUtils.assertPhysicalRowCountsMatchFiles(engine);
    }

    private static void drainWalQueueOnOneWriter() {
        engine.releaseAllReaders();
        AbstractCairoTest.drainWalQueue();
        TestUtils.assertPhysicalRowCountsMatchFiles(engine);
    }

    // Compaction is always on now; kept as a no-op so call sites still document intent.

    /**
     * Housekeeping on, with the piece-count rule's SCALED cap out of the way. The cap is
     * max(configured floor, liveRows / avg.rows.piece.lim) - see
     * {@link PartitionCompactionPolicy#effectiveMaxPieces} - so a fixture of more than a few thousand rows
     * and a deliberately small floor would silently stop triggering at that exact count. This suite tests
     * the mechanism at small, exact numbers, so the divisor is pinned above any fixture's row count and the
     * flat floor is the only threshold in play.
     * <p>
     * The same divisor sets the PRE-SPLIT's piece floor at twice its value, so this also stops the pre-split
     * cutting anything. Call {@link #letPreSplitCut()} where a fixture needs a partition of several pieces
     * to work on, and {@link #pinPieceCap(int)} to put the piece-count rule back in play afterwards.
     */
    private static void enableCompaction() {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, Long.MAX_VALUE / 8);
        // These fixtures are a handful of commits, every one of which would count as hot under the shipped
        // commit window, so the tests that want compaction turn it off and the tests that want to observe it
        // set it themselves.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
    }

    /**
     * A 32-row piece floor, so the pre-split cuts these small fixtures into several pieces instead of
     * rewriting them whole.
     */
    private static void letPreSplitCut() {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 16);
    }

    /**
     * Puts the piece-count rule in play at exactly {@code pieces}: the divisor goes back above any fixture's
     * row count, so the scaled cap is zero and the flat floor is what fires.
     */
    private static void pinPieceCap(int pieces) {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, Long.MAX_VALUE / 8);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, pieces);
    }

    private static void enableMergeAppend() {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
    }

    /**
     * Content fingerprint of ONE day, so the housekeeping commits do not move it. Sums {@code i} by
     * walking the row cursor in Java rather than with a SQL {@code sum()}, keeping this class's "did
     * compaction preserve the data" assertions independent of whether vectorized SQL aggregation over
     * a composite partition is itself correct - a question {@link O3CompositePartitionAggregationTest}
     * tests directly. (Earlier history: a vectorized aggregate over a composite partition with dead
     * space used to return wrong results or crash the JVM - tracked as "D6" in the now-deleted
     * an earlier revision of this branch - but that no longer reproduces against current HEAD.)
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

    /**
     * Same as {@link #fingerprintOfDay}, but also covers a var-size {@code s} column.
     */
    private static String fingerprintOfDayVarSize(String table, String day) throws Exception {
        final String sql = "select i, s from " + table + " where ts in '" + day + "'";
        long count = 0;
        long intSum = 0;
        long strLenSum = 0;
        try (RecordCursorFactory f = select(sql)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                while (c.hasNext()) {
                    count++;
                    intSum += c.getRecord().getInt(0);
                    final CharSequence s = c.getRecord().getStrA(1);
                    strLenSum += s == null ? 0 : s.length();
                }
            }
        }
        return count + "/" + intSum + "/" + strLenSum;
    }

    /**
     * {@link #fingerprintOfDayVarSize(String, String)} taken from a reader's OWN cursor rather than from a
     * fresh query, so it reports what that reader's pinned snapshot resolves. Fixtures put every row of the
     * day under test below {@code dayHiExclusive} and every housekeeping row above it.
     */
    private static String fingerprintOfDayVarSize(RecordCursor c, String dayHiExclusive) throws Exception {
        final long dayHi = parseMicros(dayHiExclusive + "T00:00:00.000000Z");
        long count = 0;
        long intSum = 0;
        long strLenSum = 0;
        while (c.hasNext()) {
            if (c.getRecord().getTimestamp(2) >= dayHi) {
                continue;
            }
            count++;
            intSum += c.getRecord().getInt(0);
            final CharSequence s = c.getRecord().getStrA(1);
            strLenSum += s == null ? 0 : s.length();
        }
        return count + "/" + intSum + "/" + strLenSum;
    }

    private static long frontGeometryFileLength(int generation) throws Exception {
        try (TableReader reader = engine.getReader(engine.verifyTableName("x")); Path path = new Path()) {
            setPathToFront(path, reader).concat(TableUtils.PARTITION_GEOMETRY_FILE_NAME).put('.').put(generation);
            return engine.getConfiguration().getFilesFacade().length(path.$());
        }
    }

    private static long frontGeometryRef() throws Exception {
        try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(dayTimestamp(reader, SPLIT_DAY));
            Assert.assertTrue("day has no partition", partitionIndex > -1);
            return txReader.getGeometryRef(partitionIndex);
        }
    }

    /**
     * The {@code nameTxn} of the day's OWN (first, front) partition - unchanged by MOVE-TAIL.
     */
    private static void dropTable() throws Exception {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        execute("drop table if exists x");
        drainWalQueue();
    }

    private static long frontNameTxnOfDay(String table, String day) throws Exception {
        final TableToken tt = engine.verifyTableName(table);
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(dayTimestamp(reader, day));
            Assert.assertTrue("day has no partition", partitionIndex > -1);
            return txReader.getPartitionNameTxn(partitionIndex);
        }
    }

    private static long frontRowCountOfDay(String table, String day) throws Exception {
        try (TableReader reader = engine.getReader(engine.verifyTableName(table))) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(parseMicros(day + "T00:00:00.000000Z"));
            Assert.assertTrue("day has no partition", partitionIndex > -1);
            return txReader.getPartitionSize(partitionIndex);
        }
    }

    private static int frontSquashCountOfDay(String table, String day) throws Exception {
        try (TableReader reader = engine.getReader(engine.verifyTableName(table))) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(parseMicros(day + "T00:00:00.000000Z"));
            Assert.assertTrue("day has no partition", partitionIndex > -1);
            return txReader.getPartitionSquashCount(partitionIndex);
        }
    }

    /**
     * Whether the day's live geometry chain has grown back over {@code ref}: same generation, and a record
     * published at or past that reference's own offset. That is the condition under which a pinned reader
     * still holding {@code ref} reads bytes a later commit wrote.
     */
    private static boolean geometryChainReaches(String table, String day, long ref) throws Exception {
        final TableToken tt = engine.verifyTableName(table);
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(parseMicros(day + "T00:00:00.000000Z"));
            if (partitionIndex < 0 || !txReader.isPartitionComposite(partitionIndex)) {
                return false;
            }
            final long live = txReader.getGeometryRef(partitionIndex);
            return TxReader.geometryGeneration(live) == TxReader.geometryGeneration(ref)
                    && TxReader.geometryOffset(live) >= TxReader.geometryOffset(ref);
        }
    }

    private static boolean hasFrontGeometryFile(int generation) throws Exception {
        return frontGeometryFileLength(generation) > -1;
    }

    private static void holdEveryGenerationOfFront(int sourceGeneration) throws Exception {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (
                TableReader reader = engine.getReader(engine.verifyTableName("x"));
                Path path = new Path();
                PartitionGeometryFile record = new PartitionGeometryFile(MemoryTag.NATIVE_DEFAULT)
        ) {
            setPathToFront(path, reader);
            record.read(ff, path, sourceGeneration, 0);
            for (int generation = 0; generation <= LAST_GEOMETRY_GENERATION; generation++) {
                if (generation != sourceGeneration) {
                    record.append(ff, path, generation, 0, engine.getConfiguration().getCommitMode());
                }
            }
        }
    }

    private static void insertCompactionPass(String table) throws Exception {
        // A day of its own per commit, and a day never reused by a later call: re-inserting the
        // same timestamps would be an O3 write into an existing partition, which rewrites it and
        // charges those rows to physicallyWrittenRows - so the row accounting of runCompactionPasses
        // would be wrong by however much that merge amplified rather than by the 12 rows actually inserted.
        execute("insert into " + table + " (i, ts) select cast(x as int) + 800000 + " + (passDay * 10) + " i," +
                " timestamp_sequence('" + nextPassDay() + "', 60*1000000L) ts from long_sequence(2)");
    }

    private static void insertIntoFront() throws Exception {
        execute("insert into x (i, ts) values (424_242, '2024-01-01T03:00:00.000000Z')");
        drainWalQueue();
        assertNotSuspended();
    }

    private static void insertIntoTypedFrontAndExpected(String ts) throws Exception {
        final String values = " (i, c_varchar, c_indexed, c_covering, ts) values (424_242, 'inserted-after-the-removal', 'b', 'b', '" + ts + "')";
        execute("insert into x" + values);
        execute("insert into expected" + values);
        drainWalQueue();
        assertNotSuspended();
    }

    /**
     * Whether the day's own partition is composite: more than one piece, or dead space above the live
     * rows, or rows starting above file row 0.
     */
    private static boolean isComposite(String table, String day) throws Exception {
        final TableToken tt = engine.verifyTableName(table);
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(dayTimestamp(reader, day));
            return partitionIndex > -1 && txReader.isPartitionComposite(partitionIndex);
        }
    }

    private static boolean isReadOnly(String table, String day) throws Exception {
        final TableToken tt = engine.verifyTableName(table);
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(parseMicros(day + "T00:00:00.000000Z"));
            Assert.assertTrue("day has no partition", partitionIndex > -1);
            return txReader.isPartitionReadOnly(partitionIndex);
        }
    }

    private static int makeFrontPlainNextToMoveTailSibling() throws Exception {
        return makeFrontPlainNextToMoveTailSibling("timestamp", "");
    }

    /**
     * @return the generation MAKE-PLAIN retired, whose purge is still pending
     */
    private static int makeFrontPlainNextToMoveTailSibling(String timestampType, String extraColumns) throws Exception {
        buildCompositeFrontNextToMoveTailSibling(timestampType, extraColumns);
        final int retiredGeneration = TxReader.geometryGeneration(frontGeometryRef());
        final long frontNameTxn = frontNameTxnOfDay("x", SPLIT_DAY);

        setCurrentMicros(currentMicros + 2 * Micros.MINUTE_MICROS);
        runCompactionPassesOnOneWriter("x");
        Assert.assertFalse("fixture did not reach MAKE-PLAIN", isComposite("x", SPLIT_DAY));
        Assert.assertEquals("MAKE-PLAIN moved the front", frontNameTxn, frontNameTxnOfDay("x", SPLIT_DAY));
        Assert.assertEquals("compaction changed the day's partitions", 2, partitionCountOfDay("x", SPLIT_DAY));
        Assert.assertTrue("the retired generation is gone before its purge ran", hasFrontGeometryFile(retiredGeneration));
        return retiredGeneration;
    }

    private static long liveRows(String table) throws Exception {
        return scalar("select coalesce(sum(numRows), 0) live from table_partitions('" + table + "')");
    }

    /**
     * A fresh day, well clear of every fixture's own partitions.
     */
    private static String nextPassDay() {
        return "2024-03-" + String.format("%02d", 1 + (passDay++ % 28));
    }

    private static long parseMicros(String ts) throws Exception {
        return MicrosTimestampDriver.floor(ts);
    }

    /**
     * How many {@code table_partitions()} rows the day has - more than one after a classic split.
     */
    private static long partitionCountOfDay(String table, String day) throws Exception {
        return scalar("select count() from table_partitions('" + table + "') where name like '" + day + "%'");
    }

    private static long physicallyWrittenRows() {
        return node1.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
    }

    /**
     * Pieces of one day's own partition, or 0 if the day has no partition at all.
     */
    private static long pieceCountOfDay(String table, String day) throws Exception {
        final TableToken tt = engine.verifyTableName(table);
        try (TableReader reader = engine.getReader(tt)) {
            final TxReader txReader = reader.getTxFile();
            final int partitionIndex = txReader.getPartitionIndex(parseMicros(day + "T00:00:00.000000Z"));
            return partitionIndex > -1 ? reader.getGeometry().getPieceCount(partitionIndex) : 0;
        }
    }

    private static void plantCutFront(int roomForPieces) throws Exception {
        final long cutRow = 1_000;
        NearFullGeometryFixture.plantCommittedRecord(
                engine,
                "x",
                parseMicros(SPLIT_DAY + "T00:00:00.000000Z"),
                LAST_GEOMETRY_GENERATION,
                roomForPieces,
                false,
                cutRow,
                timestampOfDayRow(cutRow - 1),
                timestampOfDayRow(cutRow)
        );
    }

    private static void removeSibling(long siblingMinTs) throws Exception {
        final TableToken tt = engine.verifyTableName("x");
        final long rangeHi;
        try (TableReader reader = engine.getReader(tt)) {
            rangeHi = dayTimestamp(reader, "2024-01-02");
        }
        try (WalWriter walWriter = engine.getWalWriter(tt)) {
            walWriter.commitWithParams(siblingMinTs, rangeHi, WAL_DEDUP_MODE_REPLACE_RANGE);
        }
    }

    /**
     * One drain of the column purge queue into the log table, then one processing pass - the job schedules the
     * queued note {@code column.purge.retry.delay} into the future, so the clock has to move between the two.
     * This is what deletes a retired {@code _geometry.<generation>} file, and so what frees that generation
     * number for a later chain to open on. Note this is the COLUMN purge job, not
     * {@link AbstractCairoTest#drainPurgeJob()}, which drains the WAL.
     */
    private static void runColumnPurgeJob() throws Exception {
        engine.releaseInactive();
        try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
            setCurrentMicros(currentMicros + Micros.SECOND_MICROS);
            purgeJob.run();
            setCurrentMicros(currentMicros + Micros.SECOND_MICROS);
            purgeJob.run();
        }
    }

    /**
     * Compaction runs inside {@code TableWriter.housekeep}, which fires once per commit. A few small
     * unrelated commits give it several chances to act - one per step, since each step of a
     * compaction is its own transaction.
     */
    private static long runCompactionPasses(String table) throws Exception {
        for (int i = 0; i < 6; i++) {
            insertCompactionPass(table);
            drainWalQueue();
        }
        engine.releaseInactive();
        return 12; // 6 commits x 2 rows, all into partitions of their own
    }

    private static void runCompactionPassesOnOneWriter(String table) throws Exception {
        for (int i = 0; i < 6; i++) {
            insertCompactionPass(table);
            drainWalQueueOnOneWriter();
        }
    }

    /**
     * {@link #runCompactionPasses} for a table that also has the var-size {@code s} column.
     */
    private static void runCompactionPassesVarSize(String table) throws Exception {
        for (int i = 0; i < 6; i++) {
            execute("insert into " + table + " (i, s, ts) select cast(x as int) + 800000 + " + (passDay * 10)
                    + ", null, timestamp_sequence('" + nextPassDay() + "', 60*1000000L) from long_sequence(2)");
            drainWalQueue();
        }
        engine.releaseInactive();
    }

    private static Path setPathToFront(Path path, TableReader reader) throws Exception {
        final TxReader txReader = reader.getTxFile();
        final long partitionTs = dayTimestamp(reader, SPLIT_DAY);
        final int partitionIndex = txReader.getPartitionIndex(partitionTs);
        Assert.assertTrue("day has no partition", partitionIndex > -1);
        path.of(engine.getConfiguration().getDbRoot()).concat(reader.getTableToken());
        TableUtils.setPathForNativePartition(path, reader.getMetadata().getTimestampType(), reader.getPartitionedBy(), partitionTs, txReader.getPartitionNameTxn(partitionIndex));
        return path;
    }

    private static void setFirstPartitionOfSplitDayReadOnly() throws Exception {
        try (TableWriter writer = getWriter(engine.verifyTableName("x"))) {
            final int partitionIndex = writer.getTxWriter().getPartitionIndex(parseMicros(SPLIT_DAY + "T00:00:00.000000Z"));
            writer.getTxWriter().setPartitionReadOnly(partitionIndex, true);
            writer.bumpPartitionTableVersion();
        }
    }

    private static void setMergeAppend(boolean isEnabled) {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, Boolean.toString(isEnabled));
        engine.releaseAllWriters();
    }

    private static long siblingMinTimestamp() throws Exception {
        return scalar("select minTimestamp from table_partitions('x')" +
                " where name like '" + SPLIT_DAY + "%' order by minTimestamp desc limit 1");
    }

    private static long timestampOfDayRow(long row) throws Exception {
        return scalar("select ts::long from x where ts in '" + SPLIT_DAY + "' limit " + row + ", " + (row + 1));
    }

    private static String snapshot(String sql) throws Exception {
        sink.clear();
        printSql(sql);
        return sink.toString();
    }

    private static long scalar(String sql) throws Exception {
        try (RecordCursorFactory f = select(sql)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                Assert.assertTrue("query returned no row: " + sql, c.hasNext());
                return c.getRecord().getLong(0);
            }
        }
    }

    private void assertRowsOfSplitDay(long cutoffTs, boolean isFrontInsertIncluded) throws Exception {
        long count = isFrontInsertIncluded ? 1 : 0;
        long sum = isFrontInsertIncluded ? 424_242 : 0;
        final long dayTs = parseMicros(SPLIT_DAY + "T00:00:00.000000Z");
        for (int i = 1; i <= SPLIT_DAY_ROWS; i++) {
            if (dayTs + (i - 1) * Micros.SECOND_MICROS < cutoffTs) {
                count++;
                sum += i;
            }
        }
        // the five 200-row strides of buildMoveTailWaste
        final long strideTs = dayTs + 5 * Micros.HOUR_MICROS;
        for (int i = 1; i <= 200; i++) {
            if (strideTs + (i - 1) * Micros.SECOND_MICROS < cutoffTs) {
                count += 5;
                sum += 5L * (i + 500_000);
            }
        }
        assertQuery("select count() c, sum(i) s from x where ts in '" + SPLIT_DAY + "'")
                .noRandomAccess()
                .expectSize()
                .returns("c\ts\n" + count + '\t' + sum + '\n');
    }

    private void assertSplitRemovalPreservesTypedFront(String timestampType, boolean isLastPartition, boolean isRewrite) throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            final int retiredGeneration = makeFrontPlainNextToMoveTailSibling(timestampType, TYPED_COLUMNS);
            execute("alter table x alter column c_indexed add index");
            execute("alter table x alter column c_covering add index type posting include (i, c_varchar)");
            execute("alter table x alter column c_to_long type long");
            execute("alter table x alter column c_to_varchar type varchar");
            execute("alter table x add column c_all_null varchar");
            execute("alter table x add column c_partial_top long");
            drainWalQueueOnOneWriter();
            execute("update x set c_partial_top = i where ts >= '2024-01-01T02:00:00.000000Z' and ts < '2024-01-01T04:00:00.000000Z'");
            drainWalQueueOnOneWriter();
            if (isLastPartition) {
                execute("alter table x drop partition where ts >= '2024-03-01'");
                drainWalQueueOnOneWriter();
            }
            Assert.assertFalse("fixture left the front composite", isComposite("x", SPLIT_DAY));
            Assert.assertEquals("fixture lost the sibling", 2, partitionCountOfDay("x", SPLIT_DAY));

            final long frontNameTxn = frontNameTxnOfDay("x", SPLIT_DAY);
            final long siblingMinTs = siblingMinTimestamp();
            execute("create table expected as (select * from x where ts in '" + SPLIT_DAY + "' and ts < " +
                    siblingMinTs + "L::" + timestampType + ") timestamp(ts) partition by day");
            drainWalQueueOnOneWriter();
            if (isRewrite) {
                holdEveryGenerationOfFront(retiredGeneration);
            }

            final String snapshotBefore = snapshot("select count() c, sum(i) s from x");
            try (TableReader pinned = engine.getReader(engine.verifyTableName("x"))) {
                removeSibling(siblingMinTs);
                // not drainWalQueueOnOneWriter(): it cannot release a pinned reader
                AbstractCairoTest.drainWalQueue();
                assertNotSuspended();
                Assert.assertEquals("wrong partitions left in the day", isRewrite ? 1 : 2, partitionCountOfDay("x", SPLIT_DAY));
                if (isRewrite) {
                    Assert.assertNotEquals("the front was not rewritten", frontNameTxn, frontNameTxnOfDay("x", SPLIT_DAY));
                    Assert.assertFalse("the rewritten front is composite", isComposite("x", SPLIT_DAY));
                    for (int generation = 0; generation <= LAST_GEOMETRY_GENERATION; generation++) {
                        Assert.assertFalse("the new version carries generation " + generation, hasFrontGeometryFile(generation));
                    }
                } else {
                    Assert.assertEquals("the carve moved the front", frontNameTxn, frontNameTxnOfDay("x", SPLIT_DAY));
                    assertFrontChainStartsAt(retiredGeneration + 1);
                }
                assertTypedFrontMatchesExpected();

                runColumnPurgeJob();
                insertIntoTypedFrontAndExpected("2024-01-01T03:00:00.500000Z");
                assertTypedFrontMatchesExpected();
                insertIntoTypedFrontAndExpected("2024-01-01T23:00:00.000000Z");
                assertTypedFrontMatchesExpected();
                Assert.assertEquals("the pinned reader's snapshot changed", snapshotBefore, countAndSum(pinned));
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertTypedFrontMatchesExpected();
        });
    }

    private void assertSplitRemovalRewriteKeepsReadOnlyAndStampsTheSeqTxn(boolean isCompositeParent) throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            final int retiredGeneration;
            if (isCompositeParent) {
                buildCompositeFrontNextToMoveTailSibling();
                retiredGeneration = -1;
            } else {
                retiredGeneration = makeFrontPlainNextToMoveTailSibling();
            }
            final TableToken tt = engine.verifyTableName("x");
            final long frontTs = parseMicros(SPLIT_DAY + "T00:00:00.000000Z");
            try (TableWriter writer = getWriter(tt)) {
                final int partitionIndex = writer.getTxWriter().getPartitionIndex(frontTs);
                writer.getTxWriter().setPartitionReadOnly(partitionIndex, true);
                writer.getTxWriter().setPartitionRemote(partitionIndex, !isCompositeParent);
                writer.bumpPartitionTableVersion();
            }
            final long siblingMinTs = siblingMinTimestamp();
            final long frontNameTxn = frontNameTxnOfDay("x", SPLIT_DAY);
            if (isCompositeParent) {
                NearFullGeometryFixture.plantCommittedRecord(engine, "x", frontTs, LAST_GEOMETRY_GENERATION);
            } else {
                holdEveryGenerationOfFront(retiredGeneration);
            }

            removeSibling(siblingMinTs);
            drainWalQueueOnOneWriter();
            assertNotSuspended();
            Assert.assertEquals("the day kept a split", 1, partitionCountOfDay("x", SPLIT_DAY));
            Assert.assertNotEquals("the front was not rewritten", frontNameTxn, frontNameTxnOfDay("x", SPLIT_DAY));
            Assert.assertFalse("the rewritten front is composite", isComposite("x", SPLIT_DAY));
            try (TableReader reader = engine.getReader(tt)) {
                final TxReader txReader = reader.getTxFile();
                final int partitionIndex = txReader.getPartitionIndex(dayTimestamp(reader, SPLIT_DAY));
                Assert.assertTrue("the rewrite made a read-only partition writable", txReader.isPartitionReadOnly(partitionIndex));
                Assert.assertFalse("the new version claims the old one's upload", txReader.isPartitionRemote(partitionIndex));
                Assert.assertEquals("the rewrite did not stamp its seqTxn", txReader.getSeqTxn(), txReader.getNativePartitionSeqTxn(partitionIndex));
            }
            assertRowsOfSplitDay(siblingMinTs, false);
        });
    }

    private void assertSquashPartitionsFoldsTheFront(boolean isCut) throws Exception {
        assertMemoryLeak(new WindowsMappedTruncateFacade(), () -> {
            for (boolean isDeadSpaceDropped : new boolean[]{false, true}) {
                for (boolean isGenerationLeft : new boolean[]{false, true}) {
                    final String shape = "isCut=" + isCut + ", isDeadSpaceDropped=" + isDeadSpaceDropped + ", isGenerationLeft=" + isGenerationLeft;
                    dropTable();
                    buildCompositeFrontNextToMoveTailSibling();
                    final long frontNameTxn = frontNameTxnOfDay("x", SPLIT_DAY);
                    final long cutRow = isCut ? 1_000 : 0;
                    NearFullGeometryFixture.plantCommittedRecord(
                            engine,
                            "x",
                            parseMicros(SPLIT_DAY + "T00:00:00.000000Z"),
                            isGenerationLeft ? LAST_GEOMETRY_GENERATION - 1 : LAST_GEOMETRY_GENERATION,
                            0,
                            isDeadSpaceDropped,
                            cutRow,
                            isCut ? timestampOfDayRow(cutRow - 1) : 0,
                            isCut ? timestampOfDayRow(cutRow) : 0
                    );
                    assertRowsOfSplitDay(Long.MAX_VALUE, false);

                    execute("alter table x squash partitions");
                    drainWalQueueOnOneWriter();
                    assertNotSuspended();
                    Assert.assertEquals(shape, 1, partitionCountOfDay("x", SPLIT_DAY));
                    Assert.assertFalse(shape, isComposite("x", SPLIT_DAY));
                    Assert.assertEquals(
                            "rewritten, " + shape,
                            !isDeadSpaceDropped && !isGenerationLeft,
                            frontNameTxn != frontNameTxnOfDay("x", SPLIT_DAY)
                    );
                    assertRowsOfSplitDay(Long.MAX_VALUE, false);

                    insertIntoFront();
                    assertRowsOfSplitDay(Long.MAX_VALUE, true);
                }
            }
        });
    }

    private void assertTypedFrontMatchesExpected() throws Exception {
        assertQuery("select i, c_varchar from x where c_covering = 'b' and ts in '" + SPLIT_DAY + "'")
                .noLeakCheck()
                .assertsPlanContaining("CoveringIndex on: c_covering");
        assertSqlCursors("expected", "select * from x where ts in '" + SPLIT_DAY + "'");
        assertSqlCursors(
                "select i, ts from expected where c_indexed = 'b'",
                "select i, ts from x where c_indexed = 'b' and ts in '" + SPLIT_DAY + "'"
        );
        assertSqlCursors(
                "select i, c_varchar from expected where c_covering = 'b'",
                "select i, c_varchar from x where c_covering = 'b' and ts in '" + SPLIT_DAY + "'"
        );
    }

    private static class DirectoryRefusingFacade extends WindowsMappedTruncateFacade {
        private final String refusedFragment;
        private final AtomicInteger refusedCount = new AtomicInteger();
        private volatile boolean isArmed;

        private DirectoryRefusingFacade(String refusedFragment) {
            this.refusedFragment = refusedFragment;
        }

        @Override
        public int mkdir(LPSZ path, int mode) {
            return isRefused(path) ? -1 : super.mkdir(path, mode);
        }

        @Override
        public int mkdirs(Path path, int mode) {
            return isRefused(path) ? -1 : super.mkdirs(path, mode);
        }

        private boolean isRefused(Utf8Sequence path) {
            if (isArmed && Utf8s.containsAscii(path, refusedFragment)) {
                refusedCount.incrementAndGet();
                return true;
            }
            return false;
        }
    }

    private static class GeometryWriteFailingFacade extends WindowsMappedTruncateFacade {
        private volatile long geometryFd = -1;
        private volatile boolean isArmed;

        @Override
        public synchronized long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (isArmed && Utf8s.containsAscii(name, TableUtils.PARTITION_GEOMETRY_FILE_NAME + '.')) {
                geometryFd = fd;
            }
            return fd;
        }

        @Override
        public long write(long fd, long address, long len, long offset) {
            final long written = super.write(fd, address, len, offset);
            return isArmed && fd == geometryFd ? -1 : written;
        }
    }

    private static class SymbolReadRefusingFacade extends WindowsMappedTruncateFacade {
        private final String refusedDirectory;
        private final AtomicInteger refusedCount = new AtomicInteger();
        private volatile boolean isArmed;

        private SymbolReadRefusingFacade(String refusedDirectory) {
            this.refusedDirectory = refusedDirectory;
        }

        @Override
        public long openRO(LPSZ name) {
            if (isArmed && Utf8s.containsAscii(name, refusedDirectory) && Utf8s.endsWithAscii(name, Files.SEPARATOR + "s.d")) {
                refusedCount.incrementAndGet();
                return -1;
            }
            return super.openRO(name);
        }
    }
}
