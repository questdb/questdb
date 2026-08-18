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

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Whole-branch review CRITICAL fix (Plan 3/3b, composite partitioning): six pre-existing raw-index
 * &lt;-&gt; ordinal conversions in {@code TableWriter} were missed when {@code TxReader}/{@code TxWriter}
 * were refactored to an INSTANCE stride field ({@code longsPerAttachedPartition}, 4 plain / 8 composite)
 * and still hardcoded the plain-table constant {@code TableUtils#LONGS_PER_TX_ATTACHED_PARTITION} (4):
 * {@code removePartition} (DROP PARTITION), {@code convertPartitionNativeToParquet},
 * {@code convertPartitionParquetToNative}, {@code switchNativePartitionWithParquet},
 * {@code squashSplitPartitions}, and {@code o3ConsumePartitionUpdateSink}. On a composite (stride-8)
 * table these compute the wrong raw {@code _txn} slot for any partition ordinal &gt;= 1, corrupting
 * partition DDL. A composite table is user-creatable and writable today (real (ts, cellKey) write
 * routing is Plan 4 -- every row lands at cellKey 0 for now), so this is reachable with ordinary DDL
 * the moment a composite table has 2+ partitions.
 * <p>
 * Each test below builds a composite table {@code c} ({@code partition by day, exchange}) side by side
 * with a plain twin {@code p} ({@code partition by day}), populates both with byte-for-byte identical
 * rows across >= 3 day partitions, drives the affected operation against a NON-FIRST partition ordinal,
 * and asserts {@code c} reads back identically to {@code p}. Pre-fix, each of these is RED (wrong
 * row/partition counts, corrupted reads, or an exception); post-fix (stride-aware conversions via
 * {@code txWriter.getLongsPerAttachedPartition()}), GREEN.
 * <p>
 * <b>Whole-branch review (Plan 4a) finding I1 update:</b> {@code c} was originally created WITHOUT
 * {@code WAL} (the harness default at the time). I1 now rejects a non-WAL composite table at CREATE
 * (its direct, synchronous row-append path hardcodes cellKey 0 and never routes -- see
 * {@code CreateTableOperationBuilderImpl#resolvePartitionSpec}), so {@code c} below is now created
 * {@code WAL}. Every row still uses the SAME single {@code exchange} value ({@code 'A'}) throughout --
 * this bug is pure {@code _txn} raw-index/ordinal-stride arithmetic, completely insensitive to how many
 * distinct dimension values are in play, so a single value continues to exercise it exactly, while
 * keeping every physical day exactly ONE cell (byte-identical partition topology to the original
 * dormant/non-WAL shape this file's hardcoded partition/row counts were built against).
 * <p>
 * <b>Plan 4a DEFINITIVE DDL gate sweep update:</b> the "is DROP/CONVERT/SQUASH PARTITION cell-AWARE for
 * a day with 2+ real cells" question this class's tests originally left deliberately unexercised is now
 * answered: NO, and each is unsafe well beyond the stride bug this class was written to regression-lock
 * (see {@code CompositeUnsupportedOpsTest} for the full multi-cell evidence, including a live-reproduced
 * infinite loop for DROP PARTITION) -- so DROP PARTITION and CONVERT PARTITION are now gated
 * unconditionally for any real composite table, single-cell-per-day or not. {@link
 * #testDropMiddlePartitionMatchesPlainEquivalent()}, {@link #testDropLastPartitionMatchesPlainEquivalent()}
 * and {@link #testConvertPartitionToParquetAndBackNonFirstDayMatchesPlainEquivalent()} were updated in
 * place to assert the new guard fires (rather than stride-correct equivalence, no longer the observable
 * behavior for composite {@code c} regardless of ordinal) while their plain twin {@code p} still proves
 * the underlying stride fix remains correct and unaffected. {@link
 * #testSquashNonFirstPartitionMatchesPlainEquivalent()}'s ORIGINAL scenario (three sequential commits,
 * the third re-touching an already-populated day to force a physical split) was already retired for a
 * documented, pre-existing, unrelated reason (see that method's own javadoc) before this update; SQUASH
 * PARTITIONS is now ALSO separately gated (see {@code CompositeUnsupportedOpsTest}), for the same
 * reason DROP/CONVERT are.
 */
public class CompositePartitionDdlTest extends AbstractCairoTest {

    /**
     * ORIGINALLY a regression test for the {@code removePartition} stride bug ({@code partitionIndex /=
     * LONGS_PER_TX_ATTACHED_PARTITION} hardcoded stride-4 on a stride-8 composite table). The stride fix
     * itself is still correct and still in production code, but Plan 4a's DEFINITIVE DDL gate sweep
     * (composite-partitioning branch, {@code CompositeUnsupportedOpsTest}) found DROP PARTITION unsafe
     * for a REAL, routed composite table for reasons well beyond the stride bug -- most severely, an
     * empirically-reproduced INFINITE LOOP for a day with 2+ cells (see that test class's own javadoc) --
     * and gated it unconditionally in {@code TableWriter#removePartition}. This test now asserts that
     * gate fires instead of asserting stride-correct equivalence, which is no longer the observable
     * behavior for ANY real composite table regardless of stride correctness. {@code p} (the plain twin)
     * is unaffected and still drops the middle partition normally, proving the gate is composite-only.
     */
    @Test
    public void testDropMiddlePartitionMatchesPlainEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            // Day 3 of 5 -- ordinal 2, non-first and non-last.
            execute("alter table p drop partition list '2020-01-03'");
            // Sub-project 1B: whole-day DROP PARTITION now WORKS on a composite table, so this test
            // returns to the assertion it was originally written for -- stride-correct equivalence
            // with the plain twin. It spent one revision asserting the WAL-apply suspension and
            // another asserting the statement-time refusal; both were scaffolding around a gate that
            // no longer exists for this shape.
            execute("alter table c drop partition list '2020-01-03'");
            drainWalQueue();

            Assert.assertFalse("a supported drop must not suspend the table",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));
            assertSqlCursors("select ts, exchange, px from p order by ts, exchange",
                    "select ts, exchange, px from c order by ts, exchange");

            engine.releaseInactive();
            assertQuery("select count() from p").noLeakCheck().noRandomAccess().expectSize().returns("count\n8\n");
            assertQuery("select partitionCount from table_storage() where tableName = 'p'")
                    .noLeakCheck().noRandomAccess().returns("partitionCount\n4\n");
        });
    }

    /**
     * Same underlying stride-bug coverage as {@link #testDropMiddlePartitionMatchesPlainEquivalent()},
     * originally exercised against a high (ordinal 4 of 6), non-first, non-LAST partition. Per that
     * test's own updated javadoc, DROP PARTITION is now gated unconditionally for any real composite
     * table (not just the active-tail shape a prior fix pass flagged but left unfixed) -- so this test
     * now asserts the SAME gate fires here too, on a differently-shaped (higher-ordinal) target, and that
     * {@code p} is unaffected.
     */
    @Test
    public void testDropLastPartitionMatchesPlainEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, px double) timestamp(ts) partition by day, exchange wal");
            execute("create table p (ts timestamp, exchange symbol, px double) timestamp(ts) partition by day");
            final String rows = " values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','A',1.5), " +
                    "('2020-01-02T00:00:00.000000Z','A',2.0), ('2020-01-02T12:00:00.000000Z','A',2.5), " +
                    "('2020-01-03T00:00:00.000000Z','A',3.0), ('2020-01-03T12:00:00.000000Z','A',3.5), " +
                    "('2020-01-04T00:00:00.000000Z','A',4.0), ('2020-01-04T12:00:00.000000Z','A',4.5), " +
                    "('2020-01-05T00:00:00.000000Z','A',5.0), ('2020-01-05T12:00:00.000000Z','A',5.5), " +
                    "('2020-01-06T00:00:00.000000Z','A',6.0), ('2020-01-06T12:00:00.000000Z','A',6.5)";
            execute("insert into c" + rows);
            execute("insert into p" + rows);
            drainWalQueue();
            engine.releaseInactive();

            // Day 5 of 6 -- ordinal 4, non-first and non-last.
            execute("alter table p drop partition list '2020-01-05'");
            // Sub-project 1B: whole-day DROP PARTITION now WORKS on a composite table, so this test
            // returns to the assertion it was originally written for -- stride-correct equivalence
            // with the plain twin. It spent one revision asserting the WAL-apply suspension and
            // another asserting the statement-time refusal; both were scaffolding around a gate that
            // no longer exists for this shape.
            execute("alter table c drop partition list '2020-01-05'");
            drainWalQueue();

            Assert.assertFalse("a supported drop must not suspend the table",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));
            assertSqlCursors("select ts, exchange, px from p order by ts, exchange",
                    "select ts, exchange, px from c order by ts, exchange");

            engine.releaseInactive();
            assertQuery("select count() from p").noLeakCheck().noRandomAccess().expectSize().returns("count\n10\n");
            assertQuery("select partitionCount from table_storage() where tableName = 'p'")
                    .noLeakCheck().noRandomAccess().returns("partitionCount\n5\n");
        });
    }

    /**
     * ORIGINALLY a regression test for the {@code convertPartitionNativeToParquet}/
     * {@code convertPartitionParquetToNative} stride bug. Plan 4a's DEFINITIVE DDL gate sweep found
     * BOTH conversion directions independently cell-blind well beyond the stride bug (every path is
     * built with the bare, non-cell-aware {@code setPathForNativePartition} overload -- see
     * {@code CompositeUnsupportedOpsTest}) and gated both unconditionally. This test now asserts CONVERT
     * TO PARQUET is rejected (the round-trip back to native is consequently unreachable via ordinary SQL
     * for a composite table, so it is not separately exercised here -- {@code
     * CompositeUnsupportedOpsTest#testConvertPartitionToNativeGated} covers that direction directly). The
     * plain twin {@code p} is unaffected and completes the full round-trip normally.
     */
    @Test
    public void testConvertPartitionToParquetAndBackNonFirstDayMatchesPlainEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            // Day 3 of 5 -- ordinal 2, non-first.
            execute("alter table p convert partition to parquet list '2020-01-03'");
            execute("alter table c convert partition to parquet list '2020-01-03'");
            drainWalQueue();

            Assert.assertTrue("c must be suspended by the new CONVERT PARTITION guard",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));
            assertQuery("select suspended, errorMessage like '%composite partitioning does not yet support CONVERT PARTITION TO PARQUET%' clearMessage " +
                    "from wal_tables() where name = 'c'")
                    .noLeakCheck().noRandomAccess()
                    .returns("suspended\tclearMessage\ntrue\ttrue\n");

            engine.releaseInactive();
            execute("alter table p convert partition to native list '2020-01-03'");
            drainWalQueue();
            engine.releaseInactive();

            assertQuery("select count() from p").noLeakCheck().noRandomAccess().expectSize().returns("count\n10\n");
        });
    }

    /**
     * RETIRED scenario (documented, not silently dropped): this test originally regression-locked the
     * {@code squashSplitPartitions} stride bug (ORDINAL x {@code LONGS_PER_TX_ATTACHED_PARTITION} -> RAW,
     * hardcoded-stride-4 on a stride-8 composite table) on the {@code copyTargetFrame} branch, which needs
     * an already-committed day PHYSICALLY SPLIT by a later out-of-order insert, then squashed back under
     * an open reader. Whole-branch review (Plan 4a) finding I1 requires {@code c} to be WAL.
     * <p>
     * UPDATE (Plan 4b Task 1b): extending an already-populated cell (a prerequisite for a split, which is
     * always a second write into an already-populated partition) is no longer blocked by {@code
     * dispatchCompositeCellRange}'s own guard -- that guard was removed once the cell-blind
     * partition-remove-candidates purge it was protecting against was fixed (see {@code TableWriter}'s own
     * updated docs and {@code CompositeRoutingTest#testSecondCommitExtendingExistingCellOutOfOrderMergeMatchesPlainTwin}).
     * The scenario remains impractical to construct here for an unrelated, purely resource reason: a
     * genuine O3 SPLIT only triggers once a partition's prefix exceeds {@code
     * TableWriter#getPartitionO3SplitThreshold()} (default {@code cairo.o3.partition.split.min.size},
     * 50 MiB) -- far beyond any ordinary unit test's row counts, composite or plain. This is a scale
     * limitation of the test harness, not a safety gate; {@code squashSplitPartitions}'s own
     * {@code partitionRemoveCandidates} call sites were threaded with {@code cellKey} defensively (Plan 4b
     * Task 1b) even though unexercised by any test today. The underlying stride fix remains covered:
     * {@link #testDropMiddlePartitionMatchesPlainEquivalent()},
     * {@link #testDropLastPartitionMatchesPlainEquivalent()} and
     * {@link #testConvertPartitionToParquetAndBackNonFirstDayMatchesPlainEquivalent()} still exercise the
     * SAME {@code txWriter.getLongsPerAttachedPartition()} fix at 3 of the original 6 sites (DROP,
     * CONVERT x2); {@code squashSplitPartitions} itself is unchanged production code, still fixed, just
     * without a DEDICATED composite regression test given the scale needed to trigger it.
     * <p>
     * What this test asserts instead: I1's own new guard -- a non-WAL composite CREATE (the only way the
     * original scenario could ever have been built) is now rejected loudly, rather than silently degrading
     * to single-cell routing.
     */
    @Test
    public void testSquashNonFirstPartitionMatchesPlainEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("create table c (ts timestamp, exchange symbol, px double) timestamp(ts) partition by day, exchange");
                Assert.fail("expected CREATE of a non-WAL composite table to be rejected");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "composite partitioning requires a WAL table");
            }
            // BYPASS WAL is just as non-WAL and must be rejected identically.
            try {
                execute("create table c2 (ts timestamp, exchange symbol, px double) timestamp(ts) partition by day, exchange bypass wal");
                Assert.fail("expected CREATE of a BYPASS WAL composite table to be rejected");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "composite partitioning requires a WAL table");
            }
        });
    }

    /**
     * Builds the composite table {@code c} ({@code partition by day, exchange}) and its plain twin
     * {@code p} ({@code partition by day}), then inserts byte-for-byte identical rows into both: 5 day
     * partitions (2020-01-01 .. 2020-01-05), 2 rows per day -- 10 rows total. Mirrors {@code
     * CompositeEndToEndTest#createAndPopulateTwins}.
     * <p>
     * {@code c} is WAL (required by I1) and every row uses the single exchange value {@code 'A'} (see
     * class javadoc for why) -- one commit, brand-new cells throughout, so this reaches the well-
     * supported single-commit routing path (Plan 4a Task 4), not the guarded extend-an-existing-cell one.
     */
    private void createAndPopulateTwins() throws Exception {
        execute("create table c (ts timestamp, exchange symbol, px double) timestamp(ts) partition by day, exchange wal");
        execute("create table p (ts timestamp, exchange symbol, px double) timestamp(ts) partition by day");

        final String rows = " values " +
                "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','A',1.5), " +
                "('2020-01-02T00:00:00.000000Z','A',2.0), ('2020-01-02T12:00:00.000000Z','A',2.5), " +
                "('2020-01-03T00:00:00.000000Z','A',3.0), ('2020-01-03T12:00:00.000000Z','A',3.5), " +
                "('2020-01-04T00:00:00.000000Z','A',4.0), ('2020-01-04T12:00:00.000000Z','A',4.5), " +
                "('2020-01-05T00:00:00.000000Z','A',5.0), ('2020-01-05T12:00:00.000000Z','A',5.5)";
        execute("insert into c" + rows);
        execute("insert into p" + rows);
        drainWalQueue();
    }
}
