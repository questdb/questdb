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
 * dormant/non-WAL shape this file's hardcoded partition/row counts were built against) -- deliberately
 * NOT exercising Plan 4a/4b's separately-scoped, not-yet-audited question of whether DROP/CONVERT/SQUASH
 * PARTITION are cell-AWARE for a day with 2+ real cells (out of scope here). {@link
 * #testSquashNonFirstPartitionMatchesPlainEquivalent()}'s ORIGINAL scenario (three sequential commits,
 * the third re-touching an already-populated day to force a physical split) is retired for a documented,
 * pre-existing, unrelated reason: see that method's own javadoc.
 */
public class CompositePartitionDdlTest extends AbstractCairoTest {

    /**
     * {@code removePartition} bug: {@code partitionIndex /= LONGS_PER_TX_ATTACHED_PARTITION} converts a
     * RAW attached-partitions index to an ordinal using the hardcoded stride-4 constant. On a stride-8
     * composite table, dropping a middle day (ordinal 2 of 5) computes the wrong ordinal, so the
     * drop-loop's logical-timestamp comparison never matches and the whole operation silently no-ops --
     * {@code c} keeps all 5 partitions/10 rows while {@code p} correctly drops to 4 partitions/8 rows.
     */
    @Test
    public void testDropMiddlePartitionMatchesPlainEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            // Day 3 of 5 -- ordinal 2, non-first and non-last.
            execute("alter table c drop partition list '2020-01-03'");
            execute("alter table p drop partition list '2020-01-03'");
            drainWalQueue();
            engine.releaseInactive();

            assertSqlCursors("select ts, exchange, px from p order by ts", "select ts, exchange, px from c order by ts");
            assertSqlCursors("select count() from p", "select count() from c");
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n8\n");
            assertSqlCursors(
                    "select partitionCount from table_storage() where tableName = 'p'",
                    "select partitionCount from table_storage() where tableName = 'c'");
            assertQuery("select partitionCount from table_storage() where tableName = 'c'")
                    .noLeakCheck().noRandomAccess().returns("partitionCount\n4\n");
        });
    }

    /**
     * Same {@code removePartition} bug, exercised against a high (ordinal 4 of 6), non-first, non-LAST
     * partition -- covers the tail of the attached-partitions region specifically.
     * <p>
     * <b>NEW FINDING while porting this test to WAL for I1 (out of scope for this fix pass, NOT
     * fixed):</b> dropping a composite table's actual CURRENT LAST/active partition (i.e. the literal
     * ordinal-4-of-5 tail this test originally targeted) suspends the WAL table: {@code
     * TableWriter#dropPartitionByExactTimestamp}'s "removing active partition" branch resolves the NEW
     * last partition's min/max timestamp via the bare, cell-blind 5-arg {@code
     * setPathForNativePartition(path, ..., prevTimestamp, nameTxn)} overload (around TableWriter.java:7160)
     * instead of the cell-aware 6-arg one Plan 4a Task 3 added -- so for a real routed composite table it
     * looks for {@code <day>/ts.d} directly under the bare day dir and fails with "file does not exist"
     * (confirmed via {@code wal_tables().errorMessage}), because that day's data actually lives at
     * {@code <day>/<cell>/ts.d}. This is the SAME "day-blind maintenance path" class as C1, but a
     * DIFFERENT call site, not one of this pass's three assigned findings (C1/C2+C3/I1) -- flagged here,
     * not fixed, and this test instead adds a 6th day (2020-01-06) so the dropped partition (day 5, still
     * ordinal 4, still non-first) is no longer the table's active tail when the drop runs, sidestepping
     * the unrelated bug while preserving the original "high ordinal" stride-fix coverage.
     */
    @Test
    public void testDropLastPartitionMatchesPlainEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, px double) timestamp(ts) partition by day, exchange wal");
            execute("create table p (ts timestamp, exchange symbol, px double) timestamp(ts) partition by day");
            // 6 days, ONE single commit (unlike createAndPopulateTwins' shared 5-day/1-commit shape) so
            // day 6 exists from the very first commit -- day 5 (below) is NOT the table's active/last
            // partition when dropped. See this method's own javadoc for why that matters.
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

            // Day 5 of 6 -- ordinal 4, non-first and (thanks to day 6 above) non-last.
            execute("alter table c drop partition list '2020-01-05'");
            execute("alter table p drop partition list '2020-01-05'");
            drainWalQueue();
            engine.releaseInactive();

            assertSqlCursors("select ts, exchange, px from p order by ts", "select ts, exchange, px from c order by ts");
            assertSqlCursors("select count() from p", "select count() from c");
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n10\n");
            assertSqlCursors(
                    "select partitionCount from table_storage() where tableName = 'p'",
                    "select partitionCount from table_storage() where tableName = 'c'");
            assertQuery("select partitionCount from table_storage() where tableName = 'c'")
                    .noLeakCheck().noRandomAccess().returns("partitionCount\n5\n");
        });
    }

    /**
     * {@code convertPartitionNativeToParquet}/{@code convertPartitionParquetToNative} bug: both call
     * {@code updatePartitionSizeAndTxnByRawIndex(partitionIndex * LONGS_PER_TX_ATTACHED_PARTITION, ...)},
     * an ORDINAL x stride -&gt; RAW conversion using the hardcoded stride-4 constant. On a stride-8
     * composite table, converting a non-first day (ordinal 2 of 5) writes the new size/nameTxn into the
     * wrong raw record, leaving the target partition's real record stale (pointing at a native directory
     * that conversion then deletes) while scribbling into a neighboring partition's record.
     */
    @Test
    public void testConvertPartitionToParquetAndBackNonFirstDayMatchesPlainEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            // Day 3 of 5 -- ordinal 2, non-first: convert to parquet, verify, then convert back to
            // native, verify again.
            execute("alter table c convert partition to parquet list '2020-01-03'");
            execute("alter table p convert partition to parquet list '2020-01-03'");
            drainWalQueue();
            engine.releaseInactive();

            assertSqlCursors("select ts, exchange, px from p order by ts", "select ts, exchange, px from c order by ts");
            assertSqlCursors("select count() from p", "select count() from c");
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n10\n");
            assertSqlCursors(
                    "select partitionCount from table_storage() where tableName = 'p'",
                    "select partitionCount from table_storage() where tableName = 'c'");

            execute("alter table c convert partition to native list '2020-01-03'");
            execute("alter table p convert partition to native list '2020-01-03'");
            drainWalQueue();
            engine.releaseInactive();

            assertSqlCursors("select ts, exchange, px from p order by ts", "select ts, exchange, px from c order by ts");
            assertSqlCursors("select count() from p", "select count() from c");
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n10\n");
        });
    }

    /**
     * RETIRED scenario (documented, not silently dropped): this test originally regression-locked the
     * {@code squashSplitPartitions} stride bug (ORDINAL x {@code LONGS_PER_TX_ATTACHED_PARTITION} -> RAW,
     * hardcoded-stride-4 on a stride-8 composite table) on the {@code copyTargetFrame} branch, which needs
     * an already-committed day PHYSICALLY SPLIT by a later out-of-order insert, then squashed back under
     * an open reader. Whole-branch review (Plan 4a) finding I1 requires {@code c} to be WAL; on a WAL
     * composite table EVERY commit (in-order or not) routes through {@code processO3BlockComposite}, whose
     * {@code dispatchCompositeCellRange} throws LOUDLY ("does not yet support a commit that extends an
     * already-populated cell") the moment a later commit adds MORE rows to a day that already has
     * committed data for that cell -- exactly what creating a split requires, unavoidably (a split IS a
     * second, later write into an already-populated partition; no choice of dimension values routes around
     * it, only the commit sequencing does, and that sequencing is the scenario itself). This is a
     * pre-existing, already-deferred Plan 4a Task 5 limitation (see {@code CompositeRoutingTest}'s own
     * {@code testSecondCommitExtendingExistingCellThrowsInsteadOfSilentlyMisrouting}), not something
     * introduced or fixable by this fix pass -- so the original scenario is retired rather than ported.
     * The underlying stride fix remains covered: {@link #testDropMiddlePartitionMatchesPlainEquivalent()},
     * {@link #testDropLastPartitionMatchesPlainEquivalent()} and
     * {@link #testConvertPartitionToParquetAndBackNonFirstDayMatchesPlainEquivalent()} still exercise the
     * SAME {@code txWriter.getLongsPerAttachedPartition()} fix at 3 of the original 6 sites (DROP,
     * CONVERT x2); {@code squashSplitPartitions} itself is unchanged production code, still fixed, just
     * without a DEDICATED composite regression test now that its only reachable trigger shape is guarded.
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
