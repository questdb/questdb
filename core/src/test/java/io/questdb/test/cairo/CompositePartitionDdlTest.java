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

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.test.AbstractCairoTest;
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
     * Same {@code removePartition} bug, exercised against the LAST partition (ordinal 4 of 5) instead of
     * a middle one -- covers the tail of the attached-partitions region specifically.
     */
    @Test
    public void testDropLastPartitionMatchesPlainEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulateTwins();
            engine.releaseInactive();

            // Day 5 of 5 -- ordinal 4, the table's last (still non-first) partition.
            execute("alter table c drop partition list '2020-01-05'");
            execute("alter table p drop partition list '2020-01-05'");
            engine.releaseInactive();

            assertSqlCursors("select ts, exchange, px from p order by ts", "select ts, exchange, px from c order by ts");
            assertSqlCursors("select count() from p", "select count() from c");
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n8\n");
            assertSqlCursors(
                    "select partitionCount from table_storage() where tableName = 'p'",
                    "select partitionCount from table_storage() where tableName = 'c'");
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
            engine.releaseInactive();

            assertSqlCursors("select ts, exchange, px from p order by ts", "select ts, exchange, px from c order by ts");
            assertSqlCursors("select count() from p", "select count() from c");
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n10\n");
            assertSqlCursors(
                    "select partitionCount from table_storage() where tableName = 'p'",
                    "select partitionCount from table_storage() where tableName = 'c'");

            execute("alter table c convert partition to native list '2020-01-03'");
            execute("alter table p convert partition to native list '2020-01-03'");
            engine.releaseInactive();

            assertSqlCursors("select ts, exchange, px from p order by ts", "select ts, exchange, px from c order by ts");
            assertSqlCursors("select count() from p", "select count() from c");
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns("count\n10\n");
        });
    }

    /**
     * {@code squashSplitPartitions} bug: {@code updatePartitionSizeAndTxnByRawIndex(targetPartitionIndex *
     * LONGS_PER_TX_ATTACHED_PARTITION, ...)}, another ORDINAL x stride -&gt; RAW conversion using the
     * hardcoded stride-4 constant, but ONLY on the {@code copyTargetFrame} branch: taken when
     * {@code canSquashOverwritePartitionTail} says the target's tail can't be overwritten in place (an
     * open reader holds the scoreboard range) yet the caller passes {@code force=true} anyway. That is
     * exactly what {@code ALTER TABLE ... SQUASH PARTITIONS} does (it calls {@code squashPartitionForce}
     * for every ordinal), mirroring {@code O3SquashPartitionTest#testSquashPartitionsOnNonEmptyTable}'s own
     * idiom -- an open reader across the split, then an explicit {@code SQUASH PARTITIONS} that forces the
     * merge anyway. An extra leading day here shifts the split target to ordinal 1 (non-first).
     * <p>
     * All row content is deterministic (no {@code rnd_*} functions) so the two independently-executed
     * {@code insert into c ...}/{@code insert into p ...} statements produce byte-identical rows
     * regardless of execution order or RNG state.
     */
    @Test
    public void testSquashNonFirstPartitionMatchesPlainEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);

            execute("create table c (ts timestamp, exchange symbol, px double) timestamp(ts) partition by day, exchange");
            execute("create table p (ts timestamp, exchange symbol, px double) timestamp(ts) partition by day");

            // Day 2020-02-03 (ordinal 0): a small seed day so the split target below is non-first.
            String seedRows = " values ('2020-02-03T00:00:00.000000Z','A',1.0), ('2020-02-03T12:00:00.000000Z','B',1.5)";
            execute("insert into c" + seedRows);
            execute("insert into p" + seedRows);

            // In-order bulk insert spanning day 2020-02-04 fully (1440 rows) and spilling into day
            // 2020-02-05 (1320 rows) -- gives 3 day partitions: 2020-02-03 (0), 2020-02-04 (1, the split
            // target below -- non-first), 2020-02-05 (2). Mirrors
            // O3SquashPartitionTest#testSquashPartitionsOnNonEmptyTable's own CTAS shape.
            String bulk = " select timestamp_sequence('2020-02-04T00', 60*1000000L) ts," +
                    " (case when x % 2 = 0 then 'A' else 'B' end) exchange, x * 1.0 px" +
                    " from long_sequence(60*(23*2))";
            execute("insert into c" + bulk);
            execute("insert into p" + bulk);

            assertSqlCursors(
                    "select minTimestamp, name from table_partitions('p') order by minTimestamp",
                    "select minTimestamp, name from table_partitions('c') order by minTimestamp");
            assertQuery("select count() from table_partitions('c')")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");

            // Hold a reader open on BOTH twins across the split + forced squash: this is what makes
            // canSquashOverwritePartitionTail(1) return false (scoreboard range [0, txn) unavailable),
            // forcing squashSplitPartitions into the buggy copyTargetFrame=true branch instead of the
            // (unaffected) in-place-overwrite branch.
            try (
                    TableReader ignoredC = getReader("c");
                    TableReader ignoredP = getReader("p")
            ) {
                // O3 insert into day 2020-02-04 (ordinal 1, non-first): with split-min-size=1 this splits
                // it into two physical partitions (day2a ordinal 1, day2b ordinal 2); day 2020-02-05 shifts
                // to ordinal 3. The open readers above prevent this from being auto-squashed back inline.
                String split = " select timestamp_sequence('2020-02-04T20:01', 1000000L) ts, 'A' exchange," +
                        " (x + 100000) * 1.0 px" +
                        " from long_sequence(200)";
                execute("insert into c" + split);
                execute("insert into p" + split);

                assertSqlCursors(
                        "select minTimestamp, numRows, name from table_partitions('p') order by minTimestamp",
                        "select minTimestamp, numRows, name from table_partitions('c') order by minTimestamp");
                assertQuery("select count() from table_partitions('c')")
                        .noLeakCheck().noRandomAccess().expectSize().returns("count\n4\n");

                // Force the squash despite the reader lock -- drives squashSplitPartitions's
                // copyTargetFrame=true branch at targetPartitionIndex=1 (non-first).
                execute("alter table c squash partitions");
                execute("alter table p squash partitions");
            }
            engine.releaseInactive();

            assertSqlCursors("select ts, exchange, px from p order by ts, px", "select ts, exchange, px from c order by ts, px");
            assertSqlCursors("select count() from p", "select count() from c");
            assertSqlCursors(
                    "select minTimestamp, numRows, name from table_partitions('p') order by minTimestamp",
                    "select minTimestamp, numRows, name from table_partitions('c') order by minTimestamp");
            // The split must have been squashed back to a single physical partition per day (proves
            // squashSplitPartitions actually ran to completion via the forced/copy branch, rather than
            // being silently skipped).
            assertQuery("select count() from table_partitions('c')")
                    .noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
        });
    }

    /**
     * Builds the composite table {@code c} ({@code partition by day, exchange}) and its plain twin
     * {@code p} ({@code partition by day}), then inserts byte-for-byte identical rows into both: 5 day
     * partitions (2020-01-01 .. 2020-01-05), 2 rows per day (one per exchange, A and B) -- 10 rows total.
     * Mirrors {@code CompositeEndToEndTest#createAndPopulateTwins}.
     */
    private void createAndPopulateTwins() throws Exception {
        execute("create table c (ts timestamp, exchange symbol, px double) timestamp(ts) partition by day, exchange");
        execute("create table p (ts timestamp, exchange symbol, px double) timestamp(ts) partition by day");

        final String rows = " values " +
                "('2020-01-01T00:00:00.000000Z','A',1.0), ('2020-01-01T12:00:00.000000Z','B',1.5), " +
                "('2020-01-02T00:00:00.000000Z','A',2.0), ('2020-01-02T12:00:00.000000Z','B',2.5), " +
                "('2020-01-03T00:00:00.000000Z','A',3.0), ('2020-01-03T12:00:00.000000Z','B',3.5), " +
                "('2020-01-04T00:00:00.000000Z','A',4.0), ('2020-01-04T12:00:00.000000Z','B',4.5), " +
                "('2020-01-05T00:00:00.000000Z','A',5.0), ('2020-01-05T12:00:00.000000Z','B',5.5)";
        execute("insert into c" + rows);
        execute("insert into p" + rows);
    }
}
