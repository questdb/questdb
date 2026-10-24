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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.log.Log;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.PropertyKey.CAIRO_WAL_APPLY_BLOCK_SIZE;
import static io.questdb.PropertyKey.CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT;
import static io.questdb.PropertyKey.CAIRO_WAL_MAX_LAG_SIZE;
import static io.questdb.cairo.TableUtils.DETACHED_DIR_MARKER;
import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

public class WalWriterReplaceRangeTest extends AbstractCairoTest {

    @Test
    public void testFullTimelineReplaceEmptiesPopulatedTable() throws Exception {
        assertFullTimelineReplaceLeavesTableEmpty(true);
    }

    @Test
    public void testFullTimelineReplaceOnAlreadyEmptyTable() throws Exception {
        assertFullTimelineReplaceLeavesTableEmpty(false);
    }

    @Test
    public void testManyTransactionsSkippedWhenTruncateIfFound() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(CAIRO_WAL_MAX_LAG_SIZE, 1);
            setProperty(CAIRO_WAL_APPLY_BLOCK_SIZE, 1);

            execute("create table stress (id long, ts timestamp, value long) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("stress");

            long lastMinuteStart = MicrosTimestampDriver.floor("2022-02-24T23:59");

            for (int i = 0; i < 100; i++) {
                try (WalWriter ww = engine.getWalWriter(tableToken)) {
                    // Add a new row with the current replace value
                    TableWriter.Row row = ww.newRow(lastMinuteStart + 30_000_000); // Middle of the last minute
                    row.putLong(0, i);
                    row.putLong(2, i * 100);
                    row.append();
                    ww.commit();
                }
            }
            execute("truncate table stress");
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                // Add a new row with the current replace value
                TableWriter.Row row = ww.newRow(lastMinuteStart + 30_000_000); // Middle of the last minute
                row.putLong(0, 1001);
                row.putLong(2, (1001) * 100);
                row.append();
                ww.commit();
            }

            // Apply any remaining WAL entries
            drainWalQueue();

            // Verify the total count
            // Expected: rows before 23:00 + 3 rows in the replace ranges
            // Based on the test run, the original data has rows from 23:00 to 23:19:26 that get replaced
            assertQuery("select count(*) from stress")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");
            try (TableReader rdr = engine.getReader(tableToken)) {
                var txn = rdr.getTxn();
                Assert.assertEquals(3, txn); // Expecting many transactions to be skipped
            }
        });
    }

    @Test
    public void testMatViewPermittedColumnAltersStayNonStructural() throws Exception {
        // Locks in the assumption behind the materialized-view skip-barrier exemption in
        // ApplyWal2TableJob.calculateSkipTransactionCount: a mat view scans PAST a non-data barrier (so an
        // insert before it can still be skipped) unless the barrier is an SQL transaction. That is safe only
        // because the column operations a mat view is allowed to run - ADD INDEX, DROP INDEX, SYMBOL CAPACITY
        // - are non-structural, so they commit as walId > 0 SQL transactions (hard barriers via the
        // futureType != SQL carve-out) rather than walId < 1 structural changes (exempted soft barriers). The
        // one row-order-dependent structural op, a column type conversion, is structural AND separately
        // rejected on a mat view, so it can never reach the exempted path.
        //
        // If any of these column alters were ever made structural (added to AlterOperation.isStructural()),
        // it would silently become an exempted soft barrier on a mat view and could let a primary and a
        // replica skip different transactions ahead of it - reopening the symbol-map divergence the barrier
        // closes. This test fails the moment that assumption breaks.
        assertMemoryLeak(() -> {
            execute("create table t (s1 symbol, s2 symbol index, ts timestamp) timestamp(ts) partition by DAY WAL");

            // The column alters that a mat view is permitted to run must be non-structural -> SQL hard barrier.
            assertAlterIsStructural("alter table t alter column s1 add index", false);
            assertAlterIsStructural("alter table t alter column s2 drop index", false);
            assertAlterIsStructural("alter table t alter column s1 symbol capacity 256", false);

            // A column type conversion is structural - the op the barrier exists for - and is separately
            // rejected on a mat view, so it never reaches the exempted soft-barrier path.
            assertAlterIsStructural("alter table t alter column s1 type varchar", true);
        });
    }

    @Test
    public void testNonDataWalTxnTypesStopTheSkipScan() {
        // calculateSkipTransactionCount stops the skip look-ahead at every non-data WAL transaction via
        // !isDataType(futureType) - a structural change, an SQL statement, a TRUNCATE, a MAT_VIEW_INVALIDATE or
        // a VIEW_DEFINITION. The end-to-end tests drive the MAT_VIEW_INVALIDATE, structural and SQL arms, but
        // VIEW_DEFINITION (type 5) has no WalWriter entry point here to commit, so its barrier behaviour rests
        // entirely on this classification. Pin it: only DATA and MAT_VIEW_DATA are data types, every other type
        // is a barrier. If a new type were added and wrongly classified as data, an insert before it could be
        // skipped and reopen the cross-instance symbol-map divergence this guards against.
        Assert.assertTrue(WalTxnType.isDataType(WalTxnType.DATA));
        Assert.assertTrue(WalTxnType.isDataType(WalTxnType.MAT_VIEW_DATA));
        Assert.assertFalse(WalTxnType.isDataType(WalTxnType.NONE));
        Assert.assertFalse(WalTxnType.isDataType(WalTxnType.SQL));
        Assert.assertFalse(WalTxnType.isDataType(WalTxnType.TRUNCATE));
        Assert.assertFalse(WalTxnType.isDataType(WalTxnType.MAT_VIEW_INVALIDATE));
        Assert.assertFalse(WalTxnType.isDataType(WalTxnType.VIEW_DEFINITION));
    }

    @Test
    public void testRemovesFirstPartitionNoRowsAdded() throws Exception {
        testRemovesFirstPartitionNoRowsAdded(true);
    }

    @Test
    public void testRemovesFirstPartitionNoRowsAddedNoRowsCommit() throws Exception {
        testRemovesFirstPartitionNoRowsAdded(false);
    }

    @Test
    public void testRemovesLastPartitionNoRowsAdded() throws Exception {
        testRemovesLastPartitionNoRowsAdded(false);
    }

    @Test
    public void testRemovesLastPartitionNoRowsAddedNoRowsCommit() throws Exception {
        testRemovesLastPartitionNoRowsAdded(true);
    }

    @Test
    public void testRemovesLastPartitionNoRowsAddedThenAppends() throws Exception {
        // A REPLACE_RANGE that empties the last partition and writes no rows leaves the writer's
        // partitionTimestampHi pointing at the partition it just deleted: the only place that
        // lowers the field for a replace commit needs at least one written row, and
        // o3ConsumePartitionUpdateSink recomputes txWriter.maxTimestamp without it. When the
        // partition that inherits last place is COMPOSITE, finishO3Commit takes the
        // append-blocked branch and skips the openPartition that would otherwise have papered
        // over the stale field, so the next commit on the same writer works against a partition
        // that no longer exists.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE rg (id LONG, ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                for (int i = 0; i < 400; i++) {
                    appendRow(ww, MicrosTimestampDriver.floor("2022-02-24T00:00:00.000000Z") + i * 180_000_000L, i, i * 10L);
                }
                appendRow(ww, MicrosTimestampDriver.floor("2022-02-25T01:00:00.000000Z"), 1000, 10_000);
                ww.commit();
            }
            drainWalQueue();

            // Backdated write into the middle of 2022-02-24 makes that partition composite.
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                appendRow(ww, MicrosTimestampDriver.floor("2022-02-24T04:00:00.000001Z"), 2000, 20_000);
                ww.commit();
            }
            drainWalQueue();

            // Both commits are drained together so they run through one TableWriter instance:
            // a pooled writer reload would refresh partitionTimestampHi and hide the defect.
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                ww.commitWithParams(
                        MicrosTimestampDriver.floor("2022-02-25T00:00:00.000000Z"),
                        MicrosTimestampDriver.floor("2022-02-26T00:00:00.000000Z"),
                        WAL_DEDUP_MODE_REPLACE_RANGE
                );
                appendRow(ww, MicrosTimestampDriver.floor("2022-02-24T23:59:00.000000Z"), 3000, 30_000);
                ww.commit();
            }
            drainWalQueue();

            Assert.assertFalse("table is suspended", engine.getTableSequencerAPI().isSuspended(tableToken));
            assertQuery("SELECT count() c, min(ts) lo, max(ts) hi FROM rg").expectSize().noRandomAccess().returns("""
                    c\tlo\thi
                    402\t2022-02-24T00:00:00.000000Z\t2022-02-24T23:59:00.000000Z
                    """);
        });
    }

    @Test
    public void testRemovesLastPartitionThenNormalCommitDoesNotSuspend() throws Exception {
        // Reproduces questdb-enterprise #1123. A REPLACE_RANGE commit that removes the table's
        // last partition moves maxTimestamp back to an earlier partition, but partitionTimestampHi
        // (advanced only via Math.max in the replace path) stays at the now-removed partition.
        // finishO3Commit normally re-syncs partitionTimestampHi by re-opening the new last
        // partition, but it skips that when the new last partition is PARQUET. When the SAME writer
        // instance then applies the next non-replace commit, the partition-timestamp consistency
        // assert at the top of TableWriter.processWalCommit fires and the table is suspended.
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            // Data spanning 2022-02-24, -25 and -26; last partition 2022-02-26.
            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(200)");
            drainWalQueue();

            // Make 2022-02-25 (the partition that becomes last once 2022-02-26 is removed) parquet,
            // so finishO3Commit's non-parquet re-open path does not re-sync partitionTimestampHi.
            execute("alter table rg convert partition to parquet list '2022-02-25'");
            drainWalQueue();

            // Commit A: REPLACE_RANGE removing the last partition (2022-02-26), no rows added.
            // maxTimestamp moves back to 2022-02-25; partitionTimestampHi stays at 2022-02-26.
            commitNoRowsWithRangeReplace(tableToken, "2022-02-26", "2022-02-27");

            // Commit B: a plain insert into an earlier (native) partition. Do NOT drain in between,
            // so the apply job runs A and B back-to-back on the same writer instance.
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2022-02-24T13:00"));
                row.putInt(0, 999);
                row.append();
                ww.commit();
            }

            drainWalQueue();

            Assert.assertFalse("table is suspended", engine.getTableSequencerAPI().isSuspended(tableToken));

            // The last partition (2022-02-26) is gone and commit B's row landed in 2022-02-24.
            assertQuery("select count() count, min(ts) min, max(ts) max from rg")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\tmin\tmax\n143\t2022-02-24T12:30:00.000000Z\t2022-02-25T23:45:00.000000Z\n");
        });
    }

    @Test
    public void testReplaceBetweenExisting() throws Exception {
        testReplaceRangeCommit("2022-02-24T16:25", "2022-02-24T16:25", "2022-02-24T16:26");
    }

    @Test
    public void testReplaceCommitAdds2PartitionsBeforeExisting() throws Exception {
        testReplaceCommitAdds2PartitionsBeforeExisting(false);
    }

    @Test
    public void testReplaceCommitAdds2PartitionsBeforeExistingNoRowsCommit() throws Exception {
        testReplaceCommitAdds2PartitionsBeforeExisting(true);
    }

    @Test
    public void testReplaceCommitNotOrdered() throws Exception {
        testReplaceCommitNotOrdered(false);
    }

    @Test
    public void testReplaceCommitNotOrderedNoRowsCommit() throws Exception {
        testReplaceCommitNotOrdered(true);
    }

    @Test
    public void testReplaceCommitRemoves2PartitionsAndAdds1() throws Exception {
        testReplaceCommitRemoves2PartitionsAndAdds1(false);
    }

    @Test
    public void testReplaceCommitRemoves2PartitionsAndAdds1NoRowsCommit() throws Exception {
        testReplaceCommitRemoves2PartitionsAndAdds1(true);
    }

    @Test
    public void testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother() throws Exception {
        testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother(false);
    }

    @Test
    public void testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother2() throws Exception {
        testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother2(false);
    }

    public void testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother2(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T21:31', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(20)");
            execute("insert into rg select x, timestamp_sequence('2022-02-28T21:31', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(20)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-25T02:36:07.769840Z", "2022-02-24T23:20:30", "2022-02-27T01:34:56.265527", tableToken, false, true, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    @Test
    public void testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother2NoRowsCommit() throws Exception {
        testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother2(true);
    }

    @Test
    public void testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnotherNoRowsCommit() throws Exception {
        testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother(true);
    }

    @Test
    public void testReplaceRangeAddsPartitionsAboveLastThenRebuilds() throws Exception {
        // Regression for a replace-mode O3 bug (reachable via live-view BACKFILL + O3).
        // Commit A is a REPLACE_RANGE [oldMax + 1us, +inf) that appends new partitions above the
        // previous last partition (2026-01-05), leaving it untouched. The open-ended high boundary
        // (Long.MAX_VALUE - 1) overflowed getCurrentPartitionMaxTimestamp, so partitionTimestampHi
        // stayed stale and finishO3Commit never switched the active columns to the new last
        // partition (2026-01-10). Commit B, a full rebuild (REPLACE_RANGE [min, +inf)), then
        // treated 2026-01-10 as the active partition, reused the stale 2026-01-05 descriptors, and
        // corrupted the view / suspended the table.
        assertMemoryLeak(() -> {
            execute("create table rg (id long, ts timestamp, v long) timestamp(ts) partition by DAY WAL");
            execute("create table expected (id long, ts timestamp, v long) timestamp(ts) partition by DAY WAL");
            TableToken rg = engine.verifyTableName("rg");
            TableToken expected = engine.verifyTableName("expected");

            // Existing data: days 2026-01-01..2026-01-05, last partition 2026-01-05 maxing at
            // exactly 2026-01-05T00:03:46.111248Z.
            for (int day = 0; day < 5; day++) {
                try (WalWriter ww = engine.getWalWriter(rg)) {
                    long[] tss = existingDay(day);
                    for (int r = 0; r < tss.length; r++) {
                        appendRow(ww, tss[r], day * 100L + r, (day * 100L + r) * 10);
                    }
                    ww.commit();
                }
                drainWalQueue();
            }

            // Commit A: replace [oldMax + 1us, +inf) with new partitions above the last: days 06,
            // 08, 09, 10 (gap at 07).
            long rangeLoA = MicrosTimestampDriver.floor("2026-01-05T00:03:46.111248Z") + 1;
            long[] newTss = newPartitionRows();
            try (WalWriter ww = engine.getWalWriter(rg)) {
                for (int i = 0; i < newTss.length; i++) {
                    appendRow(ww, newTss[i], 1000L + i, (1000L + i) * 10);
                }
                ww.commitWithParams(rangeLoA, Long.MAX_VALUE, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();

            // Commit B: full rebuild. Replace [min, +inf), re-emitting every row in ts order,
            // including a new 2026-01-07 row between commit A's partitions.
            try (WalWriter ww = engine.getWalWriter(rg)) {
                appendFinalDataset(ww);
                ww.commitWithParams(MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z"), Long.MAX_VALUE, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();

            // Oracle: the same final row set written with plain commits, no replace mode.
            try (WalWriter ww = engine.getWalWriter(expected)) {
                appendFinalDataset(ww);
                ww.commit();
            }
            drainWalQueue();

            Assert.assertFalse("table is suspended", engine.getTableSequencerAPI().isSuspended(rg));
            assertSqlCursors("select id, ts, v from expected", "select id, ts, v from rg");
            assertSqlCursors(
                    "select count(*), min(ts), max(ts) from expected",
                    "select count(*), min(ts), max(ts) from rg"
            );
        });
    }

    @Test
    public void testReplaceRangeBeforeFirstPartitionAndData() throws Exception {
        testReplaceRangeBeforeFirstPartitionAndData(false);
    }

    @Test
    public void testReplaceRangeBeforeFirstPartitionAndDataNoRowsCommit() throws Exception {
        testReplaceRangeBeforeFirstPartitionAndData(true);
    }

    @Test
    public void testReplaceRangeCommitDataRangeCommitErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");
            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");

            Utf8StringSink utf8StringSink = new Utf8StringSink();
            try {
                insertRowsWithRangeReplace(tableToken, utf8StringSink, "2022-02-24T17", "2022-02-24T18", "2022-02-25T18", true);
                Assert.fail("exception expected");
            } catch (CairoException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "Replace range low timestamp must be less than or equal to the minimum timestamp"
                );
            }

            try {
                insertRowsWithRangeReplace(tableToken, utf8StringSink, "2022-02-25T18", "2022-02-24T18", "2022-02-25T17:59:59.999999", true);
                Assert.fail("exception expected");
            } catch (CairoException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "Replace range high timestamp must be greater than the maximum timestamp"
                );
            }

            try {
                insertRowsWithRangeReplace(tableToken, utf8StringSink, "2022-02-25T18", "2022-02-25T18", "2022-02-25T17:59:59.999999", true);
                Assert.fail("exception expected");
            } catch (CairoException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "Replace range low timestamp must be less than replace range high timestamp"
                );
            }

            drainWalQueue();
            Assert.assertFalse("table is suspended", engine.getTableSequencerAPI().isSuspended(tableToken));
        });
    }

    @Test
    public void testReplaceRangeDeletesMidFirstPartition() throws Exception {
        testReplaceRangeCommit(null, "2022-02-24T18:30", "2022-02-24T20:59:31", false, false, false);
    }

    @Test
    public void testReplaceRangeDeletesMidFirstPartitionNoRowsCommit() throws Exception {
        testReplaceRangeCommit(null, "2022-02-24T18:30", "2022-02-24T20:59:31", false, false, true);
    }

    @Test
    public void testReplaceRangeDoesNotSkipInsertBeforeColumnAdd() throws Exception {
        // Exercises the generalized skip barrier with ADD COLUMN - a structural (walId < 1) transaction -
        // as the barrier, in a single apply batch. The only other add-column test here drains between
        // steps, so it never runs the barrier in the same batch as a skippable insert and a covering
        // REPLACE_RANGE; this commits insert + add-column + replace before applying so the apply job sees
        // the REPLACE_RANGE while deciding whether to skip the insert.
        //
        // Unlike the conversion cases, this is path coverage rather than a strict regression guard. In a
        // single instance the result is self-consistent whether or not the insert is skipped: the covering
        // REPLACE_RANGE recomputes the new column's column top either way, so the data, symbol count and
        // column top are identical with and without the fix (verified). The barrier matters for replica
        // consistency - two instances that skip different transactions ahead of the structural change would
        // diverge - which a single-instance test cannot observe. This test pins that the path applies
        // cleanly and returns correct data.
        assertMemoryLeak(() -> {
            execute("create table x (id int, ts timestamp) timestamp(ts) partition by DAY WAL");
            final TableToken tableToken = engine.verifyTableName("x");

            final long rangeLo = MicrosTimestampDriver.floor("2022-02-24T00");
            final long rangeHi = MicrosTimestampDriver.floor("2022-02-24T01"); // exclusive

            // INSERT covered by the later REPLACE_RANGE - the one at risk of being skipped. Committed first
            // so its skip would be decided with an empty writer LAG (a non-empty LAG makes the skip bail out).
            final StringSink insert = new StringSink();
            insert.put("insert into x values ");
            for (int i = 0; i < 10; i++) {
                if (i > 0) {
                    insert.put(',');
                }
                insert.put('(').put(i).put(",'2022-02-24T00:0").put(i).put(":00.000000Z')");
            }
            execute(insert.toString());

            // Surviving rows at HIGHER timestamps, outside the REPLACE_RANGE. They are present when ADD
            // COLUMN runs, so they sit below the new column's top and must read back as null afterwards.
            execute("insert into x values (100,'2022-02-24T05:00:00.000000Z'),(101,'2022-02-24T05:01:00.000000Z')");

            // ADD COLUMN - the structural barrier between the insert and the replace.
            execute("alter table x add column c int");

            // REPLACE_RANGE fully covering the first insert, populating the new column for those rows.
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                final int idIdx = ww.getMetadata().getColumnIndex("id");
                final int cIdx = ww.getMetadata().getColumnIndex("c");
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = ww.newRow(rangeLo + i * 60_000_000L);
                    row.putInt(idIdx, i);
                    row.putInt(cIdx, 1000 + i);
                    row.append();
                }
                ww.commitWithParams(rangeLo, rangeHi, WAL_DEDUP_MODE_REPLACE_RANGE);
            }

            // Apply insert + add-column + replace in one batch.
            drainWalQueue();

            // The replaced rows carry their c values; the surviving rows predate the column and read null.
            assertQuery("select * from x order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts\tc
                            0\t2022-02-24T00:00:00.000000Z\t1000
                            1\t2022-02-24T00:01:00.000000Z\t1001
                            2\t2022-02-24T00:02:00.000000Z\t1002
                            3\t2022-02-24T00:03:00.000000Z\t1003
                            4\t2022-02-24T00:04:00.000000Z\t1004
                            5\t2022-02-24T00:05:00.000000Z\t1005
                            6\t2022-02-24T00:06:00.000000Z\t1006
                            7\t2022-02-24T00:07:00.000000Z\t1007
                            8\t2022-02-24T00:08:00.000000Z\t1008
                            9\t2022-02-24T00:09:00.000000Z\t1009
                            100\t2022-02-24T05:00:00.000000Z\tnull
                            101\t2022-02-24T05:01:00.000000Z\tnull
                            """);
        });
    }

    @Test
    public void testReplaceRangeDoesNotSkipInsertBeforeMatViewInvalidate() throws Exception {
        // Exercises the generalized skip barrier with a MAT_VIEW_INVALIDATE - a non-data transaction with
        // walId > 0 - as the barrier, in a single apply batch. Unlike the ADD COLUMN / conversion cases
        // (structural, walId < 1, caught by the futureWalId < 1 arm), this hits the !isDataType(futureType)
        // arm, the path whose behavior the fix changed for type-4/5 transactions: the old scan walked past a
        // MAT_VIEW_INVALIDATE, the new scan stops at it. VIEW_DEFINITION (type 5) shares this exact arm.
        //
        // WalWriter.resetMatViewState emits a synthetic MAT_VIEW_INVALIDATE without requiring a real mat view
        // (WalWriter does not validate the table type), the same technique RecentWriteTrackerIntegrationTest
        // uses to drive this branch deterministically.
        //
        // STRICT guard. A MAT_VIEW_INVALIDATE builds no row-derived state, so the covering REPLACE_RANGE
        // overwrites the inserted rows either way and the rendered data is identical with or without the fix.
        // The barrier is therefore pinned via physically-written rows, the same proxy the mat-view exemption
        // tests use: on a REGULAR table the barrier stops the scan, so the INSERT is applied - its 10 rows
        // reach disk ahead of the REPLACE_RANGE's 10 - for a delta of 20. This is the exact opposite of
        // testReplaceRangeSkipsInsertBeforeBarrierOnMatView, where a mat view scans past the same barrier and
        // skips the INSERT for a delta of 10. If the fix regressed and a regular table scanned past the
        // MAT_VIEW_INVALIDATE, the INSERT would be skipped and this delta would be 10.
        assertMemoryLeak(() -> {
            execute("create table x (id int, ts timestamp) timestamp(ts) partition by DAY WAL");
            final TableToken tableToken = engine.verifyTableName("x");

            final long rangeLo = MicrosTimestampDriver.floor("2022-02-24T00");
            final long rangeHi = MicrosTimestampDriver.floor("2022-02-24T01"); // exclusive

            // INSERT covered by the later REPLACE_RANGE - the one at risk of being skipped. Committed first
            // so its skip would be decided with an empty writer LAG (a non-empty LAG makes the skip bail out).
            final StringSink insert = new StringSink();
            insert.put("insert into x values ");
            for (int i = 0; i < 10; i++) {
                if (i > 0) {
                    insert.put(',');
                }
                insert.put('(').put(i).put(",'2022-02-24T00:0").put(i).put(":00.000000Z')");
            }
            execute(insert.toString());

            // MAT_VIEW_INVALIDATE - the non-data (walId > 0) barrier between the insert and the replace.
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                ww.resetMatViewState(1, 1, true, "test invalidation", Numbers.LONG_NULL, null, -1);
            }

            // REPLACE_RANGE fully covering the insert, with new id values.
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                final int idIdx = ww.getMetadata().getColumnIndex("id");
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = ww.newRow(rangeLo + i * 60_000_000L);
                    row.putInt(idIdx, 1000 + i);
                    row.append();
                }
                ww.commitWithParams(rangeLo, rangeHi, WAL_DEDUP_MODE_REPLACE_RANGE);
            }

            // Apply insert + mat-view-invalidate + replace in one batch.
            final long physicalRowsBefore = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
            drainWalQueue();

            // Only the replacement values remain; the path applies cleanly and returns correct data.
            assertQuery("select * from x order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts
                            1000\t2022-02-24T00:00:00.000000Z
                            1001\t2022-02-24T00:01:00.000000Z
                            1002\t2022-02-24T00:02:00.000000Z
                            1003\t2022-02-24T00:03:00.000000Z
                            1004\t2022-02-24T00:04:00.000000Z
                            1005\t2022-02-24T00:05:00.000000Z
                            1006\t2022-02-24T00:06:00.000000Z
                            1007\t2022-02-24T00:07:00.000000Z
                            1008\t2022-02-24T00:08:00.000000Z
                            1009\t2022-02-24T00:09:00.000000Z
                            """);

            // The data assertion above is identical whether or not the INSERT is skipped, so it does not
            // guard the barrier. Pin it via physically-written rows: the barrier stops the scan, so the
            // INSERT's 10 rows are materialised ahead of the REPLACE_RANGE's 10. If the fix regressed and a
            // regular table scanned past the MAT_VIEW_INVALIDATE, the INSERT would be skipped, delta 10.
            Assert.assertEquals(
                    20,
                    engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - physicalRowsBefore
            );
        });
    }

    @Test
    public void testReplaceRangeDoesNotSkipInsertBeforeSqlBarrierAndTruncateOnMatView() throws Exception {
        // The mat-view exemption keeps an SQL transaction a HARD barrier even when a TRUNCATE follows it. This is
        // the truncate-clamp counterpart of testReplaceRangeDoesNotSkipInsertBeforeSqlBarrierOnMatView (which
        // puts the SQL barrier before a REPLACE_RANGE) and the SQL-barrier counterpart of
        // testReplaceRangeSkipsInsertBeforeTruncateAfterBarrierOnMatView (which uses a MAT_VIEW_INVALIDATE soft
        // barrier and so DOES skip the insert).
        //
        // Sequence on a mat view: INSERT(0) -> ADD INDEX (SQL, walId>0)(1) -> TRUNCATE(2). At the SQL barrier the
        // `isMatView && futureType != SQL` check is false (futureType IS SQL), so the scan breaks there and never
        // reaches the truncate clamp; the insert is applied, then truncated. If the futureType != SQL carve-out
        // regressed and treated SQL as a soft barrier on a mat view, the scan would record the barrier, reach the
        // truncate, and skip the insert (clamped to the barrier) - so its rows would never reach disk.
        //
        // Future-mutation tripwire, NOT a regression guard for this PR's change: reverting the generalized barrier
        // leaves this green (the pre-fix scan also broke at an SQL transaction), so it fails ONLY if the
        // futureType != SQL carve-out is later dropped and SQL becomes a soft barrier on a mat view. It pins that
        // carve-out via physically-written rows: the truncate wipes the insert either way so the rendered data is
        // identical, but applying vs skipping the insert changes the row count.
        assertMemoryLeak(() -> {
            execute("create table base (s symbol, v double, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("create materialized view mv as (select s, last(v) v, ts from base sample by 1h) partition by DAY");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("mv");
            Assert.assertTrue("mv must be a materialized view", tableToken.isMatView());

            final long rangeLo = MicrosTimestampDriver.floor("2022-02-24T00");

            // INSERT - skippable only if the scan passes the SQL barrier and reaches the truncate.
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = ww.newRow(rangeLo + i * 60_000_000L);
                    row.putSym(0, "old" + i);
                    row.putDouble(1, i);
                    row.append();
                }
                ww.commit();
            }

            // ADD INDEX - a non-structural ALTER that commits as a SQL (walId > 0) transaction: the hard barrier
            // the futureType != SQL carve-out keeps even when a TRUNCATE follows it.
            execute("alter materialized view mv alter column s add index");

            // TRUNCATE - the same call a full refresh makes - plus one surviving row so there is observable data.
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                ww.truncateSoft();
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2022-02-25T00"));
                row.putSym(0, "kept");
                row.putDouble(1, 42);
                row.append();
                ww.commit();
            }

            // Apply insert + add-index + truncate + insert in one batch.
            final long physicalRowsBefore = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
            drainWalQueue();

            // Only the post-truncate row survives.
            assertQuery("select s, v from mv order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tv
                            kept\t42.0
                            """);

            // The truncate wipes the INSERT either way, so the data assertion above does not guard the SQL hard
            // barrier. Pin it via physically-written rows: the SQL barrier stops the scan before the truncate, so
            // the INSERT's 10 rows are materialised (then truncated) ahead of the single surviving row. If the
            // futureType != SQL carve-out regressed and skipped the INSERT, those 10 rows would never be written
            // and this delta would be 1.
            Assert.assertEquals(
                    11,
                    engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - physicalRowsBefore
            );
        });
    }

    @Test
    public void testReplaceRangeDoesNotSkipInsertBeforeSqlBarrierOnMatView() throws Exception {
        // The mat-view exemption keeps an SQL transaction a HARD barrier even for a materialized view: the
        // futureType != SQL carve-out in ApplyWal2TableJob.calculateSkipTransactionCount. Unlike a
        // MAT_VIEW_INVALIDATE - a soft barrier the scan passes, so the insert before it is skipped (see
        // testReplaceRangeSkipsInsertBeforeBarrierOnMatView) - an INSERT before an SQL transaction is not
        // skipped on a mat view either.
        //
        // ADD INDEX is the realistic mat-view SQL barrier: it is non-data and non-structural, so it commits
        // as a walId > 0 SQL transaction rather than a walId < 1 structural change, and it builds row-derived
        // index state from the rows present when it runs - the kind of operation the hard SQL barrier exists
        // to protect. (A column type conversion, the other such operation, cannot run on a mat view at all.)
        //
        // This is path coverage rather than a strict guard: the covering REPLACE_RANGE overwrites the
        // inserted rows and rebuilds the index for that range either way, so a single instance ends in the
        // same state whether or not the insert is skipped. The assertion pins that the SQL-barrier-on-a-mat-
        // view arm and the ADD INDEX + REPLACE_RANGE interaction apply cleanly, return correct data, and
        // leave a usable index.
        assertMemoryLeak(() -> {
            execute("create table base (s symbol, v double, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("create materialized view mv as (select s, last(v) v, ts from base sample by 1h) partition by DAY");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("mv");
            Assert.assertTrue("mv must be a materialized view", tableToken.isMatView());

            final long rangeLo = MicrosTimestampDriver.floor("2022-02-24T00");
            final long rangeHi = MicrosTimestampDriver.floor("2022-02-24T01"); // exclusive

            // INSERT covered by the later REPLACE_RANGE - skipped only if the scan passes the barrier.
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = ww.newRow(rangeLo + i * 60_000_000L);
                    row.putSym(0, "old" + i);
                    row.putDouble(1, i);
                    row.append();
                }
                ww.commit();
            }

            // ADD INDEX - a non-structural ALTER that commits as a SQL (walId > 0) transaction, the hard
            // barrier the futureType != SQL carve-out keeps even for a mat view.
            execute("alter materialized view mv alter column s add index");

            // REPLACE_RANGE fully covering the insert, with new values.
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = ww.newRow(rangeLo + i * 60_000_000L);
                    row.putSym(0, "new" + i);
                    row.putDouble(1, 1000 + i);
                    row.append();
                }
                ww.commitWithParams(rangeLo, rangeHi, WAL_DEDUP_MODE_REPLACE_RANGE);
            }

            // Apply insert + add-index + replace in one batch.
            drainWalQueue();

            // Only the replacement values remain.
            assertQuery("select s, v from mv order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tv
                            new0\t1000.0
                            new1\t1001.0
                            new2\t1002.0
                            new3\t1003.0
                            new4\t1004.0
                            new5\t1005.0
                            new6\t1006.0
                            new7\t1007.0
                            new8\t1008.0
                            new9\t1009.0
                            """);

            // The index built by ADD INDEX and rebuilt by the REPLACE_RANGE resolves the replacement row.
            assertQuery("select s, v from mv where s = 'new5'")
                    .noLeakCheck()
                    .returns("""
                            s\tv
                            new5\t1005.0
                            """);
        });
    }

    @Test
    public void testReplaceRangeDoesNotSkipInsertBeforeSqlBarrierOnMatViewPosting() throws Exception {
        // Same scenario as testReplaceRangeDoesNotSkipInsertBeforeSqlBarrierOnMatView, with a POSTING index
        // instead of the default BITMAP. The composite executor's index rebuild (O3PartitionJob.
        // processCompositePartition, after executeCompositePlan) tags the rebuilt index's seal with the
        // txn the executing commit is ABOUT to publish (RebuildColumnBase.reindexAfterCompositeWrite) - a
        // BITMAP index has no seal/chain at all, so that path is only exercised through POSTING. A wrong
        // tag there does not fail this single-instance test (nothing here crashes a recovery walk mid this
        // commit), but it is the one behavioural difference from the BITMAP case worth a dedicated run.
        assertMemoryLeak(() -> {
            execute("create table base (s symbol, v double, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("create materialized view mv as (select s, last(v) v, ts from base sample by 1h) partition by DAY");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("mv");

            final long rangeLo = MicrosTimestampDriver.floor("2022-02-24T00");
            final long rangeHi = MicrosTimestampDriver.floor("2022-02-24T01");

            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = ww.newRow(rangeLo + i * 60_000_000L);
                    row.putSym(0, "old" + i);
                    row.putDouble(1, i);
                    row.append();
                }
                ww.commit();
            }

            execute("alter materialized view mv alter column s add index type posting");

            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = ww.newRow(rangeLo + i * 60_000_000L);
                    row.putSym(0, "new" + i);
                    row.putDouble(1, 1000 + i);
                    row.append();
                }
                ww.commitWithParams(rangeLo, rangeHi, WAL_DEDUP_MODE_REPLACE_RANGE);
            }

            drainWalQueue();

            assertQuery("select s, v from mv order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tv
                            new0\t1000.0
                            new1\t1001.0
                            new2\t1002.0
                            new3\t1003.0
                            new4\t1004.0
                            new5\t1005.0
                            new6\t1006.0
                            new7\t1007.0
                            new8\t1008.0
                            new9\t1009.0
                            """);

            assertQuery("select s, v from mv where s = 'new5'")
                    .noLeakCheck()
                    .returns("""
                            s\tv
                            new5\t1005.0
                            """);
        });
    }

    @Test
    public void testReplaceRangeDoesNotSkipInsertBeforeSymbolConversion() throws Exception {
        // The WAL apply job's replace-range skip optimization (ApplyWal2TableJob.calculateSkipTransactionCount)
        // must not skip an INSERT that precedes a STRING->SYMBOL conversion, even when a later REPLACE_RANGE
        // fully covers the insert. A conversion to SYMBOL builds the column's symbol map from the rows present
        // when it runs, so skipping the insert would make the conversion see fewer rows. In a single instance
        // that stays self-consistent, but a replica that skips a different set of transactions than the primary
        // ends up with a divergent symbol map and silently wrong data.
        //
        // We commit insert + conversion + replace before applying, so the apply job sees the REPLACE_RANGE while
        // deciding whether to skip the insert. We then assert via the symbol map that the conversion processed
        // the inserted rows: the symbol map is append-only, so the 10 distinct inserted values survive even
        // after the REPLACE_RANGE overwrites their rows. If the insert were skipped, the conversion would see 0
        // rows and the map would hold only the 10 replacement values.
        assertMemoryLeak(() -> {
            execute("create table x (s string, ts timestamp) timestamp(ts) partition by DAY WAL");
            final TableToken tableToken = engine.verifyTableName("x");

            final long rangeLo = MicrosTimestampDriver.floor("2022-02-24T00");
            final long rangeHi = MicrosTimestampDriver.floor("2022-02-24T01"); // exclusive

            // INSERT 10 distinct values into the range the REPLACE_RANGE will later cover.
            final StringSink insert = new StringSink();
            insert.put("insert into x values ");
            for (int i = 0; i < 10; i++) {
                if (i > 0) {
                    insert.put(',');
                }
                insert.put("('v").put(i).put("','2022-02-24T00:0").put(i).put(":00.000000Z')");
            }
            execute(insert.toString());

            // Convert the column to SYMBOL (structural change that reads the inserted rows).
            execute("alter table x alter column s type symbol");

            // REPLACE_RANGE fully covering the inserted rows, with 10 different values.
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                int symbolColumnIndex = -1;
                for (int i = 0, n = ww.getMetadata().getColumnCount(); i < n; i++) {
                    if (ColumnType.isSymbol(ww.getMetadata().getColumnType(i))) {
                        symbolColumnIndex = i;
                        break;
                    }
                }
                Assert.assertTrue("converted symbol column not found", symbolColumnIndex >= 0);
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = ww.newRow(rangeLo + i * 60_000_000L);
                    row.putSym(symbolColumnIndex, "w" + i);
                    row.append();
                }
                ww.commitWithParams(rangeLo, rangeHi, WAL_DEDUP_MODE_REPLACE_RANGE);
            }

            // Apply insert + conversion + replace in one batch.
            drainWalQueue();

            // Only the replacement values remain in the data.
            assertQuery("select s from x order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns("s\nw0\nw1\nw2\nw3\nw4\nw5\nw6\nw7\nw8\nw9\n");

            // But the symbol map must hold all 20 values (10 inserted + 10 replacement), proving the conversion
            // processed the inserted rows rather than skipping them. With the bug it would be 10.
            try (TableReader reader = engine.getReader(tableToken)) {
                final int colIdx = reader.getMetadata().getColumnIndex("s");
                Assert.assertEquals(20, reader.getSymbolMapReader(colIdx).getSymbolCount());
            }
        });
    }

    @Test
    public void testReplaceRangeDoesNotSkipInsertBeforeTruncateAfterBarrier() throws Exception {
        // Exercises the TRUNCATE skip path when a non-data barrier sits between the insert and the truncate
        // - the exact interaction the firstNonSkippableTxn removal changed. Previously the skip scan, on
        // reaching the truncate, would skip everything up to the first non-skippable (barrier) transaction;
        // now it stops at the barrier (ADD COLUMN, walId < 1) before it can reach the truncate, so the
        // insert is applied rather than collapsed into the truncate skip.
        //
        // Like the ADD COLUMN case this is path coverage, not a strict regression guard: the truncate wipes
        // the inserted data either way, so a single instance ends in the same state with or without the fix.
        assertMemoryLeak(() -> {
            execute("create table x (a int, ts timestamp) timestamp(ts) partition by DAY WAL");

            // Insert that the truncate (not a REPLACE_RANGE) makes skippable; a barrier follows it.
            final StringSink insert = new StringSink();
            insert.put("insert into x values ");
            for (int i = 0; i < 10; i++) {
                if (i > 0) {
                    insert.put(',');
                }
                insert.put('(').put(i).put(",'2022-02-24T00:0").put(i).put(":00.000000Z')");
            }
            execute(insert.toString());

            // ADD COLUMN barrier between the insert and the truncate.
            execute("alter table x add column c int");

            // TRUNCATE wipes everything inserted above.
            execute("truncate table x");

            // One row after the truncate, so there is observable data.
            execute("insert into x values (100, '2022-02-25T00:00:00.000000Z', 900)");

            // Apply insert + add-column + truncate + insert in a single batch.
            drainWalQueue();

            assertQuery("select * from x order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("a\tts\tc\n100\t2022-02-25T00:00:00.000000Z\t900\n");
        });
    }

    @Test
    public void testReplaceRangeDoesNotSkipInsertBeforeVarcharToSymbolConversion() throws Exception {
        // Same regression as testReplaceRangeDoesNotSkipInsertBeforeSymbolConversion, except the converted
        // column starts as VARCHAR rather than STRING. The fix turns any non-data transaction into a skip
        // barrier, so a VARCHAR->SYMBOL conversion needs the same coverage as STRING->SYMBOL: skipping the
        // insert ahead of it would make the conversion build its symbol map from fewer rows.
        assertMemoryLeak(() -> {
            execute("create table x (s varchar, ts timestamp) timestamp(ts) partition by DAY WAL");
            final TableToken tableToken = engine.verifyTableName("x");

            final long rangeLo = MicrosTimestampDriver.floor("2022-02-24T00");
            final long rangeHi = MicrosTimestampDriver.floor("2022-02-24T01"); // exclusive

            // INSERT 10 distinct values into the range the REPLACE_RANGE will later cover.
            final StringSink insert = new StringSink();
            insert.put("insert into x values ");
            for (int i = 0; i < 10; i++) {
                if (i > 0) {
                    insert.put(',');
                }
                insert.put("('v").put(i).put("','2022-02-24T00:0").put(i).put(":00.000000Z')");
            }
            execute(insert.toString());

            // Convert the column to SYMBOL (structural change that reads the inserted rows).
            execute("alter table x alter column s type symbol");

            // REPLACE_RANGE fully covering the inserted rows, with 10 different values.
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                int symbolColumnIndex = -1;
                for (int i = 0, n = ww.getMetadata().getColumnCount(); i < n; i++) {
                    if (ColumnType.isSymbol(ww.getMetadata().getColumnType(i))) {
                        symbolColumnIndex = i;
                        break;
                    }
                }
                Assert.assertTrue("converted symbol column not found", symbolColumnIndex >= 0);
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = ww.newRow(rangeLo + i * 60_000_000L);
                    row.putSym(symbolColumnIndex, "w" + i);
                    row.append();
                }
                ww.commitWithParams(rangeLo, rangeHi, WAL_DEDUP_MODE_REPLACE_RANGE);
            }

            // Apply insert + conversion + replace in one batch.
            drainWalQueue();

            // Only the replacement values remain in the data.
            assertQuery("select s from x order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns("s\nw0\nw1\nw2\nw3\nw4\nw5\nw6\nw7\nw8\nw9\n");

            // The symbol map must hold all 20 values (10 inserted + 10 replacement), proving the conversion
            // processed the inserted rows rather than skipping them. With the bug it would be 10.
            try (TableReader reader = engine.getReader(tableToken)) {
                final int colIdx = reader.getMetadata().getColumnIndex("s");
                Assert.assertEquals(20, reader.getSymbolMapReader(colIdx).getSymbolCount());
            }
        });
    }

    @Test
    public void testReplaceRangeFirstPartitionNoImpact() throws Exception {
        testReplaceRangeCommit(null, "2022-02-24T23:59:30", "2022-02-24T23:59:31", false, false, false);
    }

    @Test
    public void testReplaceRangeFirstPartitionNoImpactNoRowsCommit() throws Exception {
        testReplaceRangeCommit(null, "2022-02-24T23:59:30", "2022-02-24T23:59:31", false, false, true);
    }

    @Test
    public void testReplaceRangeLastPartition() throws Exception {
        testReplaceRangeLastPartition(true, false);
    }

    @Test
    public void testReplaceRangeLastPartitionAppend() throws Exception {
        testReplaceRangeCommit("2022-02-25T16:25", "2022-02-25T16:00", "2022-02-25T17:00");
    }

    @Test
    public void testReplaceRangeLastPartitionAppendReplacesFullPartition() throws Exception {
        testReplaceRangeCommit("2022-02-25T16:25", "2022-02-25T00:00", "2022-02-25T23:59");
    }

    @Test
    public void testReplaceRangeLastPartitionNoRowsCommit() throws Exception {
        testReplaceRangeLastPartition(false, true);
    }

    @Test
    public void testReplaceRangeLastPartitionPrefix() throws Exception {
        testReplaceRangeCommit("2022-02-25T00:53", "2022-02-25T00:00", "2022-02-25T04");
    }

    @Test
    public void testReplaceRangeLastPartitionSuffix() throws Exception {
        testReplaceRangeCommit("2022-02-25T02:53", "2022-02-25T01:00", "2022-02-25T23");
    }

    @Test
    public void testReplaceRangeParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");
            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");
            drainWalQueue();

            execute("ALTER TABLE rg CONVERT PARTITION TO PARQUET LIST '2022-02-24'");
            drainWalQueue();

            insertRowWithReplaceRange(
                    "2022-02-24T17", "2022-02-24T14", "2022-02-25T18", tableToken,
                    false, false, "rg", "expected", false, false
            );
            assertQuery("select isParquet from table_partitions('rg') limit 1")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            isParquet
                            true
                            """);
        });
    }

    @Test
    public void testReplaceRangeRemovesSplitParentThenAddColumnAndConvert() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createDayWithSplitButNoParent();
            // The split takes the new column, so _cv has no record of it at the day's floor, where
            // CONVERT re-creates the parent.
            execute("ALTER TABLE rg ADD COLUMN extra INT");
            execute("INSERT INTO rg VALUES (203, '2022-02-24T19:00', 0, 7)");
            drainWalQueue();

            execute("ALTER TABLE rg CONVERT PARTITION TO PARQUET LIST '2022-02-24'");
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertQuery("SELECT name, numRows, isParquet FROM table_partitions('rg')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows\tisParquet
                            2022-02-24\t3\ttrue
                            """);
            final String expected = """
                    id\tts\tv\textra
                    201\t2022-02-24T18:00:00.000000Z\t0\tnull
                    202\t2022-02-24T18:30:00.000000Z\t0\tnull
                    203\t2022-02-24T19:00:00.000000Z\t0\t7
                    """;
            assertQuery("SELECT * FROM rg")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(expected);

            execute("ALTER TABLE rg CONVERT PARTITION TO NATIVE LIST '2022-02-24'");
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertQuery("SELECT name, numRows, isParquet FROM table_partitions('rg')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows\tisParquet
                            2022-02-24\t3\tfalse
                            """);
            assertQuery("SELECT * FROM rg")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(expected);
        });
    }

    @Test
    public void testReplaceRangeRemovesSplitParentThenConvertFailsAndResumes() throws Exception {
        // CONVERT fails while it squashes the split into the re-created parent, and the rollback
        // cannot delete the half-built parent directory. RESUME WAL retries at the same txn, so the
        // retry meets that directory under the same name and has to reuse it.
        final AtomicBoolean failParentColumnOpen = new AtomicBoolean();
        final AtomicBoolean keepParentDirs = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failParentColumnOpen.get() && isParentDir(name) && Utf8s.endsWithAscii(name, Files.SEPARATOR + "v.d")) {
                    failParentColumnOpen.set(false);
                    return -1;
                }
                return super.openRW(name, opts);
            }

            @Override
            public boolean rmdir(Path name, boolean haltOnError) {
                return !isKept(name) && super.rmdir(name, haltOnError);
            }

            @Override
            public boolean unlinkOrRemove(Path path, Log log) {
                return !isKept(path) && super.unlinkOrRemove(path, log);
            }

            @Override
            public boolean unlinkOrRemove(Path path, int checkedType, Log log) {
                return !isKept(path) && super.unlinkOrRemove(path, checkedType, log);
            }

            private boolean isKept(Path path) {
                return keepParentDirs.get() && isParentDir(path);
            }

            // A version of the day's parent, such as '2022-02-24.3', and not the split.
            private boolean isParentDir(Utf8Sequence path) {
                return Utf8s.containsAscii(path, Files.SEPARATOR + "2022-02-24.");
            }
        };
        assertMemoryLeak(ff, () -> {
            TableToken tableToken = createDayWithSplitButNoParent();
            failParentColumnOpen.set(true);
            keepParentDirs.set(true);
            execute("ALTER TABLE rg CONVERT PARTITION TO PARQUET LIST '2022-02-24'");
            drainWalQueue();
            Assert.assertFalse("the injected failure did not fire", failParentColumnOpen.get());
            Assert.assertTrue("the failed CONVERT must suspend the table", engine.getTableSequencerAPI().isSuspended(tableToken));
            try (Path path = new Path()) {
                Assert.assertTrue(
                        "the rollback must leave the half-built parent behind",
                        Files.exists(path.of(configuration.getDbRoot()).concat(tableToken).concat("2022-02-24.3").$())
                );
            }

            execute("ALTER TABLE rg RESUME WAL");
            drainWalQueue();
            assertNotSuspended(tableToken);
            keepParentDirs.set(false);
            assertQuery("SELECT name, numRows, isParquet FROM table_partitions('rg')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows\tisParquet
                            2022-02-24\t2\ttrue
                            """);
            assertQuery("SELECT * FROM rg")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts\tv
                            201\t2022-02-24T18:00:00.000000Z\t0
                            202\t2022-02-24T18:30:00.000000Z\t0
                            """);
        });
    }

    @Test
    public void testReplaceRangeRemovesSplitParentThenConvertNanosTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // Drop the absolute split threshold, so the prefix-to-tail ratio alone decides the split.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            // Merge-append turns the O3 batch into a composite partition instead of a split.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
            assertSplitWithoutParentConverts(
                    "rg",
                    "TIMESTAMP_NS",
                    "DAY",
                    NanosTimestampDriver.floor("2022-02-24T00:00:00.000000000Z"),
                    600_000_000_000L,
                    "LIST '2022-02-24'"
            );
        });
    }

    @Test
    public void testReplaceRangeRemovesSplitParentThenConvertOtherPartitionBy() throws Exception {
        assertMemoryLeak(() -> {
            // Drop the absolute split threshold, so the prefix-to-tail ratio alone decides the split.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            // Merge-append turns the O3 batch into a composite partition instead of a split.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
            // Each step fits 61 steps into one partition of the unit.
            assertSplitWithoutParentConverts(
                    "rg_hour",
                    "TIMESTAMP",
                    "HOUR",
                    MicrosTimestampDriver.floor("2022-02-24T08:00:00.000000Z"),
                    30_000_000L,
                    "WHERE ts >= '2022-02-24T08:00:00.000000Z' AND ts < '2022-02-24T09:00:00.000000Z'"
            );
            assertSplitWithoutParentConverts(
                    "rg_week",
                    "TIMESTAMP",
                    "WEEK",
                    MicrosTimestampDriver.floor("2022-02-21T00:00:00.000000Z"),
                    3_600_000_000L,
                    "WHERE ts >= '2022-02-21T00:00:00.000000Z' AND ts < '2022-02-28T00:00:00.000000Z'"
            );
            assertSplitWithoutParentConverts(
                    "rg_month",
                    "TIMESTAMP",
                    "MONTH",
                    MicrosTimestampDriver.floor("2022-02-01T00:00:00.000000Z"),
                    21_600_000_000L,
                    "WHERE ts >= '2022-02-01T00:00:00.000000Z' AND ts < '2022-03-01T00:00:00.000000Z'"
            );
            assertSplitWithoutParentConverts(
                    "rg_year",
                    "TIMESTAMP",
                    "YEAR",
                    MicrosTimestampDriver.floor("2022-01-01T00:00:00.000000Z"),
                    259_200_000_000L,
                    "WHERE ts >= '2022-01-01T00:00:00.000000Z' AND ts < '2023-01-01T00:00:00.000000Z'"
            );
        });
    }

    @Test
    public void testReplaceRangeRemovesSplitParentThenConvertToNative() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createDayWithSplitButNoParent();
            execute("ALTER TABLE rg CONVERT PARTITION TO NATIVE WHERE ts > '2022-02-23T23:00' AND ts < '2022-02-24T23:00'");
            drainWalQueue();
            assertNotSuspended(tableToken);
            // Only native partitions split, so the day is native already and stays as it is.
            assertQuery("SELECT name, numRows, isParquet FROM table_partitions('rg')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows\tisParquet
                            2022-02-24T170000-000001\t2\tfalse
                            """);
            assertQuery("SELECT * FROM rg")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts\tv
                            201\t2022-02-24T18:00:00.000000Z\t0
                            202\t2022-02-24T18:30:00.000000Z\t0
                            """);
        });
    }

    @Test
    public void testReplaceRangeRemovesSplitParentThenConvertToParquet() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createDayWithSplitButNoParent();
            execute("ALTER TABLE rg CONVERT PARTITION TO PARQUET WHERE ts > '2022-02-23T23:00' AND ts < '2022-02-24T23:00'");
            drainWalQueue();
            assertNotSuspended(tableToken);
            // CONVERT re-creates the parent at the floor and squashes the split into it.
            assertQuery("SELECT name, numRows, isParquet FROM table_partitions('rg')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows\tisParquet
                            2022-02-24\t2\ttrue
                            """);
            assertQuery("SELECT * FROM rg")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts\tv
                            201\t2022-02-24T18:00:00.000000Z\t0
                            202\t2022-02-24T18:30:00.000000Z\t0
                            """);

            // The day is the active partition and keeps taking rows.
            execute("INSERT INTO rg VALUES (203, '2022-02-24T19:00', 0)");
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertQuery("SELECT * FROM rg")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts\tv
                            201\t2022-02-24T18:00:00.000000Z\t0
                            202\t2022-02-24T18:30:00.000000Z\t0
                            203\t2022-02-24T19:00:00.000000Z\t0
                            """);
        });
    }

    @Test
    public void testReplaceRangeRemovesSplitParentThenConvertToParquetBeforeLastPartition() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createDayWithSplitButNoParent();
            execute("INSERT INTO rg VALUES (301, '2022-02-25T10:00', 0)");
            drainWalQueue();
            assertQuery("SELECT name, numRows FROM table_partitions('rg')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows
                            2022-02-24T170000-000001\t2
                            2022-02-25\t1
                            """);

            execute("ALTER TABLE rg CONVERT PARTITION TO PARQUET LIST '2022-02-24'");
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertQuery("SELECT name, numRows, isParquet FROM table_partitions('rg')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows\tisParquet
                            2022-02-24\t2\ttrue
                            2022-02-25\t1\tfalse
                            """);
            assertQuery("SELECT * FROM rg")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts\tv
                            201\t2022-02-24T18:00:00.000000Z\t0
                            202\t2022-02-24T18:30:00.000000Z\t0
                            301\t2022-02-25T10:00:00.000000Z\t0
                            """);
        });
    }

    @Test
    public void testReplaceRangeRemovesSplitParentThenConvertToParquetWithOpenReader() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createDayWithSplitButNoParent();
            execute("ALTER TABLE rg CONVERT PARTITION TO PARQUET LIST '2022-02-24'");
            // The reader pins the latest txn, the one the re-created parent takes as its name txn.
            try (TableReader reader = getReader(tableToken)) {
                drainWalQueue();
                assertNotSuspended(tableToken);
                // The reader keeps reading its own version of the day: the split.
                TestUtils.assertReader(
                        """
                                id\tts\tv
                                201\t2022-02-24T18:00:00.000000Z\t0
                                202\t2022-02-24T18:30:00.000000Z\t0
                                """,
                        reader,
                        new StringSink()
                );
            }
            // The next commit purges the partition versions that CONVERT replaced.
            execute("INSERT INTO rg VALUES (203, '2022-02-24T19:00', 0)");
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertQuery("SELECT * FROM rg")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts\tv
                            201\t2022-02-24T18:00:00.000000Z\t0
                            202\t2022-02-24T18:30:00.000000Z\t0
                            203\t2022-02-24T19:00:00.000000Z\t0
                            """);
        });
    }

    @Test
    public void testReplaceRangeRemovesSplitParentThenConvertVarSizeColumns() throws Exception {
        assertMemoryLeak(() -> {
            // Drop the absolute split threshold, so the prefix-to-tail ratio alone decides the split.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            // Merge-append turns the O3 batch into a composite partition instead of a split.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
            execute("CREATE TABLE rg (id LONG, ts TIMESTAMP, s STRING, vc VARCHAR, b BINARY, arr DOUBLE[], sym SYMBOL, bo BOOLEAN, sh SHORT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");
            final String values = "rnd_str(3, 8, 2), rnd_varchar(3, 8, 2), rnd_bin(4, 16, 2), ARRAY[x * 1.5, x * 2.5], "
                    + "rnd_symbol('a', 'b', null), rnd_boolean(), rnd_short()";
            execute("INSERT INTO rg SELECT x, timestamp_sequence('2022-02-24T08:00', 600_000_000), " + values + " FROM long_sequence(60)");
            drainWalQueue();
            execute("INSERT INTO rg SELECT 100 + x, timestamp_sequence('2022-02-24T17:05', 600_000_000), " + values + " FROM long_sequence(5)");
            drainWalQueue();

            // The replace range removes the parent and the head of the split, so the split keeps its
            // rows from 17:30 on.
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2022-02-24T17:12"));
                row.putLong(0, 201);
                row.append();
                ww.commitWithParams(
                        MicrosTimestampDriver.floor("2022-02-24T06:00"),
                        MicrosTimestampDriver.floor("2022-02-24T17:30"),
                        WAL_DEDUP_MODE_REPLACE_RANGE
                );
            }
            drainWalQueue();
            assertQuery("SELECT name FROM table_partitions('rg')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name
                            2022-02-24T170000-000001
                            """);

            // Columns that the split takes after its parent went away have no _cv record at the floor.
            execute("ALTER TABLE rg ADD COLUMN late_vc VARCHAR");
            execute("ALTER TABLE rg ADD COLUMN late_bo BOOLEAN");
            execute("ALTER TABLE rg ADD COLUMN late_arr DOUBLE[]");
            execute("INSERT INTO rg SELECT 400 + x, timestamp_sequence('2022-02-24T19:00', 60_000_000), " + values
                    + ", rnd_varchar(2, 6, 1), rnd_boolean(), ARRAY[x * 3.0] FROM long_sequence(10)");
            drainWalQueue();
            execute("CREATE TABLE expected AS (SELECT * FROM rg) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            execute("ALTER TABLE rg CONVERT PARTITION TO PARQUET LIST '2022-02-24'");
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertQuery("SELECT name, isParquet FROM table_partitions('rg')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tisParquet
                            2022-02-24\ttrue
                            """);
            assertSqlCursors("expected", "rg");

            execute("ALTER TABLE rg CONVERT PARTITION TO NATIVE LIST '2022-02-24'");
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertSqlCursors("expected", "rg");

            // A reopened writer and fresh readers see the same data on disk.
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            assertSqlCursors("expected", "rg");
        });
    }

    @Test
    public void testReplaceRangeRemovesSplitParentThenConvertWithCoveringIndex() throws Exception {
        assertMemoryLeak(() -> {
            // Drop the absolute split threshold, so the prefix-to-tail ratio alone decides the split.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            // Merge-append turns the O3 batch into a composite partition instead of a split.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
            execute("CREATE TABLE rg (id LONG, ts TIMESTAMP, v LONG, sym SYMBOL INDEX TYPE POSTING INCLUDE (v)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");
            execute("INSERT INTO rg SELECT x, timestamp_sequence('2022-02-24T08:00', 600_000_000), x * 10, CASE WHEN x % 2 = 0 THEN 'a' ELSE 'b' END FROM long_sequence(60)");
            drainWalQueue();
            execute("INSERT INTO rg SELECT 100 + x, timestamp_sequence('2022-02-24T17:05', 600_000_000), 0, 'a' FROM long_sequence(5)");
            drainWalQueue();
            assertQuery("SELECT name, numRows FROM table_partitions('rg')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows
                            2022-02-24\t55
                            2022-02-24T170000-000001\t10
                            """);

            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2022-02-24T18:00"));
                row.putLong(0, 201);
                row.putLong(2, 2_010);
                row.putSym(3, "a");
                row.append();
                row = ww.newRow(MicrosTimestampDriver.floor("2022-02-24T18:30"));
                row.putLong(0, 202);
                row.putLong(2, 2_020);
                row.putSym(3, "b");
                row.append();
                ww.commitWithParams(
                        MicrosTimestampDriver.floor("2022-02-24T06:00"),
                        MicrosTimestampDriver.floor("2022-02-24T23:00"),
                        WAL_DEDUP_MODE_REPLACE_RANGE
                );
            }
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertQuery("SELECT name, numRows FROM table_partitions('rg')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows
                            2022-02-24T170000-000001\t2
                            """);

            // The squash into the re-created parent rebuilds the index, and the parquet partition
            // carries it over. The covering index answers the first query on its own.
            execute("ALTER TABLE rg CONVERT PARTITION TO PARQUET LIST '2022-02-24'");
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertQuery("SELECT name, numRows, isParquet FROM table_partitions('rg')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows\tisParquet
                            2022-02-24\t2\ttrue
                            """);
            assertQuery("SELECT v FROM rg WHERE sym = 'a'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            v
                            2010
                            """);
            assertQuery("SELECT id, ts FROM rg WHERE sym = 'b'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            id\tts
                            202\t2022-02-24T18:30:00.000000Z
                            """);

            execute("ALTER TABLE rg CONVERT PARTITION TO NATIVE LIST '2022-02-24'");
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertQuery("SELECT v FROM rg WHERE sym = 'a'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            v
                            2010
                            """);
            assertQuery("SELECT id, ts FROM rg WHERE sym = 'b'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            id\tts
                            202\t2022-02-24T18:30:00.000000Z
                            """);
        });
    }

    @Test
    public void testReplaceRangeRemovesSplitParentThenDetachAndAttach() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createDayWithSplitButNoParent();
            // DETACH refuses the active partition.
            execute("INSERT INTO rg VALUES (301, '2022-02-25T10:00', 0)");
            drainWalQueue();

            execute("ALTER TABLE rg DETACH PARTITION LIST '2022-02-24'");
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertQuery("SELECT * FROM rg")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts\tv
                            301\t2022-02-25T10:00:00.000000Z\t0
                            """);

            // DETACH re-creates the parent at the floor first, so the detached directory carries the
            // day's name.
            try (Path path = new Path(); Path other = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat("2022-02-24").put(DETACHED_DIR_MARKER).$();
                other.of(configuration.getDbRoot()).concat(tableToken).concat("2022-02-24").put(configuration.getAttachPartitionSuffix()).$();
                Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);
            }
            execute("ALTER TABLE rg ATTACH PARTITION LIST '2022-02-24'");
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertQuery("SELECT * FROM rg")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts\tv
                            201\t2022-02-24T18:00:00.000000Z\t0
                            202\t2022-02-24T18:30:00.000000Z\t0
                            301\t2022-02-25T10:00:00.000000Z\t0
                            """);
        });
    }

    @Test
    public void testReplaceRangeRemovesSplitParentThenDropPartition() throws Exception {
        assertMemoryLeak(() -> {
            // Drop the absolute split threshold, so the prefix-to-tail ratio alone decides the split.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            // Merge-append turns the O3 batch into a composite partition instead of a split.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
            execute("CREATE TABLE rg (id LONG, ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");
            execute("INSERT INTO rg VALUES (1, '2022-02-22T12:00', 0)");
            drainWalQueue();
            execute("INSERT INTO rg SELECT x, timestamp_sequence('2022-02-24T08:00', 600_000_000), 0 FROM long_sequence(60)");
            drainWalQueue();
            execute("INSERT INTO rg SELECT 100 + x, timestamp_sequence('2022-02-24T17:05', 600_000_000), 0 FROM long_sequence(5)");
            drainWalQueue();
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                appendRow(ww, MicrosTimestampDriver.floor("2022-02-24T18:00"), 201, 0);
                ww.commitWithParams(
                        MicrosTimestampDriver.floor("2022-02-24T06:00"),
                        MicrosTimestampDriver.floor("2022-02-24T23:00"),
                        WAL_DEDUP_MODE_REPLACE_RANGE
                );
            }
            drainWalQueue();
            execute("INSERT INTO rg VALUES (301, '2022-02-26T05:00', 0)");
            drainWalQueue();
            assertQuery("SELECT name FROM table_partitions('rg')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name
                            2022-02-22
                            2022-02-24T170000-000001
                            2022-02-26
                            """);

            // The lookup finds the split through its day's floor, and no other day through a gap.
            try (TableWriter writer = getWriter(tableToken)) {
                final TxWriter txWriter = writer.getTxWriter();
                Assert.assertEquals(-1, txWriter.getLogicalPartitionFirstIndex(MicrosTimestampDriver.floor("2022-02-21")));
                Assert.assertEquals(0, txWriter.getLogicalPartitionFirstIndex(MicrosTimestampDriver.floor("2022-02-22T23:59")));
                Assert.assertEquals(-1, txWriter.getLogicalPartitionFirstIndex(MicrosTimestampDriver.floor("2022-02-23")));
                Assert.assertEquals(1, txWriter.getLogicalPartitionFirstIndex(MicrosTimestampDriver.floor("2022-02-24")));
                Assert.assertEquals(1, txWriter.getLogicalPartitionFirstIndex(MicrosTimestampDriver.floor("2022-02-24T23:59:59.999999")));
                Assert.assertEquals(-1, txWriter.getLogicalPartitionFirstIndex(MicrosTimestampDriver.floor("2022-02-25")));
                Assert.assertEquals(2, txWriter.getLogicalPartitionFirstIndex(MicrosTimestampDriver.floor("2022-02-26")));
                Assert.assertEquals(-1, txWriter.getLogicalPartitionFirstIndex(MicrosTimestampDriver.floor("2022-02-27")));
            }

            // DROP and DETACH of the missing day before the split leave the split alone.
            execute("ALTER TABLE rg DROP PARTITION LIST '2022-02-23'");
            execute("ALTER TABLE rg DETACH PARTITION LIST '2022-02-23'");
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertQuery("SELECT name FROM table_partitions('rg')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name
                            2022-02-22
                            2022-02-24T170000-000001
                            2022-02-26
                            """);

            execute("ALTER TABLE rg DROP PARTITION LIST '2022-02-24'");
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertQuery("SELECT name FROM table_partitions('rg')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name
                            2022-02-22
                            2022-02-26
                            """);
            assertQuery("SELECT id FROM rg")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id
                            1
                            301
                            """);
        });
    }

    @Test
    public void testReplaceRangeRemovesSplitParentThenPrepareParquetWithOpenReader() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createDayWithSplitButNoParent();
            // preparePartitionForParquetConversion() skips the active partition.
            execute("INSERT INTO rg VALUES (301, '2022-02-25T10:00', 0)");
            drainWalQueue();

            final long dayTimestamp = MicrosTimestampDriver.floor("2022-02-24");
            try (TableWriter writer = getWriter(tableToken)) {
                try (TableReader ignore = getReader(tableToken)) {
                    // The day stays native, so the parent that the squash re-creates stays live. A
                    // reader on the latest txn must not make the squash copy the new parent: the copy
                    // would reuse the parent's directory name and queue that live directory for removal.
                    Assert.assertEquals(dayTimestamp, writer.preparePartitionForParquetConversion(dayTimestamp));
                }
                // The next commit runs housekeeping, which removes queued partition versions that no
                // reader uses.
                TableWriter.Row row = writer.newRow(MicrosTimestampDriver.floor("2022-02-25T11:00"));
                row.putLong(0, 302);
                row.putLong(2, 0);
                row.append();
                writer.commit();
            }
            assertQuery("SELECT name, numRows, isParquet FROM table_partitions('rg')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows\tisParquet
                            2022-02-24\t2\tfalse
                            2022-02-25\t2\tfalse
                            """);
            assertQuery("SELECT * FROM rg")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts\tv
                            201\t2022-02-24T18:00:00.000000Z\t0
                            202\t2022-02-24T18:30:00.000000Z\t0
                            301\t2022-02-25T10:00:00.000000Z\t0
                            302\t2022-02-25T11:00:00.000000Z\t0
                            """);
        });
    }

    @Test
    public void testReplaceRangeRemovesSplitParentWithTwoSplitsThenConvertToParquet() throws Exception {
        assertMemoryLeak(() -> {
            // Drop the absolute split threshold, so the prefix-to-tail ratio alone decides the split.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            // Merge-append turns the O3 batch into a composite partition instead of a split.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
            execute("CREATE TABLE rg (id LONG, ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");
            execute("INSERT INTO rg SELECT x, timestamp_sequence('2022-02-24T08:00', 600_000_000), 0 FROM long_sequence(60)");
            drainWalQueue();
            // Split the day after 17:00, grow the split up to 23:50, then split the split after 23:00.
            execute("INSERT INTO rg SELECT 100 + x, timestamp_sequence('2022-02-24T17:05', 600_000_000), 0 FROM long_sequence(5)");
            drainWalQueue();
            execute("INSERT INTO rg SELECT 1_000 + x, timestamp_sequence('2022-02-24T18:00', 600_000_000), 0 FROM long_sequence(36)");
            drainWalQueue();
            execute("INSERT INTO rg SELECT 2_000 + x, timestamp_sequence('2022-02-24T23:05', 600_000_000), 0 FROM long_sequence(5)");
            drainWalQueue();
            assertQuery("SELECT name, numRows FROM table_partitions('rg')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows
                            2022-02-24\t55
                            2022-02-24T170000-000001\t41
                            2022-02-24T230000-000001\t10
                            """);

            // The replace range covers the parent and the head of the first split.
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                appendRow(ww, MicrosTimestampDriver.floor("2022-02-24T17:12"), 3_001, 0);
                appendRow(ww, MicrosTimestampDriver.floor("2022-02-24T17:22"), 3_002, 0);
                ww.commitWithParams(
                        MicrosTimestampDriver.floor("2022-02-24T06:00"),
                        MicrosTimestampDriver.floor("2022-02-24T17:30"),
                        WAL_DEDUP_MODE_REPLACE_RANGE
                );
            }
            drainWalQueue();
            assertNotSuspended(tableToken);
            assertQuery("SELECT name, numRows FROM table_partitions('rg')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows
                            2022-02-24T170000-000001\t38
                            2022-02-24T230000-000001\t10
                            """);

            execute("ALTER TABLE rg CONVERT PARTITION TO PARQUET LIST '2022-02-24'");
            drainWalQueue();
            assertNotSuspended(tableToken);
            // CONVERT re-creates the parent and squashes both splits into it.
            assertQuery("SELECT name, numRows, isParquet FROM table_partitions('rg')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            name\tnumRows\tisParquet
                            2022-02-24\t48\ttrue
                            """);
            // Kept: ids 58..60, 104, 105, 1_001..1_036, 2_001..2_005 and the replacement rows 3_001, 3_002.
            assertQuery("SELECT count(), min(ts), max(ts), sum(id) FROM rg")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count\tmin\tmax\tsum
                            48\t2022-02-24T17:12:00.000000Z\t2022-02-24T23:50:00.000000Z\t53070
                            """);
        });
    }

    @Test
    public void testReplaceRangeSingleCommitMiddleMerge() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(47)");

            drainWalQueue();

            assertQuery("select min(ts), max(ts), count(*) from rg")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            min\tmax\tcount
                            2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t47
                            """);

            Utf8StringSink sink = new Utf8StringSink();

            insertRowsWithRangeReplace(tableToken, sink, "2022-02-24T14:45", "2022-02-24T12:45", "2022-02-24T23", true);
            drainWalQueue();

            assertQuery("select min(ts), max(ts), count(*) from rg")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            min\tmax\tcount
                            2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t6
                            """);

            assertQuery("select * from rg")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts\ty\ts\tv\tm
                            1\t2022-02-24T12:30:00.000000Z\t0\t1\t&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\ta
                            100\t2022-02-24T14:45:00.000000Z\t1000\thello\tw\tw
                            44\t2022-02-24T23:15:00.000000Z\t22\t44\t\uDAB1\uDC25J\uD969\uDF86gǢ\uDA97\uDEDC\t
                            45\t2022-02-24T23:30:00.000000Z\t22\t45\tHEZqUhE\ta
                            46\t2022-02-24T23:45:00.000000Z\t23\t46\tL^bE);P\tc
                            47\t2022-02-25T00:00:00.000000Z\t23\t47\t阇1(rոҊG\uD9A6\uDD42\uDB48\uDC78\tb
                            """);


            // Check _txn file makes sense, min, max timestamps, row counts etc.
            Assert.assertEquals(
                    """
                            {txn: 2, attachedPartitions: [
                            {ts: '2022-02-24T00:00:00.000Z', rowCount: 5},
                            {ts: '2022-02-25T00:00:00.000Z', rowCount: 1}
                            ], transientRowCount: 1, fixedRowCount: 5, \
                            minTimestamp: '2022-02-24T12:30:00.000Z', \
                            maxTimestamp: '2022-02-25T00:00:00.000Z', \
                            dataVersion: 0, structureVersion: 0, \
                            columnVersion: 0, truncateVersion: 0, seqTxn: 2, \
                            symbolColumnCount: 1, lagRowCount: 0, \
                            lagMinTimestamp: '294247-01-10T04:00:54.775Z', \
                            lagMaxTimestamp: '', lagTxnCount: 0, lagOrdered: true}""",
                    readTxnToString(tableToken, true, true)
            );
        });
    }

    @Test
    public void testReplaceRangeSingleCommitPrefixMerge() throws Exception {
        setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 500_000);
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(47)");

            drainWalQueue();

            assertQuery("select min(ts), max(ts), count(*) from rg")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            min\tmax\tcount
                            2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t47
                            """);

            Utf8StringSink sink = new Utf8StringSink();

            insertRowsWithRangeReplace(tableToken, sink, "2022-02-24T14:30", "2022-02-24", "2022-02-24T23", true);
            drainWalQueue();

            assertQuery("select min(ts), max(ts), count(*) from rg")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            min\tmax\tcount
                            2022-02-24T14:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t5
                            """);

            assertQuery("select * from rg")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts\ty\ts\tv\tm
                            100\t2022-02-24T14:30:00.000000Z\t1000\thello\tw\tw
                            44\t2022-02-24T23:15:00.000000Z\t22\t44\t\uDAB1\uDC25J\uD969\uDF86gǢ\uDA97\uDEDC\t
                            45\t2022-02-24T23:30:00.000000Z\t22\t45\tHEZqUhE\ta
                            46\t2022-02-24T23:45:00.000000Z\t23\t46\tL^bE);P\tc
                            47\t2022-02-25T00:00:00.000000Z\t23\t47\t阇1(rոҊG\uD9A6\uDD42\uDB48\uDC78\tb
                            """);


            // Check _txn file makes sense, min, max timestamps, row counts etc.
            Assert.assertEquals(
                    """
                            {txn: 2, attachedPartitions: [
                            {ts: '2022-02-24T00:00:00.000Z', rowCount: 4},
                            {ts: '2022-02-25T00:00:00.000Z', rowCount: 1}
                            ], transientRowCount: 1, fixedRowCount: 4, \
                            minTimestamp: '2022-02-24T14:30:00.000Z', \
                            maxTimestamp: '2022-02-25T00:00:00.000Z', \
                            dataVersion: 0, structureVersion: 0, \
                            columnVersion: 0, truncateVersion: 0, seqTxn: 2, \
                            symbolColumnCount: 1, lagRowCount: 0, \
                            lagMinTimestamp: '294247-01-10T04:00:54.775Z', \
                            lagMaxTimestamp: '', lagTxnCount: 0, lagOrdered: true}""",
                    readTxnToString(tableToken, true, true)
            );
        });
    }

    @Test
    public void testReplaceRangeSingleCommitSuffixMerge() throws Exception {
        setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 500_000);
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(47)");

            drainWalQueue();

            assertQuery("select min(ts), max(ts), count(*) from rg")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            min\tmax\tcount
                            2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t47
                            """);

            Utf8StringSink sink = new Utf8StringSink();

            insertRowsWithRangeReplace(tableToken, sink, "2022-02-24T14:45", "2022-02-24T12:45", "2022-02-24T23:59:59.999999", true);
            drainWalQueue();

            assertQuery("select min(ts), max(ts), count(*) from rg")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            min\tmax\tcount
                            2022-02-24T12:30:00.000000Z\t2022-02-25T00:00:00.000000Z\t3
                            """);

            assertQuery("select * from rg")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts\ty\ts\tv\tm
                            1\t2022-02-24T12:30:00.000000Z\t0\t1\t&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\ta
                            100\t2022-02-24T14:45:00.000000Z\t1000\thello\tw\tw
                            47\t2022-02-25T00:00:00.000000Z\t23\t47\t阇1(rոҊG\uD9A6\uDD42\uDB48\uDC78\tb
                            """);

            // Check _txn file makes sense, min, max timestamps, row counts etc.
            Assert.assertEquals(
                    """
                            {txn: 2, attachedPartitions: [
                            {ts: '2022-02-24T00:00:00.000Z', rowCount: 2},
                            {ts: '2022-02-25T00:00:00.000Z', rowCount: 1}
                            ], transientRowCount: 1, fixedRowCount: 2, \
                            minTimestamp: '2022-02-24T12:30:00.000Z', \
                            maxTimestamp: '2022-02-25T00:00:00.000Z', \
                            dataVersion: 0, structureVersion: 0, \
                            columnVersion: 0, truncateVersion: 0, seqTxn: 2, \
                            symbolColumnCount: 1, lagRowCount: 0, \
                            lagMinTimestamp: '294247-01-10T04:00:54.775Z', \
                            lagMaxTimestamp: '', lagTxnCount: 0, lagOrdered: true}""",
                    readTxnToString(tableToken, true, true)
            );
        });
    }

    @Test
    public void testReplaceRangeSkipsInsertBeforeBarrierOnMatView() throws Exception {
        // Mat-view counterpart of testReplaceRangeDoesNotSkipInsertBeforeMatViewInvalidate. For a regular
        // table the skip look-ahead stops at the first non-data transaction (the column-conversion divergence
        // fix). A MATERIALIZED VIEW is exempt: a column type conversion cannot run on a mat view, so the
        // cross-instance divergence the barrier guards against cannot arise, and disabling the optimization
        // would make a full mat view refresh (which truncates / replaces) materialise data it is about to
        // overwrite. So for a mat view the scan passes the MAT_VIEW_INVALIDATE barrier and the INSERT before
        // it is skipped, exactly as before the fix.
        //
        // The skip is a physical-write optimization, not a data change: the covering REPLACE_RANGE overwrites
        // the inserted rows either way, so the rendered data is identical whether or not the INSERT is skipped.
        // The test therefore guards the exemption through physically-written rows rather than the data: when the
        // INSERT is skipped, only the REPLACE_RANGE's rows reach disk; if the isMatView() branch regressed and
        // applied the INSERT, twice as many rows would be written and the assertion below would fail.
        assertMemoryLeak(() -> {
            execute("create table base (s string, v double, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("create materialized view mv as (select s, last(v) v, ts from base sample by 1h) partition by DAY");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("mv");
            Assert.assertTrue("mv must be a materialized view", tableToken.isMatView());

            final long rangeLo = MicrosTimestampDriver.floor("2022-02-24T00");
            final long rangeHi = MicrosTimestampDriver.floor("2022-02-24T01"); // exclusive

            // INSERT covered by the later REPLACE_RANGE - skipped only if the scan passes the barrier.
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = ww.newRow(rangeLo + i * 60_000_000L);
                    row.putStr(0, "old" + i);
                    row.putDouble(1, i);
                    row.append();
                }
                ww.commit();
            }

            // MAT_VIEW_INVALIDATE barrier between the insert and the replace. The table genuinely is a mat
            // view, so isMatView() selects the original (scan-past-the-barrier) behaviour.
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                ww.resetMatViewState(1, 1, true, "test invalidation", Numbers.LONG_NULL, null, -1);
            }

            // REPLACE_RANGE fully covering the insert, with new values.
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = ww.newRow(rangeLo + i * 60_000_000L);
                    row.putStr(0, "new" + i);
                    row.putDouble(1, 1000 + i);
                    row.append();
                }
                ww.commitWithParams(rangeLo, rangeHi, WAL_DEDUP_MODE_REPLACE_RANGE);
            }

            // Apply insert + mat-view-invalidate + replace in one batch.
            final long physicalRowsBefore = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
            drainWalQueue();

            // Only the replacement values remain.
            assertQuery("select s, v from mv order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tv
                            new0\t1000.0
                            new1\t1001.0
                            new2\t1002.0
                            new3\t1003.0
                            new4\t1004.0
                            new5\t1005.0
                            new6\t1006.0
                            new7\t1007.0
                            new8\t1008.0
                            new9\t1009.0
                            """);

            // The data assertion above is identical whether or not the INSERT is skipped, so it does not guard
            // the mat-view exemption on its own. Pin the skip directly via physically-written rows: skipping the
            // INSERT means only the covering REPLACE_RANGE's 10 rows reach disk, not the INSERT's 10 as well.
            // If the isMatView() exemption regressed and applied the INSERT, this delta would be 20.
            Assert.assertEquals(
                    10,
                    engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - physicalRowsBefore
            );
        });
    }

    @Test
    public void testReplaceRangeSkipsInsertBeforeMultipleBarriersAndTruncateOnMatView() throws Exception {
        // Stacked-barrier variant of testReplaceRangeSkipsInsertBeforeTruncateAfterBarrierOnMatView:
        //   INSERT(0) -> MAT_VIEW_INVALIDATE(1) -> MAT_VIEW_INVALIDATE(2) -> TRUNCATE(3)
        // For a mat view the scan records each soft barrier (firstNonSkippableTxn = min over both) and continues
        // to the truncate, returning min(firstNonSkippableTxn, truncate) = the FIRST barrier. So the INSERT is
        // skipped, but the skip is clamped to the first invalidate - neither invalidate is skipped past. This
        // exercises the firstNonSkippableTxn = Math.min(...) accumulation across more than one barrier, which the
        // single-barrier tests do not.
        //
        // The truncate wipes the insert either way, so this is guarded through physically-written rows: a skipped
        // insert never reaches disk, so only the single surviving post-truncate row is written.
        assertMemoryLeak(() -> {
            execute("create table base (s string, v double, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("create materialized view mv as (select s, last(v) v, ts from base sample by 1h) partition by DAY");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("mv");
            Assert.assertTrue("mv must be a materialized view", tableToken.isMatView());

            final long rangeLo = MicrosTimestampDriver.floor("2022-02-24T00");

            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                // INSERT - skippable only if the scan passes both barriers and reaches the truncate.
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = ww.newRow(rangeLo + i * 60_000_000L);
                    row.putStr(0, "old" + i);
                    row.putDouble(1, i);
                    row.append();
                }
                ww.commit();

                // Two MAT_VIEW_INVALIDATE barriers between the insert and the truncate.
                ww.resetMatViewState(1, 1, true, "first invalidation", Numbers.LONG_NULL, null, -1);
                ww.resetMatViewState(2, 2, true, "second invalidation", Numbers.LONG_NULL, null, -1);

                // TRUNCATE - the same call a full refresh makes.
                ww.truncateSoft();

                // One surviving row after the truncate, so there is observable data.
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2022-02-25T00"));
                row.putStr(0, "kept");
                row.putDouble(1, 42);
                row.append();
                ww.commit();
            }

            // Apply insert + two invalidates + truncate + insert in one batch.
            final long physicalRowsBefore = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
            drainWalQueue();

            // Only the post-truncate row survives.
            assertQuery("select s, v from mv order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tv
                            kept\t42.0
                            """);

            // Skipping the INSERT (clamped to the first barrier) means only the single surviving post-truncate row
            // reaches disk, not the INSERT's 10 rows first. If the exemption regressed and applied the INSERT
            // before the truncate, this delta would be 11.
            Assert.assertEquals(
                    1,
                    engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - physicalRowsBefore
            );
        });
    }

    @Test
    public void testReplaceRangeSkipsInsertBeforeTruncateAfterBarrierOnMatView() throws Exception {
        // The TRUNCATE skip-clamp path of the mat-view exemption - the reviewer's exact trace:
        //   INSERT(0) -> barrier(1) -> TRUNCATE(2)
        // For a regular table (testReplaceRangeDoesNotSkipInsertBeforeTruncateAfterBarrier) the scan breaks
        // at the barrier and the INSERT is applied then truncated. For a MATERIALIZED VIEW the scan records
        // the barrier and continues to the truncate, returning min(barrier, truncate) = barrier, so the
        // INSERT is skipped - but clamped to the barrier, so the MAT_VIEW_INVALIDATE itself is still applied,
        // not skipped past. This is the optimization a full mat view refresh relies on: refresh truncates via
        // WalWriter.truncateSoft() (see MatViewRefreshJob), so the data it is about to wipe is not
        // materialised first.
        //
        // ADD COLUMN (the barrier in the original trace) cannot run on a mat view, so MAT_VIEW_INVALIDATE is
        // the realistic mat-view barrier. The truncate wipes the insert either way, so the rendered data is
        // identical whether or not the INSERT is skipped. The test therefore guards the exemption through
        // physically-written rows rather than the data: when the INSERT is skipped, its rows never reach disk;
        // if the isMatView() branch regressed and applied the INSERT before the truncate, those rows would be
        // written first and the assertion below would fail.
        assertMemoryLeak(() -> {
            execute("create table base (s string, v double, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("create materialized view mv as (select s, last(v) v, ts from base sample by 1h) partition by DAY");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("mv");
            Assert.assertTrue("mv must be a materialized view", tableToken.isMatView());

            final long rangeLo = MicrosTimestampDriver.floor("2022-02-24T00");

            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                // INSERT - skippable only if the scan passes the barrier and reaches the truncate.
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = ww.newRow(rangeLo + i * 60_000_000L);
                    row.putStr(0, "old" + i);
                    row.putDouble(1, i);
                    row.append();
                }
                ww.commit();

                // MAT_VIEW_INVALIDATE barrier between the insert and the truncate.
                ww.resetMatViewState(1, 1, true, "test invalidation", Numbers.LONG_NULL, null, -1);

                // TRUNCATE - the same call a full refresh makes.
                ww.truncateSoft();

                // One surviving row after the truncate, so there is observable data.
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2022-02-25T00"));
                row.putStr(0, "kept");
                row.putDouble(1, 42);
                row.append();
                ww.commit();
            }

            // Apply insert + mat-view-invalidate + truncate + insert in one batch.
            final long physicalRowsBefore = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
            drainWalQueue();

            // Only the post-truncate row survives.
            assertQuery("select s, v from mv order by ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tv
                            kept\t42.0
                            """);

            // The truncate wipes the INSERT either way, so the data assertion above does not guard the mat-view
            // exemption. Pin the skip directly via physically-written rows: skipping the INSERT (clamped to the
            // barrier) means only the single surviving post-truncate row reaches disk, not the INSERT's 10 rows
            // first. If the isMatView() exemption regressed and applied the INSERT before the truncate, this
            // delta would be 11.
            Assert.assertEquals(
                    1,
                    engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - physicalRowsBefore
            );
        });
    }

    @Test
    public void testReplaceRangeTwoPartitionsDataInFirst() throws Exception {
        testReplaceRangeCommit("2022-02-24T16:25", "2022-02-24T16:25", "2022-02-25T01:00");
    }

    @Test
    public void testReplaceRangeTwoPartitionsDataInSecond() throws Exception {
        testReplaceRangeCommit("2022-02-25T01:00", "2022-02-24T16:25", "2022-02-25T01:00");
    }

    @Test
    public void testReplaceRangeWithColumnAddedInMiddleOfPartition() throws Exception {
        assertMemoryLeak(() -> {
            final int partitionRowCount = 500;

            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            // Insert 1000 rows to fill one partition
            execute("insert into rg select x, timestamp_sequence('2022-02-24T01:00', 86 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(" + partitionRowCount + ")");
            drainWalQueue();

            // Add both fixed size (int) and variable size (string) columns after the partition is full
            // This sets column_top = 1000 for both columns
            execute("alter table rg add column c4 int");
            execute("alter table rg add column c5 string");
            drainWalQueue();

            // Now perform a replace operation on ALL 1000 rows (the entire area where column data doesn't exist)
            // This should trigger the column top fix for both fixed and variable size columns
            Utf8StringSink sink = new Utf8StringSink();
            try (WalWriter ww = engine.getWalWriter(tableToken)) {
                // Add one row with new column values for the replace operation
                long ts = MicrosTimestampDriver.floor("2022-02-24T09");
                TableWriter.Row row = ww.newRow(ts);
                row.putInt(0, 999);
                row.putLong(2, 9999);
                row.putStr(3, "replaced");
                sink.clear();
                sink.put("replaced_varchar");
                row.putVarchar(4, sink);
                row.putSym(5, "replaced_sym");
                row.putInt(6, 42); // c4 - fixed size column
                row.putStr(7, "replaced_string"); // c5 - variable size column
                row.append();

                // Replace the entire partition range
                long rangeStart = MicrosTimestampDriver.floor("2022-02-24T00");
                long rangeEnd = MicrosTimestampDriver.floor("2022-02-24T10");
                ww.commitWithParams(rangeStart, rangeEnd, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();

            // Verify the row content
            assertQuery("select * from rg limit 10")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            id\tts\ty\ts\tv\tm\tc4\tc5
                            999\t2022-02-24T09:00:00.000000Z\t9999\treplaced\treplaced_varchar\treplaced_sym\t42\treplaced_string
                            378\t2022-02-24T10:00:22.000000Z\t189\t378\tNUZ[\tc\tnull\t
                            379\t2022-02-24T10:01:48.000000Z\t189\t379\t@xbR>i@s\ta\tnull\t
                            380\t2022-02-24T10:03:14.000000Z\t190\t380\t;WS\tc\tnull\t
                            381\t2022-02-24T10:04:40.000000Z\t190\t381\tV1IF \t\tnull\t
                            382\t2022-02-24T10:06:06.000000Z\t191\t382\t14wddTh&))\tb\tnull\t
                            383\t2022-02-24T10:07:32.000000Z\t191\t383\t#<Y达\u197F亙ጾ燇Ȉc\ta\tnull\t
                            384\t2022-02-24T10:08:58.000000Z\t192\t384\t\uDB9E\uDD3D\uF29Ec+ɫwՊ毷걭\ta\tnull\t
                            385\t2022-02-24T10:10:24.000000Z\t192\t385\tsoMv* !Em>\ta\tnull\t
                            386\t2022-02-24T10:11:50.000000Z\t193\t386\t@1oq.w%V\tb\tnull\t
                            """);
        });
    }

    @Test
    public void testReplaceTruncatesAllData() throws Exception {
        testReplaceTruncatesAllData(false);
    }

    @Test
    public void testReplaceTruncatesAllDataAndAddsNewBeforeExisting() throws Exception {
        testReplaceTruncatesAllDataAndAddsNewBeforeExisting(false);
    }

    @Test
    public void testReplaceTruncatesAllDataAndAddsNewBeforeExistingNoRowsCommit() throws Exception {
        testReplaceTruncatesAllDataAndAddsNewBeforeExisting(true);
    }

    @Test
    public void testReplaceTruncatesAllDataNoRowsCommit() throws Exception {
        testReplaceTruncatesAllData(true);
    }

    @Test
    public void testSkipDoesNotCrossSymbolConversion() throws Exception {
        // When a future replace-range txn justifies skipping earlier data txns, the scan must
        // not cross a structural (non-data) txn such as ALTER COLUMN ... TYPE SYMBOL. The
        // conversion seeds its symbol map from the table rows that exist at apply time; skipping
        // the pre-conversion rows under a large lookahead changes which strings get registered
        // first, silently diverging the symbol key assignment from a low-lookahead applier.
        assertMemoryLeak(() -> {
            // Build t1 with a tiny lag window (lookahead=1, maxLagSize=1, block size=1) so the apply job
            // sees only the current txn and cannot look ahead to the future replace-range.
            node1.setProperty(CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 1);
            node1.setProperty(CAIRO_WAL_MAX_LAG_SIZE, 1);
            node1.setProperty(CAIRO_WAL_APPLY_BLOCK_SIZE, 1);
            execute("create table rg_conv_small (id long, ts timestamp, s string) timestamp(ts) partition by DAY WAL");
            TableToken tt1 = engine.verifyTableName("rg_conv_small");

            // Three separate data commits with distinct strings (each is a separate sequencer txn).
            // Use separate WalWriter instances to ensure separate sequencer txns.
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2022-02-24T01:00"));
                row.putLong(0, 1);
                row.putStr(2, "AAA");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2022-02-24T02:00"));
                row.putLong(0, 2);
                row.putStr(2, "BBB");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2022-02-24T03:00"));
                row.putLong(0, 3);
                row.putStr(2, "CCC");
                row.append();
                ww.commit();
            }

            // Structural txn: convert s from STRING to SYMBOL
            execute("alter table rg_conv_small alter column s type symbol");

            // Post-conversion data commits via SQL INSERT (each INSERT is a separate sequencer txn).
            // The first-encounter order differs from the pre-conversion writes: CCC, AAA, DDD.
            execute("insert into rg_conv_small values (4, '2022-02-24T10:00:00.000000Z', 'CCC')");
            execute("insert into rg_conv_small values (5, '2022-02-24T11:00:00.000000Z', 'AAA')");
            execute("insert into rg_conv_small values (6, '2022-02-24T12:00:00.000000Z', 'DDD')");

            // Replace-range covering exactly the first three rows (01:00..03:00 window)
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                long rangeLo = MicrosTimestampDriver.floor("2022-02-24T00:00");
                long rangeHi = MicrosTimestampDriver.floor("2022-02-24T04:00");
                ww.commitWithParams(rangeLo, rangeHi, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();

            // Build t2 with a large lookahead so all future txns are visible and the
            // replace-range can justify skipping across the ALTER txn (the bug path).
            node1.setProperty(CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 10_000);
            node1.setProperty(CAIRO_WAL_MAX_LAG_SIZE, 1024 * 1024 * 1024);
            node1.setProperty(CAIRO_WAL_APPLY_BLOCK_SIZE, 1024 * 1024 * 1024);
            execute("create table rg_conv_large (id long, ts timestamp, s string) timestamp(ts) partition by DAY WAL");
            TableToken tt2 = engine.verifyTableName("rg_conv_large");

            try (WalWriter ww = engine.getWalWriter(tt2)) {
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2022-02-24T01:00"));
                row.putLong(0, 1);
                row.putStr(2, "AAA");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt2)) {
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2022-02-24T02:00"));
                row.putLong(0, 2);
                row.putStr(2, "BBB");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt2)) {
                TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor("2022-02-24T03:00"));
                row.putLong(0, 3);
                row.putStr(2, "CCC");
                row.append();
                ww.commit();
            }
            execute("alter table rg_conv_large alter column s type symbol");
            execute("insert into rg_conv_large values (4, '2022-02-24T10:00:00.000000Z', 'CCC')");
            execute("insert into rg_conv_large values (5, '2022-02-24T11:00:00.000000Z', 'AAA')");
            execute("insert into rg_conv_large values (6, '2022-02-24T12:00:00.000000Z', 'DDD')");
            try (WalWriter ww = engine.getWalWriter(tt2)) {
                long rangeLo = MicrosTimestampDriver.floor("2022-02-24T00:00");
                long rangeHi = MicrosTimestampDriver.floor("2022-02-24T04:00");
                ww.commitWithParams(rangeLo, rangeHi, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();

            // (a) Query contents must agree
            assertQuery("select id, s from rg_conv_small order by id")
                    .noLeakCheck()
                    .expectSize()
                    .returns("id\ts\n4\tCCC\n5\tAAA\n6\tDDD\n");
            assertQuery("select id, s from rg_conv_large order by id")
                    .noLeakCheck()
                    .expectSize()
                    .returns("id\ts\n4\tCCC\n5\tAAA\n6\tDDD\n");

            // (b) Symbol maps must be identical: same count, same key->string mapping.
            // Pre-fix, t1 seeds the map from the pre-conversion rows (AAA=0, BBB=1, CCC=2)
            // while t2 skips them and starts with CCC=0, AAA=1, DDD=2.
            try (TableReader r1 = engine.getReader(tt1); TableReader r2 = engine.getReader(tt2)) {
                int colIdx1 = r1.getMetadata().getColumnIndex("s");
                int colIdx2 = r2.getMetadata().getColumnIndex("s");
                SymbolMapReader sm1 = r1.getSymbolMapReader(colIdx1);
                SymbolMapReader sm2 = r2.getSymbolMapReader(colIdx2);
                int count = sm1.getSymbolCount();
                Assert.assertEquals("symbol counts differ", count, sm2.getSymbolCount());
                for (int k = 0; k < count; k++) {
                    Assert.assertEquals(
                            "symbol key " + k + " differs",
                            String.valueOf(sm1.valueOf(k)),
                            String.valueOf(sm2.valueOf(k))
                    );
                }
            }
        });
    }

    @Test
    public void testSkipDoesNotCrossTruncate() throws Exception {
        // When a future replace-range txn justifies skipping earlier data txns, the inner scan
        // must not jump straight to TRUNCATE across a non-data structural txn. A soft truncate
        // keeps symbol maps in place, so registering symbols in different orders (depending on
        // which rows were visible when the map was seeded) would make the map applier-dependent.
        assertMemoryLeak(() -> {
            // Build t1 with a tiny lag window so the apply job cannot look ahead to the
            // future replace-range and does not skip any txns.
            node1.setProperty(CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 1);
            node1.setProperty(CAIRO_WAL_MAX_LAG_SIZE, 1);
            node1.setProperty(CAIRO_WAL_APPLY_BLOCK_SIZE, 1);
            execute("create table rg_trunc_small (id long, ts timestamp, s symbol) timestamp(ts) partition by DAY WAL");
            TableToken tt1 = engine.verifyTableName("rg_trunc_small");

            long t1 = MicrosTimestampDriver.floor("2022-02-24T01:00");
            long t2 = MicrosTimestampDriver.floor("2022-02-24T02:00");
            long t3 = MicrosTimestampDriver.floor("2022-02-24T03:00");
            long t4 = MicrosTimestampDriver.floor("2022-02-24T10:00");
            long t5 = MicrosTimestampDriver.floor("2022-02-24T11:00");
            long t6 = MicrosTimestampDriver.floor("2022-02-24T12:00");

            // Three separate data commits before the truncate
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                TableWriter.Row row = ww.newRow(t1);
                row.putLong(0, 1);
                row.putSym(2, "AAA");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                TableWriter.Row row = ww.newRow(t2);
                row.putLong(0, 2);
                row.putSym(2, "BBB");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                TableWriter.Row row = ww.newRow(t3);
                row.putLong(0, 3);
                row.putSym(2, "CCC");
                row.append();
                ww.commit();
            }

            // Structural txn: TRUNCATE (soft-truncate keeps symbol maps)
            execute("truncate table rg_trunc_small");

            // More data commits after the truncate, same symbols in a different first-encounter order
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                TableWriter.Row row = ww.newRow(t4);
                row.putLong(0, 4);
                row.putSym(2, "CCC");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                TableWriter.Row row = ww.newRow(t5);
                row.putLong(0, 5);
                row.putSym(2, "AAA");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                TableWriter.Row row = ww.newRow(t6);
                row.putLong(0, 6);
                row.putSym(2, "DDD");
                row.append();
                ww.commit();
            }

            // Replace-range covering the first batch of rows
            try (WalWriter ww = engine.getWalWriter(tt1)) {
                long rangeLo = MicrosTimestampDriver.floor("2022-02-24T00:00");
                long rangeHi = MicrosTimestampDriver.floor("2022-02-24T04:00");
                ww.commitWithParams(rangeLo, rangeHi, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();

            // Build t2 with a large lag window so all future txns are visible and the
            // replace-range can justify skipping across the TRUNCATE txn (the bug path).
            node1.setProperty(CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT, 10_000);
            node1.setProperty(CAIRO_WAL_MAX_LAG_SIZE, 1024 * 1024 * 1024);
            node1.setProperty(CAIRO_WAL_APPLY_BLOCK_SIZE, 1024 * 1024 * 1024);
            execute("create table rg_trunc_large (id long, ts timestamp, s symbol) timestamp(ts) partition by DAY WAL");
            TableToken tt2 = engine.verifyTableName("rg_trunc_large");

            try (WalWriter ww = engine.getWalWriter(tt2)) {
                TableWriter.Row row = ww.newRow(t1);
                row.putLong(0, 1);
                row.putSym(2, "AAA");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt2)) {
                TableWriter.Row row = ww.newRow(t2);
                row.putLong(0, 2);
                row.putSym(2, "BBB");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt2)) {
                TableWriter.Row row = ww.newRow(t3);
                row.putLong(0, 3);
                row.putSym(2, "CCC");
                row.append();
                ww.commit();
            }
            execute("truncate table rg_trunc_large");
            try (WalWriter ww = engine.getWalWriter(tt2)) {
                TableWriter.Row row = ww.newRow(t4);
                row.putLong(0, 4);
                row.putSym(2, "CCC");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt2)) {
                TableWriter.Row row = ww.newRow(t5);
                row.putLong(0, 5);
                row.putSym(2, "AAA");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt2)) {
                TableWriter.Row row = ww.newRow(t6);
                row.putLong(0, 6);
                row.putSym(2, "DDD");
                row.append();
                ww.commit();
            }
            try (WalWriter ww = engine.getWalWriter(tt2)) {
                long rangeLo = MicrosTimestampDriver.floor("2022-02-24T00:00");
                long rangeHi = MicrosTimestampDriver.floor("2022-02-24T04:00");
                ww.commitWithParams(rangeLo, rangeHi, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();

            // (a) Query contents must agree
            assertQuery("select id, s from rg_trunc_small order by id")
                    .noLeakCheck()
                    .expectSize()
                    .returns("id\ts\n4\tCCC\n5\tAAA\n6\tDDD\n");
            assertQuery("select id, s from rg_trunc_large order by id")
                    .noLeakCheck()
                    .expectSize()
                    .returns("id\ts\n4\tCCC\n5\tAAA\n6\tDDD\n");

            // (b) Symbol maps must be identical.
            try (TableReader r1 = engine.getReader(tt1); TableReader r2 = engine.getReader(tt2)) {
                int colIdx1 = r1.getMetadata().getColumnIndex("s");
                int colIdx2 = r2.getMetadata().getColumnIndex("s");
                SymbolMapReader sm1 = r1.getSymbolMapReader(colIdx1);
                SymbolMapReader sm2 = r2.getSymbolMapReader(colIdx2);
                int count = sm1.getSymbolCount();
                Assert.assertEquals("symbol counts differ", count, sm2.getSymbolCount());
                for (int k = 0; k < count; k++) {
                    Assert.assertEquals(
                            "symbol key " + k + " differs",
                            String.valueOf(sm1.valueOf(k)),
                            String.valueOf(sm2.valueOf(k))
                    );
                }
            }
        });
    }

    @Test
    public void testStressReplaceLastMinuteRepeatedly() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 500_000);

            execute("create table stress (id long, ts timestamp, value long) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("stress");

            // Insert 100k rows spanning 1 day, so that partition is reasonably big to rewrite
            execute("insert into stress select x, cast('2022-02-24T00:00' as timestamp) + (x / 60) * 840000 * 60, x * 10 from long_sequence(100000)");
            drainWalQueue();

            // Verify initial state
            assertQuery("select count(*) from stress")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n100000\n");

            // Get the timestamp of the last minute (last row timestamp)
            long lastMinuteStart = MicrosTimestampDriver.floor("2022-02-24T23:59");
            long lastMinuteEnd = lastMinuteStart + 60 * 1_000_000; // 1 minute in microseconds

            // Perform 1,000 replace commits, each replacing the last minute with a new value
            long replaceValue = 1000L;
            long lastValue = 0;
            for (int i = 0; i < 1_000; i++) {
                try (WalWriter ww = engine.getWalWriter(tableToken)) {
                    // Add a new row with the current replace value
                    TableWriter.Row row = ww.newRow(lastMinuteStart + 30_000_000); // Middle of the last minute
                    row.putLong(0, replaceValue + i);
                    row.putLong(2, (replaceValue + i) * 100);
                    row.append();

                    // Commit with replace range for the last minute
                    ww.commitWithParams(lastMinuteStart, lastMinuteEnd, WAL_DEDUP_MODE_REPLACE_RANGE);
                    lastValue = (replaceValue + i) * 100;
                }
            }

            // Apply WAL
            drainWalQueue();

            // Verify the final state
            assertQuery("select count(*) from stress")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n100001\n");

            // Check the results of the last minute
            assertQuery("select count(*) from stress where ts >= '2022-02-24T23:59' and ts < '2022-02-25T00:00'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");

            // Verify the value is from the last replace operation
            assertQuery("select value from stress where ts >= '2022-02-24T23:59' and ts < '2022-02-25T00:00'")
                    .noLeakCheck()
                    .returns("value\n" + lastValue + "\n");
        });
    }

    @Test
    public void testStressReplaceLastMinuteRepeatedlyWithSkippableTransactions() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 500_000);

            execute("create table stress (id long, ts timestamp, value long) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("stress");

            // Insert 100k rows spanning 1 day, so that partition is reasonably big to rewrite
            execute("insert into stress select x, cast('2022-02-24T00:00' as timestamp) + (x / 60) * 840000 * 60, x * 10 from long_sequence(100000)");
            drainWalQueue();

            // Verify initial state
            assertQuery("select count(*) from stress")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n100000\n");

            // Base timestamp ranges - we'll vary these to create overlapping and non-overlapping replace ranges
            long baseHour = MicrosTimestampDriver.floor("2022-02-24T23:00");
            long minuteInMicros = 60 * 1_000_000L;

            // Perform 1000 replace commits with randomly varying replace ranges
            // Some transactions will replace the same time range (and can be skipped)
            // Others will replace different ranges (and cannot be skipped)
            long replaceValue = 1000L;
            java.util.Random rnd = new java.util.Random(42); // Fixed seed for reproducibility
            long[] lastValues = new long[3];

            for (int i = 0; i < 1_000; i++) {
                try (WalWriter ww = engine.getWalWriter(tableToken)) {
                    // Randomly choose one of 3 different time ranges to replace
                    // This creates overlaps where ~33% of transactions can be skipped
                    int rangeSelector = rnd.nextInt(3);
                    long rangeStart = baseHour + rangeSelector * 20 * minuteInMicros; // 23:00, 23:20, or 23:40
                    long rangeEnd = rangeStart + 20 * minuteInMicros; // 20 minute ranges

                    // Insert a row within the replace range
                    long insertTs = rangeStart + 10 * minuteInMicros; // Middle of the range
                    TableWriter.Row row = ww.newRow(insertTs);
                    row.putLong(0, replaceValue + i);
                    long currentValue = (replaceValue + i) * 100;
                    row.putLong(2, currentValue);
                    row.append();

                    // Commit with replace range
                    ww.commitWithParams(rangeStart, rangeEnd, WAL_DEDUP_MODE_REPLACE_RANGE);
                    lastValues[rangeSelector] = currentValue;
                }

                // Drain periodically to allow some batching and transaction skipping
                if (i % 10 == 9) {
                    drainWalQueue();
                }
            }

            // Apply any remaining WAL entries
            drainWalQueue();

            assertQuery("select value, ts from stress where ts >= '2022-02-24T23:00'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("value\tts\n" +
                            lastValues[0] + "\t2022-02-24T23:10:00.000000Z\n" +
                            lastValues[1] + "\t2022-02-24T23:30:00.000000Z\n" +
                            lastValues[2] + "\t2022-02-24T23:50:00.000000Z\n");
        });
    }

    @Test
    public void testStressReplaceWithInterleavedSkipsAndApplies() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 500_000);

            execute("create table stress (id long, ts timestamp, value long) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("stress");

            // Insert 100k rows spanning 1 day
            execute("insert into stress select x, cast('2022-02-24T00:00' as timestamp) + (x / 60) * 840000 * 60, x * 10 from long_sequence(100000)");
            drainWalQueue();

            // Verify initial state
            assertQuery("select count(*) from stress")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n100000\n");

            long baseHour = MicrosTimestampDriver.floor("2022-02-24T23:00");
            long minuteInMicros = 60 * 1_000_000L;

            // Create a pattern that forces: skip -> apply -> skip -> apply within a single batch
            // Pattern: A, A, B, C, C, D, A, C, E, F (where A-F are different time ranges)
            // Within this batch:
            // - Txns 0-1 (A,A) -> skipped (replaced by txn 6 which is the last A)
            // - Txn 2 (B) -> APPLIED (only B, not replaced by any later transaction)
            // - Txns 3-4 (C,C) -> skipped (replaced by txn 7 which is the last C)
            // - Txn 5 (D) -> APPLIED (only D, not replaced)
            // - Txn 6 (A) -> APPLIED (last A)
            // - Txn 7 (C) -> APPLIED (last C)
            // - Txn 8 (E) -> APPLIED (only E)
            // - Txn 9 (F) -> APPLIED (only F)
            // This creates: skip(0-1) -> APPLY(2) -> skip(3-4) -> APPLY(5) -> APPLY(6-9)
            // We use 6 different ranges (0-5) to ensure some are unique and must be applied
            int[] pattern = {0, 0, 1, 2, 2, 3, 0, 2, 4, 5}; // 0-5 represent 6 different time ranges
            long replaceValue = 1000L;
            long[] lastValues = new long[6];

            for (int batch = 0; batch < 10; batch++) {
                for (int rangeSelector : pattern) {
                    try (WalWriter ww = engine.getWalWriter(tableToken)) {
                        // Create 6 different 10-minute ranges to spread transactions across more ranges
                        long rangeStart = baseHour + rangeSelector * 10 * minuteInMicros; // 23:00, 23:10, 23:20, 23:30, 23:40, 23:50
                        long rangeEnd = rangeStart + 10 * minuteInMicros; // 10 minute ranges

                        // Insert a row within the replace range
                        long insertTs = rangeStart + 5 * minuteInMicros; // Middle of the range
                        TableWriter.Row row = ww.newRow(insertTs);
                        row.putLong(0, replaceValue);
                        row.putLong(2, replaceValue * 100);
                        row.append();

                        // Commit with replace range
                        ww.commitWithParams(rangeStart, rangeEnd, WAL_DEDUP_MODE_REPLACE_RANGE);
                        lastValues[rangeSelector] = replaceValue * 100;
                        replaceValue++;
                    }
                }
                // Drain after each complete pattern to allow skipping within the batch
                drainWalQueue();
            }

            // Verify we have exactly one row in each of the six 10-minute ranges
            assertQuery("select value, ts from stress where ts >= '2022-02-24T23:00'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("value\tts\n" +
                            lastValues[0] + "\t2022-02-24T23:05:00.000000Z\n" +
                            lastValues[1] + "\t2022-02-24T23:15:00.000000Z\n" +
                            lastValues[2] + "\t2022-02-24T23:25:00.000000Z\n" +
                            lastValues[3] + "\t2022-02-24T23:35:00.000000Z\n" +
                            lastValues[4] + "\t2022-02-24T23:45:00.000000Z\n" +
                            lastValues[5] + "\t2022-02-24T23:55:00.000000Z\n");
        });
    }

    // The complete final row set in ts order. Commit B and the oracle both write exactly this.
    private static void appendFinalDataset(WalWriter ww) throws NumericException {
        long id = 1;
        for (int day = 0; day < 5; day++) {
            for (long ts : existingDay(day)) {
                appendRow(ww, ts, id, id * 10);
                id++;
            }
        }
        long[] newTss = newPartitionRows();
        appendRow(ww, newTss[0], id++, 0); // 2026-01-06
        appendRow(ww, newTss[1], id++, 0); // 2026-01-06
        appendRow(ww, MicrosTimestampDriver.floor("2026-01-07T00:04:35.803821Z"), id++, 0);
        appendRow(ww, newTss[2], id++, 0); // 2026-01-08
        appendRow(ww, newTss[3], id++, 0); // 2026-01-09
        appendRow(ww, newTss[4], id++, 0); // 2026-01-10
        appendRow(ww, newTss[5], id, 0);   // 2026-01-10
    }

    private static void appendRow(WalWriter ww, long ts, long id, long v) {
        TableWriter.Row row = ww.newRow(ts);
        row.putLong(0, id);
        row.putLong(2, v);
        row.append();
    }

    private static void assertAlterIsStructural(String alterSql, boolean expectedStructural) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            AlterOperation alterOp = compiler.compile(alterSql, sqlExecutionContext).getAlterOperation();
            Assert.assertNotNull(alterSql + " did not compile to an ALTER operation", alterOp);
            if (expectedStructural) {
                Assert.assertTrue(alterSql + " must be structural", alterOp.isStructural());
            } else {
                Assert.assertFalse(
                        alterSql + " must be non-structural so it stays a hard SQL barrier on a materialized view",
                        alterOp.isStructural()
                );
            }
        }
    }

    private static void assertNotSuspended(TableToken tableToken) {
        Assert.assertFalse(
                "table is suspended: " + engine.getTableSequencerAPI().getTxnTracker(tableToken).getErrorMessage(),
                engine.getTableSequencerAPI().isSuspended(tableToken)
        );
    }

    private static void commitNoRowsWithRangeReplace(
            TableToken tableToken,
            String rangeStartStr,
            String rangeEndStr
    ) throws NumericException {
        try (WalWriter ww = engine.getWalWriter(tableToken)) {
            long rangeStart = MicrosTimestampDriver.floor(rangeStartStr);
            long rangeEnd = MicrosTimestampDriver.floor(rangeEndStr) + 1;
            ww.commitWithParams(rangeStart, rangeEnd, WAL_DEDUP_MODE_REPLACE_RANGE);
        }
    }

    // Existing rows for the 0-based day index. Days 0..3 hold five rows each; day 4 (2026-01-05)
    // holds six, the last at exactly 2026-01-05T00:03:46.111248Z (the commit-A range starts 1us
    // above it).
    private static long[] existingDay(int day) throws NumericException {
        long base = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
        if (day < 4) {
            long dayBase = base + day * 86_400_000_000L;
            return new long[]{dayBase, dayBase + 1_000_000L, dayBase + 2_000_000L, dayBase + 3_000_000L, dayBase + 4_000_000L};
        }
        long d05 = MicrosTimestampDriver.floor("2026-01-05T00:03:32.239828Z");
        return new long[]{
                d05,
                d05 + 3_000_000L,
                d05 + 4_000_000L,
                d05 + 6_000_000L,
                d05 + 9_000_000L,
                MicrosTimestampDriver.floor("2026-01-05T00:03:46.111248Z"),
        };
    }

    private static void insertRowsWithRangeReplace(
            TableToken tableToken,
            Utf8StringSink sink,
            String tsStr,
            String rangeStartStr,
            String rangeEndStr,
            boolean commitWithRangeReplace
    ) throws NumericException {
        try (WalWriter ww = engine.getWalWriter(tableToken)) {
            if (tsStr != null) {
                int i = 0;
                String[] sybmols = new String[]{"w", "d", "a", "b", "c"};
                for (String tsStrPart : tsStr.split(",")) {
                    long ts = MicrosTimestampDriver.floor(tsStrPart);
                    TableWriter.Row row = ww.newRow(ts);
                    row.putInt(0, 100);
                    row.putLong(2, 1000);
                    row.putStr(3, "hello");
                    sink.clear();
                    sink.put("w");
                    row.putVarchar(4, sink);
                    row.putSym(5, sybmols[i % sybmols.length]);
                    row.append();
                }
            }

            if (commitWithRangeReplace) {
                long rangeStart = MicrosTimestampDriver.floor(rangeStartStr);
                long rangeEnd = MicrosTimestampDriver.floor(rangeEndStr) + 1;
                ww.commitWithParams(rangeStart, rangeEnd, WAL_DEDUP_MODE_REPLACE_RANGE);
            } else {
                ww.commit();
            }
        }
    }

    // Commit-A rows: days 06, 08, 09, 10 (gap at 07), all above the 2026-01-05 max, in ts order.
    private static long[] newPartitionRows() throws NumericException {
        return new long[]{
                MicrosTimestampDriver.floor("2026-01-06T00:03:46.377496Z"),
                MicrosTimestampDriver.floor("2026-01-06T00:04:00.000000Z"),
                MicrosTimestampDriver.floor("2026-01-08T00:04:41.334009Z"),
                MicrosTimestampDriver.floor("2026-01-09T00:05:19.959452Z"),
                MicrosTimestampDriver.floor("2026-01-10T00:05:54.691962Z"),
                MicrosTimestampDriver.floor("2026-01-10T00:06:00.177392Z"),
        };
    }

    // A live view declared START FROM BEGINNING persists viewLowerBoundTimestamp = Numbers.LONG_NULL,
    // which is Long.MIN_VALUE, so its pure-delete full rebuild commits a REPLACE_RANGE over
    // [Long.MIN_VALUE, +inf) carrying no rows. That leaves the table empty, and an empty table
    // reports minTimestamp = Long.MAX_VALUE and maxTimestamp = Long.MIN_VALUE. processWalCommit's
    // post-replace bounds checks phrase "the table holds nothing in the replaced range" as
    // "ts < lo || ts >= hi", which no value can satisfy once the range spans the whole timeline -- so
    // the max check tripped, ApplyWal2TableJob suspended the table, and the view stayed empty for
    // good. Every other truncating case escapes it only because its low bound is a real timestamp
    // (see testReplaceTruncatesAllData), which is why this needs its own coverage.
    private void assertFullTimelineReplaceLeavesTableEmpty(boolean seedRows) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id long, ts timestamp, v long) timestamp(ts) partition by DAY WAL");
            TableToken rg = engine.verifyTableName("rg");

            if (seedRows) {
                execute("insert into rg select x, timestamp_sequence('2024-01-01T00:00:00.000000Z', 1_000_000), x * 10 from long_sequence(5)");
                drainWalQueue();
                assertQuery("select count(*) from rg")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("count\n5\n");
            }

            try (WalWriter ww = engine.getWalWriter(rg)) {
                ww.commitWithParams(Long.MIN_VALUE, Long.MAX_VALUE, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();

            Assert.assertFalse("table is suspended", engine.getTableSequencerAPI().isSuspended(rg));
            assertQuery("select count(*) from rg")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n0\n");
        });
    }

    // Creates WAL table tableName with the given designated timestamp type and partition unit, and
    // builds the partition at partitionFloor with a split but no parent: 60 rows from one step above
    // the floor, O3 rows that split the partition after its 55th row, then a replace-range commit
    // that removes the parent and leaves two rows in the split. Then converts that partition to
    // parquet and back to native with partitionClause and checks the data after each step.
    private void assertSplitWithoutParentConverts(
            String tableName,
            String timestampType,
            String partitionBy,
            long partitionFloor,
            long step,
            String partitionClause
    ) throws Exception {
        execute("CREATE TABLE " + tableName + " (id LONG, ts " + timestampType + ", v LONG) TIMESTAMP(ts) PARTITION BY " + partitionBy + " WAL");
        TableToken tableToken = engine.verifyTableName(tableName);
        final String sequence = "TIMESTAMP_NS".equals(timestampType) ? "timestamp_sequence_ns" : "timestamp_sequence";
        execute("INSERT INTO " + tableName + " SELECT x, " + sequence + "(" + (partitionFloor + step) + ", " + step + "), x FROM long_sequence(60)");
        drainWalQueue();
        execute("INSERT INTO " + tableName + " SELECT 100 + x, " + sequence + "(" + (partitionFloor + 55 * step + step / 2) + ", " + step + "), 0 FROM long_sequence(5)");
        drainWalQueue();
        assertQuery("SELECT count() FROM table_partitions('" + tableName + "')")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("""
                        count
                        2
                        """);

        // The replace range covers every row of the parent and of the split; both replacement rows
        // land in the split.
        try (WalWriter ww = engine.getWalWriter(tableToken)) {
            appendRow(ww, partitionFloor + 57 * step + step / 4, 201, 0);
            appendRow(ww, partitionFloor + 59 * step + step / 4, 202, 0);
            ww.commitWithParams(partitionFloor + step / 2, partitionFloor + 61 * step, WAL_DEDUP_MODE_REPLACE_RANGE);
        }
        drainWalQueue();
        assertNotSuspended(tableToken);
        assertQuery("SELECT count() FROM table_partitions('" + tableName + "')")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("""
                        count
                        1
                        """);
        execute("CREATE TABLE " + tableName + "_expected AS (SELECT * FROM " + tableName + ") TIMESTAMP(ts) PARTITION BY " + partitionBy + " WAL");
        drainWalQueue();

        execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET " + partitionClause);
        drainWalQueue();
        assertNotSuspended(tableToken);
        assertQuery("SELECT count() FROM table_partitions('" + tableName + "') WHERE isParquet")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("""
                        count
                        1
                        """);
        assertSqlCursors(tableName + "_expected", tableName);

        execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO NATIVE " + partitionClause);
        drainWalQueue();
        assertNotSuspended(tableToken);
        assertQuery("SELECT count() FROM table_partitions('" + tableName + "') WHERE isParquet")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("""
                        count
                        0
                        """);
        assertSqlCursors(tableName + "_expected", tableName);
    }

    // Creates WAL table rg whose day 2022-02-24 has a split but no parent partition. An O3 batch
    // splits the day into the parent '2022-02-24' (08:00 .. 17:00) and the split
    // '2022-02-24T170000-000001' (17:05 .. 17:50). A replace-range commit over [06:00, 23:00) then
    // removes the parent outright and leaves the split with two rows, at 18:00 and 18:30.
    // CONVERT PARTITION and DETACH PARTITION used to look the day up by its floor timestamp only, so
    // they missed it and ApplyWal2TableJob suspended the table.
    // WalWriterFuzzTest.testConvertPartitionToParquetWithCoveringIndex hit this in CI.
    private TableToken createDayWithSplitButNoParent() throws Exception {
        // Drop the absolute split threshold, so the prefix-to-tail ratio alone decides the split.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        // Merge-append turns the O3 batch into a composite partition instead of a split.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        execute("CREATE TABLE rg (id LONG, ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        TableToken tableToken = engine.verifyTableName("rg");

        // 60 rows, one every 10 minutes: 08:00 .. 17:50.
        execute("INSERT INTO rg SELECT x, timestamp_sequence('2022-02-24T08:00', 600_000_000), x * 10 FROM long_sequence(60)");
        drainWalQueue();
        // The O3 rows at 17:05 .. 17:45 split the day: the 55-row prefix (08:00 .. 17:00) stays in the
        // parent, and the merged tail moves to a split starting 1us after 17:00.
        execute("INSERT INTO rg SELECT 100 + x, timestamp_sequence('2022-02-24T17:05', 600_000_000), 0 FROM long_sequence(5)");
        drainWalQueue();
        assertQuery("SELECT name, numRows FROM table_partitions('rg')")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("""
                        name\tnumRows
                        2022-02-24\t55
                        2022-02-24T170000-000001\t10
                        """);

        // The replace range starts before the parent's first row and ends after the split's last row.
        // Both replacement rows land in the split.
        try (WalWriter ww = engine.getWalWriter(tableToken)) {
            appendRow(ww, MicrosTimestampDriver.floor("2022-02-24T18:00"), 201, 0);
            appendRow(ww, MicrosTimestampDriver.floor("2022-02-24T18:30"), 202, 0);
            ww.commitWithParams(
                    MicrosTimestampDriver.floor("2022-02-24T06:00"),
                    MicrosTimestampDriver.floor("2022-02-24T23:00"),
                    WAL_DEDUP_MODE_REPLACE_RANGE
            );
        }
        drainWalQueue();
        assertNotSuspended(tableToken);
        // The parent is gone: the split is the only partition of the day.
        assertQuery("SELECT name, numRows FROM table_partitions('rg')")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("""
                        name\tnumRows
                        2022-02-24T170000-000001\t2
                        """);
        return tableToken;
    }

    private void insertRowWithReplaceRange(
            String tsStr,
            String rangeStartStr,
            String rangeEndStr,
            TableToken tableToken,
            boolean compareTxns,
            boolean compareTruncateVersion,
            String tableName,
            String expectedTableName,
            boolean compareTxnDetails,
            boolean generateNoRowsCommit
    ) throws SqlException, NumericException {
        execute("create table " + expectedTableName + " as (select * from " + tableName + " where ts not between '" + rangeStartStr + "' and '" + rangeEndStr + "') timestamp(ts) partition by DAY WAL");

        Utf8StringSink sink = new Utf8StringSink();

        if (generateNoRowsCommit) {
            commitNoRowsWithRangeReplace(tableToken, rangeStartStr, rangeEndStr);
        }
        insertRowsWithRangeReplace(tableToken, sink, tsStr, rangeStartStr, rangeEndStr, true);
        drainWalQueue();

        var ttExpected = engine.verifyTableName(expectedTableName);
        insertRowsWithRangeReplace(ttExpected, sink, tsStr, rangeStartStr, rangeEndStr, false);
        drainWalQueue();

        Assert.assertFalse("table is suspended", engine.getTableSequencerAPI().isSuspended(tableToken));

        if (compareTxnDetails) {
            Assert.assertEquals(
                    readTxnToString(ttExpected, compareTxns, compareTruncateVersion),
                    readTxnToString(tableToken, compareTxns, compareTruncateVersion)
            );
        }

        assertSqlCursors(expectedTableName, tableName);
        assertSqlCursors("select count(*), min(ts), max(ts) from " + expectedTableName, "select count(*), min(ts), max(ts) from " + tableName);
    }

    private void runReplaceCase(String tsStr, String rangeStartStr, String rangeEndStr, boolean compareTxns, boolean compareTruncateVersion, String tableName, String expectedTableName, boolean compareTxnDetails, boolean generateNoRowsCommit) throws SqlException, NumericException {
        execute("create table " + tableName + " (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
        TableToken tableToken = engine.verifyTableName(tableName);

        execute("insert into " + tableName + " select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(100)");
        drainWalQueue();
        insertRowWithReplaceRange(tsStr, rangeStartStr, rangeEndStr, tableToken, compareTxns, compareTruncateVersion, tableName, expectedTableName, compareTxnDetails, generateNoRowsCommit);
    }

    private void testRemovesFirstPartitionNoRowsAdded(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");
            drainWalQueue();

            insertRowWithReplaceRange(null, "2022-02-19T17", "2022-02-25T01", tableToken, false, true, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    private void testRemovesLastPartitionNoRowsAdded(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(200)");
            drainWalQueue();

            insertRowWithReplaceRange(null, "2022-02-26", "2022-02-27", tableToken, false, true, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    private void testReplaceCommitAdds2PartitionsBeforeExisting(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-20T17,2022-02-21T17", "2022-02-20T17", "2022-02-21T18", tableToken, !generateNoRowsCommit, !generateNoRowsCommit, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    private void testReplaceCommitNotOrdered(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-21T17,2022-02-20T17", "2022-02-20T17", "2022-02-21T18", tableToken, !generateNoRowsCommit, !generateNoRowsCommit, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    private void testReplaceCommitRemoves2PartitionsAndAdds1(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-26T17", "2022-02-24T17", "2022-02-28T02", tableToken, !generateNoRowsCommit, !generateNoRowsCommit, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    private void testReplaceDeletesAppendsNothingToLastAndPartitionAndInsertsIntoAnother(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T00:31', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(20)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-25T02:36:07.769840Z", "2022-02-24T23:20:30", "2022-02-27T01:34:56.265527", tableToken, !generateNoRowsCommit, !generateNoRowsCommit, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    private void testReplaceRangeBeforeFirstPartitionAndData(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-20T17", "2022-02-19T17", "2022-02-21T18", tableToken, !generateNoRowsCommit, !generateNoRowsCommit, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    private void testReplaceRangeCommit(String tsStr, String rangeStartStr, String rangeEndStr, boolean compareTxns, boolean compareTruncateVersion, boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            runReplaceCase(tsStr, rangeStartStr, rangeEndStr, compareTxns, compareTruncateVersion, "rg", "expected", true, generateNoRowsCommit);

            // Run the same test with aggressive partition split
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);

            runReplaceCase(tsStr, rangeStartStr, rangeEndStr, false, false, "rg_split", "expected_split", false, generateNoRowsCommit);
        });
    }

    private void testReplaceRangeCommit(String tsStr, String rangeStartStr, String rangeEndStr) throws Exception {
        testReplaceRangeCommit(tsStr, rangeStartStr, rangeEndStr, true, true, false);
    }

    private void testReplaceRangeLastPartition(boolean compareTruncateVersion, boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T00:31', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(20)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-24T17", "2022-02-19T17", "2022-02-28T18", tableToken, false, compareTruncateVersion, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    private void testReplaceTruncatesAllData(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(200)");
            drainWalQueue();

            insertRowWithReplaceRange(null, "2022-02-20", "2022-02-27", tableToken, false, false, "rg", "expected", true, generateNoRowsCommit);
        });
    }

    private void testReplaceTruncatesAllDataAndAddsNewBeforeExisting(boolean generateNoRowsCommit) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");

            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(200)");
            drainWalQueue();

            insertRowWithReplaceRange("2022-02-21,2022-02-21T01", "2022-02-21", "2022-02-27", tableToken, false, false, "rg", "expected", true, generateNoRowsCommit);
        });
    }
}
