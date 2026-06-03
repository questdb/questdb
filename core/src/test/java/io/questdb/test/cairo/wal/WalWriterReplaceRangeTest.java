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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.PropertyKey.CAIRO_WAL_MAX_LAG_SIZE;
import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

public class WalWriterReplaceRangeTest extends AbstractCairoTest {

    @Test
    public void testManyTransactionsSkippedWhenTruncateIfFound() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(CAIRO_WAL_MAX_LAG_SIZE, 1);

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
            assertSql(
                    """
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
                            """,
                    "select * from x order by ts"
            );
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
            assertSql(
                    """
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
                            """,
                    "select * from x order by ts"
            );

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
            assertSql(
                    """
                            s\tv
                            kept\t42.0
                            """,
                    "select s, v from mv order by ts"
            );

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
            assertSql(
                    """
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
                            """,
                    "select s, v from mv order by ts"
            );

            // The index built by ADD INDEX and rebuilt by the REPLACE_RANGE resolves the replacement row.
            assertSql(
                    """
                            s\tv
                            new5\t1005.0
                            """,
                    "select s, v from mv where s = 'new5'"
            );
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
            assertSql(
                    "s\nw0\nw1\nw2\nw3\nw4\nw5\nw6\nw7\nw8\nw9\n",
                    "select s from x order by ts"
            );

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

            assertSql(
                    "a\tts\tc\n100\t2022-02-25T00:00:00.000000Z\t900\n",
                    "select * from x order by ts"
            );
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
            assertSql(
                    "s\nw0\nw1\nw2\nw3\nw4\nw5\nw6\nw7\nw8\nw9\n",
                    "select s from x order by ts"
            );

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
    public void testReplaceRangeNotSupportedParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rg (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("rg");
            execute("insert into rg select x, timestamp_sequence('2022-02-24T12:30', 15 * 60 * 1000 * 1000), x/2, cast(x as string), " +
                    "rnd_varchar(), rnd_symbol(null, 'a', 'b', 'c') from long_sequence(400)");

            execute("ALTER TABLE rg CONVERT PARTITION TO PARQUET LIST '2022-02-24'");
            drainWalQueue();

            insertRowsWithRangeReplace(tableToken, new Utf8StringSink(), "2022-02-24T17", "2022-02-14T17", "2022-02-25T18", true);
            drainWalQueue();

            Assert.assertTrue("table is suspended", engine.getTableSequencerAPI().isSuspended(tableToken));
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
            assertSql(
                    """
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
                            """,
                    "select s, v from mv order by ts"
            );

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
            assertSql(
                    """
                            s\tv
                            kept\t42.0
                            """,
                    "select s, v from mv order by ts"
            );

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
            assertSql(
                    """
                            s\tv
                            kept\t42.0
                            """,
                    "select s, v from mv order by ts"
            );

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
