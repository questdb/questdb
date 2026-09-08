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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.ColumnVersionWriter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.ScannedColumnTopProbe;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Every branch of {@link ScannedColumnTopProbe}, driven through real {@code _cv} and {@code _txn}
 * readers rather than through a query. The interval branches are otherwise reachable only by the
 * rows a query happens to return, which cannot separate "read no partition with a top" from "read
 * a partition and found no top".
 * <p>
 * Each case names the branch it is about, and asserts the answer directly. {@link #oracle}, an
 * independent per-partition walk written the obvious slow way, backs that up on the cases that
 * expect false -- it guards the direction that returns wrong rows. It does not run on a case that
 * expects true, where the direct assertion is already the stronger statement.
 */
public class ScannedColumnTopProbeTest extends AbstractCairoTest {

    private static final long DAY = 86_400_000_000L;
    private static final long JAN1 = 1_704_067_200_000_000L; // 2024-01-01T00:00:00Z
    private static final LongList NO_RECORDS = new LongList();
    private static final int PROBED_COLUMN = 1;

    // ---------- hasAnyColumnTop: table-level guards ----------

    @Test
    public void testEmptyTableHasNoTop() throws Exception {
        // partitionCount == 0: nothing to read, so nothing can lack values.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_empty (ts TIMESTAMP, val DOUBLE, sym SYMBOL)"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            releaseAll();
            try (TableReader reader = engine.getReader("t_empty")) {
                Assert.assertEquals(0, reader.getPartitionCount());
                assertProbe(reader, false, null);
                assertProbe(reader, false, intervals(JAN1, JAN1 + DAY));
            }
        });
    }

    @Test
    public void testEmptyColumnVersionFileHasNoTop() throws Exception {
        // _cv with no records at all, so the record loop never runs a single iteration. Real DDL
        // always leaves at least a default name-txn record behind, so this state is written
        // directly. The add time then reads as COL_TOP_DEFAULT_PARTITION, which comes after
        // nothing, so the second half finds nothing either.
        assertMemoryLeak(() -> {
            withRawFiles("t_raw_empty_cv", partitionsOf(JAN1, JAN1 + DAY), NO_RECORDS, (cv, tx) -> {
                Assert.assertEquals(0, cv.getCachedColumnVersionList().size());
                Assert.assertEquals(2, tx.getPartitionCount());
                Assert.assertEquals(
                        ColumnVersionReader.COL_TOP_DEFAULT_PARTITION,
                        cv.getColumnTopPartitionTimestamp(PROBED_COLUMN)
                );
                Assert.assertFalse(ScannedColumnTopProbe.hasAnyColumnTop(cv, tx, PROBED_COLUMN, null));
                Assert.assertFalse(ScannedColumnTopProbe.hasAnyColumnTop(
                        cv, tx, PROBED_COLUMN, intervals(JAN1, JAN1 + 2 * DAY)));
            });
        });
    }

    @Test
    public void testEmptyPartitionListHasNoTop() throws Exception {
        // _txn with no partitions while _cv still holds a real top. Nothing is read, so nothing
        // can lack values, and the record half must not index into an empty partition list.
        assertMemoryLeak(() -> {
            withRawFiles("t_raw_empty_txn", new LongList(), tops(JAN1, 5), (cv, tx) -> {
                Assert.assertEquals(0, tx.getPartitionCount());
                Assert.assertTrue(cv.getCachedColumnVersionList().size() > 0);
                Assert.assertFalse(ScannedColumnTopProbe.hasAnyColumnTop(cv, tx, PROBED_COLUMN, null));
                Assert.assertFalse(ScannedColumnTopProbe.hasAnyColumnTop(
                        cv, tx, PROBED_COLUMN, intervals(JAN1, JAN1 + 2 * DAY)));
                // The record names a partition _txn does not list.
                Assert.assertFalse(ScannedColumnTopProbe.isPartitionScanned(tx, 0, JAN1, null));
            });
        });
    }

    @Test
    public void testBothFilesEmptyHasNoTop() throws Exception {
        // Neither file says anything. Both halves have to fall through without touching an index.
        assertMemoryLeak(() -> {
            withRawFiles("t_raw_empty_both", new LongList(), NO_RECORDS, (cv, tx) -> {
                Assert.assertEquals(0, tx.getPartitionCount());
                Assert.assertEquals(0, cv.getCachedColumnVersionList().size());
                Assert.assertFalse(ScannedColumnTopProbe.hasAnyColumnTop(cv, tx, PROBED_COLUMN, null));
                Assert.assertFalse(ScannedColumnTopProbe.hasAnyColumnTop(cv, tx, PROBED_COLUMN, new LongList()));
                Assert.assertFalse(ScannedColumnTopProbe.hasAnyColumnTop(
                        cv, tx, PROBED_COLUMN, intervals(JAN1, JAN1 + DAY)));
            });
        });
    }

    @Test
    public void testColumnVersionRecordsButNoneForThisColumnHasNoTop() throws Exception {
        // _cv is populated, and every record belongs to a neighbouring column. The column-index
        // guard has to reject all of them, and the add time reads as the sentinel.
        assertMemoryLeak(() -> {
            final LongList neighbourTops = new LongList();
            neighbourTops.add(JAN1);
            neighbourTops.add(7);
            withRawFilesForColumn("t_raw_other_only", partitionsOf(JAN1, JAN1 + DAY),
                    neighbourTops, PROBED_COLUMN + 1, (cv, tx) -> {
                        Assert.assertTrue(cv.getCachedColumnVersionList().size() > 0);
                        Assert.assertFalse(ScannedColumnTopProbe.hasAnyColumnTop(cv, tx, PROBED_COLUMN, null));
                    });
        });
    }

    // ---------- hasScannedTopRecord: which _cv rows are considered ----------

    @Test
    public void testRecordOfAnotherColumnIsIgnored() throws Exception {
        // The column-index guard. "other" takes a top on the first partition; sym has none
        // anywhere, so reading that same partition must still answer false.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_other (ts TIMESTAMP, val DOUBLE,"
                    + " sym SYMBOL INDEX TYPE POSTING INCLUDE (val))"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t_other VALUES ('2024-01-01T00:00:00', 10.0, 'A')");
            execute("ALTER TABLE t_other ADD COLUMN other SYMBOL");
            execute("INSERT INTO t_other VALUES ('2024-01-01T01:00:00', 20.0, 'A', 'X')");
            releaseAll();
            try (TableReader reader = engine.getReader("t_other")) {
                Assert.assertTrue(
                        "the fixture must give `other` a non-zero top, or nothing is being ignored",
                        hasNonZeroTopRecord(reader, writerIndexOf(reader, "other"))
                );
                assertProbe(reader, false, null);
            }
        });
    }

    @Test
    public void testDefaultPartitionRecordIsNotReadAsATop() throws Exception {
        // COL_TOP_DEFAULT_PARTITION keeps the column's ADD TIME in the column-top slot. Reading it
        // as a top would answer true for every column added after the table was created. Here the
        // scan reads only the last partition, which has the column in full, so the answer is false
        // -- and it is false only if that pseudo-record was skipped.
        assertMemoryLeak(() -> {
            createAddedLaterTable("t_default_rec");
            try (TableReader reader = engine.getReader("t_default_rec")) {
                final int wi = writerIndexOf(reader, "sym");
                final long addedAt = reader.getColumnVersionReader().getColumnTopPartitionTimestamp(wi);
                Assert.assertTrue("the add time must be a real timestamp, not the sentinel", addedAt > 0);
                assertProbe(reader, false, intervals(JAN1 + 2 * DAY, JAN1 + 3 * DAY - 1));
            }
        });
    }

    @Test
    public void testZeroTopRecordIsNotATop() throws Exception {
        // A record with top == 0 says the column is there in full. The O3 back-fill below leaves
        // exactly that on the first two partitions.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_zero (ts TIMESTAMP, val DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t_zero VALUES
                    ('2024-01-01T12:00:00', 10.0),
                    ('2024-01-02T12:00:00', 20.0)
                    """);
            execute("ALTER TABLE t_zero ADD COLUMN sym SYMBOL");
            execute("""
                    INSERT INTO t_zero VALUES
                    ('2024-01-01T06:00:00', 11.0, 'A'),
                    ('2024-01-02T06:00:00', 21.0, 'A')
                    """);
            execute("ALTER TABLE t_zero ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (val)");
            releaseAll();
            try (TableReader reader = engine.getReader("t_zero")) {
                final int wi = writerIndexOf(reader, "sym");
                Assert.assertFalse(
                        "the fixture must leave sym only zero-top records, or this tests nothing",
                        hasNonZeroTopRecord(reader, wi)
                );
                // The record half answers false on its own.
                Assert.assertFalse(ScannedColumnTopProbe.hasScannedTopRecord(
                        reader.getColumnVersionReader(), reader.getTxFile(), reader.getPartitionCount(), wi, null));
            }
        });
    }

    @Test
    public void testTopRecordOutsideTheScanIsNotReported() throws Exception {
        // The record half finds sym's top on the first partition, but the scan reads only the
        // last one. Same table, two interval lists, opposite answers.
        assertMemoryLeak(() -> {
            createAddedLaterTable("t_outside");
            try (TableReader reader = engine.getReader("t_outside")) {
                assertProbe(reader, true, intervals(JAN1, JAN1 + DAY - 1));
                assertProbe(reader, false, intervals(JAN1 + 2 * DAY, JAN1 + 3 * DAY - 1));
            }
        });
    }

    @Test
    public void testBackFilledPartitionsBeforeTheAddAreNotATop() throws Exception {
        // The shape that makes the "before the column" half read records rather than trust the add
        // time alone:
        //
        //   addedAt = 2024-01-03
        //   _txn:  01-01  01-02  01-03  01-04
        //   _cv:   01-01 top=0   01-02 top=0   01-03 top=0
        //
        // 01-01 and 01-02 start before the add, so the two bounding facts both hold and the walk
        // runs. Each owns a zero-top record though -- an out-of-order write put the column in
        // afterwards -- so the column is there in full and nothing in range lacks values.
        assertMemoryLeak(() -> {
            final LongList partitions = partitionsOf(JAN1, JAN1 + DAY, JAN1 + 2 * DAY, JAN1 + 3 * DAY);
            final LongList zeroTops = partitionsOf(JAN1, 0, JAN1 + DAY, 0, JAN1 + 2 * DAY, 0);
            withRawFilesForColumn("t_raw_backfill", partitions, zeroTops, PROBED_COLUMN, JAN1 + 2 * DAY,
                    (cv, tx) -> {
                        Assert.assertEquals(JAN1 + 2 * DAY, cv.getColumnTopPartitionTimestamp(PROBED_COLUMN));
                        // No record states a top, so the record half finds nothing.
                        Assert.assertFalse(ScannedColumnTopProbe.hasScannedTopRecord(
                                cv, tx, tx.getPartitionCount(), PROBED_COLUMN, null));
                        // Every record says the column is present, so neither half reports a top.
                        Assert.assertFalse(ScannedColumnTopProbe.hasPartitionBeforeColumn(cv, tx, PROBED_COLUMN, null));
                        Assert.assertFalse(ScannedColumnTopProbe.hasAnyColumnTop(cv, tx, PROBED_COLUMN, null));
                        Assert.assertFalse(ScannedColumnTopProbe.hasAnyColumnTop(
                                cv, tx, PROBED_COLUMN, intervals(JAN1, JAN1 + 4 * DAY)));
                        // A scan opening at or after the add answers the same way.
                        Assert.assertFalse(ScannedColumnTopProbe.hasAnyColumnTop(
                                cv, tx, PROBED_COLUMN, intervals(JAN1 + 2 * DAY, JAN1 + 4 * DAY)));
                    });
        });
    }

    // ---------- isPartitionScanned: the overlap test ----------

    @Test
    public void testWholeTableScanReadsEveryPartition() throws Exception {
        // A null interval list means no filter at all.
        assertMemoryLeak(() -> {
            createAddedLaterTable("t_whole");
            try (TableReader reader = engine.getReader("t_whole")) {
                assertProbe(reader, true, null);
            }
        });
    }

    @Test
    public void testEmptyIntervalListReadsNoPartition() throws Exception {
        // An empty list admits nothing, so no partition is read and nothing can lack values --
        // even though this table does carry a real top. Both halves have to honour that: the
        // overlap test rejects every partition, so the record half finds nothing and the walk over
        // the partitions before the add finds nothing either.
        assertMemoryLeak(() -> {
            createAddedLaterTable("t_empty_iv");
            try (TableReader reader = engine.getReader("t_empty_iv")) {
                Assert.assertTrue("the fixture must carry a top, or this asserts nothing",
                        hasNonZeroTopRecord(reader, writerIndexOf(reader, "sym")));
                Assert.assertFalse(ScannedColumnTopProbe.hasAnyColumnTop(
                        reader.getColumnVersionReader(), reader.getTxFile(),
                        writerIndexOf(reader, "sym"), new LongList()));
            }
        });
    }

    @Test
    public void testIntervalStartingExactlyOnThePartitionIsRead() throws Exception {
        // The binary search lands on a boundary. A closed interval includes both ends.
        assertMemoryLeak(() -> {
            createAddedLaterTable("t_bound_lo");
            try (TableReader reader = engine.getReader("t_bound_lo")) {
                assertProbe(reader, true, intervals(JAN1, JAN1 + 3600 * 1_000_000L));
            }
        });
    }

    @Test
    public void testIntervalEndingExactlyOnThePartitionIsRead() throws Exception {
        // The other boundary: the interval closes on the partition's first microsecond.
        assertMemoryLeak(() -> {
            createAddedLaterTable("t_bound_hi");
            try (TableReader reader = engine.getReader("t_bound_hi")) {
                assertProbe(reader, true, intervals(JAN1 - DAY, JAN1));
            }
        });
    }

    @Test
    public void testPartitionStartInsideAnIntervalIsRead() throws Exception {
        // Odd insertion point: the partition's start sits strictly between a lo and its hi.
        assertMemoryLeak(() -> {
            createAddedLaterTable("t_inside");
            try (TableReader reader = engine.getReader("t_inside")) {
                assertProbe(reader, true, intervals(JAN1 - DAY, JAN1 + 3600 * 1_000_000L));
            }
        });
    }

    @Test
    public void testIntervalOpeningInsideThePartitionIsRead() throws Exception {
        // Even insertion point, and the next interval opens after the partition starts but before
        // it ends. This is the case a plain "is the partition start inside an interval" test gets
        // wrong, and getting it wrong drops a partition the scan really reads.
        assertMemoryLeak(() -> {
            createAddedLaterTable("t_opens_inside");
            try (TableReader reader = engine.getReader("t_opens_inside")) {
                assertProbe(reader, true, intervals(JAN1 + 6 * 3600 * 1_000_000L, JAN1 + 7 * 3600 * 1_000_000L));
            }
        });
    }

    @Test
    public void testIntervalsAllBeforeThePartitionAreNotRead() throws Exception {
        // Even insertion point past the end of the list: every interval closed before the
        // partition with the top begins.
        assertMemoryLeak(() -> {
            createAddedLaterTable("t_all_before");
            try (TableReader reader = engine.getReader("t_all_before")) {
                Assert.assertFalse(ScannedColumnTopProbe.hasScannedTopRecord(
                        reader.getColumnVersionReader(), reader.getTxFile(), reader.getPartitionCount(),
                        writerIndexOf(reader, "sym"), intervals(JAN1 - 2 * DAY, JAN1 - DAY)));
            }
        });
    }

    @Test
    public void testIntervalOpeningAfterThePartitionEndsIsNotRead() throws Exception {
        // Even insertion point with an interval still to come, but it opens in a later partition:
        // the only partition carrying a top is 2024-01-02, and this interval opens on 2024-01-03.
        assertMemoryLeak(() -> {
            createAddedLaterTable("t_opens_after");
            try (TableReader reader = engine.getReader("t_opens_after")) {
                Assert.assertFalse(ScannedColumnTopProbe.hasScannedTopRecord(
                        reader.getColumnVersionReader(), reader.getTxFile(), reader.getPartitionCount(),
                        writerIndexOf(reader, "sym"), intervals(JAN1 + 2 * DAY, JAN1 + 2 * DAY + 1)));
            }
        });
    }

    @Test
    public void testRecordForAPartitionThatIsGoneIsNotRead() throws Exception {
        // A _cv record can name a timestamp _txn no longer lists. Asking about one directly must
        // answer "not scanned" rather than index into the partition list.
        assertMemoryLeak(() -> {
            createAddedLaterTable("t_gone");
            try (TableReader reader = engine.getReader("t_gone")) {
                Assert.assertFalse(ScannedColumnTopProbe.isPartitionScanned(
                        reader.getTxFile(), reader.getPartitionCount(), JAN1 - 50 * DAY, null));
                Assert.assertFalse(ScannedColumnTopProbe.isPartitionScanned(
                        reader.getTxFile(), reader.getPartitionCount(), JAN1 + 50 * DAY, null));
                // Not a partition start, though it falls inside one.
                Assert.assertFalse(ScannedColumnTopProbe.isPartitionScanned(
                        reader.getTxFile(), reader.getPartitionCount(), JAN1 + 3600 * 1_000_000L, null));
            }
        });
    }

    // ---------- hasPartitionBeforeColumn: the two-fact test ----------

    @Test
    public void testColumnPresentSinceCreationHasNothingBefore() throws Exception {
        // Add time is COL_TOP_DEFAULT_PARTITION, which comes after nothing, so the first fact
        // fails and no partition can predate the column.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_since (ts TIMESTAMP, val DOUBLE,"
                    + " sym SYMBOL INDEX TYPE POSTING INCLUDE (val))"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t_since VALUES
                    ('2024-01-01T00:00:00', 10.0, 'A'),
                    ('2024-01-02T00:00:00', 20.0, NULL)
                    """);
            releaseAll();
            try (TableReader reader = engine.getReader("t_since")) {
                final int wi = writerIndexOf(reader, "sym");
                Assert.assertEquals(
                        ColumnVersionReader.COL_TOP_DEFAULT_PARTITION,
                        reader.getColumnVersionReader().getColumnTopPartitionTimestamp(wi)
                );
                Assert.assertFalse(before(reader, null));
                Assert.assertFalse(before(reader, intervals(JAN1 - DAY, JAN1 + 9 * DAY)));
            }
        });
    }

    @Test
    public void testScanStartingAtOrAfterTheAddHasNothingBefore() throws Exception {
        // First fact fails: the scan opens at or after the column's add time, so every partition
        // it reads already had the column.
        assertMemoryLeak(() -> {
            createAddedLaterTable("t_after_add");
            try (TableReader reader = engine.getReader("t_after_add")) {
                final long addedAt = reader.getColumnVersionReader()
                        .getColumnTopPartitionTimestamp(writerIndexOf(reader, "sym"));
                Assert.assertFalse(before(reader, intervals(addedAt, addedAt + DAY)));
                Assert.assertFalse(before(reader, intervals(addedAt + 1, addedAt + DAY)));
                // One microsecond earlier and the fact holds again.
                Assert.assertTrue(before(reader, intervals(addedAt - 1, addedAt + DAY)));
            }
        });
    }

    @Test
    public void testNoPartitionStartsBeforeTheAdd() throws Exception {
        // Second fact fails: the column was added on the table's very first partition, so nothing
        // starts before it even though the scan opens earlier.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_add_first (ts TIMESTAMP, val DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t_add_first VALUES ('2024-01-01T00:00:00', 10.0)");
            execute("ALTER TABLE t_add_first ADD COLUMN sym SYMBOL");
            execute("ALTER TABLE t_add_first ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (val)");
            releaseAll();
            try (TableReader reader = engine.getReader("t_add_first")) {
                final int wi = writerIndexOf(reader, "sym");
                Assert.assertEquals(JAN1, reader.getColumnVersionReader().getColumnTopPartitionTimestamp(wi));
                Assert.assertEquals(JAN1, reader.getTxFile().getPartitionTimestampByIndex(0));
                Assert.assertFalse(before(reader, intervals(JAN1 - 10 * DAY, JAN1 + DAY)));
            }
        });
    }

    @Test
    public void testBothFactsHoldReportsAPartitionBefore() throws Exception {
        // Both facts hold: the scan opens before the add and a partition starts before it.
        assertMemoryLeak(() -> {
            createAddedLaterTable("t_both");
            try (TableReader reader = engine.getReader("t_both")) {
                Assert.assertTrue(before(reader, null));
                Assert.assertTrue(before(reader, intervals(JAN1, JAN1 + DAY)));
            }
        });
    }

    // ---------- oracle cross-check ----------

    @Test
    public void testMatchesOracleAcrossManyIntervalShapes() throws Exception {
        // The probe against an independent per-partition walk, over every interval shape the
        // branch cases above build one at a time. The probe may over-report; it may never
        // under-report, which is the direction that returns wrong rows.
        assertMemoryLeak(() -> {
            createAddedLaterTable("t_oracle");
            try (TableReader reader = engine.getReader("t_oracle")) {
                final int wi = writerIndexOf(reader, "sym");
                final long hour = 3600 * 1_000_000L;
                final LongList[] shapes = {
                        null,
                        intervals(JAN1, JAN1 + DAY - 1),
                        intervals(JAN1 + DAY, JAN1 + 2 * DAY - 1),
                        intervals(JAN1 + 2 * DAY, JAN1 + 3 * DAY - 1),
                        intervals(JAN1 - DAY, JAN1),
                        intervals(JAN1 + 6 * hour, JAN1 + 7 * hour),
                        intervals(JAN1 - 3 * DAY, JAN1 - 2 * DAY),
                        intervals(JAN1 + 20 * DAY, JAN1 + 21 * DAY),
                        intervals(JAN1, JAN1 + hour, JAN1 + 2 * DAY, JAN1 + 2 * DAY + hour),
                        intervals(JAN1 + DAY, JAN1 + DAY + hour, JAN1 + 2 * DAY, JAN1 + 2 * DAY + hour),
                };
                for (LongList shape : shapes) {
                    final boolean probed = ScannedColumnTopProbe.hasAnyColumnTop(
                            reader.getColumnVersionReader(), reader.getTxFile(), wi, shape);
                    if (oracle(reader, wi, shape)) {
                        Assert.assertTrue("under-reported for " + shape, probed);
                    }
                }
            }
        });
    }

    // ---------- helpers ----------

    private static void assertProbe(TableReader reader, boolean expected, LongList intervals) {
        final int wi = writerIndexOf(reader, "sym");
        Assert.assertEquals(
                "probe disagrees for " + intervals,
                expected,
                ScannedColumnTopProbe.hasAnyColumnTop(reader.getColumnVersionReader(), reader.getTxFile(), wi, intervals)
        );
        if (expected || !oracle(reader, wi, intervals)) {
            return;
        }
        Assert.fail("probe under-reported for " + intervals);
    }

    private static boolean before(TableReader reader, LongList intervals) {
        return ScannedColumnTopProbe.hasPartitionBeforeColumn(
                reader.getColumnVersionReader(), reader.getTxFile(), writerIndexOf(reader, "sym"), intervals);
    }

    /**
     * Three daily partitions where {@code sym} is added on the second, so the first lacks it
     * entirely and the second carries a real top.
     */
    private static void createAddedLaterTable(String name) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, val DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("""
                INSERT INTO %s VALUES
                ('2024-01-01T00:00:00', 10.0),
                ('2024-01-02T00:00:00', 20.0)
                """.formatted(name));
        execute("ALTER TABLE " + name + " ADD COLUMN sym SYMBOL");
        execute("""
                INSERT INTO %s VALUES
                ('2024-01-02T12:00:00', 21.0, 'A'),
                ('2024-01-03T00:00:00', 30.0, 'B')
                """.formatted(name));
        execute("ALTER TABLE " + name + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (val)");
        releaseAll();
    }

    private static boolean hasNonZeroTopRecord(TableReader reader, int writerIndex) {
        final LongList records = reader.getColumnVersionReader().getCachedColumnVersionList();
        for (int i = 0, n = records.size(); i < n; i += ColumnVersionReader.BLOCK_SIZE) {
            if (records.getQuick(i + ColumnVersionReader.COLUMN_INDEX_OFFSET) == writerIndex
                    && records.getQuick(i) != ColumnVersionReader.COL_TOP_DEFAULT_PARTITION
                    && records.getQuick(i) != ColumnVersionReader.SYMBOL_TABLE_VERSION_PARTITION
                    && records.getQuick(i + ColumnVersionReader.COLUMN_TOP_OFFSET) > 0) {
                return true;
            }
        }
        return false;
    }

    private static LongList intervals(long... bounds) {
        final LongList list = new LongList();
        for (long b : bounds) {
            list.add(b);
        }
        return list;
    }

    /**
     * The obvious slow way: visit every partition, ask whether any interval overlaps it, and
     * decide it from its own record or from the column's add time.
     */
    private static boolean oracle(TableReader reader, int writerIndex, LongList intervals) {
        final ColumnVersionReader cv = reader.getColumnVersionReader();
        final TxReader tx = reader.getTxFile();
        final long addedAt = cv.getColumnTopPartitionTimestamp(writerIndex);
        for (int p = 0, n = tx.getPartitionCount(); p < n; p++) {
            final long lo = tx.getPartitionTimestampByIndex(p);
            final long hi = p + 1 < n
                    ? Math.min(tx.getPartitionTimestampByIndex(p + 1), tx.getNextLogicalPartitionTimestamp(lo)) - 1
                    : tx.getNextLogicalPartitionTimestamp(lo);
            if (!overlaps(intervals, lo, hi)) {
                continue;
            }
            final int record = cv.getRecordIndex(lo, writerIndex);
            if (record > -1) {
                if (cv.getColumnTopByIndex(record) > 0) {
                    return true;
                }
            } else if (addedAt > lo && tx.getPartitionSize(p) > 0) {
                return true;
            }
        }
        return false;
    }

    private static boolean overlaps(LongList intervals, long lo, long hi) {
        if (intervals == null) {
            return true;
        }
        for (int i = 0, n = intervals.size() / 2; i < n; i++) {
            if (intervals.getQuick(2 * i) <= hi && intervals.getQuick(2 * i + 1) >= lo) {
                return true;
            }
        }
        return false;
    }

    @FunctionalInterface
    private interface RawFileCheck {
        void run(ColumnVersionReader cv, TxReader tx);
    }

    private static LongList partitionsOf(long... timestamps) {
        return intervals(timestamps);
    }

    private static LongList tops(long partitionTimestamp, long columnTop) {
        final LongList list = new LongList();
        list.add(partitionTimestamp);
        list.add(columnTop);
        return list;
    }

    /**
     * Writes {@code _txn} and {@code _cv} by hand so the check sees exactly the state named, down
     * to an empty file. Going through DDL cannot reach these: it always leaves default records
     * behind, and a table always has a partition once it has a row.
     */
    private static void withRawFiles(String tableName, LongList partitions, LongList tops, RawFileCheck check) {
        withRawFilesForColumn(tableName, partitions, tops, PROBED_COLUMN, Long.MIN_VALUE, check);
    }

    private static void withRawFilesForColumn(
            String tableName,
            LongList partitions,
            LongList tops,
            int columnIndex,
            RawFileCheck check
    ) {
        withRawFilesForColumn(tableName, partitions, tops, columnIndex, Long.MIN_VALUE, check);
    }

    private static void withRawFilesForColumn(
            String tableName,
            LongList partitions,
            LongList tops,
            int columnIndex,
            long addedAt,
            RawFileCheck check
    ) {
        final TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
        model.timestamp();
        AbstractCairoTest.create(model);
        final TableToken tableToken = engine.verifyTableName(tableName);
        try (Path path = new Path()) {
            try (TxWriter tw = new TxWriter(configuration.getFilesFacade(), configuration).ofRW(
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME).$(),
                    ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY)) {
                for (int i = 0, n = partitions.size(); i < n; i++) {
                    tw.updatePartitionSizeByTimestamp(partitions.getQuick(i), 1 + i);
                }
                if (partitions.size() > 0) {
                    tw.updateMaxTimestamp(partitions.getQuick(partitions.size() - 1) + 1);
                }
                tw.finishPartitionSizeUpdate();
                tw.commit(new ObjList<>());
            }
            try (ColumnVersionWriter w = new ColumnVersionWriter(
                    configuration, path.of(configuration.getDbRoot()).concat(tableToken).concat("_cv").$(), true)) {
                if (addedAt != Long.MIN_VALUE) {
                    w.upsertDefaultTxnName(columnIndex, 1, addedAt);
                }
                for (int i = 0, n = tops.size() / 2; i < n; i++) {
                    w.upsert(tops.getQuick(2 * i), columnIndex, 1, tops.getQuick(2 * i + 1));
                }
                w.commit();
            }
            try (
                    TxReader tx = new TxReader(configuration.getFilesFacade());
                    ColumnVersionReader cv = new ColumnVersionReader().ofRO(
                            configuration.getFilesFacade(),
                            path.of(configuration.getDbRoot()).concat(tableToken).concat("_cv").$())
            ) {
                tx.ofRO(path.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME).$(),
                        ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
                tx.unsafeLoadAll();
                cv.readUnsafe();
                check.run(cv, tx);
            }
        }
    }

    private static void releaseAll() {
        engine.releaseAllWriters();
        engine.releaseAllReaders();
    }

    private static int writerIndexOf(TableReader reader, String columnName) {
        return reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(columnName));
    }
}
