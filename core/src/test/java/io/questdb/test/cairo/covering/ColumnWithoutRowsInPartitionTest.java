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

package io.questdb.test.cairo.covering;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Files;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A column has rows in a partition only when the partition's column version gives it a top inside the partition. A
 * column added after the partition was written has no top there (-1). A column that ATTACH PARTITION or DROP PARTITION
 * of the newest partition recorded there has a top equal to the row count. Neither needs a file in the partition, and
 * every row reads as NULL.
 */
public class ColumnWithoutRowsInPartitionTest extends AbstractCairoTest {
    private static final String OLD_DAY = "2022-02-25";

    @Test
    public void testDropIndexOfColumnBackdatedByDropPartition() throws Exception {
        testDropIndexOfColumnBackdatedByDropPartition(false);
    }

    @Test
    public void testDropIndexOfColumnBackdatedByDropPartitionMergeAppend() throws Exception {
        // the partition DROP PARTITION leaves newest stays closed, so nothing creates the column's file there
        testDropIndexOfColumnBackdatedByDropPartition(true);
    }

    @Test
    public void testDropIndexOfColumnMissingInAttachedPartition() throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            createTableWithTwoDays();
            final TableToken token = engine.verifyTableName("t");
            execute("ALTER TABLE t DETACH PARTITION LIST '" + OLD_DAY + "'");
            execute("ALTER TABLE t ADD COLUMN s2 SYMBOL");
            execute("ALTER TABLE t ALTER COLUMN s2 ADD INDEX");
            drainWalQueue();
            attachOldDay(token);
            assertColumnHasNoFile(token, "s2", 2);

            execute("ALTER TABLE t ALTER COLUMN s2 DROP INDEX");
            drainWalQueue();

            assertNotSuspended();
            assertQuery("SELECT ts, sym, v, s2 FROM t WHERE s2 IS NULL")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv\ts2
                            2022-02-25T01:00:00.000000Z\ta\t1\t
                            2022-02-25T10:00:00.000000Z\tb\t2\t
                            2022-02-26T01:00:00.000000Z\ta\t3\t
                            """);
        });
    }

    @Test
    public void testFastLagAppendToPartitionThatDropPartitionBackdatedCoveredColumnTo() throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
        try {
            assertMemoryLeak(() -> {
                createTableWithTwoDays();
                final TableToken token = engine.verifyTableName("t");
                addCoveredColumn();
                execute("ALTER TABLE t DROP PARTITION LIST '2022-02-26'");
                drainWalQueue();
                Assert.assertEquals("precondition: DROP PARTITION must record the covered column at the row count", 2, columnTop(token, "var_top"));

                PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
                execute("INSERT INTO t VALUES ('2022-02-25T12:00:00.000000Z', 'b', 4, 'x')");
                drainWalQueue();
                Assert.assertTrue("precondition: the append must publish the index on the fast-lag path", PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get() > 0);

                assertNotSuspended();
                assertCoveredRows("""
                        ts\tvar_top
                        2022-02-25T10:00:00.000000Z\t
                        2022-02-25T12:00:00.000000Z\tx
                        """);
            });
        } finally {
            PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
        }
    }

    @Test
    public void testDropIndexFailsWhenLinkOfColumnWithOneRowFails() throws Exception {
        // s2 holds only the last row of the newest partition: one row is enough to need the file
        testDropIndexFailsWhenLinkFails("s2", false, true);
    }

    @Test
    public void testDropIndexFailsWhenLinkOfColumnWithRowsFails() throws Exception {
        // sym holds every row of the partition, so a link that finds no file must not pass for "nothing to link"
        testDropIndexFailsWhenLinkFails("sym", false, false);
    }

    @Test
    public void testDropIndexFailsWhenLinkOfExistingFileOfColumnWithoutRowsFails() throws Exception {
        // ADD COLUMN created the file of s2 in the newest partition, where s2 has no rows yet: it has to be linked
        testDropIndexFailsWhenLinkFails("s2", true, false);
    }

    @Test
    public void testIndexBuildFailsWhenAuxFileOfCoveredColumnWithRowsIsMissing() throws Exception {
        testIndexBuildFailsWhenFileOfCoveredColumnWithRowsIsMissing("VARCHAR", "v.i", "'p'", "'q'", "'r'", "q");
    }

    @Test
    public void testIndexBuildFailsWhenFileOfCoveredColumnWithRowsIsMissing() throws Exception {
        testIndexBuildFailsWhenFileOfCoveredColumnWithRowsIsMissing("LONG", "v.d", "1", "2", "3", "2");
    }

    @Test
    public void testMergeAppendIntoPartitionThatPredatesCoveredColumn() throws Exception {
        testAppendToPartitionThatPredatesCoveredColumn(true);
    }

    @Test
    public void testO3AppendToPartitionThatPredatesCoveredColumn() throws Exception {
        // the O3 seal must map the covered column whose only row in the partition is the last one
        testAppendToPartitionThatPredatesCoveredColumn(false);
    }

    @Test
    public void testReindexOfPartitionThatPredatesCoveredColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            insertTwoDays();
            execute("ALTER TABLE t ADD COLUMN var_top VARCHAR");
            execute("ALTER TABLE t ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (var_top)");
            execute("INSERT INTO t VALUES ('2022-02-26T02:00:00.000000Z', 'b', 4, 'x')");
            final TableToken token = engine.verifyTableName("t");
            Assert.assertEquals("precondition: the older partition must not have the covered column", -1, columnTop(token, "var_top"));
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            execute("REINDEX TABLE t LOCK EXCLUSIVE");
            engine.releaseAllReaders();

            assertCoveredRows("""
                    ts\tvar_top
                    2022-02-25T10:00:00.000000Z\t
                    2022-02-26T02:00:00.000000Z\tx
                    """);
        });
    }

    @Test
    public void testSealFailsWhenFileOfCoveredColumnWithRowsCannotBeOpened() throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        final AtomicBoolean isOpenFailing = new AtomicBoolean();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                return isOpenFailing.get() && isColumnFileOfOldDay(name, "v.d") ? -1 : super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            createTableWithTwoDays();
            final TableToken token = engine.verifyTableName("t");
            execute("ALTER TABLE t ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (v)");
            drainWalQueue();

            isOpenFailing.set(true);
            try {
                commitEmptyReplaceRange(token);
            } finally {
                isOpenFailing.set(false);
            }
            Assert.assertTrue(engine.getTableSequencerAPI().getTxnTracker(token).isSuspended());
            TestUtils.assertContains(
                    engine.getTableSequencerAPI().getTxnTracker(token).getErrorMessage(),
                    "could not open covering column file"
            );

            execute("ALTER TABLE t RESUME WAL");
            drainWalQueue();
            assertNotSuspended();
            assertCoveredRows(
                    "SELECT ts, v FROM t WHERE sym = 'b'",
                    "ts\tv\n2022-02-25T10:00:00.000000Z\t2\n"
            );
        });
    }

    @Test
    public void testSealOfAttachedPartitionMissingCoveredColumn() throws Exception {
        // merge-append publishes the covering index on its own path; production has it off
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            createTableWithTwoDays();
            final TableToken token = engine.verifyTableName("t");
            execute("ALTER TABLE t DETACH PARTITION LIST '" + OLD_DAY + "'");
            execute("ALTER TABLE t ADD COLUMN var_top VARCHAR");
            drainWalQueue();
            attachOldDay(token);
            execute("ALTER TABLE t ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (var_top)");
            drainWalQueue();
            assertColumnHasNoFile(token, "var_top", 2);
            assertNotSuspended();
            assertCoveredRows("ts\tvar_top\n2022-02-25T10:00:00.000000Z\t\n");

            commitEmptyReplaceRange(token);

            assertNotSuspended();
            assertCoveredRows("ts\tvar_top\n2022-02-25T10:00:00.000000Z\t\n");
        });
    }

    @Test
    public void testSealOfParquetPartitionThatPredatesCoveredColumn() throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            createTableWithTwoDays();
            final TableToken token = engine.verifyTableName("t");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '" + OLD_DAY + "'");
            addCoveredColumn();
            Assert.assertEquals("precondition: the parquet partition must not have the covered column", -1, columnTop(token, "var_top"));
            assertQuery("SELECT isParquet FROM table_partitions('t') WHERE name = '" + OLD_DAY + "'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("isParquet\ntrue\n");

            commitEmptyReplaceRange(token);
            assertNotSuspended();
            assertCoveredRows("ts\tvar_top\n2022-02-25T10:00:00.000000Z\t\n");

            // rewrites the parquet file, which seals the index from it
            execute("INSERT INTO t VALUES ('2022-02-25T05:00:00.000000Z', 'b', 9, 'x')");
            drainWalQueue();
            assertNotSuspended();
            assertCoveredRows("""
                    ts\tvar_top
                    2022-02-25T05:00:00.000000Z\tx
                    2022-02-25T10:00:00.000000Z\t
                    """);
        });
    }

    private static boolean isColumnFileOfOldDay(LPSZ path, String fileName) {
        return Utf8s.containsAscii(path, OLD_DAY) && Utf8s.endsWithAscii(path, Files.SEPARATOR + fileName);
    }

    private void addCoveredColumn() throws Exception {
        execute("ALTER TABLE t ADD COLUMN var_top VARCHAR");
        execute("ALTER TABLE t ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (var_top)");
        drainWalQueue();
    }

    private void assertColumnHasNoFile(TableToken token, String columnName, long rowCount) {
        final long partitionTimestamp = MicrosTimestampDriver.floor(OLD_DAY);
        try (TableReader reader = engine.getReader(token); Path path = new Path()) {
            final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(columnName));
            Assert.assertEquals(
                    "precondition: the column must be recorded at the row count",
                    rowCount,
                    reader.getColumnVersionReader().getColumnTop(partitionTimestamp, writerIndex)
            );
            path.of(configuration.getDbRoot()).concat(token);
            TableUtils.setPathForNativePartition(
                    path,
                    ColumnType.TIMESTAMP,
                    PartitionBy.DAY,
                    partitionTimestamp,
                    reader.getTxFile().getPartitionNameTxnByPartitionTimestamp(partitionTimestamp)
            );
            Assert.assertTrue("precondition: the partition directory must exist", Files.exists(path.$()));
            TableUtils.dFile(path, columnName, reader.getColumnVersionReader().getColumnNameTxn(partitionTimestamp, writerIndex));
            Assert.assertFalse("precondition: the partition must not have a file of the column", Files.exists(path.$()));
        }
    }

    private void assertCoveredRows(String expected) throws Exception {
        assertCoveredRows("SELECT ts, var_top FROM t WHERE sym = 'b'", expected);
    }

    // noRandomAccess() and expectSize() hold only for the covering cursor, so they also prove the index serves the query
    private void assertCoveredRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().expectSize().noRandomAccess().timestamp("ts").returns(expected);
        assertSqlCursors(sql.replace("SELECT ", "SELECT /*+ no_covering */ "), sql);
    }

    private void assertNotSuspended() throws Exception {
        assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("suspended\nfalse\n");
    }

    private void attachOldDay(TableToken token) throws Exception {
        try (Path detached = new Path(); Path attachable = new Path()) {
            detached.of(configuration.getDbRoot()).concat(token).concat(OLD_DAY).put(TableUtils.DETACHED_DIR_MARKER);
            attachable.of(configuration.getDbRoot()).concat(token).concat(OLD_DAY).put(configuration.getAttachPartitionSuffix());
            Assert.assertTrue(Files.rename(detached.$(), attachable.$()) > -1);
        }
        execute("ALTER TABLE t ATTACH PARTITION LIST '" + OLD_DAY + "'");
        drainWalQueue();
    }

    private long columnTop(TableToken token, String columnName) {
        try (TableReader reader = engine.getReader(token)) {
            final int writerIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(columnName));
            return reader.getColumnVersionReader().getColumnTop(MicrosTimestampDriver.floor(OLD_DAY), writerIndex);
        }
    }

    // Zero rows, and the range holds none of the partition's rows: nothing is written to the partition.
    private void commitEmptyReplaceRange(TableToken token) {
        try (WalWriter w = engine.getWalWriter(token)) {
            w.commitWithParams(
                    MicrosTimestampDriver.floor("2022-02-25T04:00:00.000000Z"),
                    MicrosTimestampDriver.floor("2022-02-25T05:00:00.000000Z"),
                    WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
            );
        }
        drainWalQueue();
    }

    private void createTableWithTwoDays() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
        insertTwoDays();
        drainWalQueue();
    }

    private void insertTwoDays() throws Exception {
        execute("""
                INSERT INTO t VALUES
                    ('2022-02-25T01:00:00.000000Z', 'a', 1),
                    ('2022-02-25T10:00:00.000000Z', 'b', 2),
                    ('2022-02-26T01:00:00.000000Z', 'a', 3)
                """);
    }

    private void testAppendToPartitionThatPredatesCoveredColumn(boolean isMergeAppend) throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, String.valueOf(isMergeAppend));
        assertMemoryLeak(() -> {
            createTableWithTwoDays();
            final TableToken token = engine.verifyTableName("t");
            addCoveredColumn();
            Assert.assertEquals("precondition: the older partition must not have the covered column", -1, columnTop(token, "var_top"));

            execute("INSERT INTO t VALUES ('2022-02-25T12:00:00.000000Z', 'b', 4, 'x')");
            drainWalQueue();

            assertNotSuspended();
            assertCoveredRows("""
                    ts\tvar_top
                    2022-02-25T10:00:00.000000Z\t
                    2022-02-25T12:00:00.000000Z\tx
                    """);
        });
    }

    private void testDropIndexFailsWhenLinkFails(String columnName, boolean isFilePresent, boolean isRowAdded) throws Exception {
        final AtomicBoolean isLinkFailing = new AtomicBoolean();
        final String dFileName = Files.SEPARATOR + columnName + ".d";
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                return !(isLinkFailing.get() && !isFilePresent && Utf8s.containsAscii(path, dFileName)) && super.exists(path);
            }

            @Override
            public int hardLink(LPSZ src, LPSZ hardLink) {
                return isLinkFailing.get() && Utf8s.containsAscii(src, dFileName) ? -1 : super.hardLink(src, hardLink);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            insertTwoDays();
            execute("ALTER TABLE t ADD COLUMN s2 SYMBOL");
            execute("ALTER TABLE t ALTER COLUMN " + columnName + " ADD INDEX");
            if (isRowAdded) {
                execute("INSERT INTO t VALUES ('2022-02-26T01:30:00.000000Z', 'b', 9, 'd')");
            }

            isLinkFailing.set(true);
            try {
                execute("ALTER TABLE t ALTER COLUMN " + columnName + " DROP INDEX");
                Assert.fail("DROP INDEX must not leave the column without its file under the new name");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "cannot hardLink");
            } finally {
                isLinkFailing.set(false);
            }

            execute("ALTER TABLE t ALTER COLUMN " + columnName + " DROP INDEX");
            execute("INSERT INTO t VALUES ('2022-02-26T02:00:00.000000Z', 'b', 4, 'c')");
            assertQuery("SELECT ts, sym, v, s2 FROM t WHERE sym = 'b'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\tsym\tv\ts2\n"
                            + "2022-02-25T10:00:00.000000Z\tb\t2\t\n"
                            + (isRowAdded ? "2022-02-26T01:30:00.000000Z\tb\t9\td\n" : "")
                            + "2022-02-26T02:00:00.000000Z\tb\t4\tc\n"
                    );
        });
    }

    private void testDropIndexOfColumnBackdatedByDropPartition(boolean isMergeAppend) throws Exception {
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, String.valueOf(isMergeAppend));
        assertMemoryLeak(() -> {
            createTableWithTwoDays();
            final TableToken token = engine.verifyTableName("t");
            execute("ALTER TABLE t ADD COLUMN s2 SYMBOL");
            execute("ALTER TABLE t ALTER COLUMN s2 ADD INDEX");
            execute("ALTER TABLE t DROP PARTITION LIST '2022-02-26'");
            drainWalQueue();
            Assert.assertEquals("precondition: DROP PARTITION must record the column at the row count", 2, columnTop(token, "s2"));

            execute("ALTER TABLE t ALTER COLUMN s2 DROP INDEX");
            // the first row of the column in the partition: it must land in the file readers find under the new name
            execute("INSERT INTO t VALUES ('2022-02-25T12:00:00.000000Z', 'b', 4, 'c')");
            drainWalQueue();

            assertNotSuspended();
            assertQuery("SELECT ts, sym, v, s2 FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tv\ts2
                            2022-02-25T01:00:00.000000Z\ta\t1\t
                            2022-02-25T10:00:00.000000Z\tb\t2\t
                            2022-02-25T12:00:00.000000Z\tb\t4\tc
                            """);
        });
    }

    private void testIndexBuildFailsWhenFileOfCoveredColumnWithRowsIsMissing(
            String columnType,
            String missingFile,
            String value1,
            String value2,
            String value3,
            String expectedValue
    ) throws Exception {
        final AtomicBoolean isFileMissing = new AtomicBoolean();
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                return !(isFileMissing.get() && isColumnFileOfOldDay(path, missingFile)) && super.exists(path);
            }

            @Override
            public long openRO(LPSZ name) {
                return isFileMissing.get() && isColumnFileOfOldDay(name, missingFile) ? -1 : super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v " + columnType + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES"
                    + " ('2022-02-25T01:00:00.000000Z', 'a', " + value1 + "),"
                    + " ('2022-02-25T10:00:00.000000Z', 'b', " + value2 + "),"
                    + " ('2022-02-26T01:00:00.000000Z', 'a', " + value3 + ")"
            );

            isFileMissing.set(true);
            try {
                execute("ALTER TABLE t ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (v)");
                Assert.fail("the index build must not publish NULL for values the covered column holds");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not open covered column");
            } finally {
                isFileMissing.set(false);
            }

            execute("ALTER TABLE t ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (v)");
            assertCoveredRows(
                    "SELECT ts, v FROM t WHERE sym = 'b'",
                    "ts\tv\n2022-02-25T10:00:00.000000Z\t" + expectedValue + "\n"
            );
        });
    }
}
