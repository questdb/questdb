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

package io.questdb.test.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.LongHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.TreeSet;

import static io.questdb.test.fuzz.FuzzDetachAttachPartitionOperation.*;

public class FuzzDetachAttachPartitionOperationTest extends AbstractCairoTest {
    private static final String HEADER = "a\tts\tb\n";
    private static final String ROW_1 = "1\t2024-01-01T00:00:00.000000Z\tnull\n";
    private static final String ROW_2 = "2\t2024-01-02T00:00:00.000000Z\tnull\n";
    // written into the attached partition after the round trip
    private static final String ROW_2_WRITTEN = "5\t2024-01-02T12:00:00.000000Z\t50\n";
    private static final String ROW_3 = "3\t2024-01-03T00:00:00.000000Z\tnull\n";
    private static final String ROW_4 = "4\t2024-01-04T00:00:00.000000Z\t40\n";

    @Override
    @Before
    public void setUp() {
        // the detached copy takes the name ATTACH looks for, as in the fuzz tests that detach
        node1.setProperty(PropertyKey.CAIRO_ATTACH_PARTITION_SUFFIX, TableUtils.DETACHED_DIR_MARKER);
        super.setUp();
    }

    @Test
    public void testApplyDetachAttach() throws Exception {
        assertRoundTrip(false, BETWEEN_NOTHING, 0, ROW_1 + ROW_2 + ROW_2_WRITTEN + ROW_3 + ROW_4);
    }

    @Test
    public void testApplyDetachAttachWal() throws Exception {
        assertRoundTrip(true, BETWEEN_NOTHING, 0, ROW_1 + ROW_2 + ROW_2_WRITTEN + ROW_3 + ROW_4);
    }

    @Test
    public void testApplyDetachDropNewestAndOlderAttach() throws Exception {
        // a DROP from the day before the detached one leaves only the detached day
        assertRoundTrip(false, BETWEEN_DROP_NEWEST, -1, ROW_2 + ROW_2_WRITTEN);
    }

    @Test
    public void testApplyDetachDropNewestAndOlderAttachWal() throws Exception {
        assertRoundTrip(true, BETWEEN_DROP_NEWEST, -1, ROW_2 + ROW_2_WRITTEN);
    }

    @Test
    public void testApplyDetachDropNewestAttach() throws Exception {
        // b was added at 2024-01-03, which the DROP removes: the table's "added at" partition moves to 2024-01-01,
        // before the attached day, which holds no b
        assertRoundTrip(false, BETWEEN_DROP_NEWEST, 1, ROW_1 + ROW_2 + ROW_2_WRITTEN);
    }

    @Test
    public void testApplyDetachDropNewestAttachWal() throws Exception {
        assertRoundTrip(true, BETWEEN_DROP_NEWEST, 1, ROW_1 + ROW_2 + ROW_2_WRITTEN);
    }

    @Test
    public void testApplyDetachRenameAwayAndBackAttach() throws Exception {
        // 2024-01-02 holds every column the operation can rename, a and new_col_1: the ATTACH while either is renamed
        // away refuses the partition
        assertRenameAwayAndBackRoundTrip(false, RENAMED_AWAY_ATTACH_REFUSED);
    }

    @Test
    public void testApplyDetachRenameAwayAndBackAttachNoRows() throws Exception {
        // a and new_col_1, the columns the operation can rename, join the table after 2024-01-02 was written: the ATTACH
        // while either is renamed away attaches the partition, which reads both as NULL, and the operation attaches
        // nothing more
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01'), ('2024-01-02'), ('2024-01-03')");
            execute("ALTER TABLE t ADD COLUMN a INT");
            execute("ALTER TABLE t ADD COLUMN new_col_1 INT");
            execute("INSERT INTO t VALUES ('2024-01-04', 4, 40)");
            final FuzzDetachAttachPartitionOperation operation = new FuzzDetachAttachPartitionOperation(
                    MicrosTimestampDriver.floor("2024-01-02"),
                    BETWEEN_RENAME_AWAY_AND_BACK,
                    0
            );
            apply(false, operation);
            Assert.assertEquals(RENAMED_AWAY_ATTACH_ATTACHED, operation.getRenamedAwayAttachOutcome());
            final String expected = """
                    ts\ta\tnew_col_1
                    2024-01-01T00:00:00.000000Z\tnull\tnull
                    2024-01-02T00:00:00.000000Z\tnull\tnull
                    2024-01-03T00:00:00.000000Z\tnull\tnull
                    2024-01-04T00:00:00.000000Z\t4\t40
                    """;
            assertQuery("SELECT * FROM t").noLeakCheck().timestamp("ts").expectSize().returns(expected);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery("SELECT * FROM t").noLeakCheck().timestamp("ts").expectSize().returns(expected);
        });
    }

    @Test
    public void testApplyDetachRenameAwayAndBackAttachParquet() throws Exception {
        // a Parquet partition finds new_col_1 by its id: the operation tries no ATTACH while new_col_1 is renamed away
        assertMemoryLeak(() -> {
            createTableWithAddedColumn(false);
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-02'");
            final FuzzDetachAttachPartitionOperation operation = new FuzzDetachAttachPartitionOperation(
                    MicrosTimestampDriver.floor("2024-01-02"),
                    BETWEEN_RENAME_AWAY_AND_BACK,
                    0
            );
            apply(false, operation);
            Assert.assertEquals(RENAMED_AWAY_ATTACH_NOT_TRIED, operation.getRenamedAwayAttachOutcome());
            try (TableReader reader = getReader("t")) {
                Assert.assertTrue(reader.getTxFile().isPartitionParquet(1));
            }
            assertQuery("SELECT * FROM t").noLeakCheck().timestamp("ts").expectSize().returns("""
                    a\tts\tnew_col_1
                    1\t2024-01-01T00:00:00.000000Z\tnull
                    2\t2024-01-02T00:00:00.000000Z\t20
                    3\t2024-01-03T00:00:00.000000Z\t30
                    4\t2024-01-04T00:00:00.000000Z\t40
                    """);
        });
    }

    @Test
    public void testApplyDetachRenameCreatedColumnAwayAndBackAttach() throws Exception {
        // a, from CREATE TABLE, is the only column to rename: its files carry no column name txn. 2024-01-02 holds a, so
        // the ATTACH while a is renamed away refuses the partition.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES (1, '2024-01-01'), (2, '2024-01-02'), (3, '2024-01-03')");
            final long metadataVersion = getMetadataVersion();
            final FuzzDetachAttachPartitionOperation operation = new FuzzDetachAttachPartitionOperation(
                    MicrosTimestampDriver.floor("2024-01-02"),
                    BETWEEN_RENAME_AWAY_AND_BACK,
                    0
            );
            apply(false, operation);
            Assert.assertEquals(RENAMED_AWAY_ATTACH_REFUSED, operation.getRenamedAwayAttachOutcome());
            // a renamed away and back
            Assert.assertEquals(metadataVersion + 2, getMetadataVersion());
            execute("INSERT INTO t VALUES (5, '2024-01-02T12:00:00.000000Z')");
            final String expected = """
                    a\tts
                    1\t2024-01-01T00:00:00.000000Z
                    2\t2024-01-02T00:00:00.000000Z
                    5\t2024-01-02T12:00:00.000000Z
                    3\t2024-01-03T00:00:00.000000Z
                    """;
            assertQuery("SELECT * FROM t").noLeakCheck().timestamp("ts").expectSize().returns(expected);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery("SELECT * FROM t").noLeakCheck().timestamp("ts").expectSize().returns(expected);
        });
    }

    @Test
    public void testApplyDetachRenameAwayAndBackAttachWal() throws Exception {
        // the WAL table takes no renames
        assertRenameAwayAndBackRoundTrip(true, RENAMED_AWAY_ATTACH_NOT_TRIED);
    }

    @Test
    public void testApplyDetachTruncateAttach() throws Exception {
        assertRoundTrip(false, BETWEEN_TRUNCATE, 0, ROW_2 + ROW_2_WRITTEN);
    }

    @Test
    public void testApplyDetachTruncateAttachWal() throws Exception {
        assertRoundTrip(true, BETWEEN_TRUNCATE, 0, ROW_2 + ROW_2_WRITTEN);
    }

    @Test
    public void testApplyFailsWhenDetachedPartitionDoesNotAttachBack() throws Exception {
        // the ATTACH of a non-WAL table finds no detached copy, as if the DETACH had lost it
        final TestFilesFacadeImpl ff = new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                return !Utf8s.endsWithAscii(path, "2024-01-02" + TableUtils.DETACHED_DIR_MARKER) && super.exists(path);
            }
        };
        assertMemoryLeak(ff, () -> {
            createTable(false);
            final FuzzDetachAttachPartitionOperation operation = new FuzzDetachAttachPartitionOperation(
                    MicrosTimestampDriver.floor("2024-01-02"),
                    BETWEEN_NOTHING,
                    0
            );
            try (TableWriter writer = getWriter("t")) {
                operation.apply(new Rnd(), engine, writer, -1, null);
                Assert.fail("a partition that did not attach back must fail the operation");
            } catch (AssertionError e) {
                TestUtils.assertContains(e.getMessage(), "a detached partition did not attach back [table=t, partition=2024-01-02]");
            }
            assertTable(HEADER + ROW_1 + ROW_3 + ROW_4);
        });
    }

    @Test
    public void testApplyRenamesBackColumnLeftRenamedAway() throws Exception {
        // An attempt of the operation that failed between its two renames leaves 2024-01-02 detached and new_col_1
        // renamed away. The run retries the operation, whose DETACH then fails.
        assertMemoryLeak(() -> {
            createTableWithAddedColumn(false);
            execute("ALTER TABLE t DETACH PARTITION LIST '2024-01-02'");
            execute("ALTER TABLE t RENAME COLUMN new_col_1 TO " + RENAMED_AWAY_PREFIX + "new_col_1");
            final long metadataVersion = getMetadataVersion();
            apply(false, new FuzzDetachAttachPartitionOperation(MicrosTimestampDriver.floor("2024-01-02"), BETWEEN_RENAME_AWAY_AND_BACK, 0));
            // the rename back, then the two of the round trip, of a or new_col_1
            Assert.assertEquals(metadataVersion + 3, getMetadataVersion());
            assertQuery("SELECT * FROM t").noLeakCheck().timestamp("ts").expectSize().returns("""
                    a\tts\tnew_col_1
                    1\t2024-01-01T00:00:00.000000Z\tnull
                    2\t2024-01-02T00:00:00.000000Z\t20
                    3\t2024-01-03T00:00:00.000000Z\t30
                    4\t2024-01-04T00:00:00.000000Z\t40
                    """);
        });
    }

    @Test
    public void testApplySkipsActivePartition() throws Exception {
        // the newest partition cannot be detached, so it stays, and the TRUNCATE between runs as usual
        assertSkipped(false, "2024-01-04");
    }

    @Test
    public void testApplySkipsActivePartitionWal() throws Exception {
        assertSkipped(true, "2024-01-04");
    }

    @Test
    public void testApplySkipsMissingPartition() throws Exception {
        assertSkipped(false, "2023-12-31");
    }

    @Test
    public void testApplySkipsMissingPartitionWal() throws Exception {
        assertSkipped(true, "2023-12-31");
    }

    @Test
    public void testGenerateSetEmitsDetachAttachAndDropNewestPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("t");
            try (
                    TableRecordMetadata sequencerMetadata = engine.getLegacyMetadata(tableToken);
                    TableMetadata tableMetadata = engine.getTableMetadata(tableToken)
            ) {
                final long minTimestamp = MicrosTimestampDriver.floor("2024-01-01T12");
                final ObjList<FuzzTransaction> transactions = generateSet(sequencerMetadata, tableMetadata, minTimestamp, 0.1, 0.03);
                final TreeSet<Integer> betweens = new TreeSet<>();
                final LongHashSet detachedDays = new LongHashSet();
                int dropAllCount = 0;
                int dropSomeCount = 0;
                int refillCount = 0;
                long maxWritten = Long.MIN_VALUE;
                // the first day the last DROP removed, which the next data block writes again
                long refillFrom = Long.MIN_VALUE;
                for (int i = 0, n = transactions.size(); i < n; i++) {
                    final FuzzTransaction transaction = transactions.getQuick(i);
                    long minInserted = Long.MAX_VALUE;
                    long maxInserted = Long.MIN_VALUE;
                    for (int j = 0, m = transaction.operationList.size(); j < m; j++) {
                        final FuzzTransactionOperation operation = transaction.operationList.getQuick(j);
                        if (operation instanceof FuzzDetachAttachPartitionOperation detachAttach) {
                            Assert.assertTrue(transaction.waitAllDone);
                            Assert.assertEquals(0, detachAttach.getPartitionTimestamp() % Micros.DAY_MICROS);
                            Assert.assertTrue("a day is detached once a run", detachedDays.add(detachAttach.getPartitionTimestamp()));
                            // a day the run wrote, before the front
                            Assert.assertTrue(detachAttach.getPartitionTimestamp() + Micros.DAY_MICROS <= maxWritten);
                            betweens.add(detachAttach.getBetween());
                            final long dropFromDays = (detachAttach.getDropFromTimestamp() - detachAttach.getPartitionTimestamp()) / Micros.DAY_MICROS;
                            Assert.assertTrue(dropFromDays >= -2 && dropFromDays <= 2);
                            // a DROP from past the rows written so far removes none to write again
                            if (detachAttach.getBetween() == BETWEEN_DROP_NEWEST && detachAttach.getDropFromTimestamp() < maxWritten) {
                                refillFrom = detachAttach.getDropFromTimestamp();
                            }
                        } else if (operation instanceof FuzzDropPartitionOperation drop) {
                            Assert.assertTrue(drop.isDroppingNewest());
                            Assert.assertTrue(transaction.waitAllDone);
                            Assert.assertEquals(0, drop.getCutoffTimestamp() % Micros.DAY_MICROS);
                            if (drop.getCutoffTimestamp() < MicrosTimestampDriver.floor("2024-01-01")) {
                                dropAllCount++;
                            } else {
                                dropSomeCount++;
                            }
                            refillFrom = drop.getCutoffTimestamp();
                        } else {
                            final long timestamp = ((FuzzInsertOperation) operation).getTimestamp();
                            minInserted = Math.min(minInserted, timestamp);
                            maxInserted = Math.max(maxInserted, timestamp);
                        }
                    }
                    if (maxInserted != Long.MIN_VALUE) {
                        if (refillFrom != Long.MIN_VALUE) {
                            // the block after a DROP writes from its first day up to the front, not past it
                            Assert.assertTrue(minInserted >= refillFrom);
                            Assert.assertTrue(maxInserted <= maxWritten + Micros.DAY_MICROS);
                            refillFrom = Long.MIN_VALUE;
                            refillCount++;
                        }
                        maxWritten = Math.max(maxWritten, maxInserted);
                    }
                }
                Assert.assertEquals("[0, 1, 2, 3]", betweens.toString());
                Assert.assertTrue(detachedDays.size() > 1);
                Assert.assertTrue(dropAllCount > 0);
                Assert.assertTrue(dropSomeCount > 0);
                Assert.assertTrue(refillCount > 0);

                // with no probabilities for them, the generator draws the transactions it drew before they existed:
                // inserts only here
                final ObjList<FuzzTransaction> inserts = generateSet(sequencerMetadata, tableMetadata, minTimestamp, 0, 0);
                Assert.assertTrue(inserts.size() > 0);
                for (int i = 0, n = inserts.size(); i < n; i++) {
                    final FuzzTransaction transaction = inserts.getQuick(i);
                    for (int j = 0, m = transaction.operationList.size(); j < m; j++) {
                        Assert.assertTrue(transaction.operationList.getQuick(j) instanceof FuzzInsertOperation);
                    }
                }
                Misc.freeObjListAndClear(transactions);
                Misc.freeObjListAndClear(inserts);
            }
        });
    }

    private static void createTable(boolean isWal) throws Exception {
        execute("CREATE TABLE t (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + (isWal ? " WAL" : " BYPASS WAL"));
        execute("INSERT INTO t VALUES (1, '2024-01-01'), (2, '2024-01-02'), (3, '2024-01-03')");
        // added at 2024-01-03, after the rows of the days before it were written
        execute("ALTER TABLE t ADD COLUMN b INT");
        execute("INSERT INTO t VALUES (4, '2024-01-04', 40)");
        drainWalQueue();
    }

    // a table whose days from 2024-01-02 on hold all of new_col_1, named as the fuzz names the columns it adds
    private static void createTableWithAddedColumn(boolean isWal) throws Exception {
        execute("CREATE TABLE t (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + (isWal ? " WAL" : " BYPASS WAL"));
        execute("INSERT INTO t VALUES (1, '2024-01-01')");
        execute("ALTER TABLE t ADD COLUMN new_col_1 INT");
        execute("INSERT INTO t VALUES (2, '2024-01-02', 20), (3, '2024-01-03', 30), (4, '2024-01-04', 40)");
        drainWalQueue();
    }

    private static ObjList<FuzzTransaction> generateSet(
            TableRecordMetadata sequencerMetadata,
            TableMetadata tableMetadata,
            long minTimestamp,
            double probabilityOfDetachAttach,
            double probabilityOfDropNewestPartitions
    ) {
        return FuzzTransactionGenerator.generateSet(
                0,
                sequencerMetadata,
                tableMetadata,
                new Rnd(42, 4242),
                minTimestamp,
                // a run detaches each day once, and each DETACH draws one of four kinds of statement between
                minTimestamp + 20 * Micros.DAY_MICROS,
                1_000,
                500,
                false,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                1,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                5,
                new String[]{"a", "b", "c"},
                (int) sequencerMetadata.getMetadataVersion(),
                0,
                0,
                0,
                0,
                0,
                0,
                false,
                false,
                null,
                probabilityOfDetachAttach,
                probabilityOfDropNewestPartitions
        );
    }

    private static long getMetadataVersion() {
        try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("t"))) {
            return metadata.getMetadataVersion();
        }
    }

    private void apply(boolean isWal, FuzzTransactionOperation operation) {
        final TableToken token = engine.verifyTableName("t");
        if (isWal) {
            try (WalWriter writer = engine.getWalWriter(token)) {
                Assert.assertTrue(operation.apply(new Rnd(), engine, writer, -1, null));
                writer.commit();
            }
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
        } else {
            try (TableWriter writer = getWriter(token)) {
                Assert.assertTrue(operation.apply(new Rnd(), engine, writer, -1, null));
                writer.commit();
            }
        }
    }

    private void assertRenameAwayAndBackRoundTrip(boolean isWal, int expectedRenamedAwayAttachOutcome) throws Exception {
        assertMemoryLeak(() -> {
            createTableWithAddedColumn(isWal);
            final long metadataVersion = getMetadataVersion();
            final long day = MicrosTimestampDriver.floor("2024-01-02");
            final FuzzDetachAttachPartitionOperation operation = new FuzzDetachAttachPartitionOperation(day, BETWEEN_RENAME_AWAY_AND_BACK, 0);
            apply(isWal, operation);
            Assert.assertEquals(expectedRenamedAwayAttachOutcome, operation.getRenamedAwayAttachOutcome());
            // a or new_col_1 renamed away and back on the non-WAL table, the WAL table left as it was
            Assert.assertEquals(metadataVersion + (isWal ? 0 : 2), getMetadataVersion());
            execute("INSERT INTO t VALUES (5, '2024-01-02T12:00:00.000000Z', 50)");
            drainWalQueue();
            final String expected = """
                    a\tts\tnew_col_1
                    1\t2024-01-01T00:00:00.000000Z\tnull
                    2\t2024-01-02T00:00:00.000000Z\t20
                    5\t2024-01-02T12:00:00.000000Z\t50
                    3\t2024-01-03T00:00:00.000000Z\t30
                    4\t2024-01-04T00:00:00.000000Z\t40
                    """;
            assertQuery("SELECT * FROM t").noLeakCheck().timestamp("ts").expectSize().returns(expected);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertQuery("SELECT * FROM t").noLeakCheck().timestamp("ts").expectSize().returns(expected);
        });
    }

    private void assertRoundTrip(boolean isWal, int between, int dropFromDays, String expectedRows) throws Exception {
        assertMemoryLeak(() -> {
            createTable(isWal);
            final long day = MicrosTimestampDriver.floor("2024-01-02");
            apply(isWal, new FuzzDetachAttachPartitionOperation(day, between, day + dropFromDays * Micros.DAY_MICROS));
            // the attached day holds no b, also in the rows written into it after the attach
            execute("INSERT INTO t VALUES (5, '2024-01-02T12:00:00.000000Z', 50)");
            drainWalQueue();
            assertTable(HEADER + expectedRows);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertTable(HEADER + expectedRows);
        });
    }

    private void assertSkipped(boolean isWal, String day) throws Exception {
        assertMemoryLeak(() -> {
            createTable(isWal);
            apply(isWal, new FuzzDetachAttachPartitionOperation(MicrosTimestampDriver.floor(day), BETWEEN_TRUNCATE, 0));
            // the TRUNCATE between ran, the ATTACH found nothing to attach
            assertTable(HEADER);
            execute("INSERT INTO t VALUES (4, '2024-01-04', 40)");
            drainWalQueue();
            assertTable(HEADER + ROW_4);
        });
    }

    private void assertTable(String expected) throws Exception {
        assertQuery("SELECT a, ts, b FROM t").noLeakCheck().timestamp("ts").expectSize().returns(expected);
    }
}
