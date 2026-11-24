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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A row that its caller leaves unfinished - neither append() nor cancel() - and the rows after it. ILP UDP leaves the
 * row of a line unfinished when a value of the line cannot be put, and the TableWriter API lets any caller do the
 * same. {@link TableWriter#newRow(long)} cancels the unfinished row, and the new row must take the path of the rows
 * that will be committed: in order, out of order, or into the first partition of the table. The table ref gets the
 * same rows as tab, without the unfinished ones.
 */
public class TableWriterAbandonedRowTest extends AbstractCairoTest {
    private static final long DAY = 86_400_000_000L;
    private static final long DAY_NANOS = 86_400_000_000_000L;
    private static final String GEOHASH_CHARS = "0123456789bcdefghjkmnpqrstuvwxyz";
    private static final DateTimeFormatter MICROS_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter MILLIS_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter NANOS_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'").withZone(ZoneOffset.UTC);

    @Test
    public void testAbandonedFirstO3RowAfterAddColumn() throws Exception {
        // y is added after the committed rows, so it has a column top in their partition
        assertMemoryLeak(() -> {
            createTables("TIMESTAMP", "DAY", 10);
            execute("ALTER TABLE tab ADD COLUMN y INT");
            execute("ALTER TABLE ref ADD COLUMN y INT");
            try (TableWriter writer = getWriter("tab")) {
                final TableWriter.Row abandoned = writer.newRow(5);
                abandoned.putLong(1, 1005);
                abandoned.putInt(2, 5005);
                TableWriter.Row row = writer.newRow(11);
                row.putLong(1, 11);
                row.putInt(2, 111);
                row.append();
                row = writer.newRow(12);
                row.putLong(1, 12);
                row.append();
                writer.commit();
            }
            execute("INSERT INTO ref VALUES (11::TIMESTAMP, 11, 111), (12::TIMESTAMP, 12, null)");
            final StringBuilder expected = new StringBuilder("ts\tx\ty\n");
            for (int x = 1; x <= 10; x++) {
                expected.append(micros(x)).append('\t').append(x).append("\tnull\n");
            }
            expected.append(micros(11)).append("\t11\t111\n");
            expected.append(micros(12)).append("\t12\tnull\n");
            assertQuery("tab")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns(expected);
            assertSqlCursors("ref", "tab");

            execute("INSERT INTO tab VALUES (13::TIMESTAMP, 13, 113)");
            execute("INSERT INTO ref VALUES (13::TIMESTAMP, 13, 113)");
            assertQuery("tab")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns(expected + micros(13) + "\t13\t113\n");
            assertSqlCursors("ref", "tab");
        });
    }

    @Test
    public void testAbandonedFirstO3RowEveryColumnType() throws Exception {
        // the next rows must not take the values of the rows before them in any column
        assertMemoryLeak(() -> {
            final EveryTypeRows rows = EveryTypeRows.createTables(10);
            try (
                    TableWriter writer = getWriter("tab");
                    DirectArray array = new DirectArray()
            ) {
                final TestRecord.ArrayBinarySequence binary = new TestRecord.ArrayBinarySequence();
                EveryTypeRows.put(writer.newRow(5), 50, array, binary);
                rows.append(writer, 11, 11, array, binary);
                rows.appendNull(writer, 12);
                rows.append(writer, 13, 13, array, binary);
                writer.commit();
            }
            assertEveryTypeTables(rows);
            assertSymbolIndexes(rows, 13, 50);
            rows.insert(14, 14);
            assertEveryTypeTables(rows);
        });
    }

    @Test
    public void testAbandonedFirstO3RowSecondTransaction() throws Exception {
        // an earlier transaction committed out-of-order rows, so the O3 buffers hold memory already
        assertMemoryLeak(() -> {
            final Rows rows = createTables("TIMESTAMP", "DAY", 10);
            try (TableWriter writer = getWriter("tab")) {
                appendRow(writer, rows, 5, 105);
                writer.commit();
                abandonRow(writer, 6, 1006);
                appendRow(writer, rows, 11, 11);
                writer.commit();
            }
            assertTables(rows);
            insertRow(rows, 12, 12);
            assertTables(rows);
        });
    }

    @Test
    public void testAbandonedFirstO3RowThenCancelledRow() throws Exception {
        // the cancel of the next row must not take a committed row with it
        assertMemoryLeak(() -> {
            final Rows rows = createTables("TIMESTAMP", "DAY", 10);
            try (TableWriter writer = getWriter("tab")) {
                abandonRow(writer, 5, 1005);
                final TableWriter.Row row = writer.newRow(11);
                row.putLong(1, 11);
                row.cancel();
                writer.commit();
            }
            assertTables(rows);
            insertRow(rows, 11, 11);
            assertTables(rows);
        });
    }

    @Test
    public void testAbandonedFirstO3RowThenCommit() throws Exception {
        // commit() cancels the unfinished row before it commits
        assertMemoryLeak(() -> {
            final Rows rows = createTables("TIMESTAMP", "DAY", 10);
            try (TableWriter writer = getWriter("tab")) {
                abandonRow(writer, 5, 1005);
                writer.commit();
                Assert.assertFalse(writer.hasO3());
                appendRow(writer, rows, 11, 11);
                writer.commit();
            }
            assertTables(rows);
        });
    }

    @Test
    public void testAbandonedFirstO3RowThenNullRow() throws Exception {
        // a row with no value but its timestamp: x is NULL
        assertMemoryLeak(() -> {
            final Rows rows = createTables("TIMESTAMP", "DAY", 10);
            try (TableWriter writer = getWriter("tab")) {
                abandonRow(writer, 5, 1005);
                writer.newRow(11).append();
                appendRow(writer, rows, 12, 12);
                writer.commit();
            }
            execute("INSERT INTO ref (ts) VALUES (11::TIMESTAMP)");
            rows.add(11, Numbers.LONG_NULL);
            assertTables(rows);
            insertRow(rows, 13, 13);
            assertTables(rows);
        });
    }

    @Test
    public void testAbandonedFirstO3RowThenPre1970Row() throws Exception {
        // newRow() refuses a timestamp before 1970 before it cancels anything, so the unfinished row is still the
        // first out-of-order row of the transaction, and its caller can still append it
        assertMemoryLeak(() -> {
            final Rows rows = createTables("TIMESTAMP", "DAY", 10);
            try (TableWriter writer = getWriter("tab")) {
                final TableWriter.Row abandoned = abandonRow(writer, 5, 1005);
                try {
                    writer.newRow(-DAY + 5);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "designated timestamp before 1970-01-01 is not allowed");
                }
                Assert.assertTrue(writer.hasO3());
                Assert.assertEquals(0, writer.getO3RowCount());
                abandoned.append();
                writer.commit();
            }
            execute("INSERT INTO ref VALUES (5::TIMESTAMP, 1005)");
            rows.add(5, 1005);
            assertTables(rows);
            insertRow(rows, 11, 11);
            assertTables(rows);
        });
    }

    @Test
    public void testAbandonedFirstO3RowThenRowAt() throws Exception {
        // The next row in order: at the committed max timestamp, after it, and in the next partition; and out of
        // order: between committed rows, and at the timestamp of the unfinished row. Both kinds of designated
        // timestamp.
        for (String timestampType : new String[]{"TIMESTAMP", "TIMESTAMP_NS"}) {
            final long day = "TIMESTAMP".equals(timestampType) ? DAY : DAY_NANOS;
            for (long timestamp : new long[]{10, 11, day + 1, 6, 5}) {
                assertMemoryLeak(() -> {
                    try {
                        final Rows rows = createTables(timestampType, "DAY", 10);
                        try (TableWriter writer = getWriter("tab")) {
                            abandonRow(writer, 5, 1005);
                            appendRow(writer, rows, timestamp, 100);
                            writer.commit();
                        }
                        assertTables(rows);
                        insertRow(rows, day + 2, 101);
                        assertTables(rows);
                    } catch (AssertionError e) {
                        throw new AssertionError(timestampType + " " + timestamp, e);
                    }
                    dropTables();
                });
            }
        }
    }

    @Test
    public void testAbandonedFirstO3RowThenRowsAcrossCommitsAndPartitions() throws Exception {
        // the rows of the transaction of the unfinished row, of the next transaction of the same partition and of the
        // next partition; and the same rows after the writer and the readers reopen the table
        assertMemoryLeak(() -> {
            final Rows rows = createStringTables(10);
            try (TableWriter writer = getWriter("tab")) {
                final TableWriter.Row abandoned = writer.newRow(5);
                abandoned.putLong(1, 1005);
                abandoned.putStr(2, "s1005");
                for (long x = 11; x <= 13; x++) {
                    appendStringRow(writer, rows, x, x);
                }
                writer.commit();
                for (long x = 14; x <= 15; x++) {
                    appendStringRow(writer, rows, x, x);
                }
                writer.commit();
                appendStringRow(writer, rows, DAY + 1, 16);
                appendStringRow(writer, rows, DAY + 2, 17);
                writer.commit();
            }
            assertTables(rows);
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            assertTables(rows);
            execute("INSERT INTO tab VALUES (" + (DAY + 3) + "::TIMESTAMP, 18, 's18')");
            execute("INSERT INTO ref VALUES (" + (DAY + 3) + "::TIMESTAMP, 18, 's18')");
            rows.add(DAY + 3, 18);
            assertTables(rows);
        });
    }

    @Test
    public void testAbandonedFirstRowOfNewPartitionThenRow() throws Exception {
        // the cancel of the first row of 1970-01-02 opens 1970-01-01 again, before the next row switches partitions
        assertMemoryLeak(() -> {
            final Rows rows = createTables("TIMESTAMP", "DAY", 10);
            try (TableWriter writer = getWriter("tab")) {
                abandonRow(writer, DAY + 1, 100);
                appendRow(writer, rows, DAY + 2, 11);
                writer.commit();
            }
            assertTables(rows);
            insertRow(rows, 11, 12);
            assertTables(rows);
        });
    }

    @Test
    public void testAbandonedLaterO3RowThenRow() throws Exception {
        // the cancel of an out-of-order row that is not the first of the transaction keeps the rows before it
        assertMemoryLeak(() -> {
            final Rows rows = createTables("TIMESTAMP", "DAY", 10);
            try (TableWriter writer = getWriter("tab")) {
                appendRow(writer, rows, 3, 103);
                appendRow(writer, rows, 4, 104);
                appendRow(writer, rows, 6, 106);
                Assert.assertEquals(3, writer.getO3RowCount());
                abandonRow(writer, 5, 1005);
                appendRow(writer, rows, 11, 11);
                writer.commit();
            }
            assertTables(rows);
            insertRow(rows, 12, 12);
            assertTables(rows);
        });
    }

    @Test
    public void testAbandonedOnlyRowCancelFailsThenRow() throws Exception {
        // The cancel of the only row of an empty table cannot remove the directory of its partition. The row stays
        // unfinished, and the next row cancels it again.
        final AtomicBoolean isArmed = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean rmdir(Path name, boolean haltOnError) {
                if (Utf8s.containsAscii(name, "tab~") && Utf8s.containsAscii(name, "1970-01-01") && isArmed.compareAndSet(true, false)) {
                    return false;
                }
                return super.rmdir(name, haltOnError);
            }
        };
        assertMemoryLeak(ff, () -> {
            final Rows rows = createTables("TIMESTAMP", "DAY", 0);
            try (TableWriter writer = getWriter("tab")) {
                abandonRow(writer, 1, 1);
                isArmed.set(true);
                try {
                    writer.newRow(DAY + 1);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not remove directory");
                }
                Assert.assertFalse(isArmed.get());
                Assert.assertFalse(writer.isDistressed());
                appendRow(writer, rows, DAY + 1, 2);
                writer.commit();
            }
            assertTables(rows);
            assertPartitionDirectory("1970-01-01", false);
            insertRow(rows, 5, 5);
            assertTables(rows);
        });
    }

    @Test
    public void testAbandonedOnlyRowEveryColumnTypeThenNextPartition() throws Exception {
        // the cancel of the only row of an empty table removes its partition, symbol indexes included
        assertMemoryLeak(() -> {
            final EveryTypeRows rows = EveryTypeRows.createTables(0);
            try (
                    TableWriter writer = getWriter("tab");
                    DirectArray array = new DirectArray()
            ) {
                final TestRecord.ArrayBinarySequence binary = new TestRecord.ArrayBinarySequence();
                EveryTypeRows.put(writer.newRow(1), 50, array, binary);
                rows.append(writer, DAY + 1, 11, array, binary);
                writer.commit();
            }
            assertEveryTypeTables(rows);
            assertSymbolIndexes(rows, 11, 50);
            assertPartitionDirectory("1970-01-01", false);
            rows.insert(2, 12);
            assertEveryTypeTables(rows);
            assertSymbolIndexes(rows, 12, 50);
        });
    }

    @Test
    public void testAbandonedOnlyRowEveryColumnTypeThenSamePartition() throws Exception {
        // the cancel of the only row of an empty table removes its partition, and the next row creates it again
        assertMemoryLeak(() -> {
            final EveryTypeRows rows = EveryTypeRows.createTables(0);
            try (
                    TableWriter writer = getWriter("tab");
                    DirectArray array = new DirectArray()
            ) {
                final TestRecord.ArrayBinarySequence binary = new TestRecord.ArrayBinarySequence();
                EveryTypeRows.put(writer.newRow(1), 50, array, binary);
                rows.append(writer, 2, 11, array, binary);
                writer.commit();
            }
            assertEveryTypeTables(rows);
            assertSymbolIndexes(rows, 11, 50);
            rows.insert(3, 12);
            assertEveryTypeTables(rows);
            assertSymbolIndexes(rows, 12, 50);
        });
    }

    @Test
    public void testAbandonedOnlyRowThenCommit() throws Exception {
        // commit() cancels the only row of the empty table, and its partition goes
        assertMemoryLeak(() -> {
            final Rows rows = createTables("TIMESTAMP", "DAY", 0);
            try (TableWriter writer = getWriter("tab")) {
                abandonRow(writer, 1, 1);
                writer.commit();
            }
            assertTables(rows);
            assertPartitionDirectory("1970-01-01", false);
            insertRow(rows, 2, 2);
            assertTables(rows);
        });
    }

    @Test
    public void testAbandonedOnlyRowThenNextPartition() throws Exception {
        // the partition of the unfinished row goes, and the table keeps reading after the writer and the readers
        // reopen it and a row lands in 1970-01-01
        assertMemoryLeak(() -> {
            final Rows rows = createTables("TIMESTAMP", "DAY", 0);
            try (TableWriter writer = getWriter("tab")) {
                abandonRow(writer, 1, 1);
                appendRow(writer, rows, DAY + 1, 2);
                writer.commit();
            }
            assertTables(rows);
            assertPartitionDirectory("1970-01-01", false);
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            assertTables(rows);
            insertRow(rows, 5, 5);
            assertTables(rows);
        });
    }

    @Test
    public void testAbandonedOnlyRowThenSamePartition() throws Exception {
        // the cancel removes the partition of the unfinished row, and the next row of the same day creates it again
        assertMemoryLeak(() -> {
            final Rows rows = createTables("TIMESTAMP", "DAY", 0);
            try (TableWriter writer = getWriter("tab")) {
                abandonRow(writer, 1, 1);
                appendRow(writer, rows, 2, 2);
                writer.commit();
            }
            assertTables(rows);
            insertRow(rows, 3, 3);
            assertTables(rows);
        });
    }

    @Test
    public void testNewRowAllocatesNothing() throws Exception {
        // Per transaction: the first out-of-order row, left unfinished, and a row in order, which cancels it first.
        // The round that allocates the least on the heap in newRow(), putLong() and append() allocates nothing; the
        // first round warms up.
        assertMemoryLeak(() -> {
            createTables("TIMESTAMP", "DAY", 10);
            final int rounds = 4;
            final int transactionsPerRound = 1_000;
            try (
                    TableWriter writer = getWriter("tab");
                    TestUtils.ThreadMetricsScope<com.sun.management.ThreadMXBean> scope = TestUtils.threadAllocationScope()
            ) {
                final com.sun.management.ThreadMXBean bean = scope.getBean();
                long minAllocatedBytes = Long.MAX_VALUE;
                long timestamp = 11;
                for (int round = 0; round < rounds; round++) {
                    long allocatedBytes = 0;
                    for (int i = 0; i < transactionsPerRound; i++) {
                        final long allocatedBefore = bean.getCurrentThreadAllocatedBytes();
                        writer.newRow(5).putLong(1, 1005);
                        final TableWriter.Row row = writer.newRow(timestamp);
                        row.putLong(1, timestamp);
                        row.append();
                        allocatedBytes += bean.getCurrentThreadAllocatedBytes() - allocatedBefore;
                        writer.commit();
                        timestamp++;
                    }
                    minAllocatedBytes = Math.min(minAllocatedBytes, allocatedBytes);
                }
                Assert.assertEquals(0, minAllocatedBytes);
            }
            final long n = 10 + rounds * transactionsPerRound;
            assertQuery("SELECT count(), sum(x), min(x), max(x) FROM tab")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tsum\tmin\tmax\n" + n + "\t" + n * (n + 1) / 2 + "\t1\t" + n + "\n");
        });
    }

    @Test
    public void testNoPartitionAbandonedOnlyRowThenEarlierRow() throws Exception {
        // the cancel of the only row of an empty table takes its timestamp back
        assertMemoryLeak(() -> {
            final Rows rows = createTables("TIMESTAMP", "NONE", 0);
            try (TableWriter writer = getWriter("tab")) {
                abandonRow(writer, 100, 100);
                appendRow(writer, rows, 1, 1);
                writer.commit();
            }
            assertTables(rows);
            insertRow(rows, 2, 2);
            assertTables(rows);
        });
    }

    @Test
    public void testNoPartitionAbandonedRowThenEarlierRow() throws Exception {
        // the next row is out of order with the unfinished row only, not with the rows that will be committed
        assertMemoryLeak(() -> {
            final Rows rows = createTables("TIMESTAMP", "NONE", 10);
            try (TableWriter writer = getWriter("tab")) {
                abandonRow(writer, 100, 100);
                appendRow(writer, rows, 50, 50);
                writer.commit();
            }
            assertTables(rows);
            insertRow(rows, 51, 51);
            assertTables(rows);
        });
    }

    @Test
    public void testNoPartitionAbandonedRowThenOutOfOrderRow() throws Exception {
        // the next row is out of order with the committed rows: newRow() refuses it, and the unfinished row is
        // cancelled by then, so an append() of it adds nothing
        assertMemoryLeak(() -> {
            final Rows rows = createTables("TIMESTAMP", "NONE", 10);
            try (TableWriter writer = getWriter("tab")) {
                final TableWriter.Row abandoned = abandonRow(writer, 100, 100);
                try {
                    writer.newRow(5);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "cannot insert rows out of order to non-partitioned table");
                }
                abandoned.append();
                writer.commit();
            }
            assertTables(rows);
            insertRow(rows, 11, 11);
            assertTables(rows);
        });
    }

    @Test
    public void testNoTimestampAbandonedOnlyRowThenRow() throws Exception {
        // a table without a designated timestamp: the cancel of its only row moves every column back to the start
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (x LONG)");
            execute("CREATE TABLE ref (x LONG)");
            try (TableWriter writer = getWriter("tab")) {
                writer.newRow().putLong(0, 100);
                final TableWriter.Row row = writer.newRow();
                row.putLong(0, 1);
                row.append();
                writer.commit();
            }
            execute("INSERT INTO ref VALUES (1)");
            assertNoTimestampTables(1);
        });
    }

    @Test
    public void testNoTimestampAbandonedRowThenRow() throws Exception {
        // the abandoned row with a value, and without one
        for (boolean isAbandonedRowWithValue : new boolean[]{true, false}) {
            assertMemoryLeak(() -> {
                execute("CREATE TABLE tab (x LONG)");
                execute("CREATE TABLE ref (x LONG)");
                execute("INSERT INTO tab SELECT x FROM long_sequence(10)");
                execute("INSERT INTO ref SELECT x FROM long_sequence(11)");
                try (TableWriter writer = getWriter("tab")) {
                    final TableWriter.Row abandoned = writer.newRow();
                    if (isAbandonedRowWithValue) {
                        abandoned.putLong(0, 100);
                    }
                    final TableWriter.Row row = writer.newRow();
                    row.putLong(0, 11);
                    row.append();
                    writer.commit();
                }
                assertNoTimestampTables(11);
                execute("DROP TABLE tab");
                execute("DROP TABLE ref");
            });
        }
    }

    // newRow() and a value, then neither append() nor cancel()
    private static TableWriter.Row abandonRow(TableWriter writer, long timestamp, long x) {
        final TableWriter.Row row = writer.newRow(timestamp);
        row.putLong(1, x);
        return row;
    }

    // appends a row to tab through the writer, and writes the same row to ref
    private static void appendRow(TableWriter writer, Rows rows, long timestamp, long x) throws Exception {
        final TableWriter.Row row = writer.newRow(timestamp);
        row.putLong(1, x);
        row.append();
        execute("INSERT INTO ref VALUES (" + timestamp + "::" + rows.timestampType + ", " + x + ")");
        rows.add(timestamp, x);
    }

    // appends a row to the tab of createStringTables() through the writer, and writes the same row to ref
    private static void appendStringRow(TableWriter writer, Rows rows, long timestamp, long x) throws Exception {
        final TableWriter.Row row = writer.newRow(timestamp);
        row.putLong(1, x);
        row.putStr(2, "s" + x);
        row.append();
        execute("INSERT INTO ref VALUES (" + timestamp + "::TIMESTAMP, " + x + ", 's" + x + "')");
        rows.add(timestamp, x);
    }

    // whether tab has the directory of a partition, whatever its name txn
    private static void assertPartitionDirectory(String partitionName, boolean isExpected) {
        final File tableDir = new File(configuration.getDbRoot(), engine.verifyTableName("tab").getDirName());
        final File[] partitionDirs = tableDir.listFiles((dir, name) -> name.equals(partitionName) || name.startsWith(partitionName + '.'));
        Assert.assertNotNull(partitionDirs);
        Assert.assertEquals(partitionName, isExpected ? 1 : 0, partitionDirs.length);
    }

    // tab and ref (ts, x LONG, s STRING) with rows x = 1..n at ts = x, s = 's' || x, written by INSERT
    private static Rows createStringTables(int n) throws Exception {
        for (String tableName : new String[]{"tab", "ref"}) {
            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, x LONG, s STRING) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO " + tableName + " SELECT x::TIMESTAMP, x, 's' || x FROM long_sequence(" + n + ")");
        }
        final Rows rows = new Rows("TIMESTAMP", true);
        for (long x = 1; x <= n; x++) {
            rows.add(x, x);
        }
        return rows;
    }

    // tab and ref (ts, x LONG) with rows x = 1..n at ts = x, written by INSERT
    private static Rows createTables(String timestampType, String partitionBy, int n) throws Exception {
        for (String tableName : new String[]{"tab", "ref"}) {
            execute("CREATE TABLE " + tableName + " (ts " + timestampType + ", x LONG) TIMESTAMP(ts) PARTITION BY " + partitionBy + " BYPASS WAL");
            if (n > 0) {
                execute("INSERT INTO " + tableName + " SELECT x::" + timestampType + ", x FROM long_sequence(" + n + ")");
            }
        }
        final Rows rows = new Rows(timestampType, false);
        for (long x = 1; x <= n; x++) {
            rows.add(x, x);
        }
        return rows;
    }

    private static void dropTables() throws Exception {
        execute("DROP TABLE tab");
        execute("DROP TABLE ref");
    }

    // writes the same row to tab and ref by INSERT
    private static void insertRow(Rows rows, long timestamp, long x) throws Exception {
        execute("INSERT INTO tab VALUES (" + timestamp + "::" + rows.timestampType + ", " + x + ")");
        execute("INSERT INTO ref VALUES (" + timestamp + "::" + rows.timestampType + ", " + x + ")");
        rows.add(timestamp, x);
    }

    private static String micros(long micros) {
        return MICROS_FORMAT.format(Instant.EPOCH.plus(micros, ChronoUnit.MICROS));
    }

    // every row of tab, and the rows of ref
    private void assertEveryTypeTables(EveryTypeRows rows) throws Exception {
        assertQuery("tab")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(rows.expected());
        assertSqlCursors("ref", "tab");
    }

    // tab and ref without a designated timestamp hold rows x = 1..n; then the next INSERT lands
    private void assertNoTimestampTables(int n) throws Exception {
        final StringBuilder expected = new StringBuilder("x\n");
        for (int x = 1; x <= n; x++) {
            expected.append(x).append('\n');
        }
        assertQuery("tab")
                .noLeakCheck()
                .expectSize()
                .returns(expected);
        assertSqlCursors("ref", "tab");
        execute("INSERT INTO tab VALUES (" + (n + 1) + ")");
        execute("INSERT INTO ref VALUES (" + (n + 1) + ")");
        assertQuery("tab")
                .noLeakCheck()
                .expectSize()
                .returns(expected.append(n + 1).append('\n'));
        assertSqlCursors("ref", "tab");
    }

    // the indexes of both symbol columns find row n, and no row for the symbols of the unfinished row abandonedN
    private void assertSymbolIndexes(EveryTypeRows rows, int n, int abandonedN) throws Exception {
        for (String column : new String[]{"c_sym_bitmap", "c_sym_posting"}) {
            final String prefix = column.equals("c_sym_bitmap") ? "b" : "p";
            assertQuery("SELECT ts, c_long FROM tab WHERE " + column + " = '" + prefix + n + "'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .withPlanContaining("Index forward scan on: " + column)
                    .returns("ts\tc_long\n" + micros(rows.timestampOf(n)) + "\t" + n * 100_000L + "\n");
            assertQuery("SELECT ts, c_long FROM tab WHERE " + column + " = '" + prefix + abandonedN + "'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\tc_long\n");
        }
    }

    // the rows of tab: count and sum, every row, and the rows of ref
    private void assertTables(Rows rows) throws Exception {
        assertQuery("SELECT count(), sum(x) FROM tab")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\tsum\n" + rows.size() + "\t" + (rows.size() > 0 ? Long.toString(rows.sum()) : "null") + "\n");
        assertQuery("SELECT * FROM tab ORDER BY ts, x")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(rows.toString());
        assertSqlCursors("ref", "tab");
    }

    /**
     * tab and ref with a column of every type after the designated timestamp ts. Row n holds values derived from n
     * in every column; a NULL row holds its timestamp only.
     */
    private static class EveryTypeRows {
        private static final String COLUMNS = """
                ts TIMESTAMP, c_bool BOOLEAN, c_byte BYTE, c_short SHORT, c_char CHAR, c_int INT, c_long LONG,
                c_float FLOAT, c_double DOUBLE, c_date DATE, c_ts TIMESTAMP, c_ts_ns TIMESTAMP_NS, c_sym SYMBOL,
                c_sym_bitmap SYMBOL INDEX TYPE BITMAP, c_sym_posting SYMBOL INDEX TYPE POSTING, c_str STRING,
                c_varchar VARCHAR, c_varchar_long VARCHAR, c_bin BINARY, c_uuid UUID, c_long256 LONG256, c_ipv4 IPv4,
                c_geo1 GEOHASH(1c), c_geo2 GEOHASH(2c), c_geo4 GEOHASH(4c), c_geo8 GEOHASH(8c), c_dec8 DECIMAL(2, 1),
                c_dec16 DECIMAL(4, 2), c_dec32 DECIMAL(9, 3), c_dec64 DECIMAL(18, 4), c_dec128 DECIMAL(38, 5),
                c_dec256 DECIMAL(76, 6), c_arr DOUBLE[]""";
        private static final String HEADER = "ts\tc_bool\tc_byte\tc_short\tc_char\tc_int\tc_long\tc_float\tc_double\tc_date\tc_ts\tc_ts_ns\t"
                + "c_sym\tc_sym_bitmap\tc_sym_posting\tc_str\tc_varchar\tc_varchar_long\tc_bin\tc_uuid\tc_long256\tc_ipv4\t"
                + "c_geo1\tc_geo2\tc_geo4\tc_geo8\tc_dec8\tc_dec16\tc_dec32\tc_dec64\tc_dec128\tc_dec256\tc_arr\n";
        // the rows of the tables: {ts, n}, n = -1 for a NULL row
        private final ArrayList<long[]> rows = new ArrayList<>();

        // tab and ref with rows n = 1..rowCount at ts = n, written by one INSERT each
        static EveryTypeRows createTables(int rowCount) throws Exception {
            final EveryTypeRows rows = new EveryTypeRows();
            for (String tableName : new String[]{"tab", "ref"}) {
                execute("CREATE TABLE " + tableName + " (" + COLUMNS + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                if (rowCount > 0) {
                    final StringBuilder insert = new StringBuilder("INSERT INTO ").append(tableName).append(" VALUES ");
                    for (int n = 1; n <= rowCount; n++) {
                        insert.append(n > 1 ? ", " : "").append(valuesOf(n, n));
                    }
                    execute(insert);
                }
            }
            for (int n = 1; n <= rowCount; n++) {
                rows.rows.add(new long[]{n, n});
            }
            return rows;
        }

        // puts the values of row n in every column but ts
        static void put(TableWriter.Row row, int n, DirectArray array, TestRecord.ArrayBinarySequence binary) {
            row.putBool(1, n % 2 == 1);
            row.putByte(2, (byte) n);
            row.putShort(3, (short) (n * 100));
            row.putChar(4, charOf(n));
            row.putInt(5, n * 1000);
            row.putLong(6, n * 100_000L);
            row.putFloat(7, n + 0.5f);
            row.putDouble(8, n + 0.25);
            row.putDate(9, n * 86_400_000L);
            row.putTimestamp(10, n);
            row.putTimestamp(11, n);
            row.putSym(12, "s" + n);
            row.putSym(13, "b" + n);
            row.putSym(14, "p" + n);
            row.putStr(15, "str" + n);
            row.putVarchar(16, new Utf8String("v" + n));
            row.putVarchar(17, new Utf8String(longVarcharOf(n)));
            row.putBin(18, binary.of(new byte[]{(byte) n, (byte) (n + 1)}));
            row.putUuid(19, uuidOf(n));
            row.putLong256(20, long256Of(n));
            row.putIPv4(21, (10 << 24) | n);
            row.putGeoStr(22, geohashOf(n, 1));
            row.putGeoStr(23, geohashOf(n, 2));
            row.putGeoStr(24, geohashOf(n, 4));
            row.putGeoStr(25, geohashOf(n, 8));
            row.putDecimalStr(26, (n % 10) + ".5");
            row.putDecimalStr(27, n + ".25");
            row.putDecimalStr(28, n + ".125");
            row.putDecimalStr(29, n + ".0625");
            row.putDecimalStr(30, n + "000000000000000000000.03125");
            row.putDecimalStr(31, n + "0000000000000000000000000000000000000000000000000.015625");
            array.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
            array.setDimLen(0, 2);
            array.applyShape();
            array.putDouble(0, n);
            array.putDouble(1, n + 0.5);
            row.putArray(32, array);
        }

        // appends row n to tab through the writer, and writes the same row to ref
        void append(TableWriter writer, long timestamp, int n, DirectArray array, TestRecord.ArrayBinarySequence binary) throws Exception {
            final TableWriter.Row row = writer.newRow(timestamp);
            put(row, n, array, binary);
            row.append();
            execute("INSERT INTO ref VALUES " + valuesOf(timestamp, n));
            rows.add(new long[]{timestamp, n});
        }

        // appends a row without values to tab through the writer, and writes the same row to ref
        void appendNull(TableWriter writer, long timestamp) throws Exception {
            writer.newRow(timestamp).append();
            execute("INSERT INTO ref (ts) VALUES (" + timestamp + "::TIMESTAMP)");
            rows.add(new long[]{timestamp, -1});
        }

        // every row as SELECT * prints it, in timestamp order
        String expected() {
            final ArrayList<long[]> sorted = new ArrayList<>(rows);
            sorted.sort(Comparator.comparingLong(row -> row[0]));
            final StringBuilder expected = new StringBuilder(HEADER);
            for (long[] row : sorted) {
                expected.append(printedRowOf(row[0], (int) row[1]));
            }
            return expected.toString();
        }

        // writes row n at the timestamp to tab and ref by INSERT
        void insert(long timestamp, int n) throws Exception {
            execute("INSERT INTO tab VALUES " + valuesOf(timestamp, n));
            execute("INSERT INTO ref VALUES " + valuesOf(timestamp, n));
            rows.add(new long[]{timestamp, n});
        }

        // the timestamp of row n
        long timestampOf(int n) {
            for (long[] row : rows) {
                if (row[1] == n) {
                    return row[0];
                }
            }
            throw new AssertionError("no row " + n);
        }

        private static String binaryOf(int n) {
            return String.format("00000000 %02x %02x", n, n + 1);
        }

        private static char charOf(int n) {
            return (char) ('A' + n % 26);
        }

        private static String geohashOf(int n, int length) {
            final StringBuilder geohash = new StringBuilder();
            for (int i = 0; i < length; i++) {
                geohash.append(GEOHASH_CHARS.charAt((n + i) % GEOHASH_CHARS.length()));
            }
            return geohash.toString();
        }

        private static String long256Of(int n) {
            return String.format("0x%02x", n);
        }

        private static String longVarcharOf(int n) {
            return "a varchar longer than inlined " + n;
        }

        // the row as SELECT prints it; n = -1 for a NULL row
        private static String printedRowOf(long timestamp, int n) {
            if (n < 0) {
                // BOOLEAN, BYTE and SHORT have no NULL; c_date to c_dec256 print a NULL as an empty string
                return micros(timestamp) + "\tfalse\t0\t0\t\tnull\tnull\tnull\tnull" + "\t".repeat(23) + "\tnull\n";
            }
            return micros(timestamp)
                    + '\t' + (n % 2 == 1)
                    + '\t' + n
                    + '\t' + n * 100
                    + '\t' + charOf(n)
                    + '\t' + n * 1000
                    + '\t' + n * 100_000L
                    + '\t' + n + ".5"
                    + '\t' + n + ".25"
                    + '\t' + MILLIS_FORMAT.format(Instant.ofEpochMilli(n * 86_400_000L))
                    + '\t' + micros(n)
                    + '\t' + NANOS_FORMAT.format(Instant.EPOCH.plus(n, ChronoUnit.NANOS))
                    + "\ts" + n
                    + "\tb" + n
                    + "\tp" + n
                    + "\tstr" + n
                    + "\tv" + n
                    + '\t' + longVarcharOf(n)
                    + '\t' + binaryOf(n)
                    + '\t' + uuidOf(n)
                    + '\t' + long256Of(n)
                    + "\t10.0.0." + n
                    + '\t' + geohashOf(n, 1)
                    + '\t' + geohashOf(n, 2)
                    + '\t' + geohashOf(n, 4)
                    + '\t' + geohashOf(n, 8)
                    + '\t' + (n % 10) + ".5"
                    + '\t' + n + ".25"
                    + '\t' + n + ".125"
                    + '\t' + n + ".0625"
                    + '\t' + n + "000000000000000000000.03125"
                    + '\t' + n + "0000000000000000000000000000000000000000000000000.015625"
                    + "\t[" + n + ".0," + n + ".5]\n";
        }

        private static String uuidOf(int n) {
            return String.format("00000000-0000-0000-0000-%012d", n);
        }

        // the SQL tuple of row n at the timestamp
        private static String valuesOf(long timestamp, int n) {
            return "(" + timestamp + "::TIMESTAMP"
                    + ", " + (n % 2 == 1)
                    + ", " + n + "::BYTE"
                    + ", " + n * 100 + "::SHORT"
                    + ", '" + charOf(n) + "'"
                    + ", " + n * 1000
                    + ", " + n * 100_000L
                    + ", " + n + ".5::FLOAT"
                    + ", " + n + ".25"
                    + ", " + n * 86_400_000L + "::DATE"
                    + ", " + n + "::TIMESTAMP"
                    + ", " + n + "::TIMESTAMP_NS"
                    + ", 's" + n + "'"
                    + ", 'b" + n + "'"
                    + ", 'p" + n + "'"
                    + ", 'str" + n + "'"
                    + ", 'v" + n + "'"
                    + ", '" + longVarcharOf(n) + "'"
                    + ", from_base64('" + Base64.getEncoder().encodeToString(new byte[]{(byte) n, (byte) (n + 1)}) + "')"
                    + ", '" + uuidOf(n) + "'"
                    + ", '" + long256Of(n) + "'"
                    + ", '10.0.0." + n + "'"
                    + ", #" + geohashOf(n, 1)
                    + ", #" + geohashOf(n, 2)
                    + ", #" + geohashOf(n, 4)
                    + ", #" + geohashOf(n, 8)
                    + ", '" + (n % 10) + ".5'::DECIMAL(2, 1)"
                    + ", '" + n + ".25'::DECIMAL(4, 2)"
                    + ", '" + n + ".125'::DECIMAL(9, 3)"
                    + ", '" + n + ".0625'::DECIMAL(18, 4)"
                    + ", '" + n + "000000000000000000000.03125'::DECIMAL(38, 5)"
                    + ", '" + n + "0000000000000000000000000000000000000000000000000.015625'::DECIMAL(76, 6)"
                    + ", ARRAY[" + n + ".0, " + n + ".5])";
        }
    }

    /**
     * The rows of a table (ts, x LONG), or (ts, x LONG, s STRING) with s = 's' || x: in timestamp order and, for equal
     * timestamps, in the order of x, a NULL x first, as ORDER BY ts, x returns them.
     */
    private static class Rows {
        private final boolean hasStringColumn;
        private final ArrayList<long[]> rows = new ArrayList<>();
        private final String timestampType;

        Rows(String timestampType, boolean hasStringColumn) {
            this.timestampType = timestampType;
            this.hasStringColumn = hasStringColumn;
        }

        @Override
        public String toString() {
            final ArrayList<long[]> sorted = new ArrayList<>(rows);
            sorted.sort(Comparator.<long[]>comparingLong(row -> row[0]).thenComparingLong(row -> row[1]));
            final StringBuilder sb = new StringBuilder(hasStringColumn ? "ts\tx\ts\n" : "ts\tx\n");
            for (long[] row : sorted) {
                sb.append("TIMESTAMP".equals(timestampType) ? micros(row[0]) : NANOS_FORMAT.format(Instant.EPOCH.plus(row[0], ChronoUnit.NANOS)));
                sb.append('\t').append(row[1] == Numbers.LONG_NULL ? "null" : Long.toString(row[1]));
                if (hasStringColumn) {
                    sb.append("\ts").append(row[1]);
                }
                sb.append('\n');
            }
            return sb.toString();
        }

        void add(long timestamp, long x) {
            rows.add(new long[]{timestamp, x});
        }

        int size() {
            return rows.size();
        }

        long sum() {
            long sum = 0;
            for (long[] row : rows) {
                if (row[1] != Numbers.LONG_NULL) {
                    sum += row[1];
                }
            }
            return sum;
        }
    }
}
