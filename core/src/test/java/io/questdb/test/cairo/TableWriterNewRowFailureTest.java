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
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A row whose start fails inside {@link TableWriter#newRow(long)} before the writer counts it: the first
 * out-of-order row of a transaction that runs out of memory for its O3 buffers, and a partition switch that
 * fails. The failed row must not stay half-started: the next row, commit, rollback or cancel must leave the
 * committed rows as they are. A failed out-of-order start leaves the writer usable; a failed partition switch
 * distresses it. After a failed partition switch, the tests send the writer rows of the new partition only.
 */
public class TableWriterNewRowFailureTest extends AbstractCairoTest {
    private static final long DAY = 86_400_000_000L;
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").withZone(ZoneOffset.UTC);
    // The type of column s of the tables of createStringTables(), which picks STRING or VARCHAR at random. The first
    // out-of-order row of a transaction allocates the O3 buffer of a STRING column in o3OpenColumns(), and none for a
    // VARCHAR column before the O3 buffer of the designated timestamp.
    private String stringColumnType;

    @Test
    public void testO3StartFailureAfterAbandonedRow() throws Exception {
        // newRow() cancels a row its caller abandoned; that row stays cancelled when the next row fails to start
        assertMemoryLeak(() -> {
            final StringRows rows = createStringTables();
            try (TableWriter writer = getWriter("tab")) {
                final TableWriter.Row abandoned = writer.newRow(11);
                putString(abandoned, "s11");
                abandoned.putLong(2, 11);
                failO3Start(writer, 5);
                appendStringRow(writer, rows, 12, 12);
                writer.commit();
            }
            assertStringTable(rows);
        });
    }

    @Test
    public void testO3StartFailureArrayColumn() throws Exception {
        // an array column allocates no O3 buffer before the designated timestamp does
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, a DOUBLE[], x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE ref (ts TIMESTAMP, a DOUBLE[], x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO tab SELECT x::TIMESTAMP, ARRAY[x::DOUBLE, x + 0.5], x FROM long_sequence(10)");
            execute("INSERT INTO ref SELECT x::TIMESTAMP, ARRAY[x::DOUBLE, x + 0.5], x FROM long_sequence(10)");
            try (
                    TableWriter writer = getWriter("tab");
                    DirectArray array = new DirectArray()
            ) {
                failO3Start(writer, 5);
                array.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
                array.setDimLen(0, 2);
                array.applyShape();
                array.putDouble(0, 11.0);
                array.putDouble(1, 11.5);
                final TableWriter.Row row = writer.newRow(11);
                row.putArray(1, array);
                row.putLong(2, 11);
                row.append();
                writer.commit();
            }
            execute("INSERT INTO ref VALUES (11::TIMESTAMP, ARRAY[11.0, 11.5], 11)");

            final StringBuilder expected = new StringBuilder("ts\ta\tx\n");
            for (int x = 1; x <= 11; x++) {
                expected.append(timestampOf(x)).append("\t[").append(x).append(".0,").append(x).append(".5]\t").append(x).append('\n');
            }
            assertQuery("tab")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns(expected);
            assertSqlCursors("ref", "tab");
        });
    }

    @Test
    public void testO3StartFailureBinaryColumn() throws Exception {
        // the O3 buffer of a BINARY column is the one that cannot be allocated
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, b BINARY, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE ref (ts TIMESTAMP, b BINARY, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // bytes 1, 2, 3
            execute("INSERT INTO tab SELECT x::TIMESTAMP, from_base64('AQID'), x FROM long_sequence(10)");
            execute("INSERT INTO ref SELECT x::TIMESTAMP, from_base64('AQID'), x FROM long_sequence(10)");
            final long bytes = Unsafe.malloc(3, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.putByte(bytes, (byte) 4);
                Unsafe.putByte(bytes + 1, (byte) 5);
                Unsafe.putByte(bytes + 2, (byte) 6);
                try (TableWriter writer = getWriter("tab")) {
                    failO3Start(writer, 5);
                    final TableWriter.Row row = writer.newRow(11);
                    row.putBin(1, bytes, 3);
                    row.putLong(2, 11);
                    row.append();
                    writer.commit();
                }
            } finally {
                Unsafe.free(bytes, 3, MemoryTag.NATIVE_DEFAULT);
            }
            execute("INSERT INTO ref VALUES (11::TIMESTAMP, from_base64('BAUG'), 11)");

            final String b123 = Base64.getEncoder().encodeToString(new byte[]{1, 2, 3});
            final String b456 = Base64.getEncoder().encodeToString(new byte[]{4, 5, 6});
            final StringBuilder expected = new StringBuilder("ts\tb\tx\n");
            for (int x = 1; x <= 10; x++) {
                expected.append(timestampOf(x)).append('\t').append(b123).append('\t').append(x).append('\n');
            }
            expected.append(timestampOf(11)).append('\t').append(b456).append('\t').append(11).append('\n');
            assertQuery("SELECT ts, base64(b, 100) b, x FROM tab")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns(expected);
            assertSqlCursors("ref", "tab");
        });
    }

    @Test
    public void testO3StartFailureFailsTwice() throws Exception {
        assertMemoryLeak(() -> {
            final StringRows rows = createStringTables();
            try (TableWriter writer = getWriter("tab")) {
                failO3Start(writer, 5);
                failO3Start(writer, 5);
                appendStringRow(writer, rows, 11, 11);
                writer.commit();
            }
            assertStringTable(rows);
        });
    }

    @Test
    public void testO3StartFailureFixedColumns() throws Exception {
        // no column of the table allocates an O3 buffer before the designated timestamp does
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE ref (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO tab SELECT x::TIMESTAMP, x FROM long_sequence(2048)");
            execute("INSERT INTO ref SELECT x::TIMESTAMP, x FROM long_sequence(2048)");
            try (TableWriter writer = getWriter("tab")) {
                failO3Start(writer, 1024);
                TableWriter.Row row = writer.newRow(2050);
                row.putLong(1, 2050);
                row.append();
                writer.commit();
                execute("INSERT INTO ref VALUES (2050::TIMESTAMP, 2050)");
                assertQuery("SELECT count(), sum(x) FROM tab")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("count\tsum\n2049\t" + (2048L * 2049 / 2 + 2050) + "\n");
                assertQuery("SELECT * FROM tab WHERE x >= 2047")
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns("ts\tx\n"
                                + timestampOf(2047) + "\t2047\n"
                                + timestampOf(2048) + "\t2048\n"
                                + timestampOf(2050) + "\t2050\n");
                assertSqlCursors("ref", "tab");

                // the row of the next transaction holds its own value
                row = writer.newRow(2051);
                row.putLong(1, 2051);
                row.append();
                writer.commit();
            }
            execute("INSERT INTO ref VALUES (2051::TIMESTAMP, 2051)");
            assertQuery("SELECT * FROM tab WHERE x >= 2048")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\tx\n"
                            + timestampOf(2048) + "\t2048\n"
                            + timestampOf(2050) + "\t2050\n"
                            + timestampOf(2051) + "\t2051\n");
            assertSqlCursors("ref", "tab");
        });
    }

    @Test
    public void testO3StartFailureLastPartitionSingleRow() throws Exception {
        // cancelling the only row of the last partition removes the partition, so a cancel of the failed row
        // would delete the committed row of 1970-01-02
        assertMemoryLeak(() -> {
            final StringRows rows = createStringTables();
            insertStringRow(rows, DAY + 1, 11);
            try (TableWriter writer = getWriter("tab")) {
                failO3Start(writer, 5);
                appendStringRow(writer, rows, DAY + 2, 12);
                writer.commit();
            }
            assertStringTable(rows);
        });
    }

    @Test
    public void testO3StartFailureMidTransaction() throws Exception {
        // the uncommitted rows appended before the failure are kept
        assertMemoryLeak(() -> {
            final StringRows rows = createStringTables();
            try (TableWriter writer = getWriter("tab")) {
                appendStringRow(writer, rows, 11, 11);
                appendStringRow(writer, rows, 12, 12);
                appendStringRow(writer, rows, 13, 13);
                failO3Start(writer, 5);
                Assert.assertEquals(3, writer.getUncommittedRowCount());
                writer.commit();
            }
            assertStringTable(rows);
        });
    }

    @Test
    public void testO3StartFailurePre1970() throws Exception {
        // a row before 1970 is out of order too, and newRow() refuses it before it starts the row
        assertMemoryLeak(() -> {
            final StringRows rows = createStringTables();
            try (TableWriter writer = getWriter("tab")) {
                failO3Start(writer, 5);
                try {
                    writer.newRow(-DAY + 5);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "designated timestamp before 1970-01-01 is not allowed");
                }
                appendStringRow(writer, rows, 11, 11);
                writer.commit();
            }
            assertStringTable(rows);
        });
    }

    @Test
    public void testO3StartFailureReaderHeld() throws Exception {
        // the committed rows a reader holds do not change under it
        assertMemoryLeak(() -> {
            final StringRows rows = createStringTables();
            final String committed = rows.toString();
            try (TableReader reader = getReader("tab")) {
                try (TableWriter writer = getWriter("tab")) {
                    failO3Start(writer, 5);
                }
                insertStringRow(rows, 11, 11);
                TestUtils.assertReader(committed, reader, sink);
                reader.reload();
                TestUtils.assertReader(rows.toString(), reader, sink);
            }
            assertStringTable(rows);
        });
    }

    @Test
    public void testO3StartFailureReopen() throws Exception {
        assertMemoryLeak(() -> {
            final StringRows rows = createStringTables();
            try (TableWriter writer = getWriter("tab")) {
                failO3Start(writer, 5);
                writer.commit();
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            assertStringTable(rows);
            insertStringRow(rows, 11, 11);
            assertStringTable(rows);
        });
    }

    @Test
    public void testO3StartFailureSingleRowTable() throws Exception {
        // the table holds a single row, so a cancel of the failed row would remove its only partition
        assertMemoryLeak(() -> {
            final StringRows rows = createStringTables(1);
            try (TableWriter writer = getWriter("tab")) {
                failO3Start(writer, 0);
                appendStringRow(writer, rows, 2, 2);
                writer.commit();
            }
            assertStringTable(rows);
        });
    }

    @Test
    public void testO3StartFailureSqlInsert() throws Exception {
        // the writer the INSERT failed on goes back to the pool, and the next INSERT gets it
        assertMemoryLeak(() -> {
            final StringRows rows = createStringTables();
            final String sql = "INSERT INTO tab VALUES (5::TIMESTAMP, 's5', 5)";
            final TableWriter pooledWriter;
            try (TableWriter writer = getWriter("tab")) {
                pooledWriter = writer;
            }
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(sql, sqlExecutionContext);
                try (InsertOperation insert = cq.popInsertOperation()) {
                    try (InsertMethod method = insert.createMethod(sqlExecutionContext)) {
                        final long rssMemLimit = Unsafe.getRssMemLimit();
                        Unsafe.setRssMemLimit(Unsafe.getRssMemUsed());
                        try {
                            method.execute(sqlExecutionContext);
                            Assert.fail();
                        } catch (CairoException e) {
                            Assert.assertTrue(e.isOutOfMemory());
                            TestUtils.assertContains(e.getFlyweightMessage(), "global RSS memory limit exceeded");
                            // the position of the designated timestamp value
                            Assert.assertEquals(sql.indexOf("::"), e.getPosition());
                        } finally {
                            Unsafe.setRssMemLimit(rssMemLimit);
                        }
                    }
                    // the pool keeps the writer: its rollback found no row in progress
                    try (TableWriter writer = getWriter("tab")) {
                        Assert.assertSame(pooledWriter, writer);
                        Assert.assertFalse(writer.isDistressed());
                    }
                    insertStringRow(rows, 11, 11);
                    assertStringTable(rows);

                    // the same compiled INSERT, now with memory to spare
                    insert.execute(sqlExecutionContext);
                    rows.add(5, 5);
                    execute("INSERT INTO ref VALUES (5::TIMESTAMP, 's5', 5)");
                }
            }
            assertStringTable(rows);
        });
    }

    @Test
    public void testO3StartFailureStaleRowCancel() throws Exception {
        // a cancel through the Row of a row appended before the failure finds no row in progress
        assertMemoryLeak(() -> {
            final StringRows rows = createStringTables();
            try (TableWriter writer = getWriter("tab")) {
                final TableWriter.Row row = appendStringRow(writer, rows, 11, 11);
                failO3Start(writer, 5);
                row.cancel();
                writer.commit();
            }
            assertStringTable(rows);
        });
    }

    @Test
    public void testO3StartFailureThenAlter() throws Exception {
        // ALTER commits through the pooled writer the failure happened on
        assertMemoryLeak(() -> {
            final StringRows rows = createStringTables();
            try (TableWriter writer = getWriter("tab")) {
                failO3Start(writer, 5);
            }
            execute("ALTER TABLE tab ADD COLUMN y INT");
            execute("ALTER TABLE ref ADD COLUMN y INT");
            assertCountAndSum(rows);
            assertQuery("SELECT * FROM tab WHERE x >= 9")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\ts\tx\ty\n"
                            + timestampOf(9) + "\ts9\t9\tnull\n"
                            + timestampOf(10) + "\ts10\t10\tnull\n");
            assertSqlCursors("ref", "tab");
        });
    }

    @Test
    public void testO3StartFailureThenAppend() throws Exception {
        assertMemoryLeak(() -> {
            final StringRows rows = createStringTables();
            try (TableWriter writer = getWriter("tab")) {
                failO3Start(writer, 5);
                appendStringRow(writer, rows, 11, 11);
                writer.commit();
            }
            assertStringTable(rows);
        });
    }

    @Test
    public void testO3StartFailureThenCommit() throws Exception {
        assertMemoryLeak(() -> {
            final StringRows rows = createStringTables();
            try (TableWriter writer = getWriter("tab")) {
                failO3Start(writer, 5);
                writer.commit();
            }
            assertStringTable(rows);
        });
    }

    @Test
    public void testO3StartFailureThenO3Row() throws Exception {
        // the same out-of-order row again, now with memory to spare
        assertMemoryLeak(() -> {
            final StringRows rows = createStringTables();
            try (TableWriter writer = getWriter("tab")) {
                failO3Start(writer, 5);
                appendStringRow(writer, rows, 5, 5);
                writer.commit();
            }
            assertStringTable(rows);
        });
    }

    @Test
    public void testO3StartFailureThenRollback() throws Exception {
        assertMemoryLeak(() -> {
            final StringRows rows = createStringTables();
            try (TableWriter writer = getWriter("tab")) {
                failO3Start(writer, 5);
                writer.rollback();
                Assert.assertFalse(writer.isDistressed());
                assertStringTable(rows);
                appendStringRow(writer, rows, 11, 11);
                writer.commit();
            }
            assertStringTable(rows);
        });
    }

    @Test
    public void testO3StartFailureThenRollbackUncommittedRows() throws Exception {
        // rollback() distresses a writer that holds a row in progress; the failed row is none
        assertMemoryLeak(() -> {
            final StringRows rows = createStringTables();
            try (TableWriter writer = getWriter("tab")) {
                final TableWriter.Row row = writer.newRow(11);
                putString(row, "s11");
                row.putLong(2, 11);
                row.append();
                for (long x = 12; x <= 13; x++) {
                    final TableWriter.Row next = writer.newRow(x);
                    putString(next, "s" + x);
                    next.putLong(2, x);
                    next.append();
                }
                failO3Start(writer, 5);
                writer.rollback();
                Assert.assertFalse(writer.isDistressed());
                assertStringTable(rows);
                appendStringRow(writer, rows, 11, 11);
                writer.commit();
            }
            assertStringTable(rows);
        });
    }

    @Test
    public void testSwitchPartitionFailureStaleRowCancel() throws Exception {
        // a cancel through the Row of a row appended before the failed switch finds no row in progress
        final FailingFacade ff = new FailingFacade(FailingFacade.MKDIRS, "1970-01-02");
        assertMemoryLeak(ff, () -> {
            createLongTable();
            try (TableWriter writer = getWriter("tab")) {
                final TableWriter.Row row = writer.newRow(11);
                row.putLong(1, 11);
                row.append();
                failSwitch(writer, ff, "cannot create directory");
                row.cancel();
                // the next row of the writer that ILP keeps after a failed row
                final TableWriter.Row next = writer.newRow(DAY + 2);
                next.putLong(1, 12);
                next.append();
                assertDistressedCommit(writer);
            }
            assertLongTableAfterFailedSwitch();
        });
    }

    @Test
    public void testSwitchPartitionFailureThenInsert() throws Exception {
        // the pool closes the writer that the failed switch distressed, and the next INSERT opens a new one
        final FailingFacade ff = new FailingFacade(FailingFacade.MKDIRS, "1970-01-02");
        assertMemoryLeak(ff, () -> {
            createLongTable();
            ff.isArmed.set(true);
            assertExceptionNoLeakCheck("INSERT INTO tab VALUES (" + (DAY + 1) + "::TIMESTAMP, 11)", 0, "cannot create directory");
            Assert.assertFalse(ff.isArmed.get());
            execute("INSERT INTO tab VALUES (" + (DAY + 1) + "::TIMESTAMP, 11)");
            execute("INSERT INTO ref VALUES (" + (DAY + 1) + "::TIMESTAMP, 11)");
            assertQuery("tab")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns(longRows(10) + timestampOf(DAY + 1) + "\t11\n");
            assertSqlCursors("ref", "tab");
        });
    }

    @Test
    public void testSwitchPartitionIndexFilesFailureThenNewRow() throws Exception {
        // the key file of the index cannot be created in the new partition: ts is open in 1970-01-02 by then, the
        // symbol and x columns are still open in 1970-01-01
        final FailingFacade ff = new FailingFacade(FailingFacade.OPEN_RW, "1970-01-02", "sym.k");
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, sym SYMBOL INDEX, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE ref (ts TIMESTAMP, sym SYMBOL INDEX, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            final String insert = " SELECT x::TIMESTAMP, CASE WHEN x % 2 = 0 THEN 'b' ELSE 'a' END, x FROM long_sequence(10)";
            execute("INSERT INTO tab" + insert);
            execute("INSERT INTO ref" + insert);
            try (TableWriter writer = getWriter("tab")) {
                failSwitch(writer, ff, "could not open read-write");
                final TableWriter.Row row = writer.newRow(DAY + 2);
                // a symbol value that the table has, so the symbol table does not change
                row.putSym(1, "b");
                row.putLong(2, 12);
                row.append();
                assertDistressedCommit(writer);
            }
            final StringBuilder expected = new StringBuilder("ts\tsym\tx\n");
            for (int x = 1; x <= 10; x++) {
                expected.append(timestampOf(x)).append('\t').append(x % 2 == 0 ? 'b' : 'a').append('\t').append(x).append('\n');
            }
            assertQuery("tab")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns(expected);
            assertQuery("SELECT x FROM tab WHERE sym = 'a'")
                    .noLeakCheck()
                    .returns("x\n1\n3\n5\n7\n9\n");
            assertSqlCursors("ref", "tab");

            // the partition 1970-01-02 takes rows now
            execute("INSERT INTO tab VALUES (11::TIMESTAMP, 'a', 11), (" + (DAY + 2) + "::TIMESTAMP, 'b', 12)");
            execute("INSERT INTO ref VALUES (11::TIMESTAMP, 'a', 11), (" + (DAY + 2) + "::TIMESTAMP, 'b', 12)");
            assertQuery("SELECT x FROM tab WHERE sym = 'a'")
                    .noLeakCheck()
                    .returns("x\n1\n3\n5\n7\n9\n11\n");
            assertQuery("SELECT x FROM tab WHERE sym = 'b'")
                    .noLeakCheck()
                    .returns("x\n2\n4\n6\n8\n10\n12\n");
            assertSqlCursors("ref", "tab");
        });
    }

    @Test
    public void testSwitchPartitionIndexSealFailureThenNewRow() throws Exception {
        // The switch seals the index of 1970-01-01, which has a generation per commit, before it switches txWriter,
        // and the seal cannot create its value file. The partition bounds of the writer stay those of 1970-01-01,
        // so the next row switches again, and the switch closes the column files of 1970-01-01 at their append
        // offsets. The last committed row is the first of the second page of x.d.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        setProperty(PropertyKey.CAIRO_POSTING_SEAL_GEN_THRESHOLD, 1);
        final FailingFacade ff = new FailingFacade(FailingFacade.OPEN_RW, "1970-01-01", "sym.pv.");
        assertMemoryLeak(ff, () -> {
            final long n = Files.PAGE_SIZE / Long.BYTES + 1;
            execute("CREATE TABLE tab (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE ref (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            final String firstHalf = " SELECT x::TIMESTAMP, CASE WHEN x % 2 = 0 THEN 'b' ELSE 'a' END, x FROM long_sequence(" + n / 2 + ")";
            final String secondHalf = " SELECT (x + " + n / 2 + ")::TIMESTAMP, CASE WHEN (x + " + n / 2 + ") % 2 = 0 THEN 'b' ELSE 'a' END, x + " + n / 2 + " FROM long_sequence(" + (n - n / 2) + ")";
            execute("INSERT INTO tab" + firstHalf);
            execute("INSERT INTO tab" + secondHalf);
            execute("INSERT INTO ref" + firstHalf);
            execute("INSERT INTO ref" + secondHalf);
            try (TableWriter writer = getWriter("tab")) {
                ff.isArmed.set(true);
                try {
                    writer.newRow(DAY + 1);
                    Assert.fail();
                } catch (CairoError e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not open read-write");
                }
                Assert.assertFalse(ff.isArmed.get());
                Assert.assertTrue(writer.isDistressed());
                // the next row of the writer that ILP keeps after a failed row
                final TableWriter.Row row = writer.newRow(DAY + 2);
                row.putSym(1, "b");
                row.putLong(2, n + 1);
                row.append();
                assertDistressedCommit(writer);
            }
            assertQuery("SELECT count(), sum(x), min(x), max(x), min(ts), max(ts) FROM tab")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tsum\tmin\tmax\tmin1\tmax1\n"
                            + n + "\t" + n * (n + 1) / 2 + "\t1\t" + n + "\t" + timestampOf(1) + "\t" + timestampOf(n) + "\n");
            assertQuery("SELECT * FROM tab WHERE x >= " + (n - 1))
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\tsym\tx\n"
                            + timestampOf(n - 1) + "\t" + ((n - 1) % 2 == 0 ? 'b' : 'a') + "\t" + (n - 1) + "\n"
                            + timestampOf(n) + "\t" + (n % 2 == 0 ? 'b' : 'a') + "\t" + n + "\n");
            assertSqlCursors("ref", "tab");
            assertSqlCursors("SELECT * FROM ref WHERE sym = 'a'", "SELECT * FROM tab WHERE sym = 'a'");
        });
    }

    @Test
    public void testSwitchPartitionListenerFailureDistresses() throws Exception {
        // a failure that none of the steps of the switch expects, after txWriter has switched partitions; the
        // partition bounds of the writer stay those of 1970-01-01, so the next row switches again
        assertMemoryLeak(() -> {
            createLongTable();
            try (TableWriter writer = getWriter("tab")) {
                final AtomicBoolean isArmed = new AtomicBoolean(true);
                writer.setExtensionListener(timestamp -> {
                    if (isArmed.compareAndSet(true, false)) {
                        throw new IllegalStateException("listener failure");
                    }
                });
                try {
                    writer.newRow(DAY + 1);
                    Assert.fail();
                } catch (IllegalStateException e) {
                    Assert.assertEquals("listener failure", e.getMessage());
                }
                Assert.assertTrue(writer.isDistressed());
                // the next row of the writer that ILP keeps after a failed row
                final TableWriter.Row row = writer.newRow(DAY + 2);
                row.putLong(1, 12);
                row.append();
                assertDistressedCommit(writer);
            }
            assertLongTableAfterFailedSwitch();
        });
    }

    @Test
    public void testSwitchPartitionMkdirsFailureThenNewRow() throws Exception {
        final FailingFacade ff = new FailingFacade(FailingFacade.MKDIRS, "1970-01-02");
        assertMemoryLeak(ff, () -> {
            createLongTable();
            try (TableWriter writer = getWriter("tab")) {
                failSwitch(writer, ff, "cannot create directory");
                // the next row of the writer that ILP keeps after a failed row
                final TableWriter.Row row = writer.newRow(DAY + 2);
                row.putLong(1, 12);
                row.append();
                assertDistressedCommit(writer);
            }
            assertLongTableAfterFailedSwitch();
        });
    }

    private static void assertDistressedCommit(TableWriter writer) {
        Assert.assertTrue(writer.isDistressed());
        try {
            writer.commit();
            Assert.fail();
        } catch (CairoError e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "Table 'tab' is distressed");
        }
    }

    // tab and ref with rows x = 1..10 at ts = x in 1970-01-01, written by INSERT
    private static void createLongTable() throws Exception {
        execute("CREATE TABLE tab (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("CREATE TABLE ref (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO tab SELECT x::TIMESTAMP, x FROM long_sequence(10)");
        execute("INSERT INTO ref SELECT x::TIMESTAMP, x FROM long_sequence(10)");
    }

    // newRow() of the first out-of-order row of the transaction with no native memory to spare: o3OpenColumns() cannot
    // allocate the O3 buffer of a STRING or BINARY column, or, in a table without one, o3TimestampSetter() cannot
    // allocate the O3 buffer of the designated timestamp. Each test fails the first out-of-order row of its
    // transaction.
    private static void failO3Start(TableWriter writer, long timestamp) {
        final long uncommittedRowCount = writer.getUncommittedRowCount();
        final long rssMemLimit = Unsafe.getRssMemLimit();
        Unsafe.setRssMemLimit(Unsafe.getRssMemUsed());
        try {
            writer.newRow(timestamp);
            Assert.fail();
        } catch (CairoException e) {
            Assert.assertTrue(e.isOutOfMemory());
            TestUtils.assertContains(e.getFlyweightMessage(), "global RSS memory limit exceeded");
        } finally {
            Unsafe.setRssMemLimit(rssMemLimit);
        }
        Assert.assertFalse(writer.isDistressed());
        Assert.assertFalse(writer.hasO3());
        Assert.assertEquals(uncommittedRowCount, writer.getUncommittedRowCount());
    }

    // the first row of 1970-01-02 cannot switch the writer to the partition
    private static void failSwitch(TableWriter writer, FailingFacade ff, String message) {
        ff.isArmed.set(true);
        try {
            writer.newRow(DAY + 1);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), message);
        }
        Assert.assertFalse(ff.isArmed.get());
        Assert.assertTrue(writer.isDistressed());
    }

    private static void insertStringRow(StringRows rows, long timestamp, long x) throws Exception {
        execute("INSERT INTO tab VALUES (" + timestamp + "::TIMESTAMP, 's" + x + "', " + x + ")");
        execute("INSERT INTO ref VALUES (" + timestamp + "::TIMESTAMP, 's" + x + "', " + x + ")");
        rows.add(timestamp, x);
    }

    // rows x = 1..n at ts = x of a table (ts TIMESTAMP, x LONG), with the header
    private static String longRows(int n) {
        final StringBuilder sb = new StringBuilder("ts\tx\n");
        for (int x = 1; x <= n; x++) {
            sb.append(timestampOf(x)).append('\t').append(x).append('\n');
        }
        return sb.toString();
    }

    private static String timestampOf(long micros) {
        return TIMESTAMP_FORMAT.format(Instant.EPOCH.plus(micros, ChronoUnit.MICROS));
    }

    // appends a row to tab through the writer, and writes the same row to ref
    private TableWriter.Row appendStringRow(TableWriter writer, StringRows rows, long timestamp, long x) throws Exception {
        final TableWriter.Row row = writer.newRow(timestamp);
        putString(row, "s" + x);
        row.putLong(2, x);
        row.append();
        execute("INSERT INTO ref VALUES (" + timestamp + "::TIMESTAMP, 's" + x + "', " + x + ")");
        rows.add(timestamp, x);
        return row;
    }

    private void assertCountAndSum(StringRows rows) throws Exception {
        assertQuery("SELECT count(), sum(x) FROM tab")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\tsum\n" + rows.size() + "\t" + rows.sum() + "\n");
    }

    // the table after the pool closed the writer the failed switch distressed: the 10 committed rows, and the rows
    // of both days that the next writer appends
    private void assertLongTableAfterFailedSwitch() throws Exception {
        assertQuery("tab")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(longRows(10));
        assertSqlCursors("ref", "tab");

        execute("INSERT INTO tab VALUES (11::TIMESTAMP, 11), (" + (DAY + 2) + "::TIMESTAMP, 12)");
        execute("INSERT INTO ref VALUES (11::TIMESTAMP, 11), (" + (DAY + 2) + "::TIMESTAMP, 12)");
        assertQuery("tab")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(longRows(11) + timestampOf(DAY + 2) + "\t12\n");
        assertSqlCursors("ref", "tab");
    }

    private void assertStringTable(StringRows rows) throws Exception {
        assertCountAndSum(rows);
        assertQuery("tab")
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(rows.toString());
        assertSqlCursors("ref", "tab");
    }

    private StringRows createStringTables() throws Exception {
        return createStringTables(10);
    }

    // tab and ref with rows ts = x, s = 's' || x for x = 1..n, written by INSERT; s is STRING or VARCHAR at random
    private StringRows createStringTables(int n) throws Exception {
        stringColumnType = TestUtils.generateRandom(LOG).nextBoolean() ? "STRING" : "VARCHAR";
        LOG.info().$("column s is ").$(stringColumnType).$();
        execute("CREATE TABLE tab (ts TIMESTAMP, s " + stringColumnType + ", x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("CREATE TABLE ref (ts TIMESTAMP, s " + stringColumnType + ", x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO tab SELECT x::TIMESTAMP, 's' || x, x FROM long_sequence(" + n + ")");
        execute("INSERT INTO ref SELECT x::TIMESTAMP, 's' || x, x FROM long_sequence(" + n + ")");
        final StringRows rows = new StringRows();
        for (long x = 1; x <= n; x++) {
            rows.add(x, x);
        }
        return rows;
    }

    // puts the value of column s
    private void putString(TableWriter.Row row, String value) {
        if ("VARCHAR".equals(stringColumnType)) {
            row.putVarchar(1, new Utf8String(value));
        } else {
            row.putStr(1, value);
        }
    }

    /**
     * Fails, once when armed, one kind of file operation on a path of table tab that contains all the given parts.
     */
    private static class FailingFacade extends TestFilesFacadeImpl {
        static final int MKDIRS = 0;
        static final int OPEN_RW = 1;
        final AtomicBoolean isArmed = new AtomicBoolean();
        private final int operation;
        private final String[] pathParts;

        FailingFacade(int operation, String... pathParts) {
            this.operation = operation;
            this.pathParts = pathParts;
        }

        @Override
        public int mkdirs(Path path, int mode) {
            if (operation == MKDIRS && matches(path) && isArmed.compareAndSet(true, false)) {
                return -1;
            }
            return super.mkdirs(path, mode);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            if (operation == OPEN_RW && matches(name) && isArmed.compareAndSet(true, false)) {
                return -1;
            }
            return super.openRW(name, opts);
        }

        private boolean matches(Utf8Sequence path) {
            if (!Utf8s.containsAscii(path, "tab~")) {
                return false;
            }
            for (String part : pathParts) {
                if (!Utf8s.containsAscii(path, part)) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * The rows of a table (ts TIMESTAMP, s STRING or VARCHAR, x LONG): x at ts with s = 's' || x, in timestamp order.
     */
    private static class StringRows {
        private final ArrayList<long[]> rows = new ArrayList<>();

        void add(long timestamp, long x) {
            rows.add(new long[]{timestamp, x});
        }

        long sum() {
            long sum = 0;
            for (long[] row : rows) {
                sum += row[1];
            }
            return sum;
        }

        int size() {
            return rows.size();
        }

        @Override
        public String toString() {
            final ArrayList<long[]> sorted = new ArrayList<>(rows);
            sorted.sort(Comparator.comparingLong(row -> row[0]));
            final StringBuilder sb = new StringBuilder("ts\ts\tx\n");
            for (long[] row : sorted) {
                sb.append(timestampOf(row[0])).append("\ts").append(row[1]).append('\t').append(row[1]).append('\n');
            }
            return sb.toString();
        }
    }
}
