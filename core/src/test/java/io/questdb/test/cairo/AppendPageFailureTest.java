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
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.engine.TestBinarySequence;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongFunction;

/**
 * A column file that cannot grow when the append memory needs its next page, as on a full disk. The write fails,
 * and the next writes must neither crash nor lose the rows committed before it. Each test writes the rows that
 * must land to a table ref as well, which sees no failure, and compares the two.
 */
public class AppendPageFailureTest extends AbstractCairoTest {
    // the columns of the table of testInsertAfterFailedPutPage, with the SQL that gives each a value in row x
    private static final String[][] ALL_TYPES = {
            {"v_boolean", "BOOLEAN", "x % 2 = 0"},
            {"v_byte", "BYTE", "(x % 100)::BYTE"},
            {"v_geo1", "GEOHASH(1c)", "'u'::GEOHASH(1c)"},
            {"v_dec8", "DECIMAL(2, 0)", "(x % 99)::DECIMAL(2, 0)"},
            {"v_short", "SHORT", "(x % 30_000)::SHORT"},
            {"v_char", "CHAR", "'q'::CHAR"},
            {"v_geo2", "GEOHASH(2c)", "'u3'::GEOHASH(2c)"},
            {"v_dec16", "DECIMAL(4, 1)", "(x % 999)::DECIMAL(4, 1)"},
            {"v_int", "INT", "x::INT"},
            {"v_float", "FLOAT", "(x / 4.0)::FLOAT"},
            {"v_ipv4", "IPv4", "'10.0.0.1'::IPv4"},
            {"v_symbol", "SYMBOL", "'sym' || (x % 5)"},
            {"v_geo4", "GEOHASH(4c)", "'u33d'::GEOHASH(4c)"},
            {"v_dec32", "DECIMAL(9, 2)", "x::DECIMAL(9, 2)"},
            {"v_date", "DATE", "(x * 1000)::DATE"},
            {"v_long", "LONG", "x * 3"},
            {"v_double", "DOUBLE", "x / 8.0"},
            {"v_timestamp", "TIMESTAMP", "(x * 1_000_000)::TIMESTAMP"},
            {"v_timestamp_ns", "TIMESTAMP_NS", "(x * 1000)::TIMESTAMP_NS"},
            {"v_geo8", "GEOHASH(8c)", "'u33dc0cp'::GEOHASH(8c)"},
            {"v_dec64", "DECIMAL(18, 3)", "x::DECIMAL(18, 3)"},
            {"v_uuid", "UUID", "'11111111-2222-3333-4444-555555555555'::UUID"},
            {"v_dec128", "DECIMAL(38, 4)", "x::DECIMAL(38, 4)"},
            {"v_long256", "LONG256", "'0x5a5a'::LONG256"},
            {"v_dec256", "DECIMAL(76, 5)", "x::DECIMAL(76, 5)"},
            // 4 + 2 * 48 bytes
            {"v_string", "STRING", "'" + "s".repeat(48) + "'"},
            // 100 bytes, not inlined in the aux entry
            {"v_varchar", "VARCHAR", "'" + "v".repeat(100) + "'::VARCHAR"},
            // 8 + 92 bytes: base64 of 92 bytes 0x41
            {"v_binary", "BINARY", "from_base64('" + "QUFB".repeat(30) + "QUE=')"},
            // shape and padding 8 + 12 doubles
            {"v_array", "DOUBLE[]", "ARRAY[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0]"},
    };
    // where the committed rows end a data file in testNoTimestampFailedRowEveryColumnFile: at the end of its first
    // page, after values of 100 bytes and one of the rest
    private static final long DATA_PAGE_END = -2;
    // rows of 8 bytes that fill the first page of a column file
    private static final long ROWS_PER_PAGE = Files.PAGE_SIZE / Long.BYTES;
    // U+1F600, 4 bytes in UTF-8, 2 chars in UTF-16
    private static final String SURROGATE_PAIR = "\uD83D\uDE00";
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").withZone(ZoneOffset.UTC);
    private static final String[] VAR_SIZE_TYPES = {"STRING", "VARCHAR", "BINARY", "DOUBLE[]"};

    @Test
    public void testAlterAfterFailedTimestampPage() throws Exception {
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            createTableWithFullPage();
            failInsert(ff, ROWS_PER_PAGE + 1);

            // ALTER commits through the pooled writer the INSERT failed on
            execute("ALTER TABLE tab ADD COLUMN y INT");
            execute("ALTER TABLE ref ADD COLUMN y INT");
            assertRows(ROWS_PER_PAGE);
        });
    }

    @Test
    public void testColumnTopAfterFailedPutPage() throws Exception {
        // a VARCHAR column added after the table had rows: the value of the failed row crosses the end of the
        // first page of its data file, which holds the rows below the column top
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "y.d");
        assertMemoryLeak(ff, () -> {
            createTables(10);
            execute("ALTER TABLE tab ADD COLUMN y VARCHAR");
            execute("ALTER TABLE ref ADD COLUMN y VARCHAR");
            final String y = "'" + "y".repeat(100) + "'";
            final long n = Files.PAGE_SIZE / 100;
            final String insert = " SELECT (x + 10)::TIMESTAMP, x + 10, " + y + " FROM long_sequence(" + n + ")";
            execute("INSERT INTO tab" + insert);
            execute("INSERT INTO ref" + insert);
            ff.isArmed.set(true);
            assertExceptionNoLeakCheck("INSERT INTO tab VALUES (" + (n + 11) + "::TIMESTAMP, " + (n + 11) + ", " + y + ")", -1, "No space left");
            Assert.assertFalse(ff.isArmed.get());
            // longer than a VARCHAR that its aux entry holds inline: the value goes to y.d
            final String z = "z".repeat(50);
            final String next = " VALUES (" + (n + 12) + "::TIMESTAMP, " + (n + 12) + ", '" + z + "')";
            execute("INSERT INTO tab" + next);
            execute("INSERT INTO ref" + next);

            assertQuery("SELECT count(), count(y), sum(x) FROM tab")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tcount1\tsum\n" + (n + 11) + "\t" + (n + 1) + "\t" + ((n + 10) * (n + 11) / 2 + n + 12) + "\n");
            assertQuery("SELECT x, y FROM tab WHERE x IN (10, 11, " + (n + 10) + ", " + (n + 12) + ")")
                    .noLeakCheck()
                    .returns("x\ty\n"
                            + "10\t\n"
                            + "11\t" + "y".repeat(100) + "\n"
                            + (n + 10) + "\t" + "y".repeat(100) + "\n"
                            + (n + 12) + "\t" + z + "\n");
            assertSqlCursors("ref", "tab");
        });
    }

    @Test
    public void testCommitAfterFailedTimestampPage() throws Exception {
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            createTableWithFullPage();
            try (TableWriter writer = getWriter("tab")) {
                failNewRow(writer, ff, ROWS_PER_PAGE + 1);
                writer.commit();
            }
            assertRows(ROWS_PER_PAGE);
        });
    }

    @Test
    public void testDesignatedTimestampNullRowFailedPage() throws Exception {
        // A table with a designated timestamp: newRow() writes the timestamp, so the cancel of a row with NULLs in its
        // other columns moves every column back to the end of the committed rows with setAppendPosition(). x's NULL
        // landed, s's NULL needed the next page of s.d.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "s.d");
        assertMemoryLeak(ff, () -> {
            final long n = Files.PAGE_SIZE / 16;
            execute("CREATE TABLE tab (ts TIMESTAMP, x LONG, s STRING) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE ref (ts TIMESTAMP, x LONG, s STRING) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            final String rows = " SELECT x::TIMESTAMP, x, 'abcdef' FROM long_sequence(" + n + ")";
            execute("INSERT INTO tab" + rows);
            execute("INSERT INTO ref" + rows);
            ff.isArmed.set(true);
            assertExceptionNoLeakCheck("INSERT INTO tab (ts) VALUES (" + (n + 1) + "::TIMESTAMP)", -1, "No space left");
            Assert.assertFalse(ff.isArmed.get());
            final String next = " VALUES (" + (n + 2) + "::TIMESTAMP, " + (n + 2) + ", 'abcdef'), (" + (n + 3) + "::TIMESTAMP, NULL, NULL)";
            execute("INSERT INTO tab" + next);
            execute("INSERT INTO ref" + next);
            assertQuery("SELECT ts, x, s FROM tab WHERE ts >= " + (n - 1) + "::TIMESTAMP")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\tx\ts\n"
                            + timestampOf(n - 1) + '\t' + (n - 1) + "\tabcdef\n"
                            + timestampOf(n) + '\t' + n + "\tabcdef\n"
                            + timestampOf(n + 2) + '\t' + (n + 2) + "\tabcdef\n"
                            + timestampOf(n + 3) + "\tnull\t\n");
            assertSqlCursors("ref", "tab");
        });
    }

    @Test
    public void testEarlierRowAfterFailedTimestampPage() throws Exception {
        // The failed row leaves the max timestamp at the last committed row, so the next row, earlier than the failed
        // one and later than the committed ones, is in order: PARTITION BY NONE, which refuses rows out of order,
        // takes it, and the commit stores its timestamp as the max timestamp of the table.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        for (String partitionBy : new String[]{"NONE", "DAY"}) {
            final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
            assertMemoryLeak(ff, () -> {
                createTables(ROWS_PER_PAGE, partitionBy);
                failInsert(ff, ROWS_PER_PAGE + 100);
                insertRow(ROWS_PER_PAGE + 50);
                assertRows(ROWS_PER_PAGE, ROWS_PER_PAGE + 50);
                try (TableReader reader = getReader("tab")) {
                    Assert.assertEquals(partitionBy, ROWS_PER_PAGE + 50, reader.getMaxTimestamp());
                }
                // the max of the designated timestamp: the first row of a backward scan of the table
                assertQuery("SELECT max(ts) FROM tab")
                        .noLeakCheck()
                        .timestampDesc("max")
                        .expectSize()
                        .returns("max\n" + timestampOf(ROWS_PER_PAGE + 50) + "\n");
                execute("DROP TABLE tab");
                execute("DROP TABLE ref");
            });
        }
    }

    @Test
    public void testEmptyTableFirstRowFailedPage() throws Exception {
        // the first row of the table opens its partition, and the first page of ts.d cannot be allocated
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            createTables(0);
            ff.isArmed.set(true);
            assertExceptionNoLeakCheck("INSERT INTO tab VALUES (1::TIMESTAMP, 1)", -1, "No space left");
            Assert.assertFalse(ff.isArmed.get());
            insertRow(2);
            assertQuery("tab")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\n" + timestampOf(2) + "\t2\n");
            assertSqlCursors("ref", "tab");
        });
    }

    @Test
    public void testFailedTimestampPageMidTransaction() throws Exception {
        // the rows of the transaction appended before the failed row are kept
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            createTables(ROWS_PER_PAGE - 3);
            try (TableWriter writer = getWriter("tab")) {
                for (long x = ROWS_PER_PAGE - 2; x <= ROWS_PER_PAGE; x++) {
                    appendRow(writer, x);
                }
                failNewRow(writer, ff, ROWS_PER_PAGE + 1);
                Assert.assertEquals(3, writer.getUncommittedRowCount());
                writer.commit();
            }
            assertRows(ROWS_PER_PAGE);
        });
    }

    @Test
    public void testFailedTimestampPageSecondPage() throws Exception {
        // ts.d fills its third page; the row that needs the fourth fails
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            final long n = 3 * ROWS_PER_PAGE;
            createTables(n);
            failInsert(ff, n + 1);
            insertRow(n + 2);
            assertRows(n, n + 2);
        });
    }

    @Test
    public void testFailedTimestampPageTwice() throws Exception {
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            createTableWithFullPage();
            try (TableWriter writer = getWriter("tab")) {
                failNewRow(writer, ff, ROWS_PER_PAGE + 1);
                failNewRow(writer, ff, ROWS_PER_PAGE + 1);
                appendRow(writer, ROWS_PER_PAGE + 2);
                writer.commit();
            }
            assertRows(ROWS_PER_PAGE, ROWS_PER_PAGE + 2);
        });
    }

    @Test
    public void testIndexedSymbolAfterFailedTimestampPage() throws Exception {
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, sym SYMBOL INDEX, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE ref (ts TIMESTAMP, sym SYMBOL INDEX, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            final String insert = " SELECT x::TIMESTAMP, CASE WHEN x % 2 = 0 THEN 'b' ELSE 'a' END, x FROM long_sequence(" + ROWS_PER_PAGE + ")";
            execute("INSERT INTO tab" + insert);
            execute("INSERT INTO ref" + insert);
            ff.isArmed.set(true);
            assertExceptionNoLeakCheck("INSERT INTO tab VALUES (" + (ROWS_PER_PAGE + 1) + "::TIMESTAMP, 'a', -1)", -1, "No space left");
            Assert.assertFalse(ff.isArmed.get());
            final long x = ROWS_PER_PAGE + 2;
            final String next = " VALUES (" + x + "::TIMESTAMP, 'b', " + x + ")";
            execute("INSERT INTO tab" + next);
            execute("INSERT INTO ref" + next);

            final String scan = "SELECT * FROM tab WHERE sym = 'b' AND x >= " + (ROWS_PER_PAGE - 2);
            assertQuery(scan)
                    .noLeakCheck()
                    .assertsPlanContaining("Index forward scan on: sym");
            assertQuery(scan)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\tsym\tx\n"
                            + timestampOf(ROWS_PER_PAGE - 2) + "\tb\t" + (ROWS_PER_PAGE - 2) + "\n"
                            + timestampOf(ROWS_PER_PAGE) + "\tb\t" + ROWS_PER_PAGE + "\n"
                            + timestampOf(x) + "\tb\t" + x + "\n");
            assertQuery("SELECT count() FROM tab WHERE sym = 'a'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n" + ROWS_PER_PAGE / 2 + "\n");
            assertSqlCursors("ref", "tab");
            assertSqlCursors("SELECT * FROM ref WHERE sym = 'b'", "SELECT * FROM tab WHERE sym = 'b'");
        });
    }

    @Test
    public void testInsertAfterFailedPutPage() throws Exception {
        // For each column file of a table of every column type: the committed rows end at the end of the first page
        // of the file, or the value of the next row crosses it, and the next row cannot map the next page. The
        // INSERT fails; InsertRowImpl cancels the row, which moves every column back to the end of the committed
        // rows. The next rows, with and without values, land after the committed rows.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final long pageSize = Files.PAGE_SIZE;
        final Object[][] files = {
                // file, committed rows
                {"v_boolean.d", pageSize},
                {"v_byte.d", pageSize},
                {"v_geo1.d", pageSize},
                {"v_dec8.d", pageSize},
                {"v_short.d", pageSize / 2},
                {"v_char.d", pageSize / 2},
                {"v_geo2.d", pageSize / 2},
                {"v_dec16.d", pageSize / 2},
                {"v_int.d", pageSize / 4},
                {"v_float.d", pageSize / 4},
                {"v_ipv4.d", pageSize / 4},
                {"v_symbol.d", pageSize / 4},
                {"v_geo4.d", pageSize / 4},
                {"v_dec32.d", pageSize / 4},
                {"v_date.d", pageSize / 8},
                {"v_long.d", pageSize / 8},
                {"v_double.d", pageSize / 8},
                {"v_timestamp.d", pageSize / 8},
                {"v_timestamp_ns.d", pageSize / 8},
                {"v_geo8.d", pageSize / 8},
                {"v_dec64.d", pageSize / 8},
                {"v_uuid.d", pageSize / 16},
                {"v_dec128.d", pageSize / 16},
                {"v_long256.d", pageSize / 32},
                {"v_dec256.d", pageSize / 32},
                // values of 100 bytes: the next one crosses the end of the page
                {"v_string.d", pageSize / 100},
                // an offset of 8 bytes per row after the first one of 8
                {"v_string.i", pageSize / 8 - 1},
                {"v_varchar.d", pageSize / 100},
                // 16 bytes per row
                {"v_varchar.i", pageSize / 16},
                {"v_binary.d", pageSize / 100},
                {"v_binary.i", pageSize / 8 - 1},
                // values of 104 bytes
                {"v_array.d", pageSize / 104},
                {"v_array.i", pageSize / 16},
        };
        final StringBuilder columns = new StringBuilder("ts TIMESTAMP, x LONG");
        final StringBuilder values = new StringBuilder("x::TIMESTAMP, x");
        for (String[] type : ALL_TYPES) {
            columns.append(", ").append(type[0]).append(' ').append(type[1]);
            values.append(", ").append(type[2]);
        }
        for (Object[] file : files) {
            final String fileName = (String) file[0];
            final long n = (Long) file[1];
            final AllocateFailingFacade ff = new AllocateFailingFacade("tab", fileName);
            assertMemoryLeak(ff, () -> {
                execute("CREATE TABLE tab (" + columns + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("CREATE TABLE ref (" + columns + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("INSERT INTO tab SELECT " + values + " FROM long_sequence(" + n + ")");
                execute("INSERT INTO ref SELECT " + values + " FROM long_sequence(" + n + ")");
                ff.isArmed.set(true);
                try {
                    execute("INSERT INTO tab VALUES (" + values.toString().replaceAll("\\bx\\b", Long.toString(n + 1)) + ")");
                    Assert.fail(fileName);
                } catch (CairoException e) {
                    TestUtils.assertContains(fileName, e.getFlyweightMessage(), "No space left");
                }
                Assert.assertFalse(fileName, ff.isArmed.get());
                // a row with a value in every column, and a row of NULLs
                final String next = " VALUES (" + values.toString().replaceAll("\\bx\\b", Long.toString(n + 2)) + ")";
                final String nulls = " (ts, x) VALUES (" + (n + 3) + "::TIMESTAMP, " + (n + 3) + ")";
                execute("INSERT INTO tab" + next);
                execute("INSERT INTO ref" + next);
                execute("INSERT INTO tab" + nulls);
                execute("INSERT INTO ref" + nulls);

                assertQuery("SELECT count(), sum(x) FROM tab")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("count\tsum\n" + (n + 2) + "\t" + (n * (n + 1) / 2 + n + 2 + n + 3) + "\n");
                assertQuery("SELECT ts, x FROM tab WHERE x >= " + (n - 1))
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns("ts\tx\n"
                                + timestampOf(n - 1) + "\t" + (n - 1) + "\n"
                                + timestampOf(n) + "\t" + n + "\n"
                                + timestampOf(n + 2) + "\t" + (n + 2) + "\n"
                                + timestampOf(n + 3) + "\t" + (n + 3) + "\n");
                assertSqlCursors("ref", "tab");
                execute("DROP TABLE tab");
                execute("DROP TABLE ref");
            });
        }
    }

    @Test
    public void testInsertAfterFailedTimestampPage() throws Exception {
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            createTableWithFullPage();
            failInsert(ff, ROWS_PER_PAGE + 1);
            insertRow(ROWS_PER_PAGE + 2);
            assertRows(ROWS_PER_PAGE, ROWS_PER_PAGE + 2);
        });
    }

    @Test
    public void testInsertAfterFailedTimestampPageDefaultPageSize() throws Exception {
        // the server default page size of 16 MiB
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, 16 * 1024 * 1024);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            final long n = 16 * 1024 * 1024 / Long.BYTES;
            Assert.assertEquals(2_097_152, n);
            createTables(n);
            failInsert(ff, n + 1);
            insertRow(n + 2);
            assertRows(n, n + 2);
        });
    }

    @Test
    public void testInsertAfterFailedTimestampPageNonPartitioned() throws Exception {
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY NONE BYPASS WAL");
            execute("CREATE TABLE ref (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY NONE BYPASS WAL");
            execute("INSERT INTO tab SELECT x::TIMESTAMP, x FROM long_sequence(" + ROWS_PER_PAGE + ")");
            execute("INSERT INTO ref SELECT x::TIMESTAMP, x FROM long_sequence(" + ROWS_PER_PAGE + ")");
            failInsert(ff, ROWS_PER_PAGE + 1);
            insertRow(ROWS_PER_PAGE + 2);
            assertRows(ROWS_PER_PAGE, ROWS_PER_PAGE + 2);
        });
    }

    @Test
    public void testInsertAfterFailedTimestampPageTimestampNs() throws Exception {
        // a designated TIMESTAMP_NS column: rows of 8 bytes as well, values in nanoseconds
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE tab (ts TIMESTAMP_NS, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE ref (ts TIMESTAMP_NS, x LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO tab SELECT x::TIMESTAMP_NS, x FROM long_sequence(" + ROWS_PER_PAGE + ")");
            execute("INSERT INTO ref SELECT x::TIMESTAMP_NS, x FROM long_sequence(" + ROWS_PER_PAGE + ")");
            ff.isArmed.set(true);
            assertExceptionNoLeakCheck("INSERT INTO tab VALUES (" + (ROWS_PER_PAGE + 1) + "::TIMESTAMP_NS, -1)", -1, "No space left");
            Assert.assertFalse(ff.isArmed.get());
            final long x = ROWS_PER_PAGE + 2;
            execute("INSERT INTO tab VALUES (" + x + "::TIMESTAMP_NS, " + x + ")");
            execute("INSERT INTO ref VALUES (" + x + "::TIMESTAMP_NS, " + x + ")");
            assertQuery("SELECT ts, x FROM tab WHERE x >= " + (ROWS_PER_PAGE - 1))
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\tx\n"
                            + timestampNsOf(ROWS_PER_PAGE - 1) + "\t" + (ROWS_PER_PAGE - 1) + "\n"
                            + timestampNsOf(ROWS_PER_PAGE) + "\t" + ROWS_PER_PAGE + "\n"
                            + timestampNsOf(x) + "\t" + x + "\n");
            assertQuery("SELECT count(), sum(x) FROM tab")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tsum\n" + (ROWS_PER_PAGE + 1) + "\t" + (ROWS_PER_PAGE * (ROWS_PER_PAGE + 1) / 2 + x) + "\n");
            assertSqlCursors("ref", "tab");
        });
    }

    @Test
    public void testInsertAsSelectAfterFailedTimestampPage() throws Exception {
        // the first of the selected rows fails, and the INSERT commits none of them
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            createTableWithFullPage();
            final String select = " SELECT (x + " + ROWS_PER_PAGE + ")::TIMESTAMP, x + " + ROWS_PER_PAGE + " FROM long_sequence(5)";
            ff.isArmed.set(true);
            assertExceptionNoLeakCheck("INSERT INTO tab" + select, -1, "No space left");
            Assert.assertFalse(ff.isArmed.get());
            assertRows(ROWS_PER_PAGE);
            execute("INSERT INTO tab" + select);
            execute("INSERT INTO ref" + select);
            assertRows(ROWS_PER_PAGE, ROWS_PER_PAGE + 1, ROWS_PER_PAGE + 2, ROWS_PER_PAGE + 3, ROWS_PER_PAGE + 4, ROWS_PER_PAGE + 5);
        });
    }

    @Test
    public void testNewRowAfterFailedTimestampPage() throws Exception {
        // ILP and the other writers that keep the writer after a failed row
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            createTableWithFullPage();
            try (TableWriter writer = getWriter("tab")) {
                failNewRow(writer, ff, ROWS_PER_PAGE + 1);
                appendRow(writer, ROWS_PER_PAGE + 2);
                writer.commit();
            }
            assertRows(ROWS_PER_PAGE, ROWS_PER_PAGE + 2);
        });
    }

    @Test
    public void testNoTimestampAlterUpdateTruncateAfterFailedRow() throws Exception {
        // ALTER, UPDATE and TRUNCATE run on the pooled writer of a table without a designated timestamp, after its
        // all-NULL row failed: x's NULL landed, and s's NULL needed the next page of s.d
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("t", "s.d");
        assertMemoryLeak(ff, () -> {
            final long n = createStringPageTables();
            ff.isArmed.set(true);
            failInsert("INSERT INTO t VALUES (NULL, NULL)");
            Assert.assertFalse(ff.isArmed.get());

            execute("ALTER TABLE t ADD COLUMN y INT");
            execute("ALTER TABLE ref ADD COLUMN y INT");
            executeOnBoth("INSERT INTO %t VALUES (" + (n + 2) + ", 'abcdef', 1), (" + (n + 3) + ", NULL, NULL)");
            final String committed = (n - 1) + "\tabcdef\tnull\n" + n + "\tabcdef\tnull\n";
            assertQuery("SELECT x, s, y FROM t WHERE x >= " + (n - 1))
                    .noLeakCheck()
                    .returns("x\ts\ty\n" + committed + (n + 2) + "\tabcdef\t1\n" + (n + 3) + "\t\tnull\n");
            assertSqlCursors("ref", "t");

            executeOnBoth("UPDATE %t SET y = 7, s = 'upd' WHERE x = " + n);
            assertQuery("SELECT x, s, y FROM t WHERE x >= " + (n - 1))
                    .noLeakCheck()
                    .returns("x\ts\ty\n" + (n - 1) + "\tabcdef\tnull\n" + n + "\tupd\t7\n" + (n + 2) + "\tabcdef\t1\n" + (n + 3) + "\t\tnull\n");
            assertCountAndSum("t", n + 2, n * (n + 1) / 2 + n + 2 + n + 3);
            assertSqlCursors("ref", "t");

            execute("TRUNCATE TABLE t");
            execute("TRUNCATE TABLE ref");
            executeOnBoth("INSERT INTO %t VALUES (1, 'a', 2), (NULL, NULL, NULL)");
            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\ts\ty\n1\ta\t2\nnull\t\tnull\n");
            assertSqlCursors("ref", "t");
        });
    }

    @Test
    public void testNoTimestampFailedBinaryPutDefaultPageSize() throws Exception {
        // the server default page size of 16 MiB: the BINARY value, the first value of the row, crosses the end of
        // the first page of b.d
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, 16 * 1024 * 1024);
        final AllocateFailingFacade ff = new AllocateFailingFacade("t", "b.d");
        assertMemoryLeak(ff, () -> {
            // values of 8 + 92 bytes
            final long n = 16 * 1024 * 1024 / 100;
            Assert.assertEquals(167_772, n);
            final String a = sqlOf(binaryOf('A', 92));
            execute("CREATE TABLE t (b BINARY, x LONG)");
            execute("CREATE TABLE ref (b BINARY, x LONG)");
            executeOnBoth("INSERT INTO %t SELECT " + a + ", x FROM long_sequence(" + n + ")");
            ff.isArmed.set(true);
            failInsert("INSERT INTO t VALUES (" + sqlOf(binaryOf('B', 92)) + ", " + (n + 1) + ")");
            Assert.assertFalse(ff.isArmed.get());
            final byte[] c = binaryOf('C', 92);
            final byte[] d = binaryOf('D', 3);
            executeOnBoth("INSERT INTO %t VALUES (" + sqlOf(c) + ", " + (n + 2) + "), (NULL, " + (n + 3) + "), (" + sqlOf(d) + ", " + (n + 4) + ")");

            assertQuery("SELECT x, length(b) len, base64(b, 32_768) b FROM t WHERE x >= " + (n - 1))
                    .noLeakCheck()
                    .returns("x\tlen\tb\n"
                            + (n - 1) + '\t' + textOf(binaryOf('A', 92)) + '\n'
                            + n + '\t' + textOf(binaryOf('A', 92)) + '\n'
                            + (n + 2) + '\t' + textOf(c) + '\n'
                            + (n + 3) + '\t' + textOf(null) + '\n'
                            + (n + 4) + '\t' + textOf(d) + '\n');
            assertCountAndSum("t", n + 3, n * (n + 1) / 2 + n + 2 + n + 3 + n + 4);
            assertSqlCursors("ref", "t");
        });
    }

    @Test
    public void testNoTimestampFailedNullRow() throws Exception {
        // A table without a designated timestamp gets a row of NULLs only, as NULL literals or as columns the INSERT
        // leaves out. append() writes the NULL of each column in column order, and the NULL of column f needs the
        // next page of one of its files, which cannot be allocated. The columns before f hold their NULL already,
        // f may hold part of its NULL, and no column is marked; the cancel must move every one of them back to the
        // row start. The next rows, with values, NULLs, and a value in f alone, land after the committed rows.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final long pageSize = Files.PAGE_SIZE;
        final NullRowColumn[][] before = {
                {NullRowColumn.LONG},
                {NullRowColumn.VARCHAR_INLINED},
                {NullRowColumn.VARCHAR},
                {NullRowColumn.DOUBLE_ARRAY},
                {NullRowColumn.STRING},
                {NullRowColumn.BINARY},
                {NullRowColumn.SYMBOL_INDEX},
                {NullRowColumn.LONG, NullRowColumn.STRING},
        };
        final NullRowColumn fString = new NullRowColumn("STRING", "'abcdef'", x -> "abcdef", x -> "'g" + x + "'", x -> "g" + x, "", "%s");
        // NULL takes 8 bytes of f.d: a BINARY value that ends at the end of a page makes its put map the next page as
        // well, a NULL does not
        final NullRowColumn fBinary = new NullRowColumn("BINARY", "NULL::BINARY", x -> "", NullRowColumn.BINARY.literal(), NullRowColumn.BINARY.text(), "", "base64(%s, 100)");
        final NullRowColumn fLong = new NullRowColumn("LONG", "x * 11", x -> Long.toString(x * 11), x -> Long.toString(x * 11), x -> Long.toString(x * 11), "null", "%s");
        final NullRowColumn fVarchar = new NullRowColumn("VARCHAR", "('w' || x)::VARCHAR", x -> "w" + x, x -> "'w" + x + "'", x -> "w" + x, "", "%s");
        final NullRowColumn fArray = new NullRowColumn("DOUBLE[]", "ARRAY[x * 1.5]", x -> "[" + x * 1.5 + "]", x -> "ARRAY[" + x * 1.5 + "]", x -> "[" + x * 1.5 + "]", "null", "%s");
        final Object[][] failing = {
                // the NULL of f, its file, the committed rows, the value of f in the last of them (null: as in the others)
                {"STRING NULL at the start of a page", fString, "f.d", pageSize / 16, null, null},
                {"STRING NULL across the end of a page", fString, "f.d", pageSize / 16, "'abcde'", "abcde"},
                {"STRING NULL offset at the end of a page", new NullRowColumn("STRING", "'a'", x -> "a", fString.literal(), fString.text(), "", "%s"), "f.i", pageSize / 8 - 1, null, null},
                {"BINARY NULL at the start of a page", fBinary, "f.d", pageSize / 8, null, null},
                // 8 + 4 bytes in the last row end f.d 4 bytes before the end of the page
                {"BINARY NULL across the end of a page", fBinary, "f.d", pageSize / 8 - 1, "from_base64('AQIDBA==')", "AQIDBA=="},
                {"LONG NULL at the end of a page", fLong, "f.d", pageSize / 8, null, null},
                {"VARCHAR NULL at the end of a page of offsets", fVarchar, "f.i", pageSize / 16, null, null},
                {"DOUBLE[] NULL at the end of a page of offsets", fArray, "f.i", pageSize / 16, null, null},
        };
        for (NullRowColumn[] beforeColumns : before) {
            for (Object[] failingNull : failing) {
                for (boolean isNullLiterals : new boolean[]{true, false}) {
                    final String failingName = (String) failingNull[0];
                    final NullRowColumn fColumn = (NullRowColumn) failingNull[1];
                    final String failingFile = (String) failingNull[2];
                    final long n = (Long) failingNull[3];
                    final String lastLiteral = (String) failingNull[4];
                    final String lastText = (String) failingNull[5];
                    final StringBuilder columns = new StringBuilder();
                    final StringBuilder committedValues = new StringBuilder();
                    final StringBuilder lastValues = new StringBuilder();
                    final StringBuilder select = new StringBuilder();
                    final StringBuilder nullLiterals = new StringBuilder();
                    for (int i = 0; i < beforeColumns.length; i++) {
                        final String name = "b" + (i + 1);
                        columns.append(name).append(' ').append(beforeColumns[i].type()).append(", ");
                        committedValues.append(beforeColumns[i].committedValueOfX()).append(", ");
                        lastValues.append(beforeColumns[i].literal().apply(n)).append(", ");
                        select.append(String.format(beforeColumns[i].select(), name)).append(' ').append(name).append(", ");
                        nullLiterals.append("NULL, ");
                    }
                    columns.append("f ").append(fColumn.type()).append(", x LONG");
                    committedValues.append(fColumn.committedValueOfX()).append(", x");
                    lastValues.append(lastLiteral).append(", ").append(n);
                    select.append(String.format(fColumn.select(), "f")).append(" f, x");
                    nullLiterals.append("NULL, NULL");
                    final String context = failingName + " after " + columns + (isNullLiterals ? ", NULL literals" : ", columns left out");
                    final AllocateFailingFacade ff = new AllocateFailingFacade("t", failingFile);
                    assertMemoryLeak(ff, () -> assertCase(context, () -> {
                        execute("CREATE TABLE t (" + columns + ")");
                        execute("CREATE TABLE ref (" + columns + ")");
                        if (lastLiteral == null) {
                            executeOnBoth("INSERT INTO %t SELECT " + committedValues + " FROM long_sequence(" + n + ")");
                        } else {
                            executeOnBoth("INSERT INTO %t SELECT " + committedValues + " FROM long_sequence(" + (n - 1) + ")");
                            executeOnBoth("INSERT INTO %t VALUES (" + lastValues + ")");
                        }
                        ff.isArmed.set(true);
                        failInsert(context, isNullLiterals ? "INSERT INTO t VALUES (" + nullLiterals + ")" : "INSERT INTO t (x) VALUES (NULL)");
                        Assert.assertFalse(context, ff.isArmed.get());

                        // a value in every column, NULLs, and a value in f alone
                        final StringBuilder values = new StringBuilder();
                        for (NullRowColumn column : beforeColumns) {
                            values.append(column.literal().apply(n + 2)).append(", ");
                        }
                        values.append(fColumn.literal().apply(n + 2)).append(", ").append(n + 2);
                        executeOnBoth("INSERT INTO %t VALUES (" + values + ")");
                        executeOnBoth("INSERT INTO %t (x) VALUES (" + (n + 3) + ")");
                        executeOnBoth("INSERT INTO %t (f, x) VALUES (" + fColumn.literal().apply(n + 4) + ", " + (n + 4) + ")");

                        final StringBuilder expected = new StringBuilder();
                        for (int i = 0; i < beforeColumns.length; i++) {
                            expected.append("b").append(i + 1).append('\t');
                        }
                        expected.append("f\tx\n");
                        for (long x = n - 1; x <= n + 4; x++) {
                            if (x == n + 1) {
                                continue;
                            }
                            for (NullRowColumn column : beforeColumns) {
                                expected.append(x < n + 2 ? column.committedText().apply(x) : x == n + 2 ? column.text().apply(x) : column.nullText()).append('\t');
                            }
                            if (x == n && lastText != null) {
                                expected.append(lastText);
                            } else if (x <= n) {
                                expected.append(fColumn.committedText().apply(x));
                            } else {
                                expected.append(x == n + 3 ? fColumn.nullText() : fColumn.text().apply(x));
                            }
                            expected.append('\t').append(x).append('\n');
                        }
                        assertQuery("SELECT " + select + " FROM t WHERE x >= " + (n - 1))
                                .noLeakCheck()
                                .returns(expected);
                        assertCountAndSum("t", n + 3, n * (n + 1) / 2 + n + 2 + n + 3 + n + 4);
                        if (beforeColumns[0] == NullRowColumn.SYMBOL_INDEX) {
                            // the index of b1 holds the rows of the column as it is
                            final String scan = "SELECT x FROM t WHERE b1 = 'a' AND x >= " + (n - 1);
                            assertQuery(scan)
                                    .noLeakCheck()
                                    .assertsPlanContaining("Index forward scan on: b1");
                            final StringBuilder rows = new StringBuilder("x\n");
                            for (long x : new long[]{n - 1, n, n + 2}) {
                                if (x % 2 != 0) {
                                    rows.append(x).append('\n');
                                }
                            }
                            assertQuery(scan)
                                    .noLeakCheck()
                                    .returns(rows);
                        }
                        assertSqlCursors("ref", "t");
                        execute("DROP TABLE t");
                        execute("DROP TABLE ref");
                    }));
                }
            }
        }
    }

    @Test
    public void testNoTimestampFailedNullRowDefaultPageSize() throws Exception {
        // the server default page size of 16 MiB: STRING values of 1 KiB end s.d at the end of its first page, and
        // the NULL of s in an all-NULL row needs its next page after x's NULL landed
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, 16 * 1024 * 1024);
        final AllocateFailingFacade ff = new AllocateFailingFacade("t", "s.d");
        assertMemoryLeak(ff, () -> {
            // 4 + 2 * 510 = 1024 bytes a value
            final long n = 16 * 1024 * 1024 / 1024;
            Assert.assertEquals(16_384, n);
            final String s = "s".repeat(510);
            execute("CREATE TABLE t (x LONG, s STRING)");
            execute("CREATE TABLE ref (x LONG, s STRING)");
            executeOnBoth("INSERT INTO %t SELECT x, '" + s + "' FROM long_sequence(" + n + ")");
            ff.isArmed.set(true);
            failInsert("INSERT INTO t VALUES (NULL, NULL)");
            Assert.assertFalse(ff.isArmed.get());
            executeOnBoth("INSERT INTO %t VALUES (" + (n + 2) + ", 'abc'), (" + (n + 3) + ", NULL)");

            assertQuery("SELECT x, length(s) len FROM t WHERE x >= " + (n - 1) + " OR x IS NULL")
                    .noLeakCheck()
                    .returns("x\tlen\n"
                            + (n - 1) + "\t510\n"
                            + n + "\t510\n"
                            + (n + 2) + "\t3\n"
                            + (n + 3) + "\t-1\n");
            assertCountAndSum("t", n + 2, n * (n + 1) / 2 + n + 2 + n + 3);
            assertSqlCursors("ref", "t");
        });
    }

    @Test
    public void testNoTimestampFailedPutValues() throws Exception {
        // A table without a designated timestamp, its STRING or BINARY column first: the value of the column is the
        // first one the row writes, and its put fails where the column file needs its next page. No put of the row
        // returned, so the row marks no column; the cancel must still move the column back to where the row
        // started, whatever the put wrote: part of the value or of its length, nothing, the whole value and no
        // offset to it, or the whole value when the put maps the next page after its last byte. The next values
        // land after the committed rows and read back as written.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final long pageSize = Files.PAGE_SIZE;
        // 4 + 2 * 48 = 100 bytes in v.d
        final String failedString = "f".repeat(48);
        // 50 bytes before the end of the page: the length, 22 chars and the high half of the surrogate pair
        final String failedPair = "f".repeat(22) + SURROGATE_PAIR + "g".repeat(10);
        // 8 + 92 = 100 bytes in v.d
        final byte[] failedBinary = binaryOf('F', 92);
        final Object[][] cases = {
                // type, failing file, where the committed values end v.d (auxPageEnd(p): they end v.i at the end of
                // its page p instead), the failed value
                // the value crosses the end of the page
                {"STRING", "v.d", pageSize - 50, sqlOf(failedString)},
                // the end of the third page
                {"STRING", "v.d", 3 * pageSize - 50, sqlOf(failedString)},
                // the end of the page splits a surrogate pair of the value
                {"STRING", "v.d", pageSize - 50, sqlOf(failedPair)},
                // the length of the value crosses the end of the page
                {"STRING", "v.d", pageSize - 2, sqlOf(failedString)},
                // the value starts at the start of the next page
                {"STRING", "v.d", pageSize, sqlOf(failedString)},
                // the value lands, its offset needs the next page of v.i, the second or the third
                {"STRING", "v.i", auxPageEnd(1), sqlOf(failedString)},
                {"STRING", "v.i", auxPageEnd(2), sqlOf(failedString)},
                // a typed NULL crosses the end of the page, or starts at the start of the next one
                {"STRING", "v.d", pageSize - 2, "NULL::STRING"},
                {"STRING", "v.d", pageSize, "NULL::STRING"},
                {"BINARY", "v.d", pageSize - 50, sqlOf(failedBinary)},
                {"BINARY", "v.d", pageSize - 4, sqlOf(failedBinary)},
                // the end of the second page
                {"BINARY", "v.d", 2 * pageSize - 4, sqlOf(failedBinary)},
                {"BINARY", "v.d", pageSize, sqlOf(failedBinary)},
                {"BINARY", "v.i", auxPageEnd(1), sqlOf(failedBinary)},
                {"BINARY", "v.d", pageSize - 4, "NULL::BINARY"},
                {"BINARY", "v.d", pageSize, "NULL::BINARY"},
                // the value ends at the end of the page, and the put maps the next page after its last byte
                {"BINARY", "v.d", pageSize - 100, sqlOf(failedBinary)},
                // an empty value, whose length ends at the end of the page: the put maps the next page as well
                {"BINARY", "v.d", pageSize - Long.BYTES, sqlOf(new byte[0])},
        };
        for (Object[] c : cases) {
            final String type = (String) c[0];
            final boolean isBinary = "BINARY".equals(type);
            final String failingFile = (String) c[1];
            final long dataEnd = (Long) c[2];
            final String failedValue = (String) c[3];
            final AllocateFailingFacade ff = new AllocateFailingFacade("t", failingFile);
            final ObjList<Object> committed = dataEnd < 0 ? valuesEndingAuxPage(isBinary, (int) -dataEnd) : valuesEndingAt(isBinary, dataEnd);
            final long n = committed.size();
            final String context = type + ", " + failingFile + ", " + (dataEnd < 0 ? "aux page end " + -dataEnd : "data end " + dataEnd) + ", "
                    + (failedValue.length() > 40 ? failedValue.substring(0, 40) + "..." : failedValue);
            assertMemoryLeak(ff, () -> assertCase(context, () -> {
                createFirstValueTables(type, committed);
                ff.isArmed.set(true);
                failInsert(context, "INSERT INTO t VALUES (" + failedValue + ", " + (n + 1) + ")");
                Assert.assertFalse(context, ff.isArmed.get());

                // the failed row started where the committed values end v.d
                final long rowStart = dataSizeOf(committed, isBinary);
                final ObjList<Object> next = isBinary ? nextBinaryValues(rowStart) : nextStringValues(rowStart);
                insertFirstValues(next, n + 2);
                assertFirstValues(context, isBinary, committed, next);
                execute("DROP TABLE t");
                execute("DROP TABLE ref");
            }));
        }
    }

    @Test
    public void testNoTimestampFailedRowColumnTop() throws Exception {
        // STRING, VARCHAR and LONG columns added after the table had rows, and an all-NULL row whose NULL of cl needs
        // a page of cl.d that cannot be allocated, after every column before it wrote its NULL: the first row with the
        // new columns, which starts at their row 0 and needs the first page of cl.d, or a later row, which starts past
        // the column tops and needs the next page.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final long top = 10;
        for (boolean isFirstRow : new boolean[]{true, false}) {
            final AllocateFailingFacade ff = new AllocateFailingFacade("t", "cl.d");
            assertMemoryLeak(ff, () -> assertCase(isFirstRow ? "first row" : "later row", () -> {
                execute("CREATE TABLE t (x LONG, s0 STRING)");
                execute("CREATE TABLE ref (x LONG, s0 STRING)");
                executeOnBoth("INSERT INTO %t SELECT x, 's' || x FROM long_sequence(" + top + ")");
                for (String table : new String[]{"t", "ref"}) {
                    execute("ALTER TABLE " + table + " ADD COLUMN cs STRING");
                    execute("ALTER TABLE " + table + " ADD COLUMN cv VARCHAR");
                    execute("ALTER TABLE " + table + " ADD COLUMN cl LONG");
                }
                final String values = "x, 's' || x, 'c' || x, ('varchar-not-inlined-' || x)::VARCHAR, x * 3";
                long n = top;
                if (!isFirstRow) {
                    // the rows with cl end the first page of cl.d
                    final long rows = Files.PAGE_SIZE / Long.BYTES;
                    executeOnBoth("INSERT INTO %t SELECT " + values + " FROM (SELECT x + " + top + " x FROM long_sequence(" + rows + "))");
                    n += rows;
                }
                ff.isArmed.set(true);
                failInsert("INSERT INTO t VALUES (NULL, NULL, NULL, NULL, NULL)");
                Assert.assertFalse(ff.isArmed.get());
                executeOnBoth("INSERT INTO %t SELECT " + values + " FROM (SELECT x + " + (n + 1) + " x FROM long_sequence(2))");
                executeOnBoth("INSERT INTO %t (x) VALUES (" + (n + 4) + ")");

                final StringBuilder expected = new StringBuilder("x\ts0\tcs\tcv\tcl\n");
                for (long x = n - 1; x <= n + 4; x++) {
                    if (x == n + 1) {
                        continue;
                    }
                    if (x == n + 4) {
                        expected.append(x).append("\t\t\t\tnull\n");
                    } else if (x <= top) {
                        expected.append(x).append("\ts").append(x).append("\t\t\tnull\n");
                    } else {
                        expected.append(x).append("\ts").append(x).append("\tc").append(x).append("\tvarchar-not-inlined-").append(x).append('\t').append(x * 3).append('\n');
                    }
                }
                assertQuery("SELECT x, s0, cs, cv, cl FROM t WHERE x >= " + (n - 1) + " OR x IS NULL")
                        .noLeakCheck()
                        .returns(expected);
                final long withValues = n - top + 2;
                assertQuery("SELECT count(), count(cs), count(cv), count(cl), sum(x) FROM t")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("count\tcount1\tcount2\tcount3\tsum\n"
                                + (n + 3) + '\t' + withValues + '\t' + withValues + '\t' + withValues + '\t' + (n * (n + 1) / 2 + n + 2 + n + 3 + n + 4) + '\n');
                assertSqlCursors("ref", "t");
                execute("DROP TABLE t");
                execute("DROP TABLE ref");
            }));
        }
    }

    @Test
    public void testNoTimestampFailedRowDiskStaysFull() throws Exception {
        // The disk stays full after the failed row: every allocation of disk space fails. The cancel of a row that
        // marked no column maps no page that the column files lack, so each failed INSERT makes one allocation, the
        // one that failed, and the pooled writer stays as it was, not distressed: the rows that fit the pages the
        // files have land while the disk is full, and those that need a page fail the same way. Rows whose cancel
        // moves every column, those of a row that marked a column, stay out: when a column ends exactly at the end
        // of a page, that cancel maps, and allocates, the next page, which distresses the writer on a full disk (a
        // pre-existing bug, apart from this one).
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final long pageSize = Files.PAGE_SIZE;
        final String failedString = "f".repeat(48);
        final byte[] failedBinary = binaryOf('F', 92);
        // the first value of the row, STRING or BINARY, fails
        final Object[][] firstValues = {
                // type, where the committed values end v.d (auxPageEnd(1): they end v.i at the end of its first page),
                // the failed value, the values that fit the pages of the files while the disk is full
                // the value crosses the end of the page
                {"BINARY", pageSize - 50, failedBinary, new Object[]{null, binaryOf('s', 30)}},
                {"STRING", pageSize - 50, failedString, new Object[]{null, "ab"}},
                // the value lands, its offset needs the next page of v.i
                {"STRING", auxPageEnd(1), failedString, new Object[0]},
                {"BINARY", auxPageEnd(1), failedBinary, new Object[0]},
                // the value starts at the start of the next page: the put writes nothing
                {"BINARY", pageSize, failedBinary, new Object[0]},
                // the value ends at the end of the page, and the put maps the next page after its last byte
                {"BINARY", pageSize - 100, failedBinary, new Object[]{null, binaryOf('s', 41)}},
                // an empty value, whose length ends at the end of the page
                {"BINARY", pageSize - Long.BYTES, new byte[0], new Object[]{null}},
        };
        for (Object[] c : firstValues) {
            final String type = (String) c[0];
            final boolean isBinary = "BINARY".equals(type);
            final long dataEnd = (Long) c[1];
            final Object failedValue = c[2];
            final Object[] fitting = (Object[]) c[3];
            final FullDiskFacade ff = new FullDiskFacade();
            final ObjList<Object> committed = dataEnd < 0 ? valuesEndingAuxPage(isBinary, (int) -dataEnd) : valuesEndingAt(isBinary, dataEnd);
            final long n = committed.size();
            final String context = type + ", " + (dataEnd < 0 ? "aux page end " + -dataEnd : "data end " + dataEnd) + ", failed value of " + dataSizeOf(failedValue, isBinary) + " bytes";
            assertMemoryLeak(ff, () -> assertCase(context, () -> {
                createFirstValueTables(type, committed);
                final TableWriter writer = pooledWriter();
                ff.isFull.set(true);
                failInsert(context, "INSERT INTO t VALUES (" + sqlOf(failedValue) + ", " + (n + 1) + ")");
                Assert.assertEquals(context, 1, ff.failedAllocations.get());
                assertPooledWriter(context, writer);

                final ObjList<Object> landed = new ObjList<>();
                for (Object value : fitting) {
                    landed.add(value);
                }
                insertFirstValues(landed, n + 2);
                Assert.assertEquals(context, 1, ff.failedAllocations.get());
                failInsert(context, "INSERT INTO t VALUES (" + sqlOf(failedValue) + ", -1)");
                Assert.assertEquals(context, 2, ff.failedAllocations.get());
                assertPooledWriter(context, writer);

                ff.isFull.set(false);
                final long rowStart = dataSizeOf(committed, isBinary) + dataSizeOf(landed, isBinary);
                final ObjList<Object> next = isBinary ? nextBinaryValues(rowStart) : nextStringValues(rowStart);
                insertFirstValues(next, n + 2 + landed.size());
                landed.addAll(next);
                assertFirstValues(context, isBinary, committed, landed);
                execute("DROP TABLE t");
                execute("DROP TABLE ref");
            }));
        }

        // x first, at the end of the first page of x.d: its put fails and writes nothing
        final FullDiskFacade longFf = new FullDiskFacade();
        assertMemoryLeak(longFf, () -> assertCase("x first", () -> {
            final long n = pageSize / Long.BYTES;
            execute("CREATE TABLE t (x LONG, s STRING)");
            execute("CREATE TABLE ref (x LONG, s STRING)");
            executeOnBoth("INSERT INTO %t SELECT x, 's' || x FROM long_sequence(" + n + ")");
            final TableWriter writer = pooledWriter();
            longFf.isFull.set(true);
            for (int i = 1; i <= 2; i++) {
                failInsert("INSERT INTO t VALUES (" + (n + i) + ", 'u')");
                Assert.assertEquals(i, longFf.failedAllocations.get());
                assertPooledWriter("x.d", writer);
            }
            longFf.isFull.set(false);
            executeOnBoth("INSERT INTO %t VALUES (" + (n + 3) + ", 'v'), (" + (n + 4) + ", NULL)");
            assertQuery("SELECT x, s FROM t WHERE x >= " + (n - 1))
                    .noLeakCheck()
                    .returns("x\ts\n" + (n - 1) + "\ts" + (n - 1) + '\n' + n + "\ts" + n + '\n' + (n + 3) + "\tv\n" + (n + 4) + "\t\n");
            assertCountAndSum("t", n + 2, n * (n + 1) / 2 + n + 3 + n + 4);
            assertSqlCursors("ref", "t");
            execute("DROP TABLE t");
            execute("DROP TABLE ref");
        }));

        // all-NULL rows: x's NULL lands, s's NULL needs the next page of s.d, twice
        final FullDiskFacade nullFf = new FullDiskFacade();
        assertMemoryLeak(nullFf, () -> assertCase("all-NULL rows", () -> {
            final long n = createStringPageTables();
            final TableWriter writer = pooledWriter();
            nullFf.isFull.set(true);
            for (int i = 1; i <= 2; i++) {
                failInsert("INSERT INTO t VALUES (NULL, NULL)");
                Assert.assertEquals(i, nullFf.failedAllocations.get());
                assertPooledWriter("s.d", writer);
            }
            nullFf.isFull.set(false);
            assertStringPageTablesNextRows(n);
            execute("DROP TABLE t");
            execute("DROP TABLE ref");
        }));

        // all-NULL rows: s1's NULL takes the next page of s1.i, the last free space on the disk, and s's NULL needs
        // the next page of s.d; the cancel maps the page of s1.i before it back
        final FullDiskFacade auxFf = new FullDiskFacade("t", "s1.i");
        assertMemoryLeak(auxFf, () -> assertCase("all-NULL rows after s1's NULL took the next page of s1.i", () -> {
            final long n = createStringAuxPageTables();
            final TableWriter writer = pooledWriter();
            auxFf.isArmed.set(true);
            for (int i = 1; i <= 2; i++) {
                failInsert("INSERT INTO t VALUES (NULL, NULL, NULL)");
                Assert.assertTrue(auxFf.isFull.get());
                Assert.assertEquals(i, auxFf.failedAllocations.get());
                assertPooledWriter("s1.i", writer);
            }
            auxFf.isFull.set(false);
            assertStringAuxPageTablesNextRows(n);
        }));
    }

    @Test
    public void testNoTimestampFailedRowDroppedColumn() throws Exception {
        // a column dropped before the failed all-NULL row: the cancel leaves it, and moves back x, whose NULL landed
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("t", "s.d");
        assertMemoryLeak(ff, () -> {
            final long n = Files.PAGE_SIZE / 16;
            execute("CREATE TABLE t (x LONG, d LONG, s STRING)");
            execute("CREATE TABLE ref (x LONG, d LONG, s STRING)");
            executeOnBoth("INSERT INTO %t SELECT x, x * 5, 'abcdef' FROM long_sequence(" + n + ")");
            execute("ALTER TABLE t DROP COLUMN d");
            execute("ALTER TABLE ref DROP COLUMN d");
            ff.isArmed.set(true);
            failInsert("INSERT INTO t VALUES (NULL, NULL)");
            Assert.assertFalse(ff.isArmed.get());
            assertStringPageTablesNextRows(n);
        });
    }

    @Test
    public void testNoTimestampFailedRowEveryColumnFile() throws Exception {
        // For each column file of a table of every column type without a designated timestamp, a row that marks no
        // column fails where the file needs its next page: (a) in the put of the value of the column of the file,
        // the first value the INSERT lists, or (b) in the NULL of that column, in a row of NULLs only, after the
        // columns before it in the table wrote their NULL, with the columns of the table in one order and in the
        // reverse order. The cancel moves every column back to the row start: the next rows, with values and with
        // NULLs, land after the committed rows.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final long pageSize = Files.PAGE_SIZE;
        final Object[][] files = {
                // file, committed rows: the value of the next row needs the next page of the file; its NULL does
                // (DATA_PAGE_END: rows that end the data file at the end of its first page; -1: a NULL writes
                // nothing to the file)
                {"v_boolean.d", pageSize, pageSize},
                {"v_byte.d", pageSize, pageSize},
                {"v_geo1.d", pageSize, pageSize},
                {"v_dec8.d", pageSize, pageSize},
                {"v_short.d", pageSize / 2, pageSize / 2},
                {"v_char.d", pageSize / 2, pageSize / 2},
                {"v_geo2.d", pageSize / 2, pageSize / 2},
                {"v_dec16.d", pageSize / 2, pageSize / 2},
                {"v_int.d", pageSize / 4, pageSize / 4},
                {"v_float.d", pageSize / 4, pageSize / 4},
                {"v_ipv4.d", pageSize / 4, pageSize / 4},
                {"v_symbol.d", pageSize / 4, pageSize / 4},
                {"v_geo4.d", pageSize / 4, pageSize / 4},
                {"v_dec32.d", pageSize / 4, pageSize / 4},
                {"v_date.d", pageSize / 8, pageSize / 8},
                {"v_long.d", pageSize / 8, pageSize / 8},
                {"v_double.d", pageSize / 8, pageSize / 8},
                {"v_timestamp.d", pageSize / 8, pageSize / 8},
                {"v_timestamp_ns.d", pageSize / 8, pageSize / 8},
                {"v_geo8.d", pageSize / 8, pageSize / 8},
                {"v_dec64.d", pageSize / 8, pageSize / 8},
                {"v_uuid.d", pageSize / 16, pageSize / 16},
                {"v_dec128.d", pageSize / 16, pageSize / 16},
                {"v_long256.d", pageSize / 32, pageSize / 32},
                {"v_dec256.d", pageSize / 32, pageSize / 32},
                // values of 100 bytes: the next one crosses the end of the page
                {"v_string.d", pageSize / 100, DATA_PAGE_END},
                // an offset of 8 bytes per row after the first one of 8
                {"v_string.i", pageSize / 8 - 1, pageSize / 8 - 1},
                {"v_varchar.d", pageSize / 100, -1L},
                // 16 bytes per row
                {"v_varchar.i", pageSize / 16, pageSize / 16},
                {"v_binary.d", pageSize / 100, DATA_PAGE_END},
                {"v_binary.i", pageSize / 8 - 1, pageSize / 8 - 1},
                // values of 104 bytes
                {"v_array.d", pageSize / 104, -1L},
                {"v_array.i", pageSize / 16, pageSize / 16},
        };
        final int columnCount = ALL_TYPES.length + 1;
        final String[] names = new String[columnCount];
        final String[] types = new String[columnCount];
        final String[] valuesOfX = new String[columnCount];
        names[0] = "x";
        types[0] = "LONG";
        valuesOfX[0] = "x";
        for (int i = 0; i < ALL_TYPES.length; i++) {
            names[i + 1] = ALL_TYPES[i][0];
            types[i + 1] = ALL_TYPES[i][1];
            valuesOfX[i + 1] = ALL_TYPES[i][2];
        }
        for (Object[] file : files) {
            final String fileName = (String) file[0];
            final String failingColumn = fileName.substring(0, fileName.indexOf('.'));
            final int failingIndex = indexOf(names, failingColumn);
            // (a): the value of the column is the first one the INSERT lists
            final long valueRows = (Long) file[1];
            final StringBuilder insertColumns = new StringBuilder(failingColumn);
            final StringBuilder insertValues = new StringBuilder(failedValueOf(types[failingIndex], valuesOfX[failingIndex], valueRows + 1));
            for (int i = 0; i < columnCount; i++) {
                if (i != failingIndex) {
                    insertColumns.append(", ").append(names[i]);
                    insertValues.append(", ").append(valueOfRow(valuesOfX[i], valueRows + 1));
                }
            }
            assertFailedRowEveryColumnFile(
                    fileName,
                    fileName + ", first value",
                    names,
                    types,
                    valuesOfX,
                    valueRows,
                    -1,
                    "INSERT INTO t (" + insertColumns + ") VALUES (" + insertValues + ")"
            );
            // (b): a row of NULLs, with the columns in one order and in the reverse order
            final long nullRows = (Long) file[2];
            if (nullRows != -1) {
                final boolean isDataPageEnd = nullRows == DATA_PAGE_END;
                // values of 100 bytes and one of the rest of the page
                final long fullRows = isDataPageEnd ? (pageSize - Long.BYTES) / 100 : nullRows;
                final String nulls = "NULL" + ", NULL".repeat(columnCount - 1);
                for (boolean isReversed : new boolean[]{false, true}) {
                    final String[] tableNames = isReversed ? reversed(names) : names;
                    assertFailedRowEveryColumnFile(
                            fileName,
                            fileName + (isReversed ? ", NULLs, columns reversed" : ", NULLs"),
                            tableNames,
                            isReversed ? reversed(types) : types,
                            isReversed ? reversed(valuesOfX) : valuesOfX,
                            fullRows,
                            isDataPageEnd ? indexOf(tableNames, failingColumn) : -1,
                            "INSERT INTO t VALUES (" + nulls + ")"
                    );
                }
            }
        }
    }

    @Test
    public void testNoTimestampFailedRowMidTransaction() throws Exception {
        // the rows that the writer appended before the failed row, in the same transaction, are kept
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final long pageSize = Files.PAGE_SIZE;
        final AllocateFailingFacade ff = new AllocateFailingFacade("t", "s.d");
        assertMemoryLeak(ff, () -> {
            // 'abcdef' takes 16 bytes of s.d: the committed rows and 3 more end its first page
            final long n = pageSize / 16 - 3;
            execute("CREATE TABLE t (x LONG, s STRING)");
            execute("CREATE TABLE ref (x LONG, s STRING)");
            executeOnBoth("INSERT INTO %t SELECT x, 'abcdef' FROM long_sequence(" + n + ")");
            execute("INSERT INTO ref SELECT x + " + n + ", 'abcdef' FROM long_sequence(3)");
            try (TableWriter writer = getWriter("t")) {
                for (long x = n + 1; x <= n + 3; x++) {
                    final TableWriter.Row row = writer.newRow();
                    row.putLong(0, x);
                    row.putStr(1, "abcdef");
                    row.append();
                }
                final TableWriter.Row row = writer.newRow();
                ff.isArmed.set(true);
                failAppend(writer, row);
                row.cancel();
                Assert.assertEquals(3, writer.getUncommittedRowCount());
                final TableWriter.Row next = writer.newRow();
                next.putLong(0, n + 5);
                next.putStr(1, "next");
                next.append();
                writer.commit();
            }
            execute("INSERT INTO ref VALUES (" + (n + 5) + ", 'next')");
            assertQuery("SELECT x, s FROM t WHERE x >= " + (n + 2))
                    .noLeakCheck()
                    .returns("x\ts\n" + (n + 2) + "\tabcdef\n" + (n + 3) + "\tabcdef\n" + (n + 5) + "\tnext\n");
            assertCountAndSum("t", n + 4, (n + 3) * (n + 4) / 2 + n + 5);
            assertSqlCursors("ref", "t");
        });
    }

    @Test
    public void testNoTimestampFailedRowRandom() throws Exception {
        // Random tables without a designated timestamp take random rows through the writer, some of whose writes need
        // a page that a file of t cannot have: once, while the disk is full for a few rows, or with the mapping of the
        // page that the cancel moves the file back into failing as well. t must hold what ref, which sees no failure,
        // holds after each commit.
        final int tables = Integer.getInteger("questdb.fuzz.appendPageFailures", 4);
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final FailedRowShapes shapes = new FailedRowShapes();
        for (int i = 0; i < tables; i++) {
            assertFailedRowFuzz(rnd, shapes);
        }
        LOG.info().$("failed row shapes ").$(shapes.toString()).$();
    }

    @Test
    public void testNoTimestampFailedRowRandomShapes() throws Exception {
        // the random test makes, on these seeds, each shape of failed row that the cancel must take back
        final long[][] seeds = {
                // a STRING put at the start of a page, an aux page, a disk-full run
                {23L, 24L},
                // all-NULL rows that fail after an array and a fixed-size column
                {4L, 5L},
                // all-NULL rows that fail after a VARCHAR and a STRING column
                {35L, 36L},
                // a BINARY put, a mapping that fails in the cancel
                {31L, 32L},
        };
        final FailedRowShapes shapes = new FailedRowShapes();
        for (long[] seed : seeds) {
            final Rnd rnd = new Rnd(seed[0], seed[1]);
            for (int i = 0; i < 2; i++) {
                assertFailedRowFuzz(rnd, shapes);
            }
        }
        final String counts = shapes.toString();
        Assert.assertTrue(counts, shapes.stringDataPuts > 0);
        Assert.assertTrue(counts, shapes.binaryDataPuts > 0);
        Assert.assertTrue(counts, shapes.auxPages > 0);
        Assert.assertTrue(counts, shapes.fixedPages > 0);
        Assert.assertTrue(counts, shapes.valuesAtPageStart > 0);
        Assert.assertTrue(counts, shapes.nullsAfterFixed > 0);
        Assert.assertTrue(counts, shapes.nullsAfterVarchar > 0);
        Assert.assertTrue(counts, shapes.nullsAfterArray > 0);
        Assert.assertTrue(counts, shapes.nullsAfterString > 0);
        Assert.assertTrue(counts, shapes.diskFullRows > 0);
        Assert.assertTrue(counts, shapes.mmapFaults > 0);
    }

    @Test
    public void testNoTimestampFailedRowReaderHeld() throws Exception {
        // A reader opened before the failed all-NULL row reads the committed rows as they were, after the failure and
        // after the next rows; the cancel puts back the last offset of s1.i, in the page before the one that s1's
        // NULL took, with the bytes it holds
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("t", "s.d");
        assertMemoryLeak(ff, () -> {
            final long n = createStringAuxPageTables();
            final StringBuilder committed = new StringBuilder("x\ts1\ts\n");
            for (long x = 1; x <= n; x++) {
                committed.append(x).append("\tq").append(x).append('\t').append(x == 1 ? "" : x == 2 ? "abcdefghij" : "ab").append('\n');
            }
            try (TableReader reader = getReader("t")) {
                TestUtils.assertReader(committed, reader, sink);
                ff.isArmed.set(true);
                failInsert("INSERT INTO t VALUES (NULL, NULL, NULL)");
                Assert.assertFalse(ff.isArmed.get());
                TestUtils.assertReader(committed, reader, sink);
                executeOnBoth("INSERT INTO %t VALUES (" + (n + 2) + ", 'r" + (n + 2) + "', 'z'), (" + (n + 3) + ", NULL, NULL)");
                TestUtils.assertReader(committed, reader, sink);
                reader.reload();
                TestUtils.assertReader(committed.toString() + (n + 2) + "\tr" + (n + 2) + "\tz\n" + (n + 3) + "\t\t\n", reader, sink);
            }
            assertSqlCursors("ref", "t");
        });
    }

    @Test
    public void testNoTimestampFailedRowReopen() throws Exception {
        // The BINARY value, the first value of the row, crosses the end of the page of v.d. The rows read the same
        // after the writers and readers reopen; and a writer closed right after the failed row, and opened again,
        // appends the next rows after the committed ones as well.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        for (boolean isClosedAfterFailure : new boolean[]{false, true}) {
            final AllocateFailingFacade ff = new AllocateFailingFacade("t", "v.d");
            assertMemoryLeak(ff, () -> {
                final ObjList<Object> committed = valuesEndingAt(true, Files.PAGE_SIZE - 50);
                final long n = committed.size();
                final String context = isClosedAfterFailure ? "closed after the failure" : "closed after the next rows";
                createFirstValueTables("BINARY", committed);
                ff.isArmed.set(true);
                failInsert(context, "INSERT INTO t VALUES (" + sqlOf(binaryOf('F', 92)) + ", " + (n + 1) + ")");
                Assert.assertFalse(context, ff.isArmed.get());
                if (isClosedAfterFailure) {
                    engine.releaseAllWriters();
                }
                final ObjList<Object> next = nextBinaryValues(dataSizeOf(committed, true));
                insertFirstValues(next, n + 2);
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                assertFirstValues(context, true, committed, next);
                execute("DROP TABLE t");
                execute("DROP TABLE ref");
            });
        }
    }

    @Test
    public void testNoTimestampFailedRowThenCancel() throws Exception {
        // ILP, CSV import and other callers of the writer that cancel a row after its put or append() failed
        assertFailedRowThen("cancel");
    }

    @Test
    public void testNoTimestampFailedRowThenCommit() throws Exception {
        // commit() cancels a row in progress, here one whose put or append() failed
        assertFailedRowThen("commit");
    }

    @Test
    public void testNoTimestampFailedRowThenNewRow() throws Exception {
        // a caller that leaves the failed row in progress, as ILP UDP does: the next newRow() cancels it
        assertFailedRowThen("newRow");
    }

    @Test
    public void testNoTimestampFailedRowThenRollback() throws Exception {
        // rollback() distresses a writer that holds a row in progress, here one whose append() failed: the writer
        // reopens, and the committed rows stay
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("t", "s.d");
        assertMemoryLeak(ff, () -> {
            final long n = createStringPageTables();
            try (TableWriter writer = getWriter("t")) {
                final TableWriter.Row row = writer.newRow();
                ff.isArmed.set(true);
                failAppend(writer, row);
                try {
                    writer.rollback();
                    Assert.fail();
                } catch (CairoError e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Table 't' is distressed");
                }
                Assert.assertTrue(writer.isDistressed());
            }
            assertCountAndSum("t", n, n * (n + 1) / 2);
            assertSqlCursors("ref", "t");
            assertStringPageTablesNextRows(n);
        });
    }

    @Test
    public void testNoTimestampFailedRowTwice() throws Exception {
        // two failed rows in a row, each cancelled: the second starts where the first did
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        for (boolean isNullRow : new boolean[]{true, false}) {
            final AllocateFailingFacade ff = new AllocateFailingFacade("t", isNullRow ? "s.d" : "v.d");
            assertMemoryLeak(ff, () -> {
                final long n = createFailedRowTables(isNullRow);
                try (TableWriter writer = getWriter("t")) {
                    for (int i = 0; i < 2; i++) {
                        final TableWriter.Row row = writer.newRow();
                        ff.isArmed.set(true);
                        failRow(writer, row, isNullRow);
                        Assert.assertFalse(ff.isArmed.get());
                        row.cancel();
                        Assert.assertEquals(0, writer.getUncommittedRowCount());
                    }
                    appendFailedRowTablesNextRows(writer, isNullRow, n);
                    writer.commit();
                }
                assertFailedRowTablesNextRows(isNullRow, n);
                execute("DROP TABLE t");
                execute("DROP TABLE ref");
            });
        }
    }

    @Test
    public void testNoTimestampFirstRowFailed() throws Exception {
        // the first row of an empty table without a designated timestamp: its cancel takes the other branch, which
        // truncates the columns
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("t", "s.d");
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (x LONG, s STRING)");
            execute("CREATE TABLE ref (x LONG, s STRING)");
            // the writer maps the first page of each column file as it opens
            pooledWriter();
            ff.isArmed.set(true);
            failInsert("INSERT INTO t (s, x) VALUES ('" + "z".repeat((int) Files.PAGE_SIZE / 2) + "', 1)");
            Assert.assertFalse(ff.isArmed.get());
            executeOnBoth("INSERT INTO %t VALUES (2, 'b')");
            assertQuery("t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\ts\n2\tb\n");
            assertSqlCursors("ref", "t");
        });
    }

    @Test
    public void testNoTimestampInsertAsSelectFailedNullRow() throws Exception {
        // INSERT AS SELECT of all-NULL rows: the first ends s.d at the end of its first page, and the NULL of s in the
        // second needs the next page. The copy does not cancel the row, and the INSERT rolls the writer back, which
        // distresses a writer with a row in progress: no row of the INSERT stays, and the next INSERT lands with a
        // new writer.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("t", "s.d");
        assertMemoryLeak(ff, () -> {
            // 'abcdef' takes 16 bytes of s.d, 'abcd' 12: they end s.d 4 bytes before the end of its first page
            final long n = Files.PAGE_SIZE / 16;
            execute("CREATE TABLE t (x LONG, s STRING)");
            execute("CREATE TABLE ref (x LONG, s STRING)");
            executeOnBoth("INSERT INTO %t SELECT x, CASE WHEN x = " + n + " THEN 'abcd' ELSE 'abcdef' END FROM long_sequence(" + n + ")");
            ff.isArmed.set(true);
            failInsert("INSERT INTO t SELECT NULL x, NULL s FROM long_sequence(2)");
            Assert.assertFalse(ff.isArmed.get());
            assertCountAndSum("t", n, n * (n + 1) / 2);
            executeOnBoth("INSERT INTO %t VALUES (" + (n + 2) + ", 'abcdef'), (" + (n + 3) + ", NULL)");
            assertQuery("SELECT x, s FROM t WHERE x >= " + (n - 1))
                    .noLeakCheck()
                    .returns("x\ts\n" + (n - 1) + "\tabcdef\n" + n + "\tabcd\n" + (n + 2) + "\tabcdef\n" + (n + 3) + "\t\n");
            assertCountAndSum("t", n + 2, n * (n + 1) / 2 + n + 2 + n + 3);
            assertSqlCursors("ref", "t");
        });
    }

    @Test
    public void testNoTimestampInsertColumnListBinaryFirst() throws Exception {
        // t (x LONG, v BINARY): the INSERT lists v first, so its value is the first one the row writes, and it
        // crosses the end of the page of v.d
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("t", "v.d");
        assertMemoryLeak(ff, () -> {
            final ObjList<Object> committed = valuesEndingAt(true, Files.PAGE_SIZE - 50);
            final long n = committed.size();
            execute("CREATE TABLE t (x LONG, v BINARY)");
            execute("CREATE TABLE ref (x LONG, v BINARY)");
            insertFirstValues(committed, 1);
            ff.isArmed.set(true);
            failInsert("INSERT INTO t (v, x) VALUES (" + sqlOf(binaryOf('F', 92)) + ", " + (n + 1) + ")");
            Assert.assertFalse(ff.isArmed.get());
            final ObjList<Object> next = nextBinaryValues(dataSizeOf(committed, true));
            insertFirstValues(next, n + 2);
            assertFirstValues("x first", true, committed, next);
        });
    }

    @Test
    public void testNoTimestampRewindMmapFailure() throws Exception {
        // The cancel of an all-NULL row maps the page of s1.i before the one that s1's NULL took, and that mapping
        // fails: the cancel distresses the writer, the INSERT fails with that error, and the pool opens another
        // writer for the next INSERT, which lands after the committed rows
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final RewindMmapFailingFacade ff = new RewindMmapFailingFacade("t", "s.d", "s1.i");
        assertMemoryLeak(ff, () -> {
            final long n = createStringAuxPageTables();
            final TableWriter writer = pooledWriter();
            ff.isArmed.set(true);
            try {
                execute("INSERT INTO t VALUES (NULL, NULL, NULL)");
                Assert.fail();
            } catch (CairoError e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not mmap column");
            }
            Assert.assertFalse(ff.isArmed.get());
            Assert.assertFalse(ff.isMmapFailing.get());
            try (TableWriter next = getWriter("t")) {
                Assert.assertNotSame(writer, next);
                Assert.assertFalse(next.isDistressed());
            }
            assertStringAuxPageTablesNextRows(n);
        });
    }

    @Test
    public void testReaderHeldAcrossFailedTimestampPage() throws Exception {
        // the committed rows a reader holds do not change under it
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            createTableWithFullPage();
            final StringBuilder committed = new StringBuilder("ts\tx\n");
            for (long x = 1; x <= ROWS_PER_PAGE; x++) {
                committed.append(timestampOf(x)).append('\t').append(x).append('\n');
            }
            try (TableReader reader = getReader("tab")) {
                failInsert(ff, ROWS_PER_PAGE + 1);
                insertRow(ROWS_PER_PAGE + 2);
                TestUtils.assertReader(committed, reader, sink);
                reader.reload();
                TestUtils.assertReader(committed + timestampOf(ROWS_PER_PAGE + 2) + "\t" + (ROWS_PER_PAGE + 2) + "\n", reader, sink);
            }
            assertRows(ROWS_PER_PAGE, ROWS_PER_PAGE + 2);
        });
    }

    @Test
    public void testReopenAfterFailedTimestampPage() throws Exception {
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            createTableWithFullPage();
            try (TableWriter writer = getWriter("tab")) {
                failNewRow(writer, ff, ROWS_PER_PAGE + 1);
                writer.commit();
            }
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            assertRows(ROWS_PER_PAGE);
            insertRow(ROWS_PER_PAGE + 2);
            assertRows(ROWS_PER_PAGE, ROWS_PER_PAGE + 2);
        });
    }

    @Test
    public void testRollbackAfterFailedTimestampPage() throws Exception {
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            createTableWithFullPage();
            try (TableWriter writer = getWriter("tab")) {
                failNewRow(writer, ff, ROWS_PER_PAGE + 1);
                writer.rollback();
                Assert.assertFalse(writer.isDistressed());
                assertRows(ROWS_PER_PAGE);
                appendRow(writer, ROWS_PER_PAGE + 2);
                writer.commit();
            }
            assertRows(ROWS_PER_PAGE, ROWS_PER_PAGE + 2);
        });
    }

    @Test
    public void testRollbackUncommittedRowsAfterFailedTimestampPage() throws Exception {
        // rollback() distresses a writer that holds a row in progress; the failed row is none
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            createTables(ROWS_PER_PAGE - 3);
            try (TableWriter writer = getWriter("tab")) {
                for (long x = ROWS_PER_PAGE - 2; x <= ROWS_PER_PAGE; x++) {
                    final TableWriter.Row row = writer.newRow(x);
                    row.putLong(1, x);
                    row.append();
                }
                failNewRow(writer, ff, ROWS_PER_PAGE + 1);
                writer.rollback();
                Assert.assertFalse(writer.isDistressed());
                assertRows(ROWS_PER_PAGE - 3);
                appendRow(writer, ROWS_PER_PAGE - 2);
                writer.commit();
            }
            assertRows(ROWS_PER_PAGE - 2);
        });
    }

    @Test
    public void testStaleRowCancelAfterFailedTimestampPage() throws Exception {
        // a cancel through the Row of the row appended before the failed row finds no row in progress
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            createTables(ROWS_PER_PAGE - 1);
            try (TableWriter writer = getWriter("tab")) {
                final TableWriter.Row row = appendRow(writer, ROWS_PER_PAGE);
                failNewRow(writer, ff, ROWS_PER_PAGE + 1);
                row.cancel();
                writer.commit();
            }
            assertRows(ROWS_PER_PAGE);
        });
    }

    @Test
    public void testTruncateAfterFailedTimestampPage() throws Exception {
        // TRUNCATE and the ALTER after it run on the pooled writer the INSERT failed on; TRUNCATE closes the column
        // files, which the ALTER must not touch
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            createTableWithFullPage();
            failInsert(ff, ROWS_PER_PAGE + 1);
            execute("TRUNCATE TABLE tab");
            assertQuery("tab")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\n");
            execute("ALTER TABLE tab ADD COLUMN y INT");
            execute("INSERT INTO tab VALUES (1::TIMESTAMP, 1, 2)");
            assertQuery("tab")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\ty\n" + timestampOf(1) + "\t1\t2\n");
        });
    }

    @Test
    public void testUpdateAfterFailedTimestampPage() throws Exception {
        // UPDATE commits through the pooled writer the INSERT failed on
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tab", "ts.d");
        assertMemoryLeak(ff, () -> {
            createTableWithFullPage();
            failInsert(ff, ROWS_PER_PAGE + 1);
            execute("UPDATE tab SET x = -x WHERE x = " + ROWS_PER_PAGE);
            execute("UPDATE ref SET x = -x WHERE x = " + ROWS_PER_PAGE);
            assertQuery("SELECT ts, x FROM tab WHERE ts >= " + (ROWS_PER_PAGE - 1) + "::TIMESTAMP")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\tx\n"
                            + timestampOf(ROWS_PER_PAGE - 1) + "\t" + (ROWS_PER_PAGE - 1) + "\n"
                            + timestampOf(ROWS_PER_PAGE) + "\t" + -ROWS_PER_PAGE + "\n");
            assertQuery("SELECT count(), sum(x) FROM tab")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tsum\n" + ROWS_PER_PAGE + "\t" + (ROWS_PER_PAGE * (ROWS_PER_PAGE - 1) / 2 - ROWS_PER_PAGE) + "\n");
            assertSqlCursors("ref", "tab");
        });
    }

    @Test
    public void testWalInsertAfterFailedFixedPage() throws Exception {
        // a LONG256 value of a WAL table needs the next page of its column file in the WAL segment
        setProperty(PropertyKey.CAIRO_WAL_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tabw", "v.d");
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE tabw (ts TIMESTAMP, v LONG256, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final long n = Files.PAGE_SIZE / 32;
            execute("INSERT INTO tabw SELECT x::TIMESTAMP, '0x5a'::LONG256, x FROM long_sequence(" + n + ")");
            ff.isArmed.set(true);
            assertExceptionNoLeakCheck("INSERT INTO tabw VALUES (" + (n + 1) + "::TIMESTAMP, '0x6b', " + (n + 1) + ")", -1, "No space left");
            Assert.assertFalse(ff.isArmed.get());
            execute("INSERT INTO tabw VALUES (" + (n + 2) + "::TIMESTAMP, '0x7c', " + (n + 2) + ")");
            drainWalQueue();

            assertQuery("SELECT count(), sum(x) FROM tabw")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tsum\n" + (n + 1) + "\t" + (n * (n + 1) / 2 + n + 2) + "\n");
            assertQuery("SELECT ts, v, x FROM tabw WHERE x >= " + (n - 1))
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\tv\tx\n"
                            + timestampOf(n - 1) + "\t0x5a\t" + (n - 1) + "\n"
                            + timestampOf(n) + "\t0x5a\t" + n + "\n"
                            + timestampOf(n + 2) + "\t0x7c\t" + (n + 2) + "\n");
        });
    }

    @Test
    public void testWalInsertAfterFailedVarcharPage() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final AllocateFailingFacade ff = new AllocateFailingFacade("tabw", "v.d");
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE tabw (ts TIMESTAMP, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // values of 100 bytes: n of them fill the data file up to n * 100, and the next one crosses the
            // end of the first page, since a page size is not a multiple of 100
            final int len = 100;
            final long n = Files.PAGE_SIZE / len;
            final String a = "a".repeat(len);
            execute("INSERT INTO tabw SELECT x::TIMESTAMP, '" + a + "' FROM long_sequence(" + n + ")");
            ff.isArmed.set(true);
            assertExceptionNoLeakCheck("INSERT INTO tabw VALUES (" + (n + 1) + "::TIMESTAMP, '" + "b".repeat(len) + "')", -1, "No space left");
            Assert.assertFalse(ff.isArmed.get());
            final String c = "c".repeat(len);
            execute("INSERT INTO tabw VALUES (" + (n + 2) + "::TIMESTAMP, '" + c + "')");
            drainWalQueue();

            assertQuery("SELECT count(), count_distinct(v) FROM tabw")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tcount_distinct\n" + (n + 1) + "\t2\n");
            assertQuery("SELECT ts, v FROM tabw WHERE ts >= " + (n - 1) + "::TIMESTAMP")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("ts\tv\n"
                            + timestampOf(n - 1) + "\t" + a + "\n"
                            + timestampOf(n) + "\t" + a + "\n"
                            + timestampOf(n + 2) + "\t" + c + "\n");
        });
    }

    // the rows after a failed row of createFailedRowTables(), through the writer
    private static void appendFailedRowTablesNextRows(TableWriter writer, boolean isNullRow, long n) {
        TableWriter.Row row = writer.newRow();
        if (isNullRow) {
            row.putLong(0, n + 2);
            row.putStr(1, "abcdef");
            row.append();
            row = writer.newRow();
            row.putLong(0, n + 3);
        } else {
            row.putBin(0, new TestBinarySequence().of(binaryOf('C', 92)));
            row.putLong(1, n + 2);
            row.append();
            row = writer.newRow();
            row.putLong(1, n + 3);
        }
        row.append();
    }

    // the text of the rows that assertFirstValues() expects
    private static void appendFirstValueRow(StringBuilder expected, long x, Object value) {
        expected.append(x).append('\t').append(textOf(value)).append('\n');
    }

    // appends a row to tab through the writer, and writes the same row to ref
    private static TableWriter.Row appendRow(TableWriter writer, long x) throws Exception {
        final TableWriter.Row row = writer.newRow(x);
        row.putLong(1, x);
        row.append();
        execute("INSERT INTO ref VALUES (" + x + "::TIMESTAMP, " + x + ")");
        return row;
    }

    // runs one case of a test of several, naming the case when it fails
    private static void assertCase(String context, TestUtils.LeakProneCode code) throws Exception {
        try {
            code.run();
        } catch (AssertionError e) {
            throw new AssertionError(context + ": " + e.getMessage(), e);
        }
    }

    // Random rows through the writers of t and ref, with failed rows of t: see FailedRowFuzz
    private static void assertFailedRowFuzz(Rnd rnd, FailedRowShapes shapes) throws Exception {
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        final RandomFailureFacade ff = new RandomFailureFacade();
        assertMemoryLeak(ff, () -> {
            // x and 1-8 columns of the types of ALL_TYPES, half of them var-size, in random order
            final int columnCount = 2 + rnd.nextInt(8);
            final String[] names = new String[columnCount];
            final String[] types = new String[columnCount];
            final int xIndex = rnd.nextInt(columnCount);
            final StringBuilder columns = new StringBuilder();
            for (int i = 0; i < columnCount; i++) {
                names[i] = i == xIndex ? "x" : "c" + i;
                types[i] = i == xIndex ? "LONG" : rnd.nextBoolean() ? VAR_SIZE_TYPES[rnd.nextInt(VAR_SIZE_TYPES.length)] : ALL_TYPES[rnd.nextInt(ALL_TYPES.length)][1];
                if (i > 0) {
                    columns.append(", ");
                }
                columns.append(names[i]).append(' ').append(types[i]);
            }
            LOG.info().$("random table [columns=").$(columns).I$();
            execute("CREATE TABLE t (" + columns + ")");
            execute("CREATE TABLE ref (" + columns + ")");
            try (FailedRowFuzz fuzz = new FailedRowFuzz(rnd, ff, shapes, names, types)) {
                fuzz.run();
            }
            assertSqlCursors("ref", "t");
            execute("DROP TABLE t");
            execute("DROP TABLE ref");
        });
    }

    // t is the pooled writer of t, not distressed
    private static void assertPooledWriter(String context, TableWriter writer) {
        try (TableWriter pooled = getWriter("t")) {
            Assert.assertSame(context, writer, pooled);
            Assert.assertFalse(context, pooled.isDistressed());
        }
    }

    // where the failed put tests take the committed values to end the data file of a STRING or BINARY column, when
    // they end its aux file at the end of its page p instead
    private static long auxPageEnd(int p) {
        return -p;
    }

    // a BINARY value of len bytes c
    private static byte[] binaryOf(char c, int len) {
        final byte[] bytes = new byte[len];
        Arrays.fill(bytes, (byte) c);
        return bytes;
    }

    // a failed row through the writer: the all-NULL row of createStringPageTables(), or a BINARY value that crosses
    // the end of the page of v.d
    private static long createFailedRowTables(boolean isNullRow) throws Exception {
        if (isNullRow) {
            return createStringPageTables();
        }
        final ObjList<Object> committed = valuesEndingAt(true, Files.PAGE_SIZE - 50);
        createFirstValueTables("BINARY", committed);
        return committed.size();
    }

    // t and ref (v <type>, x LONG), without a designated timestamp, with the committed values in rows x = 1..n
    private static void createFirstValueTables(String type, ObjList<Object> committed) throws Exception {
        execute("CREATE TABLE t (v " + type + ", x LONG)");
        execute("CREATE TABLE ref (v " + type + ", x LONG)");
        insertFirstValues(committed, 1);
    }

    // t and ref (x LONG, s1 STRING, s STRING), s added after the first row: the rows x = 1..n end s1.i and s.d at the
    // end of their first page. In an all-NULL row, s1's NULL takes the next page of s1.i, and s's NULL needs the next
    // page of s.d.
    private static long createStringAuxPageTables() throws Exception {
        final long n = Files.PAGE_SIZE / Long.BYTES - 1;
        execute("CREATE TABLE t (x LONG, s1 STRING)");
        execute("CREATE TABLE ref (x LONG, s1 STRING)");
        executeOnBoth("INSERT INTO %t VALUES (1, 'q1')");
        execute("ALTER TABLE t ADD COLUMN s STRING");
        execute("ALTER TABLE ref ADD COLUMN s STRING");
        // 'abcdefghij' takes 24 bytes of s.d, 'ab' 8: the rows 2..n end s.d at 24 + 8 * (n - 2), the page size
        executeOnBoth("INSERT INTO %t SELECT x + 1, 'q' || (x + 1), CASE WHEN x = 1 THEN 'abcdefghij' ELSE 'ab' END FROM long_sequence(" + (n - 1) + ")");
        return n;
    }

    // t and ref (x LONG, s STRING) with the rows x = 1..n of 'abcdef', which takes 16 bytes of s.d: they end s.d at
    // the end of its first page, and in an all-NULL row s's NULL needs the next page after x's NULL landed
    private static long createStringPageTables() throws Exception {
        final long n = Files.PAGE_SIZE / 16;
        execute("CREATE TABLE t (x LONG, s STRING)");
        execute("CREATE TABLE ref (x LONG, s STRING)");
        executeOnBoth("INSERT INTO %t SELECT x, 'abcdef' FROM long_sequence(" + n + ")");
        return n;
    }

    // tables whose ts.d and x.d end exactly at the end of their first page, with the writer that appended them
    // in the pool, the next row of which maps the next page
    private static void createTableWithFullPage() throws Exception {
        createTables(ROWS_PER_PAGE);
    }

    // tab and ref with rows x = 1..n at ts = x, written by INSERT
    private static void createTables(long n) throws Exception {
        createTables(n, "DAY");
    }

    // tab and ref, partitioned as given, with rows x = 1..n at ts = x, written by INSERT
    private static void createTables(long n, String partitionBy) throws Exception {
        execute("CREATE TABLE tab (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY " + partitionBy + " BYPASS WAL");
        execute("CREATE TABLE ref (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY " + partitionBy + " BYPASS WAL");
        if (n > 0) {
            execute("INSERT INTO tab SELECT x::TIMESTAMP, x FROM long_sequence(" + n + ")");
            execute("INSERT INTO ref SELECT x::TIMESTAMP, x FROM long_sequence(" + n + ")");
        }
    }

    // bytes that a STRING or BINARY value takes in the data file of its column
    private static long dataSizeOf(Object value, boolean isBinary) {
        if (value == null) {
            return isBinary ? Long.BYTES : Integer.BYTES;
        }
        if (value instanceof byte[] bytes) {
            return Long.BYTES + bytes.length;
        }
        return Integer.BYTES + 2L * ((String) value).length();
    }

    private static long dataSizeOf(ObjList<Object> values, boolean isBinary) {
        long size = 0;
        for (int i = 0, n = values.size(); i < n; i++) {
            size += dataSizeOf(values.getQuick(i), isBinary);
        }
        return size;
    }

    // runs the statement on t and on ref, the table that sees no failure: %t stands for the table
    private static void executeOnBoth(String sql) throws Exception {
        execute(sql.replace("%t", "t"));
        execute(sql.replace("%t", "ref"));
    }

    // append() of the row fails in a NULL that needs a page the disk cannot give, and leaves the writer usable
    private static void failAppend(TableWriter writer, TableWriter.Row row) {
        try {
            row.append();
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
        }
        Assert.assertFalse(writer.isDistressed());
    }

    // an INSERT of row x into tab, the designated timestamp of which needs the next page of ts.d
    private static void failInsert(AllocateFailingFacade ff, long x) throws Exception {
        ff.isArmed.set(true);
        assertExceptionNoLeakCheck("INSERT INTO tab VALUES (" + x + "::TIMESTAMP, -1)", -1, "No space left");
        Assert.assertFalse(ff.isArmed.get());
    }

    private static void failInsert(String sql) throws Exception {
        failInsert(sql, sql);
    }

    // an INSERT whose row needs a page that the disk cannot give fails with that error, not with an error of the
    // writer
    private static void failInsert(String context, String sql) throws Exception {
        try {
            execute(sql);
            Assert.fail(context);
        } catch (CairoException e) {
            TestUtils.assertContains(context, e.getFlyweightMessage(), "No space left");
        }
    }

    // newRow() of row x through the writer, the designated timestamp of which needs the next page of ts.d
    private static void failNewRow(TableWriter writer, AllocateFailingFacade ff, long x) {
        final long uncommittedRowCount = writer.getUncommittedRowCount();
        ff.isArmed.set(true);
        try {
            writer.newRow(x);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
        }
        Assert.assertFalse(ff.isArmed.get());
        Assert.assertFalse(writer.isDistressed());
        Assert.assertEquals(uncommittedRowCount, writer.getUncommittedRowCount());
    }

    // the failed row of createFailedRowTables() through the writer: append() fails, or the put of the BINARY value
    private static void failRow(TableWriter writer, TableWriter.Row row, boolean isNullRow) {
        if (isNullRow) {
            failAppend(writer, row);
            return;
        }
        try {
            row.putBin(0, new TestBinarySequence().of(binaryOf('F', 92)));
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
        }
        Assert.assertFalse(writer.isDistressed());
    }

    // the SQL of the value of a column of ALL_TYPES in the failed row x: a var-size value differs from the values of
    // the other rows and takes as many bytes, so that a next row that reads it shows
    private static String failedValueOf(String type, String valueOfX, long x) {
        return switch (type) {
            case "STRING" -> sqlOf("f".repeat(48));
            case "VARCHAR" -> "'" + "f".repeat(100) + "'::VARCHAR";
            case "BINARY" -> sqlOf(binaryOf('F', 92));
            case "DOUBLE[]" -> "ARRAY[-1.0, -2.0, -3.0, -4.0, -5.0, -6.0, -7.0, -8.0, -9.0, -10.0, -11.0, -12.0]";
            default -> valueOfRow(valueOfX, x);
        };
    }

    private static int indexOf(String[] names, String name) {
        for (int i = 0; i < names.length; i++) {
            if (names[i].equals(name)) {
                return i;
            }
        }
        throw new AssertionError(name);
    }

    // the values into column v of table, with x from firstX
    private static void insertFirstValues(String table, ObjList<Object> values, long firstX) throws Exception {
        if (values.size() > 0) {
            final StringBuilder rows = new StringBuilder();
            for (int i = 0, n = values.size(); i < n; i++) {
                if (i > 0) {
                    rows.append(", ");
                }
                rows.append('(').append(sqlOf(values.getQuick(i))).append(", ").append(firstX + i).append(')');
            }
            execute("INSERT INTO " + table + " (v, x) VALUES " + rows);
        }
    }

    private static void insertFirstValues(ObjList<Object> values, long firstX) throws Exception {
        insertFirstValues("t", values, firstX);
        insertFirstValues("ref", values, firstX);
    }

    private static void insertRow(long x) throws Exception {
        execute("INSERT INTO tab VALUES (" + x + "::TIMESTAMP, " + x + ")");
        execute("INSERT INTO ref VALUES (" + x + "::TIMESTAMP, " + x + ")");
    }

    // BINARY values of the rows after the failed one, which start at offset rowStart of v.d: a value of 1 byte, NULL,
    // an empty value, a value of 3 bytes, a value whose bytes cross the end of a page, and a value longer than a page
    private static ObjList<Object> nextBinaryValues(long rowStart) {
        final ObjList<Object> values = new ObjList<>();
        values.add(binaryOf('a', 1));
        values.add(null);
        values.add(new byte[0]);
        values.add(binaryOf('b', 3));
        final long offset = rowStart + dataSizeOf(values, true);
        final long pageEnd = pageEndAfter(offset + Long.BYTES + 1);
        values.add(binaryOf('c', (int) (pageEnd - offset - Long.BYTES + 10)));
        values.add(binaryOf('d', (int) Files.PAGE_SIZE + 100));
        return values;
    }

    // STRING values of the rows after the failed one, which start at offset rowStart of v.d: a value, NULL, an empty
    // value, characters of 2, 3 and 4 bytes in UTF-8, a value whose surrogate pair the end of a page splits, and a
    // value longer than a page
    private static ObjList<Object> nextStringValues(long rowStart) {
        final ObjList<Object> values = new ObjList<>();
        values.add("q");
        values.add(null);
        values.add("");
        values.add("\u00e9");
        values.add("\u20ac");
        values.add(SURROGATE_PAIR);
        final long offset = rowStart + dataSizeOf(values, false);
        // the length, the chars before the pair and its high half end a page
        final long pageEnd = pageEndAfter(offset + Integer.BYTES + Character.BYTES);
        values.add("s".repeat((int) ((pageEnd - offset - Integer.BYTES - Character.BYTES) / 2)) + SURROGATE_PAIR + "z");
        values.add("L".repeat((int) Files.PAGE_SIZE));
        return values;
    }

    // the end of the page that holds the byte before offset
    private static long pageEndAfter(long offset) {
        return (offset + Files.PAGE_SIZE - 1) / Files.PAGE_SIZE * Files.PAGE_SIZE;
    }

    // the writer of t in the pool, opened if it was not
    private static TableWriter pooledWriter() {
        try (TableWriter writer = getWriter("t")) {
            return writer;
        }
    }

    private static String[] reversed(String[] values) {
        final String[] reversed = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            reversed[i] = values[values.length - 1 - i];
        }
        return reversed;
    }

    // the SQL of a STRING value (a String), a BINARY value (a byte[]) or NULL
    private static String sqlOf(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof byte[] bytes) {
            return "from_base64('" + Base64.getEncoder().encodeToString(bytes) + "')";
        }
        return "'" + value + "'";
    }

    // what assertFirstValues() prints for a STRING or BINARY value: its length, and the value, BINARY in base64
    private static String textOf(Object value) {
        if (value == null) {
            return "-1\t";
        }
        if (value instanceof byte[] bytes) {
            return bytes.length + "\t" + Base64.getEncoder().encodeToString(Arrays.copyOf(bytes, Math.min(bytes.length, 32_768)));
        }
        final String s = (String) value;
        return s.length() + "\t" + s;
    }

    private static String timestampNsOf(long nanos) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'").withZone(ZoneOffset.UTC).format(Instant.EPOCH.plus(nanos, ChronoUnit.NANOS));
    }

    private static String timestampOf(long micros) {
        return TIMESTAMP_FORMAT.format(Instant.EPOCH.plus(micros, ChronoUnit.MICROS));
    }

    // the SQL of the value of ALL_TYPES in row x
    private static String valueOfRow(String valueOfX, long x) {
        return valueOfX.replaceAll("\\bx\\b", Long.toString(x));
    }

    // values of rows 1..n of a STRING or BINARY column that end its data file at dataEnd: values that take 100 bytes,
    // and one that takes the rest. A BINARY value that ends at the end of a page makes its put map the next page as
    // well, so a NULL, which does not, ends a BINARY data file at the end of a page.
    private static ObjList<Object> valuesEndingAt(boolean isBinary, long dataEnd) {
        final boolean isNullLast = isBinary && dataEnd % Files.PAGE_SIZE == 0;
        final long valuesEnd = isNullLast ? dataEnd - Long.BYTES : dataEnd;
        final long minSize = isBinary ? Long.BYTES : Integer.BYTES;
        long fullRows = valuesEnd / 100;
        long rest = valuesEnd - fullRows * 100;
        if (rest > 0 && rest < minSize) {
            fullRows--;
            rest += 100;
        }
        final ObjList<Object> values = new ObjList<>();
        for (long x = 1; x <= fullRows; x++) {
            final char c = (char) ('a' + x % 26);
            values.add(isBinary ? binaryOf(c, 92) : String.valueOf(c).repeat(48));
        }
        if (rest > 0) {
            values.add(isBinary ? binaryOf('r', (int) (rest - Long.BYTES)) : "r".repeat((int) (rest - Integer.BYTES) / 2));
        }
        if (isNullLast) {
            values.add(null);
        }
        return values;
    }

    // values of rows 1..n of a STRING or BINARY column that end its aux file at the end of its page p
    private static ObjList<Object> valuesEndingAuxPage(boolean isBinary, int p) {
        final ObjList<Object> values = new ObjList<>();
        for (long x = 1, n = p * Files.PAGE_SIZE / Long.BYTES - 1; x <= n; x++) {
            final char c = (char) ('a' + x % 26);
            values.add(isBinary ? binaryOf(c, 1) : String.valueOf(c));
        }
        return values;
    }

    private void assertCountAndSum(String table, long count, long sum) throws Exception {
        assertQuery("SELECT count(), sum(x) FROM " + table)
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\tsum\n" + count + '\t' + sum + '\n');
    }

    // For one column file of testNoTimestampFailedRowEveryColumnFile: t and ref hold the rows x = 1..fullRows of the
    // values of ALL_TYPES and, when restIndex is a STRING or BINARY column, one more row whose value of that column
    // ends its data file at the end of its first page. The failing INSERT fails where the file needs its next page,
    // and the next rows land after the committed ones.
    private void assertFailedRowEveryColumnFile(
            String fileName,
            String context,
            String[] names,
            String[] types,
            String[] valuesOfX,
            long fullRows,
            int restIndex,
            String failingInsert
    ) throws Exception {
        final AllocateFailingFacade ff = new AllocateFailingFacade("t", fileName);
        assertMemoryLeak(ff, () -> assertCase(context, () -> {
            final StringBuilder columns = new StringBuilder();
            final StringBuilder values = new StringBuilder();
            for (int i = 0; i < names.length; i++) {
                if (i > 0) {
                    columns.append(", ");
                    values.append(", ");
                }
                columns.append(names[i]).append(' ').append(types[i]);
                values.append(valuesOfX[i]);
            }
            execute("CREATE TABLE t (" + columns + ")");
            execute("CREATE TABLE ref (" + columns + ")");
            executeOnBoth("INSERT INTO %t SELECT " + values + " FROM long_sequence(" + fullRows + ")");
            long n = fullRows;
            if (restIndex != -1) {
                // a BINARY value that ends at the end of a page makes its put map the next page as well: a NULL, which
                // does not, ends v_binary.d
                final boolean isBinary = "BINARY".equals(types[restIndex]);
                final ObjList<Object> restValues = new ObjList<>();
                restValues.add(isBinary ? binaryOf('r', (int) (Files.PAGE_SIZE - 100 * fullRows - 2 * Long.BYTES)) : "r".repeat((int) (Files.PAGE_SIZE - 100 * fullRows - Integer.BYTES) / 2));
                if (isBinary) {
                    restValues.add(null);
                }
                for (int r = 0, k = restValues.size(); r < k; r++) {
                    final StringBuilder row = new StringBuilder();
                    for (int i = 0; i < names.length; i++) {
                        if (i > 0) {
                            row.append(", ");
                        }
                        row.append(i == restIndex ? sqlOf(restValues.getQuick(r)) : valueOfRow(valuesOfX[i], n + 1));
                    }
                    executeOnBoth("INSERT INTO %t VALUES (" + row + ")");
                    n++;
                }
            }
            ff.isArmed.set(true);
            failInsert(context, failingInsert);
            Assert.assertFalse(context, ff.isArmed.get());

            // a row with a value in every column, and a row of NULLs
            final StringBuilder next = new StringBuilder();
            for (int i = 0; i < names.length; i++) {
                if (i > 0) {
                    next.append(", ");
                }
                next.append(valueOfRow(valuesOfX[i], n + 2));
            }
            executeOnBoth("INSERT INTO %t VALUES (" + next + ")");
            executeOnBoth("INSERT INTO %t (x) VALUES (" + (n + 3) + ")");
            assertQuery("SELECT x FROM t WHERE x >= " + (n - 1) + " OR x IS NULL")
                    .noLeakCheck()
                    .returns("x\n" + (n - 1) + '\n' + n + '\n' + (n + 2) + '\n' + (n + 3) + '\n');
            assertCountAndSum("t", n + 2, n * (n + 1) / 2 + n + 2 + n + 3);
            assertSqlCursors("ref", "t");
            execute("DROP TABLE t");
            execute("DROP TABLE ref");
        }));
    }

    // the rows of createFailedRowTables() and the rows of appendFailedRowTablesNextRows() after them
    private void assertFailedRowTablesNextRows(boolean isNullRow, long n) throws Exception {
        if (isNullRow) {
            execute("INSERT INTO ref VALUES (" + (n + 2) + ", 'abcdef'), (" + (n + 3) + ", NULL)");
            assertStringPageTables(n);
        } else {
            final ObjList<Object> next = new ObjList<>();
            next.add(binaryOf('C', 92));
            next.add(null);
            insertFirstValues("ref", next, n + 2);
            assertFirstValues("BINARY value", true, valuesEndingAt(true, Files.PAGE_SIZE - 50), next);
        }
    }

    // a failed row of createFailedRowTables() through the writer, then the rows after it as follow says
    private void assertFailedRowThen(String follow) throws Exception {
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
        for (boolean isNullRow : new boolean[]{true, false}) {
            final AllocateFailingFacade ff = new AllocateFailingFacade("t", isNullRow ? "s.d" : "v.d");
            assertMemoryLeak(ff, () -> assertCase(isNullRow ? "all-NULL row" : "BINARY value", () -> {
                final long n = createFailedRowTables(isNullRow);
                try (TableWriter writer = getWriter("t")) {
                    final TableWriter.Row row = writer.newRow();
                    ff.isArmed.set(true);
                    failRow(writer, row, isNullRow);
                    Assert.assertFalse(ff.isArmed.get());
                    switch (follow) {
                        case "cancel" -> row.cancel();
                        case "commit" -> writer.commit();
                        // the next newRow() cancels the row in progress
                        default -> Assert.assertEquals("newRow", follow);
                    }
                    appendFailedRowTablesNextRows(writer, isNullRow, n);
                    writer.commit();
                }
                assertFailedRowTablesNextRows(isNullRow, n);
                execute("DROP TABLE t");
                execute("DROP TABLE ref");
            }));
        }
    }

    // t and ref (v <type>, x LONG) hold the committed values and then the landed ones, in the rows after the failed
    // row n + 1: length and value of the rows from n - 1, count and sum of x, and the same rows as ref
    private void assertFirstValues(String context, boolean isBinary, ObjList<Object> committed, ObjList<Object> landed) throws Exception {
        final long n = committed.size();
        final StringBuilder expected = new StringBuilder("x\tlen\tv\n");
        appendFirstValueRow(expected, n - 1, committed.getQuick((int) n - 2));
        appendFirstValueRow(expected, n, committed.getQuick((int) n - 1));
        long landedSum = 0;
        for (int i = 0, k = landed.size(); i < k; i++) {
            appendFirstValueRow(expected, n + 2 + i, landed.getQuick(i));
            landedSum += n + 2 + i;
        }
        final long sum = n * (n + 1) / 2 + landedSum;
        assertCase(context, () -> {
            assertQuery("SELECT x, length(v) len, " + (isBinary ? "base64(v, 32_768)" : "v") + " v FROM t WHERE x >= " + (n - 1) + " OR x IS NULL")
                    .noLeakCheck()
                    .returns(expected);
            assertCountAndSum("t", n + landed.size(), sum);
            assertSqlCursors("ref", "t");
        });
    }

    // tab holds the rows x = 1..committed at ts = x, then the extra rows x at ts = x, and the same rows as ref
    private void assertRows(long committed, long... extra) throws Exception {
        final StringBuilder tail = new StringBuilder("ts\tx\n");
        long sum = committed * (committed + 1) / 2;
        for (long x = Math.max(1, committed - 1); x <= committed; x++) {
            tail.append(timestampOf(x)).append('\t').append(x).append('\n');
        }
        for (long x : extra) {
            tail.append(timestampOf(x)).append('\t').append(x).append('\n');
            sum += x;
        }
        assertQuery("SELECT ts, x FROM tab WHERE ts >= " + Math.max(1, committed - 1) + "::TIMESTAMP")
                .noLeakCheck()
                .timestamp("ts")
                .returns(tail);
        assertQuery("SELECT count(), sum(x) FROM tab")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("count\tsum\n" + (committed + extra.length) + "\t" + (committed + extra.length > 0 ? sum : "null") + "\n");
        assertSqlCursors("ref", "tab");
    }

    // the rows of createStringAuxPageTables() and two rows after the failed row n + 1, one with values, one of NULLs
    private void assertStringAuxPageTablesNextRows(long n) throws Exception {
        executeOnBoth("INSERT INTO %t VALUES (" + (n + 2) + ", 'r" + (n + 2) + "', 'z'), (" + (n + 3) + ", NULL, NULL)");
        assertQuery("SELECT x, s1, s FROM t WHERE x >= " + (n - 1) + " OR x IS NULL")
                .noLeakCheck()
                .returns("x\ts1\ts\n"
                        + (n - 1) + "\tq" + (n - 1) + "\tab\n"
                        + n + "\tq" + n + "\tab\n"
                        + (n + 2) + "\tr" + (n + 2) + "\tz\n"
                        + (n + 3) + "\t\t\n");
        assertCountAndSum("t", n + 2, n * (n + 1) / 2 + n + 2 + n + 3);
        assertSqlCursors("ref", "t");
    }

    // the rows of createStringPageTables() and two rows after the failed row n + 1: (n + 2, 'abcdef'), (n + 3, NULL)
    private void assertStringPageTables(long n) throws Exception {
        assertQuery("SELECT x, s FROM t WHERE x >= " + (n - 1) + " OR x IS NULL")
                .noLeakCheck()
                .returns("x\ts\n" + (n - 1) + "\tabcdef\n" + n + "\tabcdef\n" + (n + 2) + "\tabcdef\n" + (n + 3) + "\t\n");
        assertCountAndSum("t", n + 2, n * (n + 1) / 2 + n + 2 + n + 3);
        assertSqlCursors("ref", "t");
    }

    private void assertStringPageTablesNextRows(long n) throws Exception {
        executeOnBoth("INSERT INTO %t VALUES (" + (n + 2) + ", 'abcdef'), (" + (n + 3) + ", NULL)");
        assertStringPageTables(n);
    }

    /**
     * Fails, once when armed, the allocation of disk space for one file of one table: the file that the writer
     * opened last under that name, with or without the column name txn that a column added later has.
     */
    private static class AllocateFailingFacade extends TestFilesFacadeImpl {
        final AtomicBoolean isArmed = new AtomicBoolean();
        private final TableFile file;

        AllocateFailingFacade(String tableName, String fileName) {
            this.file = new TableFile(tableName, fileName);
        }

        @Override
        public boolean allocate(long fd, long size) {
            if (file.is(fd) && isArmed.compareAndSet(true, false)) {
                onAllocationFailed();
                return false;
            }
            return super.allocate(fd, size);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            file.onOpen(name, fd);
            return fd;
        }

        // the facade failed the allocation
        void onAllocationFailed() {
        }
    }

    /**
     * Writes random rows to t and ref through their writers, the same rows to both, and makes rows of t fail where
     * a file of t needs a page. ONCE: the next allocation of one file fails. MMAP: so does the next mapping of that
     * file after it, which a cancel that moves the file back into the page before makes, and the writer gets
     * distressed. DISK_FULL: every allocation of a file of t fails for a few rows, which are all-NULL: the cancel of
     * a row that marked a column can distress the writer on a full disk (a pre-existing bug, apart from this one).
     * A failed row is cancelled, left in progress for the next newRow() to cancel, or committed, which cancels it;
     * ref never gets it. Between failures the writers commit, go back to the pool for an INSERT, or reopen; after each
     * commit t holds what ref holds.
     */
    private static class FailedRowFuzz implements AutoCloseable {
        // the failure modes, ONCE is 0
        private static final int DISK_FULL = 1;
        private static final int MMAP = 2;
        // the shapes of a column in a row: a VALUE, a TYPED_NULL put, or NONE, when append() writes its NULL
        private static final int NONE = 2;
        private static final int TYPED_NULL = 1;
        private static final int VALUE = 0;
        private final DirectArray array = new DirectArray();
        private final TestBinarySequence binarySequence = new TestBinarySequence();
        // the bytes in the data file of each STRING and BINARY column, at the last commit and now
        private final long[] committedDataSizes;
        private final FailedRowShapes counts;
        private final long[] dataSizes;
        private final RandomFailureFacade ff;
        private final String[] names;
        private final IntList putOrder = new IntList();
        private final Rnd rnd;
        private final String[] types;
        private final Object[] values;
        private TableWriter.Row failedRow;
        private boolean isAbandoned;
        private boolean isFailedRowUnmarked;
        private TableWriter refWriter;
        private TableWriter writer;

        FailedRowFuzz(Rnd rnd, RandomFailureFacade ff, FailedRowShapes counts, String[] names, String[] types) {
            this.rnd = rnd;
            this.ff = ff;
            this.counts = counts;
            this.names = names;
            this.types = types;
            this.values = new Object[names.length];
            this.dataSizes = new long[names.length];
            this.committedDataSizes = new long[names.length];
        }

        @Override
        public void close() {
            ff.disarm();
            writer = Misc.free(writer);
            refWriter = Misc.free(refWriter);
            array.close();
        }

        void run() throws Exception {
            writer = getWriter("t");
            refWriter = getWriter("ref");
            for (int i = 0, n = 2 + rnd.nextInt(4); i < n; i++) {
                final int column = failingColumn();
                final String file = names[column] + (isVarSize(types[column]) && rnd.nextBoolean() ? ".i" : ".d");
                final int mode = rnd.nextInt(3);
                if (mode == DISK_FULL) {
                    failRowsOnFullDisk();
                } else {
                    failRow(column, file, mode == MMAP);
                }
                endAbandonedRow();
                switch (rnd.nextInt(4)) {
                    case 0 -> commit();
                    case 1 -> insertThroughSql();
                    case 2 -> reopen();
                    // the next rows join the transaction of the rows before the failed one
                    default -> {
                    }
                }
            }
            commit();
        }

        private static boolean isVarSize(String type) {
            return "STRING".equals(type) || "VARCHAR".equals(type) || "BINARY".equals(type) || "DOUBLE[]".equals(type);
        }

        // writes the row to t, and when it lands, to ref; false when the row of t failed for want of a page
        private boolean appendRow() {
            final TableWriter.Row row;
            try {
                row = writer.newRow();
            } catch (CairoError e) {
                // the newRow() cancelled the row left in progress, and that cancel failed to map a page
                recoverDistressedWriter(e);
                return appendRow();
            }
            isAbandoned = false;
            // the mapping fails only for the cancel of the failed row
            ff.disarmMmap();
            int column = -1;
            try {
                for (int i = 0, n = putOrder.size(); i < n; i++) {
                    column = putOrder.getQuick(i);
                    putValue(row, column);
                }
                column = -1;
                row.append();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "No space left");
                Assert.assertFalse(writer.isDistressed());
                failedRow = row;
                isFailedRowUnmarked = column == -1 ? putOrder.size() == 0 : putOrder.getQuick(0) == column;
                countShape(column);
                return false;
            }
            final TableWriter.Row refRow = refWriter.newRow();
            for (int i = 0, n = putOrder.size(); i < n; i++) {
                putValue(refRow, putOrder.getQuick(i));
            }
            refRow.append();
            for (int i = 0; i < names.length; i++) {
                if ("STRING".equals(types[i]) || "BINARY".equals(types[i])) {
                    dataSizes[i] += dataSizeOf(values[i], "BINARY".equals(types[i]));
                }
            }
            return true;
        }

        private void commit() throws Exception {
            endAbandonedRow();
            writer.commit();
            refWriter.commit();
            System.arraycopy(dataSizes, 0, committedDataSizes, 0, dataSizes.length);
            assertSqlCursors("ref", "t");
        }

        // the shape of the failed row: the failing column is the column of the put that failed, or of the NULL
        // that append() failed to write, since only that file fails
        private void countShape(int putColumn) {
            final String file = ff.failingFile();
            if (file == null) {
                return;
            }
            final int column = indexOf(names, file.substring(0, file.indexOf('.')));
            final String type = types[column];
            if (file.endsWith(".i")) {
                counts.auxPages++;
            } else if (!isVarSize(type)) {
                counts.fixedPages++;
            }
            if (putColumn == column && isFailedRowUnmarked && file.endsWith(".d")) {
                if ("STRING".equals(type) || "BINARY".equals(type)) {
                    if ("STRING".equals(type)) {
                        counts.stringDataPuts++;
                    } else {
                        counts.binaryDataPuts++;
                    }
                    if (dataSizes[column] % Files.PAGE_SIZE == 0) {
                        counts.valuesAtPageStart++;
                    }
                }
            }
            if (putColumn == -1 && isFailedRowUnmarked) {
                for (int i = 0; i < column; i++) {
                    switch (types[i]) {
                        case "VARCHAR" -> counts.nullsAfterVarchar++;
                        case "DOUBLE[]" -> counts.nullsAfterArray++;
                        case "STRING" -> counts.nullsAfterString++;
                        case "BINARY" -> {
                        }
                        default -> counts.nullsAfterFixed++;
                    }
                }
            }
        }

        // a row left in progress gets cancelled by the next newRow()
        private void endAbandonedRow() {
            while (isAbandoned) {
                nextRow(-1, false);
                appendRow();
            }
        }

        // rows with a value in the column of file more often than in the others, until the file needs its next page
        // and the row fails
        private void failRow(int column, String file, boolean isMmapFailing) {
            ff.arm(file, isMmapFailing);
            for (int i = 0, n = 4 * (int) Files.PAGE_SIZE; i < n; i++) {
                nextRow(column, rnd.nextInt(5) == 0);
                if (!appendRow()) {
                    followFailedRow(true);
                    break;
                }
            }
            // a mapping that is to fail stays armed for the cancel of a row left in progress
            ff.disarmAllocation();
        }

        // all-NULL rows on a disk that is full: the first that needs a page fails, then 0-2 more rows, which land
        // when they fit the pages the files have
        private void failRowsOnFullDisk() {
            ff.isFull.set(true);
            int rowsLeft = -1;
            for (int i = 0, n = 4 * (int) Files.PAGE_SIZE; i < n && rowsLeft != 0; i++) {
                nextRow(-1, true);
                final boolean isLanded = appendRow();
                if (!isLanded) {
                    counts.diskFullRows++;
                    followFailedRow(false);
                }
                if (rowsLeft > 0) {
                    rowsLeft--;
                } else if (!isLanded) {
                    rowsLeft = rnd.nextInt(3);
                }
            }
            ff.isFull.set(false);
        }

        // the column whose file is to fail: a var-size column more often than the others
        private int failingColumn() {
            if (rnd.nextBoolean()) {
                final int offset = rnd.nextInt(names.length);
                for (int i = 0; i < names.length; i++) {
                    final int column = (offset + i) % names.length;
                    if (isVarSize(types[column])) {
                        return column;
                    }
                }
            }
            return rnd.nextInt(names.length);
        }

        // the failed row is cancelled, left in progress, or committed, which cancels it; a commit on a full disk
        // stays out
        private void followFailedRow(boolean isCommitAllowed) {
            try {
                switch (rnd.nextInt(isCommitAllowed ? 3 : 2)) {
                    case 0 -> failedRow.cancel();
                    case 1 -> isAbandoned = true;
                    default -> {
                        writer.commit();
                        refWriter.commit();
                        System.arraycopy(dataSizes, 0, committedDataSizes, 0, dataSizes.length);
                    }
                }
            } catch (CairoError e) {
                recoverDistressedWriter(e);
            }
            if (!isAbandoned) {
                ff.disarmMmap();
            }
        }

        // a row of NULLs through SQL, with the writers back in the pool
        private void insertThroughSql() throws Exception {
            commit();
            writer = Misc.free(writer);
            refWriter = Misc.free(refWriter);
            executeOnBoth("INSERT INTO %t (x) VALUES (" + rnd.nextLong() + ")");
            for (int i = 0; i < names.length; i++) {
                if ("STRING".equals(types[i]) || "BINARY".equals(types[i])) {
                    dataSizes[i] += dataSizeOf((Object) null, "BINARY".equals(types[i]));
                }
            }
            System.arraycopy(dataSizes, 0, committedDataSizes, 0, dataSizes.length);
            writer = getWriter("t");
            refWriter = getWriter("ref");
        }

        // the shapes and values of the next row: column, whose file is to fail, gets a value more often
        private void nextRow(int column, boolean isAllNull) {
            putOrder.clear();
            for (int i = 0; i < names.length; i++) {
                final int dice = rnd.nextInt(100);
                int shape = isAllNull ? NONE : i == column ? (dice < 70 ? VALUE : dice < 85 ? TYPED_NULL : NONE) : (dice < 50 ? VALUE : dice < 65 ? TYPED_NULL : NONE);
                if (shape == TYPED_NULL && !isVarSize(types[i])) {
                    shape = VALUE;
                }
                values[i] = shape == VALUE ? randomValue(types[i]) : null;
                if (shape != NONE) {
                    putOrder.add(i);
                }
            }
            for (int i = putOrder.size() - 1; i > 0; i--) {
                final int j = rnd.nextInt(i + 1);
                final int k = putOrder.getQuick(i);
                putOrder.setQuick(i, putOrder.getQuick(j));
                putOrder.setQuick(j, k);
            }
            // the value of the column whose file is to fail is the first put of the row more often
            final int index = putOrder.indexOf(column, 0, putOrder.size());
            if (index > 0 && rnd.nextBoolean()) {
                putOrder.setQuick(index, putOrder.getQuick(0));
                putOrder.setQuick(0, column);
            }
        }

        private void putValue(TableWriter.Row row, int column) {
            final Object value = values[column];
            switch (types[column]) {
                case "BOOLEAN" -> row.putBool(column, ((Long) value & 1) == 1);
                case "BYTE" -> row.putByte(column, (byte) (long) (Long) value);
                case "SHORT" -> row.putShort(column, (short) (long) (Long) value);
                case "CHAR" -> row.putChar(column, (char) ('a' + Math.floorMod((Long) value, 26)));
                case "INT", "IPv4" -> row.putInt(column, (int) (long) (Long) value);
                case "FLOAT" -> row.putFloat(column, (int) (long) (Long) value / 4f);
                case "SYMBOL" -> row.putSym(column, "sym" + Math.floorMod((Long) value, 7));
                case "DATE", "LONG", "TIMESTAMP", "TIMESTAMP_NS" -> row.putLong(column, (Long) value);
                case "DOUBLE" -> row.putDouble(column, (Long) value / 8.0);
                case "UUID" -> row.putLong128(column, (Long) value, (Long) value * 31);
                case "LONG256" -> row.putLong256(column, (Long) value, 1, 2, (Long) value);
                case "GEOHASH(1c)" -> row.putGeoHash(column, (Long) value & 0x1fL);
                case "GEOHASH(2c)" -> row.putGeoHash(column, (Long) value & 0x3ffL);
                case "GEOHASH(4c)" -> row.putGeoHash(column, (Long) value & 0xfffffL);
                case "GEOHASH(8c)" -> row.putGeoHash(column, (Long) value & 0xffffffffffL);
                case "STRING" -> row.putStr(column, (CharSequence) value);
                case "VARCHAR" -> row.putVarchar(column, (Utf8String) value);
                case "BINARY" -> row.putBin(column, value == null ? null : binarySequence.of((byte[]) value));
                case "DOUBLE[]" -> {
                    if (value == null) {
                        array.ofNull();
                    } else {
                        final double[] doubles = (double[]) value;
                        array.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
                        array.setDimLen(0, doubles.length);
                        array.applyShape();
                        for (int i = 0; i < doubles.length; i++) {
                            array.putDouble(i, doubles[i]);
                        }
                    }
                    row.putArray(column, array);
                }
                // DECIMAL(precision, scale)
                default -> row.putDecimalStr(column, (String) value);
            }
        }

        private Object randomValue(String type) {
            switch (type) {
                case "STRING" -> {
                    return randomString();
                }
                case "VARCHAR" -> {
                    return new Utf8String(randomString());
                }
                case "BINARY" -> {
                    final byte[] bytes = new byte[rnd.nextInt(120)];
                    for (int i = 0; i < bytes.length; i++) {
                        bytes[i] = rnd.nextByte();
                    }
                    return bytes;
                }
                case "DOUBLE[]" -> {
                    final double[] doubles = new double[1 + rnd.nextInt(12)];
                    for (int i = 0; i < doubles.length; i++) {
                        doubles[i] = rnd.nextInt(1000) / 4.0;
                    }
                    return doubles;
                }
                default -> {
                    if (type.startsWith("DECIMAL")) {
                        // DECIMAL(precision, scale): up to 18 digits before the point, and scale digits after it
                        final int precision = Integer.parseInt(type.substring(type.indexOf('(') + 1, type.indexOf(',')).trim());
                        final int scale = Integer.parseInt(type.substring(type.indexOf(',') + 1, type.indexOf(')')).trim());
                        final StringBuilder value = new StringBuilder(rnd.nextBoolean() ? "-" : "");
                        final int digits = 1 + rnd.nextInt(Math.min(precision - scale, 18));
                        for (int i = 0; i < digits; i++) {
                            value.append((char) ('0' + rnd.nextInt(10)));
                        }
                        if (scale > 0) {
                            value.append('.');
                            for (int i = 0; i < scale; i++) {
                                value.append((char) ('0' + rnd.nextInt(10)));
                            }
                        }
                        return value.toString();
                    }
                    return rnd.nextLong();
                }
            }
        }

        // up to 60 chars, some of them of 2, 3 and 4 bytes in UTF-8
        private String randomString() {
            final StringBuilder value = new StringBuilder();
            for (int i = 0, n = rnd.nextInt(60); i < n; i++) {
                switch (rnd.nextInt(8)) {
                    case 0 -> value.append('\u00e9');
                    case 1 -> value.append('\u20ac');
                    case 2 -> value.append(SURROGATE_PAIR);
                    default -> value.append((char) ('a' + rnd.nextInt(26)));
                }
            }
            return value.toString();
        }

        // a cancel failed to map a page and distressed the writer of t: the pool closes it, a new writer opens,
        // and ref drops the rows that it did not commit, as t lost them
        private void recoverDistressedWriter(CairoError e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "could not mmap column");
            Assert.assertTrue(writer.isDistressed());
            if (isFailedRowUnmarked) {
                counts.mmapFaults++;
            }
            ff.disarm();
            isAbandoned = false;
            writer = Misc.free(writer);
            writer = getWriter("t");
            refWriter.rollback();
            System.arraycopy(committedDataSizes, 0, dataSizes, 0, dataSizes.length);
        }

        private void reopen() throws Exception {
            commit();
            writer = Misc.free(writer);
            refWriter = Misc.free(refWriter);
            engine.releaseAllWriters();
            writer = getWriter("t");
            refWriter = getWriter("ref");
        }
    }

    // the shapes of the failed rows of testNoTimestampFailedRowRandom that the cancel takes back
    private static class FailedRowShapes {
        // an offset that needs the next page of an aux file
        int auxPages;
        // the put of a BINARY value, the first put of the row, that needs the next page of its data file
        int binaryDataPuts;
        // all-NULL rows that failed on a full disk
        int diskFullRows;
        // a fixed-size value or NULL that needs the next page of its file
        int fixedPages;
        // a cancel of a row that marked no column, whose mapping of a page failed
        int mmapFaults;
        // an all-NULL row whose NULL of a column failed, after the NULL of a column of that kind before it landed
        int nullsAfterArray;
        int nullsAfterFixed;
        int nullsAfterString;
        int nullsAfterVarchar;
        // the put of a STRING value, the first put of the row, that needs the next page of its data file
        int stringDataPuts;
        // such a put of a STRING or BINARY value at the start of a page: it writes nothing
        int valuesAtPageStart;

        @Override
        public String toString() {
            return "[stringDataPuts=" + stringDataPuts
                    + ", binaryDataPuts=" + binaryDataPuts
                    + ", auxPages=" + auxPages
                    + ", fixedPages=" + fixedPages
                    + ", valuesAtPageStart=" + valuesAtPageStart
                    + ", nullsAfterFixed=" + nullsAfterFixed
                    + ", nullsAfterVarchar=" + nullsAfterVarchar
                    + ", nullsAfterArray=" + nullsAfterArray
                    + ", nullsAfterString=" + nullsAfterString
                    + ", diskFullRows=" + diskFullRows
                    + ", mmapFaults=" + mmapFaults
                    + ']';
        }
    }

    /**
     * A full disk: every allocation of disk space fails while the disk is full, and the facade counts those calls.
     * The disk fills when the test sets isFull, or when armed, as the next allocation of one file of one table takes
     * the last free space.
     */
    private static class FullDiskFacade extends TestFilesFacadeImpl {
        final AtomicInteger failedAllocations = new AtomicInteger();
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicBoolean isFull = new AtomicBoolean();
        // null: the disk fills only when the test sets isFull
        private final TableFile fillingFile;

        FullDiskFacade() {
            this.fillingFile = null;
        }

        FullDiskFacade(String tableName, String fillingFileName) {
            this.fillingFile = new TableFile(tableName, fillingFileName);
        }

        @Override
        public boolean allocate(long fd, long size) {
            if (isFull.get()) {
                failedAllocations.incrementAndGet();
                return false;
            }
            final boolean isAllocated = super.allocate(fd, size);
            if (fillingFile != null && fillingFile.is(fd) && isArmed.compareAndSet(true, false)) {
                isFull.set(true);
            }
            return isAllocated;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (fillingFile != null) {
                fillingFile.onOpen(name, fd);
            }
            return fd;
        }
    }

    /**
     * A column of the tables of testNoTimestampFailedNullRow: its type, its value in the committed row x, as SQL that
     * computes it from x and as the text that SELECT prints for it, its value in a later row x, as a literal and as
     * that text, the text of NULL, and the expression that SELECT prints the column with (%s: the column name).
     */
    private record NullRowColumn(
            String type,
            String committedValueOfX,
            LongFunction<String> committedText,
            LongFunction<String> literal,
            LongFunction<String> text,
            String nullText,
            String select
    ) {
        static final NullRowColumn BINARY = new NullRowColumn("BINARY", "from_base64(lpad(x::STRING, 8, 'A'))", NullRowColumn::base64Of, x -> "from_base64('" + base64Of(x) + "')", NullRowColumn::base64Of, "", "base64(%s, 100)");
        static final NullRowColumn DOUBLE_ARRAY = new NullRowColumn("DOUBLE[]", "ARRAY[x * 1.0, x + 0.5]", NullRowColumn::arrayOf, x -> "ARRAY[" + x + ".0, " + x + ".5]", NullRowColumn::arrayOf, "null", "%s");
        static final NullRowColumn LONG = new NullRowColumn("LONG", "x * 7", x -> Long.toString(x * 7), x -> Long.toString(x * 7), x -> Long.toString(x * 7), "null", "%s");
        static final NullRowColumn STRING = new NullRowColumn("STRING", "'p' || x", x -> "p" + x, x -> "'p" + x + "'", x -> "p" + x, "", "%s");
        static final NullRowColumn SYMBOL_INDEX = new NullRowColumn("SYMBOL INDEX", "CASE WHEN x % 2 = 0 THEN 'b' ELSE 'a' END", NullRowColumn::symbolOf, x -> "'" + symbolOf(x) + "'", NullRowColumn::symbolOf, "", "%s");
        // longer than an aux entry holds inline: the value goes to the data file
        static final NullRowColumn VARCHAR = new NullRowColumn("VARCHAR", "('varchar-not-inlined-' || x)::VARCHAR", x -> "varchar-not-inlined-" + x, x -> "'varchar-not-inlined-" + x + "'", x -> "varchar-not-inlined-" + x, "", "%s");
        static final NullRowColumn VARCHAR_INLINED = new NullRowColumn("VARCHAR", "('v' || x)::VARCHAR", x -> "v" + x, x -> "'v" + x + "'", x -> "v" + x, "", "%s");

        private static String arrayOf(long x) {
            return "[" + x + ".0," + x + ".5]";
        }

        // 8 chars of base64 for 6 bytes: as many 'A' as it takes before the digits of x
        private static String base64Of(long x) {
            final String digits = Long.toString(x);
            return "A".repeat(8 - digits.length()) + digits;
        }

        private static String symbolOf(long x) {
            return x % 2 == 0 ? "b" : "a";
        }
    }

    /**
     * The failures of FailedRowFuzz on the files of table t: the next allocation of one file, once, and then the
     * next mapping of that file, once, when armed for it; or every allocation of a file of t while the disk is full.
     */
    private static class RandomFailureFacade extends TestFilesFacadeImpl {
        final AtomicBoolean isFull = new AtomicBoolean();
        // the name of each file of t that is open, by its descriptor
        private final ConcurrentHashMap<Long, String> tableFiles = new ConcurrentHashMap<>();
        private volatile String allocationFailingFile;
        private volatile String failedFile;
        private volatile boolean isMmapArmed;
        private volatile long mmapFailingFd = -1;

        @Override
        public boolean allocate(long fd, long size) {
            final String file = tableFiles.get(fd);
            if (file != null) {
                if (isFull.get()) {
                    return false;
                }
                if (file.equals(allocationFailingFile)) {
                    allocationFailingFile = null;
                    failedFile = file;
                    if (isMmapArmed) {
                        isMmapArmed = false;
                        mmapFailingFd = fd;
                    }
                    return false;
                }
            }
            return super.allocate(fd, size);
        }

        @Override
        public boolean close(long fd) {
            tableFiles.remove(fd);
            return super.close(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            if (fd == mmapFailingFd) {
                mmapFailingFd = -1;
                return FilesFacade.MAP_FAILED;
            }
            return super.mmap(fd, len, offset, flags, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (Utf8s.containsAscii(name, Files.SEPARATOR + "t~")) {
                final String path = Utf8s.stringFromUtf8Bytes(name);
                tableFiles.put(fd, path.substring(path.lastIndexOf(Files.SEPARATOR) + 1));
            }
            return fd;
        }

        void arm(String fileName, boolean isMmapFailing) {
            failedFile = null;
            isMmapArmed = isMmapFailing;
            allocationFailingFile = fileName;
        }

        void disarm() {
            disarmAllocation();
            disarmMmap();
        }

        void disarmAllocation() {
            allocationFailingFile = null;
            isMmapArmed = false;
        }

        void disarmMmap() {
            mmapFailingFd = -1;
        }

        // the file whose allocation failed last, null on a full disk
        String failingFile() {
            return isFull.get() ? null : failedFile;
        }
    }

    /**
     * Fails, once when armed, the allocation of disk space for one file of a table, as AllocateFailingFacade does,
     * and then, once, the next mapping of another file of the table, which the cancel of the failed row makes.
     */
    private static class RewindMmapFailingFacade extends AllocateFailingFacade {
        final AtomicBoolean isMmapFailing = new AtomicBoolean();
        private final TableFile mmapFile;

        RewindMmapFailingFacade(String tableName, String allocationFileName, String mmapFileName) {
            super(tableName, allocationFileName);
            this.mmapFile = new TableFile(tableName, mmapFileName);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            if (mmapFile.is(fd) && isMmapFailing.compareAndSet(true, false)) {
                return FilesFacade.MAP_FAILED;
            }
            return super.mmap(fd, len, offset, flags, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            mmapFile.onOpen(name, fd);
            return fd;
        }

        @Override
        void onAllocationFailed() {
            isMmapFailing.set(true);
        }
    }

    // one file of one table: the descriptor that the writer opened it under last, with or without the column name txn
    // that a column added later has
    private static class TableFile {
        private final String fileSuffix;
        private final String tableDirPrefix;
        private volatile long fd = -1;

        TableFile(String tableName, String fileName) {
            this.tableDirPrefix = Files.SEPARATOR + tableName + '~';
            this.fileSuffix = Files.SEPARATOR + fileName;
        }

        boolean is(long fd) {
            return fd == this.fd;
        }

        void onOpen(LPSZ name, long fd) {
            if (Utf8s.containsAscii(name, tableDirPrefix)
                    && (Utf8s.endsWithAscii(name, fileSuffix) || Utf8s.containsAscii(name, fileSuffix + '.'))) {
                this.fd = fd;
            }
        }
    }
}
