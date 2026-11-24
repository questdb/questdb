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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.std.Files;
import io.questdb.std.str.LPSZ;
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
import java.util.concurrent.atomic.AtomicBoolean;

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
    // rows of 8 bytes that fill the first page of a column file
    private static final long ROWS_PER_PAGE = Files.PAGE_SIZE / Long.BYTES;
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").withZone(ZoneOffset.UTC);

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

    // appends a row to tab through the writer, and writes the same row to ref
    private static TableWriter.Row appendRow(TableWriter writer, long x) throws Exception {
        final TableWriter.Row row = writer.newRow(x);
        row.putLong(1, x);
        row.append();
        execute("INSERT INTO ref VALUES (" + x + "::TIMESTAMP, " + x + ")");
        return row;
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

    // tables whose ts.d and x.d end exactly at the end of their first page, with the writer that appended them
    // in the pool, the next row of which maps the next page
    private static void createTableWithFullPage() throws Exception {
        createTables(ROWS_PER_PAGE);
    }

    // an INSERT of row x into tab, the designated timestamp of which needs the next page of ts.d
    private static void failInsert(AllocateFailingFacade ff, long x) throws Exception {
        ff.isArmed.set(true);
        assertExceptionNoLeakCheck("INSERT INTO tab VALUES (" + x + "::TIMESTAMP, -1)", -1, "No space left");
        Assert.assertFalse(ff.isArmed.get());
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

    private static void insertRow(long x) throws Exception {
        execute("INSERT INTO tab VALUES (" + x + "::TIMESTAMP, " + x + ")");
        execute("INSERT INTO ref VALUES (" + x + "::TIMESTAMP, " + x + ")");
    }

    private static String timestampNsOf(long nanos) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'").withZone(ZoneOffset.UTC).format(Instant.EPOCH.plus(nanos, ChronoUnit.NANOS));
    }

    private static String timestampOf(long micros) {
        return TIMESTAMP_FORMAT.format(Instant.EPOCH.plus(micros, ChronoUnit.MICROS));
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

    /**
     * Fails, once when armed, the allocation of disk space for one file of one table: the file that the writer
     * opened last under that name, with or without the column name txn that a column added later has.
     */
    private static class AllocateFailingFacade extends TestFilesFacadeImpl {
        final AtomicBoolean isArmed = new AtomicBoolean();
        private final String fileSuffix;
        private final String tableDirPrefix;
        private volatile long fd = -1;

        AllocateFailingFacade(String tableName, String fileName) {
            this.tableDirPrefix = Files.SEPARATOR + tableName + '~';
            this.fileSuffix = Files.SEPARATOR + fileName;
        }

        @Override
        public boolean allocate(long fd, long size) {
            if (fd == this.fd && isArmed.compareAndSet(true, false)) {
                return false;
            }
            return super.allocate(fd, size);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (Utf8s.containsAscii(name, tableDirPrefix)
                    && (Utf8s.endsWithAscii(name, fileSuffix) || Utf8s.containsAscii(name, fileSuffix + '.'))) {
                this.fd = fd;
            }
            return fd;
        }
    }
}
