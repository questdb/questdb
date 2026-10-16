/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo.parquet;

import io.questdb.cairo.SqlJitMode;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Column tops of BYTE/SHORT columns in parquet partitions written by QuestDB 9.4.x, read after
 * ALTER COLUMN TYPE to a type that has a NULL value.
 * <p>
 * The fixture {@code resources/parquet-c3-legacy/legacy_byte.zip} is a table written by a 9.4.2
 * build (commit 779676dd73, an ancestor of 9.4.3), which stores BYTE and SHORT as parquet Required:
 * no definition levels, so the rows before ADD COLUMN are stored as 0. The server ran with
 * {@code cairo.partition.encoder.parquet.row.group.size=50} and statistics on (the default):
 * <pre>
 * CREATE TABLE legacy_byte (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL;
 * INSERT INTO legacy_byte SELECT '2024-01-01'::timestamp + x * 1_000_000, x::int FROM long_sequence(60);
 * ALTER TABLE legacy_byte ADD COLUMN b1 BYTE; -- and b2, b3, b4 BYTE, s1, s2, s3, s4 SHORT
 * INSERT INTO legacy_byte (120 rows, see {@link #TWIN_ROWS}: v 61..180, b/s 0 at v 61, the smallest
 *     value at v 62, the largest at v 63);
 * INSERT INTO legacy_byte VALUES ('2024-01-02T00:00:00.000000Z', 181, 82, 82, 82, 82, 1268, 1268, 1268, 1268);
 * ALTER TABLE legacy_byte CONVERT PARTITION TO PARQUET LIST '2024-01-01';
 * </pre>
 * Partition 2024-01-01 holds 180 rows in row groups of 50 with a column top of 60 for b1..s4: row
 * group 0 (v 1..50) lies wholly in the top, the top covers v 51..60 of row group 1 (v 51..100), and
 * row groups 2 and 3 lie after it. Partition 2024-01-02 stays native.
 * <p>
 * The oracle is the native twin {@code tw}: the same rows, with the columns added after 60 rows and
 * the same ALTERs. Native ALTER keeps the column top, so the top reads NULL in every target type
 * that has a NULL value.
 */
public class LegacyRequiredByteTopTest extends AbstractCairoTest {

    // b1..b4 BYTE and s1..s4 SHORT of rows v 61..180: 0, the smallest and the largest value right
    // after the top, then varied values
    private static final String TWIN_ROWS = """
            SELECT ts, v, b, b, b, b, s, s, s, s FROM (
                SELECT '2024-01-01'::timestamp + (x + 60) * 1_000_000 ts, (x + 60)::int v,
                    (CASE x + 60 WHEN 61 THEN 0 WHEN 62 THEN -128 WHEN 63 THEN 127 ELSE (x + 60) % 100 + 1 END)::byte b,
                    (CASE x + 60 WHEN 61 THEN 0 WHEN 62 THEN -32768 WHEN 63 THEN 32767 ELSE (x + 60) * 7 + 1 END)::short s
                FROM long_sequence(120)
            )""";

    private static void loadFixture() throws Exception {
        final byte[] buffer = new byte[1024 * 1024];
        final File root = new File(configuration.getDbRoot().toString());
        try (InputStream is = LegacyRequiredByteTopTest.class.getResourceAsStream("/parquet-c3-legacy/legacy_byte.zip")) {
            Assert.assertNotNull(is);
            try (ZipInputStream zip = new ZipInputStream(is)) {
                ZipEntry ze;
                while ((ze = zip.getNextEntry()) != null) {
                    final File dest = new File(root, ze.getName());
                    if (!ze.isDirectory()) {
                        final File dir = dest.getParentFile();
                        if (!dir.exists()) {
                            Assert.assertTrue(dir.mkdirs());
                        }
                        try (FileOutputStream fos = new FileOutputStream(dest)) {
                            int n;
                            while ((n = zip.read(buffer, 0, buffer.length)) > 0) {
                                fos.write(buffer, 0, n);
                            }
                        }
                    }
                    zip.closeEntry();
                }
            }
        }
        engine.reloadTableNames();
        engine.reconcileTableNameRegistryState();
    }

    // NULL count of every column of partition 2024-01-01
    private static String nullCountsSql(String table) {
        final StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 1; i <= 8; i++) {
            final String c = (i <= 4 ? "b" : "s") + ((i - 1) % 4 + 1);
            if (i > 1) {
                sb.append(", ");
            }
            sb.append("sum(CASE WHEN ").append(c).append(" = NULL THEN 1 ELSE 0 END) ").append(c);
        }
        return sb.append(" FROM ").append(table).append(" WHERE ts < '2024-01-02'").toString();
    }

    private static String nullCounts(int count) {
        return "b1\tb2\tb3\tb4\ts1\ts2\ts3\ts4\n"
                + count + "\t" + count + "\t" + count + "\t" + count + "\t"
                + count + "\t" + count + "\t" + count + "\t" + count + "\n";
    }

    @Test
    public void testByteShortTopToIntLongTimestampReadsNull() throws Exception {
        assertMemoryLeak(() -> {
            loadAndAlter("INT", "LONG", "TIMESTAMP", "DOUBLE", "INT", "LONG", "TIMESTAMP", "DATE");
            // the lazy read of the pending conversion
            assertConvertedTop();
            engine.releaseAllReaders();
            assertConvertedTop();

            // CONVERT TO NATIVE decodes the pending conversion and stores its result
            execute("ALTER TABLE legacy_byte CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            assertConvertedTop();
            engine.releaseAllReaders();
            assertConvertedTop();
        });
    }

    @Test
    public void testByteShortTopToTimestampNsReadsNullAndToShortByteReadsZero() throws Exception {
        assertMemoryLeak(() -> {
            // b4 SHORT and s4 BYTE have no NULL value: their top reads 0, as native
            loadAndAlter("INT", "LONG", "TIMESTAMP_NS", "SHORT", "INT", "LONG", "TIMESTAMP_NS", "BYTE");
            assertSqlCursors("tw", "legacy_byte");
            assertQuery("""
                    SELECT sum(CASE WHEN b3 = NULL THEN 1 ELSE 0 END) b3, sum(CASE WHEN s3 = NULL THEN 1 ELSE 0 END) s3,
                        sum(CASE WHEN b4 = 0 THEN 1 ELSE 0 END) b4, sum(CASE WHEN s4 = 0 THEN 1 ELSE 0 END) s4
                    FROM legacy_byte WHERE ts < '2024-01-02'""")
                    .noRandomAccess()
                    .expectSize()
                    // v 61 stores 0, and SHORT -32768 (v 62) and 512 (v 73) narrow to BYTE 0
                    .returns("b3\ts3\tb4\ts4\n60\t60\t61\t63\n");
            assertQuery("SELECT v, b3, s3, b4, s4 FROM legacy_byte WHERE v BETWEEN 59 AND 63")
                    .timestamp(null)
                    .returns("""
                            v\tb3\ts3\tb4\ts4
                            59\t\t\t0\t0
                            60\t\t\t0\t0
                            61\t1970-01-01T00:00:00.000000000Z\t1970-01-01T00:00:00.000000000Z\t0\t0
                            62\t1969-12-31T23:59:59.999999872Z\t1969-12-31T23:59:59.999967232Z\t-128\t0
                            63\t1970-01-01T00:00:00.000000127Z\t1970-01-01T00:00:00.000032767Z\t127\t-1
                            """);
        });
    }

    @Test
    public void testFilteredAndIntervalReadsStampTop() throws Exception {
        assertMemoryLeak(() -> {
            loadAndAlter("INT", "LONG", "TIMESTAMP", "DOUBLE", "INT", "LONG", "TIMESTAMP", "DATE");
            for (boolean isParallel : new boolean[]{true, false}) {
                sqlExecutionContext.setParallelFilterEnabled(isParallel);
                for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    sqlExecutionContext.setJitMode(jitMode);
                    assertQuery("SELECT count() FROM legacy_byte WHERE b1 = NULL AND s2 = NULL")
                            .noRandomAccess()
                            .expectSize()
                            .returns("count\n60\n");
                    // filtered late materialization decodes rows 56..63 of row group 1 compacted
                    assertQuery("SELECT v, b1, b2, b3, s1, s2, s3 FROM legacy_byte WHERE v > 55 AND v < 64")
                            .timestamp(null)
                            .returns("""
                                    v\tb1\tb2\tb3\ts1\ts2\ts3
                                    56\tnull\tnull\t\tnull\tnull\t
                                    57\tnull\tnull\t\tnull\tnull\t
                                    58\tnull\tnull\t\tnull\tnull\t
                                    59\tnull\tnull\t\tnull\tnull\t
                                    60\tnull\tnull\t\tnull\tnull\t
                                    61\t0\t0\t1970-01-01T00:00:00.000000Z\t0\t0\t1970-01-01T00:00:00.000000Z
                                    62\t-128\t-128\t1969-12-31T23:59:59.999872Z\t-32768\t-32768\t1969-12-31T23:59:59.967232Z
                                    63\t127\t127\t1970-01-01T00:00:00.000127Z\t32767\t32767\t1970-01-01T00:00:00.032767Z
                                    """);
                    bindVariableService.clear();
                    bindVariableService.setInt(0, Numbers.INT_NULL);
                    assertQuery("SELECT count() FROM legacy_byte WHERE b1 = $1")
                            .noRandomAccess()
                            .expectSize()
                            .returns("count\n60\n");
                    bindVariableService.clear();
                }
            }
            // an interval scan whose frame starts at v 55, inside row group 1
            assertQuery("SELECT v, b1, s3 FROM legacy_byte WHERE ts >= '2024-01-01T00:00:55' AND ts < '2024-01-01T00:01:02'")
                    .timestamp(null)
                    .returns("""
                            v\tb1\ts3
                            55\tnull\t
                            56\tnull\t
                            57\tnull\t
                            58\tnull\t
                            59\tnull\t
                            60\tnull\t
                            61\t0\t1970-01-01T00:00:00.000000Z
                            """);
            assertSqlCursors(
                    "tw WHERE ts >= '2024-01-01T00:00:55' AND ts < '2024-01-01T00:01:02'",
                    "legacy_byte WHERE ts >= '2024-01-01T00:00:55' AND ts < '2024-01-01T00:01:02'"
            );
        });
    }

    @Test
    public void testPendingConversionThenO3KeepsNull() throws Exception {
        assertMemoryLeak(() -> {
            loadAndAlter("INT", "LONG", "TIMESTAMP", "DOUBLE", "INT", "LONG", "TIMESTAMP", "DATE");
            // an O3 row inside the top of row group 0, a tie with the last row of row group 0, and
            // one after the partition's last row; the merge decodes the pending conversion
            for (String t : new String[]{"legacy_byte", "tw"}) {
                execute("INSERT INTO " + t + " (ts, v) VALUES ('2024-01-01T00:00:30.500000Z', 1_000), "
                        + "('2024-01-01T00:00:50.000000Z', 1_001), ('2024-01-01T00:03:30.000000Z', 1_002)");
            }
            // the 60 top rows and the 3 new rows, which set no b/s value
            assertQuery(nullCountsSql("legacy_byte")).noRandomAccess().expectSize().returns(nullCounts(63));
            assertSqlCursors("tw", "legacy_byte");
            execute("ALTER TABLE legacy_byte CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            engine.releaseAllReaders();
            assertQuery(nullCountsSql("legacy_byte")).noRandomAccess().expectSize().returns(nullCounts(63));
            assertSqlCursors("tw", "legacy_byte");
        });
    }

    @Test
    public void testSqlContextsReadNull() throws Exception {
        assertMemoryLeak(() -> {
            loadAndAlter("INT", "LONG", "TIMESTAMP", "DOUBLE", "INT", "LONG", "TIMESTAMP", "DATE");
            assertQuery("WITH x AS (SELECT b1, s3 FROM legacy_byte) SELECT count() FROM x WHERE b1 = NULL AND s3 = NULL")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n60\n");
            assertQuery("SELECT count() FROM (SELECT b2 FROM legacy_byte WHERE ts < '2024-01-02') WHERE b2 = NULL")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n60\n");
            assertQuery("SELECT b1 = NULL is_null, count() FROM legacy_byte GROUP BY 1 ORDER BY 1")
                    .expectSize()
                    .returns("is_null\tcount\nfalse\t121\ntrue\t60\n");
            assertQuery("SELECT v, b1, s2 FROM legacy_byte WHERE ts < '2024-01-01T00:01:01' ORDER BY ts LIMIT -5")
                    .timestamp(null)
                    .expectSize()
                    .returns("""
                            v\tb1\ts2
                            56\tnull\tnull
                            57\tnull\tnull
                            58\tnull\tnull
                            59\tnull\tnull
                            60\tnull\tnull
                            """);
            assertQuery("""
                    SELECT count() FROM legacy_byte l JOIN tw r ON l.ts = r.ts
                    WHERE l.b1 != r.b1 OR l.b2 != r.b2 OR l.b3 != r.b3 OR l.s1 != r.s1 OR l.s2 != r.s2 OR l.s3 != r.s3""")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n0\n");
            assertQuery("SELECT count() FROM legacy_byte l JOIN tw r ON l.ts = r.ts WHERE l.b1 = NULL AND r.b1 = NULL")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n60\n");
        });
    }

    private void alter(String... types) throws Exception {
        for (String t : new String[]{"legacy_byte", "tw"}) {
            for (int i = 0; i < types.length; i++) {
                final String c = (i < 4 ? "b" : "s") + (i % 4 + 1);
                execute("ALTER TABLE " + t + " ALTER COLUMN " + c + " TYPE " + types[i]);
            }
        }
    }

    // the converted top reads NULL on 2024-01-01 in every column, and every row equals the twin
    private void assertConvertedTop() throws Exception {
        assertQuery(nullCountsSql("legacy_byte")).noRandomAccess().expectSize().returns(nullCounts(60));
        assertQuery("SELECT v, b1, b2, b3, b4, s1, s2, s3, s4 FROM legacy_byte WHERE v BETWEEN 59 AND 63")
                .timestamp(null)
                .returns("""
                        v\tb1\tb2\tb3\tb4\ts1\ts2\ts3\ts4
                        59\tnull\tnull\t\tnull\tnull\tnull\t\t
                        60\tnull\tnull\t\tnull\tnull\tnull\t\t
                        61\t0\t0\t1970-01-01T00:00:00.000000Z\t0.0\t0\t0\t1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000Z
                        62\t-128\t-128\t1969-12-31T23:59:59.999872Z\t-128.0\t-32768\t-32768\t1969-12-31T23:59:59.967232Z\t1969-12-31T23:59:27.232Z
                        63\t127\t127\t1970-01-01T00:00:00.000127Z\t127.0\t32767\t32767\t1970-01-01T00:00:00.032767Z\t1970-01-01T00:00:32.767Z
                        """);
        assertSqlCursors("tw", "legacy_byte");
    }

    // the native twin of the fixture, with the columns added after 60 rows
    private void createTwin() throws Exception {
        execute("CREATE TABLE tw (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO tw SELECT '2024-01-01'::timestamp + x * 1_000_000, x::int FROM long_sequence(60)");
        for (String c : new String[]{"b1", "b2", "b3", "b4"}) {
            execute("ALTER TABLE tw ADD COLUMN " + c + " BYTE");
        }
        for (String c : new String[]{"s1", "s2", "s3", "s4"}) {
            execute("ALTER TABLE tw ADD COLUMN " + c + " SHORT");
        }
        execute("INSERT INTO tw " + TWIN_ROWS);
        execute("INSERT INTO tw VALUES ('2024-01-02T00:00:00.000000Z', 181, 82, 82, 82, 82, 1268, 1268, 1268, 1268)");
        // before the ALTER, the BYTE/SHORT top reads 0 in both tables
        assertSqlCursors("tw", "legacy_byte");
    }

    private void loadAndAlter(String... types) throws Exception {
        loadFixture();
        createTwin();
        alter(types);
    }
}
