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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.SymbolMapWriter;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.mp.SCSequence;
import io.questdb.std.MemoryTag;
import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.Overrides;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.TableUtils.PARQUET_METADATA_FILE_NAME;
import static io.questdb.cairo.TableUtils.PARQUET_PARTITION_NAME;

public class AlterTableConvertPartitionTest extends AbstractCairoTest {
    // column, source type, target type, value of row v
    private static final String[][] LAZY_TYPE_CONVERSIONS = {
            {"bi", "BYTE", "INT", "(v % 100)::BYTE"},
            {"il", "INT", "LONG", "CASE WHEN v % 4 = 2 THEN NULL ELSE v * 1000 END"},
            {"iv", "INT", "VARCHAR", "CASE WHEN v % 4 = 2 THEN NULL ELSE v * 7 END"},
            {"is", "INT", "STRING", "CASE WHEN v % 4 = 2 THEN NULL ELSE -v END"},
            {"bs", "BYTE", "STRING", "(v % 100 - 50)::BYTE"},
            {"vi", "VARCHAR", "INT", "CASE WHEN v % 4 = 2 THEN NULL ELSE (v * 3)::VARCHAR END"},
            {"sd", "STRING", "DOUBLE", "CASE WHEN v % 4 = 2 THEN NULL ELSE (v || '.5')::STRING END"},
            {"vd", "VARCHAR", "DECIMAL(18, 2)", "CASE WHEN v % 4 = 2 THEN NULL ELSE (v || '.25')::VARCHAR END"},
            {"sv", "STRING", "VARCHAR", "CASE WHEN v % 4 = 2 THEN NULL ELSE ('\u00e9\ud834\udd1e' || v)::STRING END"},
            {"vs", "VARCHAR", "STRING", "CASE WHEN v % 4 = 2 THEN NULL ELSE ('\u042f' || v)::VARCHAR END"},
            {"yv", "SYMBOL", "VARCHAR", "CASE WHEN v % 4 = 2 THEN NULL ELSE 'k' || (v % 3) END"},
            {"ys", "SYMBOL", "STRING", "CASE WHEN v % 4 = 2 THEN NULL ELSE 'k' || (v % 2) END"},
            {"yi", "SYMBOL", "INT", "CASE WHEN v % 4 = 2 THEN NULL ELSE (v % 5)::VARCHAR END"},
            {"vb", "VARCHAR", "BYTE", "CASE WHEN v % 4 = 2 THEN NULL ELSE (v * 37 - 150)::VARCHAR END"},
            {"sh", "STRING", "SHORT", "CASE WHEN v % 4 = 2 THEN NULL ELSE (v * 5000 - 40000)::STRING END"},
            {"yo", "SYMBOL", "BOOLEAN", "CASE WHEN v % 4 = 2 THEN NULL WHEN v % 3 = 0 THEN 'true' ELSE 'false' END"},
            {"vh", "VARCHAR", "CHAR", "CASE WHEN v % 4 = 2 THEN NULL ELSE ('\u042f' || v)::VARCHAR END"}
    };
    // column, text source type, lazy target without a NULL sentinel, follow-up type, index of the source or null
    private static final String[][] NO_SENTINEL_CONVERSIONS = {
            {"vbo", "VARCHAR", "BOOLEAN", "SYMBOL", null},
            {"vby", "VARCHAR", "BYTE", "INT", null},
            {"vsh", "VARCHAR", "SHORT", "LONG", null},
            {"vch", "VARCHAR", "CHAR", "STRING", null},
            {"sbo", "STRING", "BOOLEAN", "VARCHAR", null},
            {"sby", "STRING", "BYTE", "SYMBOL", null},
            {"ssh", "STRING", "SHORT", "DOUBLE", null},
            {"sch", "STRING", "CHAR", "SYMBOL", null},
            {"ybo", "SYMBOL", "BOOLEAN", "STRING", null},
            {"yby", "SYMBOL", "BYTE", "LONG", null},
            {"ysh", "SYMBOL", "SHORT", "SYMBOL", null},
            {"ych", "SYMBOL", "CHAR", "STRING", null},
            {"yix", "SYMBOL", "BYTE", "SYMBOL", "ADD INDEX"},
            {"ypo", "SYMBOL", "SHORT", "INT", "ADD INDEX TYPE POSTING"}
    };
    private static final String[] NO_SENTINEL_QUERIES = {
            "SELECT * FROM {t}",
            "SELECT v, ts, {c} FROM {t} WHERE {c} IS NULL",
            "SELECT {c}, count() FROM {t} GROUP BY {c} ORDER BY {c}",
            "SELECT v, {c} FROM {t} ORDER BY {c}, v",
            "SELECT v, ts, {c} FROM {t} LATEST ON ts PARTITION BY {c}"
    };
    // row r of a column at or after its top takes value (r - top) % 17: NULL, empty, numbers in and out of the
    // BYTE and SHORT ranges, text that does not parse, BOOLEAN keywords, a 2-byte and a 4-byte UTF-8 character
    private static final String[] NO_SENTINEL_VALUES = {
            null, "", "0", "-128", "127", "128", "-129", "32767", "-32768", "40000", "abc", " 12", "true", "TRUE", "false",
            "\u042f", "\ud834\udd1e"
    };
    private static final String[] ROW_GROUP_TOP_COLUMNS = {
            "bo BOOLEAN", "bt BYTE", "sh SHORT", "ch CHAR", "i INT", "l LONG", "f FLOAT", "d DOUBLE", "dt DATE", "tm TIMESTAMP",
            "tn TIMESTAMP_NS", "u UUID", "ip IPv4", "l256 LONG256", "g1 GEOHASH(1c)", "g3 GEOHASH(3c)", "g6 GEOHASH(6c)",
            "g12 GEOHASH(12c)", "d18 DECIMAL(18, 2)", "d38 DECIMAL(38, 4)", "d76 DECIMAL(76, 10)", "vc VARCHAR", "st STRING",
            "bin BINARY", "sy SYMBOL", "s1 SYMBOL", "s2 SYMBOL", "s3 SYMBOL", "s4 SYMBOL", "arr DOUBLE[]", "arr2 DOUBLE[][]"
    };
    private static final String[] ROW_GROUP_TOP_INDEXES = {
            "s1 ADD INDEX", "s2 ADD INDEX CAPACITY 2", "s3 ADD INDEX TYPE POSTING", "s4 ADD INDEX TYPE POSTING INCLUDE (d, vc)"
    };
    private static final String ROW_GROUP_TOP_LONG_TEXT = "long-\u00e9\ud834\udd1e-".repeat(20);
    // BOOLEAN, BYTE, SHORT and CHAR, the first columns, have no NULL sentinel
    private static final int ROW_GROUP_TOP_NO_SENTINEL_COLUMNS = 4;
    private static final String[] ROW_GROUP_TOP_QUERIES = {
            "SELECT * FROM {t}",
            "SELECT * FROM {t} WHERE ts IN '2024-01-01'",
            "SELECT v, ts, {s} FROM {t} WHERE {s} = 'x'",
            "SELECT v, ts, {s} FROM {t} WHERE {s} = ''",
            "SELECT v, ts, {s} FROM {t} WHERE {s} IS NULL",
            "SELECT v, ts, {s} FROM {t} WHERE {s} IN ('x', 'y')",
            "SELECT v, ts, {s} FROM {t} WHERE {s} != 'x'",
            "SELECT v, ts, {s} FROM {t} WHERE {s} = 'x' ORDER BY ts DESC",
            "SELECT v, ts, {s} FROM {t} LATEST ON ts PARTITION BY {s}",
            "SELECT v, ts, {s} FROM {t} WHERE {s} = 'y' LATEST ON ts PARTITION BY {s}",
            "SELECT count() FROM {t} WHERE {s} = 'x'",
            "SELECT ts, {s}, d, vc FROM {t} WHERE {s} = 'x'",
            "SELECT ts, {s}, d, vc FROM {t} WHERE {s} IN ('x', 'y')"
    };
    // per column of ROW_GROUP_TOP_COLUMNS, the literal of each value class: NULL, zero or empty, the
    // smallest, the largest, other
    private static final String[][] ROW_GROUP_TOP_VALUES = {
            {"NULL", "false", "false", "true", "true"},
            {"NULL", "0::BYTE", "(-128)::BYTE", "127::BYTE", "7::BYTE"},
            {"NULL", "0::SHORT", "(-32768)::SHORT", "32767::SHORT", "300::SHORT"},
            {"NULL", "'a'", "'\u042f'", "'z'", "'\u20ac'"},
            {"NULL", "0", "-2147483647", "2147483647", "7"},
            {"NULL", "0", "-9223372036854775807", "9223372036854775807", "42"},
            {"NULL", "(-0.0)::FLOAT", "(-3.4E38)::FLOAT", "3.4E38::FLOAT", "1.5E-30::FLOAT"},
            {"NULL", "-0.0", "-1.7976931348623157E308", "1.7976931348623157E308", "4.9E-324"},
            {"NULL", "'1970-01-01'::DATE", "'1969-12-31T23:59:59.999Z'::DATE", "'9999-12-31'::DATE", "'2024-02-29'::DATE"},
            {"NULL", "'1970-01-01T00:00:00.000000Z'::TIMESTAMP", "'1900-01-01T00:00:00.000001Z'::TIMESTAMP", "'2262-01-01T00:00:00.000000Z'::TIMESTAMP", "'2024-02-29T12:00:00.000000Z'::TIMESTAMP"},
            {"NULL", "'1970-01-01T00:00:00.000000000Z'::TIMESTAMP_NS", "'1900-01-01T00:00:00.000000001Z'::TIMESTAMP_NS", "'2262-04-11T00:00:00.000000000Z'::TIMESTAMP_NS", "'2024-02-29T12:00:00.123456789Z'::TIMESTAMP_NS"},
            {"NULL", "'00000000-0000-0000-0000-000000000000'", "'80000000-0000-0000-0000-000000000001'", "'ffffffff-ffff-ffff-ffff-ffffffffffff'", "'11111111-2222-3333-4444-555555555555'"},
            {"NULL", "'0.0.0.1'", "'1.2.3.4'", "'255.255.255.255'", "'10.0.0.7'"},
            {"NULL", "'0x01'::LONG256", "'0x8000000000000000000000000000000000000000000000000000000000000001'::LONG256", "'0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'::LONG256", "'0x2a'::LONG256"},
            {"NULL", "#0", "#z", "#u", "#s"},
            {"NULL", "#000", "#zzz", "#u33", "#sp0"},
            {"NULL", "#000000", "#zzzzzz", "#u33d8b", "#sp052r"},
            {"NULL", "#000000000000", "#zzzzzzzzzzzz", "#u33d8b121234", "#sp052r0h1234"},
            {"NULL", "'0'::DECIMAL(18, 2)", "'-9999999999999999.99'::DECIMAL(18, 2)", "'9999999999999999.99'::DECIMAL(18, 2)", "'1.25'::DECIMAL(18, 2)"},
            {"NULL", "'0'::DECIMAL(38, 4)", "'-" + "9".repeat(34) + ".9999'::DECIMAL(38, 4)", "'" + "9".repeat(34) + ".9999'::DECIMAL(38, 4)", "'12.3456'::DECIMAL(38, 4)"},
            {"NULL", "'0'::DECIMAL(76, 10)", "'-" + "9".repeat(66) + "." + "9".repeat(10) + "'::DECIMAL(76, 10)", "'" + "9".repeat(66) + "." + "9".repeat(10) + "'::DECIMAL(76, 10)", "'1.0000000001'::DECIMAL(76, 10)"},
            {"NULL", "''", "'\u00e9\ud834\udd1e'", "'" + ROW_GROUP_TOP_LONG_TEXT + "'", "'vc'"},
            {"NULL", "''", "'\u042f\ud834\udd1e'", "'" + ROW_GROUP_TOP_LONG_TEXT + "'", "'st'"},
            {"NULL", "''::BINARY", "'b'::BINARY", "'" + ROW_GROUP_TOP_LONG_TEXT + "'::BINARY", "'xyz'::BINARY"},
            {"NULL", "''", "'\u00e9'", "'k1'", "'k2'"},
            {"NULL", "''", "'x'", "'y'", "'x'"},
            {"NULL", "''", "'x'", "'y'", "'x'"},
            {"NULL", "''", "'x'", "'y'", "'x'"},
            {"NULL", "''", "'x'", "'y'", "'x'"},
            {"NULL", "ARRAY[0.0]", "ARRAY[-1.5, 2.5]", "ARRAY[1.0, 2.0, 3.0]", "ARRAY[4.5]"},
            {"NULL", "ARRAY[[0.0]]", "ARRAY[[1.0, 2.0], [3.0, 4.0]]", "ARRAY[[-1.5]]", "ARRAY[[5.0, 6.0]]"}
    };
    private final SCSequence eventSubSequence = new SCSequence();
    private final TestTimestampType timestampType;

    public AlterTableConvertPartitionTest() {
        this.timestampType = TestUtils.getTimestampType();
    }

    @Test
    public void testConvertAllPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertAllPartitions";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-06-11T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(3, '2024-06-12T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(4, '2024-06-12T00:00:01.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(5, '2024-06-15T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(6, '2024-06-12T00:00:02.000000Z')"
            );

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET WHERE timestamp > 0");

            assertPartitionExists(tableName, "2024-06-10.8");
            assertPartitionExists(tableName, "2024-06-11.6");
            assertPartitionExists(tableName, "2024-06-12.7");
            // last partition is not converted
        });
    }

    @Test
    public void testConvertAllNullDeltaDataPageRoundTrips() throws Exception {
        // Regression for the all-null DELTA_BINARY_PACKED page bug (questdb#7212).
        // The bug needs a real, delta-encoded data page that is entirely null,
        // read EAGERLY: a lazy SELECT fills nulls from definition levels without
        // decoding the values page, and a column top is written without the delta
        // value encoder, so neither reproduces. Here 'b' has column_top=0 plus a
        // leading null run that fills one whole delta page (page size set small),
        // and converting the parquet partition back to native decodes every page
        // eagerly -- the same read WAL apply performs.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_DATA_PAGE_SIZE, 64); // 8 LONG rows per page
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE x (ts TIMESTAMP, b LONG) TIMESTAMP(ts) PARTITION BY DAY");
            // Partition 1970-01-01: b is null for the first full page (8 rows), then 99.
            execute("INSERT INTO x SELECT (x * 1000000)::timestamp, CASE WHEN x <= 8 THEN NULL ELSE 99 END FROM long_sequence(9)");
            // 1970-01-02 keeps the active partition out of the conversion.
            execute("INSERT INTO x VALUES ('1970-01-02T00:00:00.000000Z', 7)");
            execute("ALTER TABLE x ALTER COLUMN b SET PARQUET(DELTA_BINARY_PACKED)");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '1970-01-01'");

            // Lazy read over the parquet partition returns nulls (no eager decode).
            assertQuery("SELECT b FROM x WHERE ts = '1970-01-01T00:00:01.000000Z'")
                    .noLeakCheck()
                    .returns("b\nnull\n");

            // Eager full-page decode: pre-fix this fails with
            // "delta binary packed block size must be greater than zero".
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '1970-01-01'");

            // Data survived the parquet round trip.
            assertQuery("SELECT b FROM x WHERE ts = '1970-01-01T00:00:09.000000Z'")
                    .noLeakCheck()
                    .returns("b\n99\n");
        });
    }

    @Test
    public void testConvertAllPartitionsToParquetAndBack() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertAllPartitionsToParquetAndBack";
            createTableStr(
                    "INSERT INTO " + tableName + " VALUES(1, 'abc', '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, 'edf', '2024-06-11T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(3, 'abc', '2024-06-12T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(4, 'edf', '2024-06-12T00:00:01.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(5, 'abc', '2024-06-15T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(6, 'edf', '2024-06-12T00:00:02.000000Z')"
            );

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET WHERE timestamp > 0");

            assertPartitionExists(tableName, "2024-06-10.8");
            assertPartitionExists(tableName, "2024-06-11.6");
            assertPartitionExists(tableName, "2024-06-12.7");

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO NATIVE WHERE timestamp > 0");
            assertPartitionDoesNotExist(tableName, "2024-06-10.12");
            assertPartitionDoesNotExist(tableName, "2024-06-11.11");
            assertPartitionDoesNotExist(tableName, "2024-06-12.10");
            assertQuery("select * from " + tableName)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .expectSize()
                    .returns(replaceTimestampSuffix("""
                            id\tstr\ttimestamp
                            1\tabc\t2024-06-10T00:00:00.000000Z
                            2\tedf\t2024-06-11T00:00:00.000000Z
                            3\tabc\t2024-06-12T00:00:00.000000Z
                            4\tedf\t2024-06-12T00:00:01.000000Z
                            6\tedf\t2024-06-12T00:00:02.000000Z
                            5\tabc\t2024-06-15T00:00:00.000000Z
                            """, timestampType.getTypeName()));
        });
    }

    @Test
    public void testConvertAllPartitionsWithBloomFilterColumns() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertWithBloom";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-06-11T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(3, '2024-06-12T00:00:00.000000Z')"
            );

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (bloom_filter_columns = 'id')");
            assertPartitionExists(tableName, "2024-06-10.3");
        });
    }

    @Test
    public void testConvertAllPartitionsWithBloomFilterColumnsAndFpp() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertWithBloomAndFpp";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-06-11T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(3, '2024-06-12T00:00:00.000000Z')"
            );

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (bloom_filter_columns = 'id', fpp = '0.05')");
            assertPartitionExists(tableName, "2024-06-10.3");
        });
    }

    @Test
    public void testConvertAllPartitionsWithBloomFilterFpp() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertWithFpp";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-06-11T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(3, '2024-06-12T00:00:00.000000Z')"
            );

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (fpp = '0.01')");
            assertPartitionExists(tableName, "2024-06-10.3");
        });
    }

    @Test
    public void testConvertAllPartitionsWithBloomFilterWhereClause() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertWithBloomWhere";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-06-11T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(3, '2024-06-12T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(4, '2024-06-15T00:00:00.000000Z')"
            );

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET WHERE timestamp > 0 WITH (bloom_filter_columns = 'id', fpp = '0.1')");
            assertPartitionExists(tableName, "2024-06-10.6");
            assertPartitionExists(tableName, "2024-06-11.4");
            assertPartitionExists(tableName, "2024-06-12.5");
        });
    }

    @Test
    public void testConvertAllPartitionsWithBloomFilterWithClauseErrors() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "x";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-06-11T00:00:00.000000Z')"
            );

            // missing '('
            assertQuery("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH bloom_filter_columns")
                    .fails(66, "'(' expected");

            // unknown option
            assertQuery("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (unknown_option = 'val')")
                    .fails(67, "bloom_filter_columns or fpp expected");

            // missing '=' after bloom_filter_columns
            assertQuery("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (bloom_filter_columns 'id')")
                    .fails(88, "'=' expected");

            // missing '=' after fpp
            assertQuery("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (fpp '0.05')")
                    .fails(71, "'=' expected");

            // fpp = 0 (out of range)
            assertQuery("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (fpp = 0)")
                    .fails(73, "fpp must be between 0 and 1 (exclusive)");

            // fpp = 1 (out of range)
            assertQuery("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (fpp = 1)")
                    .fails(73, "fpp must be between 0 and 1 (exclusive)");

            // fpp = 1.5 (out of range)
            assertQuery("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (fpp = '1.5')")
                    .fails(73, "fpp must be between 0 and 1 (exclusive)");

            // fpp = -0.1 (negative, out of range)
            assertQuery("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (fpp = '-0.1')")
                    .fails(73, "fpp must be between 0 and 1 (exclusive)");

            // fpp = non-numeric
            assertQuery("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (fpp = abc)")
                    .fails(73, "invalid fpp value");

            // non-existent bloom filter column
            assertQuery("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (bloom_filter_columns = 'nonexistent')")
                    .fails(90, "bloom_filter_columns contains non-existent column: nonexistent");

            // bad delimiter (missing ',' or ')')
            assertQuery("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06-10' WITH (bloom_filter_columns = 'id' fpp = 0.05)")
                    .fails(95, "',' or ')' expected");
        });
    }

    // ============================================================
    // Metadata-driven bloom filter conversion tests (A1-A4, B1, C1-C2, D1-D2, H1)
    // ============================================================

    @Test
    public void testMetadataBloomFilterBasic() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val INT PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);

            // Convert WITHOUT explicit bloom_filter_columns — should use metadata
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // Verify bloom filter is active by querying for a non-existent value
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 25_000")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue("metadata bloom filter should enable row group skipping",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // Verify existing value is still found
            assertQuery("SELECT val FROM x WHERE val = 50_000")
                    .noLeakCheck()
                    .returns("val\n50000\n");
        });
    }

    @Test
    public void testMetadataBloomFilterSelectiveColumns() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        a INT PARQUET(BLOOM_FILTER),
                        b INT,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, 1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, 50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, 100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, 100_001, '2024-01-02T01:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // Column 'a' has bloom filter — row groups should be skipped
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT a FROM x WHERE a = 25_000")
                    .noLeakCheck()
                    .returns("a\n");
            long skippedWithBloom = ParquetRowGroupFilter.getRowGroupsSkipped();
            Assert.assertTrue("column with BLOOM_FILTER should skip row groups", skippedWithBloom > 0);
        });
    }

    @Test
    public void testMetadataBloomFilterNoColumns() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val INT,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-02T01:00:00.000000Z')
                    """);

            // No bloom filter columns — should convert without error
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 1")
                    .noLeakCheck()
                    .returns("""
                            val
                            1
                            """);
        });
    }

    @Test
    public void testMetadataBloomFilterWithEncodingAndCompression() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val INT PARQUET(DELTA_BINARY_PACKED, ZSTD(3), BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 25_000")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue("bloom filter with custom encoding should still skip row groups",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMetadataBloomFilterExplicitOverride() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            // Column 'a' has bloom filter in metadata, column 'b' does not
            execute("""
                    CREATE TABLE x (
                        a INT PARQUET(BLOOM_FILTER),
                        b INT,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, 1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, 50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, 100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, 100_001, '2024-01-02T01:00:00.000000Z')
                    """);

            // Explicit override: bloom filter on 'b' only, ignoring metadata on 'a'
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'b')");

            // Column 'b' should have bloom filter (from explicit override)
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b = 25_000")
                    .noLeakCheck()
                    .returns("b\n");
            Assert.assertTrue("explicit bloom_filter_columns should override metadata",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMetadataBloomFilterSetViaDdlThenConvert() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val INT,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);

            // Add bloom filter via ALTER TABLE, then convert
            execute("ALTER TABLE x ALTER COLUMN val SET PARQUET(BLOOM_FILTER)");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 25_000")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue("bloom filter set via ALTER TABLE should be used during conversion",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMetadataBloomFilterClearViaDdlThenConvert() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val INT PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);

            // Clear bloom filter, then convert
            execute("ALTER TABLE x ALTER COLUMN val SET PARQUET(default)");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // With bloom filter cleared, row group skipping should not happen via bloom filter
            // (min/max statistics may still skip some groups, so we just verify no error)
            assertQuery("SELECT val FROM x WHERE val = 50_000")
                    .noLeakCheck()
                    .returns("val\n50000\n");
        });
    }

    @Test
    public void testMetadataBloomFilterWithDeletedColumn() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        a INT PARQUET(BLOOM_FILTER),
                        b INT PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, 1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, 50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, 100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, 100_001, '2024-01-02T01:00:00.000000Z')
                    """);

            // Drop column 'a', then convert — bloom filter on 'b' should still work
            execute("ALTER TABLE x DROP COLUMN a");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b = 25_000")
                    .noLeakCheck()
                    .returns("b\n");
            Assert.assertTrue("bloom filter on remaining column should work after sibling column dropped",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMetadataBloomFilterWithColumnTop() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        a INT,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);

            // Add column with bloom filter after initial data — creates a column top
            execute("ALTER TABLE x ADD COLUMN b INT");
            execute("ALTER TABLE x ALTER COLUMN b SET PARQUET(BLOOM_FILTER)");
            execute("""
                    INSERT INTO x VALUES
                    (3, '2024-01-01T02:00:00.000000Z', 50_000),
                    (4, '2024-01-02T01:00:00.000000Z', 100_000)
                    """);

            // Convert — column 'b' has a column top in the first partition
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // Verify data is correct (nulls for column top rows)
            assertQuery("SELECT * FROM x WHERE ts < '2024-01-02'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            a\tts\tb
                            1\t2024-01-01T00:00:00.000000Z\tnull
                            2\t2024-01-01T01:00:00.000000Z\tnull
                            3\t2024-01-01T02:00:00.000000Z\t50000
                            """);
        });
    }

    @Test
    public void testMetadataBloomFilterSymbolColumn() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        s SYMBOL PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    ('aaa', '2024-01-01T00:00:00.000000Z'),
                    ('bbb', '2024-01-01T01:00:00.000000Z'),
                    ('ccc', '2024-01-01T02:00:00.000000Z'),
                    ('ddd', '2024-01-02T01:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT s FROM x WHERE s = 'zzz'")
                    .noLeakCheck()
                    .returns("s\n");
            Assert.assertTrue("bloom filter on SYMBOL column should enable row group skipping",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQuery("SELECT s FROM x WHERE s = 'aaa'")
                    .noLeakCheck()
                    .returns("s\naaa\n");
        });
    }

    @Test
    public void testMetadataBloomFilterSurvivesNativeRoundTrip() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val INT PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);

            // Convert to parquet, back to native, then re-convert
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE WHERE ts >= 0");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // Bloom filter metadata should still be active after round-trip
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 25_000")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue("bloom filter should survive parquet→native→parquet cycle",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMetadataBloomFilterWithNulls() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val INT PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (NULL, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (NULL, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // Bloom filter should still skip row groups for absent values
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 25_000")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue("bloom filter should handle NULL values and still skip row groups",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // Present value should still be found
            assertQuery("SELECT val FROM x WHERE val = 50_000")
                    .noLeakCheck()
                    .returns("val\n50000\n");
        });
    }

    @Test
    public void testMetadataBloomFilterWithEmptyStrings() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val VARCHAR PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    ('', '2024-01-01T00:00:00.000000Z'),
                    ('hello', '2024-01-01T01:00:00.000000Z'),
                    ('world', '2024-01-01T02:00:00.000000Z'),
                    ('test', '2024-01-02T01:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // Bloom filter should skip row groups for absent values
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 'nonexistent'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue("bloom filter should handle empty strings and skip row groups for absent values",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // Empty string should still be found
            assertQuery("SELECT val FROM x WHERE val = ''")
                    .noLeakCheck()
                    .returns("val\n\n");
        });
    }

    @Test
    public void testMetadataBloomFilterMultiplePartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val INT PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-02T00:00:00.000000Z'),
                    (200_000, '2024-01-02T01:00:00.000000Z'),
                    (300_000, '2024-01-03T00:00:00.000000Z'),
                    (400_000, '2024-01-03T01:00:00.000000Z'),
                    (999_999, '2024-01-04T00:00:00.000000Z')
                    """);

            // Convert all sealed partitions at once
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // Bloom filter should work across multiple parquet partitions
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 25_000")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue("bloom filter should work across multiple converted partitions",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // Verify a value in a later partition is found
            assertQuery("SELECT val FROM x WHERE val = 300_000")
                    .noLeakCheck()
                    .returns("val\n300000\n");
        });
    }

    @Test
    public void testMetadataBloomFilterLong() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val LONG PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (10_000_000_000, '2024-01-01T01:00:00.000000Z'),
                    (20_000_000_000, '2024-01-01T02:00:00.000000Z'),
                    (30_000_000_000, '2024-01-02T01:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 5_000_000_000")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue("bloom filter on LONG column should skip row groups",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQuery("SELECT val FROM x WHERE val = 10_000_000_000")
                    .noLeakCheck()
                    .returns("val\n10000000000\n");
        });
    }

    @Test
    public void testMetadataBloomFilterVarchar() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("""
                    CREATE TABLE x (
                        val VARCHAR PARQUET(BLOOM_FILTER),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    ('alpha', '2024-01-01T00:00:00.000000Z'),
                    ('bravo', '2024-01-01T01:00:00.000000Z'),
                    ('charlie', '2024-01-01T02:00:00.000000Z'),
                    ('delta', '2024-01-02T01:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 'zulu'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue("bloom filter on VARCHAR column should skip row groups",
                    ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQuery("SELECT val FROM x WHERE val = 'alpha'")
                    .noLeakCheck()
                    .returns("val\nalpha\n");
        });
    }

    @Test
    public void testConvertLastPartition() throws Exception {
        final long rows = 10;
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table x as (select" +
                            " x id," +
                            " rnd_boolean() a_boolean," +
                            " rnd_byte() a_byte," +
                            " timestamp_sequence('2024-06', 500)::" + timestampType.getTypeName() + " designated_ts" +
                            " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month"
            );

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06'");
            assertPartitionDoesNotExist("x", "2024-06.1");

            execute("insert into x(designated_ts) values('1970-01')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '1970-01'");
            assertPartitionExists("x", "1970-01.2");
        });
    }

    @Test
    public void testConvertLastPartitionWal() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "CREATE TABLE x AS (SELECT" +
                            " x id," +
                            " timestamp_sequence('2024-06', 500)::" + timestampType.getTypeName() + " designated_ts" +
                            " FROM long_sequence(10)) timestamp(designated_ts) PARTITION BY MONTH WAL"
            );
            drainWalQueue();

            // Verify initial row count.
            assertQuery("SELECT count() FROM x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n10\n");

            // Convert the last (and only) partition to parquet.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06'");
            drainWalQueue();

            // Verify parquet file exists and count is still correct after conversion.
            assertPartitionExists("x", "2024-06.1");
            assertQuery("SELECT count() FROM x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n10\n");

            // Insert O3 data into the same parquet partition.
            execute("INSERT INTO x(id, designated_ts) VALUES (100, '2024-06-15T00:00:00.000000Z')");
            drainWalQueue();

            // Verify data correctness: original 10 rows + 1 new row.
            assertQuery("SELECT count() FROM x")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n11\n");

            // Verify the O3-inserted row is present.
            assertQuery("SELECT id, designated_ts FROM x WHERE id = 100")
                    .noLeakCheck()
                    .timestamp("designated_ts")
                    .returns("id\tdesignated_ts\n" +
                            "100\t2024-06-15T00:00:00.000000" + (timestampType == TestTimestampType.NANO ? "000Z" : "Z") + "\n");
        });
    }

    @Test
    public void testConvertListPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertListPartitions";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-06-11T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(3, '2024-06-12T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(4, '2024-06-12T00:00:01.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(5, '2024-06-15T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(6, '2024-06-12T00:00:02.000000Z')"
            );

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-06-10', '2024-06-11', '2024-06-12'");

            assertPartitionExists(tableName, "2024-06-10.6");
            assertPartitionExists(tableName, "2024-06-11.7");
            assertPartitionExists(tableName, "2024-06-12.8");
            assertPartitionDoesNotExist(tableName, "2024-06-15.3");
        });
    }

    @Test
    public void testConvertListZeroSizeArrayData() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table x (" +
                            " an_array double[]," +
                            " a_ts timestamp" +
                            ") timestamp(a_ts) partition by month;"
            );

            execute("insert into x(an_array, a_ts) values(array[], '2024-07');");
            execute("insert into x(an_array, a_ts) values(array[1.0, 2.0, 3.0], '2024-08');");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE a_ts > 0;");
            assertPartitionExists("x", "2024-07.2");
        });
    }

    @Test
    public void testConvertListZeroSizeVarcharData() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table x as (select" +
                            " case when x % 2 = 0 then rnd_varchar(1, 40, 1) end as a_varchar," +
                            " to_timestamp('2024-07', 'yyyy-MM')::" + timestampType.getTypeName() + " as a_ts," +
                            " from long_sequence(1)) timestamp (a_ts) partition by MONTH"
            );

            execute("insert into x(a_varchar, a_ts) values('', '2024-08')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE a_ts > 0");
            assertPartitionExists("x", "2024-07.2");
        });
    }

    @Test
    public void testConvertPartitionAllTypes() throws Exception {
        final long rows = 1000;
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table x as (select" +
                            " x id," +
                            " rnd_boolean() a_boolean," +
                            " rnd_byte() a_byte," +
                            " rnd_short() a_short," +
                            " rnd_char() a_char," +
                            " rnd_int() an_int," +
                            " rnd_long() a_long," +
                            " rnd_float() a_float," +
                            " rnd_double() a_double," +
                            " rnd_symbol('a','b','c') a_symbol," +
                            " rnd_geohash(4) a_geo_byte," +
                            " rnd_geohash(8) a_geo_short," +
                            " rnd_geohash(16) a_geo_int," +
                            " rnd_geohash(32) a_geo_long," +
                            " rnd_str('hello', 'world', '!') a_string," +
                            " rnd_bin() a_bin," +
                            " rnd_varchar('ганьба','слава','добрий','вечір') a_varchar," +
                            " rnd_ipv4() a_ip," +
                            " rnd_uuid4() a_uuid," +
                            " rnd_long256() a_long256," +
                            " to_long128(rnd_long(), rnd_long()) a_long128," +
                            " rnd_double_array(1) an_array," +
                            " cast(timestamp_sequence(600000000000, 700) as date) a_date," +
                            " timestamp_sequence(500000000000, 600)::" + timestampType.getTypeName() + " a_ts," +
                            " timestamp_sequence(400000000000, " + Micros.DAY_MICROS / 12 + ")::" + timestampType.getTypeName() + " designated_ts" +
                            " from long_sequence(" + rows + ")), index(a_symbol) timestamp(designated_ts) partition by month"
            );

            assertQuery("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06'")
                    .fails(0, "cannot convert partition to parquet, partition does not exist");

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '1970-01', '1970-02'");
            assertPartitionExists("x", "1970-01.1");
            assertPartitionExists("x", "1970-02.2");
            assertPartitionDoesNotExist("x", "1970-03.3");
        });
    }

    @Test
    public void testConvertPartitionWithZeroRowIndexedSymbol() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "x";
            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO " + tableName + " SELECT timestamp_sequence(0, 1000000), x::int FROM long_sequence(10)");
            drainWalQueue();

            // sym_top is added once the partition already holds all its rows, so its
            // column top equals the partition's row count: it holds zero real rows here.
            execute("ALTER TABLE " + tableName + " ADD COLUMN sym_top SYMBOL INDEX");
            drainWalQueue();

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            drainWalQueue();

            // Force a cold reopen of the writer. This used to crash configuring
            // sym_top's BITMAP indexer, because its .k/.v files were never carried
            // into the new parquet partition directory (colTop == partitionRowCount
            // was treated the same as "column does not exist in this partition").
            engine.releaseInactive();
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertNotNull(writer);
            }
        });
    }

    @Test
    public void testConvertPartitionBrokenSymbols() throws Exception {
        final long rows = 10;
        final String tableName = "x";
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table " + tableName + " as (select" +
                            " x id," +
                            " rnd_symbol('a','b','c') a_symbol," +
                            " timestamp_sequence(400000000000, " + Micros.DAY_MICROS * 5 + ")::" + timestampType.getTypeName() + " designated_ts" +
                            " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month"
            );

            engine.releaseInactive();
            try (Path path = new Path().of(configuration.getDbRoot())) {
                TableToken tableToken = engine.getTableTokenIfExists(tableName);
                path.concat(tableToken.getDirName()).concat("a_symbol").put(".o");
                FilesFacade ff = configuration.getFilesFacade();
                Assert.assertTrue(ff.exists(path.$()));
                long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue(configuration.getFilesFacade().truncate(fd, SymbolMapWriter.HEADER_SIZE - 2));
                ff.close(fd);
            }
            try {
                execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '1970-01'");
                Assert.fail();
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), " SymbolMap is too short");
            }
            assertPartitionDoesNotExist(tableName, "1970-01.1");
        });
    }

    @Test
    public void testConvertPartitionDoesNotFsyncParquetMetadataInNoSyncMode() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");

        final AtomicLong pmFd = new AtomicLong(-1);
        final AtomicReference<String> partitionDirRef = new AtomicReference<>();
        final AtomicLong partitionDirFd = new AtomicLong(-1);
        final AtomicBoolean pmFsynced = new AtomicBoolean();
        final AtomicBoolean partitionDirFsynced = new AtomicBoolean();

        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public void fsync(long fd) {
                if (fd == pmFd.get()) {
                    pmFsynced.set(true);
                }
                super.fsync(fd);
            }

            @Override
            public void fsyncAndClose(long fd) {
                if (fd == partitionDirFd.get()) {
                    partitionDirFsynced.set(true);
                }
                super.fsyncAndClose(fd);
            }

            @Override
            public long openRONoCache(LPSZ path) {
                long fd = super.openRONoCache(path);
                String parent = partitionDirRef.get();
                if (parent != null && parent.equals(Utf8s.stringFromUtf8Bytes(path))) {
                    partitionDirFd.set(fd);
                }
                return fd;
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                long fd = super.openRW(name, opts);
                if (fd > -1 && Utf8s.endsWithAscii(name, Files.SEPARATOR + PARQUET_METADATA_FILE_NAME)) {
                    String pmPath = Utf8s.stringFromUtf8Bytes(name);
                    pmFd.set(fd);
                    partitionDirRef.set(pmPath.substring(0, pmPath.length() - PARQUET_METADATA_FILE_NAME.length() - 1));
                }
                return fd;
            }
        };

        assertMemoryLeak(ff, () -> {
            final String tableName = "testConvertParquetMetadataNoSync";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-06-15T00:00:00.000000Z')"
            );
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-06-10'");

            Assert.assertNotEquals("openRW for parquet metadata file was not intercepted", -1L, pmFd.get());
            Assert.assertFalse("parquet metadata file must not be fsynced when commit mode is nosync", pmFsynced.get());
            Assert.assertEquals("partition dir must not be opened with openRONoCache when commit mode is nosync", -1L, partitionDirFd.get());
            Assert.assertFalse("partition dir must not be fsynced when commit mode is nosync", partitionDirFsynced.get());
        });
    }

    @Test
    public void testConvertPartitionFsyncsParquetMetadataInSyncMode() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");

        final AtomicLong pmFd = new AtomicLong(-1);
        final AtomicReference<String> partitionDirRef = new AtomicReference<>();
        final AtomicLong partitionDirFd = new AtomicLong(-1);
        final AtomicBoolean pmFsynced = new AtomicBoolean();
        final AtomicBoolean partitionDirFsynced = new AtomicBoolean();

        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public void fsync(long fd) {
                if (fd == pmFd.get()) {
                    pmFsynced.set(true);
                }
                super.fsync(fd);
            }

            @Override
            public void fsyncAndClose(long fd) {
                if (fd == partitionDirFd.get()) {
                    partitionDirFsynced.set(true);
                }
                super.fsyncAndClose(fd);
            }

            @Override
            public long openRONoCache(LPSZ path) {
                long fd = super.openRONoCache(path);
                String parent = partitionDirRef.get();
                if (parent != null && parent.equals(Utf8s.stringFromUtf8Bytes(path))) {
                    partitionDirFd.set(fd);
                }
                return fd;
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                long fd = super.openRW(name, opts);
                if (fd > -1 && Utf8s.endsWithAscii(name, Files.SEPARATOR + PARQUET_METADATA_FILE_NAME)) {
                    String pmPath = Utf8s.stringFromUtf8Bytes(name);
                    pmFd.set(fd);
                    partitionDirRef.set(pmPath.substring(0, pmPath.length() - PARQUET_METADATA_FILE_NAME.length() - 1));
                }
                return fd;
            }
        };

        assertMemoryLeak(ff, () -> {
            final String tableName = "testConvertParquetMetadataSync";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-06-15T00:00:00.000000Z')"
            );
            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-06-10'");

            Assert.assertNotEquals("openRW for parquet metadata file was not intercepted", -1L, pmFd.get());
            Assert.assertTrue("parquet metadata file must be fsynced when commit mode is sync", pmFsynced.get());
            if (Os.isWindows()) {
                Assert.assertEquals("partition dir fsync is skipped on Windows", -1L, partitionDirFd.get());
                Assert.assertFalse("partition dir fsync is skipped on Windows", partitionDirFsynced.get());
            } else {
                Assert.assertNotEquals("openRONoCache for partition dir was not intercepted", -1L, partitionDirFd.get());
                Assert.assertTrue("partition dir must be fsynced when commit mode is sync", partitionDirFsynced.get());
            }
        });
    }

    @Test
    public void testConvertPartitionParquetAndBackAllTypes() throws Exception {
        final long rows = 1000;
        Overrides overrides = node1.getConfigurationOverrides();
        // test multiple row groups
        overrides.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 101);
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table x as (select" +
                            " x id," +
                            " rnd_boolean() a_boolean," +
                            " rnd_byte() a_byte," +
                            " rnd_short() a_short," +
                            " rnd_char() a_char," +
                            " rnd_int() an_int," +
                            " rnd_long() a_long," +
                            " rnd_float() a_float," +
                            " rnd_double() a_double," +
                            " rnd_symbol('a','b','c') a_symbol," +
                            " rnd_geohash(4) a_geo_byte," +
                            " rnd_geohash(8) a_geo_short," +
                            " rnd_geohash(16) a_geo_int," +
                            " rnd_geohash(32) a_geo_long," +
                            " rnd_str('abc', 'def', 'ghk') a_string," +
                            " rnd_bin() a_bin," +
                            " rnd_ipv4() a_ip," +
                            " rnd_varchar('ганьба','слава','добрий','вечір', '1111111111111111') a_varchar," +
                            " rnd_uuid4() a_uuid," +
                            " rnd_long256() a_long256," +
                            " to_long128(rnd_long(), rnd_long()) a_long128," +
                            " rnd_double_array(1) an_array," +
                            " cast(timestamp_sequence(600000000000, 700) as date) a_date," +
                            " timestamp_sequence(500000000000, 600)::" + timestampType.getTypeName() + " a_ts," +
                            " timestamp_sequence(400000000000, " + Micros.DAY_MICROS / 12 + ")::" + timestampType.getTypeName() + " designated_ts" +
                            " from long_sequence(" + rows + ")), index(a_symbol) timestamp(designated_ts) partition by month"
            );

            execute("create table y as (select * from x)", sqlExecutionContext);

            assertQuery("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-06'")
                    .fails(0, "cannot convert partition to parquet, partition does not exist");

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '1970-01'");
            assertPartitionExists("x", "1970-01.1");
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '1970-01'");
            assertPartitionDoesNotExist("x", "1970-01.1");

            assertSqlCursors("select * from x", "select * from y");
        });
    }

    @Test
    public void testConvertPartitionSymbolMapDoesNotExist() throws Exception {
        final long rows = 10;
        final String tableName = "x";
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table " + tableName + " as (select" +
                            " x id," +
                            " rnd_symbol('a','b','c') a_symbol," +
                            " timestamp_sequence(400000000000, 500)::" + timestampType.getTypeName() + " designated_ts" +
                            " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month"
            );

            engine.releaseInactive();
            try (Path path = new Path().of(configuration.getDbRoot())) {
                TableToken tableToken = engine.getTableTokenIfExists(tableName);
                path.concat(tableToken.getDirName()).concat("a_symbol").put(".o");
                FilesFacade ff = configuration.getFilesFacade();
                ff.remove(path.$());
            }
            try {
                execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '1970-01'");
                Assert.fail();
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(), "SymbolMap does not exist");
            }
            assertPartitionDoesNotExist(tableName, "1970-01.1");
        });
    }

    @Test
    public void testConvertPartitionsWithColTops() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertPartitionsWithColTops";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-06-11T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(3, '2024-06-12T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(4, '2024-06-12T00:00:01.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(5, '2024-06-15T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(6, '2024-06-12T00:00:02.000000Z')"
            );

            execute("ALTER TABLE " + tableName + " ADD COLUMN a int");
            execute("INSERT INTO " + tableName + " VALUES(7, '2024-06-10T00:00:00.000000Z', 1)");

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET WHERE timestamp > 0 AND timestamp < '2024-06-15'");

            assertPartitionExists(tableName, "2024-06-10.10");
            assertPartitionExists(tableName, "2024-06-11.8");
            assertPartitionExists(tableName, "2024-06-12.9");

            assertPartitionDoesNotExist(tableName, "2024-06-15.3");
        });
    }

    @Test
    public void testConvertPartitionsWithColTopsSelect() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertPartitionsWithColTopsSelect";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-11-01T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-11-02T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(3, '2024-11-03T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(4, '2024-11-04T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(5, '2024-11-05T00:00:00.000000Z')"
            );

            execute("ALTER TABLE " + tableName + " ADD COLUMN a int");
            execute("INSERT INTO " + tableName + " VALUES(5, '2024-11-05T00:00:00.000000Z', 5)");
            execute("INSERT INTO " + tableName + " VALUES(6, '2024-11-06T00:00:00.000000Z', 6)");
            execute("INSERT INTO " + tableName + " VALUES(7, '2024-11-07T00:00:00.000000Z', 7)");

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET WHERE timestamp >= 0");

            assertQuery(tableName).timestamp("timestamp").expectSize().returns(replaceTimestampSuffix("""
                    id\ttimestamp\ta
                    1\t2024-11-01T00:00:00.000000Z\tnull
                    2\t2024-11-02T00:00:00.000000Z\tnull
                    3\t2024-11-03T00:00:00.000000Z\tnull
                    4\t2024-11-04T00:00:00.000000Z\tnull
                    5\t2024-11-05T00:00:00.000000Z\tnull
                    5\t2024-11-05T00:00:00.000000Z\t5
                    6\t2024-11-06T00:00:00.000000Z\t6
                    7\t2024-11-07T00:00:00.000000Z\t7
                    """, timestampType.getTypeName()));
        });
    }

    @Test
    public void testConvertSecondCallIgnored() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute(
                    "create table x as (select" +
                            " x a_long," +
                            " to_timestamp('2024-07', 'yyyy-MM')::" + timestampType.getTypeName() + " as a_ts," +
                            " from long_sequence(10)) timestamp (a_ts) partition by MONTH"
            );

            execute("insert into x(a_long, a_ts) values('42', '2024-08')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE a_ts > 0");
            assertPartitionExists("x", "2024-07.2");

            // Second call should be ignored
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE a_ts > 0");
            assertPartitionExists("x", "2024-07.2");
        });
    }

    @Test
    public void testConvertTimestampPartitions() throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertTimestampPartitions";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-06-11T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(3, '2024-06-12T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(4, '2024-06-12T00:00:01.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(5, '2024-06-15T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(6, '2024-06-12T00:00:02.000000Z')"
            );

            assertQuery("select index, name, readOnly, isParquet, parquetFileSize from table_partitions('" + tableName + "')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            index\tname\treadOnly\tisParquet\tparquetFileSize
                            0\t2024-06-10\tfalse\tfalse\t-1
                            1\t2024-06-11\tfalse\tfalse\t-1
                            2\t2024-06-12\tfalse\tfalse\t-1
                            3\t2024-06-15\tfalse\tfalse\t-1
                            """);

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET WHERE timestamp = to_timestamp('2024-06-12', 'yyyy-MM-dd')");

            assertQuery("select index, name, readOnly, isParquet, parquetFileSize   > 0 isNonEmpty, minTimestamp, maxTimestamp from table_partitions('" + tableName + "')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(replaceTimestampSuffix(
                            """
                                    index\tname\treadOnly\tisParquet\tisNonEmpty\tminTimestamp\tmaxTimestamp
                                    0\t2024-06-10\tfalse\tfalse\tfalse\t2024-06-10T00:00:00.000000Z\t2024-06-10T00:00:00.000000Z
                                    1\t2024-06-11\tfalse\tfalse\tfalse\t2024-06-11T00:00:00.000000Z\t2024-06-11T00:00:00.000000Z
                                    2\t2024-06-12\tfalse\ttrue\ttrue\t2024-06-12T00:00:00.000000Z\t2024-06-12T00:00:02.000000Z
                                    3\t2024-06-15\tfalse\tfalse\tfalse\t2024-06-15T00:00:00.000000Z\t2024-06-15T00:00:00.000000Z
                                    """,
                            timestampType.getTypeName()
                    ));

            assertPartitionDoesNotExist(tableName, "2024-06-10");
            assertPartitionDoesNotExist(tableName, "2024-06-11.0");
            assertPartitionExists(tableName, "2024-06-12.6");
            assertPartitionDoesNotExist(tableName, "2024-06-15.3");
        });
    }

    @Test
    public void testConvertToNativeAlterColumnTypeTwoPartitionsColumnTops() throws Exception {
        // ALTER COLUMN TYPE SYMBOL converts both parquet partitions to native in one batch (doCommit=false):
        // 2024-01-01 with a top of 5 of 8 rows for the a_ columns, 2024-01-02 with 9 of 12 for the b_ ones
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE pt (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO pt SELECT x::INT, dateadd('h', (x - 1)::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP) FROM long_sequence(5)");
            execute("ALTER TABLE pt ADD COLUMN a_bt BYTE");
            execute("ALTER TABLE pt ADD COLUMN a_st STRING");
            execute("ALTER TABLE pt ADD COLUMN a_s SYMBOL INDEX");
            execute("""
                    INSERT INTO pt SELECT x::INT, dateadd('h', (x - 1)::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP), x::BYTE, '\u00e9' || x, 's' || (x % 2)
                    FROM long_sequence(8) WHERE x > 5""");
            execute("""
                    INSERT INTO pt SELECT x::INT, dateadd('h', (x - 1)::INT, '2024-01-02T00:00:00.000000Z'::TIMESTAMP), x::BYTE, '\u00e9' || x, 's' || (x % 2)
                    FROM long_sequence(9)""");
            execute("ALTER TABLE pt ADD COLUMN b_bt BYTE");
            execute("ALTER TABLE pt ADD COLUMN b_st STRING");
            execute("ALTER TABLE pt ADD COLUMN b_s SYMBOL INDEX");
            execute("""
                    INSERT INTO pt SELECT x::INT, dateadd('h', (x - 1)::INT, '2024-01-02T00:00:00.000000Z'::TIMESTAMP), x::BYTE, '\u00e9' || x, 's' || (x % 2),
                    (-x)::BYTE, '\ud834\udd1e' || x, 'q' || (x % 2)
                    FROM long_sequence(12) WHERE x > 9""");
            execute("INSERT INTO pt (v, ts, a_s, b_s) VALUES (99, '2024-01-03T00:00:00.000000Z', 's1', 'q1')");
            execute("CREATE TABLE tw AS (SELECT * FROM pt) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
            for (String table : new String[]{"pt", "tw"}) {
                execute("ALTER TABLE " + table + " ALTER COLUMN v TYPE SYMBOL");
            }
            assertPartitionFormat("pt", 0, PartitionFormat.NATIVE);
            assertPartitionFormat("pt", 1, PartitionFormat.NATIVE);
            Assert.assertEquals(5, getColumnTop("pt", 0, "a_bt"));
            Assert.assertEquals(4, getColumnTop("pt", 0, "a_st"));
            Assert.assertEquals(4, getColumnTop("pt", 0, "a_s"));
            Assert.assertEquals(9, getColumnTop("pt", 1, "b_bt"));
            Assert.assertEquals(8, getColumnTop("pt", 1, "b_st"));
            Assert.assertEquals(8, getColumnTop("pt", 1, "b_s"));
            assertSqlCursors("tw", "pt");
            for (String sql : new String[]{
                    "SELECT v, ts, a_s FROM {t} WHERE a_s = 's1'",
                    "SELECT v, ts, a_s FROM {t} WHERE a_s IS NULL",
                    "SELECT v, ts, b_s FROM {t} WHERE b_s = 'q0'",
                    "SELECT v, ts, b_s FROM {t} WHERE b_s IS NULL"
            }) {
                assertSqlCursors(sql.replace("{t}", "tw"), sql.replace("{t}", "pt"));
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertSqlCursors("tw", "pt");
        });
    }

    @Test
    public void testConvertToNativeColumnTopDefaultRowGroupSize() throws Exception {
        // the server default row group size, 100_000 rows: a top of 100_002 rows spans the first row group
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100_000);
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE pt (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO pt SELECT x, timestamp_sequence('2024-01-01T00:00:00.000000Z'::TIMESTAMP, 100_000) FROM long_sequence(100_002)");
            execute("ALTER TABLE pt ADD COLUMN s SYMBOL INDEX");
            execute("ALTER TABLE pt ADD COLUMN d DOUBLE");
            execute("ALTER TABLE pt ADD COLUMN vc STRING");
            execute("ALTER TABLE pt ADD COLUMN vb VARCHAR");
            execute("""
                    INSERT INTO pt VALUES
                    (100_003, '2024-01-01T23:00:00.000000Z', 'x', 1.5, '\u00e9\ud834\udd1e', '12'),
                    (100_004, '2024-01-01T23:00:01.000000Z', NULL, -0.0, '', NULL),
                    (100_005, '2024-01-01T23:00:02.000000Z', 'x', NULL, NULL, '300'),
                    (100_006, '2024-01-02T00:00:00.000000Z', 'y', 2.5, 'p2', '-1')""");
            execute("CREATE TABLE tw AS (SELECT * FROM pt) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            // vb changes lazily to BYTE on the parquet partition
            for (String table : new String[]{"pt", "tw"}) {
                execute("ALTER TABLE " + table + " ALTER COLUMN vb TYPE BYTE");
            }
            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            for (String column : new String[]{"s", "d", "vc"}) {
                Assert.assertEquals("column " + column, 100_000, getColumnTop("pt", 0, column));
            }
            // BYTE has no NULL sentinel: the native top is the column top itself
            Assert.assertEquals(100_002, getColumnTop("pt", 0, "vb"));
            assertSqlCursors("tw", "pt");
            assertQuery("SELECT v, ts, s, d, vc FROM pt WHERE s = 'x'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            v\tts\ts\td\tvc
                            100003\t2024-01-01T23:00:00.000000Z\tx\t1.5\t\u00e9\ud834\udd1e
                            100005\t2024-01-01T23:00:02.000000Z\tx\tnull\t
                            """);
            assertQuery("SELECT count() FROM pt WHERE s IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n100003\n");
            assertSqlCursors("SELECT * FROM tw WHERE s::VARCHAR IS NULL", "SELECT * FROM pt WHERE s IS NULL");
            // the rows before ADD COLUMN read NULL in a type with a NULL sentinel; the NULL inserted after them
            // is a BYTE 0
            execute("ALTER TABLE pt ALTER COLUMN vb TYPE SYMBOL");
            assertQuery("SELECT count() FROM pt WHERE vb IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n100002\n");
            assertQuery("SELECT v, vb FROM pt WHERE v > 100_002")
                    .noLeakCheck()
                    .returns("""
                            v\tvb
                            100003\t12
                            100004\t0
                            100005\t44
                            100006\t-1
                            """);
        });
    }

    @Test
    public void testConvertToNativeColumnTopSpansRowGroups() throws Exception {
        // Columns added after `top` of `rows` rows of partition 2024-01-01, row group size 4. CONVERT TO
        // PARQUET zeroes a top inside the partition, but the file keeps it and the decoder skips every
        // row group it covers. CONVERT TO NATIVE must restore the top from the first row group it decodes.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final int[][] topsAndRows = {{3, 8}, {4, 8}, {5, 8}, {9, 12}, {8, 8}, {7, 8}};
            for (String walMode : new String[]{"WAL", "BYPASS WAL"}) {
                for (int[] topAndRows : topsAndRows) {
                    final int top = topAndRows[0];
                    final int rows = topAndRows[1];
                    final String context = walMode + ", top " + top + " of " + rows;
                    execute("DROP TABLE IF EXISTS pt");
                    execute("DROP TABLE IF EXISTS tw");
                    createRowGroupTopTable("pt", walMode, top, rows, true);
                    createRowGroupTopTable("tw", "BYPASS WAL", top, rows, false);
                    execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                    drainWalQueue();
                    assertRowGroupTopQueriesMatchTwin(context + ", parquet", top < rows);
                    final String fullSql = "SELECT * FROM pt WHERE ts IN '2024-01-01'";
                    final String nullSql = "SELECT v, ts, s1 FROM pt WHERE s1 IS NULL AND ts IN '2024-01-01'";
                    try (
                            RecordCursorFactory fullFactory = select(fullSql);
                            RecordCursor fullCursor = fullFactory.getCursor(sqlExecutionContext);
                            RecordCursorFactory nullFactory = select(nullSql);
                            RecordCursor nullCursor = nullFactory.getCursor(sqlExecutionContext)
                    ) {
                        execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                        drainWalQueue();
                        // cursors opened on the parquet partition keep its rows
                        assertCursorMatchesQuery(context + ", stale full scan", fullSql.replace("pt", "tw"), fullFactory, fullCursor);
                        assertCursorMatchesQuery(context + ", stale index scan", "SELECT v, ts, s1 FROM tw WHERE s1::VARCHAR IS NULL AND ts IN '2024-01-01'", nullFactory, nullCursor);
                    }
                    assertPartitionFormat("pt", 0, PartitionFormat.NATIVE);
                    assertRowGroupTopColumnTops(context, top, rows);
                    assertRowGroupTopLiterals(context, top, rows);
                    assertRowGroupTopQueriesMatchTwin(context + ", native", top < rows);
                    assertQuery("SELECT v, ts, s1 FROM pt WHERE s1 = 'x'")
                            .noLeakCheck()
                            .assertsPlanContaining("Index forward scan on: s1");
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    try {
                        assertRowGroupTopQueriesMatchTwin(context + ", native, JIT off", top < rows);
                    } finally {
                        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                    }
                    engine.releaseAllReaders();
                    engine.releaseAllWriters();
                    assertRowGroupTopColumnTops(context + ", reopen", top, rows);
                    assertRowGroupTopQueriesMatchTwin(context + ", reopen", top < rows);

                    // a second round trip keeps the data and the tops
                    execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                    drainWalQueue();
                    assertRowGroupTopQueriesMatchTwin(context + ", parquet again", top < rows);
                    execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                    drainWalQueue();
                    assertRowGroupTopColumnTops(context + ", native again", top, rows);
                    assertRowGroupTopQueriesMatchTwin(context + ", native again", top < rows);

                    // rows inserted into the converted partition land after the restored tops
                    final String insert = rowGroupTopInsert("pt", top, rows, 2, "2024-01-01T23:");
                    execute(insert);
                    execute(insert.replace("INSERT INTO pt ", "INSERT INTO tw "));
                    drainWalQueue();
                    assertRowGroupTopQueriesMatchTwin(context + ", after insert", top < rows);
                    if (walMode.equals("WAL")) {
                        assertQuery("SELECT suspended FROM wal_tables() WHERE name = 'pt'")
                                .noLeakCheck()
                                .noRandomAccess()
                                .returns("suspended\nfalse\n");
                    }
                }
            }
        });
    }

    @Test
    public void testConvertToNativeColumnTopSpansRowGroupsLastPartition() throws Exception {
        // WAL table whose last partition is parquet: the writer reopens the converted partition with the
        // restored tops and appends after them
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            createRowGroupTopTable("pt", "WAL", 5, 8, true);
            createRowGroupTopTable("tw", "BYPASS WAL", 5, 8, false);
            // the last partition, 2024-01-02, gets its own top: late is added after 5 of its 8 rows
            final String insertBefore = rowGroupTopInsert("pt", 0, 99, 4, "2024-01-02T22:");
            final String insertAfter = rowGroupTopInsert("pt", 0, 103, 3, "2024-01-02T22:", "7");
            for (String table : new String[]{"pt", "tw"}) {
                execute(insertBefore.replace("INSERT INTO pt ", "INSERT INTO " + table + " "));
                execute("ALTER TABLE " + table + " ADD COLUMN late INT");
                execute(insertAfter.replace("INSERT INTO pt ", "INSERT INTO " + table + " "));
            }
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
            drainWalQueue();
            assertPartitionFormat("pt", 1, PartitionFormat.PARQUET);
            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01', '2024-01-02'");
            drainWalQueue();
            assertPartitionFormat("pt", 0, PartitionFormat.NATIVE);
            assertPartitionFormat("pt", 1, PartitionFormat.NATIVE);
            assertRowGroupTopColumnTops("last partition", 5, 8);
            Assert.assertEquals(4, getColumnTop("pt", 1, "late"));
            assertRowGroupTopQueriesMatchTwin("last partition");
            assertSqlCursors("SELECT v, ts, late FROM tw", "SELECT v, ts, late FROM pt");
            final String append = rowGroupTopInsert("pt", 0, 200, 3, "2024-01-02T23:", "8");
            execute(append);
            execute(append.replace("INSERT INTO pt ", "INSERT INTO tw "));
            drainWalQueue();
            assertRowGroupTopQueriesMatchTwin("last partition, after insert");
            assertSqlCursors("SELECT v, ts, late FROM tw", "SELECT v, ts, late FROM pt");
            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 'pt'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("suspended\nfalse\n");
        });
    }

    @Test
    public void testConvertToNativeLazyTypeChangeColumnTopSpansRowGroups() throws Exception {
        // ALTER COLUMN TYPE leaves these parquet columns in the old type; CONVERT TO NATIVE decodes each
        // one into the new type through the fixed->fixed, fixed->var, var->fixed and var->var arms
        assertLazyTypeChangeColumnTopSpansRowGroups(false);
    }

    @Test
    public void testConvertToNativeLazyTypeChangePrepassColumnTopSpansRowGroups() throws Exception {
        // as above, but a last ALTER COLUMN TYPE SYMBOL converts the parquet partition to native in its
        // pre-pass, which decodes every lazily changed column
        assertLazyTypeChangeColumnTopSpansRowGroups(true);
    }

    @Test
    public void testConvertToNativeLazyVarToNoSentinelKeepsColumnTop() throws Exception {
        // VARCHAR, STRING and SYMBOL columns added after `top` of `rows` rows of 2024-01-01 change lazily to
        // BOOLEAN, BYTE, SHORT or CHAR on the parquet partition. The parquet -> native conversion has to keep
        // their column top, which then reads as NULL after a follow-up ALTER to a type with a NULL sentinel.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final int[][] topsAndRows = {{0, 8}, {2, 8}, {4, 8}, {5, 8}, {7, 8}, {9, 12}, {8, 8}, {3, 21}};
            // CONVERT TO NATIVE, the ALTER COLUMN TYPE pre-pass of another column and of the follow-up ALTER
            // itself, and CONVERT TO NATIVE and back to parquet before the follow-up ALTER
            for (String mode : new String[]{"convert", "convert WAL", "pre-pass other column", "pre-pass", "round trip"}) {
                for (int[] topAndRows : topsAndRows) {
                    final int top = topAndRows[0];
                    final int rows = topAndRows[1];
                    final String context = mode + ", top " + top + " of " + rows;
                    createNoSentinelTables(mode.endsWith("WAL") ? "WAL" : "BYPASS WAL", top, rows, true);
                    switch (mode) {
                        case "convert", "convert WAL", "round trip" -> {
                            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                            drainWalQueue();
                        }
                        case "pre-pass other column" -> {
                            for (String table : new String[]{"pt", "tw"}) {
                                execute("ALTER TABLE " + table + " ALTER COLUMN v TYPE SYMBOL");
                            }
                        }
                        default -> {
                        }
                    }
                    if (!mode.equals("pre-pass")) {
                        assertPartitionFormat("pt", 0, PartitionFormat.NATIVE);
                        assertNoSentinelColumnTops(context, top, rows);
                        assertSqlCursors("tw", "pt");
                        engine.releaseAllReaders();
                        engine.releaseAllWriters();
                        assertNoSentinelColumnTops(context + ", reopen", top, rows);
                    }
                    if (mode.equals("round trip")) {
                        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                        assertPartitionFormat("pt", 0, PartitionFormat.PARQUET);
                        assertSqlCursors("tw", "pt");
                    }
                    alterNoSentinelColumnsToFollowUpTypes();
                    assertPartitionFormat("pt", 0, PartitionFormat.NATIVE);
                    assertNoSentinelFollowUpLiterals(context, "pt", top, rows);
                    assertNoSentinelFollowUpLiterals(context, "tw", top, rows);
                    assertNoSentinelQueriesMatchTwin(context);
                }
            }
        });
    }

    @Test
    public void testConvertToNativeLazyVarToNoSentinelLastPartition() throws Exception {
        // the last partition of a WAL table is parquet: after CONVERT TO NATIVE the writer appends to it
        // after the restored column tops
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            createNoSentinelTables("WAL", 5, 8, false);
            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
            assertPartitionFormat("pt", 0, PartitionFormat.NATIVE);
            assertNoSentinelColumnTops("last partition", 5, 8);
            final String insert = noSentinelTypedInsert("pt", 5, 8, 3);
            execute(insert);
            execute(insert.replace("INSERT INTO pt ", "INSERT INTO tw "));
            drainWalQueue();
            assertNoSentinelColumnTops("last partition, after insert", 5, 8);
            alterNoSentinelColumnsToFollowUpTypes();
            assertNoSentinelFollowUpLiterals("last partition", "pt", 5, 11);
            assertNoSentinelFollowUpLiterals("last partition", "tw", 5, 11);
            assertNoSentinelQueriesMatchTwin("last partition");
            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 'pt'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("suspended\nfalse\n");
        });
    }

    @Test
    public void testConvertToNativeLazyVarToNoSentinelCoveredColumn() throws Exception {
        // b changes lazily from VARCHAR to BYTE on the parquet partition, then the posting index of s covers it.
        // CONVERT TO NATIVE restores the top of b, not of s; the covering index answers with b's top.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            for (String table : new String[]{"pt", "tw"}) {
                execute("CREATE TABLE " + table + " (v INT, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("INSERT INTO " + table + " SELECT x::INT, CASE WHEN x % 2 = 0 THEN 'x' ELSE 'y' END, dateadd('h', (x - 1)::INT, '2024-01-01'::TIMESTAMP)"
                        + " FROM long_sequence(5)");
                execute("ALTER TABLE " + table + " ADD COLUMN b VARCHAR");
                execute("""
                        INSERT INTO {t} VALUES
                        (6, 'x', '2024-01-01T05:00:00.000000Z', '-7'),
                        (7, 'y', '2024-01-01T06:00:00.000000Z', NULL),
                        (8, 'x', '2024-01-01T07:00:00.000000Z', NULL),
                        (99, 'x', '2024-01-02T00:00:00.000000Z', '300')""".replace("{t}", table));
            }
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            for (String table : new String[]{"pt", "tw"}) {
                execute("ALTER TABLE " + table + " ALTER COLUMN b TYPE BYTE");
            }
            // the index covers b in its new type
            execute("ALTER TABLE pt ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (b)");
            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            assertPartitionFormat("pt", 0, PartitionFormat.NATIVE);
            Assert.assertEquals(0, getColumnTop("pt", 0, "s"));
            Assert.assertEquals(5, getColumnTop("pt", 0, "b"));
            assertQuery("SELECT ts, b FROM pt WHERE s = 'x'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("CoveringIndex on: s")
                    .returns("""
                            ts\tb
                            2024-01-01T01:00:00.000000Z\t0
                            2024-01-01T03:00:00.000000Z\t0
                            2024-01-01T05:00:00.000000Z\t-7
                            2024-01-01T07:00:00.000000Z\t0
                            2024-01-02T00:00:00.000000Z\t44
                            """);
            assertSqlCursors("SELECT ts, b FROM tw WHERE s = 'x'", "SELECT ts, b FROM pt WHERE s = 'x'");
            assertSqlCursors("tw", "pt");
            // after a change to a type with a NULL sentinel the column top reads NULL, the NULL inserted after it 0
            for (String table : new String[]{"pt", "tw"}) {
                execute("ALTER TABLE " + table + " ALTER COLUMN b TYPE INT");
            }
            assertQuery("SELECT ts, b FROM pt WHERE s = 'x'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tb
                            2024-01-01T01:00:00.000000Z\tnull
                            2024-01-01T03:00:00.000000Z\tnull
                            2024-01-01T05:00:00.000000Z\t-7
                            2024-01-01T07:00:00.000000Z\t0
                            2024-01-02T00:00:00.000000Z\t44
                            """);
            assertSqlCursors("SELECT ts, b FROM tw WHERE s = 'x'", "SELECT ts, b FROM pt WHERE s = 'x'");
            assertSqlCursors("tw", "pt");
        });
    }

    @Test
    public void testConvertToNativeLazyVarToNoSentinelFailureKeepsColumnTops() throws Exception {
        // CONVERT TO NATIVE fails on the append of the STRING column st in row group 1, after the BYTE column
        // vb, decoded before it, took its top from row group 0: through the writer pool, the command queue of
        // a writer the test holds, and the pre-pass of ALTER COLUMN TYPE. Each failure keeps the committed
        // state of before; the last CONVERT restores the top.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        final AppendFailingFacade ff = new AppendFailingFacade();
        assertMemoryLeak(ff, () -> {
            for (String table : new String[]{"pt", "tw"}) {
                execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("INSERT INTO " + table + " SELECT x::INT, dateadd('h', (x - 1)::INT, '2024-01-01'::TIMESTAMP) FROM long_sequence(2)");
                execute("ALTER TABLE " + table + " ADD COLUMN vb VARCHAR");
                execute("ALTER TABLE " + table + " ADD COLUMN st STRING");
                execute("INSERT INTO " + table + " SELECT x::INT, dateadd('h', (x - 1)::INT, '2024-01-01'::TIMESTAMP), (x * 40)::VARCHAR, 's' || x"
                        + " FROM long_sequence(8) WHERE x > 2");
                execute("INSERT INTO " + table + " VALUES (99, '2024-01-02', '1', 's99')");
            }
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            for (String table : new String[]{"pt", "tw"}) {
                execute("ALTER TABLE " + table + " ALTER COLUMN vb TYPE BYTE");
            }
            final long vbTop = getColumnTop("pt", 0, "vb");
            final long stTop = getColumnTop("pt", 0, "st");
            final long columnVersion = readColumnVersionFileVersion("pt");
            final String convertSql = "ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'";

            ff.arm("st.d", 2);
            try {
                execute(convertSql);
                Assert.fail("pooled writer: the CONVERT must fail");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "cannot append data");
            }
            assertNoSentinelFailureKeptState("pooled writer", ff, vbTop, stTop, columnVersion);

            try (TableWriter writer = getWriter("pt")) {
                ff.arm("st.d", 2);
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    final CompiledQuery cq = compiler.compile(convertSql, sqlExecutionContext);
                    try (OperationFuture fut = cq.execute(eventSubSequence)) {
                        writer.tick();
                        fut.await();
                    }
                    Assert.fail("async writer: the CONVERT must fail");
                } catch (SqlException | CairoException e) {
                    TestUtils.assertContains(e.getMessage(), "cannot append data");
                }
                Assert.assertEquals(PartitionFormat.PARQUET, writer.getPartitionFormat(0));
                Assert.assertEquals(vbTop, writer.columnVersionReader().getColumnTop(writer.getPartitionTimestamp(0), writer.getMetadata().getColumnIndex("vb")));
            }
            assertNoSentinelFailureKeptState("async writer", ff, vbTop, stTop, columnVersion);

            ff.arm("st.d", 2);
            try {
                execute("ALTER TABLE pt ALTER COLUMN v TYPE SYMBOL");
                Assert.fail("ALTER COLUMN TYPE must fail");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "cannot append data");
            }
            engine.releaseAllWriters();
            assertNoSentinelFailureKeptState("pre-pass", ff, vbTop, stTop, columnVersion);

            execute(convertSql);
            assertPartitionFormat("pt", 0, PartitionFormat.NATIVE);
            Assert.assertEquals(2, getColumnTop("pt", 0, "vb"));
            for (String table : new String[]{"pt", "tw"}) {
                execute("ALTER TABLE " + table + " ALTER COLUMN vb TYPE SYMBOL");
            }
            assertQuery("SELECT v, vb, st FROM pt")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v\tvb\tst
                            1\t\t
                            2\t\t
                            3\t120\ts3
                            4\t-96\ts4
                            5\t-56\ts5
                            6\t-16\ts6
                            7\t24\ts7
                            8\t64\ts8
                            99\t1\ts99
                            """);
            assertSqlCursors("tw", "pt");
        });
    }

    @Test
    public void testConvertToNativeLazyVarToNoSentinelTwoPartitions() throws Exception {
        // one ALTER COLUMN TYPE converts two parquet partitions in its pre-pass: a_vb has a top of 5 of 8
        // rows in 2024-01-01 and none in 2024-01-02, b_vb has no rows in 2024-01-01 and a top of 9 of 12
        // rows in 2024-01-02
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            for (String table : new String[]{"pt", "tw"}) {
                execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("INSERT INTO " + table + " SELECT x::INT, dateadd('h', (x - 1)::INT, '2024-01-01'::TIMESTAMP) FROM long_sequence(5)");
                execute("ALTER TABLE " + table + " ADD COLUMN a_vb VARCHAR");
                execute("INSERT INTO " + table + " SELECT x::INT, dateadd('h', (x - 1)::INT, '2024-01-01'::TIMESTAMP), (x * 50)::VARCHAR"
                        + " FROM long_sequence(8) WHERE x > 5");
                execute("INSERT INTO " + table + " SELECT x::INT, dateadd('h', (x - 1)::INT, '2024-01-02'::TIMESTAMP), (-x)::VARCHAR FROM long_sequence(9)");
                execute("ALTER TABLE " + table + " ADD COLUMN b_vb STRING");
                execute("INSERT INTO " + table + " SELECT x::INT, dateadd('h', (x - 1)::INT, '2024-01-02'::TIMESTAMP), (-x)::VARCHAR, (x * 3)::STRING"
                        + " FROM long_sequence(12) WHERE x > 9");
                execute("INSERT INTO " + table + " VALUES (99, '2024-01-03', '1', '2')");
            }
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
            for (String table : new String[]{"pt", "tw"}) {
                execute("ALTER TABLE " + table + " ALTER COLUMN a_vb TYPE BYTE");
                execute("ALTER TABLE " + table + " ALTER COLUMN b_vb TYPE SHORT");
            }
            assertSqlCursors("tw", "pt");
            for (String table : new String[]{"pt", "tw"}) {
                execute("ALTER TABLE " + table + " ALTER COLUMN v TYPE SYMBOL");
            }
            assertPartitionFormat("pt", 0, PartitionFormat.NATIVE);
            assertPartitionFormat("pt", 1, PartitionFormat.NATIVE);
            Assert.assertEquals(5, getColumnTop("pt", 0, "a_vb"));
            Assert.assertEquals(0, getColumnTop("pt", 1, "a_vb"));
            // absent from 2024-01-01, as before CONVERT TO PARQUET
            Assert.assertEquals(-1, getColumnTop("pt", 0, "b_vb"));
            Assert.assertEquals(9, getColumnTop("pt", 1, "b_vb"));
            for (String table : new String[]{"pt", "tw"}) {
                execute("ALTER TABLE " + table + " ALTER COLUMN a_vb TYPE SYMBOL");
                execute("ALTER TABLE " + table + " ALTER COLUMN b_vb TYPE INT");
            }
            final String expected = """
                    v\ta_vb\tb_vb
                    1\t\tnull
                    2\t\tnull
                    3\t\tnull
                    4\t\tnull
                    5\t\tnull
                    6\t44\tnull
                    7\t94\tnull
                    8\t-112\tnull
                    1\t-1\tnull
                    2\t-2\tnull
                    3\t-3\tnull
                    4\t-4\tnull
                    5\t-5\tnull
                    6\t-6\tnull
                    7\t-7\tnull
                    8\t-8\tnull
                    9\t-9\tnull
                    10\t-10\t30
                    11\t-11\t33
                    12\t-12\t36
                    99\t1\t2
                    """;
            for (String table : new String[]{"pt", "tw"}) {
                assertQuery("SELECT v, a_vb, b_vb FROM " + table)
                        .noLeakCheck()
                        .expectSize()
                        .returns(expected);
            }
            assertSqlCursors("tw", "pt");
        });
    }

    private static void assertCursorMatchesQuery(String context, String expectedSql, RecordCursorFactory factory, RecordCursor cursor) throws Exception {
        final StringSink expected = new StringSink();
        try (RecordCursorFactory expectedFactory = select(expectedSql); RecordCursor expectedCursor = expectedFactory.getCursor(sqlExecutionContext)) {
            CursorPrinter.println(expectedCursor, expectedFactory.getMetadata(), expected);
        }
        final StringSink actual = new StringSink();
        CursorPrinter.println(cursor, factory.getMetadata(), actual);
        TestUtils.assertEquals(context, expected, actual);
    }

    private void assertLazyTypeChangeColumnTopSpansRowGroups(boolean isPrepass) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            // the first chunk in row group 1 or 2, and a second chunk after it
            for (int[] topAndRows : new int[][]{{5, 8}, {9, 12}, {5, 12}}) {
                final int top = topAndRows[0];
                final int rows = topAndRows[1];
                final String context = "top " + top + " of " + rows;
                // tw, the native twin, gets the same statements without the CONVERT
                for (String table : new String[]{"pt", "tw"}) {
                    execute("DROP TABLE IF EXISTS " + table);
                    execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                    execute("INSERT INTO " + table + " SELECT x::INT, dateadd('h', (x - 1)::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP) FROM long_sequence(" + top + ")");
                    final StringSink select = new StringSink();
                    for (String[] conversion : LAZY_TYPE_CONVERSIONS) {
                        execute("ALTER TABLE " + table + " ADD COLUMN " + conversion[0] + " " + conversion[1]);
                        select.put(", ").put(conversion[3]);
                    }
                    execute("INSERT INTO " + table + " SELECT v, ts" + select + " FROM (SELECT x::INT v, dateadd('h', (x - 1)::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP) ts"
                            + " FROM long_sequence(" + rows + ") WHERE x > " + top + ")");
                    execute("INSERT INTO " + table + " SELECT v, ts" + select + " FROM (SELECT 99 v, '2024-01-02T00:00:00.000000Z'::TIMESTAMP ts)");
                }
                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                for (String[] conversion : LAZY_TYPE_CONVERSIONS) {
                    for (String table : new String[]{"pt", "tw"}) {
                        execute("ALTER TABLE " + table + " ALTER COLUMN " + conversion[0] + " TYPE " + conversion[2]);
                    }
                }
                assertPartitionFormat("pt", 0, PartitionFormat.PARQUET);
                assertSqlCursors("tw", "pt");
                if (isPrepass) {
                    for (String table : new String[]{"pt", "tw"}) {
                        execute("ALTER TABLE " + table + " ALTER COLUMN v TYPE SYMBOL");
                    }
                } else {
                    execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                }
                assertPartitionFormat("pt", 0, PartitionFormat.NATIVE);
                assertSqlCursors("tw", "pt");
                for (String[] conversion : LAZY_TYPE_CONVERSIONS) {
                    // BOOLEAN, BYTE, SHORT and CHAR keep the column top; a type with a NULL sentinel starts
                    // at the first row of the first row group with data
                    final long expectedTop = ColumnType.isNoNullSentinelFixedType(ColumnType.typeOf(conversion[2])) ? top : 4L * (top / 4);
                    Assert.assertEquals(context + ", column " + conversion[0], expectedTop, getColumnTop("pt", 0, conversion[0]));
                }
                engine.releaseAllReaders();
                assertSqlCursors("tw", "pt");
                // a type with a NULL sentinel reads the column top of a BOOLEAN, BYTE, SHORT or CHAR column as NULL
                for (String[] conversion : LAZY_TYPE_CONVERSIONS) {
                    if (ColumnType.isNoNullSentinelFixedType(ColumnType.typeOf(conversion[2]))) {
                        for (String table : new String[]{"pt", "tw"}) {
                            execute("ALTER TABLE " + table + " ALTER COLUMN " + conversion[0] + " TYPE SYMBOL");
                        }
                        // a CHAR NULL is the character 0, which SYMBOL reads as NULL; rows with v % 4 = 2 hold NULL
                        long nullCount = top;
                        for (int v = top + 1; v <= rows && ColumnType.typeOf(conversion[2]) == ColumnType.CHAR; v++) {
                            nullCount += v % 4 == 2 ? 1 : 0;
                        }
                        assertQuery("SELECT count() FROM pt WHERE ts IN '2024-01-01' AND " + conversion[0] + " IS NULL")
                                .noLeakCheck()
                                .noRandomAccess()
                                .expectSize()
                                .returns("count\n" + nullCount + "\n");
                    }
                }
                assertSqlCursors("tw", "pt");
            }
        });
    }

    private static void alterNoSentinelColumnsToFollowUpTypes() throws Exception {
        for (String[] conversion : NO_SENTINEL_CONVERSIONS) {
            for (String table : new String[]{"pt", "tw"}) {
                execute("ALTER TABLE " + table + " ALTER COLUMN " + conversion[0] + " TYPE " + conversion[3]);
            }
        }
        drainWalQueue();
    }

    /**
     * The native column tops of the BOOLEAN, BYTE, SHORT and CHAR columns of {@link #NO_SENTINEL_CONVERSIONS}:
     * the column top itself, or the row count for a column without rows.
     */
    private static void assertNoSentinelColumnTops(String context, int top, int rows) {
        for (String[] conversion : NO_SENTINEL_CONVERSIONS) {
            Assert.assertEquals(context + ", column " + conversion[0], Math.min(top, rows), getColumnTop("pt", 0, conversion[0]));
        }
    }

    /**
     * The rows of 2024-01-01 after the follow-up ALTER of every column of {@link #NO_SENTINEL_CONVERSIONS}, from
     * the inserted text cast by hand: NULL in the column top, then {@link #noSentinelValue(int, String)} in the
     * follow-up type.
     */
    private void assertNoSentinelFollowUpLiterals(String context, String table, int top, int rows) throws Exception {
        final StringSink expected = new StringSink();
        final StringSink select = new StringSink();
        expected.put('v');
        select.put("SELECT v");
        for (String[] conversion : NO_SENTINEL_CONVERSIONS) {
            expected.put('\t').put(conversion[0]);
            select.put(", ").put(conversion[0]);
        }
        expected.put('\n');
        select.put(" FROM ").put(table).put(" WHERE ts IN '2024-01-01'");
        for (int row = 0; row < rows; row++) {
            expected.put(row + 1);
            for (String[] conversion : NO_SENTINEL_CONVERSIONS) {
                String text = row < top ? null : noSentinelValue(row - top, conversion[2]);
                if (text != null && conversion[3].equals("DOUBLE")) {
                    text += ".0";
                }
                if (text == null) {
                    text = switch (conversion[3]) {
                        case "INT", "LONG", "DOUBLE" -> "null";
                        default -> "";
                    };
                }
                expected.put('\t').put(text);
            }
            expected.put('\n');
        }
        try {
            assertQuery(select.toString())
                    .noLeakCheck()
                    .returns(expected);
        } catch (AssertionError e) {
            throw new AssertionError(context + ", table " + table, e);
        }
    }

    /**
     * Every query of {@link #NO_SENTINEL_QUERIES}, on each column of {@link #NO_SENTINEL_CONVERSIONS}, returns on
     * pt what it returns on the native twin tw; the NULL filter with JIT on and off.
     */
    private static void assertNoSentinelQueriesMatchTwin(String context) throws Exception {
        for (String sql : NO_SENTINEL_QUERIES) {
            for (String[] conversion : NO_SENTINEL_CONVERSIONS) {
                if (sql.contains("LATEST ON") && !conversion[3].equals("SYMBOL")) {
                    continue;
                }
                final String query = sql.replace("{c}", conversion[0]);
                final int jitModeCount = sql.contains("IS NULL") ? 2 : 1;
                for (int jitMode = 0; jitMode < jitModeCount; jitMode++) {
                    sqlExecutionContext.setJitMode(jitMode == 0 ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
                    try {
                        assertSqlCursors(query.replace("{t}", "tw"), query.replace("{t}", "pt"));
                    } catch (AssertionError e) {
                        throw new AssertionError(context + ": " + query + (jitMode == 0 ? "" : ", JIT off"), e);
                    } finally {
                        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                    }
                }
                if (!sql.contains("{c}")) {
                    break;
                }
            }
        }
    }

    /**
     * After a failed parquet -> native conversion of 2024-01-01 of pt: the facade failed once, the partition
     * is still parquet, the new partition directory is gone, the column tops and the column version file are
     * those of before, and the table reads as its twin.
     */
    private static void assertNoSentinelFailureKeptState(String context, AppendFailingFacade ff, long vbTop, long stTop, long columnVersion) throws Exception {
        Assert.assertTrue(context, ff.hasFired());
        assertPartitionFormat("pt", 0, PartitionFormat.PARQUET);
        Assert.assertEquals(context, vbTop, getColumnTop("pt", 0, "vb"));
        Assert.assertEquals(context, stTop, getColumnTop("pt", 0, "st"));
        Assert.assertEquals(context, columnVersion, readColumnVersionFileVersion("pt"));
        final File tableDir = new File(configuration.getDbRoot().toString(), engine.verifyTableName("pt").getDirName());
        final String[] partitionDirs = tableDir.list((_, name) -> name.startsWith("2024-01-01"));
        Assert.assertNotNull(partitionDirs);
        Assert.assertEquals(context + ": " + Arrays.toString(partitionDirs), 1, partitionDirs.length);
        assertSqlCursors("tw", "pt");
    }

    private static void assertPartitionFormat(String tableName, int partitionIndex, byte expectedFormat) {
        try (TableReader reader = getReader(tableName)) {
            Assert.assertEquals(expectedFormat, reader.getPartitionFormat(partitionIndex));
        }
    }

    /**
     * The tops CONVERT TO NATIVE derives from the decoded row groups of 4 rows: the first row of the first
     * row group with data for a type with a NULL sentinel, the column top itself for BOOLEAN, BYTE, SHORT
     * and CHAR, and the row count for a column without rows.
     */
    private static void assertRowGroupTopColumnTops(String context, int top, int rows) {
        for (int c = 0; c < ROW_GROUP_TOP_COLUMNS.length; c++) {
            final String column = ROW_GROUP_TOP_COLUMNS[c].substring(0, ROW_GROUP_TOP_COLUMNS[c].indexOf(' '));
            final long expectedTop = top >= rows ? rows : c < ROW_GROUP_TOP_NO_SENTINEL_COLUMNS ? top : 4L * (top / 4);
            Assert.assertEquals(context + ", column " + column, expectedTop, getColumnTop("pt", 0, column));
        }
    }

    /**
     * The rows of partition 2024-01-01 in the columns without a NULL sentinel, a STRING and an indexed
     * SYMBOL, from the INSERT literals.
     */
    private void assertRowGroupTopLiterals(String context, int top, int rows) throws Exception {
        final String[][] display = {
                {"false", "false", "false", "true", "true"},
                {"0", "0", "-128", "127", "7"},
                {"0", "0", "-32768", "32767", "300"},
                {"", "a", "\u042f", "z", "\u20ac"}
        };
        final int stColumn = 22;
        final int s1Column = 25;
        final StringSink expected = new StringSink();
        expected.put("v\tbo\tbt\tsh\tch\tst\ts1\n");
        for (int row = 0; row < rows; row++) {
            expected.put(row + 1);
            for (int c = 0; c < ROW_GROUP_TOP_NO_SENTINEL_COLUMNS; c++) {
                expected.put('\t').put(display[c][row < top ? 0 : (row - top + c) % 5]);
            }
            for (int c : new int[]{stColumn, s1Column}) {
                final String literal = ROW_GROUP_TOP_VALUES[c][row < top ? 0 : (row - top + c) % 5];
                expected.put('\t').put(literal.equals("NULL") ? "" : literal.substring(1, literal.length() - 1));
            }
            expected.put('\n');
        }
        try {
            assertQuery("SELECT v, bo, bt, sh, ch, st, s1 FROM pt WHERE ts IN '2024-01-01'")
                    .noLeakCheck()
                    .returns(expected);
        } catch (AssertionError e) {
            throw new AssertionError(context, e);
        }
    }

    private static void assertRowGroupTopQueriesMatchTwin(String context) throws Exception {
        assertRowGroupTopQueriesMatchTwin(context, true);
    }

    /**
     * Every query of {@link #ROW_GROUP_TOP_QUERIES}, on each indexed symbol of pt, returns what it returns
     * on the twin tw without indexes. An index-driven LATEST ON drops the NULL key of a partition where
     * the column has no rows, a separate bug: {@code isLatestOnNullKeyChecked} false skips that query.
     */
    private static void assertRowGroupTopQueriesMatchTwin(String context, boolean isLatestOnNullKeyChecked) throws Exception {
        for (String sql : ROW_GROUP_TOP_QUERIES) {
            if (!isLatestOnNullKeyChecked && sql.endsWith("LATEST ON ts PARTITION BY {s}") && !sql.contains("WHERE")) {
                continue;
            }
            for (String s : sql.contains("{s}") ? new String[]{"s1", "s2", "s3", "s4"} : new String[]{"s1"}) {
                final String query = sql.replace("{s}", s);
                try {
                    assertSqlCursors(query.replace("{t}", "tw"), query.replace("{t}", "pt"));
                } catch (AssertionError e) {
                    throw new AssertionError(context + ": " + query, e);
                }
            }
        }
    }

    /**
     * Partition 2024-01-01 gets {@code rows} rows, one per hour, and the columns of
     * {@link #ROW_GROUP_TOP_COLUMNS} after the first {@code top} of them. 2024-01-02 gets one row. The
     * twin, without indexes, answers every index query with a full scan.
     */
    private static void createRowGroupTopTable(String table, String walMode, int top, int rows, boolean isIndexed) throws Exception {
        execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
        execute("INSERT INTO " + table + " SELECT x::INT, dateadd('h', (x - 1)::INT, '2024-01-01T00:00:00.000000Z'::TIMESTAMP) FROM long_sequence(" + top + ")");
        // one column per statement: a WAL table takes one structure change per ALTER
        for (String column : ROW_GROUP_TOP_COLUMNS) {
            execute("ALTER TABLE " + table + " ADD COLUMN " + column);
        }
        if (isIndexed) {
            for (String index : ROW_GROUP_TOP_INDEXES) {
                execute("ALTER TABLE " + table + " ALTER COLUMN " + index);
            }
        }
        if (rows > top) {
            execute(rowGroupTopInsert(table, top, top, rows - top, "2024-01-01T"));
        }
        execute(rowGroupTopInsert(table, 0, 98, 1, "2024-01-02T"));
        drainWalQueue();
    }

    /**
     * pt (in {@code walMode}, indexes on the indexed SYMBOL columns) and its native twin tw get {@code rows}
     * rows in 2024-01-01, one per hour, the columns of {@link #NO_SENTINEL_CONVERSIONS} after the first
     * {@code top} of them and, when {@code hasNextPartition}, a row in 2024-01-02. CONVERT TO PARQUET turns
     * 2024-01-01 of pt to parquet, then both tables change the columns to BOOLEAN, BYTE, SHORT or CHAR.
     */
    private static void createNoSentinelTables(String walMode, int top, int rows, boolean hasNextPartition) throws Exception {
        for (String table : new String[]{"pt", "tw"}) {
            final boolean isPt = table.equals("pt");
            execute("DROP TABLE IF EXISTS " + table);
            execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + (isPt ? walMode : "BYPASS WAL"));
            if (top > 0) {
                execute("INSERT INTO " + table + " SELECT x::INT, dateadd('h', (x - 1)::INT, '2024-01-01'::TIMESTAMP) FROM long_sequence(" + top + ")");
            }
            // one column per statement: a WAL table takes one structure change per ALTER
            for (String[] conversion : NO_SENTINEL_CONVERSIONS) {
                execute("ALTER TABLE " + table + " ADD COLUMN " + conversion[0] + " " + conversion[1]);
                if (isPt && conversion[4] != null) {
                    execute("ALTER TABLE " + table + " ALTER COLUMN " + conversion[0] + " " + conversion[4]);
                }
            }
            if (rows > top) {
                execute(noSentinelInsert(table, top, top, rows - top));
            }
            if (hasNextPartition) {
                execute("INSERT INTO " + table + " VALUES (99, '2024-01-02'" + ", '7'".repeat(NO_SENTINEL_CONVERSIONS.length) + ")");
            }
        }
        drainWalQueue();
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
        for (String[] conversion : NO_SENTINEL_CONVERSIONS) {
            for (String table : new String[]{"pt", "tw"}) {
                execute("ALTER TABLE " + table + " ALTER COLUMN " + conversion[0] + " TYPE " + conversion[2]);
            }
        }
        drainWalQueue();
        assertPartitionFormat("pt", 0, PartitionFormat.PARQUET);
        assertSqlCursors("tw", "pt");
    }

    private static long getColumnTop(String tableName, int partitionIndex, String columnName) {
        try (TableReader reader = getReader(tableName)) {
            final int columnIndex = reader.getMetadata().getWriterIndex(reader.getMetadata().getColumnIndex(columnName));
            return reader.getColumnVersionReader().getColumnTop(reader.getPartitionTimestampByIndex(partitionIndex), columnIndex);
        }
    }

    /**
     * Inserts {@code count} rows of 2024-01-01 with v from {@code firstRow + 1}, at hour {@code r} of row
     * {@code r}, with value {@code (r - top) % 17} of {@link #NO_SENTINEL_VALUES} in every column of
     * {@link #NO_SENTINEL_CONVERSIONS}.
     */
    private static String noSentinelInsert(String table, int top, int firstRow, int count) {
        final StringSink sql = new StringSink();
        sql.put("INSERT INTO ").put(table).put(" VALUES ");
        for (int row = firstRow; row < firstRow + count; row++) {
            if (row > firstRow) {
                sql.put(", ");
            }
            sql.put('(').put(row + 1).put(", '2024-01-01T").put(row < 10 ? "0" : "").put(row).put(":00:00.000000Z'");
            final String value = NO_SENTINEL_VALUES[(row - top) % NO_SENTINEL_VALUES.length];
            for (int c = 0; c < NO_SENTINEL_CONVERSIONS.length; c++) {
                sql.put(", ").put(value == null ? "NULL" : "'" + value + "'");
            }
            sql.put(')');
        }
        return sql.toString();
    }

    /**
     * Inserts rows like {@link #noSentinelInsert(String, int, int, int)}, after the columns changed to their
     * BOOLEAN, BYTE, SHORT or CHAR type: each value cast by hand to that type.
     */
    private static String noSentinelTypedInsert(String table, int top, int firstRow, int count) {
        final StringSink sql = new StringSink();
        sql.put("INSERT INTO ").put(table).put(" VALUES ");
        for (int row = firstRow; row < firstRow + count; row++) {
            if (row > firstRow) {
                sql.put(", ");
            }
            sql.put('(').put(row + 1).put(", '2024-01-01T").put(row < 10 ? "0" : "").put(row).put(":00:00.000000Z'");
            for (String[] conversion : NO_SENTINEL_CONVERSIONS) {
                final String value = noSentinelValue(row - top, conversion[2]);
                sql.put(", ").put(switch (conversion[2]) {
                    case "BOOLEAN" -> value;
                    case "BYTE", "SHORT" -> "(" + value + ")::" + conversion[2];
                    default -> value == null ? "NULL" : "'" + value + "'";
                });
            }
            sql.put(')');
        }
        return sql.toString();
    }

    /**
     * Value {@code index % 17} of {@link #NO_SENTINEL_VALUES} cast by hand to {@code type}: the BOOLEAN keyword
     * test, the integer narrowed to BYTE or SHORT (0 for NULL or text that does not parse), or the first
     * UTF-16 unit as CHAR, null for the character 0 of NULL or empty text, which casts to NULL.
     */
    private static String noSentinelValue(int index, String type) {
        final String value = NO_SENTINEL_VALUES[index % NO_SENTINEL_VALUES.length];
        return switch (type) {
            case "BOOLEAN" -> String.valueOf("true".equalsIgnoreCase(value));
            case "BYTE" -> String.valueOf((byte) parseIntOrZero(value));
            case "SHORT" -> String.valueOf((short) parseIntOrZero(value));
            default -> value == null || value.isEmpty() ? null : String.valueOf(value.charAt(0));
        };
    }

    private static long readColumnVersionFileVersion(String tableName) {
        try (ColumnVersionReader columnVersionReader = new ColumnVersionReader(); Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(engine.verifyTableName(tableName)).concat(TableUtils.COLUMN_VERSION_FILE_NAME);
            columnVersionReader.ofRO(configuration.getFilesFacade(), path.$());
            return columnVersionReader.readUnsafe();
        }
    }

    private static int parseIntOrZero(String value) {
        try {
            return value == null ? 0 : Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /**
     * Inserts {@code count} rows with v from {@code firstRow + 1}; row {@code r} takes value class
     * {@code (r - top + c) % 5} in column {@code c} of {@link #ROW_GROUP_TOP_VALUES}. The time is hour
     * {@code r % 24} of a day prefix ending in T, or minute {@code r % 60} of an hour prefix.
     */
    private static String rowGroupTopInsert(String table, int top, int firstRow, int count, String timestampPrefix) {
        return rowGroupTopInsert(table, top, firstRow, count, timestampPrefix, null);
    }

    /**
     * Like {@link #rowGroupTopInsert(String, int, int, int, String)}, with {@code lastColumnValue}, when
     * not null, for a column added after those of {@link #ROW_GROUP_TOP_COLUMNS}.
     */
    private static String rowGroupTopInsert(String table, int top, int firstRow, int count, String timestampPrefix, String lastColumnValue) {
        final boolean isDayPrefix = timestampPrefix.endsWith("T");
        final StringSink sql = new StringSink();
        sql.put("INSERT INTO ").put(table).put(" VALUES ");
        for (int row = firstRow; row < firstRow + count; row++) {
            if (row > firstRow) {
                sql.put(", ");
            }
            final int timeUnit = isDayPrefix ? row % 24 : row % 60;
            sql.put('(').put(row + 1).put(", '").put(timestampPrefix).put(timeUnit < 10 ? "0" : "").put(timeUnit)
                    .put(isDayPrefix ? ":00:00.000000Z'" : ":00.000000Z'");
            for (int c = 0; c < ROW_GROUP_TOP_VALUES.length; c++) {
                sql.put(", ").put(ROW_GROUP_TOP_VALUES[c][(row - top + c) % 5]);
            }
            if (lastColumnValue != null) {
                sql.put(", ").put(lastColumnValue);
            }
            sql.put(')');
        }
        return sql.toString();
    }

    private void assertPartitionDoesNotExist(String tableName, String partition) {
        assertPartitionOnDisk0(tableName, false, partition);
    }

    @Test
    public void testConvertPartitionEmbedsSeqTxnInPm() throws Exception {
        // The produced _pm must carry TableWriter's current seqTxn.
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            final String tableName = "testConvertSeqTxn";
            createTable(
                    tableName,
                    "INSERT INTO " + tableName + " VALUES(1, '2024-06-10T00:00:00.000000Z')",
                    "INSERT INTO " + tableName + " VALUES(2, '2024-06-11T00:00:00.000000Z')"
            );

            execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-06-10'");

            final TableToken token = engine.verifyTableName(tableName);
            final long expectedSeqTxn;
            final long partitionTimestamp;
            final long parquetNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                expectedSeqTxn = reader.getTxFile().getSeqTxn();
                partitionTimestamp = reader.getTxFile().getPartitionTimestampByIndex(0);
                parquetNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }

            try (Path pmPath = new Path()) {
                TableUtils.setPathForParquetPartitionMetadata(
                        pmPath.of(configuration.getDbRoot()).concat(token),
                        timestampType.getTimestampType(),
                        PartitionBy.DAY,
                        partitionTimestamp,
                        parquetNameTxn
                );
                Assert.assertTrue("produced _pm must exist at " + pmPath, ff.exists(pmPath.$()));

                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                final long addr = ParquetMetaFileReader.openAndMapRO(ff, pmPath.$(), reader);
                Assert.assertTrue("openAndMapRO should succeed", addr > 0);
                try {
                    Assert.assertTrue(reader.resolveLastFooter());
                    Assert.assertEquals(
                            "seqTxn in _pm must match TableWriter.getSeqTxn()",
                            expectedSeqTxn, reader.getResolvedSeqTxn()
                    );
                } finally {
                    ff.munmap(addr, reader.getFileSize(), MemoryTag.MMAP_PARQUET_METADATA_READER);
                    reader.clear();
                }
            }
        });
    }

    private void assertPartitionExists(String tableName, String partition) {
        assertPartitionOnDisk0(tableName, true, partition);
    }

    private void assertPartitionOnDisk0(String tableName, boolean exists, String partition) {
        Path path = Path.getThreadLocal(configuration.getDbRoot());
        path.concat(engine.verifyTableName(tableName));
        path.concat(partition).concat(PARQUET_PARTITION_NAME);

        if (exists) {
            Assert.assertTrue(ff.exists(path.$()));
        } else {
            Assert.assertFalse(ff.exists(path.$()));
        }
    }

    private void createTable(String tableName, String... inserts) throws Exception {
        TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                .col("id", ColumnType.INT)
                .timestamp(timestampType.getTimestampType());
        AbstractCairoTest.create(model);
        for (int i = 0, n = inserts.length; i < n; i++) {
            execute(inserts[i]);
        }
    }

    private void createTableStr(String... inserts) throws Exception {
        TableModel model = new TableModel(configuration, "testConvertAllPartitionsToParquetAndBack", PartitionBy.DAY)
                .col("id", ColumnType.INT)
                .col("str", ColumnType.STRING)
                .timestamp(timestampType.getTimestampType());
        AbstractCairoTest.create(model);
        for (int i = 0, n = inserts.length; i < n; i++) {
            execute(inserts[i]);
        }
    }

    /**
     * Fails the {@code n}-th append to the armed file, once, as a full disk would.
     */
    private static class AppendFailingFacade extends TestFilesFacadeImpl {
        private final AtomicInteger appendsBeforeFailure = new AtomicInteger();
        private final AtomicLong failingFd = new AtomicLong(-1);
        private final AtomicReference<String> failingFileNameRef = new AtomicReference<>();
        private final AtomicBoolean isFired = new AtomicBoolean();

        @Override
        public long append(long fd, long buf, long len) {
            if (fd == failingFd.get() && appendsBeforeFailure.decrementAndGet() == 0) {
                failingFd.set(-1);
                isFired.set(true);
                return -1;
            }
            return super.append(fd, buf, len);
        }

        @Override
        public long openAppend(LPSZ name) {
            final long fd = super.openAppend(name);
            final String fileName = failingFileNameRef.get();
            if (fileName != null && fd != -1) {
                final String path = Utf8s.stringFromUtf8Bytes(name);
                final String lastSegment = path.substring(path.lastIndexOf(Files.SEPARATOR) + 1);
                if ((lastSegment.equals(fileName) || lastSegment.startsWith(fileName + '.')) && failingFileNameRef.compareAndSet(fileName, null)) {
                    failingFd.set(fd);
                }
            }
            return fd;
        }

        void arm(String fileName, int failingAppend) {
            isFired.set(false);
            failingFd.set(-1);
            appendsBeforeFailure.set(failingAppend);
            failingFileNameRef.set(fileName);
        }

        boolean hasFired() {
            return isFired.get();
        }
    }
}
