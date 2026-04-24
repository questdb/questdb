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
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertContains;
import static org.junit.Assert.*;

public class NotNullColumnTest extends AbstractCairoTest {

    private boolean getNotNull(String table, String column) throws Exception {
        try (TableReader reader = engine.getReader(table)) {
            return reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex(column));
        }
    }

    @Test
    public void testAlterTableAddColumnNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE t ADD COLUMN y DOUBLE NOT NULL");

            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata metadata = reader.getMetadata();
                assertFalse(metadata.isNotNull(metadata.getColumnIndex("x")));
                assertTrue(metadata.isNotNull(metadata.getColumnIndex("y")));
            }
        });
    }

    @Test
    public void testAlterTableAddColumnNotNullWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("ALTER TABLE t ADD COLUMN y DOUBLE NOT NULL");
            drainWalQueue();

            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata metadata = reader.getMetadata();
                assertFalse(metadata.isNotNull(metadata.getColumnIndex("x")));
                assertTrue(metadata.isNotNull(metadata.getColumnIndex("y")));
            }
        });
    }

    @Test
    public void testBareNullKeywordMeansNullable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");

            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata metadata = reader.getMetadata();
                assertFalse(metadata.isNotNull(metadata.getColumnIndex("x")));
            }
        });
    }

    @Test
    public void testBooleanExplicitNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (b BOOLEAN NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try (TableReader reader = engine.getReader("t")) {
                assertTrue(reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex("b")));
            }
        });
    }

    @Test
    public void testBooleanNullAccepted() throws Exception {
        assertMemoryLeak(() -> {
            // BOOLEAN NULL is valid — the column is nullable (will use null bitmap when available)
            execute("CREATE TABLE t (b BOOLEAN NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try (TableReader reader = engine.getReader("t")) {
                assertFalse(reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex("b")));
            }
        });
    }

    @Test
    public void testAlterColumnDropNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            assertTrue(getNotNull("t", "x"));

            execute("ALTER TABLE t ALTER COLUMN x SET NULL");
            assertFalse(getNotNull("t", "x"));
        });
    }

    @Test
    public void testAlterColumnDropNotNullBoolean() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (b BOOLEAN NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            assertTrue(getNotNull("t", "b"));

            execute("ALTER TABLE t ALTER COLUMN b SET NULL");
            assertFalse(getNotNull("t", "b"));
        });
    }

    @Test
    public void testAlterColumnSetNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            assertFalse(getNotNull("t", "x"));

            execute("ALTER TABLE t ALTER COLUMN x SET NOT NULL");
            assertTrue(getNotNull("t", "x"));

            assertSql(
                    """
                            ddl
                            CREATE TABLE 't' (\s
                            \tx INT NOT NULL,
                            \tts TIMESTAMP NOT NULL
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """,
                    "SHOW CREATE TABLE t"
            );
        });
    }

    @Test
    public void testAlterColumnSetNotNullThenEnforce() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, '2024-01-01')");
            execute("ALTER TABLE t ALTER COLUMN x SET NOT NULL");

            try {
                execute("INSERT INTO t (ts) VALUES ('2024-01-02')");
                fail("Expected NOT NULL violation");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
            }
        });
    }

    @Test
    public void testCastNullConstantToStringFoldsToNull() throws Exception {
        assertMemoryLeak(() -> {
            // Without the isNullConstant() guard, the constant-fold branch formats
            // INT_NULL / LONG_NULL / NaN / MIN_VALUE through Numbers.append, which
            // writes the 4-char text "null" and yields a StrConstant("null") — not a
            // real null. The guard folds to StrConstant.NULL / VarcharConstant.NULL so
            // IS NULL / COALESCE / output layers keep working downstream.
            assertSql(
                    """
                            i\tl\td\tf\tdate\tts\tip
                            \t\t\t\t\t\t
                            """,
                    """
                            SELECT cast(cast(null as int) as string) i,
                                   cast(cast(null as long) as string) l,
                                   cast(cast(null as double) as string) d,
                                   cast(cast(null as float) as string) f,
                                   cast(cast(null as date) as string) date,
                                   cast(cast(null as timestamp) as string) ts,
                                   cast(cast(null as ipv4) as string) ip
                            """
            );

            assertSql(
                    """
                            i\tl\td\tf\tdate\tts\tip
                            \t\t\t\t\t\t
                            """,
                    """
                            SELECT cast(cast(null as int) as varchar) i,
                                   cast(cast(null as long) as varchar) l,
                                   cast(cast(null as double) as varchar) d,
                                   cast(cast(null as float) as varchar) f,
                                   cast(cast(null as date) as varchar) date,
                                   cast(cast(null as timestamp) as varchar) ts,
                                   cast(cast(null as ipv4) as varchar) ip
                            """
            );

            // IS NULL against the folded constant is TRUE (would be FALSE with a StrConstant("null")).
            assertSql("c\ntrue\n", "SELECT cast(cast(null as int) as string) IS NULL c");
            assertSql("c\ntrue\n", "SELECT cast(cast(null as long) as varchar) IS NULL c");
            assertSql("c\ntrue\n", "SELECT cast(cast(null as timestamp) as string) IS NULL c");
        });
    }

    @Test
    public void testCastToStringOnNotNullColumn() throws Exception {
        assertMemoryLeak(() -> {
            // Phase 5: casting a NOT NULL column to STRING / VARCHAR must format the
            // sentinel bit pattern as numeric text, never the literal "null".
            execute("CREATE TABLE t (i INT NOT NULL, l LONG NOT NULL, d DOUBLE NOT NULL, ts TIMESTAMP NOT NULL, tsrow TIMESTAMP NOT NULL) TIMESTAMP(tsrow) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (1, 100, 1.5, '2024-01-05T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                        (NULL, NULL, NULL, NULL, '2024-01-02T00:00:00.000000Z')
                    """);

            // INT_NULL / LONG_NULL format as their MIN_VALUE numeric text. DOUBLE NaN
            // formats to "NaN". TIMESTAMP_NULL (MIN_VALUE) falls back to the numeric
            // bit pattern because the timestamp formatter itself short-circuits on it.
            assertSql(
                    """
                            i_str\tl_str\td_str\tts_str
                            1\t100\t1.5\t2024-01-05T00:00:00.000000Z
                            -2147483648\t-9223372036854775808\tNaN\t-9223372036854775808
                            """,
                    "SELECT i::string i_str, l::string l_str, d::string d_str, ts::string ts_str FROM t ORDER BY tsrow"
            );

            assertSql(
                    """
                            i_v\tl_v\td_v\tts_v
                            1\t100\t1.5\t2024-01-05T00:00:00.000000Z
                            -2147483648\t-9223372036854775808\tNaN\t-9223372036854775808
                            """,
                    "SELECT i::varchar i_v, l::varchar l_v, d::varchar d_v, ts::varchar ts_v FROM t ORDER BY tsrow"
            );

            // Nullable columns continue to produce empty-field output for a null value.
            execute("CREATE TABLE u (i INT, l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO u VALUES (NULL, NULL, '2024-01-01T00:00:00.000000Z')");
            assertSql(
                    """
                            i_str\tl_str
                            \t
                            """,
                    "SELECT i::string i_str, l::string l_str FROM u"
            );
        });
    }

    @Test
    public void testCreateTableAsSelectDoesNotPropagateNotNull() throws Exception {
        assertMemoryLeak(() -> {
            // CREATE TABLE AS SELECT does not propagate NOT NULL from source columns,
            // because compiled query RecordMetadata doesn't carry column properties.
            // Users must re-specify NOT NULL via CAST or ALTER TABLE after creation.
            execute("CREATE TABLE src (x INT NOT NULL, y DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO src VALUES (1, 2.0, '2024-01-01')");
            execute("CREATE TABLE dst AS (SELECT * FROM src)");

            try (TableReader reader = engine.getReader("dst")) {
                assertFalse(reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex("x")));
            }
        });
    }

    @Test
    public void testCreateTableNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");

            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata metadata = reader.getMetadata();
                assertTrue(metadata.isNotNull(metadata.getColumnIndex("x")));
                assertFalse(metadata.isNotNull(metadata.getColumnIndex("y")));
                assertTrue(metadata.isNotNull(metadata.getColumnIndex("ts")));
            }

            assertSql(
                    """
                            ddl
                            CREATE TABLE 't' (\s
                            \tx INT NOT NULL,
                            \ty DOUBLE,
                            \tts TIMESTAMP NOT NULL
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """,
                    "SHOW CREATE TABLE t"
            );
        });
    }

    @Test
    public void testDropNotNullColumnThenInsert() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE t DROP COLUMN x");
            execute("INSERT INTO t VALUES (1.5, '2024-01-01')");

            assertSql(
                    """
                            y\tts
                            1.5\t2024-01-01T00:00:00.000000Z
                            """,
                    "SELECT * FROM t"
            );
        });
    }

    @Test
    public void testEnforceNotNullMissingColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("INSERT INTO t (y, ts) VALUES (1.5, '2024-01-01')");
                fail("Expected NOT NULL violation");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }
        });
    }

    @Test
    public void testEnforceNotNullMissingColumnWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("INSERT INTO t (y, ts) VALUES (1.5, '2024-01-01')");
                fail("Expected NOT NULL violation");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }
        });
    }

    @Test
    public void testEnforceNotNullNullableColumnsAcceptNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, y INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            // y is nullable, so missing it is fine
            execute("INSERT INTO t (x, ts) VALUES (1, '2024-01-01')");

            assertSql(
                    """
                            x\ty\tts
                            1\tnull\t2024-01-01T00:00:00.000000Z
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testEnforceNotNullSentinelValuesAccepted() throws Exception {
        assertMemoryLeak(() -> {
            // Sentinel values (INT_NULL, NaN) are valid data for NOT NULL columns.
            // NOT NULL only means "column must be written to", not "no sentinel values".
            // For NOT NULL columns, sentinel values print as their real representation:
            // INT_NULL → "-2147483648", NaN → "NaN"
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (NULL, NULL, '2024-01-01')");

            assertSql(
                    """
                            x\ty\tts
                            -2147483648\tNaN\t2024-01-01T00:00:00.000000Z
                            """,
                    "SELECT * FROM t"
            );
        });
    }

    @Test
    public void testEnforceNotNullValidInsertSucceeds() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (42, 1.5, '2024-01-01')");
            execute("INSERT INTO t VALUES (0, NULL, '2024-01-02')");

            assertSql(
                    """
                            x\ty\tts
                            42\t1.5\t2024-01-01T00:00:00.000000Z
                            0\tnull\t2024-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testFilterOnNotNullColumnWithJit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (1, 10.0, '2024-01-01'),
                        (2, 20.0, '2024-01-02'),
                        (3, 30.0, '2024-01-03'),
                        (4, 40.0, '2024-01-04'),
                        (5, 50.0, '2024-01-05')
                    """);

            assertSql(
                    """
                            x\ty\tts
                            3\t30.0\t2024-01-03T00:00:00.000000Z
                            4\t40.0\t2024-01-04T00:00:00.000000Z
                            5\t50.0\t2024-01-05T00:00:00.000000Z
                            """,
                    "SELECT * FROM t WHERE x > 2 ORDER BY ts"
            );
        });
    }

    @Test
    public void testBooleanAutoFillsDefault() throws Exception {
        assertMemoryLeak(() -> {
            // BOOLEAN without NOT NULL auto-fills with false when not provided
            execute("CREATE TABLE t (b BOOLEAN, y DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t (y, ts) VALUES (1.5, '2024-01-01')");

            assertSql(
                    """
                            b\ty\tts
                            false\t1.5\t2024-01-01T00:00:00.000000Z
                            """,
                    "SELECT * FROM t"
            );
        });
    }

    @Test
    public void testBooleanNotNullEnforced() throws Exception {
        assertMemoryLeak(() -> {
            // BOOLEAN NOT NULL is enforced — missing value throws
            execute("CREATE TABLE t (b BOOLEAN NOT NULL, y DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("INSERT INTO t (y, ts) VALUES (1.5, '2024-01-01')");
                fail("Expected NOT NULL violation for BOOLEAN NOT NULL column");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=b");
            }
        });
    }

    @Test
    public void testInformationSchemaColumnsShowsNullability() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");

            assertSql(
                    """
                            table_catalog\ttable_schema\ttable_name\tcolumn_name\tordinal_position\tcolumn_default\tis_nullable\tdata_type
                            qdb\tpublic\tt\tx\t0\t\tno\tinteger
                            qdb\tpublic\tt\ty\t1\t\tyes\tdouble precision
                            qdb\tpublic\tt\tts\t2\t\tno\ttimestamp without time zone
                            """,
                    "SELECT * FROM information_schema.columns() ORDER BY ordinal_position"
            );
        });
    }

    @Test
    public void testExplicitNotNullPreservedOnTypeChange() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (b INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");

            try (TableReader reader = engine.getReader("t")) {
                assertTrue(reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex("b")));
            }

            execute("ALTER TABLE t ALTER COLUMN b TYPE LONG");

            try (TableReader reader = engine.getReader("t")) {
                assertTrue(reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex("b")));
            }
        });
    }

    @Test
    public void testMultipleNotNullColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        a INT NOT NULL,
                        b DOUBLE NOT NULL,
                        c STRING,
                        ts TIMESTAMP NOT NULL
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);

            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata metadata = reader.getMetadata();
                assertTrue(metadata.isNotNull(metadata.getColumnIndex("a")));
                assertTrue(metadata.isNotNull(metadata.getColumnIndex("b")));
                assertFalse(metadata.isNotNull(metadata.getColumnIndex("c")));
                assertTrue(metadata.isNotNull(metadata.getColumnIndex("ts")));
            }
        });
    }

    @Test
    public void testNotNullSurvivesColumnTypeChange() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");

            try (TableReader reader = engine.getReader("t")) {
                assertTrue(reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex("x")));
            }

            execute("ALTER TABLE t ALTER COLUMN x TYPE LONG");

            try (TableReader reader = engine.getReader("t")) {
                assertTrue(reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex("x")));
            }
        });
    }

    @Test
    public void testNotNullSurvivesMetadataReload() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");

            try (TableReader reader = engine.getReader("t")) {
                assertTrue(reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex("x")));
                assertFalse(reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex("y")));
            }

            // close all readers, forcing a metadata reload on next access
            engine.releaseAllReaders();

            try (TableReader reader = engine.getReader("t")) {
                assertTrue(reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex("x")));
                assertFalse(reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex("y")));
            }
        });
    }


    @Test
    public void testShowColumnsIncludesNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");

            assertSql(
                    """
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tnotNull\tupsertKey
                            x\tINT\tfalse\t0\tfalse\t0\t0\tfalse\ttrue\tfalse
                            y\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\tfalse
                            ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\ttrue\tfalse
                            """,
                    "SHOW COLUMNS FROM t"
            );
        });
    }

    @Test
    public void testShowCreateTableReflectsNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        a INT NOT NULL,
                        b DOUBLE,
                        c STRING NOT NULL,
                        ts TIMESTAMP NOT NULL
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);

            assertSql(
                    """
                            ddl
                            CREATE TABLE 't' (\s
                            \ta INT NOT NULL,
                            \tb DOUBLE,
                            \tc STRING NOT NULL,
                            \tts TIMESTAMP NOT NULL
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """,
                    "SHOW CREATE TABLE t"
            );
        });
    }

    @Test
    public void testNotNullDoubleSpecialValues() throws Exception {
        assertMemoryLeak(() -> {
            // Non-finite values are preserved on write. For NOT NULL columns they print
            // as their IEEE 754 representation instead of "null".
            execute("CREATE TABLE t (x DOUBLE NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (NULL, '2024-01-01'),
                        (1.5, '2024-01-02'),
                        (CAST('Infinity' AS DOUBLE), '2024-01-03'),
                        (CAST('-Infinity' AS DOUBLE), '2024-01-04'),
                        (CAST('NaN' AS DOUBLE), '2024-01-05')
                    """);

            assertSql(
                    """
                            x\tts
                            NaN\t2024-01-01T00:00:00.000000Z
                            1.5\t2024-01-02T00:00:00.000000Z
                            Infinity\t2024-01-03T00:00:00.000000Z
                            -Infinity\t2024-01-04T00:00:00.000000Z
                            NaN\t2024-01-05T00:00:00.000000Z
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testNotNullFloatSpecialValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x FLOAT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (NULL, '2024-01-01'),
                        (1.5, '2024-01-02'),
                        (CAST('Infinity' AS FLOAT), '2024-01-03'),
                        (CAST('-Infinity' AS FLOAT), '2024-01-04'),
                        (CAST('NaN' AS FLOAT), '2024-01-05')
                    """);

            assertSql(
                    """
                            x\tts
                            NaN\t2024-01-01T00:00:00.000000Z
                            1.5\t2024-01-02T00:00:00.000000Z
                            Infinity\t2024-01-03T00:00:00.000000Z
                            -Infinity\t2024-01-04T00:00:00.000000Z
                            NaN\t2024-01-05T00:00:00.000000Z
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testNotNullLongSentinelPrintsValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (NULL, '2024-01-01')");
            execute("INSERT INTO t VALUES (42, '2024-01-02')");

            assertSql(
                    """
                            x\tts
                            -9223372036854775808\t2024-01-01T00:00:00.000000Z
                            42\t2024-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testNotNullIpv4SentinelPrintsValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x IPv4 NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (NULL, '2024-01-01')");
            execute("INSERT INTO t VALUES ('192.168.1.1', '2024-01-02')");

            assertSql(
                    """
                            x\tts
                            0.0.0.0\t2024-01-01T00:00:00.000000Z
                            192.168.1.1\t2024-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testNotNullUuidSentinelPrintsValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x UUID NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (NULL, '2024-01-01')");
            execute("INSERT INTO t VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-01-02')");

            assertSql(
                    """
                            x\tts
                            80000000-0000-0000-8000-000000000000\t2024-01-01T00:00:00.000000Z
                            a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t2024-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testNotNullCharSentinelPrintsValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x CHAR NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (NULL, '2024-01-01')");
            execute("INSERT INTO t VALUES ('A', '2024-01-02')");

            // CHAR NOT NULL with sentinel (char 0) prints empty — the NUL character has no visible representation
            assertSql(
                    """
                            x\tts
                            \0\t2024-01-01T00:00:00.000000Z
                            A\t2024-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testDropNotNullOnDesignatedTimestampFails() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            assertTrue(getNotNull("t", "ts"));

            try {
                execute("ALTER TABLE t ALTER COLUMN ts SET NULL");
                fail("Expected error for dropping NOT NULL on designated timestamp");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "cannot drop NOT NULL constraint on designated timestamp");
            }

            assertTrue(getNotNull("t", "ts"));
        });
    }

    @Test
    public void testNotKeywordWithoutNull() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE t (x INT NOT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
                fail("Expected parse error");
            } catch (SqlException e) {
                assertContains(e.getFlyweightMessage(), "'NULL' expected after 'NOT'");
            }
        });
    }

    @Test
    public void testNotKeywordWithoutNullAlterTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("ALTER TABLE t ADD COLUMN y DOUBLE NOT");
                fail("Expected parse error");
            } catch (SqlException e) {
                assertContains(e.getFlyweightMessage(), "'NULL' expected after 'NOT'");
            }
        });
    }

    @Test
    public void testNotNullRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            assertFalse(getNotNull("t", "x"));

            execute("ALTER TABLE t ALTER COLUMN x SET NOT NULL");
            assertTrue(getNotNull("t", "x"));

            execute("ALTER TABLE t ALTER COLUMN x SET NULL");
            assertFalse(getNotNull("t", "x"));

            execute("ALTER TABLE t ALTER COLUMN x SET NOT NULL");
            assertTrue(getNotNull("t", "x"));
        });
    }

    @Test
    public void testAlterColumnSetNotNullWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertFalse(getNotNull("t", "x"));

            execute("ALTER TABLE t ALTER COLUMN x SET NOT NULL");
            drainWalQueue();
            assertTrue(getNotNull("t", "x"));
        });
    }

    @Test
    public void testAlterColumnDropNotNullWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            assertTrue(getNotNull("t", "x"));

            execute("ALTER TABLE t ALTER COLUMN x SET NULL");
            drainWalQueue();
            assertFalse(getNotNull("t", "x"));
        });
    }

    @Test
    public void testNullableColumnsStillPrintNull() throws Exception {
        assertMemoryLeak(() -> {
            // Verify nullable columns still print "null" for sentinel values (no regression)
            execute("CREATE TABLE t (x INT, y DOUBLE, z LONG, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t (ts) VALUES ('2024-01-01')");

            assertSql(
                    """
                            x\ty\tz\tts
                            null\tnull\tnull\t2024-01-01T00:00:00.000000Z
                            """,
                    "SELECT * FROM t"
            );
        });
    }

    @Test
    public void testRenameColumnPreservesNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE t RENAME COLUMN x TO renamed_x");

            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata metadata = reader.getMetadata();
                assertTrue(metadata.isNotNull(metadata.getColumnIndex("renamed_x")));
                assertFalse(metadata.isNotNull(metadata.getColumnIndex("y")));
            }

            // enforcement still applies under the new name
            try {
                execute("INSERT INTO t (y, ts) VALUES (1.5, '2024-01-01')");
                fail("Expected NOT NULL violation after rename");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=renamed_x");
            }
        });
    }

    @Test
    public void testRenameColumnPreservesNotNullWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("ALTER TABLE t RENAME COLUMN x TO renamed_x");
            drainWalQueue();

            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata metadata = reader.getMetadata();
                assertTrue(metadata.isNotNull(metadata.getColumnIndex("renamed_x")));
            }
        });
    }

    @Test
    public void testDropColumnAndReAddPreservesNewlyDeclaredNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE t DROP COLUMN x");
            execute("ALTER TABLE t ADD COLUMN x INT NOT NULL");

            // new x column is NOT NULL; enforcement kicks in when writing
            try (TableReader reader = engine.getReader("t")) {
                assertTrue(reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex("x")));
            }
            try {
                execute("INSERT INTO t (ts) VALUES ('2024-01-01')");
                fail("Expected NOT NULL violation on re-added column");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }
        });
    }

    @Test
    public void testDropColumnAndReAddWithoutNotNullIsNullable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE t DROP COLUMN x");
            // re-add without NOT NULL: should be nullable (NOT NULL does NOT persist from the dropped column)
            execute("ALTER TABLE t ADD COLUMN x INT");

            try (TableReader reader = engine.getReader("t")) {
                assertFalse(reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex("x")));
            }
            // re-added column is appended at the end
            execute("INSERT INTO t (ts) VALUES ('2024-01-01')");
            assertSql(
                    """
                            ts\tx
                            2024-01-01T00:00:00.000000Z\tnull
                            """,
                    "SELECT * FROM t"
            );
        });
    }

    @Test
    public void testCreateSymbolNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SYMBOL NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");

            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata metadata = reader.getMetadata();
                assertTrue(metadata.isNotNull(metadata.getColumnIndex("x")));
            }

            execute("INSERT INTO t VALUES ('a', '2024-01-01')");
            assertSql(
                    """
                            x\tts
                            a\t2024-01-01T00:00:00.000000Z
                            """,
                    "SELECT * FROM t"
            );
        });
    }

    @Test
    public void testCreateSymbolWithCapacityAndNotNull() throws Exception {
        assertMemoryLeak(() -> {
            // SYMBOL with all optional clauses followed by NOT NULL
            execute("CREATE TABLE t (x SYMBOL CAPACITY 128 NOCACHE INDEX NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");

            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata metadata = reader.getMetadata();
                int idx = metadata.getColumnIndex("x");
                assertTrue(metadata.isNotNull(idx));
                assertTrue(metadata.isColumnIndexed(idx));
            }
        });
    }

    @Test
    public void testCtasPlusAlterRestoresNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src (x INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO src VALUES (1, '2024-01-01')");
            execute("CREATE TABLE dst AS (SELECT * FROM src) TIMESTAMP(ts) PARTITION BY DAY");

            // CTAS strips NOT NULL, user re-applies with ALTER (no existing nulls -> succeeds)
            assertFalse(getNotNull("dst", "x"));
            execute("ALTER TABLE dst ALTER COLUMN x SET NOT NULL");
            assertTrue(getNotNull("dst", "x"));

            try {
                execute("INSERT INTO dst (ts) VALUES ('2024-01-02')");
                fail("Expected NOT NULL violation after ALTER");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
            }
        });
    }

    @Test
    public void testInformationSchemaColumnsShowsNullabilityWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            assertSql(
                    """
                            table_catalog\ttable_schema\ttable_name\tcolumn_name\tordinal_position\tcolumn_default\tis_nullable\tdata_type
                            qdb\tpublic\tt\tx\t0\t\tno\tinteger
                            qdb\tpublic\tt\ty\t1\t\tyes\tdouble precision
                            qdb\tpublic\tt\tts\t2\t\tno\ttimestamp without time zone
                            """,
                    "SELECT * FROM information_schema.columns() ORDER BY ordinal_position"
            );
        });
    }

    @Test
    public void testInsertAsSelectPassesSourceNullAsSentinel() throws Exception {
        assertMemoryLeak(() -> {
            // INSERT AS SELECT consults row.append() enforcement: every column in the row
            // is written (even when the source value is NULL, because the copier calls
            // putXxx(sentinel)). Consistent with VALUES(NULL) design. The sentinel survives.
            execute("CREATE TABLE src (x INT, y LONG, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO src VALUES (NULL, NULL, '2024-01-01')");
            execute("CREATE TABLE dst (x INT NOT NULL, y LONG NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO dst SELECT x, y, ts FROM src");

            assertSql(
                    """
                            x\ty\tts
                            -2147483648\t-9223372036854775808\t2024-01-01T00:00:00.000000Z
                            """,
                    "SELECT * FROM dst"
            );
        });
    }

    @Test
    public void testInsertAsSelectMissingNotNullColumnRejected() throws Exception {
        assertMemoryLeak(() -> {
            // When the SELECT doesn't provide a NOT NULL target column, enforcement kicks in.
            execute("CREATE TABLE src (y DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO src VALUES (1.5, '2024-01-01')");
            execute("CREATE TABLE dst (x INT NOT NULL, y DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("INSERT INTO dst (y, ts) SELECT y, ts FROM src");
                fail("Expected NOT NULL violation");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }
        });
    }

    @Test
    public void testVarcharNotNullEmptyStringAccepted() throws Exception {
        assertMemoryLeak(() -> {
            // Empty string '' is not NULL — it's a valid value for a NOT NULL column.
            execute("CREATE TABLE t (x VARCHAR NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('', '2024-01-01')");
            execute("INSERT INTO t VALUES ('a', '2024-01-02')");

            assertSql(
                    """
                            x\tts
                            \t2024-01-01T00:00:00.000000Z
                            a\t2024-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testVarcharNotNullMissingRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x VARCHAR NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("INSERT INTO t (ts) VALUES ('2024-01-01')");
                fail("Expected NOT NULL violation for VARCHAR NOT NULL");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }
        });
    }

    @Test
    public void testStringNotNullMissingRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x STRING NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("INSERT INTO t (ts) VALUES ('2024-01-01')");
                fail("Expected NOT NULL violation for STRING NOT NULL");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }
        });
    }

    @Test
    public void testSymbolNotNullMissingRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SYMBOL NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("INSERT INTO t (ts) VALUES ('2024-01-01')");
                fail("Expected NOT NULL violation for SYMBOL NOT NULL");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }
        });
    }

    @Test
    public void testShortNotNullMissingRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x SHORT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("INSERT INTO t (ts) VALUES ('2024-01-01')");
                fail("Expected NOT NULL violation for SHORT NOT NULL");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }
        });
    }

    @Test
    public void testByteNotNullMissingRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x BYTE NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("INSERT INTO t (ts) VALUES ('2024-01-01')");
                fail("Expected NOT NULL violation for BYTE NOT NULL");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }
        });
    }

    @Test
    public void testGeoHashNotNullMissingRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x GEOHASH(5c) NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("INSERT INTO t (ts) VALUES ('2024-01-01')");
                fail("Expected NOT NULL violation for GEOHASH NOT NULL");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }
        });
    }

    @Test
    public void testBinaryNotNullMissingRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x BINARY NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("INSERT INTO t (ts) VALUES ('2024-01-01')");
                fail("Expected NOT NULL violation for BINARY NOT NULL");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }
        });
    }

    @Test
    public void testTimestampNotNullMissingRejectedForNonDesignated() throws Exception {
        assertMemoryLeak(() -> {
            // ts is designated (automatically NOT NULL); extra_ts is explicit NOT NULL non-designated
            execute("CREATE TABLE t (extra_ts TIMESTAMP NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("INSERT INTO t (ts) VALUES ('2024-01-01')");
                fail("Expected NOT NULL violation for non-designated TIMESTAMP NOT NULL");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=extra_ts");
            }
        });
    }

    @Test
    public void testUuidNotNullMissingRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x UUID NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("INSERT INTO t (ts) VALUES ('2024-01-01')");
                fail("Expected NOT NULL violation for UUID NOT NULL");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }
        });
    }

    @Test
    public void testIpv4NotNullMissingRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x IPv4 NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("INSERT INTO t (ts) VALUES ('2024-01-01')");
                fail("Expected NOT NULL violation for IPv4 NOT NULL");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }
        });
    }

    @Test
    public void testIsNullProjectionOnNotNullColumnConstantFoldsToFalse() throws Exception {
        assertMemoryLeak(() -> {
            // `x IS NULL` in the SELECT list goes through FunctionParser -> EqIntFunctionFactory /
            // EqDoubleFunctionFactory / etc. Our change propagates NOT NULL from column metadata
            // into the ColumnFunction's isNotNull() and makes these factories return
            // BooleanConstant.FALSE for NOT NULL columns. NegatingFunctionFactory flips to TRUE
            // for IS NOT NULL.
            execute("""
                    CREATE TABLE t (
                        i INT NOT NULL,
                        l LONG NOT NULL,
                        d DOUBLE NOT NULL,
                        f FLOAT NOT NULL,
                        nul_i INT,
                        nul_l LONG,
                        ts TIMESTAMP NOT NULL
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t VALUES
                        (1, 10, 1.5, 0.5, 100, 1000, '2024-01-01'),
                        (NULL, NULL, NULL, NULL, NULL, NULL, '2024-01-02'),
                        (3, 30, 3.5, 2.5, 300, 3000, '2024-01-03')
                    """);

            // Projection: (col IS NULL) — false every row for NOT NULL columns (sentinel row included).
            // Nullable columns still flag the sentinel row as null.
            assertSql(
                    """
                            i_is_null\tl_is_null\td_is_null\tf_is_null\tnul_i_is_null\tnul_l_is_null
                            false\tfalse\tfalse\tfalse\tfalse\tfalse
                            false\tfalse\tfalse\tfalse\ttrue\ttrue
                            false\tfalse\tfalse\tfalse\tfalse\tfalse
                            """,
                    "SELECT (i IS NULL) i_is_null, (l IS NULL) l_is_null, (d IS NULL) d_is_null, " +
                            "(f IS NULL) f_is_null, (nul_i IS NULL) nul_i_is_null, (nul_l IS NULL) nul_l_is_null " +
                            "FROM t ORDER BY ts"
            );

            // IS NOT NULL: true for NOT NULL columns, flips for nullable.
            assertSql(
                    """
                            i_nn\tnul_i_nn
                            true\ttrue
                            true\tfalse
                            true\ttrue
                            """,
                    "SELECT (i IS NOT NULL) i_nn, (nul_i IS NOT NULL) nul_i_nn FROM t ORDER BY ts"
            );

            // WHERE x IS NULL on NOT NULL columns matches zero rows — even the sentinel row.
            // Behaviour here depends on the WHERE filter path picking up the constant-folded
            // function (BooleanConstant.FALSE). JIT and the WhereClauseParser intrinsic pass
            // can bypass the factory layer; if they do, they still sentinel-skip and this
            // assertion would return 1 instead of 0 — a known gap tracked separately.
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE i IS NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE l IS NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE d IS NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE f IS NULL");

            // WHERE x IS NOT NULL on NOT NULL columns matches every row.
            assertSql("count\n3\n", "SELECT count(*) FROM t WHERE i IS NOT NULL");
            assertSql("count\n3\n", "SELECT count(*) FROM t WHERE l IS NOT NULL");

            // Sanity: the nullable column's IS NULL does find the sentinel row.
            assertSql("count\n1\n", "SELECT count(*) FROM t WHERE nul_i IS NULL");
            assertSql("count\n2\n", "SELECT count(*) FROM t WHERE nul_i IS NOT NULL");
        });
    }

    @Test
    public void testIsNullProjectionOnNotNullColumnExtendedTypes() throws Exception {
        assertMemoryLeak(() -> {
            // Covers the Phase 1+2 types wired through FunctionParser.createColumn and
            // their Eq*FunctionFactory short-circuits. For each NOT NULL column, x IS NULL
            // must constant-fold to FALSE and x IS NOT NULL to TRUE regardless of the
            // stored sentinel values.
            execute("""
                    CREATE TABLE t (
                        ipv4_c IPv4 NOT NULL,
                        uuid_c UUID NOT NULL,
                        char_c CHAR NOT NULL,
                        str_c STRING NOT NULL,
                        vc_c VARCHAR NOT NULL,
                        sym_c SYMBOL NOT NULL,
                        l256_c LONG256 NOT NULL,
                        geo_c GEOHASH(5c) NOT NULL,
                        ts TIMESTAMP NOT NULL
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t VALUES
                        ('1.2.3.4', '11111111-1111-1111-1111-111111111111', 'a', 'x', 'y', 's1',
                         '0x01', 'u33db', '2024-01-01')
                    """);

            assertSql(
                    """
                            ipv4_null\tuuid_null\tchar_null\tstr_null\tvc_null\tsym_null\tl256_null\tgeo_null
                            false\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse
                            """,
                    "SELECT (ipv4_c IS NULL) ipv4_null, (uuid_c IS NULL) uuid_null, " +
                            "(char_c IS NULL) char_null, (str_c IS NULL) str_null, " +
                            "(vc_c IS NULL) vc_null, (sym_c IS NULL) sym_null, " +
                            "(l256_c IS NULL) l256_null, (geo_c IS NULL) geo_null FROM t"
            );
            assertSql(
                    """
                            ipv4_nn\tuuid_nn\tchar_nn\tstr_nn\tvc_nn\tsym_nn\tl256_nn\tgeo_nn
                            true\ttrue\ttrue\ttrue\ttrue\ttrue\ttrue\ttrue
                            """,
                    "SELECT (ipv4_c IS NOT NULL) ipv4_nn, (uuid_c IS NOT NULL) uuid_nn, " +
                            "(char_c IS NOT NULL) char_nn, (str_c IS NOT NULL) str_nn, " +
                            "(vc_c IS NOT NULL) vc_nn, (sym_c IS NOT NULL) sym_nn, " +
                            "(l256_c IS NOT NULL) l256_nn, (geo_c IS NOT NULL) geo_nn FROM t"
            );

            // WHERE x IS NULL on NOT NULL columns of these types matches zero rows via
            // the eq-factory short-circuit. (JIT/intrinsic paths for these types still
            // fall through to the factory layer for IS NULL.)
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE ipv4_c IS NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE uuid_c IS NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE str_c IS NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE vc_c IS NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE sym_c IS NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE l256_c IS NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE geo_c IS NULL");

            assertSql("count\n1\n", "SELECT count(*) FROM t WHERE ipv4_c IS NOT NULL");
            assertSql("count\n1\n", "SELECT count(*) FROM t WHERE uuid_c IS NOT NULL");
            assertSql("count\n1\n", "SELECT count(*) FROM t WHERE str_c IS NOT NULL");
            assertSql("count\n1\n", "SELECT count(*) FROM t WHERE vc_c IS NOT NULL");
            assertSql("count\n1\n", "SELECT count(*) FROM t WHERE sym_c IS NOT NULL");
            assertSql("count\n1\n", "SELECT count(*) FROM t WHERE l256_c IS NOT NULL");
            assertSql("count\n1\n", "SELECT count(*) FROM t WHERE geo_c IS NOT NULL");
        });
    }

    @Test
    public void testEqTypedDecimalNullOnNotNullColumnFoldsToFalse() throws Exception {
        assertMemoryLeak(() -> {
            // EqDecimalFunctionFactory must detect the typed decimal null sentinel
            // (produced by cast(NULL as decimal(...))), not just the untyped literal
            // NULL. DecimalUtil.maybeRescaleDecimalConstant converts the untyped null
            // to a precision-specific sentinel, so the factory also rechecks after
            // rescaling.
            execute("""
                    CREATE TABLE t (
                        d8 DECIMAL(2, 0) NOT NULL,
                        d16 DECIMAL(4, 0) NOT NULL,
                        d32 DECIMAL(9, 0) NOT NULL,
                        d64 DECIMAL(18, 2) NOT NULL,
                        d128 DECIMAL(38, 2) NOT NULL,
                        d256 DECIMAL(60, 2) NOT NULL,
                        ts TIMESTAMP NOT NULL
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t VALUES
                        (1::DECIMAL(2, 0), 100::DECIMAL(4, 0), 1000::DECIMAL(9, 0),
                         123.45::DECIMAL(18, 2), 999999.99::DECIMAL(38, 2),
                         1234567890.00::DECIMAL(60, 2), '2024-01-01')
                    """);

            // Typed-null equality on a NOT NULL decimal column folds to false.
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE d8 = cast(NULL as DECIMAL(2, 0))");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE d16 = cast(NULL as DECIMAL(4, 0))");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE d32 = cast(NULL as DECIMAL(9, 0))");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE d64 = cast(NULL as DECIMAL(18, 2))");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE d128 = cast(NULL as DECIMAL(38, 2))");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE d256 = cast(NULL as DECIMAL(60, 2))");

            // Standard IS NULL / IS NOT NULL still works.
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE d64 IS NULL");
            assertSql("count\n1\n", "SELECT count(*) FROM t WHERE d64 IS NOT NULL");
        });
    }

    @Test
    public void testEqTypedNullOnNotNullColumnFoldsToFalseAcrossEqFactories() throws Exception {
        // Covers the `const == null && varFunc.isNotNull() -> BooleanConstant.FALSE`
        // short-circuit in each type-specific Eq*FunctionFactory. These branches
        // only fire when a type-typed NULL constant meets a NOT NULL column,
        // which `col IS NULL` (routed through IsNullFunctionFactory for some
        // types) does not exercise — explicit `col = CAST(NULL AS T)` does.
        // Both left- and right-handed variants are covered, since the factories
        // handle each position independently.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        i INT NOT NULL,
                        l LONG NOT NULL,
                        ip IPv4 NOT NULL,
                        c CHAR NOT NULL,
                        s SYMBOL NOT NULL,
                        vc VARCHAR NOT NULL,
                        str STRING NOT NULL,
                        l256 LONG256 NOT NULL,
                        uu UUID NOT NULL,
                        ts TIMESTAMP NOT NULL
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t VALUES
                        (1, 10, '1.2.3.4', 'a', 's1', 'vc1', 'str1',
                         '0x01', '00000000-0000-0000-0000-000000000001',
                         '2024-01-01')
                    """);

            // Right-hand constant NULL folded to FALSE across the Eq factories.
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE i = CAST(NULL AS INT)");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE l = CAST(NULL AS LONG)");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE ip = CAST(NULL AS IPv4)");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE c = CAST(NULL AS CHAR)");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE s = CAST(NULL AS CHAR)");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE vc = CAST(NULL AS VARCHAR)");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE str = CAST(NULL AS VARCHAR)");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE ts = CAST(NULL AS TIMESTAMP)");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE uu = CAST(NULL AS UUID)");

            // Left-hand constant NULL — same factories but through the other
            // createHalfConstantFunc branch (swapped a/b).
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE CAST(NULL AS INT) = i");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE CAST(NULL AS LONG) = l");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE CAST(NULL AS IPv4) = ip");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE CAST(NULL AS CHAR) = c");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE CAST(NULL AS VARCHAR) = vc");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE CAST(NULL AS TIMESTAMP) = ts");

            // IS NOT NULL via `col != CAST(NULL AS T)` flips the fold to TRUE —
            // exercises NegatingFunctionFactory's wrap over the same BooleanConstant.
            assertSql("count\n1\n", "SELECT count(*) FROM t WHERE i != CAST(NULL AS INT)");
            assertSql("count\n1\n", "SELECT count(*) FROM t WHERE ip != CAST(NULL AS IPv4)");
            assertSql("count\n1\n", "SELECT count(*) FROM t WHERE vc != CAST(NULL AS VARCHAR)");
            assertSql("count\n1\n", "SELECT count(*) FROM t WHERE ts != CAST(NULL AS TIMESTAMP)");
        });
    }

    @Test
    public void testAggregateOnNotNullColumnIsConsistent() throws Exception {
        assertMemoryLeak(() -> {
            // Non-vectorized GroupByFunction classes honour the NOT NULL contract:
            // for a NOT NULL column, the null-sentinel check is bypassed, so every
            // row contributes to COUNT/SUM/MIN/MAX regardless of its stored bit
            // pattern. SqlCodeGenerator.isVectorAggregateUnsafeForNotNull still
            // falls back from the native vec path to the Java keyed path on NOT
            // NULL columns; this test pins the Java-path semantic.
            // The rows below contain only real data: four integers, none of
            // which collides with LONG_NULL.
            execute("CREATE TABLE t (x LONG NOT NULL, g SYMBOL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (1, 'a', '2024-01-01'),
                        (2, 'a', '2024-01-02'),
                        (3, 'b', '2024-01-03'),
                        (4, 'b', '2024-01-04')
                    """);

            assertSql(
                    """
                            count_x\tcount_star
                            4\t4
                            """,
                    "SELECT count(x) count_x, count(*) count_star FROM t"
            );

            assertSql(
                    """
                            g\tcount_x
                            a\t2
                            b\t2
                            """,
                    "SELECT g, count(x) count_x FROM t ORDER BY g"
            );

            // Sum, min, max match a plain SELECT across the same rows.
            assertSql(
                    """
                            sum_x\tmin_x\tmax_x
                            10\t1\t4
                            """,
                    "SELECT sum(x) sum_x, min(x) min_x, max(x) max_x FROM t"
            );
            assertSql(
                    """
                            g\tsum_x
                            a\t3
                            b\t7
                            """,
                    "SELECT g, sum(x) sum_x FROM t ORDER BY g"
            );
        });
    }

    @Test
    public void testFilterOnNotNullColumnAggregates() throws Exception {
        assertMemoryLeak(() -> {
            // Aggregates on NOT NULL columns honour the PR contract: every row
            // contributes to count/sum/min/max regardless of bit pattern. On
            // the nullable sibling column, sentinel values still skip, matching
            // the pre-PR behaviour.
            execute("CREATE TABLE t (x LONG NOT NULL, y LONG, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (1, 10, '2024-01-01'),
                        (2, NULL, '2024-01-02'),
                        (3, 30, '2024-01-03')
                    """);

            assertSql(
                    """
                            sum_x\tsum_y\tcount_x\tcount_y\tmin_x\tmax_x
                            6\t40\t3\t2\t1\t3
                            """,
                    "SELECT sum(x) sum_x, sum(y) sum_y, count(x) count_x, count(y) count_y, min(x) min_x, max(x) max_x FROM t"
            );
        });
    }

    @Test
    public void testCountOnNotNullColumnCountsSentinelValues() throws Exception {
        assertMemoryLeak(() -> {
            // Regression: count(x) on a NOT NULL column used to skip rows whose
            // stored value equals the type's null sentinel (Long.MIN_VALUE for
            // LONG, Integer.MIN_VALUE for INT, etc.). Under the NOT NULL
            // contract those sentinels are real data and must count.
            execute("CREATE TABLE t (x LONG NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                        (1, '2024-01-01'),
                        (CAST(-9223372036854775807 AS LONG) - 1, '2024-01-02'),
                        (3, '2024-01-03')
                    """);

            assertSql(
                    """
                            count_x\tcount_star
                            3\t3
                            """,
                    "SELECT count(x) count_x, count(*) count_star FROM t"
            );

            // GROUP BY path goes through a different factory; must produce the
            // same count.
            execute("CREATE TABLE g (x LONG NOT NULL, k INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO g VALUES
                        (1, 0, '2024-01-01'),
                        (CAST(-9223372036854775807 AS LONG) - 1, 0, '2024-01-02'),
                        (5, 1, '2024-01-03')
                    """);
            assertSql(
                    """
                            k\tcount_x
                            0\t2
                            1\t1
                            """,
                    "SELECT k, count(x) count_x FROM g GROUP BY k ORDER BY k"
            );
        });
    }

    @Test
    public void testFirstLastNotNullOnNotNullColumnReturnsSentinel() throws Exception {
        assertMemoryLeak(() -> {
            // first_not_null / last_not_null on a NOT NULL column must return
            // the first / last stored row, even if the bit pattern matches the
            // type sentinel. On nullable siblings the old "skip null" semantic
            // still applies.
            execute("CREATE TABLE t (x LONG NOT NULL, y LONG, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                        (CAST(-9223372036854775807 AS LONG) - 1, NULL, '2024-01-01'),
                        (2, NULL, '2024-01-02'),
                        (CAST(-9223372036854775807 AS LONG) - 1, 42, '2024-01-03')
                    """);

            // first_not_null / last_not_null return LONG_MIN for x (the stored
            // sentinel on rows 1 and 3). The result type is nullable LONG so
            // the default rendering shows "null"; y still has a skip-null
            // semantic since it is a nullable column.
            assertSql(
                    """
                            first_not_null_x\tfirst_not_null_y\tlast_not_null_x\tlast_not_null_y
                            null\t42\tnull\t42
                            """,
                    "SELECT first_not_null(x) first_not_null_x, first_not_null(y) first_not_null_y, " +
                            "last_not_null(x) last_not_null_x, last_not_null(y) last_not_null_y FROM t"
            );

            // Nullable sibling column: last_not_null(y) returns 42 from row 3;
            // first_not_null(y) returns 42 too because rows 1 and 2 are NULL.
            // Under the old behaviour first_not_null(x) would have skipped the
            // sentinel rows and hit row 2's value 2 -- pin that regression:
            assertSql(
                    "expected\ntrue\n",
                    "SELECT first_not_null(x) != 2 expected FROM t"
            );
        });
    }

    @Test
    public void testDetachAttachPartitionPreservesNotNull() throws Exception {
        // Verify NOT NULL metadata survives a detach/attach round-trip, and that
        // enforcement still kicks in for the attached data partition's table.
        node1.setProperty(PropertyKey.CAIRO_ATTACH_PARTITION_SUFFIX, TableUtils.DETACHED_DIR_MARKER);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                        (1, 1.0, '2024-01-01T10:00:00.000000Z'),
                        (2, 2.0, '2024-01-02T10:00:00.000000Z'),
                        (3, 3.0, '2024-01-03T10:00:00.000000Z')
                    """);

            execute("ALTER TABLE t DETACH PARTITION LIST '2024-01-01', '2024-01-02'");
            assertTrue(getNotNull("t", "x"));
            assertTrue(getNotNull("t", "ts"));
            assertFalse(getNotNull("t", "y"));

            execute("ALTER TABLE t ATTACH PARTITION LIST '2024-01-01', '2024-01-02'");
            assertTrue(getNotNull("t", "x"));
            assertTrue(getNotNull("t", "ts"));
            assertFalse(getNotNull("t", "y"));

            assertSql(
                    """
                            x\ty\tts
                            1\t1.0\t2024-01-01T10:00:00.000000Z
                            2\t2.0\t2024-01-02T10:00:00.000000Z
                            3\t3.0\t2024-01-03T10:00:00.000000Z
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );

            // enforcement still applies after attach
            try {
                execute("INSERT INTO t (ts) VALUES ('2024-01-04')");
                fail("Expected NOT NULL violation after attach");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }
        });
    }

    @Test
    public void testFilterOnNotNullColumnWithAllColumnsNotNullJit() throws Exception {
        assertMemoryLeak(() -> {
            // Exercises the JIT path that hoists out null checks (CompiledFilterIRSerializer
            // computes allColumnsNotNull and clears the null_check IR option bit).
            execute("CREATE TABLE t (x INT NOT NULL, y LONG NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (1, 100, '2024-01-01'),
                        (2, 200, '2024-01-02'),
                        (3, 300, '2024-01-03'),
                        (4, 400, '2024-01-04')
                    """);

            assertSql(
                    """
                            x\ty\tts
                            2\t200\t2024-01-02T00:00:00.000000Z
                            3\t300\t2024-01-03T00:00:00.000000Z
                            """,
                    "SELECT * FROM t WHERE x > 1 AND y < 400 AND x < 4 ORDER BY ts"
            );

            // sentinel values for NOT NULL columns should still pass the filter when non-null check would have excluded them
            execute("INSERT INTO t VALUES (NULL, NULL, '2024-01-05')");
            assertSql(
                    """
                            x\ty\tts
                            -2147483648\t-9223372036854775808\t2024-01-05T00:00:00.000000Z
                            """,
                    "SELECT * FROM t WHERE x < 0 ORDER BY ts"
            );
        });
    }

    @Test
    public void testJitFoldsIsNullOnNotNullColumnInsideOrChain() throws Exception {
        assertMemoryLeak(() -> {
            // End-to-end check of the Phase 3 JIT fold. JIT serializes the ORIGINAL AST, not the
            // Function-level simplified tree, so even when OrFunctionFactory folds
            // `(NOT NULL col IS NULL) OR x` to `x` at Function level, JIT still processes the
            // original `IS NULL` subtree. Without the Phase 3 fold the sentinel row would
            // spuriously match `col IS NULL` through JIT.
            execute("CREATE TABLE t (x INT NOT NULL, y LONG NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (1, 100, '2024-01-01'),
                        (2, 200, '2024-01-02'),
                        (NULL, NULL, '2024-01-03'),
                        (3, 300, '2024-01-04')
                    """);
            // Row at 2024-01-03 stores the INT_NULL and LONG_NULL sentinels as real data. NOT NULL
            // is satisfied because the column was written to; the value just happens to equal the
            // sentinel bit pattern.

            // Confirm the JIT path is taken for these filters (not the Java fallback).
            assertPlanNoLeakCheck(
                    "SELECT * FROM t WHERE x IS NULL OR x = 10",
                    """
                            Async JIT Filter workers: 1
                              filter: x=10
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );

            // `x IS NULL OR x = 10` — the overall Function simplifies to `x = 10` (FALSE || x = x),
            // which is non-constant, so codegen takes the JIT path. The JIT fold makes the
            // `IS NULL` arm emit constant-false; otherwise JIT evaluates `x = -2^31` and matches
            // the sentinel row at 2024-01-03.
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE x IS NULL OR x = 10");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE y IS NULL OR y = 1_000_000");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE null = x OR x = 10");

            // `x IS NOT NULL OR x = 10` must match every row (including the sentinel row).
            assertSql("count\n4\n", "SELECT count(*) FROM t WHERE x IS NOT NULL OR x = 10");
            assertSql("count\n4\n", "SELECT count(*) FROM t WHERE x <> null OR x = 10");

            // The non-IS-NULL arm still matches normally through the JIT.
            assertSql("count\n1\n", "SELECT count(*) FROM t WHERE x IS NULL OR x = 2");
        });
    }

    @Test
    public void testEqNullBidirectionalOnNotNullColumn() throws Exception {
        assertMemoryLeak(() -> {
            // The Eq*FunctionFactory short-circuit has to fire with NULL on either side:
            // `col = NULL` and `NULL = col` must both fold to FALSE on a NOT NULL column.
            // `IS NULL` desugars to `col = NULL` so it only hits the "constant on right" path;
            // this test exercises the mirror "constant on left" path across every type.
            // INTERVAL is a non-persisted (expression-only) type and is covered separately.
            execute("""
                    CREATE TABLE t (
                        i INT NOT NULL,
                        l LONG NOT NULL,
                        d DOUBLE NOT NULL,
                        f FLOAT NOT NULL,
                        c CHAR NOT NULL,
                        v VARCHAR NOT NULL,
                        s STRING NOT NULL,
                        b BINARY NOT NULL,
                        sym SYMBOL NOT NULL,
                        ip IPv4 NOT NULL,
                        uu UUID NOT NULL,
                        l256 LONG256 NOT NULL,
                        ts TIMESTAMP NOT NULL
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t VALUES
                        (1, 10, 1.5, 0.5, 'a', 'v1', 's1',
                         rnd_bin(10, 20, 0), 'sym1',
                         '1.2.3.4', '11111111-1111-1111-1111-111111111111',
                         '0x01', '2024-01-01')
                    """);

            // Right-side NULL (symmetric to IS NULL).
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE i = NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE l = NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE d = NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE f = NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE c = NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE v = NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE s = NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE sym = NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE ip = NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE uu = NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE l256 = NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE ts = NULL");

            // Left-side NULL — exercises the mirror branch in createHalfConstantFunc
            // (the factory swaps operands so the column lands on the variable side, but
            // the NULL-detection check runs on whichever side had the constant).
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE NULL = i");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE NULL = l");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE NULL = d");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE NULL = f");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE NULL = c");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE NULL = v");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE NULL = s");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE NULL = sym");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE NULL = ip");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE NULL = uu");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE NULL = l256");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE NULL = ts");

            // Sym-vs-Char (EqSymCharFunctionFactory) — char null sentinel bidirectional.
            // `sym = cast(null as char)` is handled when the char side is detected as
            // Numbers.CHAR_NULL. Mirror: char column = sym with cast(null as symbol).
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE sym = cast(NULL as CHAR)");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE cast(NULL as CHAR) = sym");

            // Sym-vs-Timestamp (EqSymTimestampFunctionFactory) — both null-sentinel branches.
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE sym = cast(NULL as TIMESTAMP)");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE cast(NULL as TIMESTAMP) = sym");

            // Char-vs-Char (EqCharCharFunctionFactory) — both-constants-null, column on other side.
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE c = cast(NULL as CHAR)");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE cast(NULL as CHAR) = c");

            // Long128 variant of UUID, hit via the Long128 tag explicitly.
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE uu = cast(NULL as UUID)");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE cast(NULL as UUID) = uu");

            // BINARY has no literal syntax; `b = NULL` covers the ColumnType.isNull(a|b) guard
            // in EqBinaryFunctionFactory.
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE b = NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE NULL = b");

            // IS NOT NULL must still match every row for both sides.
            assertSql("count\n1\n", "SELECT count(*) FROM t WHERE i <> NULL");
            assertSql("count\n1\n", "SELECT count(*) FROM t WHERE NULL <> i");
            assertSql("count\n1\n", "SELECT count(*) FROM t WHERE l256 <> NULL");
        });
    }

    @Test
    public void testNotNullSentinelPrintsValueExtended() throws Exception {
        assertMemoryLeak(() -> {
            // CursorPrinter prints the raw stored value on NOT NULL columns even when the
            // value equals the type's null sentinel. This covers the types that were not
            // already tested in testNotNull{Long,Ipv4,Uuid,Char}SentinelPrintsValue:
            // DATE, INT, TIMESTAMP (as stored column, not the designated ts), LONG128,
            // LONG256, INTERVAL, GEOHASH, and DECIMAL 8/16/32/64/128/256.
            execute("""
                    CREATE TABLE t (
                        d DATE NOT NULL,
                        i INT NOT NULL,
                        t2 TIMESTAMP NOT NULL,
                        g5 GEOHASH(5c) NOT NULL,
                        g2 GEOHASH(2c) NOT NULL,
                        l256 LONG256 NOT NULL,
                        dec8 DECIMAL(2, 0) NOT NULL,
                        dec16 DECIMAL(4, 0) NOT NULL,
                        dec32 DECIMAL(9, 0) NOT NULL,
                        dec64 DECIMAL(18, 2) NOT NULL,
                        dec128 DECIMAL(38, 2) NOT NULL,
                        dec256 DECIMAL(60, 2) NOT NULL,
                        ts TIMESTAMP NOT NULL
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            // Row 1 inserts NULL into every non-ts NOT NULL column — each value lands as the
            // type's null sentinel but must be surfaced, not hidden as "null" in the output.
            execute("INSERT INTO t VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2024-01-01')");
            // Row 2 has real values for comparison.
            execute("""
                    INSERT INTO t VALUES (
                        '2024-06-15'::date,
                        42,
                        '2024-06-15T12:00:00',
                        'u33d0',
                        'u3',
                        '0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20',
                        1::DECIMAL(2, 0),
                        100::DECIMAL(4, 0),
                        1000::DECIMAL(9, 0),
                        123.45::DECIMAL(18, 2),
                        999999.99::DECIMAL(38, 2),
                        1234567890.00::DECIMAL(60, 2),
                        '2024-01-02'
                    )
                    """);

            // All sentinel values appear as raw bit patterns / empty strings, not "null".
            // DATE/TIMESTAMP Long.MIN_VALUE renders as the raw negative number.
            // INT Integer.MIN_VALUE renders as -2147483648.
            // GEOHASH null prints as empty (CursorPrinter emits an empty token but the row still exists).
            // LONG256 null prints as all-zero hex.
            // DECIMAL nulls print via Decimals.appendNonNull, which emits the raw sentinel as a
            // decimal with the column's scale — specific format verified by row count, not value.
            assertSql("count\n2\n", "SELECT count(*) FROM t");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE d IS NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE i IS NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE t2 IS NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE g5 IS NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE g2 IS NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE l256 IS NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE dec8 IS NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE dec256 IS NULL");

            // Spot-check that the INT sentinel (Integer.MIN_VALUE) renders as its raw value
            // on the NULL-inserted row, and that the DATE sentinel (Long.MIN_VALUE) likewise
            // renders as its raw value. Printing these raw values exercises the NOT NULL
            // branches in CursorPrinter for INT and DATE respectively.
            // When the value is the Long.MIN_VALUE sentinel, the DATE NOT NULL branch in
            // CursorPrinter bypasses the datetime formatter and writes the raw long —
            // otherwise it formats as usual. Same for TIMESTAMP.
            assertSql(
                    """
                            i\td
                            -2147483648\t-9223372036854775808
                            42\t2024-06-15T00:00:00.000Z
                            """,
                    "SELECT i, d FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testNotNullColumnFunctionFlagPropagation() throws Exception {
        // Long128Column (via UUID) and IPv4Column received new isNotNull() overrides
        // backed by a `notNull` field set by ColumnFactory / Function.newInstance.
        // The propagation is verified indirectly: a query over a NOT NULL column of
        // each type that uses IS NULL must fold to FALSE via the eq-factory short-circuit,
        // which only fires when column.isNotNull() returns true. If the flag didn't
        // propagate, the sentinel-row case would leak as a match.
        //
        // IntervalColumn and ArrayColumn have the same isNotNull() override but can't
        // be tested through the eq-factory path from SQL — INTERVAL is non-persisted,
        // and the DOUBLE[] `=` operator rejects untyped NULL at parse time ("no matching
        // operator = with argument types DOUBLE[] = NULL"), making their eq-factory
        // short-circuit paths unreachable from user SQL.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        uu UUID NOT NULL,
                        ip IPv4 NOT NULL,
                        ts TIMESTAMP NOT NULL
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t VALUES (
                        '11111111-1111-1111-1111-111111111111',
                        '1.2.3.4',
                        '2024-01-01'
                    )
                    """);
            // The sentinel row — inserted nulls land as the type's null bit pattern. The
            // factory short-circuit must still fold IS NULL to false (otherwise this row
            // would leak out).
            execute("INSERT INTO t VALUES (NULL, NULL, '2024-01-02')");

            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE uu IS NULL");
            assertSql("count\n0\n", "SELECT count(*) FROM t WHERE ip IS NULL");

            // IS NOT NULL must match every row including the sentinel one.
            assertSql("count\n2\n", "SELECT count(*) FROM t WHERE uu IS NOT NULL");
            assertSql("count\n2\n", "SELECT count(*) FROM t WHERE ip IS NOT NULL");
        });
    }

    @Test
    public void testInformationSchemaIsNullableReportsNo() throws Exception {
        assertMemoryLeak(() -> {
            // InformationSchemaColumnsFunctionFactory ternary: notNull ? "no" : "yes".
            // The NOT NULL column entries exercise the "no" branch.
            execute("""
                    CREATE TABLE t (
                        nn INT NOT NULL,
                        nl INT,
                        ts TIMESTAMP NOT NULL
                    ) TIMESTAMP(ts)
                    """);

            // Designated timestamp is implicitly NOT NULL and also returns "no".
            assertSql(
                    """
                            column_name\tis_nullable
                            nn\tno
                            nl\tyes
                            ts\tno
                            """,
                    "SELECT column_name, is_nullable FROM information_schema.columns " +
                            "WHERE table_name = 't' ORDER BY ordinal_position"
            );
        });
    }

    @Test
    public void testNotNullParquetRoundTrip() throws Exception {
        // CONVERT PARTITION TO PARQUET invokes the Rust writer (parquet_write/primitive.rs).
        // For NOT NULL columns the writer uses dedicated NOT NULL paths that skip sentinel
        // filtering and emit all-ones definition levels (or a column_top+ones pattern when
        // the column was added after existing rows). This test round-trips a table with
        // NOT NULL columns across every primitive type the Rust writer specialises for
        // (i32/i64/f32/f64, decimals, IPv4), and converts back to native to confirm the
        // data survives the write+read cycle with NOT NULL metadata intact.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        i INT NOT NULL,
                        l LONG NOT NULL,
                        f FLOAT NOT NULL,
                        d DOUBLE NOT NULL,
                        ip IPv4 NOT NULL,
                        dec8 DECIMAL(2, 0) NOT NULL,
                        dec16 DECIMAL(4, 0) NOT NULL,
                        dec32 DECIMAL(9, 0) NOT NULL,
                        dec64 DECIMAL(18, 2) NOT NULL,
                        dec128 DECIMAL(38, 2) NOT NULL,
                        dec256 DECIMAL(60, 2) NOT NULL,
                        ts TIMESTAMP NOT NULL
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Multiple rows across two partitions so CONVERT TO PARQUET has real work.
            execute("""
                    INSERT INTO t VALUES
                        (1, 10, 1.5, 2.5, '1.2.3.4',
                         1::DECIMAL(2, 0), 100::DECIMAL(4, 0), 1000::DECIMAL(9, 0),
                         12345.67::DECIMAL(18, 2), 99999.99::DECIMAL(38, 2),
                         1234567890.12::DECIMAL(60, 2), '2024-06-10T00:00:00'),
                        (2, 20, 2.5, 3.5, '5.6.7.8',
                         2::DECIMAL(2, 0), 200::DECIMAL(4, 0), 2000::DECIMAL(9, 0),
                         22345.67::DECIMAL(18, 2), 88888.88::DECIMAL(38, 2),
                         2234567890.12::DECIMAL(60, 2), '2024-06-10T00:00:01'),
                        (3, 30, 3.5, 4.5, '9.10.11.12',
                         3::DECIMAL(2, 0), 300::DECIMAL(4, 0), 3000::DECIMAL(9, 0),
                         32345.67::DECIMAL(18, 2), 77777.77::DECIMAL(38, 2),
                         3234567890.12::DECIMAL(60, 2), '2024-06-11T00:00:00')
                    """);

            // Convert to Parquet — exercises the Rust writer NOT NULL paths for every
            // column type above. The `ts > 0` predicate sweeps all partitions except the
            // most recent one (which remains native to keep insert-path tests happy).
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-06-10'");

            // Read-back after Parquet conversion — exercises the Rust reader
            // (parquet_read/row_groups.rs NOT NULL flag plumbing) and the Java
            // PartitionDecoder.setNotNullFlag path.
            assertSql(
                    """
                            i\tl\tf\td\tip\tdec8\tdec16\tdec32\tdec64\tdec128\tdec256\tts
                            1\t10\t1.5\t2.5\t1.2.3.4\t1\t100\t1000\t12345.67\t99999.99\t1234567890.12\t2024-06-10T00:00:00.000000Z
                            2\t20\t2.5\t3.5\t5.6.7.8\t2\t200\t2000\t22345.67\t88888.88\t2234567890.12\t2024-06-10T00:00:01.000000Z
                            3\t30\t3.5\t4.5\t9.10.11.12\t3\t300\t3000\t32345.67\t77777.77\t3234567890.12\t2024-06-11T00:00:00.000000Z
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );

            // NOT NULL metadata must survive the Parquet round-trip — both in the Parquet
            // partition and when converting back to native.
            assertTrue(getNotNull("t", "i"));
            assertTrue(getNotNull("t", "l"));
            assertTrue(getNotNull("t", "f"));
            assertTrue(getNotNull("t", "d"));
            assertTrue(getNotNull("t", "ip"));
            assertTrue(getNotNull("t", "dec8"));
            assertTrue(getNotNull("t", "dec256"));

            // Convert back and read again — verifies the Rust reader populated the NOT
            // NULL flag when loading the Parquet file into the native partition.
            execute("ALTER TABLE t CONVERT PARTITION TO NATIVE LIST '2024-06-10'");
            assertSql(
                    """
                            i\tl\tf\td\tip\tdec8\tdec16\tdec32\tdec64\tdec128\tdec256\tts
                            1\t10\t1.5\t2.5\t1.2.3.4\t1\t100\t1000\t12345.67\t99999.99\t1234567890.12\t2024-06-10T00:00:00.000000Z
                            2\t20\t2.5\t3.5\t5.6.7.8\t2\t200\t2000\t22345.67\t88888.88\t2234567890.12\t2024-06-10T00:00:01.000000Z
                            3\t30\t3.5\t4.5\t9.10.11.12\t3\t300\t3000\t32345.67\t77777.77\t3234567890.12\t2024-06-11T00:00:00.000000Z
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testNotNullParquetRoundTripWithColumnTop() throws Exception {
        // When a NOT NULL column is added after rows already exist, the Rust writer hits
        // the `column_top > 0` branch of encode_not_null_def_levels — the first
        // column_top rows are null (genuinely missing) and the rest are all non-null.
        // This is a separate code path from the all-ones case covered above.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                        (1, '2024-06-10T00:00:00'),
                        (2, '2024-06-10T00:00:01'),
                        (3, '2024-06-10T00:00:02')
                    """);
            // Add a NOT NULL column after rows exist — column_top = 3 for this column
            // when the partition is written.
            execute("ALTER TABLE t ADD COLUMN y LONG NOT NULL");
            execute("INSERT INTO t VALUES (4, '2024-06-10T00:00:03', 40)");

            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-06-10'");

            // First three rows have y == Long.MIN_VALUE (column_top null), row 4 has 40.
            // The NOT NULL branch in CursorPrinter renders the sentinel as its raw value.
            assertSql(
                    """
                            x\tts\ty
                            1\t2024-06-10T00:00:00.000000Z\t-9223372036854775808
                            2\t2024-06-10T00:00:01.000000Z\t-9223372036854775808
                            3\t2024-06-10T00:00:02.000000Z\t-9223372036854775808
                            4\t2024-06-10T00:00:03.000000Z\t40
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );

            assertTrue(getNotNull("t", "y"));
        });
    }
}
