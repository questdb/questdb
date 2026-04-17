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
    public void testAggregateOnNotNullColumnIsConsistent() throws Exception {
        assertMemoryLeak(() -> {
            // Every native aggregate kernel today (both the vectorized Vect.* / Rosti::keyed*
            // path and the non-vectorized GroupByFunction classes) sentinel-skips, which
            // disagrees with the NOT NULL semantic that "sentinels are valid values".
            // SqlCodeGenerator.isVectorAggregateUnsafeForNotNull refuses the vec path for
            // NOT NULL columns so a Java-only fast-path cannot silently diverge from the
            // keyed path; the query falls back to the non-vectorized GroupByFunction
            // pipeline. That pipeline also sentinel-skips today — fixing the semantic
            // properly waits for validity bitmaps (Part 2 of the null-bitmaps plan). The
            // invariant we DO enforce here: keyed and non-keyed aggregates agree for the
            // same data, and sum of per-group aggregates equals the non-keyed aggregate.
            execute("CREATE TABLE t (x LONG NOT NULL, g SYMBOL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (1, 'a', '2024-01-01'),
                        (2, 'a', '2024-01-02'),
                        (3, 'b', '2024-01-03'),
                        (NULL, 'b', '2024-01-04')
                    """);

            assertSql(
                    """
                            count_x\tcount_star
                            3\t4
                            """,
                    "SELECT count(x) count_x, count(*) count_star FROM t"
            );

            assertSql(
                    """
                            g\tcount_x
                            a\t2
                            b\t1
                            """,
                    "SELECT g, count(x) count_x FROM t ORDER BY g"
            );

            // Sum, min, max are similarly consistent between keyed and non-keyed paths.
            assertSql(
                    """
                            sum_x\tmin_x\tmax_x
                            6\t1\t3
                            """,
                    "SELECT sum(x) sum_x, min(x) min_x, max(x) max_x FROM t"
            );
            assertSql(
                    """
                            g\tsum_x
                            a\t3
                            b\t3
                            """,
                    "SELECT g, sum(x) sum_x FROM t ORDER BY g"
            );
        });
    }

    @Test
    public void testFilterOnNotNullColumnAggregates() throws Exception {
        assertMemoryLeak(() -> {
            // Aggregates on NOT NULL columns should produce the same results as nullable
            // (but conceptually could skip null checks). Verify correctness at minimum.
            execute("CREATE TABLE t (x LONG NOT NULL, y LONG, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (1, 10, '2024-01-01'),
                        (2, NULL, '2024-01-02'),
                        (3, 30, '2024-01-03')
                    """);

            // SUM on NOT NULL column vs nullable column
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
}
