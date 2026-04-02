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
    public void testAlterTableAddBooleanAutoNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE t ADD COLUMN b BOOLEAN");

            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata metadata = reader.getMetadata();
                assertTrue(metadata.isNotNull(metadata.getColumnIndex("b")));
            }
        });
    }

    @Test
    public void testAlterTableAddColumnNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
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
            execute("CREATE TABLE t (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
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
            execute("CREATE TABLE t (x INT NULL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata metadata = reader.getMetadata();
                assertFalse(metadata.isNotNull(metadata.getColumnIndex("x")));
            }
        });
    }

    @Test
    public void testBooleanAutoNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (b BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata metadata = reader.getMetadata();
                assertTrue(metadata.isNotNull(metadata.getColumnIndex("b")));
            }

            assertSql(
                    """
                            ddl
                            CREATE TABLE 't' (\s
                            \tb BOOLEAN NOT NULL,
                            \tts TIMESTAMP
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """,
                    "SHOW CREATE TABLE t"
            );
        });
    }

    @Test
    public void testBooleanExplicitNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (b BOOLEAN NOT NULL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            try (TableReader reader = engine.getReader("t")) {
                assertTrue(reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex("b")));
            }
        });
    }

    @Test
    public void testBooleanNullRejected() throws Exception {
        assertException(
                "CREATE TABLE t (b BOOLEAN NULL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                26,
                "NULL is not supported for BOOLEAN columns"
        );
    }

    @Test
    public void testByteAutoNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (b BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

            try (TableReader reader = engine.getReader("t")) {
                assertTrue(reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex("b")));
            }
        });
    }

    @Test
    public void testByteNullRejected() throws Exception {
        assertException(
                "CREATE TABLE t (b BYTE NULL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                23,
                "NULL is not supported for BYTE columns"
        );
    }

    @Test
    public void testAlterColumnDropNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertTrue(getNotNull("t", "x"));

            execute("ALTER TABLE t ALTER COLUMN x SET NULL");
            assertFalse(getNotNull("t", "x"));
        });
    }

    @Test
    public void testAlterColumnDropNotNullBooleanRejected() throws Exception {
        assertException(
                "ALTER TABLE t ALTER COLUMN b SET NULL",
                "CREATE TABLE t (b BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                33,
                "cannot set NULL for BOOLEAN columns"
        );
    }

    @Test
    public void testAlterColumnSetNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertFalse(getNotNull("t", "x"));

            execute("ALTER TABLE t ALTER COLUMN x SET NOT NULL");
            assertTrue(getNotNull("t", "x"));

            assertSql(
                    """
                            ddl
                            CREATE TABLE 't' (\s
                            \tx INT NOT NULL,
                            \tts TIMESTAMP
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """,
                    "SHOW CREATE TABLE t"
            );
        });
    }

    @Test
    public void testAlterColumnSetNotNullThenEnforce() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
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
            execute("CREATE TABLE src (x INT NOT NULL, y DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
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
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata metadata = reader.getMetadata();
                assertTrue(metadata.isNotNull(metadata.getColumnIndex("x")));
                assertFalse(metadata.isNotNull(metadata.getColumnIndex("y")));
                assertFalse(metadata.isNotNull(metadata.getColumnIndex("ts")));
            }

            assertSql(
                    """
                            ddl
                            CREATE TABLE 't' (\s
                            \tx INT NOT NULL,
                            \ty DOUBLE,
                            \tts TIMESTAMP
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """,
                    "SHOW CREATE TABLE t"
            );
        });
    }

    @Test
    public void testDropNotNullColumnThenInsert() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
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
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
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
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
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
            execute("CREATE TABLE t (x INT NOT NULL, y INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
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
            // Sentinel values (INT_NULL, NaN, null) are valid data for NOT NULL columns.
            // NOT NULL only means "column must be written to", not "no sentinel values".
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE NOT NULL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (NULL, NULL, '2024-01-01')");

            assertSql(
                    """
                            x\ty\tts
                            null\tnull\t2024-01-01T00:00:00.000000Z
                            """,
                    "SELECT * FROM t"
            );
        });
    }

    @Test
    public void testEnforceNotNullValidInsertSucceeds() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
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
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
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
    public void testImplicitNotNullBooleanEnforcedByDefault() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (b BOOLEAN, y DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("INSERT INTO t (y, ts) VALUES (1.5, '2024-01-01')");
                fail("Expected NOT NULL violation for BOOLEAN column");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=b");
            }
        });
    }

    @Test
    public void testImplicitNotNullBooleanDefaultValues() throws Exception {
        setProperty(PropertyKey.CAIRO_IMPLICIT_NOT_NULL_DEFAULT_VALUES, "true");
        assertMemoryLeak(() -> {
            // With the config flag enabled, BOOLEAN auto-fills with false when not provided
            execute("CREATE TABLE t (b BOOLEAN, y DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
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
    public void testInformationSchemaColumnsShowsNullability() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

            assertSql(
                    """
                            table_catalog\ttable_schema\ttable_name\tcolumn_name\tordinal_position\tcolumn_default\tis_nullable\tdata_type
                            qdb\tpublic\tt\tx\t0\t\tno\tinteger
                            qdb\tpublic\tt\ty\t1\t\tyes\tdouble precision
                            qdb\tpublic\tt\tts\t2\t\tyes\ttimestamp without time zone
                            """,
                    "SELECT * FROM information_schema.columns() ORDER BY ordinal_position"
            );
        });
    }

    @Test
    public void testImplicitNotNullNotPreservedOnTypeChange() throws Exception {
        assertMemoryLeak(() -> {
            // BOOLEAN is implicitly NOT NULL. Changing to LONG should NOT
            // inherit the implicit constraint — the user never asked for it.
            execute("CREATE TABLE t (b BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

            try (TableReader reader = engine.getReader("t")) {
                assertTrue(reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex("b")));
            }

            execute("ALTER TABLE t ALTER COLUMN b TYPE LONG");

            try (TableReader reader = engine.getReader("t")) {
                assertFalse(reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex("b")));
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
            execute("CREATE TABLE t (x INT NOT NULL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

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
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

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
    public void testShortAutoNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            try (TableReader reader = engine.getReader("t")) {
                assertTrue(reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex("s")));
            }
        });
    }

    @Test
    public void testShortNullRejected() throws Exception {
        assertException(
                "CREATE TABLE t (s SHORT NULL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY",
                24,
                "NULL is not supported for SHORT columns"
        );
    }

    @Test
    public void testShowColumnsIncludesNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT NOT NULL, y DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");

            assertSql(
                    """
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tnotNull\tupsertKey
                            x\tINT\tfalse\t0\tfalse\t0\t0\tfalse\ttrue\tfalse
                            y\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\tfalse
                            ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\tfalse
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
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);

            assertSql(
                    """
                            ddl
                            CREATE TABLE 't' (\s
                            \ta INT NOT NULL,
                            \tb DOUBLE,
                            \tc STRING NOT NULL,
                            \tts TIMESTAMP
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """,
                    "SHOW CREATE TABLE t"
            );
        });
    }
}
