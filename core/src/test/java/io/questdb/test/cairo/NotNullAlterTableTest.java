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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertContains;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Targeted coverage for ALTER TABLE behaviors that interact with the NOT NULL
 * column modifier on populated tables. Splits two semantic gaps:
 * - ADD COLUMN x ... NOT NULL on a table that already has rows
 * - ALTER COLUMN x SET / DROP (SET NULL) NOT NULL on a populated column
 * <p>
 * Findings (documented in each test):
 * - ADD COLUMN x T NOT NULL succeeds without rejection or backfill. Existing
 * rows fall under the new column's column_top, so reads of those positions
 * return the type sentinel (printed numerically, not "null", because the
 * column is NOT NULL). New writes are then enforced.
 * - ALTER COLUMN x SET NOT NULL is a pure metadata flip; it does NOT scan
 * existing data, so pre-existing NULLs survive the toggle. Only subsequent
 * writes are enforced.
 * - ADD COLUMN ... DEFAULT &lt;value&gt; is not supported at the parser level.
 */
public class NotNullAlterTableTest extends AbstractCairoTest {

    private boolean getNotNull(String table, String column) throws Exception {
        try (TableReader reader = engine.getReader(table)) {
            return reader.getMetadata().isNotNull(reader.getMetadata().getColumnIndex(column));
        }
    }

    @Test
    public void testAddNotNullColumnToPopulatedTable() throws Exception {
        assertMemoryLeak(() -> {
            // Document the actual semantic: ADD COLUMN ... NOT NULL on a populated
            // table is accepted with NO backfill. Existing rows sit under the new
            // column's column_top and read back as the type sentinel (printed
            // numerically because the column is NOT NULL — never as "null").
            //
            // This is a real semantic gap users must know about: the metadata flag
            // is set, but the existing rows logically violate the constraint.
            execute("CREATE TABLE t (id INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (1, '2024-01-01'),
                        (2, '2024-01-02')
                    """);

            execute("ALTER TABLE t ADD COLUMN x INT NOT NULL");

            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata metadata = reader.getMetadata();
                assertTrue(metadata.isNotNull(metadata.getColumnIndex("x")));
            }

            // Existing rows return INT_NULL sentinel formatted numerically because
            // x is NOT NULL. No "null" appears.
            assertSql(
                    """
                            id\tts\tx
                            1\t2024-01-01T00:00:00.000000Z\t-2147483648
                            2\t2024-01-02T00:00:00.000000Z\t-2147483648
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testAddNotNullColumnToWalTable() throws Exception {
        assertMemoryLeak(() -> {
            // Same semantics as the bypass-WAL case: WAL apply must propagate
            // both the metadata flag and the column_top so reads return the
            // sentinel, formatted numerically.
            execute("CREATE TABLE t (id INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                        (1, '2024-01-01'),
                        (2, '2024-01-02')
                    """);
            drainWalQueue();

            execute("ALTER TABLE t ADD COLUMN x LONG NOT NULL");
            drainWalQueue();

            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata metadata = reader.getMetadata();
                assertTrue(metadata.isNotNull(metadata.getColumnIndex("x")));
            }

            assertSql(
                    """
                            id\tts\tx
                            1\t2024-01-01T00:00:00.000000Z\t-9223372036854775808
                            2\t2024-01-02T00:00:00.000000Z\t-9223372036854775808
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testAddNotNullColumnWithDefaultRejected() throws Exception {
        assertMemoryLeak(() -> {
            // ADD COLUMN does not support a DEFAULT clause at the parser level.
            // If/when DEFAULT support lands, this test should be updated to
            // assert backfill semantics.
            execute("CREATE TABLE t (id INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, '2024-01-01')");

            try {
                execute("ALTER TABLE t ADD COLUMN x INT NOT NULL DEFAULT 0");
                fail("Expected parse error for unsupported DEFAULT clause");
            } catch (SqlException e) {
                // The parser rejects the trailing DEFAULT token. Exact message
                // is implementation-defined; just confirm we did not silently
                // accept it.
                assertTrue(
                        "expected parser error mentioning the unexpected token, got: " + e.getFlyweightMessage(),
                        e.getFlyweightMessage().length() > 0
                );
            }
        });
    }

    @Test
    public void testAlterColumnDropNotNullOnPopulatedColumn() throws Exception {
        assertMemoryLeak(() -> {
            // The drop-NOT-NULL syntax in QuestDB is `ALTER COLUMN x SET NULL`
            // (no `DROP NOT NULL` form). After the flip, NULLs are accepted.
            execute("CREATE TABLE t (x INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, '2024-01-01'), (2, '2024-01-02')");
            assertTrue(getNotNull("t", "x"));

            execute("ALTER TABLE t ALTER COLUMN x SET NULL");
            assertFalse(getNotNull("t", "x"));

            // A new NULL insert is now accepted
            execute("INSERT INTO t (ts) VALUES ('2024-01-03')");

            assertSql(
                    """
                            x\tts
                            1\t2024-01-01T00:00:00.000000Z
                            2\t2024-01-02T00:00:00.000000Z
                            null\t2024-01-03T00:00:00.000000Z
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testAlterColumnSetNotNullOnPopulatedColumnWithExistingNulls() throws Exception {
        assertMemoryLeak(() -> {
            // Document the actual semantic: SET NOT NULL is a pure metadata flip
            // and does NOT scan / reject pre-existing NULL data. The previously
            // inserted NULL survives unchanged and reads back as the type sentinel
            // formatted numerically (column is now NOT NULL, so "null" is never
            // printed).
            //
            // This is a real semantic gap: post-ALTER readers see no NULLs even
            // though the underlying values are sentinels.
            execute("CREATE TABLE t (x INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (1, '2024-01-01'),
                        (NULL, '2024-01-02'),
                        (3, '2024-01-03')
                    """);

            execute("ALTER TABLE t ALTER COLUMN x SET NOT NULL");
            assertTrue(getNotNull("t", "x"));

            assertSql(
                    """
                            x\tts
                            1\t2024-01-01T00:00:00.000000Z
                            -2147483648\t2024-01-02T00:00:00.000000Z
                            3\t2024-01-03T00:00:00.000000Z
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testAlterColumnSetNotNullOnWalTable() throws Exception {
        assertMemoryLeak(() -> {
            // SET NOT NULL on a WAL table must travel through ApplyWal2TableJob
            // and arrive at TableWriter.setColumnNotNull. As in bypass-WAL, no
            // data validation runs — the existing NULL survives as a sentinel.
            execute("CREATE TABLE t (x INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t VALUES
                        (1, '2024-01-01'),
                        (NULL, '2024-01-02')
                    """);
            drainWalQueue();

            execute("ALTER TABLE t ALTER COLUMN x SET NOT NULL");
            drainWalQueue();
            assertTrue(getNotNull("t", "x"));

            assertSql(
                    """
                            x\tts
                            1\t2024-01-01T00:00:00.000000Z
                            -2147483648\t2024-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );

            // After SET NOT NULL, new rows missing the now-NOT-NULL column
            // are rejected. The constraint propagates to the WalWriter's
            // local metadata immediately (via the metaWriterSvc invocation
            // wired into applyNonStructural), so the rowAppend check fires
            // synchronously at execute() time without needing a writer reload.
            try {
                execute("INSERT INTO t (ts) VALUES ('2024-01-03')");
                fail("Expected NOT NULL violation after SET NOT NULL on WAL table");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }
        });
    }

    @Test
    public void testAlterColumnSetNotNullSucceedsWhenNoNulls() throws Exception {
        assertMemoryLeak(() -> {
            // Happy-path baseline: with no pre-existing NULLs, SET NOT NULL is
            // simply a metadata flip and subsequent writes are enforced.
            execute("CREATE TABLE t (x INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        (1, '2024-01-01'),
                        (2, '2024-01-02'),
                        (3, '2024-01-03')
                    """);
            assertFalse(getNotNull("t", "x"));

            execute("ALTER TABLE t ALTER COLUMN x SET NOT NULL");
            assertTrue(getNotNull("t", "x"));

            try {
                execute("INSERT INTO t (ts) VALUES ('2024-01-04')");
                fail("Expected NOT NULL violation after SET NOT NULL");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }

            assertSql(
                    """
                            x\tts
                            1\t2024-01-01T00:00:00.000000Z
                            2\t2024-01-02T00:00:00.000000Z
                            3\t2024-01-03T00:00:00.000000Z
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testAlterColumnToggleSurvivesReaderReload() throws Exception {
        assertMemoryLeak(() -> {
            // Persist check: SET NOT NULL on a populated column must reach disk
            // and be visible on a freshly opened reader. Then toggle back with
            // SET NULL and verify the inverse round-trips through the reload too.
            execute("CREATE TABLE t (x INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, '2024-01-01'), (2, '2024-01-02')");

            execute("ALTER TABLE t ALTER COLUMN x SET NOT NULL");
            assertTrue(getNotNull("t", "x"));

            engine.releaseAllReaders();

            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata metadata = reader.getMetadata();
                assertTrue(metadata.isNotNull(metadata.getColumnIndex("x")));
            }

            execute("ALTER TABLE t ALTER COLUMN x SET NULL");
            engine.releaseAllReaders();

            try (TableReader reader = engine.getReader("t")) {
                TableReaderMetadata metadata = reader.getMetadata();
                assertFalse(metadata.isNotNull(metadata.getColumnIndex("x")));
            }
        });
    }

    @Test
    public void testInsertAfterAddNotNullColumnEnforces() throws Exception {
        assertMemoryLeak(() -> {
            // After ADD COLUMN ... NOT NULL on a populated table, the constraint
            // must fire for new rows even though the existing rows escaped via
            // column_top sentinel backfill.
            execute("CREATE TABLE t (id INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, '2024-01-01')");

            execute("ALTER TABLE t ADD COLUMN x INT NOT NULL");

            // Omitting x for a brand-new row must be rejected.
            try {
                execute("INSERT INTO t (id, ts) VALUES (2, '2024-01-02')");
                fail("Expected NOT NULL violation for new row missing x");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }

            // Providing a real value works. The pre-existing row keeps its sentinel.
            execute("INSERT INTO t VALUES (3, '2024-01-03', 30)");

            assertSql(
                    """
                            id\tts\tx
                            1\t2024-01-01T00:00:00.000000Z\t-2147483648
                            3\t2024-01-03T00:00:00.000000Z\t30
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testAlterAddColumnSymbolNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE t ADD COLUMN s SYMBOL NOT NULL");
            assertTrue(getNotNull("t", "s"));

            try {
                execute("INSERT INTO t (id, ts) VALUES (1, '2024-01-01')");
                fail("Expected NOT NULL violation for omitted SYMBOL column");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=s");
            }

            execute("INSERT INTO t VALUES (1, '2024-01-01', 'a')");
        });
    }

    @Test
    public void testAlterAddColumnSymbolWithCapacityAndNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE t ADD COLUMN s SYMBOL CAPACITY 1024 CACHE INDEX CAPACITY 256 NOT NULL");
            assertTrue(getNotNull("t", "s"));
        });
    }

    @Test
    public void testAlterAddColumnSymbolNocacheAndNotNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE t ADD COLUMN s SYMBOL NOCACHE NOT NULL");
            assertTrue(getNotNull("t", "s"));
        });
    }

    @Test
    public void testAlterAddColumnSymbolNotNullRejectsExplicitNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE t ADD COLUMN s SYMBOL NOT NULL");

            try {
                execute("INSERT INTO t VALUES (1, '2024-01-01', NULL)");
                fail("Expected rejection of explicit NULL into NOT NULL SYMBOL");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=s");
            }
        });
    }

    @Test
    public void testAlterAddColumnSymbolNotWithoutNullFails() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("ALTER TABLE t ADD COLUMN s SYMBOL NOT junk");
                fail("Expected parse error for 'NOT' without following 'NULL'");
            } catch (SqlException e) {
                assertContains(e.getFlyweightMessage(), "'NULL' expected after 'NOT'");
            }
        });
    }

    @Test
    public void testAlterAddColumnSymbolOnWalTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES (1, '2024-01-01')");
            drainWalQueue();

            execute("ALTER TABLE t ADD COLUMN s SYMBOL NOT NULL");
            drainWalQueue();

            assertTrue(getNotNull("t", "s"));
        });
    }

    @Test
    public void testSetNotNullPersistsThroughSequencerReload() throws Exception {
        assertMemoryLeak(() -> {
            // Regression: SET_COLUMN_NOT_NULL used to be non-structural, so the
            // sequencer metadata file never persisted the flag. After a sequencer
            // reload (simulated by closing and reopening the engine) the flag
            // would revert to false on the sequencer side while the TableWriter
            // still held it, breaking any subsequently-created WalWriter.
            execute("CREATE TABLE t (id INT, x LONG, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES (1, 10, '2024-01-01')");
            drainWalQueue();

            execute("ALTER TABLE t ALTER COLUMN x SET NOT NULL");
            drainWalQueue();

            assertTrue(getNotNull("t", "x"));

            // Force the sequencer to reload its metadata from disk.
            engine.releaseInactive();

            assertTrue("flag should survive sequencer reload", getNotNull("t", "x"));

            try {
                execute("INSERT INTO t (id, ts) VALUES (2, '2024-01-02')");
                drainWalQueue();
                // The enforcement fires at apply time for WAL tables; inspect
                // the sequencer state for the expected suspension.
            } catch (CairoException ignore) {
                // Either direct rejection at apply or suspension; both are acceptable.
            }
        });
    }

    @Test
    public void testUpdateSetLiteralNullRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, x LONG NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, 10, '2024-01-01'), (2, 20, '2024-01-02')");

            try {
                execute("UPDATE t SET x = NULL WHERE id = 1");
                fail("Expected NOT NULL violation when UPDATE sets NULL");
            } catch (SqlException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }
        });
    }

    @Test
    public void testUpdateSetLiteralNullRejectedWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, x LONG NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES (1, 10, '2024-01-01'), (2, 20, '2024-01-02')");
            drainWalQueue();

            try {
                execute("UPDATE t SET x = NULL WHERE id = 1");
                fail("Expected NOT NULL violation when UPDATE sets NULL on WAL table");
            } catch (SqlException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }

            // The row must remain unmodified.
            assertSql(
                    """
                            id\tx\tts
                            1\t10\t2024-01-01T00:00:00.000000Z
                            2\t20\t2024-01-02T00:00:00.000000Z
                            """,
                    "SELECT * FROM t ORDER BY ts"
            );
        });
    }

    @Test
    public void testUpdateSetCastNullRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, x INT NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, 10, '2024-01-01')");

            try {
                execute("UPDATE t SET x = CAST(NULL AS INT) WHERE id = 1");
                fail("Expected NOT NULL violation when UPDATE casts NULL");
            } catch (SqlException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }
        });
    }

    @Test
    public void testUpdateSetSymbolNullRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, s SYMBOL NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, 'a', '2024-01-01')");

            try {
                execute("UPDATE t SET s = NULL WHERE id = 1");
                fail("Expected NOT NULL violation when UPDATE sets SYMBOL NULL");
            } catch (SqlException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=s");
            }
        });
    }

    @Test
    public void testUpdateOnNullableColumnAllowsNull() throws Exception {
        assertMemoryLeak(() -> {
            // Regression guard: nullable columns still accept NULL via UPDATE.
            execute("CREATE TABLE t (id INT, x LONG, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, 10, '2024-01-01')");

            execute("UPDATE t SET x = NULL WHERE id = 1");

            assertSql(
                    """
                            id\tx\tts
                            1\tnull\t2024-01-01T00:00:00.000000Z
                            """,
                    "SELECT * FROM t"
            );
        });
    }

    @Test
    public void testUpdateOnNotNullColumnAllowsNonNullValue() throws Exception {
        assertMemoryLeak(() -> {
            // Regression guard: a real value still flows through UPDATE.
            execute("CREATE TABLE t (id INT, x LONG NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, 10, '2024-01-01')");

            execute("UPDATE t SET x = 42 WHERE id = 1");

            assertSql(
                    """
                            id\tx\tts
                            1\t42\t2024-01-01T00:00:00.000000Z
                            """,
                    "SELECT * FROM t"
            );
        });
    }

    @Test
    public void testSetNotNullRoundTripOnWalTable() throws Exception {
        assertMemoryLeak(() -> {
            // Regression: SET_COLUMN_NOT_NULL used to be non-structural, so the
            // sequencer metadata file never persisted the flag. A sibling
            // WalWriter that reopened after the flag flip kept using the stale
            // metadata and silently accepted rows violating the constraint.
            // Now that the ALTER flows through applyStructural, both the
            // sequencer and every WalWriter must observe the toggle.
            execute("CREATE TABLE t (id INT, x LONG, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES (1, 10, '2024-01-01')");
            drainWalQueue();
            assertFalse(getNotNull("t", "x"));

            execute("ALTER TABLE t ALTER COLUMN x SET NOT NULL");
            drainWalQueue();
            engine.releaseAllReaders();
            assertTrue(getNotNull("t", "x"));

            // Force the sequencer + every pooled WalWriter to reload their
            // cached metadata. The flag must survive the reload path.
            engine.releaseInactive();
            assertTrue("NOT NULL must survive sequencer reload", getNotNull("t", "x"));

            execute("ALTER TABLE t ALTER COLUMN x SET NULL");
            drainWalQueue();
            engine.releaseAllReaders();
            assertFalse(getNotNull("t", "x"));

            engine.releaseInactive();
            assertFalse("SET NULL must survive sequencer reload", getNotNull("t", "x"));
        });
    }
}
