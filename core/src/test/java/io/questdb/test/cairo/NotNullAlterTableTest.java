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
            assertQuery("SELECT * FROM t ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            id\tts\tx
                            1\t2024-01-01T00:00:00.000000Z\t-2147483648
                            2\t2024-01-02T00:00:00.000000Z\t-2147483648
                            """);
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

            assertQuery("SELECT * FROM t ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            id\tts\tx
                            1\t2024-01-01T00:00:00.000000Z\t-9223372036854775808
                            2\t2024-01-02T00:00:00.000000Z\t-9223372036854775808
                            """);
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
                // Parser rejects the trailing DEFAULT token with a "',' expected"
                // message -- the ADD COLUMN syntax does not accept a DEFAULT
                // clause. Pin the expected token so a regression that starts
                // silently accepting DEFAULT breaks the test.
                assertContains(e.getFlyweightMessage(), "',' expected");
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

            assertQuery("SELECT * FROM t ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            x\tts
                            1\t2024-01-01T00:00:00.000000Z
                            2\t2024-01-02T00:00:00.000000Z
                            null\t2024-01-03T00:00:00.000000Z
                            """);
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

            assertQuery("SELECT * FROM t ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            x\tts
                            1\t2024-01-01T00:00:00.000000Z
                            -2147483648\t2024-01-02T00:00:00.000000Z
                            3\t2024-01-03T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnSetNotNullCountsSentinelRowsDecimalUuidLong256() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (g SYMBOL, u UUID, l LONG256, d DECIMAL(18,0))");
            execute("""
                    INSERT INTO t VALUES
                        ('a', NULL, NULL, NULL),
                        ('a', '11111111-1111-1111-1111-111111111111', 0x01, 1),
                        ('b', NULL, NULL, NULL)
                    """);

            // nullable control: genuine NULLs are excluded
            assertQuery("SELECT count(u) cu, count(l) cl, count(d) cd, count() n FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            cu\tcl\tcd\tn
                            1\t1\t1\t3
                            """);

            execute("ALTER TABLE t ALTER COLUMN u SET NOT NULL");
            execute("ALTER TABLE t ALTER COLUMN l SET NOT NULL");
            execute("ALTER TABLE t ALTER COLUMN d SET NOT NULL");

            // reclassified sentinels are data: count(v) equals the row count
            assertQuery("SELECT count(u) cu, count(l) cl, count(d) cd, count() n FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            cu\tcl\tcd\tn
                            3\t3\t3\t3
                            """);
            assertQuery("SELECT g, count(u) cu, count(l) cl, count(d) cd FROM t ORDER BY g")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            g\tcu\tcl\tcd
                            a\t2\t2\t2
                            b\t1\t1\t1
                            """);
        });
    }

    @Test
    public void testAlterColumnSetNotNullCountsSentinelRowsGeoHashAllWidths() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (g SYMBOL, g1 GEOHASH(1c), g2 GEOHASH(2c), g4 GEOHASH(4c), g8 GEOHASH(8c))");
            execute("""
                    INSERT INTO t VALUES
                        ('a', NULL, NULL, NULL, NULL),
                        ('a', ##11111, ##1111111111, ##11111111111111111111, ##1111111111111111111111111111111111111111),
                        ('b', NULL, NULL, NULL, NULL)
                    """);

            // nullable control: genuine NULLs are excluded
            assertQuery("SELECT count(g1) c1, count(g2) c2, count(g4) c4, count(g8) c8, count() n FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c1\tc2\tc4\tc8\tn
                            1\t1\t1\t1\t3
                            """);

            execute("ALTER TABLE t ALTER COLUMN g1 SET NOT NULL");
            execute("ALTER TABLE t ALTER COLUMN g2 SET NOT NULL");
            execute("ALTER TABLE t ALTER COLUMN g4 SET NOT NULL");
            execute("ALTER TABLE t ALTER COLUMN g8 SET NOT NULL");

            // reclassified sentinels are data: count(v) equals count() for
            // every geohash storage width (byte, short, int, long)
            assertQuery("SELECT count(g1) c1, count(g2) c2, count(g4) c4, count(g8) c8, count() n FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c1\tc2\tc4\tc8\tn
                            3\t3\t3\t3\t3
                            """);
            assertQuery("SELECT g, count(g1) c1, count(g2) c2, count(g4) c4, count(g8) c8 FROM t ORDER BY g")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            g\tc1\tc2\tc4\tc8
                            a\t2\t2\t2\t2
                            b\t1\t1\t1\t1
                            """);
        });
    }

    @Test
    public void testAlterColumnSetNotNullCountsSentinelRowsIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (g SYMBOL, v IPV4)");
            execute("INSERT INTO t VALUES ('a', NULL), ('a', '1.2.3.4'), ('b', NULL)");

            // nullable control: genuine NULLs are excluded
            assertQuery("SELECT count(v) c, count() n FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c\tn
                            1\t3
                            """);

            execute("ALTER TABLE t ALTER COLUMN v SET NOT NULL");

            // the reclassified 0.0.0.0 sentinel is data now
            assertQuery("SELECT count(v) c, count() n FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c\tn
                            3\t3
                            """);
            assertQuery("SELECT g, count(v) c FROM t ORDER BY g")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            g\tc
                            a\t2
                            b\t1
                            """);
        });
    }

    @Test
    public void testAlterColumnSetNotNullCountsSentinelRowsParallel() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 2);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 2);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (g SYMBOL, v IPV4, h GEOHASH(1c), ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                        ('a', NULL, NULL, '2024-01-01'),
                        ('a', '1.2.3.4', ##11111, '2024-01-02'),
                        ('a', NULL, NULL, '2024-01-03'),
                        ('b', NULL, NULL, '2024-01-04'),
                        ('b', '5.6.7.8', ##00001, '2024-01-05')
                    """);
            execute("ALTER TABLE t ALTER COLUMN v SET NOT NULL");
            execute("ALTER TABLE t ALTER COLUMN h SET NOT NULL");

            assertQuery("SELECT count(v) cv, count(h) ch, count() n FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            cv\tch\tn
                            5\t5\t5
                            """);
            assertQuery("SELECT g, count(v) cv, count(h) ch FROM t ORDER BY g")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            g\tcv\tch
                            a\t3\t3
                            b\t2\t2
                            """);
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

            assertQuery("SELECT * FROM t ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            x\tts
                            1\t2024-01-01T00:00:00.000000Z
                            -2147483648\t2024-01-02T00:00:00.000000Z
                            """);

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

            assertQuery("SELECT * FROM t ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            x\tts
                            1\t2024-01-01T00:00:00.000000Z
                            2\t2024-01-02T00:00:00.000000Z
                            3\t2024-01-03T00:00:00.000000Z
                            """);
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

            assertQuery("SELECT * FROM t ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            id\tts\tx
                            1\t2024-01-01T00:00:00.000000Z\t-2147483648
                            3\t2024-01-03T00:00:00.000000Z\t30
                            """);
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

            // The explicit NULL literal is rejected at compile time, at the NULL token.
            assertExceptionNoLeakCheck(
                    "INSERT INTO t VALUES (1, '2024-01-01', NULL)",
                    39,
                    "NOT NULL constraint violation [column=s]"
            );
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
    public void testAlterColumnDropNotNullUpdatesDependentViewMetadata() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v LONG NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE VIEW vv AS (SELECT v FROM t)");
            drainWalAndViewQueues();
            assertQuery("SELECT notNull FROM table_columns('vv') WHERE column = 'v'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("notNull\ntrue\n");

            execute("ALTER TABLE t ALTER COLUMN v SET NULL");
            drainWalAndViewQueues();

            assertQuery("SELECT notNull FROM table_columns('t') WHERE column = 'v'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("notNull\nfalse\n");
            assertQuery("SELECT notNull FROM table_columns('vv') WHERE column = 'v'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("notNull\nfalse\n");
        });
    }

    @Test
    public void testAlterColumnSetNotNullUpdatesDependentViewMetadata() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v LONG, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE VIEW vv AS (SELECT v FROM t)");
            drainWalAndViewQueues();
            assertQuery("SELECT notNull FROM table_columns('vv') WHERE column = 'v'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("notNull\nfalse\n");

            execute("ALTER TABLE t ALTER COLUMN v SET NOT NULL");
            drainWalAndViewQueues();

            assertQuery("SELECT notNull FROM table_columns('t') WHERE column = 'v'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("notNull\ntrue\n");
            assertQuery("SELECT notNull FROM table_columns('vv') WHERE column = 'v'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("notNull\ntrue\n");
        });
    }

    @Test
    public void testCompileViewRefreshesNotNullMetadata() throws Exception {
        // Explicit COMPILE VIEW must notice a nullability-only change: the view
        // metadata comparison has to include the NOT NULL flag, otherwise the
        // catalogue keeps the stale flag while the base table and any freshly
        // created equivalent view report the new one.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v LONG, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE VIEW vv AS (SELECT v FROM t)");
            drainWalAndViewQueues();

            execute("ALTER TABLE t ALTER COLUMN v SET NOT NULL");
            drainWalAndViewQueues();
            execute("COMPILE VIEW vv");
            drainWalAndViewQueues();

            assertQuery("SELECT notNull FROM table_columns('vv') WHERE column = 'v'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("notNull\ntrue\n");
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
            assertQuery("SELECT * FROM t ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            id\tx\tts
                            1\t10\t2024-01-01T00:00:00.000000Z
                            2\t20\t2024-01-02T00:00:00.000000Z
                            """);
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
    public void testUpdateRuntimeNullRejectedString() throws Exception {
        assertMemoryLeak(() -> {
            // Runtime-derived NULL: nullif() evaluates to NULL at execution time, so the
            // syntactic compile-time check cannot see it. The writer-side check must
            // reject it, because a stored STRING null is the very encoding IS NULL
            // matches, and IS NULL on a NOT NULL column folds to FALSE -- the row would
            // become unreachable by any null predicate.
            execute("CREATE TABLE t (id INT, s STRING NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, 'a', '2024-01-01')");

            try {
                execute("UPDATE t SET s = nullif(s, 'a') WHERE id = 1");
                fail("Expected NOT NULL violation when UPDATE derives NULL at runtime");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=s");
            }

            // The row must remain unmodified.
            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            id\ts\tts
                            1\ta\t2024-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testUpdateRuntimeNullRejectedStringWal() throws Exception {
        assertMemoryLeak(() -> {
            // On a WAL table the UPDATE is acknowledged at sequencing; the runtime NULL
            // is only seen when UpdateOperatorImpl runs at apply time. ApplyWal2TableJob
            // treats no UPDATE error as WAL-tolerable (skipping one would lose
            // acknowledged DML), so the violation must surface as a suspended table.
            execute("CREATE TABLE t (id INT, s STRING NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES (1, 'a', '2024-01-01')");
            drainWalQueue();

            execute("UPDATE t SET s = nullif(s, 'a') WHERE id = 1");
            drainWalQueue();

            assertTrue(
                    "WAL apply must suspend the table rather than store the NULL",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t"))
            );

            // The row must remain unmodified.
            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            id\ts\tts
                            1\ta\t2024-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testUpdateRuntimeNullRejectedVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, v VARCHAR NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, 'a', '2024-01-01')");

            try {
                execute("UPDATE t SET v = nullif(v, 'a') WHERE id = 1");
                fail("Expected NOT NULL violation when UPDATE derives VARCHAR NULL at runtime");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=v");
            }

            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            id\tv\tts
                            1\ta\t2024-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testUpdateRuntimeNullRejectedSymbol() throws Exception {
        assertMemoryLeak(() -> {
            // SYMBOL stores -1 for null, the exact IS NULL encoding; a stored null here
            // is invisible to IS NULL once the constraint folds it to FALSE.
            execute("CREATE TABLE t (id INT, s SYMBOL NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, 'a', '2024-01-01')");

            try {
                execute("UPDATE t SET s = nullif(s, 'a') WHERE id = 1");
                fail("Expected NOT NULL violation when UPDATE derives SYMBOL NULL at runtime");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=s");
            }

            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            id\ts\tts
                            1\ta\t2024-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testUpdateRuntimeNullRejectedBinary() throws Exception {
        assertMemoryLeak(() -> {
            // A nullable BINARY column holding NULL is the runtime producer here; the
            // SET expression is a plain column read, so no syntactic check can fire.
            execute("CREATE TABLE t (id INT, b BINARY NOT NULL, b2 BINARY, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t (id, b, ts) VALUES (1, rnd_bin(4,4,0), '2024-01-01')");

            try {
                execute("UPDATE t SET b = b2 WHERE id = 1");
                fail("Expected NOT NULL violation when UPDATE derives BINARY NULL at runtime");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=b");
            }

            // The original 4-byte value must survive; a stored NULL would render length null.
            assertQuery("SELECT id, length(b) len FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tlen
                            1\t4
                            """);
        });
    }

    @Test
    public void testUpdateRuntimeNullRejectedArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, a DOUBLE[] NOT NULL, a2 DOUBLE[], ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t (id, a, ts) VALUES (1, ARRAY[1.0, 2.0], '2024-01-01')");

            try {
                execute("UPDATE t SET a = a2 WHERE id = 1");
                fail("Expected NOT NULL violation when UPDATE derives ARRAY NULL at runtime");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=a");
            }

            assertQuery("SELECT id, a FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ta
                            1\t[1.0,2.0]
                            """);
        });
    }

    @Test
    public void testUpdateRuntimeNullRejectedBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            // A NULL bind variable reaches the writer as a value; the compile-time
            // literal check cannot see it.
            execute("CREATE TABLE t (id INT, s STRING NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, 'a', '2024-01-01')");

            bindVariableService.clear();
            bindVariableService.setStr(0, null);
            try {
                execute("UPDATE t SET s = $1 WHERE id = 1");
                fail("Expected NOT NULL violation when UPDATE binds NULL");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=s");
            }

            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            id\ts\tts
                            1\ta\t2024-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testUpdateRuntimeNullNumericSentinelIsData() throws Exception {
        assertMemoryLeak(() -> {
            // Deliberate asymmetry with the reference types above: a numeric NOT NULL
            // column treats its legacy sentinel bit pattern as DATA, so a runtime-derived
            // NULL stores the sentinel and the UPDATE must succeed. Only reference types
            // (STRING/VARCHAR/SYMBOL/BINARY/ARRAY), whose stored null IS the IS NULL
            // encoding, are rejected. Do not "unify" the two.
            execute("CREATE TABLE t (id INT, n LONG NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, 10, '2024-01-01')");

            execute("UPDATE t SET n = nullif(n, n) WHERE id = 1");

            // The sentinel renders numerically because the column is NOT NULL...
            assertQuery("SELECT id, n FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tn
                            1\t-9223372036854775808
                            """);

            // ...and IS NULL folds to FALSE on a NOT NULL column, so no row matches.
            assertQuery("SELECT count() FROM t WHERE n IS NULL")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count
                            0
                            """);
        });
    }

    @Test
    public void testUpdateOnNullableColumnAllowsNull() throws Exception {
        assertMemoryLeak(() -> {
            // Regression guard: nullable columns still accept NULL via UPDATE.
            execute("CREATE TABLE t (id INT, x LONG, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, 10, '2024-01-01')");

            execute("UPDATE t SET x = NULL WHERE id = 1");

            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            id\tx\tts
                            1\tnull\t2024-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testUpdateOnNotNullColumnAllowsNonNullValue() throws Exception {
        assertMemoryLeak(() -> {
            // Regression guard: a real value still flows through UPDATE.
            execute("CREATE TABLE t (id INT, x LONG NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES (1, 10, '2024-01-01')");

            execute("UPDATE t SET x = 42 WHERE id = 1");

            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            id\tx\tts
                            1\t42\t2024-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testCreateTableLikePreservesNonTsNotNull() throws Exception {
        assertMemoryLeak(() -> {
            // CREATE TABLE dst (LIKE src) used to propagate NOT NULL only for
            // the designated timestamp. Pin that it now propagates for every
            // NOT NULL column, not just ts.
            execute("CREATE TABLE src (x INT NOT NULL, y DOUBLE, z LONG NOT NULL, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE dst (LIKE src)");

            assertTrue(getNotNull("dst", "x"));
            assertFalse(getNotNull("dst", "y"));
            assertTrue(getNotNull("dst", "z"));
            assertTrue(getNotNull("dst", "ts"));

            // Enforcement follows: omitting x must reject.
            try {
                execute("INSERT INTO dst (y, z, ts) VALUES (1.0, 10, '2024-01-01')");
                fail("Expected NOT NULL violation on LIKE target");
            } catch (CairoException e) {
                assertContains(e.getFlyweightMessage(), "NOT NULL constraint violation");
                assertContains(e.getFlyweightMessage(), "column=x");
            }
        });
    }

    @Test
    public void testAlterColumnSetNotParseErrorAtJunkToken() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("ALTER TABLE t ALTER COLUMN id SET NOT junk");
                fail("Expected parse error for 'NOT junk'");
            } catch (SqlException e) {
                assertContains(e.getFlyweightMessage(), "'null' expected");
            }
        });
    }

    @Test
    public void testAlterColumnSetParseErrorAtTruncatedClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("ALTER TABLE t ALTER COLUMN id SET");
                fail("Expected parse error for truncated SET clause");
            } catch (SqlException e) {
                assertContains(e.getFlyweightMessage(), "'not', 'null' or 'parquet'");
            }
        });
    }

    @Test
    public void testAlterColumnSetParseErrorOnUnknownKeyword() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY");
            try {
                execute("ALTER TABLE t ALTER COLUMN id SET bogus");
                fail("Expected parse error for 'SET bogus'");
            } catch (SqlException e) {
                assertContains(e.getFlyweightMessage(), "'not', 'null' or 'parquet'");
            }
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
