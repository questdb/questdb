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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.mv.MatViewBareNameGuard;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewRefreshTask;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateStoreImpl;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.seq.TableMetadataChange;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base DROP, RENAME and ALTER COLUMN TYPE invalidate no materialized view when they apply. The view's next refresh
 * that recomputes rows compiles its SQL, which fails, and so invalidates the view with the compile error, only when
 * the SQL reads the changed column. The bare-name guard is the exception: a column change under the name of a
 * zero-argument function that a view's own SQL text uses as a bare select-list name invalidates that view when it
 * applies, since the name would switch between a column and a function call and the SQL would still compile.
 * <p>
 * Expected rows come from the base rows each test inserts: a bucket of the 1h views holds the last value of its
 * symbol's base rows within the hour.
 */
public class MatViewColumnChangeTest extends AbstractCairoTest {
    private static final String AMOUNT_VIEW_ROWS = """
            ts\tsym\tamount
            2024-01-01T00:00:00.000000Z\ta\t30.0
            2024-01-01T00:00:00.000000Z\tb\t20.0
            2024-01-01T01:00:00.000000Z\ta\t40.0
            """;
    private static final String AMOUNT_VIEW_SQL = "SELECT ts, sym, last(amount) AS amount FROM base SAMPLE BY 1h";
    private static final String BARE_VERSION_VIEW_ROWS = """
            ts\tsym\tversion\tprice
            2024-01-01T00:00:00.000000Z\ta\t1\t1.5
            2024-01-01T00:00:00.000000Z\ta\t2\t3.5
            2024-01-01T00:00:00.000000Z\tb\t1\t2.5
            2024-01-01T01:00:00.000000Z\ta\t2\t4.5
            """;
    private static final String BARE_VERSION_VIEW_SQL = "SELECT ts, sym, version, last(price) AS price FROM base SAMPLE BY 1h";
    // COUNT_CALL_SQL and COUNT_VIEW_SQL over base2, see createViewToMoveToBase2()
    private static final String BASE2_COUNT_CALL_SQL = "SELECT ts, sym, count() AS count, last(price) AS price FROM base2 SAMPLE BY 1h";
    private static final String BASE2_COUNT_VIEW_SQL = "SELECT ts, sym, count, last(price) AS price FROM base2 SAMPLE BY 1h";
    // the columns of the base of createCountBase() once it dropped count
    private static final String BASE_COLUMNS_WITHOUT_COUNT = """
            column\ttype
            sym\tSYMBOL
            price\tDOUBLE
            ts\tTIMESTAMP
            """;
    // the rows of COUNT_VIEW_SQL over the base of createCountBase() that call count(): one base row per bucket and sym
    private static final String COUNT_CALL_ROWS = """
            ts\tsym\tcount\tprice
            2024-01-01T00:00:00.000000Z\ta\t1\t1.0
            2024-01-01T01:00:00.000000Z\tb\t1\t2.0
            """;
    // COUNT_VIEW_SQL with count written as a call, the one reading of a view whose rows call count()
    private static final String COUNT_CALL_SQL = "SELECT ts, sym, count() AS count, last(price) AS price FROM base SAMPLE BY 1h";
    // the rows of COUNT_VIEW_SQL over the base of createCountBase() that read count as the column, a grouping key
    private static final String COUNT_COLUMN_ROWS = """
            ts\tsym\tcount\tprice
            2024-01-01T00:00:00.000000Z\ta\t7\t1.0
            2024-01-01T01:00:00.000000Z\tb\t8\t2.0
            """;
    // the refusal of RESUME for the view mv, whose SQL reads count bare, once the base has no column count
    private static final String COUNT_COMPILE_REFUSAL = "materialized view SQL does not compile, fix it with ALTER MATERIALIZED VIEW ... AS first "
            + "[view=mv, error=count is not a column; write count() to call the function]";
    // the refusal of a REFRESH of the view mv that the bare-name guard invalidated for count
    private static final String COUNT_INVALID_REFUSAL = "materialized view is invalid, run REFRESH MATERIALIZED VIEW mv RESUME first [reason="
            + MatViewBareNameGuard.invalidationReason("count") + ']';
    private static final String COUNT_REASON = MatViewBareNameGuard.invalidationReason("count");
    // the refusal of RESUME for the view mv, whose SQL reads count bare, once a switch of count stands
    private static final String COUNT_SWITCH_REFUSAL = "materialized view SQL reads a bare name whose meaning a base column change switched, fix it with "
            + "ALTER MATERIALIZED VIEW ... AS first [view=mv, error=" + MatViewBareNameGuard.invalidationReason("count") + ']';
    // reads count bare: the base column while the base has one, a call of count() otherwise
    private static final String COUNT_VIEW_SQL = "SELECT ts, sym, count, last(price) AS price FROM base SAMPLE BY 1h";
    private static final String CREATE_COUNT_BASE = "CREATE TABLE base (sym SYMBOL, price DOUBLE, count LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL";
    private static final String DROP_COUNT = "ALTER TABLE base DROP COLUMN count";
    private static final String FILTER_VIEW_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t3.5
            2024-01-01T01:00:00.000000Z\ta\t4.5
            """;
    private static final String FILTER_VIEW_SQL = "SELECT ts, sym, last(price) AS price FROM base WHERE amount > 25 SAMPLE BY 1h";
    private static final String INSERT_BASE_ROWS = """
            INSERT INTO base VALUES
            ('a', 1.5, 10.0, 1, '2024-01-01T00:10:00.000000Z'),
            ('b', 2.5, 20.0, 1, '2024-01-01T00:20:00.000000Z'),
            ('a', 3.5, 30.0, 2, '2024-01-01T00:40:00.000000Z'),
            ('a', 4.5, 40.0, 2, '2024-01-01T01:10:00.000000Z')
            """;
    // a row of the hour 02:00 for the base of createCountBase() once the base has no column count
    private static final String INSERT_C_ROW = "INSERT INTO base (sym, price, ts) VALUES ('c', 3.0, '2024-01-01T02:10:00.000000Z')";
    private static final String PRICE_VIEW_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t3.5
            2024-01-01T00:00:00.000000Z\tb\t2.5
            2024-01-01T01:00:00.000000Z\ta\t4.5
            """;
    private static final String PRICE_VIEW_ROWS_AFTER_INSERT = PRICE_VIEW_ROWS + "2024-01-01T02:00:00.000000Z\tb\t5.5\n";
    private static final String PRICE_VIEW_SQL = "SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h";
    private static final String REGULAR_VIEW_SQL = "SELECT ts, sym, last(amount) AS amount FROM rv SAMPLE BY 1h";
    // the rows of RENAMED_VERSION_VIEW_SQL over the base rows once an UPDATE set the price of a to 9.0
    private static final String RENAMED_VERSION_UPDATED_ROWS = """
            ts\tsym\tversion\tprice
            2024-01-01T00:00:00.000000Z\ta\t1\t9.0
            2024-01-01T00:00:00.000000Z\ta\t2\t9.0
            2024-01-01T00:00:00.000000Z\tb\t1\t2.5
            2024-01-01T01:00:00.000000Z\ta\t2\t9.0
            """;
    // BARE_VERSION_VIEW_SQL fixed for a base that renamed version to ver
    private static final String RENAMED_VERSION_VIEW_SQL = "SELECT ts, sym, ver AS version, last(price) AS price FROM base SAMPLE BY 1h";
    // the rows of COUNT_VIEW_SQL over the table that replaces the base in createViewRebuiltFromReplacedBase()
    private static final String REPLACED_BASE_ROWS = """
            ts\tsym\tcount\tprice
            2024-01-01T03:00:00.000000Z\td\t5\t4.0
            """;
    private static final String SELF_JOIN_VIEW_SQL = "SELECT a.ts, a.sym, last(b.amount) AS amount FROM base a ASOF JOIN base b ON (sym) SAMPLE BY 1h";
    private static final String VERSION_REASON = MatViewBareNameGuard.invalidationReason("version");
    private static final int VIEW_NAME_POSITION = "REFRESH MATERIALIZED VIEW ".length();
    // A test that races a column change with a refresh pauses the WAL apply of the change in its removal of the swap
    // file of the changed table's metadata, see raceColumnChange(): the bare-name guard has run, and a new reader does
    // not see the change yet.
    private final CountDownLatch applyPaused = new CountDownLatch(1);
    private final CountDownLatch applyRelease = new CountDownLatch(1);
    private final AtomicReference<Throwable> applierError = new AtomicReference<>();
    private final AtomicReference<String> pauseSuffix = new AtomicReference<>();
    // A test that fails the WAL apply of a column change sets it to the changed table's directory and the swap file of
    // its metadata: every removal of the swap file then fails, after the pause when the test races the change, and the
    // apply fails before the change is visible.
    private final AtomicReference<String> removalFailSuffix = new AtomicReference<>();
    private final FilesFacade pausingFf = new TestFilesFacadeImpl() {
        @Override
        public boolean removeQuiet(LPSZ name) {
            final String suffix = pauseSuffix.get();
            if (suffix != null && Utf8s.endsWithAscii(name, suffix) && pauseSuffix.compareAndSet(suffix, null)) {
                applyPaused.countDown();
                try {
                    Assert.assertTrue(applyRelease.await(60, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            final String failSuffix = removalFailSuffix.get();
            if (failSuffix != null && Utf8s.containsAscii(name, failSuffix)) {
                return false;
            }
            return super.removeQuiet(name);
        }
    };
    private Thread applier;

    @Test
    public void testAlterColumnTypeCastsToViewColumn() throws Exception {
        // The view column keeps its type, and the next refresh casts the changed query column to it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, qty INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            createView("qty_1h", "SELECT ts, sym, last(qty) AS qty FROM base SAMPLE BY 1h");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 7, '2024-01-01T00:10:00.000000Z'),
                    ('b', 8, '2024-01-01T00:20:00.000000Z')
                    """);
            drainWalAndMatViewQueues();

            execute("ALTER TABLE base ALTER COLUMN qty TYPE LONG");
            drainWalAndMatViewQueues();
            assertViewValid("qty_1h");
            // the last row of a's first bucket and a new bucket
            execute("""
                    INSERT INTO base VALUES
                    ('a', 9, '2024-01-01T00:30:00.000000Z'),
                    ('a', 3_000_000_000, '2024-01-01T01:10:00.000000Z'),
                    ('a', 11, '2024-01-01T01:20:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            assertViewValid("qty_1h");
            assertColumns("qty_1h", """
                    column\ttype
                    ts\tTIMESTAMP
                    sym\tSYMBOL
                    qty\tINT
                    """);
            assertViewRows("qty_1h", """
                    ts\tsym\tqty
                    2024-01-01T00:00:00.000000Z\ta\t9
                    2024-01-01T00:00:00.000000Z\tb\t8
                    2024-01-01T01:00:00.000000Z\ta\t11
                    """);
        });
    }

    @Test
    public void testBareSelectNamesFollowTheSql() throws Exception {
        // A definition parses its SQL once for the unqualified column references of every select list of its own
        // text, and parses it again once its SQL changes. The parse alone decides, so the SQL need not compile.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE VIEW rv_version AS (SELECT ts, sym, price, version FROM base)");
            drainWalAndViewQueues();
            createView("versions", BARE_VERSION_VIEW_SQL);
            final TableToken viewToken = engine.verifyTableName("versions");
            final MatViewDefinition definition = new MatViewDefinition();
            try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1)) {
                initDefinition(definition, viewToken, BARE_VERSION_VIEW_SQL);
                final LowerCaseCharSequenceHashSet names = definition.getBareSelectNames(engine, context);
                assertNames(names, "ts", "sym", "VERSION");
                Assert.assertSame(names, definition.getBareSelectNames(engine, context));

                // qualified references, calls and the wildcard of the top-level select list are no bare names
                initDefinition(definition, viewToken, "SELECT * FROM (SELECT b.ts, count() AS count, b.sym FROM base b SAMPLE BY 1h)");
                assertNames(definition.getBareSelectNames(engine, context));
                // a sub-query of an expression and a branch of a union
                initDefinition(definition, viewToken, "SELECT ts, sym, last(price) AS price FROM base WHERE sym IN (SELECT pi FROM base) SAMPLE BY 1h");
                assertNames(definition.getBareSelectNames(engine, context), "ts", "sym", "pi");
                initDefinition(definition, viewToken, "SELECT ts, sym FROM base UNION ALL SELECT ts, version FROM base");
                assertNames(definition.getBareSelectNames(engine, context), "ts", "sym", "version");
                // the text of a regular view that the SQL reads is not the view's own
                initDefinition(definition, viewToken, "SELECT ts, sym, last(price) AS price FROM rv_version SAMPLE BY 1h");
                assertNames(definition.getBareSelectNames(engine, context), "ts", "sym");
            }
        });
    }

    @Test
    public void testDropAndReAddColumnIsFollowed() throws Exception {
        // A column dropped and added again under the same name is read as any column: the next refresh compiles,
        // and a bucket recomputed later reads the new column, which holds NULL for the rows written before it. The
        // insert spans 00:50 to 02:10, so the refresh recomputes the buckets from 00:00 to 02:00.
        assertMemoryLeak(() -> {
            createBase();
            createView("amount_1h", AMOUNT_VIEW_SQL);
            insertBaseRows();
            drainWalAndMatViewQueues();

            execute("ALTER TABLE base DROP COLUMN amount");
            drainWalAndMatViewQueues();
            execute("ALTER TABLE base ADD COLUMN amount DOUBLE");
            drainWalAndMatViewQueues();
            execute("""
                    INSERT INTO base (sym, price, amount, version, ts) VALUES
                    ('b', 5.5, 55.0, 3, '2024-01-01T02:10:00.000000Z'),
                    ('a', 6.5, 66.0, 3, '2024-01-01T00:50:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            assertViewValid("amount_1h");
            assertViewRows("amount_1h", """
                    ts\tsym\tamount
                    2024-01-01T00:00:00.000000Z\ta\t66.0
                    2024-01-01T00:00:00.000000Z\tb\tnull
                    2024-01-01T01:00:00.000000Z\ta\tnull
                    2024-01-01T02:00:00.000000Z\tb\t55.0
                    """);
        });
    }

    @Test
    public void testDropUnreadColumnKeepsViewValid() throws Exception {
        assertUnreadColumnChangeKeepsViewValid(
                "ALTER TABLE base DROP COLUMN amount",
                "INSERT INTO base (sym, price, version, ts) VALUES ('b', 5.5, 3, '2024-01-01T02:10:00.000000Z')"
        );
    }

    @Test
    public void testGuardAddColumnUnderBareCallInvalidatesView() throws Exception {
        // A view created before the explicit name rules calls count without parentheses. A base column named count
        // would turn the call into a column read, and in a SAMPLE BY query into a grouping key.
        assertMemoryLeak(() -> {
            createBase();
            executeWithoutExplicitNamesCheck("CREATE MATERIALIZED VIEW counts AS (SELECT ts, sym, count FROM base SAMPLE BY 1h) PARTITION BY DAY");
            insertBaseRows();
            drainWalAndMatViewQueues();
            final String rows = """
                    ts\tsym\tcount
                    2024-01-01T00:00:00.000000Z\ta\t2
                    2024-01-01T00:00:00.000000Z\tb\t1
                    2024-01-01T01:00:00.000000Z\ta\t1
                    """;
            assertViewRows("counts", rows);

            execute("ALTER TABLE base ADD COLUMN count LONG");
            drainWalQueue();
            Assert.assertEquals(MatViewBareNameGuard.invalidationReason("count"), getViewState("counts").getPendingInvalidationReason());
            drainWalAndMatViewQueues();
            assertViewInvalid("counts", MatViewBareNameGuard.invalidationReason("count"));
            assertViewRows("counts", rows);

            // the rows the base receives later do not reach the invalid view
            execute("INSERT INTO base (sym, price, count, ts) VALUES ('b', 5.5, 42, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewInvalid("counts", MatViewBareNameGuard.invalidationReason("count"));
            assertViewRows("counts", rows);
        });
    }

    @Test
    public void testGuardAfterPassChecksViewDroppedAndRecreatedWhileDropApplies() throws Exception {
        // The guard checks the refreshed view mv before the DROP of count applies. While the DROP applies, the view is
        // dropped and created again under its name with the same SQL, and the new view is built from the base without
        // the DROP. The guard's check after the change tells the new view from the one it checked by the table id, and
        // invalidates it. The base change applies, and the view's name is free once the new view is dropped too.
        assertMemoryLeak(pausingFf, () -> {
            createRefreshedCountView();
            final int droppedViewId = engine.verifyTableName("mv").getTableId();
            raceColumnChange("base", DROP_COUNT, () -> {
                execute("DROP MATERIALIZED VIEW mv");
                createViewAndBuild("mv", "REFRESH MANUAL", COUNT_VIEW_SQL);
                Assert.assertNotEquals(droppedViewId, engine.verifyTableName("mv").getTableId());
                assertViewState("mv", "valid", "", 1);
            });
            drainWalAndMatViewQueues();
            Assert.assertEquals(2, appliedTxn(engine.verifyTableName("base")));
            assertColumns("base", BASE_COLUMNS_WITHOUT_COUNT);
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);

            execute("DROP MATERIALIZED VIEW mv");
            drainWalAndMatViewQueues();
            Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testGuardAfterPassContinuesPastViewCheckedBeforeChange() throws Exception {
        // The refreshed view mv0, which calls count(), precedes the view mv, created and built while the DROP of count
        // applies, in the base's dependent views. The guard's check after the change skips mv0, whose SQL the check
        // before the change found without a bare count, and goes on to invalidate mv.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            createViewAndBuild("mv0", "REFRESH MANUAL", COUNT_CALL_SQL);
            assertViewState("mv0", "valid", "", 1);
            raceColumnChange("base", DROP_COUNT, () -> createViewAndBuild("mv", "REFRESH MANUAL", COUNT_VIEW_SQL));
            drainWalAndMatViewQueues();
            assertViewState("mv0", "valid", "", 1);
            Assert.assertFalse(getViewState("mv0").hasPendingInvalidationReason());
            assertViewRows("mv0", COUNT_CALL_ROWS);
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardAfterPassForgetsChangeWhoseApplyFailed() throws Exception {
        // The WAL apply job's guard checks the base's views before the DROP of count applies, and the apply fails and
        // suspends the base's WAL, so the guard's check after the change does not run. The same job then applies a
        // change of base2 that changes no column name, and the guard checks no view of base2 after it: the refreshed
        // view mv2, whose SQL reads base2's column count bare, stays valid and keeps reading the column.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            execute("CREATE TABLE base2 (sym SYMBOL, price DOUBLE, count LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base2 VALUES ('a', 1.0, 7, '2024-01-01T00:10:00.000000Z'), ('b', 2.0, 8, '2024-01-01T01:10:00.000000Z')");
            drainWalQueue();
            createViewAndBuild("mv2", "REFRESH MANUAL", BASE2_COUNT_VIEW_SQL);
            assertViewState("mv2", "valid", "", 1);
            final TableToken baseToken = engine.verifyTableName("base");
            final TableToken base2Token = engine.verifyTableName("base2");
            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                removalFailSuffix.set(baseToken.getDirName() + Files.SEPARATOR + TableUtils.META_SWAP_FILE_NAME);
                execute(DROP_COUNT);
                drainWalQueue(walApplyJob);
                removalFailSuffix.set(null);
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(baseToken));
                Assert.assertEquals(1, appliedTxn(baseToken));

                // the guard sees structural changes only, such as DEDUP ENABLE, which changes no column name
                execute("ALTER TABLE base2 DEDUP ENABLE UPSERT KEYS(ts)");
                drainWalQueue(walApplyJob);
                Assert.assertEquals(2, appliedTxn(base2Token));
            }
            Assert.assertFalse(getViewState("mv2").hasPendingInvalidationReason());
            drainWalAndMatViewQueues();
            assertViewState("mv2", "valid", "", 1);
            assertViewRows("mv2", COUNT_COLUMN_ROWS);

            execute("INSERT INTO base2 VALUES ('c', 3.0, 9, '2024-01-01T02:10:00.000000Z')");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW mv2 INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewState("mv2", "valid", "", 3);
            assertViewRows("mv2", COUNT_COLUMN_ROWS + "2024-01-01T02:00:00.000000Z\tc\t9\t3.0\n");
            assertSqlCursors(BASE2_COUNT_VIEW_SQL + " ORDER BY ts, sym", "mv2 ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardAfterPassIgnoresUnrelatedDrop() throws Exception {
        // The DROP of a column whose name is no function's switches no bare name, so the guard's check after the change
        // does not even parse the SQL of the view created while the DROP applies: a SQL that does not parse logs
        // nothing. The view, whose SQL reads the column count bare, keeps reading it.
        assertMemoryLeak(pausingFf, () -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, count LONG, amount DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('a', 1.0, 7, 10.0, '2024-01-01T00:10:00.000000Z'), ('b', 2.0, 8, 20.0, '2024-01-01T01:10:00.000000Z')");
            drainWalQueue();
            final LogCapture logCapture = new LogCapture();
            logCapture.start();
            try {
                raceColumnChange("base", "ALTER TABLE base DROP COLUMN amount", () -> {
                    createViewAndBuild("mv", "REFRESH MANUAL", COUNT_VIEW_SQL);
                    getViewState("mv").getViewDefinition().setMatViewSqlForTesting("SELECT ts, sym, count, last(price) AS price FROM");
                });
                logCapture.drain();
                Assert.assertEquals(0, logCapture.countLoggedRE("could not parse materialized view SQL to find its bare select-list names \\[view=mv~"));
            } finally {
                logCapture.stop();
            }
            getViewState("mv").getViewDefinition().setMatViewSqlForTesting(COUNT_VIEW_SQL);
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 1);
            Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
            assertViewRows("mv", COUNT_COLUMN_ROWS);

            execute("INSERT INTO base (sym, price, count, ts) VALUES ('c', 3.0, 9, '2024-01-01T02:10:00.000000Z')");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 3);
            assertViewRows("mv", COUNT_COLUMN_ROWS + "2024-01-01T02:00:00.000000Z\tc\t9\t3.0\n");
            assertSqlCursors(COUNT_VIEW_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardAfterPassInvalidatesViewWhoseStateAppearedWhileDropApplies() throws Exception {
        // The view has no state when the guard checks the base's views before the DROP of count applies, so the guard
        // skips it. Its state appears while the DROP applies, and its first build reads count as the column. The
        // guard's check after the change finds the view and invalidates it.
        assertMemoryLeak(pausingFf, () -> {
            createCountBaseWithView("REFRESH MANUAL DEFERRED");
            final TableToken viewToken = engine.verifyTableName("mv");
            engine.getMatViewStateStore().removeViewState(viewToken);
            raceColumnChange("base", DROP_COUNT, () -> {
                engine.getMatViewStateStore().createViewState(engine.getDependentViewGraph().getViewDefinition(viewToken));
                execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
                drainMatViewQueue(engine);
                drainWalQueue();
                assertViewState("mv", "valid", "", 1);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardAfterPassInvalidationIsCoveredByFull() throws Exception {
        // As testGuardInvalidatesViewCreatedWhileDropAppliesRefreshedBeforeCheck(), with a FULL refresh in place of the
        // incremental one: it rebuilds the view from base txn 2, the DROP's own, before the guard's check after the
        // change publishes its invalidation of the DROP's txn, which the FULL's snapshot covers. The view stays valid,
        // every row calling count().
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            raceColumnChange("base", DROP_COUNT, () -> {
                createViewAndBuild("mv", "REFRESH MANUAL", COUNT_VIEW_SQL);
                runWhileGuardChecksAfterChange("mv", () -> {
                    execute("REFRESH MATERIALIZED VIEW mv FULL");
                    drainWalAndMatViewQueues();
                    drainWalAndMatViewQueues();
                    assertViewState("mv", "valid", "", 2);
                    assertViewRows("mv", COUNT_CALL_ROWS);
                });
                Assert.assertEquals(COUNT_REASON, getViewState("mv").getPendingInvalidationReason());
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 2);
            Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
            assertViewRows("mv", COUNT_CALL_ROWS);
            assertSqlCursors(COUNT_CALL_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardAfterPassInvalidationSurvivesRestart() throws Exception {
        // The guard's check after the DROP of count invalidates the view created and built while the DROP applied. The
        // view's state keeps the guard's reason across a restart that loses the switch the guard recorded in memory,
        // and RESUME refuses the SQL, which reads count as a column the base no longer has.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            raceColumnChange("base", DROP_COUNT, () -> createViewAndBuild("mv", "REFRESH MANUAL", COUNT_VIEW_SQL));
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            restartViewStates();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
            execute(INSERT_C_ROW);
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW mv RESUME", VIEW_NAME_POSITION, COUNT_COMPILE_REFUSAL);
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
        });
    }

    @Test
    public void testGuardAfterPassLeavesUnparsableSqlToNextRefresh() throws Exception {
        // When the SQL of the view created while the DROP of count applies does not parse, the guard's check after the
        // change cannot tell whether it uses count bare, and leaves the view to its next refresh, whose compile fails
        // on the same error.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            final String brokenSql = "SELECT ts, sym, count, last(price) AS price FROM";
            final LogCapture logCapture = new LogCapture();
            logCapture.start();
            try {
                raceColumnChange("base", DROP_COUNT, () -> {
                    createViewAndBuild("mv", "REFRESH MANUAL", COUNT_VIEW_SQL);
                    getViewState("mv").getViewDefinition().setMatViewSqlForTesting(brokenSql);
                });
                logCapture.drain();
                logCapture.assertLoggedRE("could not parse materialized view SQL to find its bare select-list names \\[view=mv~\\d+, error=table name or sub-query expected\\]");
            } finally {
                logCapture.stop();
            }
            Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 1);

            execute(INSERT_C_ROW);
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewInvalid("mv", "[" + brokenSql.length() + "]: table name or sub-query expected");
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardAfterPassRetriesViewWhoseCheckFailed() throws Exception {
        // The guard's check of the refreshed view before the DROP of count fails on the heap while it records the
        // switch. The check after the change finds the view unchecked, records the switch and invalidates the view.
        assertMemoryLeak(() -> {
            createRefreshedCountView();
            final AtomicBoolean isFired = new AtomicBoolean();
            getViewState("mv").setOnBareNameSwitchCopiedForTesting(() -> {
                isFired.set(true);
                throw new OutOfMemoryError("test bare-name switch record failure");
            });
            execute(DROP_COUNT);
            drainWalAndMatViewQueues();
            Assert.assertTrue(isFired.get());
            Assert.assertEquals(2, appliedTxn(engine.verifyTableName("base")));
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardAfterPassSkipsLiveView() throws Exception {
        // A live view on the base precedes the view created while the DROP of count applies in the base's dependent
        // views. The guard's check after the change leaves the live view alone, as the check before it does, and
        // invalidates the view.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT ts, sym, count(*) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE price > 0");
            raceColumnChange("base", DROP_COUNT, () -> createViewAndBuild("mv", "REFRESH MANUAL", COUNT_VIEW_SQL));
            drainWalAndMatViewQueues();
            Assert.assertFalse(engine.getLiveViewRegistry().getViewInstance("lv").isInvalid());
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardAfterPassSkipsViewCheckedBeforeChange() throws Exception {
        // The guard checks the never-refreshed view before the DROP of count applies, and its check after the change
        // skips the view, whose SQL is the one it checked: the switch is recorded once. The INVALIDATE handler declines
        // the guard's invalidation of the view without a refresh position, and the view's first build after the DROP
        // calls count() in every bucket.
        assertMemoryLeak(pausingFf, () -> {
            createCountBaseWithView("REFRESH MANUAL DEFERRED");
            final AtomicBoolean isRecordedAfterChange = new AtomicBoolean();
            raceColumnChange("base", DROP_COUNT, () -> getViewState("mv").setOnBareNameSwitchRecordedForTesting(() -> isRecordedAfterChange.set(true)));
            Assert.assertFalse(isRecordedAfterChange.get());
            getViewState("mv").setOnBareNameSwitchRecordedForTesting(null);
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", -1);

            execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 2);
            Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
            assertViewRows("mv", COUNT_CALL_ROWS);
            assertSqlCursors(COUNT_CALL_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardAfterPassSkipsViewWhoseDefinitionChangedWithSameSql() throws Exception {
        // ALTER MATERIALIZED VIEW ... SET REFRESH LIMIT installs a new definition of the refreshed view while the DROP
        // of count applies, with the SQL that the guard checked before the change. The guard's check after the change
        // compares the SQL text and skips the view, which the check before the change invalidated.
        assertMemoryLeak(pausingFf, () -> {
            createRefreshedCountView();
            final MatViewDefinition checkedDefinition = getViewState("mv").getViewDefinition();
            final AtomicBoolean isRecordedAfterChange = new AtomicBoolean();
            raceColumnChange("base", DROP_COUNT, () -> {
                execute("ALTER MATERIALIZED VIEW mv SET REFRESH LIMIT 1 DAYS");
                drainWalQueue();
                Assert.assertNotSame(checkedDefinition, getViewState("mv").getViewDefinition());
                Assert.assertEquals(COUNT_VIEW_SQL, getViewState("mv").getViewDefinition().getMatViewSql());
                getViewState("mv").setOnBareNameSwitchRecordedForTesting(() -> isRecordedAfterChange.set(true));
            });
            Assert.assertFalse(isRecordedAfterChange.get());
            getViewState("mv").setOnBareNameSwitchRecordedForTesting(null);
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardAfterPassSkipsViewWithoutState() throws Exception {
        // The views mv and mv2 are created while the DROP of count applies, and mv's state goes, as during a concurrent
        // DROP MATERIALIZED VIEW. The guard's check after the change skips mv and invalidates mv2, which follows it in
        // the base's dependent views and was built from the base without the DROP. The base change applies.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            raceColumnChange("base", DROP_COUNT, () -> {
                execute("CREATE MATERIALIZED VIEW mv REFRESH MANUAL DEFERRED AS (" + COUNT_VIEW_SQL + ") PARTITION BY DAY");
                createViewAndBuild("mv2", "REFRESH MANUAL", COUNT_VIEW_SQL);
                engine.getMatViewStateStore().removeViewState(engine.verifyTableName("mv"));
            });
            drainWalAndMatViewQueues();
            Assert.assertEquals(2, appliedTxn(engine.verifyTableName("base")));
            assertColumns("base", BASE_COLUMNS_WITHOUT_COUNT);
            assertViewState("mv2", "invalid", COUNT_REASON, 1);
            assertViewRows("mv2", COUNT_COLUMN_ROWS);
            execute("DROP MATERIALIZED VIEW mv");
            drainWalQueue();
            Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testGuardAfterPassSurvivesFailedInvalidationEnqueue() throws Exception {
        // The guard's check after the DROP of count sets the marker of the view created and built while the DROP
        // applied, and the enqueue of its INVALIDATE task fails. The store records a retry, the base change stays
        // applied, and the refresh job's retry scan delivers the invalidation.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            final MatViewStateStoreImpl store = (MatViewStateStoreImpl) engine.getMatViewStateStore();
            final AtomicBoolean isFired = new AtomicBoolean();
            try {
                raceColumnChange("base", DROP_COUNT, () -> {
                    createViewAndBuild("mv", "REFRESH MANUAL", COUNT_VIEW_SQL);
                    // the guard's INVALIDATE task is the first task that the store queues after the pause
                    store.setOnTaskQueueAppendForTesting(() -> {
                        if (isFired.compareAndSet(false, true)) {
                            throw new OutOfMemoryError("test invalidation enqueue failure");
                        }
                    });
                });
            } finally {
                store.setOnTaskQueueAppendForTesting(null);
            }
            Assert.assertTrue(isFired.get());
            Assert.assertEquals(2, appliedTxn(engine.verifyTableName("base")));
            assertColumns("base", BASE_COLUMNS_WITHOUT_COUNT);
            Assert.assertEquals(COUNT_REASON, getViewState("mv").getPendingInvalidationReason());
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardChildViewReadsParentColumnBare() throws Exception {
        // The guard also covers a view built on another view: a view DROP COLUMN of count, which the child reads
        // bare, would turn the child's count into a call of count().
        assertMemoryLeak(() -> {
            createBase();
            createView("parent", "SELECT ts, sym, count() AS count, last(price) AS price FROM base SAMPLE BY 1h");
            createView("child", "SELECT ts, sym, count, last(price) AS price FROM parent SAMPLE BY 1d");
            createView("other_child", "SELECT ts, sym, max(price) AS price FROM parent SAMPLE BY 1d");
            insertBaseRows();
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            final String childRows = """
                    ts\tsym\tcount\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1\t4.5
                    2024-01-01T00:00:00.000000Z\ta\t2\t3.5
                    2024-01-01T00:00:00.000000Z\tb\t1\t2.5
                    """;
            assertViewRows("child", "ts, sym, count", childRows);

            execute("ALTER MATERIALIZED VIEW parent DROP COLUMN count");
            drainWalAndMatViewQueues();
            assertViewInvalid("child", MatViewBareNameGuard.invalidationReason("count"));
            assertViewRows("child", "ts, sym, count", childRows);
            assertViewValid("parent");
            assertViewValid("other_child");

            execute("INSERT INTO base (sym, price, amount, version, ts) VALUES ('a', 9.5, 90.0, 3, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewValid("other_child");
            assertViewRows("other_child", """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t9.5
                    2024-01-01T00:00:00.000000Z\tb\t2.5
                    """);
            assertViewInvalid("child", MatViewBareNameGuard.invalidationReason("count"));
            assertViewRows("child", "ts, sym, count", childRows);
        });
    }

    @Test
    public void testGuardColumnChangeInLaterBatchKeepsAttachRangeForResume() throws Exception {
        // As testGuardColumnChangeInLaterBatchKeepsUpdateRangeForResume(), with an ATTACH PARTITION in place of the
        // UPDATE: the ATTACH's range task takes the attached day into the still valid view's cache, the bare-name guard's
        // invalidation makes the cache the backlog, and RESUME brings the attached day.
        assertMemoryLeak(() -> {
            createBase();
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.5, 10.0, 1, '2024-01-01T00:10:00.000000Z'),
                    ('c', 3.5, 30.0, 2, '2024-01-02T00:10:00.000000Z')
                    """);
            drainWalQueue();
            execute("ALTER TABLE base DETACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            makeAttachable("2024-01-01");
            createView("versions", BARE_VERSION_VIEW_SQL);
            drainWalAndMatViewQueues();
            final String dayTwoRows = """
                    ts\tsym\tversion\tprice
                    2024-01-02T00:00:00.000000Z\tc\t2\t3.5
                    """;
            assertViewRows("versions", "ts, sym, version", dayTwoRows);
            assertRefreshBaseTxn("versions", 2);

            // base txn 3, then base txn 4 in the next WAL batch
            execute("ALTER TABLE base ATTACH PARTITION LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE base RENAME COLUMN version TO ver");
            drainWalQueue();
            final MatViewState state = getViewState("versions");
            Assert.assertEquals(VERSION_REASON, state.getPendingInvalidationReason());
            Assert.assertFalse(state.isInvalid());
            final TableToken viewToken = engine.verifyTableName("versions");
            final long viewSeqTxn = getSeqTxn(viewToken);
            drainWalAndMatViewQueues();
            assertViewInvalid("versions", VERSION_REASON);
            // the range task's view state event, then the invalidation's
            Assert.assertEquals(viewSeqTxn + 2, getSeqTxn(viewToken));
            Assert.assertNull(state.getBacklogLostReason());
            assertFirstDayCached(state, ColumnType.TIMESTAMP_MICRO, 4);
            assertViewRows("versions", "ts, sym, version", dayTwoRows);

            execute("ALTER MATERIALIZED VIEW versions AS (" + RENAMED_VERSION_VIEW_SQL + ")");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW versions RESUME");
            drainWalAndMatViewQueues();
            assertViewValid("versions");
            assertViewRows("versions", "ts, sym, version", """
                    ts\tsym\tversion\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1\t1.5
                    2024-01-02T00:00:00.000000Z\tc\t2\t3.5
                    """);
            assertSqlCursors(RENAMED_VERSION_VIEW_SQL + " ORDER BY ts, sym, version", "versions ORDER BY ts, sym, version");
        });
    }

    @Test
    public void testGuardColumnChangeInLaterBatchKeepsUpdateRangeForResume() throws Exception {
        // A base UPDATE applies in one WAL batch, and its range task still waits in the refresh queue when a RENAME or
        // DROP of the bare name version applies in the next batch, whose bare-name guard publishes the view's
        // invalidation first. The range task takes the UPDATE's range, which only it carries, into the still valid
        // view's cache, and the invalidation makes the cache, the range included, the backlog the view starts with.
        // Once ALTER ... AS fixes the SQL, RESUME catches up on the UPDATE: for an IMMEDIATE view at once, for a MANUAL
        // view at its next REFRESH ... INCREMENTAL. The range is in the base's timestamp units.
        assertMemoryLeak(() -> {
            for (int timestampType : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                for (boolean isDrop : new boolean[]{false, true}) {
                    for (boolean isImmediate : new boolean[]{true, false}) {
                        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, version INT, ts " + ColumnType.nameOf(timestampType)
                                + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
                        insertBaseRows();
                        drainWalQueue();
                        execute("CREATE MATERIALIZED VIEW versions " + (isImmediate ? "" : "REFRESH MANUAL ")
                                + "AS (" + BARE_VERSION_VIEW_SQL + ") PARTITION BY DAY");
                        drainWalQueue();
                        if (!isImmediate) {
                            execute("REFRESH MATERIALIZED VIEW versions INCREMENTAL");
                        }
                        drainWalAndMatViewQueues();
                        assertViewRows("versions", "ts, sym, version", inTimestampUnits(BARE_VERSION_VIEW_ROWS, timestampType));
                        assertRefreshBaseTxn("versions", 1);

                        // base txn 2, then base txn 3 in the next WAL batch
                        execute("UPDATE base SET price = 9.0 WHERE sym = 'a'");
                        drainWalQueue();
                        execute(isDrop ? "ALTER TABLE base DROP COLUMN version" : "ALTER TABLE base RENAME COLUMN version TO ver");
                        drainWalQueue();
                        final MatViewState state = getViewState("versions");
                        Assert.assertEquals(VERSION_REASON, state.getPendingInvalidationReason());
                        Assert.assertFalse(state.isInvalid());
                        final TableToken viewToken = engine.verifyTableName("versions");
                        final long viewSeqTxn = getSeqTxn(viewToken);
                        drainWalAndMatViewQueues();
                        assertViewInvalid("versions", VERSION_REASON);
                        // the range task's view state event, then the invalidation's
                        Assert.assertEquals(viewSeqTxn + 2, getSeqTxn(viewToken));
                        Assert.assertNull(state.getBacklogLostReason());
                        assertFirstDayCached(state, timestampType, 3);
                        assertViewRows("versions", "ts, sym, version", inTimestampUnits(BARE_VERSION_VIEW_ROWS, timestampType));

                        final String fixedSql = isDrop
                                ? "SELECT ts, sym, NULL::INT AS version, last(price) AS price FROM base SAMPLE BY 1h"
                                : RENAMED_VERSION_VIEW_SQL;
                        execute("ALTER MATERIALIZED VIEW versions AS (" + fixedSql + ")");
                        drainWalQueue();
                        execute("REFRESH MATERIALIZED VIEW versions RESUME");
                        drainWalQueue();
                        if (!isImmediate) {
                            execute("REFRESH MATERIALIZED VIEW versions INCREMENTAL");
                        }
                        drainWalAndMatViewQueues();
                        assertViewValid("versions");
                        assertViewRows("versions", "ts, sym, version", inTimestampUnits(isDrop ? """
                                ts\tsym\tversion\tprice
                                2024-01-01T00:00:00.000000Z\ta\tnull\t9.0
                                2024-01-01T00:00:00.000000Z\tb\tnull\t2.5
                                2024-01-01T01:00:00.000000Z\ta\tnull\t9.0
                                """ : RENAMED_VERSION_UPDATED_ROWS, timestampType));
                        assertSqlCursors(fixedSql + " ORDER BY ts, sym, version", "versions ORDER BY ts, sym, version");
                        execute("DROP MATERIALIZED VIEW versions");
                        execute("DROP TABLE base");
                        drainWalAndMatViewQueues();
                    }
                }
            }
        });
    }

    @Test
    public void testGuardColumnChangeInUpdateBatchResumesExactly() throws Exception {
        // A base UPDATE and a RENAME of the bare name version apply in one WAL batch: the bare-name guard publishes the
        // view's invalidation before the batch queues the UPDATE's range task, so the view is invalid when the task
        // takes the range into its backlog, and RESUME, once ALTER ... AS fixes the SQL, catches up on the UPDATE.
        assertMemoryLeak(() -> {
            createBase();
            createView("versions", BARE_VERSION_VIEW_SQL);
            insertBaseRows();
            drainWalAndMatViewQueues();
            assertViewRows("versions", "ts, sym, version", BARE_VERSION_VIEW_ROWS);

            execute("UPDATE base SET price = 9.0 WHERE sym = 'a'");
            execute("ALTER TABLE base RENAME COLUMN version TO ver");
            final TableToken viewToken = engine.verifyTableName("versions");
            final long viewSeqTxn = getSeqTxn(viewToken);
            drainWalAndMatViewQueues();
            assertViewInvalid("versions", VERSION_REASON);
            // the invalidation's view state event, then the range task's
            Assert.assertEquals(viewSeqTxn + 2, getSeqTxn(viewToken));
            final MatViewState state = getViewState("versions");
            Assert.assertNull(state.getBacklogLostReason());
            assertFirstDayCached(state, ColumnType.TIMESTAMP_MICRO, 3);

            assertResumeAfterFixCatchesUpOnUpdate();
        });
    }

    @Test
    public void testGuardDropBareColumnInvalidatesOnApply() throws Exception {
        // The view reads the base column version bare. Once the column is gone, the same SQL compiles to a call of
        // version(), so the guard invalidates the view when the DROP applies, before any refresh.
        assertMemoryLeak(() -> {
            createBase();
            createView("versions", BARE_VERSION_VIEW_SQL);
            insertBaseRows();
            drainWalAndMatViewQueues();
            assertViewRows("versions", "ts, sym, version", BARE_VERSION_VIEW_ROWS);
            final TableToken viewToken = engine.verifyTableName("versions");
            final long viewSeqTxn = getSeqTxn(viewToken);

            execute("ALTER TABLE base DROP COLUMN version");
            drainWalQueue();
            // published on apply, before the refresh job ran
            Assert.assertEquals(VERSION_REASON, getViewState("versions").getPendingInvalidationReason());
            Assert.assertFalse(getViewState("versions").isInvalid());
            drainMatViewQueue(engine);
            drainWalQueue();
            assertViewInvalid("versions", VERSION_REASON);
            assertViewRows("versions", "ts, sym, version", BARE_VERSION_VIEW_ROWS);
            // the invalidation is the only transaction the view received, and the refresh position stayed at the
            // insert, base txn 1
            Assert.assertEquals(viewSeqTxn + 1, getSeqTxn(viewToken));
            assertRefreshBaseTxn("versions", 1);
        });
    }

    @Test
    public void testGuardFindsBareNamesInEverySelectList() throws Exception {
        // The guard reads every select list of the view's own text, such as one below a wildcard or in a join
        // branch, wherever the name resolves.
        assertMemoryLeak(() -> {
            createBase();
            createView("wildcard", "SELECT * FROM (SELECT ts, sym, version, last(price) AS price FROM base SAMPLE BY 1h)");
            createView(
                    "join_branch",
                    "SELECT a.ts, a.sym, last(b.price) AS price FROM base a ASOF JOIN (SELECT ts, sym, price, version FROM base) b ON (sym) SAMPLE BY 1h"
            );
            createView("price_1h", PRICE_VIEW_SQL);
            insertBaseRows();
            drainWalAndMatViewQueues();
            for (String view : new String[]{"wildcard", "join_branch", "price_1h"}) {
                assertViewValid(view);
            }

            execute("ALTER TABLE base DROP COLUMN version");
            drainWalAndMatViewQueues();
            for (String view : new String[]{"wildcard", "join_branch"}) {
                assertViewInvalid(view, VERSION_REASON);
            }
            assertViewValid("price_1h");
        });
    }

    @Test
    public void testGuardIgnoresNamesNotUsedBare() throws Exception {
        // A DROP of a column named like a function that no view reads bare, and of a column whose name is no
        // function, invalidate no view on apply. The views that read the dropped column go invalid at their next
        // refresh that recomputes rows, when their SQL no longer compiles.
        assertMemoryLeak(() -> {
            createBase();
            final String aggregated = "SELECT ts, sym, last(version) AS version FROM base SAMPLE BY 1h";
            final String qualified = "SELECT b.ts, b.sym, b.version, last(b.price) AS price FROM base b SAMPLE BY 1h";
            final String bareAmount = "SELECT ts, sym, amount, last(price) AS price FROM base SAMPLE BY 1h";
            createView("aggregated", aggregated);
            createView("qualified", qualified);
            createView("bare_amount", bareAmount);
            createView("price_1h", PRICE_VIEW_SQL);
            insertBaseRows();
            drainWalAndMatViewQueues();

            execute("ALTER TABLE base DROP COLUMN version");
            drainWalQueue();
            for (String view : new String[]{"aggregated", "qualified", "bare_amount", "price_1h"}) {
                Assert.assertFalse(view, getViewState(view).hasPendingInvalidationReason());
            }
            drainWalAndMatViewQueues();
            execute("ALTER TABLE base DROP COLUMN amount");
            drainWalAndMatViewQueues();
            // a column added under a function's name that no view reads bare
            execute("ALTER TABLE base ADD COLUMN version LONG");
            drainWalAndMatViewQueues();
            for (String view : new String[]{"aggregated", "qualified", "bare_amount", "price_1h"}) {
                assertViewValid(view);
            }

            execute("INSERT INTO base (sym, price, version, ts) VALUES ('b', 5.5, 3, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            // version was dropped and added again, so the views that read it compile
            assertViewValid("aggregated");
            assertViewValid("qualified");
            assertViewInvalid("bare_amount", "[" + bareAmount.indexOf("amount") + "]: Invalid column: amount");
            assertViewValid("price_1h");
            assertViewRows("price_1h", PRICE_VIEW_ROWS_AFTER_INSERT);
            assertViewRows("aggregated", """
                    ts\tsym\tversion
                    2024-01-01T00:00:00.000000Z\ta\t2
                    2024-01-01T00:00:00.000000Z\tb\t1
                    2024-01-01T01:00:00.000000Z\ta\t2
                    2024-01-01T02:00:00.000000Z\tb\t3
                    """);
        });
    }

    @Test
    public void testGuardIgnoresSqlThatIsNoQuery() throws Exception {
        // A definition whose SQL is no query, which no CREATE writes, has no select list, so the guard finds no bare
        // name in it.
        assertMemoryLeak(() -> {
            createBase();
            createView("versions", BARE_VERSION_VIEW_SQL);
            insertBaseRows();
            drainWalAndMatViewQueues();
            final MatViewDefinition definition = getViewState("versions").getViewDefinition();
            definition.setMatViewSqlForTesting("INSERT INTO base (sym, ts) VALUES ('c', '2024-01-01T05:00:00.000000Z')");
            try {
                execute("ALTER TABLE base DROP COLUMN version");
                drainWalQueue();
                Assert.assertFalse(getViewState("versions").hasPendingInvalidationReason());
            } finally {
                definition.setMatViewSqlForTesting(BARE_VERSION_VIEW_SQL);
            }
            drainWalAndMatViewQueues();
            assertViewValid("versions");
        });
    }

    @Test
    public void testGuardInvalidatesChildCreatedWhileParentDropApplies() throws Exception {
        // The child view, created and built while its parent view drops the column count, reads the parent's count bare
        // as the column. The guard's check after the parent's change invalidates the child and keeps the parent valid.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            createView("p", "SELECT ts, sym, last(count) AS count, last(price) AS price FROM base SAMPLE BY 1h");
            drainWalAndMatViewQueues();
            final long parentTxn = getSeqTxn(engine.verifyTableName("p"));
            final String childRows = """
                    ts\tsym\tcount\tprice
                    2024-01-01T00:00:00.000000Z\ta\t7\t1.0
                    2024-01-01T00:00:00.000000Z\tb\t8\t2.0
                    """;
            raceColumnChange("p", "ALTER MATERIALIZED VIEW p DROP COLUMN count", () -> {
                createViewAndBuild("child", "REFRESH IMMEDIATE", "SELECT ts, sym, count, last(price) AS price FROM p SAMPLE BY 1d");
                assertViewState("child", "valid", "", parentTxn);
                assertViewRows("child", childRows);
            });
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewState("child", "invalid", COUNT_REASON, parentTxn);
            assertViewRows("child", childRows);
            assertViewValid("p");
        });
    }

    @Test
    public void testGuardInvalidatesChildViewRebuiltWhileParentDropApplies() throws Exception {
        // The child view reads its parent view's column count bare. The parent drops the column, and the child's INVALIDATE
        // handler marks the child invalid while the drop applies. The child's FULL refresh then runs before the drop is
        // visible: it reads count as the parent's column, commits the parent's txn before the drop as its refresh
        // position, below the drop's txn that the guard recorded for the parent, and publishes the guard's invalidation
        // again.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            createView("p", "SELECT ts, sym, last(count) AS count, last(price) AS price FROM base SAMPLE BY 1h");
            createView("child", "SELECT ts, sym, count, last(price) AS price FROM p SAMPLE BY 1d");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            final long parentTxn = getSeqTxn(engine.verifyTableName("p"));
            final String childRows = """
                    ts\tsym\tcount\tprice
                    2024-01-01T00:00:00.000000Z\ta\t7\t1.0
                    2024-01-01T00:00:00.000000Z\tb\t8\t2.0
                    """;
            assertViewState("child", "valid", "", parentTxn);
            assertViewRows("child", childRows);

            raceColumnChange("p", "ALTER MATERIALIZED VIEW p DROP COLUMN count", () -> {
                drainMatViewQueue(engine);
                drainWalQueue();
                assertViewState("child", "invalid", COUNT_REASON, parentTxn);
                execute("REFRESH MATERIALIZED VIEW child FULL");
                final AtomicReference<String> stateAtUnlock = captureStateAtUnlock("child");
                drainMatViewQueue(engine);
                Assert.assertEquals(pendingGuardState("p", parentTxn, parentTxn + 1), stateAtUnlock.get());
                drainWalQueue();
                assertViewState("child", "invalid", COUNT_REASON, parentTxn);
                assertViewRows("child", childRows);
            });
            drainWalAndMatViewQueues();
            assertViewState("child", "invalid", COUNT_REASON, parentTxn);
            assertViewRows("child", childRows);
        });
    }

    @Test
    public void testGuardInvalidatesFirstBuildOfEmptyBaseRacedDrop() throws Exception {
        // The view's first build on an empty base finds nothing to recompute while the DROP of count, the base's txn 1,
        // applies. It still commits refresh position 0, below the switch the guard recorded, so it publishes the
        // guard's invalidation again: the decline of the guard's invalidation is for a view without a refresh position.
        assertMemoryLeak(pausingFf, () -> {
            execute(CREATE_COUNT_BASE);
            execute("CREATE MATERIALIZED VIEW mv REFRESH MANUAL DEFERRED AS (" + COUNT_VIEW_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            raceColumnChange("base", DROP_COUNT, () -> {
                drainMatViewQueue(engine);
                drainWalQueue();
                assertViewState("mv", "valid", "", -1);
                execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
                final AtomicReference<String> stateAtUnlock = captureStateAtUnlock("mv");
                drainMatViewQueue(engine);
                Assert.assertEquals(pendingGuardState("base", 0, 1), stateAtUnlock.get());
                drainWalQueue();
                assertViewState("mv", "invalid", COUNT_REASON, 0);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 0);
            assertViewRows("mv", "ts\tsym\tcount\tprice\n");
        });
    }

    @Test
    public void testGuardInvalidatesImmediateViewCreatedWhileDropApplies() throws Exception {
        // The CREATE of an IMMEDIATE view that reads count bare queues the view's first build, which runs while the
        // DROP of count applies and reads count as the column. The guard's check after the change invalidates the view,
        // and the refreshes that the base's commits trigger skip it, so the rows of later inserts do not reach it.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            raceColumnChange("base", DROP_COUNT, () -> {
                createViewAndBuild("mv", "REFRESH IMMEDIATE", COUNT_VIEW_SQL);
                assertViewState("mv", "valid", "", 1);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            execute(INSERT_C_ROW);
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardInvalidatesImmediateViewWhoseFirstBuildRacedDrop() throws Exception {
        // An IMMEDIATE view's first build runs while the DROP of count applies, after the INVALIDATE handler declined
        // the guard's invalidation of the never-refreshed view. The build reads count as the column and publishes the
        // guard's invalidation again. The refresh that the DROP's batch notifies then skips the invalid view, so its
        // refresh position stays below the DROP, and the rows of later inserts do not reach it.
        assertMemoryLeak(pausingFf, () -> {
            createCountBaseWithView("REFRESH IMMEDIATE DEFERRED");
            raceColumnChange("base", DROP_COUNT, () -> {
                drainMatViewQueue(engine);
                Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
                Assert.assertEquals(-1, getViewState("mv").getLastRefreshBaseTxn());
                engine.getMatViewStateStore().enqueueIncrementalRefresh(engine.verifyTableName("mv"));
                final AtomicReference<String> stateAtUnlock = captureStateAtUnlock("mv");
                drainMatViewQueue(engine);
                Assert.assertEquals(pendingGuardState("base", 1, 2), stateAtUnlock.get());
                drainWalQueue();
                assertViewState("mv", "invalid", COUNT_REASON, 1);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            execute(INSERT_C_ROW);
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardInvalidatesInvalidViewMovedToBaseWhileDropApplies() throws Exception {
        // As testGuardInvalidatesViewMovedToBaseWhileDropApplies(), with the DROP of count becoming visible once the
        // FULL refresh of the moved view fixed its base reader: the guard's check after the change finds the view
        // invalid with the reason of the base change and the FULL pending, records the switch and publishes its
        // invalidation, as for a view that a pending FULL rebuilds. The FULL rebuilds the view from base2 without the
        // DROP and commits refresh position 1, below the switch.
        assertMemoryLeak(pausingFf, () -> {
            createViewToMoveToBase2();
            raceColumnChange("base2", "ALTER TABLE base2 DROP COLUMN count", () -> {
                moveViewToBase2();
                execute("REFRESH MATERIALIZED VIEW mv FULL");
                final AtomicReference<String> stateAtCheck = new AtomicReference<>();
                final AtomicReference<String> stateAtUnlock = captureStateAtUnlock("mv");
                try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                    job.setOnBaseReaderSnapshotForTesting(() -> {
                        if (stateAtCheck.get() == null) {
                            final MatViewState state = getViewState("mv");
                            final String invalidBefore = "invalid=" + state.isInvalid() + ", reason=" + state.getInvalidationReasonForReport();
                            finishColumnChange();
                            stateAtCheck.set(invalidBefore + ", pendingReason=" + state.getPendingInvalidationReason());
                        }
                    });
                    drainMatViewQueue(job);
                }
                Assert.assertEquals(
                        "invalid=true, reason=" + MatViewRefreshJob.BASE_TABLE_CHANGED_REASON + ", pendingReason=" + COUNT_REASON,
                        stateAtCheck.get()
                );
                Assert.assertEquals(pendingGuardState("base2", 1, 2), stateAtUnlock.get());
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardInvalidatesInvalidViewRebuiltWhileDropApplies() throws Exception {
        // The view is invalid for another reason when the base drops count, so the bare-name guard records the switch
        // and keeps the view's reason. A FULL refresh then runs while the DROP applies: it rebuilds the view from a base
        // snapshot without the DROP, below the switch, and publishes the guard's invalidation, which the view never had.
        assertMemoryLeak(pausingFf, () -> {
            createRefreshedCountView();
            engine.getMatViewStateStore().enqueueInvalidate(engine.verifyTableName("mv"), "test invalidation");
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", "test invalidation", 1);
            raceColumnChange("base", DROP_COUNT, () -> {
                Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
                execute("REFRESH MATERIALIZED VIEW mv FULL");
                final AtomicReference<String> stateAtUnlock = captureStateAtUnlock("mv");
                drainMatViewQueue(engine);
                Assert.assertEquals(pendingGuardState("base", 1, 2), stateAtUnlock.get());
                drainWalQueue();
                assertViewState("mv", "invalid", COUNT_REASON, 1);
                assertViewRows("mv", COUNT_COLUMN_ROWS);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardInvalidatesNestedViewCreatedWhileDropApplies() throws Exception {
        // The view created and built while the DROP of count applies reads count bare in a nested select list. The
        // guard's check after the change finds the name there, as the check before it does, and invalidates the view.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            raceColumnChange("base", DROP_COUNT, () -> createViewAndBuild("mv", "REFRESH MANUAL", "SELECT * FROM (" + COUNT_VIEW_SQL + ")"));
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardInvalidatesNewViewAlteredAndBuiltWhileDropApplies() throws Exception {
        // As testGuardInvalidatesViewAlteredWhileDropApplies(), with a view that never completed a refresh: its first
        // build runs after the view's WAL installed the new SQL, still before the DROP of count is visible, and reads
        // count as the column.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            execute("CREATE MATERIALIZED VIEW mv REFRESH MANUAL DEFERRED AS (" + COUNT_CALL_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", -1);
            raceColumnChange("base", DROP_COUNT, () -> {
                execute("ALTER MATERIALIZED VIEW mv AS (" + COUNT_VIEW_SQL + ")");
                drainWalQueue();
                Assert.assertEquals(COUNT_VIEW_SQL, getViewState("mv").getViewDefinition().getMatViewSql());
                execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
                drainMatViewQueue(engine);
                drainWalQueue();
                assertViewState("mv", "valid", "", 1);
                assertViewRows("mv", COUNT_COLUMN_ROWS);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardInvalidatesOnlyViewsUsingTheName() throws Exception {
        // One apply batch carries an insert, the DROP and another insert. The guard invalidates the view that reads
        // version bare and leaves the batch's incremental notification alone, so the other views refresh every row.
        assertMemoryLeak(() -> {
            createBase();
            createView("versions", BARE_VERSION_VIEW_SQL);
            createView("price_1h", PRICE_VIEW_SQL);
            createView("amount_1h", AMOUNT_VIEW_SQL);
            insertBaseRows();
            drainWalAndMatViewQueues();

            execute("INSERT INTO base (sym, price, amount, version, ts) VALUES ('b', 5.5, 50.0, 3, '2024-01-01T02:10:00.000000Z')");
            execute("ALTER TABLE base DROP COLUMN version");
            execute("INSERT INTO base (sym, price, amount, ts) VALUES ('a', 6.5, 60.0, '2024-01-01T03:10:00.000000Z')");
            drainWalQueue();
            drainMatViewQueue(engine);
            drainWalQueue();

            assertViewInvalid("versions", VERSION_REASON);
            assertViewRows("versions", "ts, sym, version", BARE_VERSION_VIEW_ROWS);
            assertViewValid("price_1h");
            assertViewRows("price_1h", PRICE_VIEW_ROWS_AFTER_INSERT + "2024-01-01T03:00:00.000000Z\ta\t6.5\n");
            assertViewValid("amount_1h");
            assertViewRows("amount_1h", AMOUNT_VIEW_ROWS
                    + "2024-01-01T02:00:00.000000Z\tb\t50.0\n"
                    + "2024-01-01T03:00:00.000000Z\ta\t60.0\n");
            // the batch's last txn is 4: create 0, inserts 1 and 2, drop 3, insert 4
            assertRefreshBaseTxn("price_1h", 4);
            assertRefreshBaseTxn("amount_1h", 4);
        });
    }

    @Test
    public void testGuardInvalidatesRebuildWhoseSqlNoLongerParses() throws Exception {
        // The view's first build runs while the DROP of count applies, and commits a batch per bucket. After the first
        // batch the view's SQL stops parsing, as the test seam replaces it in place, and the build completes with the
        // factory it compiled. Its commit cannot tell whether the SQL still reads count bare, so it publishes the
        // guard's invalidation again, under the name the guard recorded.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, 1);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_INSERT_AS_SELECT_BATCH_SIZE, 1);
        assertMemoryLeak(pausingFf, () -> {
            createCountBaseWithView("REFRESH MANUAL DEFERRED");
            raceColumnChange("base", DROP_COUNT, () -> {
                drainMatViewQueue(engine);
                Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
                execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
                final MatViewDefinition definition = getViewState("mv").getViewDefinition();
                final AtomicBoolean isReplaced = new AtomicBoolean();
                final AtomicReference<String> stateAtUnlock = captureStateAtUnlock("mv");
                final LogCapture logCapture = new LogCapture();
                logCapture.start();
                try {
                    try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                        job.setOnBatchCommittedForTesting(() -> {
                            if (isReplaced.compareAndSet(false, true)) {
                                definition.setMatViewSqlForTesting("SELECT ts, sym, count, last(price) AS price FROM");
                            }
                        });
                        drainMatViewQueue(job);
                    }
                    Assert.assertTrue(isReplaced.get());
                    Assert.assertEquals(pendingGuardState("base", 1, 2), stateAtUnlock.get());
                    logCapture.waitForRegex("could not parse materialized view SQL to find its bare select-list names \\[view=mv~\\d+, error=table name or sub-query expected\\]");
                } finally {
                    logCapture.stop();
                }
                drainWalQueue();
                assertViewState("mv", "invalid", COUNT_REASON, 1);
                assertViewRows("mv", COUNT_COLUMN_ROWS);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardInvalidatesUpperCaseBareNameCreatedWhileDropApplies() throws Exception {
        // The view created and built while the DROP of count applies writes the bare name in upper case. The guard's
        // check after the change compares names ignoring case, and its reason names count as the view's SQL writes it.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            raceColumnChange(
                    "base",
                    DROP_COUNT,
                    () -> createViewAndBuild("mv", "REFRESH MANUAL", "SELECT ts, sym, COUNT, last(price) AS price FROM base SAMPLE BY 1h")
            );
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", MatViewBareNameGuard.invalidationReason("COUNT"), 1);
        });
    }

    @Test
    public void testGuardInvalidatesVersionColumnViewCreatedWhileDropApplies() throws Exception {
        // As testGuardInvalidatesViewCreatedAndBuiltWhileDropApplies(), with a VARCHAR base column named version, which
        // the view's SQL reads bare: once the base drops it, a FULL refresh calls version() in every bucket.
        assertMemoryLeak(pausingFf, () -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, version VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('a', 1.0, 'v1', '2024-01-01T00:10:00.000000Z'), ('b', 2.0, 'v2', '2024-01-01T01:10:00.000000Z')");
            drainWalQueue();
            raceColumnChange("base", "ALTER TABLE base DROP COLUMN version", () -> createViewAndBuild("mv", "REFRESH MANUAL", BARE_VERSION_VIEW_SQL));
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", VERSION_REASON, 1);
            assertViewRows("mv", """
                    ts\tsym\tversion\tprice
                    2024-01-01T00:00:00.000000Z\ta\tv1\t1.0
                    2024-01-01T01:00:00.000000Z\tb\tv2\t2.0
                    """);

            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 2);
            final String version = queryVersion();
            assertViewRows("mv", """
                    ts\tsym\tversion\tprice
                    2024-01-01T00:00:00.000000Z\ta\t%s\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t%s\t2.0
                    """.formatted(version, version));
            assertSqlCursors(
                    "SELECT ts, sym, version()::VARCHAR AS version, last(price) AS price FROM base SAMPLE BY 1h ORDER BY ts, sym",
                    "mv ORDER BY ts, sym"
            );
        });
    }

    @Test
    public void testGuardInvalidatesViewAfterFullRefreshRebuild() throws Exception {
        // A FULL refresh rebuilds an invalid view under one reading of its SQL, so it does not stop on the guard's
        // invalidation, not even when it compiles the SQL after the invalidation is published: here a view ADD
        // COLUMN that the FULL's truncate commit picks up makes it compile the SQL again. A guarded change that
        // applies after the FULL fixed its base reader is not in the rebuilt rows, and the view goes invalid with
        // the guard's reason once the FULL is done.
        assertMemoryLeak(() -> {
            createBase();
            final String sql = "SELECT ts, sym, version, last(amount) AS amount FROM base SAMPLE BY 1h";
            createView("versions", sql);
            insertBaseRows();
            drainWalAndMatViewQueues();
            execute("ALTER TABLE base RENAME COLUMN amount TO amount2");
            execute("INSERT INTO base (sym, price, amount2, version, ts) VALUES ('b', 5.5, 50.0, 3, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewInvalid("versions", "[" + sql.indexOf("amount") + "]: Invalid column: amount");
            execute("ALTER TABLE base RENAME COLUMN amount2 TO amount");
            drainWalAndMatViewQueues();

            final AtomicBoolean isFired = new AtomicBoolean();
            final LogCapture logCapture = new LogCapture();
            logCapture.start();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnBaseReaderSnapshotForTesting(() -> {
                    if (isFired.compareAndSet(false, true)) {
                        try {
                            execute("ALTER MATERIALIZED VIEW versions ADD COLUMN note INT");
                            execute("ALTER TABLE base DROP COLUMN version");
                        } catch (SqlException e) {
                            throw new RuntimeException(e);
                        }
                        drainWalQueue();
                        Assert.assertEquals(VERSION_REASON, getViewState("versions").getPendingInvalidationReason());
                    }
                });
                execute("REFRESH MATERIALIZED VIEW versions FULL");
                drainMatViewQueue(job);
                logCapture.waitForRegex("compiling materialized view \\[view=versions~\\d+, attempt=0\\]");
            } finally {
                logCapture.stop();
            }
            drainWalQueue();
            Assert.assertTrue(isFired.get());
            assertViewInvalid("versions", VERSION_REASON);
            // the FULL rebuilt every row from the base before the DROP
            assertViewRows("versions", "ts, sym, version", """
                    ts\tsym\tversion\tamount\tnote
                    2024-01-01T00:00:00.000000Z\ta\t1\t10.0\tnull
                    2024-01-01T00:00:00.000000Z\ta\t2\t30.0\tnull
                    2024-01-01T00:00:00.000000Z\tb\t1\t20.0\tnull
                    2024-01-01T01:00:00.000000Z\ta\t2\t40.0\tnull
                    2024-01-01T02:00:00.000000Z\tb\t3\t50.0\tnull
                    """);
        });
    }

    @Test
    public void testGuardInvalidatesViewAlteredWhileDropApplies() throws Exception {
        // ALTER MATERIALIZED VIEW ... AS gives the refreshed view, whose SQL calls count(), a SQL that reads count
        // bare, and the view's WAL installs it while the DROP of count applies, after the guard checked the call. The
        // guard's check after the change finds the new SQL and invalidates the view: it cannot tell on which side of
        // the change a refresh of the new SQL read the base. FULL rebuilds the view under the call reading.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            execute("CREATE MATERIALIZED VIEW mv REFRESH MANUAL AS (" + COUNT_CALL_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 1);
            assertViewRows("mv", COUNT_CALL_ROWS);
            raceColumnChange("base", DROP_COUNT, () -> {
                execute("ALTER MATERIALIZED VIEW mv AS (" + COUNT_VIEW_SQL + ")");
                drainWalQueue();
                Assert.assertEquals(COUNT_VIEW_SQL, getViewState("mv").getViewDefinition().getMatViewSql());
                assertViewState("mv", "valid", "", 1);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_CALL_ROWS);

            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 2);
            assertViewRows("mv", COUNT_CALL_ROWS);
            assertSqlCursors(COUNT_CALL_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardInvalidatesViewCreatedAndBuiltWhileDropApplies() throws Exception {
        // CREATE MATERIALIZED VIEW registers a view that reads count bare while the DROP of count applies, after the
        // guard checked the base's views, and the view's first build reads count as the column. The guard's check after
        // the change finds the view and invalidates it, so no refresh after the DROP, which would call count(), adds
        // its rows next to the build's. RESUME refuses the SQL once the base has no column count, and FULL rebuilds
        // the view under the call reading.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            raceColumnChange("base", DROP_COUNT, () -> {
                createViewAndBuild("mv", "REFRESH MANUAL", COUNT_VIEW_SQL);
                assertViewState("mv", "valid", "", 1);
                assertViewRows("mv", COUNT_COLUMN_ROWS);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);

            execute(INSERT_C_ROW);
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW mv INCREMENTAL", VIEW_NAME_POSITION, COUNT_INVALID_REFUSAL);
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW mv RESUME", VIEW_NAME_POSITION, COUNT_COMPILE_REFUSAL);
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);

            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 3);
            assertViewRows("mv", COUNT_CALL_ROWS + "2024-01-01T02:00:00.000000Z\tc\t1\t3.0\n");
            assertSqlCursors(COUNT_CALL_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardInvalidatesViewCreatedAndBuiltWhileRenameApplies() throws Exception {
        // As testGuardInvalidatesViewCreatedAndBuiltWhileDropApplies(), with a RENAME of count away from the bare name.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            raceColumnChange("base", "ALTER TABLE base RENAME COLUMN count TO cnt", () -> createViewAndBuild("mv", "REFRESH MANUAL", COUNT_VIEW_SQL));
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);

            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 2);
            assertViewRows("mv", COUNT_CALL_ROWS);
            assertSqlCursors(COUNT_CALL_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardInvalidatesViewCreatedWhileDropAppliesRefreshedBeforeCheck() throws Exception {
        // The view is created and built from base txn 1 while the DROP of count applies. The DROP becomes visible, and
        // an incremental refresh moves the view's refresh position to the DROP's txn 2 before the guard's check after
        // the change reads it. The refresh recomputes no rows, so they still read count as the column. The check cannot
        // tell such a view from one built after the change, and invalidates it.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            raceColumnChange("base", DROP_COUNT, () -> {
                createViewAndBuild("mv", "REFRESH MANUAL", COUNT_VIEW_SQL);
                runWhileGuardChecksAfterChange("mv", () -> {
                    execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
                    drainWalAndMatViewQueues();
                    assertViewState("mv", "valid", "", 2);
                    // the switch at the DROP's txn is not above the refresh position
                    Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
                    assertViewRows("mv", COUNT_COLUMN_ROWS);
                });
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 2);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardInvalidatesViewCreatedWhileDropAppliesWhoseBuildSpansIt() throws Exception {
        // The view created while the DROP of count applies fixes the base reader of its first build before the DROP is
        // visible, and the DROP becomes visible after the build's first batch commit. The guard's check after the
        // change records the switch and finds no refresh position, so it publishes nothing. The build reads count as
        // the column through its reader, commits refresh position 1, below the switch, and publishes the invalidation.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, 1);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_INSERT_AS_SELECT_BATCH_SIZE, 1);
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            raceColumnChange("base", DROP_COUNT, () -> {
                execute("CREATE MATERIALIZED VIEW mv REFRESH MANUAL DEFERRED AS (" + COUNT_VIEW_SQL + ") PARTITION BY DAY");
                drainWalQueue();
                execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
                final AtomicReference<String> stateAtCheck = new AtomicReference<>();
                final AtomicReference<String> stateAtUnlock = captureStateAtUnlock("mv");
                try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                    job.setOnBatchCommittedForTesting(() -> {
                        if (stateAtCheck.get() == null) {
                            finishColumnChange();
                            final MatViewState state = getViewState("mv");
                            stateAtCheck.set("refreshBaseTxn=" + state.getLastRefreshBaseTxn() + ", pendingReason=" + state.getPendingInvalidationReason());
                        }
                    });
                    drainMatViewQueue(job);
                }
                Assert.assertEquals("refreshBaseTxn=-1, pendingReason=null", stateAtCheck.get());
                Assert.assertEquals(pendingGuardState("base", 1, 2), stateAtUnlock.get());
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardInvalidatesViewCreatedWhileDropAppliesWhoseFullSpansIt() throws Exception {
        // As testGuardInvalidatesViewCreatedWhileDropAppliesWhoseBuildSpansIt(), with a FULL refresh in place of the
        // first build: the FULL fixes its base reader before the DROP of count is visible, and the DROP becomes visible
        // right after.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            raceColumnChange("base", DROP_COUNT, () -> {
                execute("CREATE MATERIALIZED VIEW mv REFRESH MANUAL DEFERRED AS (" + COUNT_VIEW_SQL + ") PARTITION BY DAY");
                drainWalQueue();
                execute("REFRESH MATERIALIZED VIEW mv FULL");
                final AtomicReference<String> stateAtCheck = new AtomicReference<>();
                final AtomicReference<String> stateAtUnlock = captureStateAtUnlock("mv");
                try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                    job.setOnBaseReaderSnapshotForTesting(() -> {
                        if (stateAtCheck.get() == null) {
                            finishColumnChange();
                            final MatViewState state = getViewState("mv");
                            stateAtCheck.set("refreshBaseTxn=" + state.getLastRefreshBaseTxn() + ", pendingReason=" + state.getPendingInvalidationReason());
                        }
                    });
                    drainMatViewQueue(job);
                }
                Assert.assertEquals("refreshBaseTxn=-1, pendingReason=null", stateAtCheck.get());
                Assert.assertEquals(pendingGuardState("base", 1, 2), stateAtUnlock.get());
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardInvalidatesViewCreatedWhileRenameSwitchesTwoNames() throws Exception {
        // A view created before the explicit-name rule reads count bare as the base column, and calls version without
        // parentheses. It is registered and built while the base renames count to version, which switches both names.
        // The guard's check after the change finds both names bare in the view's SQL, and its reason names the first
        // name of the change, the old one.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            final String version = queryVersion();
            final String rows = """
                    ts\tsym\tcount\tversion\tprice
                    2024-01-01T00:00:00.000000Z\ta\t7\t%s\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t8\t%s\t2.0
                    """.formatted(version, version);
            raceColumnChange("base", "ALTER TABLE base RENAME COLUMN count TO version", () -> {
                executeWithoutExplicitNamesCheck("CREATE MATERIALIZED VIEW mv REFRESH MANUAL AS (SELECT ts, sym, count, version, last(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
                drainMatViewQueue(engine);
                drainWalQueue();
                assertViewState("mv", "valid", "", 1);
                assertViewRows("mv", rows);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", rows);
        });
    }

    @Test
    public void testGuardInvalidatesViewMovedToBaseWhileDropApplies() throws Exception {
        // ALTER MATERIALIZED VIEW ... WITH BASE moves the view to base2 with a SQL that reads count bare while base2's
        // DROP of count applies, after the guard checked base2's views, and a FULL refresh rebuilds the moved view from
        // base2 without the DROP. The guard's check after the change finds the view among base2's views and invalidates
        // it. A FULL refresh after the DROP rebuilds the view under the call reading.
        assertMemoryLeak(pausingFf, () -> {
            createViewToMoveToBase2();
            raceColumnChange("base2", "ALTER TABLE base2 DROP COLUMN count", () -> {
                moveViewToBase2();
                execute("REFRESH MATERIALIZED VIEW mv FULL");
                drainWalAndMatViewQueues();
                drainWalAndMatViewQueues();
                assertViewState("mv", "valid", "", 1);
                assertViewRows("mv", COUNT_COLUMN_ROWS);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);

            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 2);
            assertViewRows("mv", COUNT_CALL_ROWS);
            assertSqlCursors(BASE2_COUNT_CALL_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardInvalidatesViewOfEmptyBaseCreatedWhileDropApplies() throws Exception {
        // The view is created while the DROP of count, the base's txn 1, applies, and its first build on the empty base
        // commits refresh position 0, below the switch. The guard's check after the change invalidates it.
        assertMemoryLeak(pausingFf, () -> {
            execute(CREATE_COUNT_BASE);
            raceColumnChange("base", DROP_COUNT, () -> {
                createViewAndBuild("mv", "REFRESH MANUAL", COUNT_VIEW_SQL);
                assertViewState("mv", "valid", "", 0);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 0);
            assertViewRows("mv", "ts\tsym\tcount\tprice\n");
        });
    }

    @Test
    public void testGuardInvalidatesViewOfReplacedBaseRebuiltWhileDropApplies() throws Exception {
        // The view's state holds the switch of count that the guard recorded for the dropped base table, and then the
        // one it records for the table that replaced it, whose DROP of count applies while a FULL refresh rebuilds the
        // view from its txn 1. The FULL's commit passes over the dropped table's switch, which is not the view's, to
        // the new table's switch at txn 2, above the refresh position, and publishes the guard's invalidation again.
        assertMemoryLeak(pausingFf, () -> {
            createViewRebuiltFromReplacedBase();
            raceColumnChange("base", DROP_COUNT, () -> {
                drainMatViewQueue(engine);
                drainWalQueue();
                assertViewState("mv", "invalid", COUNT_REASON, 1);
                execute("REFRESH MATERIALIZED VIEW mv FULL");
                final AtomicReference<String> stateAtUnlock = captureStateAtUnlock("mv");
                drainMatViewQueue(engine);
                Assert.assertEquals(pendingGuardState("base", 1, 2), stateAtUnlock.get());
                drainWalQueue();
                assertViewState("mv", "invalid", COUNT_REASON, 1);
                assertViewRows("mv", REPLACED_BASE_ROWS);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", REPLACED_BASE_ROWS);
        });
    }

    @Test
    public void testGuardInvalidatesViewRebuiltWhileBaseSuspendedAfterFailedDrop() throws Exception {
        // The bare-name guard records the switch of count and publishes its invalidation before the DROP applies, but the
        // apply fails and suspends the base's WAL at base txn 1. A FULL refresh and a first build meanwhile read count as
        // the column the base keeps and commit refresh position 1, below the switch, so both views end invalid with the
        // guard's reason: the commit cannot tell a change that failed from one that has yet to apply. Once RESUME WAL
        // skips the DROP, a FULL rebuilds the view from base txn 2, which no switch passes, and the view stays valid.
        final AtomicReference<String> failSuffix = new AtomicReference<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean removeQuiet(LPSZ name) {
                final String suffix = failSuffix.get();
                if (suffix != null && Utf8s.containsAscii(name, suffix)) {
                    return false;
                }
                return super.removeQuiet(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            createRefreshedCountView();
            execute("CREATE MATERIALIZED VIEW mv2 REFRESH MANUAL DEFERRED AS (" + COUNT_VIEW_SQL + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final TableToken baseToken = engine.verifyTableName("base");
            failSuffix.set(baseToken.getDirName() + Files.SEPARATOR + TableUtils.META_SWAP_FILE_NAME);
            execute(DROP_COUNT);
            drainWalAndMatViewQueues();
            failSuffix.set(null);
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(baseToken));
            Assert.assertEquals(1, appliedTxn(baseToken));
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            // the INVALIDATE handler declines the guard's invalidation of the never-refreshed view
            assertViewState("mv2", "valid", "", -1);

            execute("REFRESH MATERIALIZED VIEW mv FULL");
            execute("REFRESH MATERIALIZED VIEW mv2 INCREMENTAL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
            assertViewState("mv2", "invalid", COUNT_REASON, 1);
            assertViewRows("mv2", COUNT_COLUMN_ROWS);

            // the DROP is the base's txn 2, which the resume skips
            execute("ALTER TABLE base RESUME WAL FROM TXN 3");
            drainWalAndMatViewQueues();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(baseToken));
            Assert.assertEquals(2, appliedTxn(baseToken));
            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 2);
            Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
            assertViewRows("mv", COUNT_COLUMN_ROWS);
            assertSqlCursors(COUNT_VIEW_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardInvalidatesViewRebuiltWhileDropApplies() throws Exception {
        // The INVALIDATE handler marks the refreshed view invalid with the guard's reason while the DROP of count
        // applies, and a FULL refresh then rebuilds the view from a base snapshot without the DROP. The FULL commits
        // refresh position 1, below the switch the guard recorded, so it publishes the guard's invalidation again,
        // and the view does not stay valid with rows that later refreshes would read the other way.
        assertMemoryLeak(pausingFf, () -> {
            createRefreshedCountView();
            raceColumnChange("base", DROP_COUNT, () -> {
                drainMatViewQueue(engine);
                drainWalQueue();
                assertViewState("mv", "invalid", COUNT_REASON, 1);
                execute("REFRESH MATERIALIZED VIEW mv FULL");
                final AtomicReference<String> stateAtUnlock = captureStateAtUnlock("mv");
                drainMatViewQueue(engine);
                Assert.assertEquals(pendingGuardState("base", 1, 2), stateAtUnlock.get());
                drainWalQueue();
                assertViewState("mv", "invalid", COUNT_REASON, 1);
                assertViewRows("mv", COUNT_COLUMN_ROWS);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardInvalidatesViewRegisteredWhileAddApplies() throws Exception {
        // A view created before the explicit-name rule, whose SQL calls count without parentheses, is registered and
        // built while the base gains a column named count: its build calls count(). The guard's check after the change
        // invalidates it and records the switch, on which RESUME refuses the SQL, which now reads count as the column.
        // A FULL refresh rebuilds the view under the column reading.
        assertMemoryLeak(pausingFf, () -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES ('a', 1.0, '2024-01-01T00:10:00.000000Z'), ('b', 2.0, '2024-01-01T01:10:00.000000Z')");
            drainWalQueue();
            raceColumnChange("base", "ALTER TABLE base ADD COLUMN count LONG", () -> {
                executeWithoutExplicitNamesCheck("CREATE MATERIALIZED VIEW mv REFRESH MANUAL AS (" + COUNT_VIEW_SQL + ") PARTITION BY DAY");
                drainMatViewQueue(engine);
                drainWalQueue();
                assertViewState("mv", "valid", "", 1);
                assertViewRows("mv", COUNT_CALL_ROWS);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_CALL_ROWS);
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW mv RESUME", VIEW_NAME_POSITION, COUNT_SWITCH_REFUSAL);
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);

            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 2);
            assertViewRows("mv", """
                    ts\tsym\tcount\tprice
                    2024-01-01T00:00:00.000000Z\ta\tnull\t1.0
                    2024-01-01T01:00:00.000000Z\tb\tnull\t2.0
                    """);
            assertSqlCursors(COUNT_VIEW_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardInvalidatesViewWhoseFirstBuildRacedAdd() throws Exception {
        // The older view calls count without parentheses, and the base gains a column named count. The INVALIDATE
        // handler declines the guard's invalidation of the never-refreshed view, and the view's first build runs before
        // the ADD is visible: it calls count(), commits refresh position 1, below the switch, and publishes the guard's
        // invalidation again. A FULL refresh after the ADD rebuilds the view under the column reading.
        assertMemoryLeak(pausingFf, () -> {
            createOlderCountView();
            raceColumnChange("base", "ALTER TABLE base ADD COLUMN count LONG", () -> {
                drainMatViewQueue(engine);
                drainWalQueue();
                assertViewState("mv", "valid", "", -1);
                execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
                final AtomicReference<String> stateAtUnlock = captureStateAtUnlock("mv");
                drainMatViewQueue(engine);
                Assert.assertEquals(pendingGuardState("base", 1, 2), stateAtUnlock.get());
                drainWalQueue();
                assertViewState("mv", "invalid", COUNT_REASON, 1);
                assertViewRows("mv", COUNT_CALL_ROWS);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_CALL_ROWS);

            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 2);
            final String columnRows = """
                    ts\tsym\tcount\tprice
                    2024-01-01T00:00:00.000000Z\ta\tnull\t1.0
                    2024-01-01T01:00:00.000000Z\tb\tnull\t2.0
                    """;
            assertViewRows("mv", columnRows);
            execute("INSERT INTO base (sym, price, count, ts) VALUES ('c', 3.0, 9, '2024-01-01T02:10:00.000000Z')");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 3);
            assertViewRows("mv", columnRows + "2024-01-01T02:00:00.000000Z\tc\t9\t3.0\n");
            assertSqlCursors(COUNT_VIEW_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardInvalidatesViewWhoseFirstBuildRacedDrop() throws Exception {
        // The bare-name guard publishes its invalidation before the DROP of count applies, and the INVALIDATE handler
        // declines the view, which never completed a refresh. The view's first build then runs while the DROP applies:
        // its reader does not see the DROP, so it reads count as the column, and it commits refresh position 1, below
        // the DROP's base txn 2 that the guard recorded. The build publishes the guard's invalidation again, and the
        // view ends invalid with its rows under one reading, as when the build runs before the guard.
        assertMemoryLeak(pausingFf, () -> {
            createCountBaseWithView("REFRESH MANUAL DEFERRED");
            raceColumnChange("base", DROP_COUNT, () -> {
                drainMatViewQueue(engine);
                drainWalQueue();
                assertViewState("mv", "valid", "", -1);
                Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
                execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
                final AtomicReference<String> stateAtUnlock = captureStateAtUnlock("mv");
                drainMatViewQueue(engine);
                Assert.assertEquals(pendingGuardState("base", 1, 2), stateAtUnlock.get());
                drainWalQueue();
                assertViewState("mv", "invalid", COUNT_REASON, 1);
                assertViewRows("mv", COUNT_COLUMN_ROWS);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);

            // the rows the base receives later do not reach the invalid view
            execute(INSERT_C_ROW);
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "REFRESH MATERIALIZED VIEW mv INCREMENTAL",
                    VIEW_NAME_POSITION,
                    "materialized view is invalid, run REFRESH MATERIALIZED VIEW mv RESUME first [reason=" + COUNT_REASON + ']'
            );
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);

            // FULL rebuilds the view under the call reading
            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 3);
            assertViewRows("mv", COUNT_CALL_ROWS + "2024-01-01T02:00:00.000000Z\tc\t1\t3.0\n");
            assertSqlCursors(COUNT_CALL_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardInvalidatesViewWhoseFirstBuildRacedRename() throws Exception {
        // As testGuardInvalidatesViewWhoseFirstBuildRacedDrop(), with a RENAME of count away from the bare name.
        assertMemoryLeak(pausingFf, () -> {
            createCountBaseWithView("REFRESH MANUAL DEFERRED");
            raceColumnChange("base", "ALTER TABLE base RENAME COLUMN count TO cnt", () -> {
                drainMatViewQueue(engine);
                Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
                execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
                final AtomicReference<String> stateAtUnlock = captureStateAtUnlock("mv");
                drainMatViewQueue(engine);
                Assert.assertEquals(pendingGuardState("base", 1, 2), stateAtUnlock.get());
                drainWalQueue();
                assertViewState("mv", "invalid", COUNT_REASON, 1);
                assertViewRows("mv", COUNT_COLUMN_ROWS);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);

            execute(INSERT_C_ROW);
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 3);
            assertViewRows("mv", COUNT_CALL_ROWS + "2024-01-01T02:00:00.000000Z\tc\t1\t3.0\n");
            assertSqlCursors(COUNT_CALL_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardInvalidatesViewWhoseFirstBuildReaderPredatesAppliedDrop() throws Exception {
        // The view's first build fixes its base reader while the DROP of count applies, and the DROP becomes visible
        // after the build's first batch commit. The build reads the base through its reader, so it reads count as the
        // column and commits refresh position 1. The commit compares that position, not the base's applied txn, with
        // the switch, and publishes the guard's invalidation again.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, 1);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_INSERT_AS_SELECT_BATCH_SIZE, 1);
        assertMemoryLeak(pausingFf, () -> {
            createCountBaseWithView("REFRESH MANUAL DEFERRED");
            final TableToken baseToken = engine.verifyTableName("base");
            raceColumnChange("base", DROP_COUNT, () -> {
                drainMatViewQueue(engine);
                Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
                execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
                final AtomicLong appliedBaseTxn = new AtomicLong(-1);
                final AtomicReference<String> stateAtUnlock = captureStateAtUnlock("mv");
                try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                    job.setOnBatchCommittedForTesting(() -> {
                        if (appliedBaseTxn.get() == -1) {
                            finishColumnChange();
                            appliedBaseTxn.set(appliedTxn(baseToken));
                        }
                    });
                    drainMatViewQueue(job);
                }
                Assert.assertEquals(2, appliedBaseTxn.get());
                Assert.assertEquals(pendingGuardState("base", 1, 2), stateAtUnlock.get());
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardInvalidatesViewWhoseFullRacedDrop() throws Exception {
        // As testGuardInvalidatesViewWhoseFirstBuildRacedDrop(), with a FULL refresh in place of the first build: the
        // FULL rebuilds the view from a base snapshot without the DROP, and publishes the guard's invalidation again.
        assertMemoryLeak(pausingFf, () -> {
            createCountBaseWithView("REFRESH MANUAL DEFERRED");
            raceColumnChange("base", DROP_COUNT, () -> {
                drainMatViewQueue(engine);
                Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
                Assert.assertEquals(-1, getViewState("mv").getLastRefreshBaseTxn());
                execute("REFRESH MATERIALIZED VIEW mv FULL");
                final AtomicReference<String> stateAtUnlock = captureStateAtUnlock("mv");
                drainMatViewQueue(engine);
                Assert.assertEquals(pendingGuardState("base", 1, 2), stateAtUnlock.get());
                drainWalQueue();
                assertViewState("mv", "invalid", COUNT_REASON, 1);
                assertViewRows("mv", COUNT_COLUMN_ROWS);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardInvalidatesViewWhoseFullReaderPredatesAppliedDrop() throws Exception {
        // A FULL refresh fixes its base reader while the DROP of count applies, and the DROP becomes visible before the
        // FULL truncates the view. The FULL reads the base through its reader, so it reads count as the column and
        // commits refresh position 1. The commit compares that position, not the base's applied txn, with the switch,
        // and publishes the guard's invalidation again.
        assertMemoryLeak(pausingFf, () -> {
            createRefreshedCountView();
            final TableToken baseToken = engine.verifyTableName("base");
            raceColumnChange("base", DROP_COUNT, () -> {
                drainMatViewQueue(engine);
                drainWalQueue();
                assertViewState("mv", "invalid", COUNT_REASON, 1);
                execute("REFRESH MATERIALIZED VIEW mv FULL");
                final AtomicLong appliedBaseTxn = new AtomicLong(-1);
                final AtomicReference<String> stateAtUnlock = captureStateAtUnlock("mv");
                try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                    job.setOnBaseReaderSnapshotForTesting(() -> {
                        if (appliedBaseTxn.get() == -1) {
                            finishColumnChange();
                            appliedBaseTxn.set(appliedTxn(baseToken));
                        }
                    });
                    drainMatViewQueue(job);
                }
                Assert.assertEquals(2, appliedBaseTxn.get());
                Assert.assertEquals(pendingGuardState("base", 1, 2), stateAtUnlock.get());
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardInvalidatesViewWhoseParkedFullRanWhileDropApplies() throws Exception {
        // A FULL refresh requested right after the INVALIDATE handler marked the view invalid waits for the view's own
        // WAL. The WAL apply job redelivers it once that WAL applies, still before the DROP of count is visible, and the
        // FULL rebuilds the view below the switch and publishes the guard's invalidation again.
        assertMemoryLeak(pausingFf, () -> {
            createRefreshedCountView();
            raceColumnChange("base", DROP_COUNT, () -> {
                drainMatViewQueue(engine);
                Assert.assertTrue(getViewState("mv").isInvalid());
                execute("REFRESH MATERIALIZED VIEW mv FULL");
                drainMatViewQueue(engine);
                // the FULL waits for the view's WAL, which holds the invalid state
                Assert.assertTrue(getViewState("mv").hasPendingFullRefreshOwnerForTesting());
                drainWalQueue();
                final AtomicReference<String> stateAtUnlock = captureStateAtUnlock("mv");
                drainMatViewQueue(engine);
                Assert.assertEquals(pendingGuardState("base", 1, 2), stateAtUnlock.get());
                Assert.assertFalse(getViewState("mv").hasPendingFullRefreshOwnerForTesting());
                drainWalQueue();
                assertViewState("mv", "invalid", COUNT_REASON, 1);
                assertViewRows("mv", COUNT_COLUMN_ROWS);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardInvalidationKeepsUpdateRangeAcrossRestart() throws Exception {
        // As testGuardColumnChangeInLaterBatchKeepsUpdateRangeForResume(), with a restart after the bare-name guard's
        // invalidation: the view's state keeps the backlog with the UPDATE's range, and loses the switch the guard
        // recorded in memory. RESUME refuses the SQL, which reads version as a column the base no longer has, and once
        // ALTER ... AS fixes it, RESUME catches up on the UPDATE.
        assertMemoryLeak(() -> {
            createBase();
            createView("versions", BARE_VERSION_VIEW_SQL);
            insertBaseRows();
            drainWalAndMatViewQueues();
            execute("UPDATE base SET price = 9.0 WHERE sym = 'a'");
            drainWalQueue();
            execute("ALTER TABLE base RENAME COLUMN version TO ver");
            drainWalQueue();
            drainWalAndMatViewQueues();
            assertViewInvalid("versions", VERSION_REASON);

            restartViewStates();
            assertViewInvalid("versions", VERSION_REASON);
            final MatViewState state = getViewState("versions");
            Assert.assertTrue(state.isInvalid());
            Assert.assertNull(state.getBacklogLostReason());
            assertFirstDayCached(state, ColumnType.TIMESTAMP_MICRO, 3);
            assertExceptionNoLeakCheck(
                    "REFRESH MATERIALIZED VIEW versions RESUME",
                    VIEW_NAME_POSITION,
                    "materialized view SQL does not compile, fix it with ALTER MATERIALIZED VIEW ... AS first "
                            + "[view=versions, error=version is not a column; write version() to call the function]"
            );
            Assert.assertTrue(state.isInvalid());
            assertFirstDayCached(state, ColumnType.TIMESTAMP_MICRO, 3);

            assertResumeAfterFixCatchesUpOnUpdate();
        });
    }

    @Test
    public void testGuardInvalidationLosingLatchKeepsUpdateRangeForResume() throws Exception {
        // A base UPDATE and a RENAME of the bare name version apply in one WAL batch while a refresh holds the view
        // latch: the bare-name guard's invalidation cannot take the latch and stays pending, and the UPDATE's range
        // task, which finds the latch taken, goes back on the queue. Once the holder lets go, the task takes the range
        // into the still valid view's cache, the invalidation that the release wakes makes the cache the backlog, and
        // RESUME, once ALTER ... AS fixes the SQL, catches up on the UPDATE.
        assertMemoryLeak(() -> {
            createBase();
            createView("versions", BARE_VERSION_VIEW_SQL);
            insertBaseRows();
            drainWalAndMatViewQueues();
            execute("UPDATE base SET price = 9.0 WHERE sym = 'a'");
            execute("ALTER TABLE base RENAME COLUMN version TO ver");
            drainWalQueue();
            final TableToken viewToken = engine.verifyTableName("versions");
            final MatViewState state = getViewState("versions");
            final long viewSeqTxn = getSeqTxn(viewToken);
            Assert.assertTrue(state.tryLock());
            try {
                drainMatViewQueue(engine);
                Assert.assertFalse(state.isInvalid());
                Assert.assertEquals(VERSION_REASON, state.getPendingInvalidationReason());
                Assert.assertEquals(0, state.getRefreshIntervals().size());
                Assert.assertEquals(viewSeqTxn, getSeqTxn(viewToken));
            } finally {
                MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken, state, false);
            }
            drainWalAndMatViewQueues();
            assertViewInvalid("versions", VERSION_REASON);
            // the range task's view state event, then the invalidation's
            Assert.assertEquals(viewSeqTxn + 2, getSeqTxn(viewToken));
            Assert.assertNull(state.getBacklogLostReason());
            assertFirstDayCached(state, ColumnType.TIMESTAMP_MICRO, 3);

            assertResumeAfterFixCatchesUpOnUpdate();
        });
    }

    @Test
    public void testGuardInvalidationOfRaceIsCoveredByLaterFull() throws Exception {
        // A FULL refresh fixes its base reader while the DROP of count applies, the DROP becomes visible, and the FULL
        // publishes the guard's invalidation of the DROP's base txn 2 again when it commits refresh position 1. A second
        // FULL, requested meanwhile, runs before the INVALIDATE task that the first one's release enqueues: it rebuilds
        // the view from base txn 2, which covers the invalidation, and the INVALIDATE task finds it covered.
        assertMemoryLeak(pausingFf, () -> {
            createRefreshedCountView();
            raceColumnChange("base", DROP_COUNT, () -> {
                drainMatViewQueue(engine);
                drainWalQueue();
                assertViewState("mv", "invalid", COUNT_REASON, 1);
                execute("REFRESH MATERIALIZED VIEW mv FULL");
                final AtomicBoolean isApplied = new AtomicBoolean();
                final AtomicReference<String> stateAtUnlock = captureStateAtUnlock("mv");
                try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                    job.setOnBaseReaderSnapshotForTesting(() -> {
                        if (isApplied.compareAndSet(false, true)) {
                            finishColumnChange();
                            try {
                                execute("REFRESH MATERIALIZED VIEW mv FULL");
                            } catch (SqlException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
                    drainMatViewQueue(job);
                }
                Assert.assertTrue(isApplied.get());
                Assert.assertEquals(pendingGuardState("base", 1, 2), stateAtUnlock.get());
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 2);
            Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
            assertViewRows("mv", COUNT_CALL_ROWS);
            assertSqlCursors(COUNT_CALL_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardInvalidationOfRaceSurvivesFailedPublication() throws Exception {
        // The view's first build runs while the DROP of count applies, and the publication of the guard's invalidation
        // that its commit makes fails on the heap. The refresh fails, which leaves the view invalid with its rows under
        // one reading. A FULL refresh after the DROP rebuilds the view under the call reading.
        assertMemoryLeak(pausingFf, () -> {
            createCountBaseWithView("REFRESH MANUAL DEFERRED");
            raceColumnChange("base", DROP_COUNT, () -> {
                drainMatViewQueue(engine);
                Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
                execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
                getViewState("mv").setOnPendingInvalidationMarkerReadForTesting(() -> {
                    throw new OutOfMemoryError("guard republish");
                });
                drainMatViewQueue(engine);
                drainWalQueue();
                assertViewState("mv", "invalid", "guard republish", 1);
                Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
                assertViewRows("mv", COUNT_COLUMN_ROWS);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", "guard republish", 1);

            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 2);
            assertViewRows("mv", COUNT_CALL_ROWS);
            assertSqlCursors(COUNT_CALL_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardInvalidationOfRaceSurvivesRestart() throws Exception {
        // The view's first build races the DROP of count and leaves the view invalid with the guard's reason, which the
        // view's state keeps across a restart that loses the switch the guard recorded in memory.
        assertMemoryLeak(pausingFf, () -> {
            createCountBaseWithView("REFRESH MANUAL DEFERRED");
            raceColumnChange("base", DROP_COUNT, () -> {
                drainMatViewQueue(engine);
                execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
                drainMatViewQueue(engine);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            restartViewStates();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
            execute(INSERT_C_ROW);
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
        });
    }

    @Test
    public void testGuardInvalidationOfRacedAddRefusesResumeAfterRestart() throws Exception {
        // The older view's first build races the ADD of a column named count and leaves the view invalid with the
        // guard's reason. A restart loses the switch the guard recorded in memory, and RESUME, which compiles count as
        // the new column, refuses on the guard's reason that the view's state keeps.
        assertMemoryLeak(pausingFf, () -> {
            createOlderCountView();
            raceColumnChange("base", "ALTER TABLE base ADD COLUMN count LONG", () -> {
                drainMatViewQueue(engine);
                execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
                drainMatViewQueue(engine);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            restartViewStates();
            final String refusal = "materialized view SQL reads a bare name whose meaning a base column change switched, fix it with "
                    + "ALTER MATERIALIZED VIEW ... AS first [view=mv, error=" + COUNT_REASON + ']';
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW mv RESUME", VIEW_NAME_POSITION, refusal);
            assertExceptionNoLeakCheck("REFRESH MATERIALIZED VIEW mv RESUME SKIP BACKLOG", VIEW_NAME_POSITION, refusal);
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            assertViewRows("mv", COUNT_CALL_ROWS);
        });
    }

    @Test
    public void testGuardInvalidationOfRacedFullSurvivesFailedPublication() throws Exception {
        // As testGuardInvalidationOfRaceSurvivesFailedPublication(), with a FULL refresh of the view that the INVALIDATE
        // handler marked invalid while the DROP of count applies.
        assertMemoryLeak(pausingFf, () -> {
            createRefreshedCountView();
            raceColumnChange("base", DROP_COUNT, () -> {
                drainMatViewQueue(engine);
                drainWalQueue();
                assertViewState("mv", "invalid", COUNT_REASON, 1);
                execute("REFRESH MATERIALIZED VIEW mv FULL");
                getViewState("mv").setOnPendingInvalidationMarkerReadForTesting(() -> {
                    throw new OutOfMemoryError("guard republish");
                });
                drainMatViewQueue(engine);
                drainWalQueue();
                assertViewState("mv", "invalid", "guard republish", 1);
                Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
                assertViewRows("mv", COUNT_COLUMN_ROWS);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", "guard republish", 1);

            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 2);
            assertViewRows("mv", COUNT_CALL_ROWS);
            assertSqlCursors(COUNT_CALL_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardKeepsPassthroughViewCreatedWhileDropApplies() throws Exception {
        // A passthrough view's refresh reads every bare name as a column, so the guard's check after the DROP of count
        // leaves the passthrough view created while the DROP applies to its next refresh, whose compile fails.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            final String sql = "SELECT ts, sym, count FROM base";
            raceColumnChange("base", DROP_COUNT, () -> {
                createViewAndBuild("mv", "REFRESH IMMEDIATE", sql);
                Assert.assertTrue(getViewState("mv").getViewDefinition().isPassthrough());
            });
            Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
            drainWalAndMatViewQueues();
            assertViewValid("mv");

            execute(INSERT_C_ROW);
            drainWalAndMatViewQueues();
            assertViewInvalid("mv", "[" + sql.indexOf("count") + "]: Invalid column: count");
            assertViewRows("mv", """
                    ts\tsym\tcount
                    2024-01-01T00:10:00.000000Z\ta\t7
                    2024-01-01T01:10:00.000000Z\tb\t8
                    """);
        });
    }

    @Test
    public void testGuardKeepsReasonOfInvalidView() throws Exception {
        // An invalidation that reaches an invalid view is dropped, the guard's included, so the view keeps the
        // reason it went invalid with.
        assertMemoryLeak(() -> {
            createBase();
            final String sql = "SELECT ts, sym, version, last(amount) AS amount FROM base SAMPLE BY 1h";
            createView("versions", sql);
            insertBaseRows();
            drainWalAndMatViewQueues();
            execute("ALTER TABLE base DROP COLUMN amount");
            execute("INSERT INTO base (sym, price, version, ts) VALUES ('b', 5.5, 3, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            final String reason = "[" + sql.indexOf("amount") + "]: Invalid column: amount";
            assertViewInvalid("versions", reason);

            execute("ALTER TABLE base DROP COLUMN version");
            drainWalQueue();
            Assert.assertFalse(getViewState("versions").hasPendingInvalidationReason());
            drainWalAndMatViewQueues();
            assertViewInvalid("versions", reason);
        });
    }

    @Test
    public void testGuardKeepsViewAlteredToCallWhileDropApplies() throws Exception {
        // While the DROP of count applies, ALTER ... AS writes count() as a call, and a FULL refresh rebuilds the view
        // from a base snapshot without the DROP. The view's SQL no longer reads count bare, so the switch the guard
        // recorded is not the view's, and the view stays valid under the one reading of its new SQL.
        assertMemoryLeak(pausingFf, () -> {
            createRefreshedCountView();
            raceColumnChange("base", DROP_COUNT, () -> {
                drainMatViewQueue(engine);
                drainWalQueue();
                assertViewState("mv", "invalid", COUNT_REASON, 1);
                execute("ALTER MATERIALIZED VIEW mv AS (" + COUNT_CALL_SQL + ")");
                drainWalQueue();
                execute("REFRESH MATERIALIZED VIEW mv FULL");
                drainMatViewQueue(engine);
                drainWalQueue();
                assertViewState("mv", "valid", "", 1);
                Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
                assertViewRows("mv", COUNT_CALL_ROWS);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 1);
            execute(INSERT_C_ROW);
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 3);
            assertViewRows("mv", COUNT_CALL_ROWS + "2024-01-01T02:00:00.000000Z\tc\t1\t3.0\n");
            assertSqlCursors(COUNT_CALL_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardKeepsViewCreatedWhileDropAppliesBuiltAfter() throws Exception {
        // The view created while the DROP of count applies has its first build queued, and the build runs once the DROP
        // is visible, from base txn 2. The guard's check after the change records the switch, and publishes nothing to
        // the view without a refresh position, so the build runs and calls count() in every bucket.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            raceColumnChange("base", DROP_COUNT, () -> {
                execute("CREATE MATERIALIZED VIEW mv REFRESH MANUAL AS (" + COUNT_VIEW_SQL + ") PARTITION BY DAY");
                drainWalQueue();
                assertViewState("mv", "valid", "", -1);
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 2);
            Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
            assertViewRows("mv", COUNT_CALL_ROWS);

            execute("INSERT INTO base (sym, price, ts) VALUES ('a', 5.0, '2024-01-01T02:10:00.000000Z'), ('a', 6.0, '2024-01-01T02:20:00.000000Z')");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 3);
            assertViewRows("mv", COUNT_CALL_ROWS + "2024-01-01T02:00:00.000000Z\ta\t2\t6.0\n");
            assertSqlCursors(COUNT_CALL_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardKeepsViewCreatedWhileDropFailsToApply() throws Exception {
        // The view is created and built from base txn 1 while the DROP of count applies, after the guard checked the
        // base's views, and the apply then fails and suspends the base's WAL. The guard checks no view after a change
        // that did not apply, so the view stays valid. RESUME WAL then skips the DROP, the base keeps the column count,
        // and the view keeps refreshing with rows that read it. No row inserted after the resume could hold a count:
        // the base's writes follow its sequencer, which has dropped the column.
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            final TableToken baseToken = engine.verifyTableName("base");
            removalFailSuffix.set(baseToken.getDirName() + Files.SEPARATOR + TableUtils.META_SWAP_FILE_NAME);
            raceColumnChange("base", DROP_COUNT, () -> createViewAndBuild("mv", "REFRESH MANUAL", COUNT_VIEW_SQL));
            removalFailSuffix.set(null);
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(baseToken));
            Assert.assertEquals(1, appliedTxn(baseToken));
            Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 1);
            assertViewRows("mv", COUNT_COLUMN_ROWS);

            // the DROP is the base's txn 2, which the resume skips, and the refresh moves past it with nothing to
            // recompute
            execute("ALTER TABLE base RESUME WAL FROM TXN 3");
            drainWalAndMatViewQueues();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(baseToken));
            Assert.assertEquals(2, appliedTxn(baseToken));
            execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 2);
            assertViewRows("mv", COUNT_COLUMN_ROWS);
            assertSqlCursors(COUNT_VIEW_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardKeepsViewCreatedWithCallWhileDropApplies() throws Exception {
        // The view created and built while the DROP of count applies calls count() and reads no name bare, so the
        // guard's check after the change leaves it valid, and its refreshes keep calling count().
        assertMemoryLeak(pausingFf, () -> {
            createCountBase();
            raceColumnChange("base", DROP_COUNT, () -> createViewAndBuild("mv", "REFRESH MANUAL", COUNT_CALL_SQL));
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 1);
            Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
            assertViewRows("mv", COUNT_CALL_ROWS);

            execute(INSERT_C_ROW);
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 3);
            assertViewRows("mv", COUNT_CALL_ROWS + "2024-01-01T02:00:00.000000Z\tc\t1\t3.0\n");
            assertSqlCursors(COUNT_CALL_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardKeepsViewRebuiltFromReplacedBase() throws Exception {
        // The guard recorded the switch of count on the dropped base table. A FULL refresh rebuilds the view from the
        // table that replaced it, at its txn 1, below the switch's txn 2: the switch belongs to another table, so the
        // view stays valid and reads count as the new table's column.
        assertMemoryLeak(() -> {
            createViewRebuiltFromReplacedBase();
            execute("INSERT INTO base VALUES ('e', 5.0, 6, '2024-01-01T04:10:00.000000Z')");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 2);
            assertViewRows("mv", REPLACED_BASE_ROWS + "2024-01-01T04:00:00.000000Z\te\t6\t5.0\n");
            assertSqlCursors(COUNT_VIEW_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardKeepsViewWhoseFirstBuildFollowsDrop() throws Exception {
        // The INVALIDATE handler declines the guard's invalidation of a view that never completed a refresh. The view's
        // first build runs once the DROP of count applied, from base txn 2, the DROP's own, which the switch does not
        // pass: the build calls count() in every bucket, and the view stays valid.
        assertMemoryLeak(() -> {
            createCountBaseWithView("REFRESH MANUAL DEFERRED");
            execute(DROP_COUNT);
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", -1);
            execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 2);
            Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
            assertViewRows("mv", COUNT_CALL_ROWS);
            execute(INSERT_C_ROW);
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 3);
            assertViewRows("mv", COUNT_CALL_ROWS + "2024-01-01T02:00:00.000000Z\tc\t1\t3.0\n");
            assertSqlCursors(COUNT_CALL_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardKeepsViewWhoseFullFollowsDrop() throws Exception {
        // A FULL refresh rebuilds the view that the guard invalidated from base txn 2, the DROP's own, which the switch
        // does not pass: the FULL calls count() in every bucket, and the view stays valid.
        assertMemoryLeak(() -> {
            createRefreshedCountView();
            execute(DROP_COUNT);
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 1);
            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 2);
            Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
            assertViewRows("mv", COUNT_CALL_ROWS);
            execute(INSERT_C_ROW);
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewState("mv", "valid", "", 3);
            assertViewRows("mv", COUNT_CALL_ROWS + "2024-01-01T02:00:00.000000Z\tc\t1\t3.0\n");
            assertSqlCursors(COUNT_CALL_SQL + " ORDER BY ts, sym", "mv ORDER BY ts, sym");
        });
    }

    @Test
    public void testGuardLeavesUnparsableSqlToNextRefresh() throws Exception {
        // When the view's SQL does not parse, the guard cannot tell whether it uses the name bare and leaves the
        // view to its next refresh, whose compile fails on the same error. The refresh caches its compiled factory
        // with the SQL text it was compiled from, so the refresh that follows the DROP compiles the replaced text.
        assertMemoryLeak(() -> {
            createBase();
            createView("versions", BARE_VERSION_VIEW_SQL);
            insertBaseRows();
            drainWalAndMatViewQueues();
            final MatViewDefinition definition = getViewState("versions").getViewDefinition();
            final String brokenSql = "SELECT ts, sym, version, last(price) AS price FROM";
            definition.setMatViewSqlForTesting(brokenSql);

            final LogCapture logCapture = new LogCapture();
            logCapture.start();
            try {
                execute("ALTER TABLE base DROP COLUMN version");
                drainWalQueue();
                logCapture.waitForRegex("could not parse materialized view SQL to find its bare select-list names \\[view=versions~\\d+, error=table name or sub-query expected\\]");
            } finally {
                logCapture.stop();
            }
            Assert.assertFalse(getViewState("versions").hasPendingInvalidationReason());
            drainWalAndMatViewQueues();
            assertViewInvalid("versions", "[" + brokenSql.length() + "]: table name or sub-query expected");
            assertViewRows("versions", "ts, sym, version", BARE_VERSION_VIEW_ROWS);

            execute("INSERT INTO base (sym, price, amount, ts) VALUES ('b', 5.5, 50.0, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewInvalid("versions", "[" + brokenSql.length() + "]: table name or sub-query expected");
            assertViewRows("versions", "ts, sym, version", BARE_VERSION_VIEW_ROWS);
        });
    }

    @Test
    public void testGuardRaceLeavesBaseNotificationIdle() throws Exception {
        // The base commits an insert, base txn 2, and the DROP of count, base txn 3, pauses. The IMMEDIATE view's first
        // build runs from a base notification meanwhile: it reads the base at txn 2 under the column reading, commits
        // refresh position 2 and publishes the guard's invalidation again, whose delivery marks the view invalid. The
        // next base notification's pass skips the view and writes nothing, so the refresh job goes idle while the DROP
        // applies. The insert applies before the DROP's apply run: a data transaction that the thread of the paused
        // apply applies leaves a native path of that short-lived thread behind.
        assertMemoryLeak(pausingFf, () -> {
            createCountBaseWithView("REFRESH IMMEDIATE DEFERRED");
            execute("INSERT INTO base VALUES ('c', 3.0, 9, '2024-01-01T02:10:00.000000Z')");
            drainWalQueue();
            final String rows = COUNT_COLUMN_ROWS + "2024-01-01T02:00:00.000000Z\tc\t9\t3.0\n";
            final TableToken viewToken = engine.verifyTableName("mv");
            raceColumnChange("base", DROP_COUNT, () -> {
                // the insert's notification skips the view, whose guard invalidation is pending, and the INVALIDATE
                // handler declines the never-refreshed view
                drainMatViewQueue(engine);
                Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
                Assert.assertEquals(-1, getViewState("mv").getLastRefreshBaseTxn());
                final MatViewRefreshTask task = new MatViewRefreshTask();
                task.operation = MatViewRefreshTask.INCREMENTAL_REFRESH;
                task.baseTableToken = engine.verifyTableName("base");
                engine.notifyMatViewBaseTableCommit(task, 2);
                final AtomicReference<String> stateAtUnlock = captureStateAtUnlock("mv");
                runRefreshJobUntilIdle();
                Assert.assertEquals(pendingGuardState("base", 2, 3), stateAtUnlock.get());
                drainWalQueue();
                assertViewState("mv", "invalid", COUNT_REASON, 2);
                assertViewRows("mv", rows);

                final long viewSeqTxn = getSeqTxn(viewToken);
                engine.notifyMatViewBaseTableCommit(task, 2);
                runRefreshJobUntilIdle();
                Assert.assertEquals(viewSeqTxn, getSeqTxn(viewToken));
            });
            drainWalAndMatViewQueues();
            assertViewState("mv", "invalid", COUNT_REASON, 2);
            assertViewRows("mv", rows);
        });
    }

    @Test
    public void testGuardRenameColumnInvalidatesView() throws Exception {
        // A RENAME away from a bare name turns the name into a call, and a RENAME to it turns a call into a column
        // read: the guard checks both names of the change.
        assertMemoryLeak(() -> {
            createBase();
            createView("versions", BARE_VERSION_VIEW_SQL);
            createView("price_1h", PRICE_VIEW_SQL);
            execute("CREATE TABLE base2 (sym SYMBOL, price DOUBLE, release INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // created before the explicit name rules: version is no column of base2, so the view calls version()
            executeWithoutExplicitNamesCheck("CREATE MATERIALIZED VIEW calls AS (SELECT ts, sym, version, last(price) AS price FROM base2 SAMPLE BY 1h) PARTITION BY DAY");
            insertBaseRows();
            execute("INSERT INTO base2 VALUES ('a', 1.5, 7, '2024-01-01T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            final String callRows = """
                    ts\tsym\tversion\tprice
                    2024-01-01T00:00:00.000000Z\ta\t%s\t1.5
                    """.formatted(queryVersion());
            assertViewRows("calls", callRows);

            execute("ALTER TABLE base RENAME COLUMN version TO release");
            execute("ALTER TABLE base2 RENAME COLUMN release TO version");
            drainWalQueue();
            Assert.assertEquals(VERSION_REASON, getViewState("versions").getPendingInvalidationReason());
            Assert.assertEquals(VERSION_REASON, getViewState("calls").getPendingInvalidationReason());
            Assert.assertFalse(getViewState("price_1h").hasPendingInvalidationReason());
            drainWalAndMatViewQueues();
            assertViewInvalid("versions", VERSION_REASON);
            assertViewRows("versions", "ts, sym, version", BARE_VERSION_VIEW_ROWS);
            assertViewInvalid("calls", VERSION_REASON);
            assertViewRows("calls", callRows);
            assertViewValid("price_1h");

            execute("INSERT INTO base (sym, price, release, ts) VALUES ('b', 5.5, 3, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewRows("price_1h", PRICE_VIEW_ROWS_AFTER_INSERT);
            assertViewRows("versions", "ts, sym, version", BARE_VERSION_VIEW_ROWS);
        });
    }

    @Test
    public void testGuardSkipsLiveViewsAndViewsWithoutState() throws Exception {
        // A live view on the base is not the guard's: live views keep their own narrow check. A view whose state is
        // gone, as during a concurrent DROP MATERIALIZED VIEW, has nothing to invalidate. The base change applies.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT ts, sym, count(*) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE price > 0");
            createView("versions", BARE_VERSION_VIEW_SQL);
            createView("versions2", BARE_VERSION_VIEW_SQL);
            insertBaseRows();
            drainWalAndMatViewQueues();
            final TableToken viewToken = engine.verifyTableName("versions2");
            engine.getMatViewStateStore().removeViewState(viewToken);

            execute("ALTER TABLE base DROP COLUMN version");
            drainWalAndMatViewQueues();
            assertViewInvalid("versions", VERSION_REASON);
            assertColumns("base", """
                    column\ttype
                    sym\tSYMBOL
                    price\tDOUBLE
                    amount\tDOUBLE
                    ts\tTIMESTAMP
                    """);
            Assert.assertFalse(engine.getLiveViewRegistry().getViewInstance("lv").isInvalid());
            execute("DROP MATERIALIZED VIEW versions2");
            drainWalQueue();
            Assert.assertNull(engine.getTableTokenIfExists("versions2"));
        });
    }

    @Test
    public void testGuardSkipsPassthroughView() throws Exception {
        // A passthrough view's refresh reads every bare name as a column, so no column change switches one: the
        // DROP of a column it reads leaves it to its next refresh, whose compile fails.
        assertMemoryLeak(() -> {
            createBase();
            final String sql = "SELECT ts, sym, version FROM base";
            createView("copy", sql);
            insertBaseRows();
            drainWalAndMatViewQueues();
            Assert.assertTrue(getViewState("copy").getViewDefinition().isPassthrough());

            execute("ALTER TABLE base DROP COLUMN version");
            drainWalQueue();
            Assert.assertFalse(getViewState("copy").hasPendingInvalidationReason());
            drainWalAndMatViewQueues();
            assertViewValid("copy");

            execute("INSERT INTO base (sym, price, amount, ts) VALUES ('b', 5.5, 50.0, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewInvalid("copy", "[" + sql.indexOf("version") + "]: Invalid column: version");
        });
    }

    @Test
    public void testGuardStopsIncrementalRefreshBeforeCompile() throws Exception {
        // The refresh fixes its base reader after an unrelated ADD COLUMN, so the factory it cached at the previous
        // refresh no longer opens on it, and insertAsSelect() compiles the view's SQL again. The DROP applies
        // between the reader's snapshot and that compile: the refresh stops before the compile and writes nothing,
        // although the compile, which reads the base through the older reader, would still read version as a column.
        assertMemoryLeak(() -> {
            createBase();
            createView("versions", BARE_VERSION_VIEW_SQL);
            insertBaseRows();
            drainWalAndMatViewQueues();
            execute("ALTER TABLE base ADD COLUMN extra INT");
            execute("INSERT INTO base (sym, price, amount, version, ts) VALUES ('b', 5.5, 50.0, 3, '2024-01-01T02:10:00.000000Z')");
            drainWalQueue();

            final AtomicBoolean isDropApplied = new AtomicBoolean();
            final LogCapture logCapture = new LogCapture();
            logCapture.start();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnBeforeCompileForTesting(() -> {
                    if (isDropApplied.compareAndSet(false, true)) {
                        try {
                            execute("ALTER TABLE base DROP COLUMN version");
                        } catch (SqlException e) {
                            throw new RuntimeException(e);
                        }
                        drainWalQueue();
                    }
                });
                drainMatViewQueue(job);
                logCapture.waitForRegex("materialized view invalidation is pending, ending refresh without writing \\[view=versions~\\d+, stage=compile\\]");
            } finally {
                logCapture.stop();
            }
            drainWalQueue();
            Assert.assertTrue(isDropApplied.get());
            assertViewInvalid("versions", VERSION_REASON);
            assertViewRows("versions", "ts, sym, version", BARE_VERSION_VIEW_ROWS);
            // the refresh position stayed at the first insert, base txn 1
            assertRefreshBaseTxn("versions", 1);
        });
    }

    @Test
    public void testGuardStopsQueuedIncrementalRefresh() throws Exception {
        // An incremental refresh queued before the DROP takes the latch, and the DROP applies before it fixes its
        // base reader. The reader sees the change, so the refresh ends without writing, neither rows nor its
        // refresh position, and the view goes invalid with the guard's reason.
        for (boolean hasBaseRows : new boolean[]{true, false}) {
            assertMemoryLeak(() -> {
                createBase();
                createView("versions", BARE_VERSION_VIEW_SQL);
                insertBaseRows();
                drainWalAndMatViewQueues();
                final TableToken viewToken = engine.verifyTableName("versions");
                final long viewSeqTxn = getSeqTxn(viewToken);
                if (hasBaseRows) {
                    execute("INSERT INTO base (sym, price, amount, version, ts) VALUES ('b', 5.5, 50.0, 3, '2024-01-01T02:10:00.000000Z')");
                    drainWalQueue();
                } else {
                    engine.getMatViewStateStore().enqueueIncrementalRefresh(viewToken);
                }

                final AtomicBoolean isDropApplied = new AtomicBoolean();
                final LogCapture logCapture = new LogCapture();
                logCapture.start();
                try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                    job.setOnHoldingLockForTesting(() -> {
                        if (isDropApplied.compareAndSet(false, true)) {
                            try {
                                execute("ALTER TABLE base DROP COLUMN version");
                            } catch (SqlException e) {
                                throw new RuntimeException(e);
                            }
                            drainWalQueue();
                        }
                    });
                    drainMatViewQueue(job);
                    logCapture.waitForRegex("materialized view invalidation is pending, ending refresh without writing \\[view=versions~\\d+, stage=reader\\]");
                } finally {
                    logCapture.stop();
                }
                drainWalQueue();
                Assert.assertTrue(isDropApplied.get());
                assertViewInvalid("versions", VERSION_REASON);
                assertViewRows("versions", "ts, sym, version", BARE_VERSION_VIEW_ROWS);
                Assert.assertEquals(viewSeqTxn + 1, getSeqTxn(viewToken));
                assertRefreshBaseTxn("versions", 1);
                execute("DROP TABLE base");
                execute("DROP MATERIALIZED VIEW versions");
                drainWalQueue();
            });
        }
    }

    @Test
    public void testGuardStopsRangeRefresh() throws Exception {
        // A user REFRESH ... RANGE that takes the latch before the DROP applies and fixes its base reader after it
        // ends without writing, as an incremental refresh does.
        assertMemoryLeak(() -> {
            createBase();
            createView("versions", BARE_VERSION_VIEW_SQL);
            insertBaseRows();
            drainWalAndMatViewQueues();
            final TableToken viewToken = engine.verifyTableName("versions");
            final long viewSeqTxn = getSeqTxn(viewToken);

            final AtomicBoolean isDropApplied = new AtomicBoolean();
            final LogCapture logCapture = new LogCapture();
            logCapture.start();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (isDropApplied.compareAndSet(false, true)) {
                        try {
                            execute("ALTER TABLE base DROP COLUMN version");
                        } catch (SqlException e) {
                            throw new RuntimeException(e);
                        }
                        drainWalQueue();
                    }
                });
                execute("REFRESH MATERIALIZED VIEW versions RANGE FROM '2024-01-01T00:00:00.000000Z' TO '2024-01-01T02:00:00.000000Z'");
                drainMatViewQueue(job);
                logCapture.waitForRegex("materialized view invalidation is pending, ending refresh without writing \\[view=versions~\\d+, stage=reader\\]");
            } finally {
                logCapture.stop();
            }
            drainWalQueue();
            Assert.assertTrue(isDropApplied.get());
            assertViewInvalid("versions", VERSION_REASON);
            assertViewRows("versions", "ts, sym, version", BARE_VERSION_VIEW_ROWS);
            Assert.assertEquals(viewSeqTxn + 1, getSeqTxn(viewToken));
        });
    }

    @Test
    public void testGuardSurvivesFailedInvalidationEnqueue() throws Exception {
        // The guard sets the view's marker before it enqueues the INVALIDATE task. When the enqueue fails, the
        // store records a retry, the base change still applies, and the refresh job's retry scan delivers the
        // invalidation.
        assertMemoryLeak(() -> {
            createBase();
            createView("versions", BARE_VERSION_VIEW_SQL);
            insertBaseRows();
            drainWalAndMatViewQueues();
            final MatViewStateStoreImpl store = (MatViewStateStoreImpl) engine.getMatViewStateStore();
            final AtomicBoolean isFired = new AtomicBoolean();
            store.setOnTaskQueueAppendForTesting(() -> {
                if (isFired.compareAndSet(false, true)) {
                    throw new OutOfMemoryError("test invalidation enqueue failure");
                }
            });
            try {
                execute("ALTER TABLE base DROP COLUMN version");
                drainWalQueue();
            } finally {
                store.setOnTaskQueueAppendForTesting(null);
            }
            Assert.assertTrue(isFired.get());
            Assert.assertEquals(VERSION_REASON, getViewState("versions").getPendingInvalidationReason());
            assertColumns("base", """
                    column\ttype
                    sym\tSYMBOL
                    price\tDOUBLE
                    amount\tDOUBLE
                    ts\tTIMESTAMP
                    """);
            drainWalAndMatViewQueues();
            assertViewInvalid("versions", VERSION_REASON);
            assertViewRows("versions", "ts, sym, version", BARE_VERSION_VIEW_ROWS);
        });
    }

    @Test
    public void testManualViewStaysValidUntilRefreshFindsBaseChanges() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE MATERIALIZED VIEW amount_1h REFRESH MANUAL AS (" + AMOUNT_VIEW_SQL + ") PARTITION BY DAY");
            insertBaseRows();
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW amount_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewRows("amount_1h", AMOUNT_VIEW_ROWS);

            execute("ALTER TABLE base DROP COLUMN amount");
            drainWalAndMatViewQueues();
            assertViewValid("amount_1h");
            // the refresh finds the DROP, which changes no rows, so it has nothing to recompute
            execute("REFRESH MATERIALIZED VIEW amount_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewValid("amount_1h");
            assertRefreshBaseTxn("amount_1h", 2);
            // base rows alone do not refresh a manual view
            execute("INSERT INTO base (sym, price, version, ts) VALUES ('b', 5.5, 3, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewValid("amount_1h");

            execute("REFRESH MATERIALIZED VIEW amount_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewInvalid("amount_1h", "[" + AMOUNT_VIEW_SQL.indexOf("amount") + "]: Invalid column: amount");
            assertViewRows("amount_1h", AMOUNT_VIEW_ROWS);
            assertRefreshBaseTxn("amount_1h", 2);
        });
    }

    @Test
    public void testMetadataChangeReportsNoColumnNamesByDefault() {
        // A metadata change other than ADD, DROP or RENAME COLUMN changes no column name the guard checks.
        final TableMetadataChange change = (svc, contextAllowsAnyStructureChanges) -> 0;
        Assert.assertEquals(0, change.getChangedColumnNameCount());
        try {
            change.getChangedColumnName(0);
            Assert.fail("a change without column names has none to return");
        } catch (UnsupportedOperationException ignore) {
            // expected
        }
    }

    @Test
    public void testParentViewDropColumnInvalidatesOnlyChildrenReadingIt() throws Exception {
        // A view DROP COLUMN on a parent view applies like a base DROP COLUMN to its children: the children that
        // read the column go invalid at their next refresh, after the parent's next refresh.
        assertMemoryLeak(() -> {
            createBase();
            createView("parent", "SELECT ts, sym, last(price) AS price, last(amount) AS amount FROM base SAMPLE BY 1h");
            final String amountChildSql = "SELECT ts, sym, max(amount) AS amount FROM parent SAMPLE BY 1d";
            createView("price_child", "SELECT ts, sym, max(price) AS price FROM parent SAMPLE BY 1d");
            createView("amount_child", amountChildSql);
            insertBaseRows();
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            final String amountChildRows = """
                    ts\tsym\tamount
                    2024-01-01T00:00:00.000000Z\ta\t40.0
                    2024-01-01T00:00:00.000000Z\tb\t20.0
                    """;
            assertViewRows("amount_child", amountChildRows);

            execute("ALTER MATERIALIZED VIEW parent DROP COLUMN amount");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewValid("price_child");
            assertViewValid("amount_child");

            execute("INSERT INTO base (sym, price, amount, version, ts) VALUES ('b', 7.5, 70.0, 3, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            drainWalAndMatViewQueues();
            assertViewValid("parent");
            assertViewValid("price_child");
            assertViewRows("price_child", """
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t4.5
                    2024-01-01T00:00:00.000000Z\tb\t7.5
                    """);
            assertViewInvalid("amount_child", "[" + amountChildSql.indexOf("amount") + "]: Invalid column: amount");
            assertViewRows("amount_child", amountChildRows);
        });
    }

    @Test
    public void testProjectionAliasFallbackIsFollowed() throws Exception {
        // While the base has amount, amt reads it. Once the base drops it, amt reads the projection alias amount of
        // the same select list, and the SQL still compiles.
        assertMemoryLeak(() -> {
            createBase();
            createView(
                    "amt_1h",
                    "SELECT ts, sym, last(amt) AS amt FROM (SELECT ts, sym, price AS amount, amount AS amt FROM base) SAMPLE BY 1h"
            );
            insertBaseRows();
            drainWalAndMatViewQueues();
            assertViewRows("amt_1h", AMOUNT_VIEW_ROWS.replace("amount", "amt"));

            execute("ALTER TABLE base DROP COLUMN amount");
            execute("INSERT INTO base (sym, price, version, ts) VALUES ('b', 5.5, 3, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewValid("amt_1h");
            assertViewRows("amt_1h", AMOUNT_VIEW_ROWS.replace("amount", "amt") + "2024-01-01T02:00:00.000000Z\tb\t5.5\n");
        });
    }

    @Test
    public void testReadColumnDropInvalidatesAtNextRefresh() throws Exception {
        assertReadColumnChangeInvalidatesAtNextRefresh(
                "ALTER TABLE base DROP COLUMN amount",
                "INSERT INTO base (sym, price, version, ts) VALUES ('b', 5.5, 3, '2024-01-01T02:10:00.000000Z')"
        );
    }

    @Test
    public void testReadColumnRenameInvalidatesAtNextRefresh() throws Exception {
        assertReadColumnChangeInvalidatesAtNextRefresh(
                "ALTER TABLE base RENAME COLUMN amount TO amount2",
                "INSERT INTO base (sym, price, amount2, version, ts) VALUES ('b', 5.5, 50.0, 3, '2024-01-01T02:10:00.000000Z')"
        );
    }

    @Test
    public void testRefreshWithNothingToRecomputeKeepsViewValid() throws Exception {
        // The refresh that the DROP's own commit triggers moves the view's refresh position past the DROP without
        // compiling the SQL, which reads the dropped column. The next base insert gives it rows to recompute.
        assertMemoryLeak(() -> {
            createBase();
            createView("amount_1h", AMOUNT_VIEW_SQL);
            insertBaseRows();
            drainWalAndMatViewQueues();
            assertRefreshBaseTxn("amount_1h", 1);

            execute("ALTER TABLE base DROP COLUMN amount");
            drainWalAndMatViewQueues();
            assertViewValid("amount_1h");
            assertRefreshBaseTxn("amount_1h", 2);
            assertViewRows("amount_1h", AMOUNT_VIEW_ROWS);

            execute("INSERT INTO base (sym, price, version, ts) VALUES ('b', 5.5, 3, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewInvalid("amount_1h", "[" + AMOUNT_VIEW_SQL.indexOf("amount") + "]: Invalid column: amount");
            assertViewRows("amount_1h", AMOUNT_VIEW_ROWS);
        });
    }

    @Test
    public void testRegularViewBareVersionIsFollowed() throws Exception {
        // A bare name inside a regular view that the view's SQL reads keeps today's reading and has no guard: once
        // the base drops version, the regular view calls version(), and the view's SQL still compiles.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE VIEW rv_version AS (SELECT ts, sym, price, version FROM base)");
            drainWalAndViewQueues();
            execute("CREATE MATERIALIZED VIEW price_rv WITH BASE base AS (SELECT ts, sym, last(price) AS price FROM rv_version SAMPLE BY 1h) PARTITION BY DAY");
            insertBaseRows();
            drainWalAndMatViewQueues();
            assertViewRows("price_rv", PRICE_VIEW_ROWS);

            execute("ALTER TABLE base DROP COLUMN version");
            drainWalQueue();
            Assert.assertFalse(getViewState("price_rv").hasPendingInvalidationReason());
            execute("INSERT INTO base (sym, price, amount, ts) VALUES ('b', 5.5, 50.0, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewValid("price_rv");
            assertViewRows("price_rv", PRICE_VIEW_ROWS_AFTER_INSERT);
        });
    }

    @Test
    public void testRenameUnreadColumnKeepsViewValid() throws Exception {
        assertUnreadColumnChangeKeepsViewValid(
                "ALTER TABLE base RENAME COLUMN amount TO amount2",
                "INSERT INTO base (sym, price, amount2, version, ts) VALUES ('b', 5.5, 50.0, 3, '2024-01-01T02:10:00.000000Z')"
        );
    }

    @Test
    public void testRestartBetweenColumnChangeAndRefresh() throws Exception {
        // Nothing about a column change is kept in memory: a restart between the DROP and the next refresh leaves
        // the same decision to the compile of a refresh after it. The view that reads the dropped column ends
        // invalid with the compile error and without the new rows, and the view that does not read it refreshes.
        assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration conf = new DefaultTestCairoConfiguration(temp.newFolder().getAbsolutePath()) {
                @Override
                public int getCommitMode() {
                    return configuration.getCommitMode();
                }
            };
            try (CairoEngine first = new CairoEngine(conf)) {
                first.load();
                first.execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, version INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                first.execute("CREATE MATERIALIZED VIEW amount_1h AS (" + AMOUNT_VIEW_SQL + ") PARTITION BY DAY");
                first.execute("CREATE MATERIALIZED VIEW price_1h AS (" + PRICE_VIEW_SQL + ") PARTITION BY DAY");
                try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(first, 1)) {
                    first.execute(INSERT_BASE_ROWS, context);
                }
                drainWalAndMatViewQueues(first);
                first.execute("ALTER TABLE base DROP COLUMN amount");
                drainWalQueue(first);
            }
            try (CairoEngine second = new CairoEngine(conf)) {
                second.load();
                second.hydrateMatViewStateStore();
                drainWalAndMatViewQueues(second);
                second.execute("INSERT INTO base (sym, price, version, ts) VALUES ('b', 5.5, 3, '2024-01-01T02:10:00.000000Z')");
                drainWalAndMatViewQueues(second);
                assertViewStatus(second, "amount_1h", "invalid", "[" + AMOUNT_VIEW_SQL.indexOf("amount") + "]: Invalid column: amount");
                assertViewStatus(second, "price_1h", "valid", "");
                try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(second, 1)) {
                    assertQuery("amount_1h ORDER BY ts, sym")
                            .withEngine(second)
                            .withContext(context)
                            .timestamp("ts")
                            .noLeakCheck()
                            .expectSize()
                            .returns(AMOUNT_VIEW_ROWS);
                    assertQuery("price_1h ORDER BY ts, sym")
                            .withEngine(second)
                            .withContext(context)
                            .timestamp("ts")
                            .noLeakCheck()
                            .expectSize()
                            .returns(PRICE_VIEW_ROWS_AFTER_INSERT);
                }
            }
        });
    }

    // The last sequencer txn that the WAL table has applied.
    private static long appliedTxn(TableToken tableToken) {
        return engine.getTableSequencerAPI().getTxnTracker(tableToken).getWriterTxn();
    }

    // Asserts that the view's cache holds the base's first day, 2024-01-01, in the base's timestamp units, at the base
    // table txn.
    private static void assertFirstDayCached(MatViewState state, int timestampType, long baseTxn) throws NumericException {
        final TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        final LongList firstDay = new LongList();
        firstDay.add(driver.parseFloorLiteral("2024-01-01T00:00:00.000000Z"));
        firstDay.add(driver.parseFloorLiteral("2024-01-02T00:00:00.000000Z") - 1);
        Assert.assertEquals(baseTxn, state.getRefreshIntervalsBaseTxn());
        TestUtils.assertEquals(firstDay, state.getRefreshIntervals());
    }

    private static void assertNames(LowerCaseCharSequenceHashSet names, String... expected) {
        Assert.assertNotNull(names);
        Assert.assertEquals(expected.length, names.size());
        for (String name : expected) {
            Assert.assertTrue(name, names.contains(name));
        }
    }

    // Captures the view's state when the refresh that holds the view's latch releases it: after the refresh committed,
    // and before the release wakes a pending invalidation, see describeState().
    private static AtomicReference<String> captureStateAtUnlock(String viewName) {
        final MatViewState state = getViewState(viewName);
        final AtomicReference<String> captured = new AtomicReference<>();
        state.setOnBeforeUnlockForTesting(() -> captured.set(describeState(state)));
        return captured;
    }

    private static void createBase() throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, version INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    // A base with the column count and two rows, base txn 1.
    private static void createCountBase() throws SqlException {
        execute(CREATE_COUNT_BASE);
        execute("INSERT INTO base VALUES ('a', 1.0, 7, '2024-01-01T00:10:00.000000Z'), ('b', 2.0, 8, '2024-01-01T01:10:00.000000Z')");
        drainWalQueue();
    }

    // createCountBase() and the view mv with the refresh clause, whose SQL reads the column count bare.
    private static void createCountBaseWithView(String refreshClause) throws SqlException {
        createCountBase();
        execute("CREATE MATERIALIZED VIEW mv " + refreshClause + " AS (" + COUNT_VIEW_SQL + ") PARTITION BY DAY");
        drainWalAndMatViewQueues();
    }

    // A base without a column count and with two rows, base txn 1, and the MANUAL DEFERRED view mv, created before the
    // explicit-name rule, whose SQL calls count without parentheses.
    private static void createOlderCountView() throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO base VALUES ('a', 1.0, '2024-01-01T00:10:00.000000Z'), ('b', 2.0, '2024-01-01T01:10:00.000000Z')");
        drainWalQueue();
        executeWithoutExplicitNamesCheck("CREATE MATERIALIZED VIEW mv REFRESH MANUAL DEFERRED AS (" + COUNT_VIEW_SQL + ") PARTITION BY DAY");
        drainWalAndMatViewQueues();
    }

    private static void createView(String viewName, String sql) throws SqlException {
        execute("CREATE MATERIALIZED VIEW " + viewName + " AS (" + sql + ") PARTITION BY DAY");
    }

    // Creates the view with the refresh clause, runs the refresh job, which builds the view with the first refresh that
    // the CREATE queued, and applies the WAL. While a column change applies, the build reads the base without it.
    private static void createViewAndBuild(String viewName, String refreshClause, String sql) throws SqlException {
        execute("CREATE MATERIALIZED VIEW " + viewName + ' ' + refreshClause + " AS (" + sql + ") PARTITION BY DAY");
        drainMatViewQueue(engine);
        drainWalQueue();
    }

    // The view's validity, refresh position and pending invalidation, with the reason's base table and base txn.
    private static String describeState(MatViewState state) {
        final TableToken pendingBaseTableToken = state.getPendingInvalidationBaseTableTokenForTesting();
        return "invalid=" + state.isInvalid()
                + ", refreshBaseTxn=" + state.getLastRefreshBaseTxn()
                + ", pendingReason=" + state.getPendingInvalidationReason()
                + ", pendingBaseTable=" + (pendingBaseTableToken != null ? pendingBaseTableToken.getTableName() : null)
                + ", pendingBaseTxn=" + state.getPendingInvalidationBaseTxnForTesting();
    }

    private static long getSeqTxn(TableToken tableToken) {
        return engine.getTableSequencerAPI().getTxnTracker(tableToken).getSeqTxn();
    }

    private static MatViewState getViewState(String viewName) {
        final MatViewState state = engine.getMatViewStateStore().getViewState(engine.verifyTableName(viewName));
        Assert.assertNotNull(state);
        return state;
    }

    private static void initDefinition(MatViewDefinition definition, TableToken viewToken, String sql) {
        definition.init(
                MatViewDefinition.REFRESH_TYPE_IMMEDIATE,
                false,
                ColumnType.TIMESTAMP,
                viewToken,
                sql,
                "base",
                1,
                'h',
                null,
                null,
                0,
                0,
                (char) 0,
                Numbers.LONG_NULL,
                null,
                0,
                (char) 0,
                0,
                (char) 0,
                false
        );
    }

    // The rows as a view over a base of the timestamp type prints them: a TIMESTAMP_NS timestamp has nine fraction
    // digits.
    private static String inTimestampUnits(String rows, int timestampType) {
        return timestampType == ColumnType.TIMESTAMP_NANO ? rows.replace("Z\t", "000Z\t") : rows;
    }

    private static void insertBaseRows() throws SqlException {
        execute(INSERT_BASE_ROWS);
    }

    // Turns the detached partition of the base into one that ATTACH PARTITION takes.
    private static void makeAttachable(String partition) {
        final TableToken baseToken = engine.verifyTableName("base");
        try (Path path = new Path(); Path other = new Path()) {
            path.of(configuration.getDbRoot()).concat(baseToken).concat(partition).put(TableUtils.DETACHED_DIR_MARKER).$();
            other.of(configuration.getDbRoot()).concat(baseToken).concat(partition).put(configuration.getAttachPartitionSuffix()).$();
            Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);
        }
    }

    // The state, see describeState(), of a valid view at the refresh position, with the bare-name guard's invalidation
    // of the switch of count at the base table's txn pending.
    private static String pendingGuardState(String baseTableName, long refreshBaseTxn, long switchTxn) {
        return "invalid=false, refreshBaseTxn=" + refreshBaseTxn
                + ", pendingReason=" + COUNT_REASON
                + ", pendingBaseTable=" + baseTableName
                + ", pendingBaseTxn=" + switchTxn;
    }

    private static String queryVersion() throws SqlException {
        try (var factory = select("SELECT version()"); var cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue(cursor.hasNext());
            return cursor.getRecord().getStrA(0).toString();
        }
    }

    private void assertColumns(String tableName, String expected) throws Exception {
        assertQuery("SELECT \"column\", type FROM table_columns('" + tableName + "')")
                .noLeakCheck()
                .noRandomAccess()
                .returns(expected);
    }

    private void assertReadColumnChangeInvalidatesAtNextRefresh(String changeSql, String insertSql) throws Exception {
        // Each view reads amount in another place of its SQL: the projection, the filter, the other side of a self
        // join, and a regular view that the SQL reads. The view next to them does not read it.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE VIEW rv AS (SELECT ts, sym, amount FROM base)");
            drainWalAndViewQueues();
            createView("projection", AMOUNT_VIEW_SQL);
            createView("filter", FILTER_VIEW_SQL);
            createView("self_join", SELF_JOIN_VIEW_SQL);
            execute("CREATE MATERIALIZED VIEW regular_view WITH BASE base AS (" + REGULAR_VIEW_SQL + ") PARTITION BY DAY");
            createView("price_1h", PRICE_VIEW_SQL);
            insertBaseRows();
            drainWalAndMatViewQueues();
            final String[] views = {"projection", "filter", "self_join", "regular_view"};
            final String[] rows = {AMOUNT_VIEW_ROWS, FILTER_VIEW_ROWS, AMOUNT_VIEW_ROWS, AMOUNT_VIEW_ROWS};
            for (int i = 0; i < views.length; i++) {
                assertViewRows(views[i], rows[i]);
            }

            execute(changeSql);
            drainWalAndMatViewQueues();
            for (String view : views) {
                assertViewValid(view);
            }

            execute(insertSql);
            drainWalAndMatViewQueues();
            assertViewInvalid("projection", "[" + AMOUNT_VIEW_SQL.indexOf("amount") + "]: Invalid column: amount");
            assertViewInvalid("filter", "[" + FILTER_VIEW_SQL.indexOf("amount") + "]: Invalid column: amount");
            assertViewInvalid("self_join", "[" + SELF_JOIN_VIEW_SQL.indexOf("b.amount") + "]: Invalid column: b.amount");
            // the error of the regular view's expansion, as a compile of the SQL reports it now
            try {
                select(REGULAR_VIEW_SQL).close();
                Assert.fail("the SQL must not compile");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column: amount");
                assertViewInvalid("regular_view", "[" + e.getPosition() + "]: " + e.getFlyweightMessage());
            }
            for (int i = 0; i < views.length; i++) {
                assertViewRows(views[i], rows[i]);
            }
            assertViewValid("price_1h");
            assertViewRows("price_1h", PRICE_VIEW_ROWS_AFTER_INSERT);
        });
    }

    // A restart of the view states: the store reloads them from disk, as at startup.
    private static void restartViewStates() {
        engine.getMatViewStateStore().clear();
        engine.hydrateMatViewStateStore();
    }

    // Runs a refresh job until it has nothing to do, a bounded number of times, and asserts that the queue is empty.
    private static void runRefreshJobUntilIdle() {
        try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
            int busyRuns = 0;
            while (job.run()) {
                Assert.assertTrue("the refresh job does not go idle", ++busyRuns < 10);
            }
        }
        Assert.assertTrue(engine.getMatViewStateStore().isRefreshQueueEmpty());
    }

    private void assertRefreshBaseTxn(String viewName, long expectedTxn) throws Exception {
        assertQuery("SELECT refresh_base_table_txn FROM materialized_views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("refresh_base_table_txn\n" + expectedTxn + "\n");
    }

    // Fixes the SQL of the view versions, whose bare version the base renamed to ver, with ALTER ... AS, and resumes the
    // view: RESUME catches up on the UPDATE that set the price of a to 9.0, as the fixed SQL run on the base gives.
    private void assertResumeAfterFixCatchesUpOnUpdate() throws Exception {
        execute("ALTER MATERIALIZED VIEW versions AS (" + RENAMED_VERSION_VIEW_SQL + ")");
        drainWalQueue();
        execute("REFRESH MATERIALIZED VIEW versions RESUME");
        drainWalAndMatViewQueues();
        assertViewValid("versions");
        assertViewRows("versions", "ts, sym, version", RENAMED_VERSION_UPDATED_ROWS);
        assertSqlCursors(RENAMED_VERSION_VIEW_SQL + " ORDER BY ts, sym, version", "versions ORDER BY ts, sym, version");
    }

    private void assertUnreadColumnChangeKeepsViewValid(String changeSql, String insertSql) throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            createView("price_1h", PRICE_VIEW_SQL);
            insertBaseRows();
            drainWalAndMatViewQueues();
            assertViewRows("price_1h", PRICE_VIEW_ROWS);

            execute(changeSql);
            drainWalAndMatViewQueues();
            assertViewValid("price_1h");
            assertRefreshBaseTxn("price_1h", 2);

            execute(insertSql);
            drainWalAndMatViewQueues();
            assertViewValid("price_1h");
            assertViewRows("price_1h", PRICE_VIEW_ROWS_AFTER_INSERT);
            assertRefreshBaseTxn("price_1h", 3);

            // a RANGE refresh compiles the SQL too; it records no finish time, so the view reports it refreshing
            execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01T00:00:00.000000Z' TO '2024-01-01T03:00:00.000000Z'");
            drainWalAndMatViewQueues();
            assertViewStatus(engine, "price_1h", "refreshing", "");
            assertViewRows("price_1h", PRICE_VIEW_ROWS_AFTER_INSERT);
        });
    }

    private void assertViewInvalid(String viewName, String reason) throws Exception {
        assertViewStatus(engine, viewName, "invalid", reason);
    }

    private void assertViewRows(String viewName, String expected) throws Exception {
        assertViewRows(viewName, "ts, sym", expected);
    }

    private void assertViewRows(String viewName, String orderBy, String expected) throws Exception {
        assertQuery(viewName + " ORDER BY " + orderBy).timestamp("ts").noLeakCheck().expectSize().returns(expected);
    }

    private void assertViewStatus(CairoEngine engine, String viewName, String status, String reason) throws Exception {
        try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1)) {
            assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = '" + viewName + "'")
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\tinvalidation_reason\n" + status + '\t' + reason + '\n');
        }
    }

    // The view's status, reason and refresh position as materialized_views() reports them: from the view's state file,
    // which the apply of the view's WAL writes, so the caller applies the view's WAL first.
    private void assertViewState(String viewName, String status, String reason, long refreshBaseTxn) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason, refresh_base_table_txn FROM materialized_views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\trefresh_base_table_txn\n" + status + '\t' + reason + '\t' + refreshBaseTxn + '\n');
    }

    private void assertViewValid(String viewName) throws Exception {
        assertViewStatus(engine, viewName, "valid", "");
    }

    // createCountBaseWithView() with a MANUAL view, built by its first refresh from base txn 1 under the column reading.
    private void createRefreshedCountView() throws Exception {
        createCountBaseWithView("REFRESH MANUAL");
        execute("REFRESH MATERIALIZED VIEW mv INCREMENTAL");
        drainWalAndMatViewQueues();
        assertViewState("mv", "valid", "", 1);
        assertViewRows("mv", COUNT_COLUMN_ROWS);
    }

    // createRefreshedCountView() and the DROP of count, whose switch the guard records for the base table, which a
    // table with the column count then replaces: its txn 1 inserts one row. A FULL refresh rebuilds the view from the
    // new table, below the switch's txn 2, and the view stays valid, as the switch belongs to the dropped table.
    private void createViewRebuiltFromReplacedBase() throws Exception {
        createRefreshedCountView();
        execute(DROP_COUNT);
        drainWalAndMatViewQueues();
        assertViewState("mv", "invalid", COUNT_REASON, 1);
        execute("DROP TABLE base");
        execute(CREATE_COUNT_BASE);
        execute("INSERT INTO base VALUES ('d', 4.0, 5, '2024-01-01T03:10:00.000000Z')");
        drainWalAndMatViewQueues();
        assertViewState("mv", "invalid", COUNT_REASON, 1);

        execute("REFRESH MATERIALIZED VIEW mv FULL");
        drainWalAndMatViewQueues();
        drainWalAndMatViewQueues();
        assertViewState("mv", "valid", "", 1);
        Assert.assertFalse(getViewState("mv").hasPendingInvalidationReason());
        assertViewRows("mv", REPLACED_BASE_ROWS);
    }

    // The tables base, with one row at base txn 1 and no column count, and base2, with the column count and the rows of
    // createCountBase() at base2 txn 1, and the MANUAL view mv on base that calls count(), built at base txn 1.
    private void createViewToMoveToBase2() throws Exception {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO base VALUES ('z', 1.0, '2024-01-01T05:10:00.000000Z')");
        execute("CREATE TABLE base2 (sym SYMBOL, price DOUBLE, count LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO base2 VALUES ('a', 1.0, 7, '2024-01-01T00:10:00.000000Z'), ('b', 2.0, 8, '2024-01-01T01:10:00.000000Z')");
        drainWalQueue();
        execute("CREATE MATERIALIZED VIEW mv REFRESH MANUAL AS (" + COUNT_CALL_SQL + ") PARTITION BY DAY");
        drainWalAndMatViewQueues();
        assertViewState("mv", "valid", "", 1);
        assertViewRows("mv", """
                ts\tsym\tcount\tprice
                2024-01-01T05:00:00.000000Z\tz\t1\t1.0
                """);
    }

    // Lets the paused WAL apply of the column change finish, and waits for it, see raceColumnChange().
    private void finishColumnChange() {
        applyRelease.countDown();
        try {
            applier.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    // ALTER MATERIALIZED VIEW ... WITH BASE moves the view mv of createViewToMoveToBase2() to base2 with a SQL that
    // reads count bare, and the view's WAL installs the move: the view is invalid with the reason of a base change.
    private void moveViewToBase2() throws Exception {
        execute("ALTER MATERIALIZED VIEW mv WITH BASE base2 AS (" + BASE2_COUNT_VIEW_SQL + ")");
        drainWalQueue();
        drainMatViewQueue(engine);
        drainWalQueue();
        Assert.assertEquals(BASE2_COUNT_VIEW_SQL, getViewState("mv").getViewDefinition().getMatViewSql());
        assertViewState("mv", "invalid", MatViewRefreshJob.BASE_TABLE_CHANGED_REASON, 1);
    }

    // Runs the column change of the table, and applies the WAL on another thread, whose apply of the change pauses after
    // the bare-name guard ran and before the change is visible, see pausingFf. The test thread runs racer meanwhile, and
    // then lets the apply finish. The test runs under assertMemoryLeak(pausingFf, ...).
    private void raceColumnChange(String tableName, String changeSql, TestUtils.LeakProneCode racer) throws Exception {
        pauseSuffix.set(engine.verifyTableName(tableName).getDirName() + Files.SEPARATOR + TableUtils.META_SWAP_FILE_NAME);
        execute(changeSql);
        applier = new Thread(() -> {
            try {
                drainWalQueue();
            } catch (Throwable th) {
                applierError.set(th);
            } finally {
                Path.clearThreadLocals();
            }
        });
        applier.start();
        try {
            Assert.assertTrue(applyPaused.await(30, TimeUnit.SECONDS));
            racer.run();
        } finally {
            finishColumnChange();
        }
        Assert.assertNull(applierError.get());
    }

    // Called by a racer of raceColumnChange(): lets the paused apply of the change finish, holds the bare-name guard's
    // check after the change once it recorded a switch on the view, see
    // MatViewState.setOnBareNameSwitchRecordedForTesting(), and runs the code on this thread meanwhile. The check then
    // reads the view's refresh position, and the apply finishes.
    private void runWhileGuardChecksAfterChange(String viewName, TestUtils.LeakProneCode code) throws Exception {
        final CountDownLatch recorded = new CountDownLatch(1);
        final CountDownLatch proceed = new CountDownLatch(1);
        getViewState(viewName).setOnBareNameSwitchRecordedForTesting(() -> {
            recorded.countDown();
            try {
                Assert.assertTrue(proceed.await(60, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        applyRelease.countDown();
        try {
            Assert.assertTrue("the guard's check after the change recorded no switch", recorded.await(30, TimeUnit.SECONDS));
            code.run();
        } finally {
            proceed.countDown();
        }
        finishColumnChange();
    }
}
