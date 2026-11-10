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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewBareNameGuard;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateStoreImpl;
import io.questdb.cairo.wal.seq.TableMetadataChange;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

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

    private static final String PRICE_VIEW_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t3.5
            2024-01-01T00:00:00.000000Z\tb\t2.5
            2024-01-01T01:00:00.000000Z\ta\t4.5
            """;
    private static final String PRICE_VIEW_ROWS_AFTER_INSERT = PRICE_VIEW_ROWS + "2024-01-01T02:00:00.000000Z\tb\t5.5\n";
    private static final String PRICE_VIEW_SQL = "SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h";
    private static final String REGULAR_VIEW_SQL = "SELECT ts, sym, last(amount) AS amount FROM rv SAMPLE BY 1h";
    private static final String SELF_JOIN_VIEW_SQL = "SELECT a.ts, a.sym, last(b.amount) AS amount FROM base a ASOF JOIN base b ON (sym) SAMPLE BY 1h";
    private static final String VERSION_REASON = MatViewBareNameGuard.invalidationReason("version");

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

    private static void assertNames(LowerCaseCharSequenceHashSet names, String... expected) {
        Assert.assertNotNull(names);
        Assert.assertEquals(expected.length, names.size());
        for (String name : expected) {
            Assert.assertTrue(name, names.contains(name));
        }
    }

    private static void createBase() throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, amount DOUBLE, version INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private static void createView(String viewName, String sql) throws SqlException {
        execute("CREATE MATERIALIZED VIEW " + viewName + " AS (" + sql + ") PARTITION BY DAY");
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

    private static void insertBaseRows() throws SqlException {
        execute(INSERT_BASE_ROWS);
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

    private void assertRefreshBaseTxn(String viewName, long expectedTxn) throws Exception {
        assertQuery("SELECT refresh_base_table_txn FROM materialized_views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("refresh_base_table_txn\n" + expectedTxn + "\n");
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

    private void assertViewValid(String viewName) throws Exception {
        assertViewStatus(engine, viewName, "valid", "");
    }
}
