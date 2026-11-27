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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A validation-only compile of the statements this feature adds, as the web console's syntax check runs it, writes
 * nothing: RESUME and RESUME SKIP BACKLOG leave an invalid view invalid, ALTER ... WITH BASE leaves the base, and view
 * ADD COLUMN, DROP COLUMN and DROP PARTITION leave the view's columns and rows. The view's WAL gets no transaction.
 * Nor does a validation-only view ALTER or REFRESH ... RANGE wait for an earlier ALTER ... AS to apply, or refuse while
 * the apply of the view's WAL is suspended: a view ALTER checks the definition the view's WAL applied, as REFRESH does,
 * and a REFRESH enqueues no refresh.
 */
public class MatViewValidationOnlyTest extends AbstractCairoTest {
    private static final String VIEW_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t1.0
            2024-01-02T00:00:00.000000Z\tb\t2.0
            """;

    @Test
    public void testValidationOnlyDoesNotWaitForPendingDefinition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE base2 (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, '2024-01-02T00:10:00.000000Z')
                    """);
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final TableToken viewToken = engine.verifyTableName("price_1h");
            final String newSql = "SELECT ts, sym, max(price) AS price FROM base SAMPLE BY 1h";
            execute("ALTER MATERIALIZED VIEW price_1h AS (" + newSql + ")");
            final String[] waitingStatements = {
                    "ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra INT",
                    "ALTER MATERIALIZED VIEW price_1h DROP COLUMN price",
                    "ALTER MATERIALIZED VIEW price_1h AS (SELECT ts, sym, min(price) AS price FROM base SAMPLE BY 1h)",
                    "ALTER MATERIALIZED VIEW price_1h WITH BASE base2 AS (SELECT ts, sym, last(price) AS price FROM base2 SAMPLE BY 1h)",
                    "REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01' TO '2024-01-02'",
            };
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            // a statement that waits fails instead of polling forever under the test's circuit breaker
            state.setOnDefinitionWaitForTesting(() -> {
                throw new AssertionError("the validation-only statement waited for the pending definition event");
            });
            try {
                for (String sql : waitingStatements) {
                    assertValidationOnlyWritesNothing(viewToken, sql);
                }
                execute("ALTER MATERIALIZED VIEW price_1h SUSPEND WAL");
                for (String sql : waitingStatements) {
                    assertValidationOnlyWritesNothing(viewToken, sql);
                }
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, viewToken.getDirName());
                for (String sql : waitingStatements) {
                    assertValidationOnlyWritesNothing(viewToken, sql);
                }
            } finally {
                state.setOnDefinitionWaitForTesting(null);
                setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, null);
            }
            execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
            drainWalAndMatViewQueues();
            assertState("valid", "", "base");
            Assert.assertEquals(newSql, engine.getDependentViewGraph().getViewDefinition(viewToken).getMatViewSql());
            assertQuery("price_1h ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(VIEW_ROWS);
            assertQuery("SELECT \"column\" FROM table_columns('price_1h')").noLeakCheck().noRandomAccess().returns("""
                    column
                    ts
                    sym
                    price
                    """);
        });
    }

    @Test
    public void testValidationOnlyWritesNothing() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE base2 (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                    ('b', 2.0, '2024-01-02T00:10:00.000000Z')
                    """);
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            final TableToken viewToken = engine.verifyTableName("price_1h");

            final String[] validOnlyStatements = {
                    "ALTER MATERIALIZED VIEW price_1h ADD COLUMN extra INT",
                    "ALTER MATERIALIZED VIEW price_1h DROP COLUMN price",
                    "ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-01-01'",
                    "ALTER MATERIALIZED VIEW price_1h DROP PARTITION WHERE ts < '2024-01-03'",
                    "ALTER MATERIALIZED VIEW price_1h WITH BASE base2 AS (SELECT ts, sym, last(price) AS price FROM base2 SAMPLE BY 1h)",
            };
            for (String sql : validOnlyStatements) {
                assertValidationOnlyWritesNothing(viewToken, sql);
            }
            drainWalAndMatViewQueues();
            assertState("valid", "", "base");
            assertQuery("price_1h ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(VIEW_ROWS);

            engine.getMatViewStateStore().enqueueInvalidate(viewToken, "test invalidation");
            drainWalAndMatViewQueues();
            for (String sql : new String[]{
                    "REFRESH MATERIALIZED VIEW price_1h RESUME",
                    "REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG"
            }) {
                assertValidationOnlyWritesNothing(viewToken, sql);
            }
            drainWalAndMatViewQueues();
            assertState("invalid", "test invalidation", "base");
            assertQuery("price_1h ORDER BY ts").timestamp("ts").noLeakCheck().expectSize().returns(VIEW_ROWS);
            assertQuery("SELECT \"column\" FROM table_columns('price_1h')").noLeakCheck().noRandomAccess().returns("""
                    column
                    ts
                    sym
                    price
                    """);
        });
    }

    private void assertState(String status, String reason, String baseTableName) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason, base_table_name FROM materialized_views()").noLeakCheck().noRandomAccess()
                .returns("view_status\tinvalidation_reason\tbase_table_name\n" + status + '\t' + reason + '\t' + baseTableName + '\n');
    }

    private static void assertValidationOnlyWritesNothing(TableToken viewToken, String sql) throws Exception {
        final long viewSeqTxn = engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn();
        final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
        context.setValidationOnly(true);
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.compile(sql, context);
        } finally {
            context.setValidationOnly(false);
        }
        Assert.assertEquals(sql, viewSeqTxn, engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn());
        Assert.assertTrue(sql, engine.getMatViewStateStore().isRefreshQueueEmpty());
    }
}
