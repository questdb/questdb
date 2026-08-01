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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewRefreshSqlExecutionContext;
import io.questdb.griffin.SqlCompiler;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Upgrade-compatibility regression tests.
 * <p>
 * A rejected CREATE is annoying but visible. The severe failure mode is different: persisted view
 * SQL is recompiled under {@link MatViewRefreshSqlExecutionContext} on every cache miss and after
 * every restart, and a {@code SqlException} there routes to
 * {@code MatViewRefreshJob#refreshFailState}, which persists {@code invalid=true}, replicates it to
 * peers and cascades it to dependent views. Recovery needs a manual REFRESH ... FULL after
 * rewriting SQL that the guard still rejects.
 * <p>
 * These tests exercise exactly the refresh job's compile step, so a guard that over-rejects fails
 * here even when CREATE was accepted on the node that ran it.
 */
@RunWith(Parameterized.class)
public class MatViewRefreshRecompileCompatibilityTest extends AbstractCairoTest {
    private final boolean parallel;

    public MatViewRefreshRecompileCompatibilityTest(boolean parallel) {
        this.parallel = parallel;
    }

    @Parameterized.Parameters(name = "parallel={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{true}, {false}});
    }

    @Test
    public void testRefreshRecompileSucceedsAfterUnrelatedAddIndex() throws Exception {
        // An index on the *config* table swaps the sub-query's filtered scan for an index-driven row
        // cursor. That must not retroactively invalidate an already-created view.
        assertRefreshRecompiles(
                "ts > (SELECT max(lim) FROM cfg WHERE k = 'a')",
                true
        );
    }

    @Test
    public void testRefreshRecompileSucceedsForMonotonicWrapperAfterAddIndex() throws Exception {
        // Unlike the bare-ts shape above, a monotonic wrapper over the designated timestamp keeps
        // the original predicate as a residual filter that re-reads the pruning bound through
        // ScalarSubQueryBoundRefFunction, so this exercises the shared-holder path in the refresh
        // recompile. ADD INDEX flips the sub-query factory's fail-safe determinism hint under the
        // live view; the guard must not read that hint.
        assertRefreshRecompiles(
                "dateadd('h', 1, ts) >= (SELECT max(lim) FROM cfg WHERE k = 'a')",
                true
        );
    }

    @Test
    public void testRefreshRecompileSucceedsForAggregateSubQuery() throws Exception {
        assertRefreshRecompiles("ts > (SELECT max(lim) FROM cfg)", false);
    }

    @Test
    public void testRefreshRecompileSucceedsForGroupBySubQuery() throws Exception {
        assertRefreshRecompiles("ts > (SELECT max(lim) FROM cfg WHERE n > 0 GROUP BY k LIMIT 1)", false);
    }

    @Test
    public void testRefreshRecompileSucceedsForJoinSubQuery() throws Exception {
        assertRefreshRecompiles("ts > (SELECT max(c1.lim) FROM cfg c1 JOIN cfg c2 ON c1.k = c2.k)", false);
    }

    @Test
    public void testRefreshRecompileSucceedsForOrderedLimitSubQuery() throws Exception {
        assertRefreshRecompiles("ts > (SELECT lim FROM cfg ORDER BY ts DESC LIMIT 1)", false);
    }

    private void assertRefreshRecompiles(String predicate, boolean addIndexAfterCreate) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, String.valueOf(parallel));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED, String.valueOf(parallel));
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE, n LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE cfg (ts TIMESTAMP, k SYMBOL, lim TIMESTAMP, n LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO cfg VALUES ('2024-01-01T00:00:00Z', 'a', '2024-01-01T00:00:00Z', 1)");
            execute("INSERT INTO base VALUES ('2024-01-02T00:00:00Z', 'a', 1.0, 1)");
            drainWalQueue();

            execute("CREATE MATERIALIZED VIEW mv AS SELECT ts, sum(v) AS s FROM base WHERE "
                    + predicate + " SAMPLE BY 1h");
            drainWalQueue();

            if (addIndexAfterCreate) {
                execute("ALTER TABLE cfg ALTER COLUMN k ADD INDEX");
                drainWalQueue();
                engine.releaseInactive();
            }

            recompileAsRefreshJobWould(predicate);
        });
    }

    /**
     * Mirrors the refresh job's cache-miss path: compile the persisted view SQL under the refresh
     * execution context, which is the context that disallows non-deterministic functions.
     */
    private void recompileAsRefreshJobWould(String predicate) {
        final TableToken viewToken = engine.verifyTableName("mv");
        final MatViewDefinition definition = engine.getMatViewGraph().getViewDefinition(viewToken);
        final String viewSql = definition.getMatViewSql();
        final TableToken baseToken = engine.verifyTableName(definition.getBaseTableName());
        try (
                MatViewRefreshSqlExecutionContext refreshContext = new MatViewRefreshSqlExecutionContext(engine, 1);
                TableReader baseReader = engine.getReader(baseToken)
        ) {
            refreshContext.of(baseReader);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.compile(viewSql, refreshContext).getRecordCursorFactory().close();
            }
        } catch (Throwable e) {
            throw new AssertionError(
                    "persisted materialized-view SQL must recompile under the refresh context. Failing "
                            + "here means the view is marked invalid=true on the next refresh or restart, "
                            + "replicated to peers and cascaded to dependent views.\n  predicate: "
                            + predicate + "\n  parallel=" + parallel + "\n  error: " + e.getMessage(), e);
        }
    }
}
