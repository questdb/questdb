/*******************************************************************************
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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A monotonic-timestamp predicate whose bound is a table-backed scalar sub-query
 * (e.g. {@code dateadd('h',1,ts) >= (SELECT max(b) FROM bounds)}) is consumed twice within one
 * execution: once by the interval-pruning inverter and once by the retained residual row filter
 * (the inversion of a runtime bound is not proven exact at compile time, so the predicate always
 * stays a residual filter). Before value sharing each side compiled and opened the sub-query
 * independently, and a freshly borrowed reader is refreshed to the latest commit via
 * {@code goActive()} - so a commit to {@code bounds} landing between the two opens could make the
 * pruning bound stricter than the residual and silently drop qualifying rows.
 *
 * <p>{@link io.questdb.griffin.WhereClauseParser} now evaluates each dynamic bound exactly once: the
 * pruning inverter's {@link io.questdb.griffin.engine.functions.ScalarSubQueryTimestampFunction}
 * publishes its single per-execution value into a
 * {@link io.questdb.griffin.model.ScalarTimestampBoundHolder} at partition-frame open, and the
 * residual - re-compiled from the same sub-query node, including per-worker filter clones - reads
 * that frozen value through a
 * {@link io.questdb.griffin.engine.functions.ScalarSubQueryBoundRefFunction} instead of opening the
 * sub-query again. The plan-shape tests below pin that residual reader (it renders as
 * {@code scalar_subquery_bound}) so the residual can no longer diverge from the pruning bound, while
 * the oracle tests confirm the pruned result equals the equivalent literal-bound query.
 */
public class ScalarSubqueryBoundSingleEvaluationTest extends AbstractCairoTest {

    @Test
    public void testBetweenTableBoundsResultMatchesLiteralOracle() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertSqlCursors(
                    "SELECT ts, v FROM t WHERE dateadd('d', 1, ts) BETWEEN '2020-06-01T00:00:00.000000Z' AND '2020-06-02T00:00:00.000000Z'",
                    "SELECT ts, v FROM t WHERE dateadd('d', 1, ts) BETWEEN (SELECT min(b) FROM bounds) AND (SELECT max(b) FROM bounds)"
            );
        });
    }

    // Both BETWEEN bounds prune AND both are read from the shared holder in the residual (rendered
    // twice as scalar_subquery_bound), rather than re-opening the two sub-queries independently.
    @Test
    public void testMonotonicBetweenBoundsShareValueWithResidual() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('d', 1, ts) BETWEEN (SELECT min(b) FROM bounds) AND (SELECT max(b) FROM bounds)")
                    .assertsPlanContaining(
                            "between scalar_subquery_bound and scalar_subquery_bound",
                            "Interval forward scan on: t"
                    );
        });
    }

    // The residual filter reads the pruning bound's shared value (scalar_subquery_bound) instead of
    // re-opening the sub-query; the Interval scan proves pruning is still applied.
    @Test
    public void testMonotonicSingleBoundSharesValueWithResidual() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT max(b) FROM bounds)")
                    .assertsPlanContaining(
                            "dateadd('h',1,ts)>=scalar_subquery_bound",
                            "Interval forward scan on: t"
                    );
        });
    }

    @Test
    public void testMonotonicWrappedTableBoundResultMatchesLiteralOracle() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // pruning and residual must agree: identical to the same predicate with the literal max
            assertSqlCursors(
                    "SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= '2020-06-02T00:00:00.000000Z'",
                    "SELECT ts, v FROM t WHERE dateadd('h', 1, ts) >= (SELECT max(b) FROM bounds)"
            );
        });
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO t VALUES " +
                "('2020-06-01T00:00:00.000000Z', 1), " +
                "('2020-06-02T00:00:00.000000Z', 2), " +
                "('2020-06-03T00:00:00.000000Z', 3)");
        execute("CREATE TABLE bounds (b TIMESTAMP)");
        execute("INSERT INTO bounds VALUES ('2020-06-01T00:00:00.000000Z'), ('2020-06-02T00:00:00.000000Z')");
    }
}
