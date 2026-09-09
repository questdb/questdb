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
 * A monotonic-timestamp predicate with a scalar sub-query bound - {@code dateadd('h',1,ts) >= (SELECT ...)}
 * - prunes through an inverter that is adopted by the runtime interval model, and links the same
 * sub-query node to a residual reader that consumes the inverter's single published value.
 * <p>
 * A later conjunct can collapse that model to the empty set, which releases the adopted inverter and
 * with it the only publisher. Unless the collapse also marks the whole WHERE clause as constant-false,
 * the residual reader survives into the generated filter with nothing left to publish into it, and the
 * statement fails at scan open with {@code AssertionError: scalar sub-query bound read before it was
 * published} (silently reading a stale bound with assertions off).
 * <p>
 * Conjunct order matters and is not source order: the AND traversal is reversed, so the collapse must
 * be written <em>before</em> the monotonic predicate to be processed <em>after</em> it. Both orders are
 * pinned here.
 *
 * @see io.questdb.griffin.model.IntrinsicModel
 * @see io.questdb.griffin.engine.functions.ScalarSubQueryBoundRefFunction
 */
public class ScalarSubqueryBoundEmptyIntervalCollapseTest extends AbstractCairoTest {

    // A NULL BETWEEN endpoint on the designated timestamp is a contradiction: it empties the interval
    // model through the builder's between path, releasing the adopted inverter. Written first in the
    // source, so the reversed AND traversal processes it *after* the monotonic predicate has already
    // adopted the inverter and linked the residual reader - the orphaning order.
    @Test
    public void testBetweenNullEndpointAfterMonotonicBound() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();

            assertQuery("""
                    SELECT ts, v FROM t
                    WHERE ts BETWEEN null AND '2020-06-05'
                      AND dateadd('h', 1, ts) >= (SELECT max(b) FROM bounds)
                    """)
                    .timestamp("ts")
                    .withPlanContaining("Empty table")
                    .returns("ts\tv\n");

            // The count() shape from the original report: the aggregate hides the row output, so the
            // failure surfaced only as the internal error.
            assertQuery("""
                    SELECT count(*) FROM t
                    WHERE ts BETWEEN null AND '2020-06-05'
                      AND dateadd('h', 1, ts) >= (SELECT max(b) FROM bounds)
                    """)
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Empty table")
                    .returns("count\n0\n");
        });
    }

    // Both endpoints NULL: the same collapse via a different arm of the between path.
    @Test
    public void testBetweenNullEndpointBothNulls() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();

            assertQuery("""
                    SELECT ts, v FROM t
                    WHERE ts BETWEEN null AND null
                      AND dateadd('h', 1, ts) >= (SELECT max(b) FROM bounds)
                    """)
                    .timestamp("ts")
                    .withPlanContaining("Empty table")
                    .returns("ts\tv\n");
        });
    }

    // A typed NULL endpoint reaches the collapse through the constant-folding path rather than the
    // untyped null keyword, so it is pinned separately.
    @Test
    public void testBetweenNullEndpointCastToTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();

            assertQuery("""
                    SELECT ts, v FROM t
                    WHERE ts BETWEEN cast(null as timestamp) AND '2020-06-05'
                      AND dateadd('h', 1, ts) >= (SELECT max(b) FROM bounds)
                    """)
                    .timestamp("ts")
                    .withPlanContaining("Empty table")
                    .returns("ts\tv\n");
        });
    }

    // The monotonic predicate written first in the source is traversed last, so the model is already
    // empty when it runs and the inverter is released before adoption. This order was always correct;
    // it is pinned so a fix cannot regress it, and to document why a probe using this order alone
    // would wrongly conclude the failure is unreachable.
    @Test
    public void testBetweenNullEndpointInReversedSourceOrder() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();

            assertQuery("""
                    SELECT ts, v FROM t
                    WHERE dateadd('h', 1, ts) >= (SELECT max(b) FROM bounds)
                      AND ts BETWEEN null AND '2020-06-05'
                    """)
                    .timestamp("ts")
                    .withPlanContaining("Empty table")
                    .returns("ts\tv\n");
        });
    }

    // A non-timestamp conjunct ahead of the contradiction changes the traversal shape without changing
    // the outcome.
    @Test
    public void testBetweenNullEndpointWithLeadingConjunct() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();

            assertQuery("""
                    SELECT ts, v FROM t
                    WHERE v > 0
                      AND ts BETWEEN null AND '2020-06-05'
                      AND dateadd('h', 1, ts) >= (SELECT max(b) FROM bounds)
                    """)
                    .timestamp("ts")
                    .withPlanContaining("Empty table")
                    .returns("ts\tv\n");
        });
    }

    // The monotonic predicate carrying two scalar sub-query bounds links two residual readers at once;
    // the collapse must orphan neither.
    @Test
    public void testBetweenNullEndpointWithMonotonicBetweenBounds() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();

            assertQuery("""
                    SELECT ts, v FROM t
                    WHERE ts BETWEEN null AND '2020-06-05'
                      AND dateadd('h', 1, ts) BETWEEN (SELECT min(b) FROM bounds) AND (SELECT max(b) FROM bounds)
                    """)
                    .timestamp("ts")
                    .withPlanContaining("Empty table")
                    .returns("ts\tv\n");
        });
    }

    // Control: with both BETWEEN endpoints present nothing collapses, so the scalar sub-query bound must
    // still prune and the residual reader must still be fed. Guards against a fix that over-eagerly
    // marks the clause constant-false.
    @Test
    public void testNonNullBetweenStillPrunesWithScalarBound() throws Exception {
        assertMemoryLeak(() -> {
            createFixture();

            assertQuery("""
                    SELECT ts, v FROM t
                    WHERE ts BETWEEN '2020-06-01' AND '2020-06-05'
                      AND dateadd('h', 1, ts) >= (SELECT max(b) FROM bounds)
                    """)
                    .timestamp("ts")
                    .withPlanContaining("Interval forward scan on: t")
                    .returns("ts\tv\n2020-06-02T00:00:00.000000Z\t2\n");
        });
    }

    private static void createFixture() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO t VALUES ('2020-06-01T00:00:00.000000Z', 1), ('2020-06-02T00:00:00.000000Z', 2)");
        execute("CREATE TABLE bounds (b TIMESTAMP)");
        execute("INSERT INTO bounds VALUES ('2020-06-01T12:00:00.000000Z')");
    }
}
