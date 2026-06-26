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

package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.groupby.GroupByNotKeyedRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Locks in the two factory-shape predicates the query fuzzer's swallow oracle
 * relies on to decide when a fired-but-not-surfaced fault is legitimate.
 * <p>
 * {@link QueryRunner#factoryHasEarlyExitGroupBy} recognises a
 * {@code count_distinct} over a constant, which compiles to the serial
 * {@link GroupByNotKeyedRecordCursorFactory} with an early-exit cursor that
 * stops at the first matching row; under the eager parallel filter beneath it a
 * fired {@code test_fault()} can land on a discarded frame, so the oracle must
 * tolerate the shape rather than report it as a swallowed error.
 * <p>
 * {@link QueryRunner#factoryHasBlockingAggregation} recognises the opposite: a
 * {@code count()} / {@code sum()} / keyed or non-keyed {@code GROUP BY} that
 * drains every frame before a downstream {@code LIMIT} can apply. A fault fired
 * under such an aggregate is always pulled and must surface, so the oracle must
 * NOT tolerate a swallow there even though the SQL text carries a {@code LIMIT}.
 */
public class EarlyExitGroupByDetectionTest extends AbstractCairoTest {

    @Test
    public void testBlockingAggregationDetection() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL, s VARCHAR, l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT rnd_symbol('a','b'), rnd_varchar(1, 4, 1), x, " +
                    "timestamp_sequence(0, 1000000L) FROM long_sequence(10)");

            // A LIMIT over a blocking aggregate does not stop the scan: the aggregate
            // drains every frame before the LIMIT can apply, so a fault fired on any
            // frame must surface. count(*) routes to CountRecordCursorFactory; the
            // keyless and keyed aggregates route to the async group bys; count_distinct
            // over a column is a non-early-exit aggregate. All are blocking.
            assertBlockingAggregation("SELECT count(*) FROM t WHERE l > 0 LIMIT 5", true);
            assertBlockingAggregation("SELECT sum(l) FROM t WHERE l > 0 LIMIT 5", true);
            assertBlockingAggregation("SELECT avg(l) FROM t WHERE l > 0 LIMIT 1", true);
            assertBlockingAggregation("SELECT sym, count() FROM t WHERE l > 0 GROUP BY sym LIMIT 5", true);
            assertBlockingAggregation("SELECT count_distinct(s) FROM t WHERE l > 0 LIMIT 5", true);

            // An early-exit non-keyed group by (count_distinct over a constant) stops
            // before draining, so it is not blocking -- the swallow oracle tolerates it
            // through factoryHasEarlyExitGroupBy instead.
            assertBlockingAggregation("SELECT count_distinct('B') FROM t WHERE l > 0 LIMIT 5", false);

            // Streaming consumers a LIMIT genuinely stops early are not blocking: a
            // window function and a bare projection both abandon the eager scan once
            // the LIMIT is met, so a fault on a discarded frame is legitimate.
            assertBlockingAggregation("SELECT l, max(l) OVER (ORDER BY ts) FROM t WHERE l > 0 LIMIT 5", false);
            assertBlockingAggregation("SELECT * FROM t WHERE l > 0 LIMIT 5", false);
        });
    }

    @Test
    public void testCountDistinctConstantIsEarlyExit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL, s VARCHAR, l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT rnd_symbol('a','b'), rnd_varchar(1, 4, 1), x, " +
                    "timestamp_sequence(0, 1000000L) FROM long_sequence(10)");

            // count_distinct over a string constant routes to the serial non-keyed
            // group by with an early-exit cursor, with and without a filter.
            assertEarlyExit("SELECT count_distinct('B') FROM t", true);
            assertEarlyExit("SELECT count_distinct('B') FROM t WHERE l > 0", true);

            // A long constant has no early exit; count(*) is not a count_distinct.
            assertEarlyExit("SELECT count_distinct(10) FROM t", false);
            assertEarlyExit("SELECT count(*) FROM t WHERE l > 0", false);
            assertEarlyExit("SELECT count_distinct(s) FROM t WHERE l > 0", false);
        });
    }

    private static void assertBlockingAggregation(String sql, boolean expected) throws Exception {
        // engine.select wraps the plan in a QueryProgress factory, so the aggregation
        // is reached through getBaseFactory; the oracle walks that chain.
        try (RecordCursorFactory factory = engine.select(sql, sqlExecutionContext)) {
            assertEquals(
                    "blocking-aggregation detection for: " + sql,
                    expected,
                    QueryRunner.factoryHasBlockingAggregation(factory)
            );
        }
    }

    private static void assertEarlyExit(String sql, boolean expected) throws Exception {
        // engine.select wraps the plan in a QueryProgress factory, so the early-exit
        // group by is reached through getBaseFactory; the oracle walks that chain.
        try (RecordCursorFactory factory = engine.select(sql, sqlExecutionContext)) {
            assertEquals(
                    "early-exit detection for: " + sql,
                    expected,
                    QueryRunner.factoryHasEarlyExitGroupBy(factory)
            );
            // Spot-check the factory predicate the walk relies on against the
            // first GroupByNotKeyedRecordCursorFactory on the base chain.
            for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
                if (f instanceof GroupByNotKeyedRecordCursorFactory groupBy) {
                    assertEquals(sql, expected, groupBy.isEarlyExitSupported());
                    break;
                }
            }
        }
    }
}
