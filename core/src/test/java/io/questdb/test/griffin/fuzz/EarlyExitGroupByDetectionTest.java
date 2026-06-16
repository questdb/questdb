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
 * Locks in the early-exit non-keyed group-by detection the query fuzzer's
 * swallow oracle relies on (see {@link QueryRunner#factoryHasEarlyExitGroupBy}).
 * A {@code count_distinct} over a constant compiles to the serial
 * {@link GroupByNotKeyedRecordCursorFactory} with an early-exit cursor, which
 * stops at the first matching row; under the eager parallel filter beneath it a
 * fired {@code test_fault()} can land on a discarded frame, and the oracle must
 * recognise the shape rather than report it as a swallowed error.
 */
public class EarlyExitGroupByDetectionTest extends AbstractCairoTest {

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
