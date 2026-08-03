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

package io.questdb.test.griffin.engine;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression guard for the circuit-breaker throttle defect: empty/instant/optimized-to-empty queries
 * must still observe a tripped breaker even though they consult it only a handful of times.
 * <p>
 * Crucially this runs at the PRODUCTION throttle (2,000,000). {@code QueryExecutionTimeoutTest} pins
 * the throttle to 0, so it never exercises the throttled path where the single-shot open/build/
 * pre-dispatch checks live. With the buggy throttle (skip the first {@code throttle-1} calls) these
 * queries complete silently; with the fix (perform a real check on the first consultation after each
 * per-query reset) they abort.
 */
public class EmptyQueryCancellabilityTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        // Production throttle: throttle=0 would mask the defect this test guards.
        final SqlExecutionCircuitBreakerConfiguration config = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public int getCircuitBreakerThrottle() {
                return 2_000_000;
            }
        };
        circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine, config) {
            @Override
            protected boolean testConnection(long fd) {
                return false;
            }
        };
        ((SqlExecutionContextImpl) sqlExecutionContext).with(circuitBreaker);
        super.setUp();
    }

    @Test
    public void testEmptyAndInstantQueriesAbortOnTrippedBreaker() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (s symbol, v long, ts timestamp) timestamp(ts) partition by DAY");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                // each shape maps to a single-shot open/build/pre-dispatch check over an empty base
                assertAbortsWhenBreakerTripped(compiler, "select 1");                              // instant single row (long_sequence(1))
                assertAbortsWhenBreakerTripped(compiler, "select * from t where false");           // optimized down to EmptyTable
                assertAbortsWhenBreakerTripped(compiler, "select * from t");                        // bare page-frame scan, zero frames
                assertAbortsWhenBreakerTripped(compiler, "select * from t where v < 0");            // async filter, pre-dispatch
                assertAbortsWhenBreakerTripped(compiler, "select v, count() from t");              // async group by, pre-dispatch
                assertAbortsWhenBreakerTripped(compiler, "select count() from t");                 // count, instant single row
                assertAbortsWhenBreakerTripped(compiler, "select distinct v from t");              // async group by, pre-dispatch
                assertAbortsWhenBreakerTripped(compiler, "select * from t order by v");            // sort, before consuming base
                assertAbortsWhenBreakerTripped(compiler, "select * from t a join t b on a.s = b.s"); // hash join build
                assertAbortsWhenBreakerTripped(compiler, "select * from t a left join t b on a.s = b.s"); // outer hash join build
                assertAbortsWhenBreakerTripped(compiler, "select * from t latest on ts partition by s"); // deferred latest by, at open
                assertAbortsWhenBreakerTripped(compiler, "select sum(v) from t");                  // vector group by, instant
            }
        });
    }

    private void assertAbortsWhenBreakerTripped(SqlCompiler compiler, String query) throws Exception {
        // Disarm while compiling so only execution observes the trip.
        circuitBreaker.setTimeout(Long.MAX_VALUE);
        circuitBreaker.resetTimer();
        final CompiledQuery cc = compiler.compile(query, sqlExecutionContext);
        try (RecordCursorFactory factory = cc.getRecordCursorFactory()) {
            // Arm the breaker exactly where production calls resetTimer() before draining the cursor:
            // a fresh throttle window plus an already-elapsed timeout, so the first real check aborts.
            circuitBreaker.resetTimer();
            circuitBreaker.setTimeout(-1000);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                    // drain
                }
            }
            Assert.fail("expected '" + query + "' to abort on a tripped circuit breaker, but it completed");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "timeout, query aborted");
            Assert.assertTrue("expected interruption flag for: " + query, e.isInterruption());
        }
    }
}
