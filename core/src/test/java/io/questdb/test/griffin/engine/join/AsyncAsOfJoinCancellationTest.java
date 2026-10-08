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

package io.questdb.test.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Async AsOf Join stops on a cancelled query: in the span scan of a frame, in the lean join, and
 * between frames.
 */
public class AsyncAsOfJoinCancellationTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 1000);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        final SqlExecutionCircuitBreakerConfiguration config = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public int getCircuitBreakerThrottle() {
                return 0;
            }
        };
        circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine, config) {
            @Override
            protected boolean testConnection(long fd) {
                return false;
            }
        };
        super.setUp();
        ((SqlExecutionContextImpl) sqlExecutionContext).with(circuitBreaker);
    }

    @Test
    public void testCancelledMidQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO quotes SELECT '2024-01-01'::timestamp + x * 100_000L, 'f' || (x % 10), x FROM long_sequence(200000)");
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO trades SELECT '2024-01-01'::timestamp + x * 1_000_000L, 'f' || (x % 12), x FROM long_sequence(20000)");
            circuitBreaker.resetTimer();
            circuitBreaker.setTimeout(Long.MAX_VALUE);
            try (RecordCursorFactory factory = select("SELECT /*+ asof_parallel(t q) */ t.ts, q.bid FROM trades t ASOF JOIN quotes q ON (sym)")) {
                AsyncAsOfJoinTest.findAtom(factory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    circuitBreaker.cancel();
                    try {
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                        }
                        Assert.fail("expected the join to stop on a cancelled query");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getFlyweightMessage().toString(), e.isInterruption());
                    }
                }
            }
        });
    }
}
