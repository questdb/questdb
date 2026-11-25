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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.join.AsyncAsOfJoinAtom;
import io.questdb.griffin.engine.join.AsyncAsOfJoinRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Async AsOf Join stops on a cancelled query: between frames, and inside one frame in the span scan,
 * the walk and the serial way.
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
    public void testCancelledInsideFrameSerial() throws Exception {
        assertCancelledInsideFrame(AsyncAsOfJoinRecordCursorFactory.WALK_AUTO, AsyncAsOfJoinAtom.KEYS_SERIAL);
    }

    @Test
    public void testCancelledInsideFrameSpan() throws Exception {
        assertCancelledInsideFrame(AsyncAsOfJoinRecordCursorFactory.WALK_NEVER, AsyncAsOfJoinAtom.KEYS_PARALLEL);
    }

    @Test
    public void testCancelledInsideFrameWalk() throws Exception {
        assertCancelledInsideFrame(AsyncAsOfJoinRecordCursorFactory.WALK_ALWAYS, AsyncAsOfJoinAtom.KEYS_PARALLEL);
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

    // One master frame of 100k rows against 1M slave rows: the join consults the breaker hundreds of
    // times inside the frame, and only a handful of times outside it. A breaker that trips on its
    // 20th consultation must stop the query, which only checks inside the frame can do.
    private void assertCancelledInsideFrame(int walkMode, int keysMode) throws Exception {
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 200_000);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 200_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO quotes SELECT '2024-01-01'::timestamp + x * 10_000L, 'f' || (x % 10), x FROM long_sequence(1000000)");
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO trades SELECT '2024-01-01'::timestamp + x * 100_000L + 1, 'f' || (x % 12), x FROM long_sequence(100000)");
            final TripAfterChecks breaker = new TripAfterChecks(engine);
            ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
            AsyncAsOfJoinRecordCursorFactory.WALK_MODE = walkMode;
            AsyncAsOfJoinAtom.KEYS_MODE = keysMode;
            try (RecordCursorFactory factory = select("SELECT /*+ asof_parallel(t q) */ t.ts, q.bid FROM trades t ASOF JOIN quotes q ON (sym)")) {
                AsyncAsOfJoinTest.findAtom(factory);
                breaker.tripAfter(20);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                    Assert.fail("expected the join to stop inside the frame, after " + breaker.getChecks() + " checks");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getFlyweightMessage().toString(), e.isInterruption() || e.isCancellation());
                }
            } finally {
                AsyncAsOfJoinRecordCursorFactory.WALK_MODE = AsyncAsOfJoinRecordCursorFactory.WALK_AUTO;
                AsyncAsOfJoinAtom.KEYS_MODE = AsyncAsOfJoinAtom.KEYS_AUTO;
                ((SqlExecutionContextImpl) sqlExecutionContext).with(circuitBreaker);
            }
        });
    }

    // thread-safe, so that the workers consult it directly; cancels itself on the n-th consultation
    private static class TripAfterChecks extends AtomicBooleanCircuitBreaker {
        private final AtomicLong checks = new AtomicLong();
        private volatile long tripAfter = Long.MAX_VALUE;

        TripAfterChecks(CairoEngine engine) {
            super(engine);
        }

        @Override
        public void statefulThrowExceptionIfTripped() {
            if (checks.incrementAndGet() >= tripAfter) {
                cancel();
            }
            super.statefulThrowExceptionIfTripped();
        }

        long getChecks() {
            return checks.get();
        }

        void tripAfter(long n) {
            checks.set(0);
            tripAfter = n;
        }
    }
}
