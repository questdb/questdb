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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OrderByTimeoutTest extends AbstractCairoTest {
    private static int breakConnection = -1;

    @Before
    public void setUp() {
        circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public boolean checkConnection() {
                return breakConnection == 0 || --breakConnection == 0;
            }

            @Override
            public @NotNull MillisecondClock getClock() {
                return () -> {
                    if (breakConnection == 0 || --breakConnection == 0) {
                        return 100;
                    }
                    return 0;
                };
            }

            @Override
            public long getQueryTimeout() {
                return 50;
            }
        };
        circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine, circuitBreakerConfiguration, MemoryTag.NATIVE_HTTP_CONN) {
            @Override
            public boolean checkIfTripped(long millis, long fd) {
                return breakConnection == 0 || --breakConnection == 0;
            }

            @Override
            public void statefulThrowExceptionIfTripped() {
                statefulThrowExceptionIfTrippedNoThrottle();
            }

            @Override
            public void statefulThrowExceptionIfTrippedNoThrottle() {
                if (breakConnection == 0 || --breakConnection == 0) {
                    throw CairoException.nonCritical().put("timeout").setInterruption(true);
                }
            }
        };
        ((SqlExecutionContextImpl) sqlExecutionContext).with(circuitBreaker);
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        breakConnection = -1;
    }

    @Test
    public void testTimeoutLimitedSizeSortedLightRecordCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trips as (" +
                            "select rnd_long() a, rnd_long() b, timestamp_sequence('2022-01-03', 50000000000) ts from long_sequence(100)" +
                            ") timestamp(ts) partition by day;"
            );

            // This test somehow used to rely on code calling "getClock()" to decrement the breakLimit.
            // The clock is cached and relying on it is not a good idea.
            // We should select more rows than max value of the break limit.
            int breakTestLimit = 20;
            String sql = "select * from trips order by b desc limit 30";
            final boolean oldParallelTopKEnabled = sqlExecutionContext.isParallelTopKEnabled();
            sqlExecutionContext.setParallelTopKEnabled(false);
            try {
                testSql(breakTestLimit, sql);
            } finally {
                sqlExecutionContext.setParallelTopKEnabled(oldParallelTopKEnabled);
            }
        });
    }

    @Test
    public void testTimeoutSortedLightRecordCursorFactory() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trips as (" +
                            "select rnd_long() a, rnd_long() b, timestamp_sequence('2022-01-03', 50000000) ts from long_sequence(20)" +
                            ") timestamp(ts) partition by day;"
            );

            testSql(5, "select * from trips where a > 1234567890L order by b desc");
        });
    }

    @Test
    public void testTimeoutSortedRecordCursorFactory() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trips as (" +
                            "select rnd_long() a, rnd_long() b, timestamp_sequence('2022-01-03', 50000000) ts from long_sequence(20)" +
                            ") timestamp(ts) partition by day;"
            );
            testSql(
                    20,
                    "select * from trips where a > 1234567890L " +
                            "union all " +
                            "select rnd_long() a, rnd_long() b, timestamp_sequence('2022-01-03', 50000000) ts from long_sequence(20) " +
                            "order by b desc"
            );
        });
    }

    private static void testSql(int breakTestLimit, String sql) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            for (int i = 0; i < breakTestLimit; i++) {
                breakConnection = i;
                try {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        println(factory, cursor);
                    }
                    Assert.fail();
                } catch (CairoException ex) {
                    Assert.assertTrue(ex.isInterruption());
                }
                breakConnection = -1;
            }
        }
    }
}
