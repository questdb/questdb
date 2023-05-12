/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

public class OrderByTimeoutTest extends AbstractGriffinTest {

    public static int breakConnection = -1;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public boolean checkConnection() {
                return breakConnection == 0 || --breakConnection == 0;
            }
        };
        circuitBreaker = new NetworkSqlExecutionCircuitBreaker(circuitBreakerConfiguration, MemoryTag.NATIVE_HTTP_CONN) {
            @Override
            public boolean checkIfTripped(long millis, int fd) {
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
        AbstractGriffinTest.setUpStatic();
    }

    @AfterClass
    public static void tearDownStatic() throws Exception {
        AbstractGriffinTest.tearDownStatic();
        circuitBreaker = Misc.free(circuitBreaker);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        breakConnection = -1;
    }

    @Test
    public void testTimeoutLimitedSizeSortedLightRecordCursor() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE trips as (" +
                    "select rnd_long() a, rnd_long() b, timestamp_sequence('2022-01-03', 50000000000) ts from long_sequence(20)" +
                    ") timestamp(ts) partition by day;");

            int breakTestLimit = 20;
            String sql = "select * from trips " +
                    " where ts between '2022-01-03' and '2022-02-01' and a > 1234567890L order by b desc limit 10";

            testSql(breakTestLimit, sql);
        });
    }

    @Test
    public void testTimeoutSortedLightRecordCursorFactory() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE trips as (" +
                    "select rnd_long() a, rnd_long() b, timestamp_sequence('2022-01-03', 50000000) ts from long_sequence(20)" +
                    ") timestamp(ts) partition by day;");

            testSql(5, "select * from trips where a > 1234567890L order by b desc");
        });
    }

    @Test
    public void testTimeoutSortedRecordCursorFactory() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE trips as (" +
                    "select rnd_long() a, rnd_long() b, timestamp_sequence('2022-01-03', 50000000) ts from long_sequence(20)" +
                    ") timestamp(ts) partition by day;");
            testSql(20, "select * from trips where a > 1234567890L " +
                    "union all " +
                    "select rnd_long() a, rnd_long() b, timestamp_sequence('2022-01-03', 50000000) ts from long_sequence(20) " +
                    "order by b desc");
        });
    }

    private static void testSql(int breakTestLimit, String sql) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            for (int i = 0; i < breakTestLimit; i++) {
                breakConnection = i;
                try {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        TestUtils.printCursor(cursor, factory.getMetadata(), true, sink, printer);
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
