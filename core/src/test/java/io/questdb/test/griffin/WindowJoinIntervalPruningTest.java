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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

public class WindowJoinIntervalPruningTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 2);
    }

    @Test
    public void testDynamicOffsetsDoNotNarrowSlaveScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master (sym SYMBOL, lo_bound INT, hi_bound INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE slave (sym SYMBOL, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO master VALUES ('a', 120, 60, '2024-01-01T12:00:00.000000Z')");
            execute("""
                    INSERT INTO slave VALUES
                        ('a', 2.0, '2024-01-01T10:30:00.000000Z'),
                        ('a', 3.0, '2024-01-01T12:30:00.000000Z')
                    """);

            for (boolean isParallel : new boolean[]{false, true}) {
                sqlExecutionContext.setParallelWindowJoinEnabled(isParallel);
                for (boolean isKeyed : new boolean[]{false, true}) {
                    final String query = "SELECT m.ts, m.sym, sum(s.val) AS agg " +
                            "FROM master m WINDOW JOIN slave s" + (isKeyed ? " ON (m.sym = s.sym)" : "") +
                            " RANGE BETWEEN m.lo_bound MINUTES PRECEDING AND m.hi_bound MINUTES FOLLOWING " +
                            "EXCLUDE PREVAILING " +
                            "WHERE m.ts = '2024-01-01T12:00:00.000000Z'::TIMESTAMP " +
                            "ORDER BY m.ts";
                    assertQuery(query)
                            .withPlanContaining(isParallel ? "Async Window Join" : "Window Join")
                            .noLeakCheck()
                            .noRandomAccess()
                            .timestamp("ts")
                            .returns("""
                                    ts\tsym\tagg
                                    2024-01-01T12:00:00.000000Z\ta\t5.0
                                    """);
                }
            }
        });
    }

    @Test
    public void testDisjointMasterIntervalsRemainAUnion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE slave (sym SYMBOL, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO master VALUES
                        ('a', '2024-01-01T12:00:00.000000Z'),
                        ('a', '2024-01-01T13:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO slave VALUES
                        ('a', 10.0, '2024-01-01T10:30:00.000000Z'),
                        ('a', 20.0, '2024-01-01T11:00:00.000000Z'),
                        ('a', 30.0, '2024-01-01T11:40:00.000000Z'),
                        ('a', 40.0, '2024-01-01T12:40:00.000000Z')
                    """);

            for (boolean isParallel : new boolean[]{false, true}) {
                sqlExecutionContext.setParallelWindowJoinEnabled(isParallel);
                for (boolean isKeyed : new boolean[]{false, true}) {
                    final String query = "SELECT m.ts, m.sym, sum(s.val) AS agg " +
                            "FROM master m WINDOW JOIN slave s" + (isKeyed ? " ON (m.sym = s.sym)" : "") +
                            " RANGE BETWEEN 120 MINUTES PRECEDING AND 30 MINUTES PRECEDING " +
                            "EXCLUDE PREVAILING " +
                            "WHERE m.ts != '2020-01-01T00:00:00.000000Z'::TIMESTAMP " +
                            "ORDER BY m.ts";
                    final String plan = isParallel
                            ? (isKeyed ? "Async Window Fast Join" : "Async Window Join")
                            : (isKeyed ? "Window Fast Join" : "Window Join");
                    assertQuery(query)
                            .withPlanContaining(plan)
                            .noLeakCheck()
                            .noRandomAccess()
                            .timestamp("ts")
                            .returns("""
                                    ts\tsym\tagg
                                    2024-01-01T12:00:00.000000Z\ta\t30.0
                                    2024-01-01T13:00:00.000000Z\ta\t50.0
                                    """);
                }
            }
        });
    }

    @Test
    public void testMicroMasterUnitlessRangeDoesNotPruneNanoSlaveMatches() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE slave (sym SYMBOL, val DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO master VALUES ('a', '2024-01-01T00:00:00.000000Z')");
            execute("""
                    INSERT INTO slave VALUES
                        ('a', 2.0, '2023-12-31T23:59:59.999999500Z'),
                        ('a', 3.0, '2024-01-01T00:00:00.000000500Z')
                    """);

            for (boolean isParallel : new boolean[]{false, true}) {
                sqlExecutionContext.setParallelWindowJoinEnabled(isParallel);
                for (boolean isKeyed : new boolean[]{false, true}) {
                    final String query = "SELECT m.ts, m.sym, sum(s.val) AS agg " +
                            "FROM master m WINDOW JOIN slave s" + (isKeyed ? " ON (m.sym = s.sym)" : "") +
                            " RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING " +
                            "EXCLUDE PREVAILING " +
                            "WHERE m.ts = '2024-01-01T00:00:00.000000Z'::TIMESTAMP " +
                            "ORDER BY m.ts";
                    final String plan = isParallel
                            ? (isKeyed ? "Async Window Fast Join" : "Async Window Join")
                            : (isKeyed ? "Window Fast Join" : "Window Join");
                    assertQuery(query)
                            .withPlanContaining(plan)
                            .noLeakCheck()
                            .noRandomAccess()
                            .timestamp("ts")
                            .returns("""
                                    ts\tsym\tagg
                                    2024-01-01T00:00:00.000000Z\ta\t5.0
                                    """);
                }
            }
        });
    }

    @Test
    public void testNanoLowerOffsetOverflowDoesNotPruneMatches() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master (sym SYMBOL, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE slave (sym SYMBOL, val DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO master VALUES ('a', '2020-01-01T00:00:05.000000000Z')");
            execute("INSERT INTO slave VALUES ('a', 3.0, '2020-01-01T00:00:02.000000000Z')");

            for (boolean isParallel : new boolean[]{false, true}) {
                sqlExecutionContext.setParallelWindowJoinEnabled(isParallel);
                for (boolean isKeyed : new boolean[]{false, true}) {
                    final String query = "SELECT m.ts, m.sym, sum(s.val) AS agg " +
                            "FROM master m WINDOW JOIN slave s" + (isKeyed ? " ON (m.sym = s.sym)" : "") +
                            " RANGE BETWEEN 10 SECONDS PRECEDING AND 0 SECONDS FOLLOWING " +
                            "EXCLUDE PREVAILING " +
                            "WHERE m.ts >= '1677-09-21T00:12:44.000000000Z'::TIMESTAMP_NS " +
                            "ORDER BY m.ts";
                    final String plan = isParallel
                            ? (isKeyed ? "Async Window Fast Join" : "Async Window Join")
                            : (isKeyed ? "Window Fast Join" : "Window Join");
                    assertQuery(query)
                            .withPlanContaining(plan)
                            .noLeakCheck()
                            .noRandomAccess()
                            .timestamp("ts")
                            .returns("""
                                    ts\tsym\tagg
                                    2020-01-01T00:00:05.000000000Z\ta\t3.0
                                    """);
                }
            }
        });
    }

    @Test
    public void testNanoMasterSubMicroRangeDoesNotPruneMicroSlaveMatches() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master (sym SYMBOL, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE slave (sym SYMBOL, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO master VALUES
                        ('a', '2024-01-01T00:00:00.000000001Z'),
                        ('a', '2024-01-01T00:00:00.000000999Z')
                    """);
            execute("""
                    INSERT INTO slave VALUES
                        ('a', 2.0, '2024-01-01T00:00:00.000000Z'),
                        ('a', 3.0, '2024-01-01T00:00:00.000001Z')
                    """);

            for (boolean isParallel : new boolean[]{false, true}) {
                sqlExecutionContext.setParallelWindowJoinEnabled(isParallel);
                for (boolean isKeyed : new boolean[]{false, true}) {
                    final String query = "SELECT m.ts, m.sym, sum(s.val) AS agg " +
                            "FROM master m WINDOW JOIN slave s" + (isKeyed ? " ON (m.sym = s.sym)" : "") +
                            " RANGE BETWEEN 1 NANOSECOND PRECEDING AND 1 NANOSECOND FOLLOWING " +
                            "EXCLUDE PREVAILING " +
                            "WHERE m.ts IN ('2024-01-01T00:00:00.000000001Z'::TIMESTAMP_NS, " +
                            "'2024-01-01T00:00:00.000000999Z'::TIMESTAMP_NS) " +
                            "ORDER BY m.ts";
                    final String plan = isParallel
                            ? (isKeyed ? "Async Window Fast Join" : "Async Window Join")
                            : (isKeyed ? "Window Fast Join" : "Window Join");
                    assertQuery(query)
                            .withPlanContaining(plan)
                            .noLeakCheck()
                            .noRandomAccess()
                            .timestamp("ts")
                            .returns("""
                                    ts\tsym\tagg
                                    2024-01-01T00:00:00.000000001Z\ta\t2.0
                                    2024-01-01T00:00:00.000000999Z\ta\t3.0
                                    """);
                }
            }
        });
    }

    @Test
    public void testNanoUpperOffsetOverflowDoesNotPruneMatches() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master (sym SYMBOL, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE slave (sym SYMBOL, val DOUBLE, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO master VALUES ('a', '2020-01-01T00:00:05.000000000Z')");
            execute("INSERT INTO slave VALUES ('a', 3.0, '2020-01-01T00:00:08.000000000Z')");

            for (boolean isParallel : new boolean[]{false, true}) {
                sqlExecutionContext.setParallelWindowJoinEnabled(isParallel);
                for (boolean isKeyed : new boolean[]{false, true}) {
                    final String query = "SELECT m.ts, m.sym, sum(s.val) AS agg " +
                            "FROM master m WINDOW JOIN slave s" + (isKeyed ? " ON (m.sym = s.sym)" : "") +
                            " RANGE BETWEEN 0 SECONDS PRECEDING AND 10 SECONDS FOLLOWING " +
                            "EXCLUDE PREVAILING " +
                            "WHERE m.ts <= '2262-04-11T23:47:12.000000000Z'::TIMESTAMP_NS " +
                            "ORDER BY m.ts";
                    final String plan = isParallel
                            ? (isKeyed ? "Async Window Fast Join" : "Async Window Join")
                            : (isKeyed ? "Window Fast Join" : "Window Join");
                    assertQuery(query)
                            .withPlanContaining(plan)
                            .noLeakCheck()
                            .noRandomAccess()
                            .timestamp("ts")
                            .returns("""
                                    ts\tsym\tagg
                                    2020-01-01T00:00:05.000000000Z\ta\t3.0
                                    """);
                }
            }
        });
    }
}
