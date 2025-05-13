/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class AsOfJoinFuzzTest extends AbstractCairoTest {
    @Test
    public void testFuzzManyDuplicates() throws Exception {
        testFuzz(50);
    }

    @Test
    public void testFuzzNoDuplicates() throws Exception {
        testFuzz(0);
    }

    @Test
    public void testFuzzPartitionByNoneManyDuplicates() throws Exception {
        testFuzzPartitionByNone(50);
    }

    @Test
    public void testFuzzPartitionByNoneNoDuplicates() throws Exception {
        testFuzzPartitionByNone(0);
    }

    @Test
    public void testFuzzPartitionByNoneSomeDuplicates() throws Exception {
        testFuzzPartitionByNone(10);
    }

    @Test
    public void testFuzzSomeDuplicates() throws Exception {
        testFuzz(10);
    }

    private void assertResultSetsMatch0(Rnd rnd) throws Exception {
        Object[][] allParameterPermutations = TestUtils.cartesianProduct(new Object[][]{
                JoinType.values(),
                {true, false}, // exercise interval intrinsics
                LimitType.values(),
                {true, false}, // exercise filters
                ProjectionType.values(),
                {true, false} // apply outer projection
        });
        for (int i = 0, n = allParameterPermutations.length; i < n; i++) {
            Object[] params = allParameterPermutations[i];
            JoinType joinType = (JoinType) params[0];
            boolean exerciseIntervals = (boolean) params[1];
            LimitType limitType = (LimitType) params[2];
            boolean exerciseFilters = (boolean) params[3];
            ProjectionType projectionType = (ProjectionType) params[4];
            boolean applyOuterProjection = (boolean) params[5];
            try {
                assertResultSetsMatch0(joinType, exerciseIntervals, limitType, exerciseFilters, projectionType, applyOuterProjection, rnd);
            } catch (AssertionError e) {
                throw new AssertionError("Failed with parameters: " +
                        "joinType=" + joinType +
                        ", exerciseIntervals=" + exerciseIntervals +
                        ", limitType=" + limitType +
                        ", exerciseFilters=" + exerciseFilters +
                        ", projectionType=" + projectionType +
                        ", applyOuterProjection = " + applyOuterProjection,
                        e);
            }
        }
    }

    private void assertResultSetsMatch0(JoinType joinType, boolean exerciseIntervals, LimitType limitType, boolean exerciseFilters, ProjectionType projectionType, boolean applyOuterProjection, Rnd rnd) throws Exception {
        String join;
        String onSuffix = "";
        switch (joinType) {
            case ASOF:
                join = " ASOF";
                onSuffix = (projectionType == ProjectionType.RENAME_COLUMN) ? " on t1.s = t2.s2 " : " on s ";
                break;
            case ASOF_NONKEYD:
                join = " ASOF";
                break;
            case LT_NONKEYD:
                join = " LT";
                break;
            default:
                throw new IllegalArgumentException("Unexpected join type: " + joinType);
        }

        StringSink filter = new StringSink();
        if (exerciseIntervals) {
            int n = rnd.nextInt(5) + 1;
            long baseTs = TimestampFormatUtils.parseTimestamp("2000-01-01T00:00:00.000Z");
            for (int i = 0; i < n; i++) {
                if (i == 0) {
                    filter.put(" where ts between '");
                } else {
                    filter.put(" or ts between '");
                }
                int startDays = rnd.nextInt(10 * (i + 1));
                int endDays = startDays + rnd.nextInt(100) + 1;
                long tsStart = baseTs + Timestamps.DAY_MICROS * startDays;
                long tsEnd = baseTs + Timestamps.DAY_MICROS * endDays;
                TimestampFormatUtils.appendDateTimeUSec(filter, tsStart);
                filter.put("' and '");
                TimestampFormatUtils.appendDateTimeUSec(filter, tsEnd);
                filter.put("'");
            }
        }
        if (exerciseFilters) {
            int n = rnd.nextInt(5) + 1;
            for (int i = 0; i < n; i++) {
                if (i == 0 && !exerciseIntervals) {
                    filter.put("where i != ");
                } else {
                    filter.put(" and i != ");
                }
                int toBeExcluded = rnd.nextInt(100);
                filter.put(toBeExcluded);
            }
            // let's exercise symbol columns too,
            // symbols and symbol sources can be tricky
            filter.put(" and s = 's_0' ");
        }

        String projection = "";
        // (ts TIMESTAMP, i INT, s SYMBOL)
        switch (projectionType) {
            case NONE:
                projection = "*";
                break;
            case CROSS_COLUMN:
                projection = "s, ts, i";
                break;
            case RENAME_COLUMN:
                projection = "s as s2, ts as ts2, i as i2";
                break;
            case ADD_COLUMN:
                projection = "*, i as i2";
                break;
            case REMOVE_SYMBOL_COLUMN:
                if (joinType == JoinType.ASOF) {
//                     key-ed ASOF join can't remove symbol column since it is used as a JOIN key
                    return;
                }
                projection = "ts, i, ts";
                break;
        }

        String outerProjection = "*";
        if (applyOuterProjection) {
            char mainProjectionSuffix = projectionType == ProjectionType.RENAME_COLUMN ? '2' : ' ';
            outerProjection = "t1.ts, t2.i" + mainProjectionSuffix;
        }

        // we can always hint to use BINARY_SEARCH, it's ignored in cases where it doesn't apply
        String query = "select /*+ USE_ASOF_BINARY_SEARCH(t1 t2) */ " + outerProjection + " from " + "t1" + join + " JOIN " + "(select " + projection + " from t2 " + filter + ") t2" + onSuffix;
        int limit;
        switch (limitType) {
            case POSITIVE_LIMIT:
                limit = rnd.nextInt(100);
                query = "select * from (" + query + " ) limit " + limit;
                break;
            case NEGATIVE_LIMIT:
                limit = rnd.nextInt(100) + 1;
                query = "select * from (" + query + ") limit -" + limit;
                break;
            case NO_LIMIT:
                break;
        }

        final StringSink expectedSink = new StringSink();
        sink.clear();
        printSql(query, true);
        expectedSink.put(sink);

        // sanity check: make sure non-keyd ASOF join use the Fast-path
        if (joinType == JoinType.ASOF_NONKEYD) {
            sink.clear();
            printSql("EXPLAIN " + query, false);
            TestUtils.assertContains(sink, "AsOf Join Fast Scan");
        }

        final StringSink actualSink = new StringSink();
        sink.clear();
        printSql(query, false);
        actualSink.put(sink);
        TestUtils.assertEquals(expectedSink, actualSink);
    }

    private void testFuzz(int tsDuplicatePercentage) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            final int table1Size = rnd.nextPositiveInt() % 1000;
            final int table2Size = rnd.nextPositiveInt() % 1000;

            execute("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            long ts = TimestampFormatUtils.parseTimestamp("2000-01-01T00:00:00.000Z");
            ts += Timestamps.HOUR_MICROS * (rnd.nextLong() % 48);
            for (int i = 0; i < table1Size; i++) {
                if (rnd.nextInt(100) >= tsDuplicatePercentage) {
                    ts += Timestamps.HOUR_MICROS * rnd.nextLong(24);
                }
                String symbol = "s_" + rnd.nextInt(10);
                execute("INSERT INTO t1 values (" + ts + ", " + i + ", '" + symbol + "');");
            }

            execute("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal");
            ts = TimestampFormatUtils.parseTimestamp("2000-01-01T00:00:00.000Z");
            ts += Timestamps.HOUR_MICROS * rnd.nextLong(48);
            for (int i = 0; i < table2Size; i++) {
                if (rnd.nextInt(100) >= tsDuplicatePercentage) {
                    ts += Timestamps.HOUR_MICROS * rnd.nextLong(24);
                }
                String symbol = "s_" + rnd.nextInt(10);
                execute("INSERT INTO t2 values (" + ts + ", " + i + ", '" + symbol + "');");
            }

            assertResultSetsMatch0(rnd);
        });
    }

    private void testFuzzPartitionByNone(int tsDuplicatePercentage) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            final int table1Size = rnd.nextPositiveInt() % 1000;
            final int table2Size = rnd.nextPositiveInt() % 1000;

            execute("CREATE TABLE t1 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts)");
            long ts = TimestampFormatUtils.parseTimestamp("2000-01-01T00:00:00.000Z");
            ts += Timestamps.HOUR_MICROS * (rnd.nextLong() % 48);
            for (int i = 0; i < table1Size; i++) {
                if (rnd.nextInt(100) >= tsDuplicatePercentage) {
                    ts += Timestamps.HOUR_MICROS * rnd.nextLong(24);
                }
                String symbol = "s_" + rnd.nextInt(10);
                execute("INSERT INTO t1 values (" + ts + ", " + i + ", '" + symbol + "');");
            }

            execute("CREATE TABLE t2 (ts TIMESTAMP, i INT, s SYMBOL) timestamp(ts)");
            ts = TimestampFormatUtils.parseTimestamp("2000-01-01T00:00:00.000Z");
            ts += Timestamps.HOUR_MICROS * rnd.nextLong(48);
            for (int i = 0; i < table2Size; i++) {
                if (rnd.nextInt(100) >= tsDuplicatePercentage) {
                    ts += Timestamps.HOUR_MICROS * rnd.nextLong(24);
                }
                String symbol = "s_" + rnd.nextInt(10);
                execute("INSERT INTO t2 values (" + ts + ", " + i + ", '" + symbol + "');");
            }

            assertResultSetsMatch0(rnd);
        });
    }

    private enum JoinType {
        ASOF, ASOF_NONKEYD, LT_NONKEYD
    }

    private enum LimitType {
        NO_LIMIT,
        POSITIVE_LIMIT,
        NEGATIVE_LIMIT
    }

    private enum ProjectionType {
        NONE,
        CROSS_COLUMN,
        RENAME_COLUMN,
        ADD_COLUMN,
        REMOVE_SYMBOL_COLUMN
    }
}
