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

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class AsOfJoinFuzzTest extends AbstractCairoTest {
    private static final boolean RUN_ALL_PERMUTATIONS = false;
    private final TestTimestampType leftTableTimestampType;
    private final TestTimestampType rightTableTimestampType;

    public AsOfJoinFuzzTest(TestTimestampType leftTimestampType, TestTimestampType rightTimestampType) {
        this.leftTableTimestampType = leftTimestampType;
        this.rightTableTimestampType = rightTimestampType;
    }

    @Parameterized.Parameters(name = "{0}-{1}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO, TestTimestampType.MICRO}, {TestTimestampType.MICRO, TestTimestampType.NANO},
                {TestTimestampType.NANO, TestTimestampType.MICRO}, {TestTimestampType.NANO, TestTimestampType.NANO}
        });
    }

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
        Object[][] allOpts = {
                JoinType.values(),
                {true, false}, // exercise interval intrinsics
                LimitType.values(),
                {true, false}, // exercise filters
                ProjectionType.values(),
                {true, false}, // apply outer projection
                {-1L, 100_000L}, // max tolerance in seconds, -1 = no tolerance
                {true, false} // AVOID BINARY_SEARCH hint
        };

        Object[][] permutations;
        if (RUN_ALL_PERMUTATIONS) {
            permutations = TestUtils.cartesianProduct(allOpts);
        } else {
            // to keep the test fast, we only run a single permutation at a time
            permutations = new Object[1][allOpts.length];
            for (int i = 0; i < allOpts.length; i++) {
                Object[] opts = allOpts[i];
                permutations[0][i] = opts[rnd.nextInt(opts.length)];
            }
        }

        for (int i = 0, n = permutations.length; i < n; i++) {
            Object[] params = permutations[i];
            JoinType joinType = (JoinType) params[0];
            boolean exerciseIntervals = (boolean) params[1];
            LimitType limitType = (LimitType) params[2];
            boolean exerciseFilters = (boolean) params[3];
            ProjectionType projectionType = (ProjectionType) params[4];
            boolean applyOuterProjection = (boolean) params[5];
            long maxTolerance = (long) params[6];
            boolean avoidBinarySearchHint = (boolean) params[7];
            try {
                assertResultSetsMatch0(joinType, exerciseIntervals, limitType, exerciseFilters, projectionType, applyOuterProjection, maxTolerance, avoidBinarySearchHint, rnd);
            } catch (AssertionError e) {
                throw new AssertionError("Failed with parameters: " +
                        "joinType=" + joinType +
                        ", exerciseIntervals=" + exerciseIntervals +
                        ", limitType=" + limitType +
                        ", exerciseFilters=" + exerciseFilters +
                        ", projectionType=" + projectionType +
                        ", applyOuterProjection = " + applyOuterProjection +
                        ", maxTolerance=" + maxTolerance +
                        ", avoidBinarySearchHint=" + avoidBinarySearchHint,
                        e);
            }
        }
    }

    private void assertResultSetsMatch0(
            JoinType joinType,
            boolean exerciseIntervals,
            LimitType limitType,
            boolean exerciseFilters,
            ProjectionType projectionType,
            boolean applyOuterProjection,
            long maxTolerance,
            boolean avoidBinarySearchHint,
            Rnd rnd
    ) throws Exception {
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
            case LT:
                join = " LT";
                onSuffix = (projectionType == ProjectionType.RENAME_COLUMN) ? " on t1.s = t2.s2 " : " on s ";
                break;
            default:
                throw new IllegalArgumentException("Unexpected join type: " + joinType);
        }

        long toleranceSeconds = 0;
        if (maxTolerance != -1) {
            toleranceSeconds = rnd.nextLong(maxTolerance) + 1;
            onSuffix += " tolerance " + toleranceSeconds + "s ";
        }

        StringSink filter = new StringSink();
        if (exerciseIntervals) {
            int n = rnd.nextInt(5) + 1;
            long baseTs = MicrosFormatUtils.parseTimestamp("2000-01-01T00:00:00.000Z");
            for (int i = 0; i < n; i++) {
                if (i == 0) {
                    filter.put(" where ts between '");
                } else {
                    filter.put(" or ts between '");
                }
                int startDays = rnd.nextInt(10 * (i + 1));
                int endDays = startDays + rnd.nextInt(100) + 1;
                long tsStart = baseTs + Micros.DAY_MICROS * startDays;
                long tsEnd = baseTs + Micros.DAY_MICROS * endDays;
                MicrosFormatUtils.appendDateTimeUSec(filter, tsStart);
                filter.put("' and '");
                MicrosFormatUtils.appendDateTimeUSec(filter, tsEnd);
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

        String projection;
        // (ts TIMESTAMP, i INT, s SYMBOL)
        String slaveTimestampColumnName = "ts1";
        switch (projectionType) {
            case NONE:
                projection = "*";
                break;
            case CROSS_COLUMN:
                projection = "s, ts, i";
                break;
            case RENAME_COLUMN:
                projection = "s as s2, ts as ts2, i as i2";
                slaveTimestampColumnName = "ts2";
                break;
            case ADD_COLUMN:
                projection = "*, i as i2";
                break;
            case REMOVE_SYMBOL_COLUMN:
                if (joinType == JoinType.ASOF || joinType == JoinType.LT) {
                    //  key-ed joins can't remove symbol column since it is used as a JOIN key
                    return;
                }
                projection = "ts, i, ts";
                break;
            case REMOVE_TIMESTAMP_COLUMN:
                projection = "i, s";
                slaveTimestampColumnName = null;
                break;
            default:
                throw new IllegalArgumentException("Unexpected projection type: " + projectionType);
        }

        String outerProjection = "*";
        if (applyOuterProjection) {
            char mainProjectionSuffix = projectionType == ProjectionType.RENAME_COLUMN ? '2' : ' ';
            outerProjection = "t1.ts, t2.i" + mainProjectionSuffix;
            slaveTimestampColumnName = null;
        }

        String hint = "";
        if (avoidBinarySearchHint) {
            switch (joinType) {
                case ASOF:
                    // intentional fallthrough
                case ASOF_NONKEYD:
                    hint = " /*+ AVOID_ASOF_BINARY_SEARCH(t1 t2) */ ";
                    break;
                case LT:
                    // intentional fallthrough
                case LT_NONKEYD:
                    hint = " /*+ AVOID_LT_BINARY_SEARCH(t1 t2) */ ";
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected join type: " + joinType);
            }
        }
        String query = "select " + hint + outerProjection + " from " + "t1" + join + " JOIN " + "(select " + projection + " from t2 " + filter + ") t2" + onSuffix;
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

        sink.clear();
        printSql("EXPLAIN " + query, false);
        if (avoidBinarySearchHint) {
            TestUtils.assertNotContains(sink, "AsOf Join Fast Scan");
            TestUtils.assertNotContains(sink, "Lt Join Fast Scan");
        } else if (joinType == JoinType.ASOF_NONKEYD || (joinType == JoinType.ASOF && projectionType == ProjectionType.NONE && !exerciseFilters && !exerciseIntervals)) {
            TestUtils.assertContains(sink, "AsOf Join Fast Scan");
        }

        final StringSink actualSink = new StringSink();
        sink.clear();
        printSql(query, false);
        actualSink.put(sink);
        TestUtils.assertEquals(expectedSink, actualSink);

        if (slaveTimestampColumnName != null) {
            try (RecordCursorFactory factory = select(query);
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Record record = cursor.getRecord();
                RecordMetadata metadata = factory.getMetadata();
                int masterColIndex = metadata.getColumnIndex("ts");
                int slaveColIndex = metadata.getColumnIndex(slaveTimestampColumnName);
                TimestampDriver masterTimestampDriver = ColumnType.getTimestampDriver(metadata.getColumnType(masterColIndex));
                TimestampDriver slaveTimestampDriver = ColumnType.getTimestampDriver(metadata.getColumnType(slaveColIndex));

                while (cursor.hasNext()) {
                    long masterTimestamp = masterTimestampDriver.toMicros(record.getTimestamp(masterColIndex));
                    long slaveTimestamp = slaveTimestampDriver.toMicros(record.getTimestamp(slaveColIndex));
                    Assert.assertTrue(slaveTimestamp <= masterTimestamp);

                    if (maxTolerance != -1 && slaveTimestamp != Numbers.LONG_NULL) {
                        long minSlaveTimestamp = masterTimestamp - (toleranceSeconds * Micros.SECOND_MICROS);
                        Assert.assertTrue("Slave timestamp " + Micros.toString(slaveTimestamp) + " is less than minimum allowed " + Micros.toString(masterTimestamp),
                                slaveTimestamp >= minSlaveTimestamp);
                    }
                }
            }
        }
    }

    private void testFuzz(int tsDuplicatePercentage) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_SQL_ASOF_JOIN_EVACUATION_THRESHOLD, String.valueOf(rnd.nextInt(10) + 1));

        assertMemoryLeak(() -> {
            final int table1Size = rnd.nextPositiveInt() % 1000;
            final int table2Size = rnd.nextPositiveInt() % 1000;

            final TimestampDriver leftTimestampDriver = leftTableTimestampType.getDriver();
            executeWithRewriteTimestamp("CREATE TABLE t1 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal", leftTableTimestampType.getTypeName());
            long ts = leftTimestampDriver.parseFloorLiteral("2000-01-01T00:00:00.000Z");
            ts += leftTimestampDriver.fromHours((int) (rnd.nextLong() % 48));
            for (int i = 0; i < table1Size; i++) {
                if (rnd.nextInt(100) >= tsDuplicatePercentage) {
                    ts += leftTimestampDriver.fromHours((int) rnd.nextLong(24));
                }
                String symbol = "s_" + rnd.nextInt(10);
                execute("INSERT INTO t1 values (" + ts + ", " + i + ", '" + symbol + "');");
            }

            final TimestampDriver rightTimestampDriver = rightTableTimestampType.getDriver();
            executeWithRewriteTimestamp("CREATE TABLE t2 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal", rightTableTimestampType.getTypeName());
            ts = rightTimestampDriver.parseFloorLiteral("2000-01-01T00:00:00.000Z");
            ts += rightTimestampDriver.fromHours((int) rnd.nextLong(48));
            for (int i = 0; i < table2Size; i++) {
                if (rnd.nextInt(100) >= tsDuplicatePercentage) {
                    ts += rightTimestampDriver.fromHours((int) rnd.nextLong(24));
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

            final TimestampDriver leftTimestampDriver = leftTableTimestampType.getDriver();
            executeWithRewriteTimestamp("CREATE TABLE t1 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts)", leftTableTimestampType.getTypeName());
            long ts = leftTimestampDriver.parseFloorLiteral("2000-01-01T00:00:00.000Z");
            ts += leftTimestampDriver.fromHours((int) (rnd.nextLong() % 48));
            for (int i = 0; i < table1Size; i++) {
                if (rnd.nextInt(100) >= tsDuplicatePercentage) {
                    ts += leftTimestampDriver.fromHours((int) rnd.nextLong(24));
                }
                String symbol = "s_" + rnd.nextInt(10);
                execute("INSERT INTO t1 values (" + ts + ", " + i + ", '" + symbol + "');");
            }

            final TimestampDriver rightTimestampDriver = rightTableTimestampType.getDriver();
            executeWithRewriteTimestamp("CREATE TABLE t2 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts)", rightTableTimestampType.getTypeName());
            ts = rightTimestampDriver.parseFloorLiteral("2000-01-01T00:00:00.000Z");
            ts += rightTimestampDriver.fromHours((int) rnd.nextLong(48));
            for (int i = 0; i < table2Size; i++) {
                if (rnd.nextInt(100) >= tsDuplicatePercentage) {
                    ts += rightTimestampDriver.fromHours((int) rnd.nextLong(24));
                }
                String symbol = "s_" + rnd.nextInt(10);
                execute("INSERT INTO t2 values (" + ts + ", " + i + ", '" + symbol + "');");
            }

            assertResultSetsMatch0(rnd);
        });
    }

    private enum JoinType {
        ASOF, ASOF_NONKEYD, LT_NONKEYD, LT
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
        REMOVE_SYMBOL_COLUMN,
        REMOVE_TIMESTAMP_COLUMN
    }
}
