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

package io.questdb.test.griffin.engine.orderby;

import io.questdb.cairo.SqlJitMode;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

public class OrderByNegativeLimitFuzzTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(OrderByNegativeLimitFuzzTest.class);

    private final int N_PARTITIONS = 100;

    @Test
    public void testFuzz() throws Exception {

        // Fuzzy testing negative LIMIT and ORDER BY on timestamp column with an interval filter.
        // We compare the result sets with the same query with a positive LIMIT and reverse ORDER BY.

        final Rnd rnd = TestUtils.generateRandom(LOG);
        Object[][] permutations = TestUtils.cartesianProduct(new Object[][]{
                // columns to filter on
                {
                        null,
                        new Object[]{"price"},
                        new Object[]{"price", "AND", "quantity"},
                        new Object[]{"price", "OR", "quantity"},
                        new Object[]{"quantity"},
                },
                {-1, 0, 1, 10, N_PARTITIONS}, // how many partitions should the interval filter span on (or -1 to disable)
                {1, 10, 100}, // maximum number of rows to return
                {false, true}, // JIT mode
                {"asc", "desc"}, // order by direction
        });

        execute("create table x as " +
                "(" +
                "select " +
                "rnd_double() price, " +
                "rnd_long() quantity, " +
                "timestamp_sequence('2024', 24 * 60 * 60 * 1000) ts " +
                "from " +
                "long_sequence(" + N_PARTITIONS + " * 1000)" +
                ") timestamp(ts) partition by day bypass wal");

        for (int i = 0, n = permutations.length; i < n; i++) {
            Object[] permutation = permutations[i];
            String whereClause = generateWhereClause(rnd, (int) permutation[1], (Object[]) permutation[0]);
            int limit = (int) permutation[2];
            boolean jitMode = (boolean) permutation[3];
            String order = (String) permutation[4];

            try {
                assertQuery(whereClause, limit, jitMode, order);
            } catch (AssertionError e) {
                throw new AssertionError("Failed with parameters: " +
                        "whereClause=" + whereClause +
                        ", limit=" + limit +
                        ", jitMode=" + jitMode +
                        ", order=" + order +
                        e);
            }

        }

    }

    private void assertQuery(String whereClause, int limit, boolean jitMode, String order) throws Exception {
        sqlExecutionContext.setJitMode(jitMode ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);

        String query = "select * from x " + whereClause + " order by ts " + (order.equals("asc") ? "desc" : "asc") + " limit " + limit;

        final StringSink expectedSink = new StringSink();
        sink.clear();
        printSql(query, true);
        expectedSink.put(sink);

        query = "select * from x " + whereClause + " order by ts " + order + " limit " + -limit;

        final StringSink actualSink = new StringSink();
        sink.clear();
        printSql(query, true);
        actualSink.put(sink);

        int expectedIndex = expectedSink.indexOf("\n");
        CharSequence expected = expectedSink.subSequence(0, expectedIndex);
        int actualIndex = actualSink.indexOf("\n");
        CharSequence actual = sink.subSequence(0, actualIndex);
        TestUtils.assertEquals(expected, actual);

        expected = expectedSink.subSequence(expectedIndex + 1, expectedSink.length());
        actual = sink.subSequence(actualIndex + 1, sink.length());
        TestUtils.assertReverseLinesEqual(null, expected, actual);
    }

    private @Nullable String generateFilter(Rnd rnd, @Nullable Object[] columns) {
        if (columns == null) {
            return null;
        }

        String filter = columns[0] + " ";
        filter += rnd.nextBoolean() ? "> " : "<= ";
        filter += columns[0] == "price" ? rnd.nextDouble() : rnd.nextLong();
        if (columns.length > 1) {
            filter += " " + columns[1] + " " + columns[2];
            filter += rnd.nextBoolean() ? ">" : "<=";
            filter += columns[2] == "price" ? rnd.nextDouble() : rnd.nextLong();
        }

        return filter;
    }

    private @Nullable String generateInterval(Rnd rnd, int nPartitions) {
        if (nPartitions < 0) {
            return null;
        }

        if (nPartitions == 0) {
            // We stay before the first partitions
            final long start = MicrosFormatUtils.parseTimestamp("2000-01-01") + rnd.nextLong(7 * 24 * 60 * 60) * Micros.SECOND_MICROS;
            final long end = start + rnd.nextLong(7 * 24 * 60 * 60) * Micros.SECOND_MICROS;
            return "ts >= " + start + " AND ts <= " + end;
        }

        // We span over a year, with a partition per day so keep things in this interval
        final long min = MicrosFormatUtils.parseTimestamp("2024-01-01");
        final long max = (N_PARTITIONS - nPartitions) * Micros.DAY_MICROS;
        final long start = min + (max == 0L ? 0 : rnd.nextLong(max));
        final long end = start + nPartitions * Micros.DAY_MICROS + rnd.nextLong(Micros.DAY_MICROS) - 1;
        return "ts >= " + start + " AND ts < " + end;
    }

    private String generateWhereClause(Rnd rnd, int nPartitions, @Nullable Object[] columns) {
        String interval = generateInterval(rnd, nPartitions);
        String filter = generateFilter(rnd, columns);
        if (interval == null && filter == null) {
            return "";
        }
        if (interval == null) {
            return "where " + filter;
        }
        if (filter == null) {
            return "where " + interval;
        }
        return "where " + interval + " AND (" + filter + ")";
    }
}
