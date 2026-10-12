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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class SubsampleTimestampSpanTest extends AbstractCairoTest {
    private static final String ALL_GAP_IDS = """
            id
            1
            2
            3
            4
            """;
    private static final String GAP_IDS = """
            id
            1
            4
            """;
    private static final String LTTB_IDS = """
            id
            1
            2
            4
            """;
    private static final String M4_IDS = """
            id
            1
            2
            3
            5
            6
            7
            8
            10
            """;
    private static final String MINMAX_IDS = """
            id
            2
            3
            7
            8
            """;

    @Test
    public void testLttbGapUnsignedTimestampSpanSubsample() throws Exception {
        assertGapSpans(true);
    }

    @Test
    public void testLttbGapUnsignedTimestampSpanWindow() throws Exception {
        assertGapSpans(false);
    }

    @Test
    public void testLttbUnsignedTimestampSpanSubsample() throws Exception {
        assertTriangleSpans(true);
    }

    @Test
    public void testLttbUnsignedTimestampSpanWindow() throws Exception {
        assertTriangleSpans(false);
    }

    @Test
    public void testM4UnsignedTimestampSpanSubsample() throws Exception {
        assertBucketSpans(true, "m4", "8", M4_IDS);
    }

    @Test
    public void testM4UnsignedTimestampSpanWindow() throws Exception {
        assertBucketSpans(false, "m4", "8", M4_IDS);
    }

    @Test
    public void testMinMaxUnsignedTimestampSpanSubsample() throws Exception {
        assertBucketSpans(true, "minmax", "4", MINMAX_IDS);
    }

    @Test
    public void testMinMaxUnsignedTimestampSpanWindow() throws Exception {
        assertBucketSpans(false, "minmax", "4", MINMAX_IDS);
    }

    private void assertBucketSpans(boolean isSubsample, String function, String target, String expected) throws Exception {
        assertMemoryLeak(() -> {
            createBucketData();
            // Two buckets split at zero: extrema are 2,3 and 7,8; M4 also keeps 1,5 and 6,10.
            assertQuery(query(isSubsample, function, "ts", target)).returns(expected);
            assertQuery(query(isSubsample, function, "(ts::LONG / 1000)::TIMESTAMP", target)).returns(expected);

            // Maximum non-NULL span (2^64 - 2), including Long.MAX_VALUE in the final bucket.
            execute("""
                    UPDATE t SET ts = CASE id
                        WHEN 1 THEN -9_223_372_036_854_775_807
                        WHEN 10 THEN 9_223_372_036_854_775_807
                        ELSE ts::LONG
                    END::TIMESTAMP_NS
                    """);
            assertQuery(query(isSubsample, function, "ts", target)).returns(expected);

            // Spans at signed MAX and MAX+1 keep the split between IDs 5 and 6.
            execute("""
                    UPDATE t SET ts = CASE id
                        WHEN 1 THEN 0
                        WHEN 10 THEN 9_223_372_036_854_775_807
                        ELSE (id - 1)::LONG * 1_000_000_000_000_000_000
                    END::TIMESTAMP_NS
                    """);
            assertQuery(query(isSubsample, function, "ts", target)).returns(expected);
            execute("UPDATE t SET ts = -1::TIMESTAMP_NS WHERE id = 1");
            assertQuery(query(isSubsample, function, "ts", target)).returns(expected);
        });
    }

    private void assertGapSpans(boolean isSubsample) throws Exception {
        assertMemoryLeak(() -> {
            createGapData();
            // 14e18 ns is below 200000d (17.28e18 ns), but above 150000d (12.96e18 ns).
            assertQuery(query(isSubsample, "lttb", "ts", "2, '200000d'")).returns(GAP_IDS);
            assertQuery(query(isSubsample, "lttb", "(ts::LONG / 1000)::TIMESTAMP", "2, '200000d'")).returns(GAP_IDS);
            assertQuery(query(isSubsample, "lttb", "ts", "2, '150000d'")).returns(ALL_GAP_IDS);
            // A microsecond column must retain the unscaled, signed-positive threshold.
            assertQuery(query(isSubsample, "lttb", "ts::LONG::TIMESTAMP", "2, '100000000d'")).returns(ALL_GAP_IDS);

            // Equality above signed MAX is not a gap; exceeding it by 1ns is a gap.
            execute("""
                    UPDATE t SET ts = CASE id
                        WHEN 1 THEN -9_000_000_000_000_000_000
                        WHEN 2 THEN -8_640_000_000_000_000_000
                        WHEN 3 THEN 8_640_000_000_000_000_000
                        ELSE 9_000_000_000_000_000_000
                    END::TIMESTAMP_NS
                    """);
            assertQuery(query(isSubsample, "lttb", "ts", "2, '200000d'")).returns(GAP_IDS);
            execute("UPDATE t SET ts = 8_640_000_000_000_000_001::TIMESTAMP_NS WHERE id = 3");
            assertQuery(query(isSubsample, "lttb", "ts", "2, '200000d'")).returns(ALL_GAP_IDS);

            // 300000d exceeds even the full unsigned range; saturation must not wrap.
            execute("""
                    UPDATE t SET ts = CASE id
                        WHEN 1 THEN -9_223_372_036_854_775_807
                        WHEN 2 THEN -9_223_372_036_854_775_806
                        WHEN 3 THEN 9_223_372_036_854_775_806
                        ELSE 9_223_372_036_854_775_807
                    END::TIMESTAMP_NS
                    """);
            assertQuery(query(isSubsample, "lttb", "ts", "2, '300000d'")).returns(GAP_IDS);
            assertQuery(query(isSubsample, "lttb", "ts", "2, '200000d'")).returns(ALL_GAP_IDS);
        });
    }

    private void assertTriangleSpans(boolean isSubsample) throws Exception {
        assertMemoryLeak(() -> {
            createTriangleData();
            // Exact doubled areas: ID 2 = 84e18, ID 3 = 76e18; first and last are fixed.
            assertQuery(query(isSubsample, "lttb", "ts", "3")).returns(LTTB_IDS);
            assertQuery(query(isSubsample, "lttb", "(ts::LONG / 1000)::TIMESTAMP", "3")).returns(LTTB_IDS);
            // Symmetric endpoints near the full domain still give ID 2 the larger area.
            execute("""
                    UPDATE t SET ts = CASE id
                        WHEN 1 THEN -9_223_372_036_854_775_807
                        WHEN 4 THEN 9_223_372_036_854_775_807
                        ELSE ts::LONG
                    END::TIMESTAMP_NS
                    """);
            assertQuery(query(isSubsample, "lttb", "ts", "3")).returns(LTTB_IDS);
        });
    }

    private static void createBucketData() throws Exception {
        execute("CREATE TABLE t (id INT, v DOUBLE, ts TIMESTAMP_NS)");
        execute("""
                INSERT INTO t VALUES
                (1, 0, -9_000_000_000_000_000_000::TIMESTAMP_NS),
                (2, -10, -7_000_000_000_000_000_000::TIMESTAMP_NS),
                (3, 9, -5_000_000_000_000_000_000::TIMESTAMP_NS),
                (4, 1, -3_000_000_000_000_000_000::TIMESTAMP_NS),
                (5, 2, -1_000_000_000_000_000_000::TIMESTAMP_NS),
                (6, 3, 1_000_000_000_000_000_000::TIMESTAMP_NS),
                (7, -9, 3_000_000_000_000_000_000::TIMESTAMP_NS),
                (8, 10, 5_000_000_000_000_000_000::TIMESTAMP_NS),
                (9, 2, 7_000_000_000_000_000_000::TIMESTAMP_NS),
                (10, 1, 9_000_000_000_000_000_000::TIMESTAMP_NS)
                """);
    }

    private static void createTriangleData() throws Exception {
        execute("CREATE TABLE t (id INT, v DOUBLE, ts TIMESTAMP_NS)");
        execute("""
                INSERT INTO t VALUES
                (1, 0, -8_000_000_000_000_000_000::TIMESTAMP_NS),
                (2, -5, -4_000_000_000_000_000_000::TIMESTAMP_NS),
                (3, -4, 4_000_000_000_000_000_000::TIMESTAMP_NS),
                (4, 1, 8_000_000_000_000_000_000::TIMESTAMP_NS)
                """);
    }

    private static void createGapData() throws Exception {
        execute("CREATE TABLE t (id INT, v DOUBLE, ts TIMESTAMP_NS)");
        execute("""
                INSERT INTO t VALUES
                (1, 0, -8_000_000_000_000_000_000::TIMESTAMP_NS),
                (2, 1, -7_000_000_000_000_000_000::TIMESTAMP_NS),
                (3, 2, 7_000_000_000_000_000_000::TIMESTAMP_NS),
                (4, 3, 8_000_000_000_000_000_000::TIMESTAMP_NS)
                """);
    }

    private static String query(boolean isSubsample, String function, String timestamp, String tail) {
        if (isSubsample) {
            return "SELECT id FROM (SELECT id, v, ts FROM (SELECT id, v, " + timestamp
                    + " AS ts FROM t ORDER BY ts) TIMESTAMP(ts) SUBSAMPLE " + function + "(v, " + tail + "))";
        }
        return "SELECT id FROM (SELECT id, " + function + "(" + timestamp + ", v, " + tail
                + ") OVER (ORDER BY ts) AS keep FROM t) WHERE keep";
    }
}
