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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByAtom;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GroupedDistinctTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        // Force even the small fixtures through pair-shard merge, parallel pair collapse,
        // and final group-shard merge.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1);
    }

    @Test
    public void testAdaptiveGroupedDistinctIntLongNullAndZeroSemantics() throws Exception {
        assertMemoryLeak(() -> {
            createIntLongTable();

            assertQuery("select g, count_distinct(v) c from gd group by g order by g")
                    .withPlanContaining("groupedDistinct: adaptive")
                    .expectSize()
                    .returns("""
                            g\tc
                            -1\t1
                            0\t1
                            1\t2
                            2\t1
                            3\t0
                            """);

            // Exercise cached-factory clear/reopen and the filtered nested path.
            assertQuery("select g, count_distinct(v) c from gd where v is null or v > 0 group by g order by g")
                    .withPlanNotContaining("groupedDistinct: adaptive")
                    .expectSize()
                    .returns("""
                            g\tc
                            -1\t0
                            0\t0
                            1\t2
                            3\t0
                            """);

            assertQuery("select g, count_distinct(v) c from gd group by g order by g")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            g\tc
                            -1\t1
                            0\t1
                            1\t2
                            2\t1
                            3\t0
                            """);
        });
    }

    @Test
    public void testAdaptiveGroupedDistinctShardsAtBatchBoundaryAndReopens() throws Exception {
        final int batchSize = 128;
        final int rowCount = 32_000;
        final int shardingThreshold = 2_000;
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_BATCH_SIZE, batchSize);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, shardingThreshold);

        assertMemoryLeak(() -> {
            execute("create table gd_batch as ("
                    + "select (x % 4)::int g, x::long v from long_sequence(" + rowCount + ")"
                    + ")");

            final String adaptiveSql = "select g, count_distinct(v) c from gd_batch group by g order by g";
            final String nestedSql = "select g, count_distinct(v + 0) c from gd_batch group by g order by g";

            try (RecordCursorFactory factory = select(adaptiveSql)) {
                final AsyncGroupByAtom atom = (AsyncGroupByAtom) TestUtils.findAtom(factory, adaptiveSql);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        // Drain the first execution, which starts with unsharded pair fragments.
                    }
                    Assert.assertTrue(atom.isGroupedDistinctFlat());
                    Assert.assertTrue(atom.isSharded());
                    final long unshardedPairCount = atom.getGroupedDistinctUnshardedPairCount();
                    Assert.assertTrue(unshardedPairCount > shardingThreshold);
                    Assert.assertTrue(unshardedPairCount < rowCount);
                }

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        // The first merge leaves a sharded hint for this factory execution.
                    }
                    Assert.assertTrue(atom.isGroupedDistinctFlat());
                    Assert.assertTrue(atom.isSharded());
                    Assert.assertEquals(0, atom.getGroupedDistinctUnshardedPairCount());
                }
            }

            assertSqlCursors(nestedSql, adaptiveSql);
        });
    }

    @Test
    public void testAdaptiveGroupedDistinctIPv4TimestampTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table gd_ip (g ipv4, v timestamp)");
            execute("insert into gd_ip values "
                    + "('1.2.3.4', 0), ('1.2.3.4', 0), ('1.2.3.4', 1000000), "
                    + "('5.6.7.8', null), ('5.6.7.8', null)");

            assertQuery("select g, count_distinct(v) c from gd_ip group by g order by g")
                    .withPlanContaining("groupedDistinct: adaptive")
                    .expectSize()
                    .returns("""
                            g\tc
                            1.2.3.4\t2
                            5.6.7.8\t0
                            """);
        });
    }

    @Test
    public void testAdaptiveGroupedDistinctSymbolKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table gd_sym (s symbol, v long)");
            execute("insert into gd_sym values ('a', 1), ('a', 1), ('a', 2), ('b', null), ('b', null), (null, 7), (null, 7)");

            assertQuery("select s, count_distinct(v) c from gd_sym group by s order by s")
                    .withPlanContaining("groupedDistinct: adaptive")
                    .expectSize()
                    .returns("""
                            s\tc
                            \t1
                            a\t2
                            b\t0
                            """);
        });
    }

    @Test
    public void testGroupedDistinctTwoGroupKeysRetainsNestedPath() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table gd_two (g short, s symbol, v long, x long)");
            execute("insert into gd_two "
                    + "select (x % 7 - 3)::short, "
                    + "cast(case when x % 3 = 0 then 'a' when x % 3 = 1 then 'b' else null end as symbol), "
                    + "case when x % 11 = 0 then null else (x % 17)::long end, x "
                    + "from long_sequence(20000)");

            final String adaptiveSql = "select g, s, sum(v), count(), count_distinct(v) "
                    + "from gd_two group by g, s order by g, s";
            final String nestedSql = "select g, s, sum(v), count(), count_distinct(v + 0) "
                    + "from gd_two group by g, s order by g, s";

            assertQuery(adaptiveSql).assertsPlanNotContaining("groupedDistinct: adaptive");
            assertSqlCursors(nestedSql, adaptiveSql);
            assertSqlCursors(nestedSql, adaptiveSql);

            // Cover the filtered nested path and cached-factory reuse as well.
            final String filteredAdaptiveSql = "select g, s, sum(v), count(), count_distinct(v) "
                    + "from gd_two where x % 5 != 0 group by g, s order by g, s";
            final String filteredNestedSql = "select g, s, sum(v), count(), count_distinct(v + 0) "
                    + "from gd_two where x % 5 != 0 group by g, s order by g, s";
            assertSqlCursors(filteredNestedSql, filteredAdaptiveSql);
            assertSqlCursors(filteredNestedSql, filteredAdaptiveSql);
        });
    }

    @Test
    public void testAdaptiveGroupedDistinctFallsBackForUniqueGroups() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table gd_unique as ("
                    + "select x::int g, (x % 17)::long v from long_sequence(10000)"
                    + ")");

            final String adaptiveSql = "select g, sum(v), count_distinct(v) "
                    + "from gd_unique group by g order by g";
            final String nestedSql = "select g, sum(v), count_distinct(v + 0) "
                    + "from gd_unique group by g order by g";

            // Every sampled row has a unique group key, deterministically selecting the existing
            // nested implementation. Keep this branch covered even though the factory remains
            // eligible for adaptive grouped DISTINCT.
            assertQuery(adaptiveSql).assertsPlanContaining("groupedDistinct: adaptive");
            assertQuery(nestedSql).assertsPlanNotContaining("groupedDistinct: adaptive");
            assertSqlCursors(nestedSql, adaptiveSql);
            assertSqlCursors(nestedSql, adaptiveSql);
        });
    }

    @Test
    public void testAdaptiveGroupedDistinctWithParquetLateMaterializationAndStatefulAggregates() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table gd_parquet as ("
                    + "select timestamp_sequence(0, 10000000) ts, "
                    + "(x % 8)::int g, "
                    + "case when x % 8 = 0 then null "
                    + "when x % 5 = 0 then 42::long else (x % 101)::long end v, "
                    + "(x % 97)::int keep "
                    + "from long_sequence(20000)"
                    + ") timestamp(ts) partition by day");
            execute("alter table gd_parquet convert partition to parquet where ts < '1970-01-03'");

            final String adaptiveSql = "select g, first(v), last(v), mode(v), count_distinct(v) "
                    + "from gd_parquet where ts in '1970-01-01' and keep = 0 group by g order by g";
            final String nestedSql = "select g, first(v), last(v), mode(v), count_distinct(v + 0) "
                    + "from gd_parquet where ts in '1970-01-01' and keep = 0 group by g order by g";

            // The selective filter initially engages Parquet late materialization. The result
            // simultaneously covers the established filtered nested path, first/last row-id
            // merging, pointer-backed mode state, and an all-null group.
            assertQuery(adaptiveSql).assertsPlanNotContaining("groupedDistinct: adaptive");
            assertQuery(nestedSql).assertsPlanNotContaining("groupedDistinct: adaptive");
            assertSqlCursors(nestedSql, adaptiveSql);
            assertSqlCursors(nestedSql, adaptiveSql);
        });
    }

    @Test
    public void testAdaptiveGroupedDistinctWithOrdinaryAggregates() throws Exception {
        assertMemoryLeak(() -> {
            createIntLongTable();

            // Keep DISTINCT last, as it is in ClickBench Q9, to verify that eligibility does
            // not assume the DISTINCT function is the first aggregate.
            assertQuery("select g, sum(v) s, count() n, avg(v) a, count_distinct(v) c from gd group by g order by g")
                    .withPlanContaining("groupedDistinct: adaptive")
                    .expectSize()
                    .returns("""
                            g\ts\tn\ta\tc
                            -1\t-2\t3\t-1.0\t1
                            0\t0\t3\t0.0\t1
                            1\t40\t3\t13.333333333333334\t2
                            2\t0\t2\t0.0\t1
                            3\tnull\t2\tnull\t0
                            """);

            // Exercise the filtered nested path and cached-factory clear/reopen with mixed state.
            assertQuery("select g, sum(v) s, count() n, avg(v) a, count_distinct(v) c "
                    + "from gd where v is null or v > 0 group by g order by g")
                    .withPlanNotContaining("groupedDistinct: adaptive")
                    .expectSize()
                    .returns("""
                            g\ts\tn\ta\tc
                            -1\tnull\t1\tnull\t0
                            0\tnull\t1\tnull\t0
                            1\t40\t3\t13.333333333333334\t2
                            3\tnull\t2\tnull\t0
                            """);

            assertQuery("select g, sum(v) s, count() n, avg(v) a, count_distinct(v) c from gd group by g order by g")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            g\ts\tn\ta\tc
                            -1\t-2\t3\t-1.0\t1
                            0\t0\t3\t0.0\t1
                            1\t40\t3\t13.333333333333334\t2
                            2\t0\t2\t0.0\t1
                            3\tnull\t2\tnull\t0
                            """);
        });
    }

    @Test
    public void testAdaptiveGroupedDistinctWithPointerBackedOrdinaryAggregate() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table gd_mode as ("
                    + "select (x % 7)::int g, "
                    + "case when x % 5 = 0 then 42::long else (x % 101)::long end v "
                    + "from long_sequence(50000)"
                    + ")");

            final String adaptiveSql = "select g, mode(v) m, count_distinct(v) c "
                    + "from gd_mode group by g order by g";
            final String nestedSql = "select g, mode(v) m, count_distinct(v + 0) c "
                    + "from gd_mode group by g order by g";

            assertQuery(adaptiveSql)
                    .withPlanContaining("groupedDistinct: adaptive")
                    .expectSize()
                    .returns("""
                            g\tm\tc
                            0\t42\t101
                            1\t42\t101
                            2\t42\t101
                            3\t42\t101
                            4\t42\t101
                            5\t42\t101
                            6\t42\t101
                            """);

            // The expression keeps the oracle on the existing nested DISTINCT path. Repeat the
            // comparison to cover clear/reopen of both flat-path map contexts.
            assertSqlCursors(nestedSql, adaptiveSql);
            assertSqlCursors(nestedSql, adaptiveSql);

            // Widen the ordinary state beyond the unordered-map entry limit while retaining the
            // pointer-backed mode state. This covers the same placeholder replacement invariant
            // when the output context falls back to OrderedMap, as ClickBench Q9 does.
            final String wideAdaptiveSql = "select g, sum(v), count(), avg(v), mode(v), count_distinct(v) "
                    + "from gd_mode group by g order by g";
            final String wideNestedSql = "select g, sum(v), count(), avg(v), mode(v), count_distinct(v + 0) "
                    + "from gd_mode group by g order by g";
            assertSqlCursors(wideNestedSql, wideAdaptiveSql);
            assertSqlCursors(wideNestedSql, wideAdaptiveSql);
        });
    }

    @Test
    public void testAdaptiveGroupedDistinctWithPointerBackedOrdinaryAggregateWithoutSharding() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1_000_000);
        assertMemoryLeak(() -> {
            execute("create table gd_mode_owner as ("
                    + "select (x % 7)::int g, "
                    + "case when x % 5 = 0 then 42::long else (x % 101)::long end v "
                    + "from long_sequence(5000)"
                    + ")");

            final String adaptiveSql = "select g, mode(v) m, count_distinct(v) c "
                    + "from gd_mode_owner group by g order by g";
            final String nestedSql = "select g, mode(v) m, count_distinct(v + 0) c "
                    + "from gd_mode_owner group by g order by g";

            assertQuery(adaptiveSql)
                    .withPlanContaining("groupedDistinct: adaptive")
                    .expectSize()
                    .returns("""
                            g\tm\tc
                            0\t42\t101
                            1\t42\t101
                            2\t42\t101
                            3\t42\t101
                            4\t42\t101
                            5\t42\t101
                            6\t42\t101
                            """);
            assertSqlCursors(nestedSql, adaptiveSql);
        });
    }

    @Test
    public void testAdaptiveGroupedDistinctUsesFlatPathForReusedGroups() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table gd_flat as ("
                    + "select (x % 2)::int g, (x % 7)::long v from long_sequence(1000)"
                    + ")");

            // Two groups across 1,000 sampled rows select the flat pair topology. The forced
            // sharding threshold then covers pair-shard collapse and the final output merge.
            assertQuery("select g, sum(v) s, count() n, avg(v) a, count_distinct(v) c "
                    + "from gd_flat group by g order by g")
                    .withPlanContaining("groupedDistinct: adaptive")
                    .expectSize()
                    .returns("""
                            g\ts\tn\ta\tc
                            0\t1503\t500\t3.006\t7
                            1\t1500\t500\t3.0\t7
                            """);
        });
    }

    @Test
    public void testGenericLongKeyUsesFlatPathOnFirstExecution() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table gd_long_flat as ("
                    + "select (x % 2)::long g, (x % 7)::long v from long_sequence(1000)"
                    + ")");

            final String sql = "select g, count_distinct(v) c from gd_long_flat group by g order by g";
            final String expected = """
                    g\tc
                    0\t7
                    1\t7
                    """;

            assertQuery(sql).assertsPlanContaining("groupedDistinct: adaptive");
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncGroupByAtom atom = (AsyncGroupByAtom) TestUtils.findAtom(factory, sql);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, sink);
                }
                Assert.assertTrue(atom.isGroupedDistinctFlat());

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, sink);
                }
                Assert.assertTrue(atom.isGroupedDistinctFlat());
            }
        });
    }

    @Test
    public void testGenericVarcharKeyComposesWithOrdinaryAggregatesAndReopens() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table gd_varchar as ("
                    + "select (x % 2)::varchar g, (x % 5)::long v, "
                    + "('url_' || (x % 3))::varchar url, "
                    + "('title_' || (x % 4))::varchar title "
                    + "from long_sequence(1000)"
                    + ")");

            final String adaptiveSql = "select g, min(url), min(title), count(), count_distinct(v) "
                    + "from gd_varchar where title like '%title%' and g is not null group by g order by g";
            final String nestedSql = "select g, min(url), min(title), count(), count_distinct(v + 0) "
                    + "from gd_varchar where title like '%title%' and g is not null group by g order by g";
            final String expected = """
                    g\tmin\tmin1\tcount\tcount_distinct
                    0\turl_0\ttitle_0\t500\t5
                    1\turl_0\ttitle_1\t500\t5
                    """;

            assertQuery(adaptiveSql).assertsPlanNotContaining("groupedDistinct: adaptive");
            try (RecordCursorFactory factory = select(adaptiveSql)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, sink);
                }

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, sink);
                }

                // Cover clear/reopen of the filtered nested path with a generic OrderedMap key
                // and pointer-backed minima.
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.assertCursor(expected, cursor, factory.getMetadata(), true, sink);
                }
            }
            assertSqlCursors(nestedSql, adaptiveSql);
        });
    }

    @Test
    public void testGenericVarcharKeyRetainsNestedPathForUniqueGroups() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table gd_varchar_unique as ("
                    + "select x::varchar g, (x % 17)::long v from long_sequence(1000)"
                    + ")");

            final String sql = "select g, count_distinct(v) from gd_varchar_unique group by g";
            assertQuery(sql).assertsPlanContaining("groupedDistinct: adaptive");
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncGroupByAtom atom = (AsyncGroupByAtom) TestUtils.findAtom(factory, sql);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        // Schema-driven sampling sees one group per row and selects nested state
                        // on the first execution.
                    }
                }
                Assert.assertFalse(atom.isGroupedDistinctFlat());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        // Schema-driven sampling sees one group per row and retains nested state.
                    }
                }
                Assert.assertFalse(atom.isGroupedDistinctFlat());
            }
        });
    }

    @Test
    public void testGenericWiderCompositeKeyRetainsNestedPath() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table gd_three_keys as ("
                    + "select (x % 2)::int g, (x % 3)::int h, (x % 5)::long k, "
                    + "(x % 7)::long v from long_sequence(1000)"
                    + ")");

            final String adaptiveSql = "select g, h, k, count_distinct(v) c "
                    + "from gd_three_keys group by g, h, k order by g, h, k";
            final String nestedSql = "select g, h, k, count_distinct(v + 0) c "
                    + "from gd_three_keys group by g, h, k order by g, h, k";

            assertQuery(adaptiveSql).assertsPlanNotContaining("groupedDistinct: adaptive");
            assertSqlCursors(nestedSql, adaptiveSql);
            assertSqlCursors(nestedSql, adaptiveSql);
        });
    }

    @Test
    public void testSemanticEligibilityAndUnsupportedDistinctShapes() throws Exception {
        assertMemoryLeak(() -> {
            createIntLongTable();

            assertQuery("select g, count_distinct(v) c, count_distinct(v + 1) c2 from gd group by g order by g")
                    .withPlanNotContaining("groupedDistinct: adaptive")
                    .expectSize()
                    .returns("""
                            g\tc\tc2
                            -1\t1\t1
                            0\t1\t1
                            1\t2\t2
                            2\t1\t1
                            3\t0\t0
                            """);

            assertQuery("select g, count_distinct(v + 1) c from gd group by g order by g")
                    .withPlanNotContaining("groupedDistinct: adaptive")
                    .expectSize()
                    .returns("""
                            g\tc
                            -1\t1
                            0\t1
                            1\t2
                            2\t1
                            3\t0
                            """);

            execute("create table gd_long (g long, v long)");
            execute("insert into gd_long values (1, 10), (1, 10), (1, 20), (2, null)");
            assertQuery("select g, count_distinct(v) c from gd_long group by g order by g")
                    .withPlanContaining("groupedDistinct: adaptive")
                    .expectSize()
                    .returns("""
                            g\tc
                            1\t2
                            2\t0
                            """);

            execute("create table gd_two_generic (g int, h int, v long)");
            execute("insert into gd_two_generic values (1, 2, 10), (1, 2, 10), (1, 2, 20)");
            assertQuery("select g, h, count_distinct(v) c from gd_two_generic group by g, h")
                    .withPlanNotContaining("groupedDistinct: adaptive")
                    .expectSize()
                    .returns("""
                            g\th\tc
                            1\t2\t2
                            """);
        });
    }

    private static void createIntLongTable() throws Exception {
        execute("create table gd (g int, v long)");
        execute("insert into gd values "
                + "(-1, -1), (-1, -1), (-1, null), "
                + "(0, 0), (0, 0), (0, null), "
                + "(1, 10), (1, 20), (1, 10), "
                + "(2, 0), (2, 0), "
                + "(3, null), (3, null)");
    }
}
