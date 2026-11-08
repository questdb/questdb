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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.PropertyKey;
import io.questdb.griffin.SqlException;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import java.util.function.IntFunction;

/**
 * The VARCHAR GROUP BY functions return a value that lives in a buffer of the GROUP BY allocator. Hash
 * maps - those of joins, GROUP BY, DISTINCT and window partitions - and regexp_replace() read such a
 * value through {@code Utf8Sequence.ptr()} rather than byte by byte. These tests feed the result of each
 * function to each of those operators and assert that the operator sees the value the function returned.
 */
public class VarcharGroupByResultConsumerTest extends AbstractCairoTest {
    // first(), last(), first_not_null(), last_not_null() and arg_max() keep a pointer to a stable value,
    // such as a column's, and copy any other value, such as a cast's; each function runs over both
    private static final String[] ARGUMENTS = {"v", "v::string::varchar"};
    // %s is the argument
    private static final String[] FUNCTIONS = {
            "max(%s)",
            "min(%s)",
            "mode(%s)",
            "first(%s)",
            "last(%s)",
            "first_not_null(%s)",
            "last_not_null(%s)",
            "arg_max(%s, d)",
            "arg_max(%s, i)",
            "arg_max(%s, l)",
            "arg_max(%s, ts)",
    };
    private static final String LONG_VALUE = "q".repeat(1_000);
    // Group k of src holds VALUES[k - 1] twice, so every function returns that value for the group. The
    // values cover empty, a single byte, the 6-byte inlined prefix, 9 bytes - the size of the header of the
    // buffer -, multibyte and 4-byte UTF-8, values of one length, a long value and NULL.
    private static final String[] VALUES = {
            "",
            "c",
            "abcdef",
            "abcdefghi",
            "abcdefghij",
            "żółw",
            "ab😀",
            "banana",
            "cherry",
            LONG_VALUE,
            null
    };
    private static final int NULL_GROUP = VALUES.length;

    @Override
    @Before
    public void setUp() {
        // small frames and sharding, so the parallel GROUP BY merges the values of a group
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 64);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 2);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 2);
        super.setUp();
    }

    @Test
    public void testAsOfJoin() throws Exception {
        assertEveryFunction(
                "SELECT a.k, b.v FROM (SELECT ts, k, %s v FROM src SAMPLE BY 1d) a " +
                        "ASOF JOIN (SELECT v, ts FROM dimts ORDER BY ts LIMIT 100) b ON (v) ORDER BY a.k",
                "AsOf Join Light",
                true,
                "k\tv\n" + matchedRows() + NULL_GROUP + "\t\n"
        );
    }

    @Test
    public void testDistinct() throws Exception {
        // DISTINCT keeps one row per value; the join to dimk names the values that survived it
        assertEveryFunction(
                "SELECT d.k FROM (SELECT DISTINCT v FROM (SELECT k, %s v FROM src)) x JOIN dimk d ON x.v = d.v ORDER BY d.k",
                // DISTINCT compiles to GROUP BY
                "GroupBy",
                false,
                "k\n" + perGroup(k -> k + "\n")
        );
    }

    @Test
    public void testFullJoin() throws Exception {
        assertEveryFunction(
                "SELECT a.k, d.v FROM (SELECT k, %s v FROM src) a FULL JOIN dim d ON a.v = d.v ORDER BY a.k, d.v",
                "Hash Full Outer Join Light",
                false,
                "k\tv\nnull\tkiwi\n" + matchedRows() + NULL_GROUP + "\t\n"
        );
    }

    @Test
    public void testGroupBy() throws Exception {
        assertEveryFunction(
                "SELECT min(k) k, v, count() c FROM (SELECT k, %s v FROM src) GROUP BY v ORDER BY k",
                "GroupBy",
                true,
                "k\tv\tc\n" + perGroup(k -> k + "\t" + valueOf(k) + "\t1\n")
        );
    }

    @Test
    public void testGroupByRollup() throws Exception {
        // the grand total row has a NULL v, the smallest k and the count of all groups
        assertEveryFunction(
                "SELECT min(k) k, v, count() c FROM (SELECT k, %s v FROM src) GROUP BY ROLLUP(v) ORDER BY k, c",
                null,
                true,
                "k\tv\tc\n" + perGroup(k -> k + "\t" + valueOf(k) + "\t1\n" + (k == 1 ? "1\t\t" + NULL_GROUP + "\n" : ""))
        );
    }

    @Test
    public void testGrownValueAsJoinKey() throws Exception {
        // the second row of each group grows the buffer that holds the value of max(), min() or last()
        final String b40 = "b".repeat(40);
        final String a40 = "a".repeat(40);
        final String maxRows = "k\tv\n1\t" + b40 + "\n2\tz\n";
        final String minRows = "k\tv\n1\ta\n2\t" + a40 + "\n";
        final String firstRows = "k\tv\n1\ta\n2\tz\n";
        final String lastRows = "k\tv\n1\t" + b40 + "\n2\t" + a40 + "\n";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE grown (k INT, v VARCHAR, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO grown VALUES " +
                    "(1, 'a', 1.0, '2024-01-01T00:00:00.000001Z'), " +
                    "(2, 'z', 1.0, '2024-01-01T00:00:00.000002Z'), " +
                    "(1, '" + b40 + "', 2.0, '2024-01-01T00:00:00.000003Z'), " +
                    "(2, '" + a40 + "', 2.0, '2024-01-01T00:00:00.000004Z')");
            execute("CREATE TABLE grown_dim (v VARCHAR)");
            execute("INSERT INTO grown_dim VALUES ('a'), ('z'), ('" + b40 + "'), ('" + a40 + "')");
            final String[][] cases = {
                    {"max(%s)", maxRows},
                    {"min(%s)", minRows},
                    {"first(%s)", firstRows},
                    {"first_not_null(%s)", firstRows},
                    {"last(%s)", lastRows},
                    {"last_not_null(%s)", lastRows},
                    {"arg_max(%s, d)", lastRows},
            };
            for (String argument : ARGUMENTS) {
                for (String[] c : cases) {
                    final String function = String.format(c[0], argument);
                    assertQuery("SELECT a.k, d.v FROM (SELECT k, " + function + " v FROM grown) a JOIN grown_dim d ON a.v = d.v ORDER BY a.k")
                            .noLeakCheck()
                            .withPlanContaining("Hash Join Light")
                            .returns(c[1]);
                }
            }
        });
    }

    @Test
    public void testInnerJoinBuild() throws Exception {
        // the GROUP BY is the side the join hashes
        assertEveryFunction(
                "SELECT a.k, d.v FROM dim d JOIN (SELECT k, %s v FROM src) a ON a.v = d.v ORDER BY a.k",
                "Hash Join Light",
                false,
                "k\tv\n" + matchedRows()
        );
    }

    @Test
    public void testInnerJoinBuildAndProbe() throws Exception {
        // both sides read a GROUP BY: values of one length must match only themselves; NULL keys match
        // each other, as they do in every hash join
        assertEveryFunction(
                "WITH a AS (SELECT k, %s v FROM src) SELECT a.k, b.k FROM a JOIN a b ON a.v = b.v ORDER BY a.k",
                "Hash Join Light",
                false,
                "k\tk1\n" + perGroup(k -> k + "\t" + k + "\n")
        );
    }

    @Test
    public void testInnerJoinProbe() throws Exception {
        // the GROUP BY is the side the join probes with
        assertEveryFunction(
                "SELECT a.k, d.v FROM (SELECT k, %s v FROM src) a JOIN dim d ON a.v = d.v ORDER BY a.k",
                "Hash Join Light",
                false,
                "k\tv\n" + matchedRows()
        );
    }

    @Test
    public void testLeftJoin() throws Exception {
        assertEveryFunction(
                "SELECT a.k, d.v FROM (SELECT k, %s v FROM src) a LEFT JOIN dim d ON a.v = d.v ORDER BY a.k",
                "Hash Left Outer Join Light",
                false,
                "k\tv\n" + matchedRows() + NULL_GROUP + "\t\n"
        );
    }

    @Test
    public void testLtJoin() throws Exception {
        assertEveryFunction(
                "SELECT a.k, b.v FROM (SELECT ts, k, %s v FROM src SAMPLE BY 1d) a " +
                        "LT JOIN (SELECT v, ts FROM dimts ORDER BY ts LIMIT 100) b ON (v) ORDER BY a.k",
                "Lt Join Light",
                true,
                "k\tv\n" + matchedRows() + NULL_GROUP + "\t\n"
        );
    }

    @Test
    public void testMergedValueAsJoinKey() throws Exception {
        // 4 groups over 4_000 rows that span many frames and partitions: the parallel GROUP BY computes
        // each group on several workers and merges the values, so the join reads values that merge() kept
        final String createSql = "CREATE TABLE psrc AS (" +
                "SELECT (x % 4)::int k, ('g' || (x % 4) || '_' || (1_000_000 + x))::varchar v, ('m' || (x % 4))::varchar m, " +
                "x::double d, (x * 3_600_000_000L)::timestamp ts FROM long_sequence(4_000)" +
                ") TIMESTAMP(ts) PARTITION BY DAY";
        // values of group k are 'gk_' followed by 1_000_000 + x, for x = k, k + 4, ... up to 4_000:
        // the smallest x of a group comes first, the largest last
        final StringBuilder dim = new StringBuilder("INSERT INTO pdim VALUES ");
        final StringBuilder maxRows = new StringBuilder("k\tv\n");
        final StringBuilder minRows = new StringBuilder("k\tv\n");
        final StringBuilder modeRows = new StringBuilder("k\tv\n");
        for (int k = 0; k < 4; k++) {
            final int minX = k == 0 ? 4 : k;
            final int maxX = 4_000 - (4 - k) % 4;
            final String min = "g" + k + "_" + (1_000_000 + minX);
            final String max = "g" + k + "_" + (1_000_000 + maxX);
            dim.append(k == 0 ? "" : ", ").append("('").append(min).append("'), ('").append(max).append("'), ('m").append(k).append("')");
            minRows.append(k).append('\t').append(min).append('\n');
            maxRows.append(k).append('\t').append(max).append('\n');
            modeRows.append(k).append('\t').append('m').append(k).append('\n');
        }
        final String[][] cases = {
                {"max(%s)", "v", maxRows.toString()},
                {"min(%s)", "v", minRows.toString()},
                {"first(%s)", "v", minRows.toString()},
                {"first_not_null(%s)", "v", minRows.toString()},
                {"last(%s)", "v", maxRows.toString()},
                {"last_not_null(%s)", "v", maxRows.toString()},
                {"arg_max(%s, d)", "v", maxRows.toString()},
                {"mode(%s)", "m", modeRows.toString()},
        };
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)))) {
                TestUtils.execute(pool, (ignore, compiler, ctx) -> {
                    execute(compiler, createSql, ctx);
                    execute(compiler, "CREATE TABLE pdim (v VARCHAR)", ctx);
                    execute(compiler, dim, ctx);
                    for (String[] c : cases) {
                        for (String argument : new String[]{c[1], c[1] + "::string::varchar"}) {
                            final String function = String.format(c[0], argument);
                            final String query = "SELECT a.k, d.v FROM (SELECT k, " + function + " v FROM psrc) a JOIN pdim d ON a.v = d.v ORDER BY a.k";
                            assertQuery(query)
                                    .noLeakCheck()
                                    .withCompiler(compiler)
                                    .withContext(ctx)
                                    .assertsPlanContaining("Hash Join Light", "Async Group By");
                            assertQuery(query)
                                    .noLeakCheck()
                                    .withCompiler(compiler)
                                    .withContext(ctx)
                                    .returns(c[2]);
                        }
                    }
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testNotKeyedJoinProbe() throws Exception {
        // a GROUP BY without keys returns one row, which the join probes with
        assertEveryFunction(
                "SELECT d.v FROM (SELECT %s v FROM src WHERE k = 9) a JOIN dim d ON a.v = d.v ORDER BY d.v",
                "Hash Join Light",
                false,
                "v\ncherry\n"
        );
    }

    @Test
    public void testNotKeyedJoinProbeLongValue() throws Exception {
        // over the column, first(), last(), first_not_null(), last_not_null() and arg_max() keep a pointer to
        // the long value rather than a copy of it
        assertEveryFunction(
                "SELECT length(d.v) n FROM (SELECT %s v FROM src WHERE k = 10) a JOIN dim d ON a.v = d.v ORDER BY n",
                "Hash Join Light",
                false,
                "n\n" + LONG_VALUE.length() + "\n"
        );
    }

    @Test
    public void testRegexpReplace() throws Exception {
        // a pattern that captures the whole value takes the path that reads the value through ptr()
        assertEveryFunction(
                "SELECT k, regexp_replace(v, '^(.+)$', '$1') r FROM (SELECT k, %s v FROM src) ORDER BY k",
                null,
                true,
                "k\tr\n" + perGroup(k -> k + "\t" + valueOf(k) + "\n")
        );
    }

    @Test
    public void testRightJoin() throws Exception {
        assertEveryFunction(
                "SELECT a.k, d.v FROM (SELECT k, %s v FROM src) a RIGHT JOIN dim d ON a.v = d.v ORDER BY a.k",
                "Hash Right Outer Join Light",
                false,
                "k\tv\nnull\tkiwi\n" + matchedRows()
        );
    }

    @Test
    public void testSerialGroupByJoinProbe() throws Exception {
        // UNION ALL is not a frame source, so the GROUP BY runs serially; it doubles every group, which
        // changes no function's value
        assertEveryFunction(
                "SELECT a.k, d.v FROM (SELECT k, %s v FROM (SELECT * FROM src UNION ALL SELECT * FROM src)) a JOIN dim d ON a.v = d.v ORDER BY a.k",
                "GroupBy vectorized: false",
                false,
                "k\tv\n" + matchedRows()
        );
    }

    @Test
    public void testWindowPartitionBy() throws Exception {
        // every group has a value of its own, so every window partition holds one row
        assertEveryFunction(
                "SELECT k, count() OVER (PARTITION BY v) c FROM (SELECT k, %s v FROM src) ORDER BY k",
                "Window",
                true,
                "k\tc\n" + perGroup(k -> k + "\t1\n")
        );
    }

    private void assertEveryFunction(String queryTemplate, @Nullable String planFragment, boolean isSizeKnown, String expected) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String argument : ARGUMENTS) {
                for (String function : FUNCTIONS) {
                    final String query = String.format(queryTemplate, String.format(function, argument));
                    assertQuery(query)
                            .noLeakCheck()
                            .withPlanContaining(planFragment)
                            .expectSize(isSizeKnown)
                            .returns(expected);
                }
            }
        });
    }

    private static void createTables() throws SqlException {
        execute("CREATE TABLE src (k INT, v VARCHAR, d DOUBLE, i INT, l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        // the second row of a group has the larger d, i, l and ts, which arg_max() picks either way
        final StringBuilder src = new StringBuilder("INSERT INTO src VALUES ");
        for (int k = 1; k <= VALUES.length; k++) {
            for (int row = 1; row <= 2; row++) {
                final int n = 2 * k + row;
                src.append(k == 1 && row == 1 ? "" : ", ")
                        .append('(').append(k).append(", ").append(literal(VALUES[k - 1]))
                        .append(", ").append(n).append(".0, ").append(n).append(", ").append(n)
                        .append(", '2024-01-01T00:00:00.0000").append(n < 10 ? "0" : "").append(n).append("Z')");
            }
        }
        execute(src);

        // dim holds every value but NULL, and a value of no group
        execute("CREATE TABLE dim (v VARCHAR)");
        // dimk numbers every value, NULL included, by its group
        execute("CREATE TABLE dimk (k INT, v VARCHAR)");
        // dimts holds the values of dim the day before the SAMPLE BY bucket of src
        execute("CREATE TABLE dimts (v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        final StringBuilder dim = new StringBuilder("INSERT INTO dim VALUES ('kiwi')");
        final StringBuilder dimk = new StringBuilder("INSERT INTO dimk VALUES ");
        final StringBuilder dimts = new StringBuilder("INSERT INTO dimts VALUES ('kiwi', '2023-12-31T00:00:00.000000Z')");
        for (int k = 1; k <= VALUES.length; k++) {
            final String value = literal(VALUES[k - 1]);
            dimk.append(k == 1 ? "" : ", ").append('(').append(k).append(", ").append(value).append(')');
            if (VALUES[k - 1] != null) {
                dim.append(", (").append(value).append(')');
                dimts.append(", (").append(value).append(", '2023-12-31T00:00:00.000000Z')");
            }
        }
        execute(dim);
        execute(dimk);
        execute(dimts);
    }

    private static String literal(@Nullable String value) {
        return value == null ? "NULL" : "'" + value + "'";
    }

    // groups whose value dim holds, by k
    private static String matchedRows() {
        final StringBuilder sb = new StringBuilder();
        for (int k = 1; k < NULL_GROUP; k++) {
            sb.append(k).append('\t').append(VALUES[k - 1]).append('\n');
        }
        return sb.toString();
    }

    private static String perGroup(IntFunction<String> row) {
        final StringBuilder sb = new StringBuilder();
        for (int k = 1; k <= VALUES.length; k++) {
            sb.append(row.apply(k));
        }
        return sb.toString();
    }

    private static String valueOf(int k) {
        final String value = VALUES[k - 1];
        return value == null ? "" : value;
    }
}
