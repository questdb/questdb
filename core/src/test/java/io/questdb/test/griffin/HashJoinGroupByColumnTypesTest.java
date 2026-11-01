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

import io.questdb.cairo.SqlJitMode;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.assertDifferential;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.context;

/**
 * Column types the fused plan reads outside the eleven the operator started with. A probe column
 * never reaches the build's row heap - the reducer reads it straight off the page frame - so a
 * probe column of any type may appear in a filter, a grouping key or an aggregate argument. A
 * build column does reach the heap, so it answers to
 * {@link io.questdb.griffin.HashJoinGroupByMetadata#isSupportedValueType(int)}, which now holds
 * every fixed-size type and leaves out the variable-size ones. A build column that only the build's
 * filters read never reaches the heap: the build runs its scan's WHERE filter and an outer join's ON
 * condition on the build alone over its page frames, so such a filter may read a column of any type.
 * <p>
 * The LEFT JOIN in every query pins which input is which: the planner always builds the slave of
 * an outer join, so {@code p} is the probe and {@code b} the build whatever the two sizes are.
 * {@code IntHashJoinBuildTest} pins the row heap's own layout and getters; this class pins what a
 * query returns once the planner routes such a column to the operator.
 */
public class HashJoinGroupByColumnTypesTest extends AbstractCairoTest {
    // Fixed-size types the row heap copies, so both inputs may carry them. Each width of GEOHASH
    // and of DECIMAL is a type of its own, and each takes its own arm of the layout switch.
    private static final String[] PAYLOAD_COLUMNS = {
            "ip", "u", "l256", "g1", "g3", "g6", "g8",
            "dec8", "dec16", "dec32", "dec64", "dec128", "dec256"
    };
    // Variable-size types, which the row heap cannot copy, so only the probe may carry them outside
    // the build's filters.
    private static final String[] PROBE_ONLY_COLUMNS = {"vc", "str"};

    private final HashJoinBuildMode buildMode;
    private final HashJoinPayloadLayout payloadLayout;

    public HashJoinGroupByColumnTypesTest() {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        this.payloadLayout = HashJoinPayloadLayout.random(rnd);
        this.buildMode = HashJoinBuildMode.random(rnd);
    }

    @Before
    public void setUpPayloadLayoutAndBuildMode() {
        payloadLayout.apply(node1.getConfigurationOverrides());
        buildMode.apply(node1.getConfigurationOverrides(), sqlExecutionContext);
    }

    @After
    public void restorePageFrameSizes() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testBuildColumnsOfFixedSizeTypesOnNativeAndParquetFrames() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                for (int format = 0; format < 2; format++) {
                    if (format == 1) {
                        execute("ALTER TABLE p CONVERT PARTITION TO PARQUET WHERE t < '2020-01-02'");
                        execute("ALTER TABLE b CONVERT PARTITION TO PARQUET WHERE t < '2020-01-02'");
                    }
                    for (String column : PAYLOAD_COLUMNS) {
                        // The column is a payload the heap copies and the grouping key that reads
                        // it back. The LEFT JOIN's misses take the type's own null instead.
                        assertDifferential("SELECT b." + column + " k, count(*) n FROM p LEFT JOIN b ON p.id=b.id ORDER BY k",
                                context, true);
                        // A post-join filter over the payload, which reads it on the reducer.
                        assertDifferential("SELECT count(*) n FROM p LEFT JOIN b ON p.id=b.id WHERE b." + column + " IS NOT NULL",
                                context, true);
                        assertDifferential("SELECT count(b." + column + ") n FROM p LEFT JOIN b ON p.id=b.id", context, true);
                        // A build-side filter, which the build cursor applies before the copy.
                        assertDifferential("SELECT count(*) n, sum(p.v) pv FROM p LEFT JOIN b ON p.id=b.id "
                                + "AND b." + column + " IS NOT NULL", context, true);
                    }
                    // Every payload type at once, so the layout has to pack widths 1 to 32 in one row.
                    StringBuilder projection = new StringBuilder();
                    for (String column : PAYLOAD_COLUMNS) {
                        projection.append("b.").append(column).append(", ");
                    }
                    // Both build routes copy through the same row heap: the narrow INT layout and
                    // the map the composite key stages into.
                    for (String on : new String[]{"p.id=b.id", "p.id=b.id AND p.l=b.l"}) {
                        assertDifferential("SELECT " + projection + "count(*) n FROM p LEFT JOIN b ON " + on
                                + " ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13", context, true);
                    }
                }
            }
        });
    }

    @Test
    public void testBuildColumnsOfVariableSizeTypesKeepTheOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                for (String column : new String[]{"vc", "str", "bin", "arr"}) {
                    // The row heap cannot copy a variable-size payload, so the shape keeps the
                    // ordinary plan rather than failing the query.
                    assertDifferential("SELECT b." + column + " k, count(*) n FROM p LEFT JOIN b ON p.id=b.id",
                            context, false);
                }
                for (String predicate : new String[]{"b.vc IS NOT NULL", "b.str IS NOT NULL",
                        "b.bin IS NOT NULL", "array_sum(b.arr) > 0.5"}) {
                    assertDifferential("SELECT count(*) n FROM p LEFT JOIN b ON p.id=b.id WHERE " + predicate,
                            context, false);
                }
                for (String column : PROBE_ONLY_COLUMNS) {
                    assertDifferential("SELECT count(b." + column + ") n FROM p LEFT JOIN b ON p.id=b.id", context, false);
                }
            }
        });
    }

    @Test
    public void testBuildFiltersOverVariableSizeColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            insertProbeDay();
            // b gains two text columns after its first two days. The rows that follow fill the
            // second day's last half hour and a third day, so the first day lacks both columns, the
            // second has a column top over all but its last row, and the third has none.
            execute("ALTER TABLE b ADD COLUMN vc2 VARCHAR");
            execute("ALTER TABLE b ADD COLUMN s2 STRING");
            execute("""
                    INSERT INTO b (id, l, v, vc2, s2, t)
                    SELECT rnd_int(0, 2, 1), rnd_int(0, 2, 1)::LONG, x * 0.5,
                        CASE WHEN x % 2 = 0 THEN 'qv' ELSE NULL END::VARCHAR,
                        CASE WHEN x % 3 = 0 THEN 'qs' ELSE NULL END::STRING,
                        timestamp_sequence('2020-01-02T23:30', 1_800_000_000L)
                    FROM long_sequence(12)
                    """);
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                final String payload = payloadLayout == HashJoinPayloadLayout.COPIED
                        ? "buildPayload: copied when the probe is large enough" : null;
                for (int format = 0; format < 2; format++) {
                    if (format == 1) {
                        // The topped days turn Parquet, and the third, the last, stays native.
                        execute("ALTER TABLE p CONVERT PARTITION TO PARQUET WHERE t < '2020-01-02'");
                        execute("ALTER TABLE b CONVERT PARTITION TO PARQUET WHERE t < '2020-01-03'");
                    }
                    for (boolean isJit : new boolean[]{false, true}) {
                        context.setJitMode(isJit ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
                        // A compiled NULL check falls back to the interpreted filter on a frame with
                        // a column top, which reads the top's rows as NULL.
                        final String nullCheck = isJit ? "buildJitFilter: " : "buildFilter: ";
                        // An INNER join builds b, the smaller input.
                        final String inner = "SELECT count(*) n, sum(b.v) bv FROM p JOIN b ON p.id = b.id WHERE ";
                        assertAgainstOrdinaryPlan(inner + "b.vc2 ~ 'q'", context, "buildFilter: ", payload);
                        assertAgainstOrdinaryPlan(inner + "b.s2 = 'qs'", context, "buildFilter: ", payload);
                        assertAgainstOrdinaryPlan(inner + "b.vc2 IS NULL", context, nullCheck, payload);
                        assertAgainstOrdinaryPlan(inner + "b.s2 IS NOT NULL", context, nullCheck, payload);
                        final String left = "SELECT count(*) n, count(b.id) m, sum(b.v) bv FROM p LEFT JOIN b ON p.id = b.id AND ";
                        assertAgainstOrdinaryPlan(left + "b.vc2 ~ 'q'", context, "buildOnFilter: ", payload);
                        assertAgainstOrdinaryPlan(left + "b.vc2 IS NOT NULL", context, "buildOnFilter: ", payload);
                        assertAgainstOrdinaryPlan(left + "b.s2 IS NULL", context, "buildOnFilter: ", payload);
                        assertAgainstOrdinaryPlan("SELECT count(*) n, count(b.id) m, sum(b.v) bv FROM p "
                                        + "LEFT JOIN (SELECT * FROM b WHERE s2 = 'qs') b ON p.id = b.id AND b.vc2 IS NULL",
                                context, "buildFilter: ", "buildOnFilter: ", payload);
                    }
                }
            }
        });
    }

    @Test
    public void testBuildFiltersOverVariableSizeColumnsOfTheFirstInput() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            insertProbeDay();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                final String payload = payloadLayout == HashJoinPayloadLayout.COPIED
                        ? "buildPayload: copied when the probe is large enough" : null;
                for (int format = 0; format < 2; format++) {
                    if (format == 1) {
                        execute("ALTER TABLE p CONVERT PARTITION TO PARQUET WHERE t < '2020-01-02'");
                        execute("ALTER TABLE b CONVERT PARTITION TO PARQUET WHERE t < '2020-01-02'");
                    }
                    for (boolean isJit : new boolean[]{false, true}) {
                        context.setJitMode(isJit ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
                        final String nullCheck = isJit ? "buildJitFilter: " : "buildFilter: ";
                        // An INNER join builds b, the smaller input, though b comes first: the join
                        // model itself is then the build, and its own WHERE clause the build scan's
                        // filter.
                        final String swapped = "SELECT count(*) n, sum(b.v) bv, sum(p.v) pv FROM b JOIN p ON b.id = p.id WHERE ";
                        assertAgainstOrdinaryPlan(swapped + "b.str = 'sa'", context, "inputSwapped: true", "buildFilter: ", payload);
                        assertAgainstOrdinaryPlan(swapped + "b.vc ~ 'a' AND p.str ~ 's'", context,
                                "inputSwapped: true", "buildFilter: ", "probeFilter: ", payload);
                        assertAgainstOrdinaryPlan(swapped + "b.vc IS NOT NULL", context, "inputSwapped: true", nullCheck, payload);
                        assertAgainstOrdinaryPlan(swapped + "b.bin IS NULL AND array_sum(b.arr) > 0.5", context,
                                "inputSwapped: true", "buildFilter: ", payload);
                    }
                }
            }
        });
    }

    @Test
    public void testBuildFiltersOverVariableSizeColumnsOnNativeAndParquetFrames() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                // Every query reads b.v or b.id, a fixed-size payload, and no variable-size one: the
                // build filters read their columns off the build's page frames, before a row reaches
                // the row heap, so the copy can hold the payload.
                final String payload = payloadLayout == HashJoinPayloadLayout.COPIED
                        ? "buildPayload: copied when the probe is large enough" : null;
                for (int format = 0; format < 2; format++) {
                    if (format == 1) {
                        execute("ALTER TABLE p CONVERT PARTITION TO PARQUET WHERE t < '2020-01-02'");
                        execute("ALTER TABLE b CONVERT PARTITION TO PARQUET WHERE t < '2020-01-02'");
                    }
                    for (boolean isJit : new boolean[]{false, true}) {
                        context.setJitMode(isJit ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
                        // The JIT compiles a NULL check on a variable-size column, and nothing else here.
                        final String nullCheck = isJit ? "buildJitFilter: " : "buildFilter: ";
                        // An INNER join builds b, the second of two inputs of one size, and runs the
                        // WHERE clause on b as the build scan's filter.
                        final String inner = "SELECT count(*) n, sum(b.v) bv FROM p JOIN b ON p.id = b.id WHERE ";
                        assertAgainstOrdinaryPlan(inner + "b.str = 'sa'", context, "buildFilter: ", payload);
                        assertAgainstOrdinaryPlan(inner + "b.vc ~ 'a'", context, "buildFilter: ", payload);
                        assertAgainstOrdinaryPlan(inner + "b.vc IS NOT NULL", context, nullCheck, payload);
                        assertAgainstOrdinaryPlan(inner + "b.str IS NULL", context, nullCheck, payload);
                        assertAgainstOrdinaryPlan(inner + "b.bin IS NOT NULL", context, nullCheck, payload);
                        assertAgainstOrdinaryPlan(inner + "array_sum(b.arr) > 0.5", context, "buildFilter: ", payload);
                        // An outer join's ON condition on b alone, which the build runs over each frame.
                        final String left = "SELECT count(*) n, count(b.id) m, sum(b.v) bv FROM p LEFT JOIN b ON p.id = b.id AND ";
                        assertAgainstOrdinaryPlan(left + "b.vc ~ 'a'", context, "buildOnFilter: ", payload);
                        assertAgainstOrdinaryPlan(left + "b.str <> 'sa'", context, "buildOnFilter: ", payload);
                        assertAgainstOrdinaryPlan(left + "b.vc IS NULL", context, "buildOnFilter: ", payload);
                        assertAgainstOrdinaryPlan(left + "b.bin IS NOT NULL", context, "buildOnFilter: ", payload);
                        assertAgainstOrdinaryPlan(left + "array_sum(b.arr) > 0.5", context, "buildOnFilter: ", payload);
                        // A RIGHT join builds b, the input before it, and runs as a LEFT one.
                        assertAgainstOrdinaryPlan("SELECT count(*) n, count(b.id) m, sum(p.v) pv FROM b RIGHT JOIN p "
                                + "ON p.id = b.id AND b.str ~ 'a'", context, "buildOnFilter: ", payload);
                        // A composite key stages into the build's map rather than the INT layout.
                        assertAgainstOrdinaryPlan("SELECT count(*) n, count(b.id) m, sum(b.v) bv FROM p LEFT JOIN b "
                                + "ON p.id = b.id AND p.l = b.l AND b.vc ~ 'b'", context, "buildOnFilter: ", payload);
                        // Both build filters at once.
                        assertAgainstOrdinaryPlan("SELECT count(*) n, count(b.id) m, sum(b.v) bv FROM p "
                                        + "LEFT JOIN (SELECT * FROM b WHERE str = 'sb') b ON p.id = b.id AND b.vc ~ 'a'",
                                context, "buildFilter: ", "buildOnFilter: ", payload);
                    }
                }
            }
        });
    }

    @Test
    public void testProbeColumnsOfWideTypesOnNativeAndParquetFrames() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                for (int format = 0; format < 2; format++) {
                    if (format == 1) {
                        execute("ALTER TABLE p CONVERT PARTITION TO PARQUET WHERE t < '2020-01-02'");
                        execute("ALTER TABLE b CONVERT PARTITION TO PARQUET WHERE t < '2020-01-02'");
                    }
                    for (String column : probeColumns()) {
                        // A grouping key, which reaches the fragments' key sink.
                        assertDifferential("SELECT p." + column + " k, count(*) n, sum(b.v) bv "
                                + "FROM p LEFT JOIN b ON p.id=b.id ORDER BY k", context, true);
                        // A filter, which the reducer runs over the raw frame.
                        assertDifferential("SELECT count(*) n, sum(b.v) bv FROM p LEFT JOIN b ON p.id=b.id "
                                + "WHERE p." + column + " IS NOT NULL", context, true);
                        // An aggregate argument, which item 3's registry admits for these types.
                        assertDifferential("SELECT count(p." + column + ") n, sum(b.v) bv "
                                + "FROM p LEFT JOIN b ON p.id=b.id", context, true);
                    }
                }
            }
        });
    }

    @Test
    public void testProbeColumnsOfWideTypesUnderJitAndAStagedKey() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                for (boolean isJit : new boolean[]{false, true}) {
                    context.setJitMode(isJit ? SqlJitMode.JIT_MODE_FORCE_SCALAR : SqlJitMode.JIT_MODE_DISABLED);
                    for (String on : new String[]{"p.id=b.id", "p.id=b.id AND p.l=b.l"}) {
                        // A wide probe column as the key of the GROUP BY above a staged join key.
                        assertDifferential("SELECT p.vc k, count(*) n, sum(b.v) bv FROM p LEFT JOIN b ON " + on
                                + " WHERE p.l > 0 ORDER BY k", context, true);
                        assertDifferential("SELECT p.u k, count(p.l256) n256, count(p.dec256) n256d "
                                + "FROM p LEFT JOIN b ON " + on + " ORDER BY k", context, true);
                    }
                }
            }
        });
    }

    @Test
    public void testProbeExpressionsOverWideColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                // An expression over a wide probe column, whose own type decides the gate.
                assertDifferential("SELECT length(p.vc) k, count(*) n FROM p LEFT JOIN b ON p.id=b.id ORDER BY k",
                        context, true);
                assertDifferential("SELECT p.str || 'x' k, count(*) n FROM p LEFT JOIN b ON p.id=b.id ORDER BY k",
                        context, true);
                assertDifferential("SELECT count(*) n FROM p LEFT JOIN b ON p.id=b.id WHERE p.vc LIKE 'v%'",
                        context, true);
                // A wide probe column compared against a build column of the same type reaches the
                // post-join filter rather than the key, and the build column disqualifies it.
                assertDifferential("SELECT count(*) n FROM p LEFT JOIN b ON p.id=b.id WHERE p.vc = b.vc",
                        context, false);
            }
        });
    }

    @Test
    public void testUnstagedProbeTypesKeepTheOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                // BINARY and ARRAY are outside the GROUP BY key set, so grouping by one keeps the
                // ordinary plan instead of failing the query with a fused-only error.
                assertDifferential("SELECT p.bin k, count(*) n FROM p LEFT JOIN b ON p.id=b.id", context, false);
                assertDifferential("SELECT p.arr k, count(*) n FROM p LEFT JOIN b ON p.id=b.id", context, false);
                // A filter over either one is a BOOLEAN expression, so the shape still qualifies.
                assertDifferential("SELECT count(*) n FROM p LEFT JOIN b ON p.id=b.id WHERE p.bin IS NOT NULL",
                        context, true);
                assertDifferential("SELECT count(*) n FROM p LEFT JOIN b ON p.id=b.id WHERE array_sum(p.arr) > 0.5",
                        context, true);
            }
        });
    }

    @Test
    public void testVariableSizeBuildColumnsOutsideBuildFiltersKeepTheOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                // A build filter over a variable-size column qualifies, but the same column read
                // anywhere on the joined record would be a payload the row heap cannot copy: an
                // aggregate argument, an expression inside one, a post-join filter, or an ON
                // condition that reads the probe too.
                assertAgainstOrdinaryPlan("SELECT count(b.str) n FROM p JOIN b ON p.id = b.id WHERE b.str = 'sa'", context);
                assertAgainstOrdinaryPlan("SELECT sum(length(b.vc)) n FROM p JOIN b ON p.id = b.id WHERE b.vc ~ 'a'", context);
                assertAgainstOrdinaryPlan("SELECT count(b.vc) n FROM p LEFT JOIN b ON p.id = b.id AND b.vc ~ 'a'", context);
                assertAgainstOrdinaryPlan("SELECT count(*) n FROM p LEFT JOIN b ON p.id = b.id WHERE b.vc ~ 'a'", context);
                assertAgainstOrdinaryPlan("SELECT count(*) n FROM p LEFT JOIN b ON p.id = b.id AND b.str = 'sa' "
                        + "WHERE b.str IS NULL", context);
                assertAgainstOrdinaryPlan("SELECT count(*) n FROM p LEFT JOIN b ON p.id = b.id AND b.vc <> p.vc", context);
                // A grouping key over the build column reads it on the joined record too.
                context.setParallelHashJoinGroupByEnabled(false);
                final String expected;
                try {
                    expected = TestUtils.printSqlToString(engine, context,
                            "SELECT b.vc k, count(*) n FROM p JOIN b ON p.id = b.id WHERE b.vc ~ 'a' ORDER BY k", sink);
                } finally {
                    context.setParallelHashJoinGroupByEnabled(true);
                }
                assertQuery("SELECT b.vc k, count(*) n FROM p JOIN b ON p.id = b.id WHERE b.vc ~ 'a' ORDER BY k")
                        .withContext(context)
                        .noLeakCheck()
                        .expectSize()
                        .withPlanNotContaining("Hash Join Group By")
                        .returns(expected);
            }
        });
    }

    private static String[] probeColumns() {
        final String[] columns = new String[PAYLOAD_COLUMNS.length + PROBE_ONLY_COLUMNS.length];
        System.arraycopy(PAYLOAD_COLUMNS, 0, columns, 0, PAYLOAD_COLUMNS.length);
        System.arraycopy(PROBE_ONLY_COLUMNS, 0, columns, PAYLOAD_COLUMNS.length, PROBE_ONLY_COLUMNS.length);
        return columns;
    }

    /**
     * Checks a scalar aggregate's rows against the ordinary plan's. With plan fragments, the query
     * must run on the fused plan and its plan contain each non-null fragment; without them, the query
     * must keep the ordinary plan.
     */
    private void assertAgainstOrdinaryPlan(String sql, SqlExecutionContextImpl context, String... fusedPlanFragments) throws Exception {
        context.setParallelHashJoinGroupByEnabled(false);
        final String expected;
        try {
            expected = TestUtils.printSqlToString(engine, context, sql, sink);
        } finally {
            context.setParallelHashJoinGroupByEnabled(true);
        }
        final QueryAssertion assertion = assertQuery(sql)
                .withContext(context)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize();
        if (fusedPlanFragments.length > 0) {
            final String[] fragments = new String[fusedPlanFragments.length + 1];
            fragments[0] = "Hash Join Group By";
            System.arraycopy(fusedPlanFragments, 0, fragments, 1, fusedPlanFragments.length);
            assertion.withPlanContaining(fragments);
        } else {
            assertion.withPlanNotContaining("Hash Join Group By");
        }
        assertion.returns(expected);
    }

    private void createTables() throws Exception {
        for (String name : new String[]{"p", "b"}) {
            execute("CREATE TABLE " + name + " (id INT, l LONG, vc VARCHAR, str STRING, ip IPV4, u UUID, "
                    + "l256 LONG256, g1 GEOHASH(1c), g3 GEOHASH(3c), g6 GEOHASH(6c), g8 GEOHASH(8c), "
                    + "dec8 DECIMAL(2,1), dec16 DECIMAL(4,1), dec32 DECIMAL(9,2), dec64 DECIMAL(18,2), "
                    + "dec128 DECIMAL(38,2), dec256 DECIMAL(50,2), "
                    + "bin BINARY, arr DOUBLE[], v DOUBLE, t TIMESTAMP) TIMESTAMP(t) PARTITION BY DAY");
        }
    }

    // Gives p a third day of 24 rows, so that b, at 48 rows, is the smaller input an INNER join builds.
    private void insertProbeDay() throws Exception {
        execute("""
                INSERT INTO p (id, l, vc, str, v, t)
                SELECT rnd_int(0, 2, 1), rnd_int(0, 2, 1)::LONG, rnd_varchar('va', 'vb', NULL), rnd_str('sa', 'sb', NULL),
                    x * 0.25, timestamp_sequence('2020-01-03', 3_600_000_000L)
                FROM long_sequence(24)
                """);
    }

    private void insertRows() throws Exception {
        // Small overlapping domains with NULLs on both sides, spread over two partitions so that
        // the parquet run converts one of them and leaves the other native. rnd_double_array(1, 2)
        // makes about half the elements NULL, so array_sum(arr) is NULL on some rows, at most 0.5
        // on others and above 0.5 on the rest, and a filter over it keeps some rows but not all.
        for (String name : new String[]{"p", "b"}) {
            execute("INSERT INTO " + name + " (id, l, vc, str, ip, u, l256, g1, g3, g6, g8, "
                    + "dec8, dec16, dec32, dec64, dec128, dec256, bin, arr, v, t) "
                    + "SELECT rnd_int(0, 2, 1), rnd_int(0, 2, 1)::long, "
                    + "rnd_varchar('va', 'vb', NULL), rnd_str('sa', 'sb', NULL), "
                    + "rnd_ipv4('10.0.0.1/30', 1), "
                    + "rnd_str('11111111-1111-1111-1111-111111111111', "
                    + "'22222222-2222-2222-2222-222222222222', NULL)::uuid, "
                    + "rnd_str('0x01', '0x02', NULL)::long256, "
                    + "rnd_str('s', 'e', NULL)::geohash(1c), "
                    + "rnd_str('sp0', 'ezs', NULL)::geohash(3c), "
                    + "rnd_str('sp052w', 'ezs42e', NULL)::geohash(6c), "
                    + "rnd_str('sp052w92', 'ezs42e44', NULL)::geohash(8c), "
                    + "rnd_int(0, 2, 1)::decimal(2,1), rnd_int(0, 2, 1)::decimal(4,1), "
                    + "rnd_int(0, 2, 1)::decimal(9,2), rnd_int(0, 2, 1)::decimal(18,2), "
                    + "rnd_int(0, 2, 1)::decimal(38,2), rnd_int(0, 2, 1)::decimal(50,2), "
                    + "rnd_bin(4, 8, 2), rnd_double_array(1, 2), "
                    + "x*0.25, timestamp_sequence('2020-01-01', 3600000000) FROM long_sequence(48)");
        }
    }
}
