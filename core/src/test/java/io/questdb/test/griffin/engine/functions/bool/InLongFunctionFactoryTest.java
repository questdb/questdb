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

package io.questdb.test.griffin.engine.functions.bool;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.bind.IndexedParameterLinkFunction;
import io.questdb.griffin.engine.functions.bind.NamedParameterLinkFunction;
import io.questdb.griffin.engine.functions.bool.InLongFunctionFactory;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVarTuple;
import io.questdb.test.tools.BindVariableTestSetter;
import org.junit.Assert;
import org.junit.Test;

public class InLongFunctionFactoryTest extends AbstractCairoTest {
    // Only row 1, whose key overflows INT. It carries ONE value - the wrap - so only a binding
    // naming that value selects it; a binding naming the pre-wrap product selects nothing.
    private static final String MATCHED_ROW = """
            rn
            1
            """;
    // The header alone: the key matched no element.
    private static final String NO_ROWS = "rn\n";
    // The rows whose INT-arithmetic key is NULL, i.e. whose getInt() carries INT_NULL.
    private static final String NULL_KEY_ROWS = """
            rn
            1
            2
            5
            """;
    // Row 1's key overflows INT: 100000*100000 wraps to 1_410_065_408 at every width, so the
    // 10_000_000_000 binding matches nothing. Rows 2 and 3 stay small and match neither.
    private static final String WIDTH_SPLIT_TABLE = """
            CREATE TABLE x AS (SELECT
              cast(CASE WHEN x = 1 THEN 100000 ELSE 3 END AS INT) a,
              cast(CASE WHEN x = 1 THEN 100000 ELSE 5 END AS INT) b,
              cast(x AS INT) rn
            FROM long_sequence(3))""";
    // b is a column, so "a IN ($1,b)" reaches InLongVarFunction; b never equals a, so only $1 selects.
    private static final String VAR_PATH_QUERY = "SELECT rn FROM x WHERE a IN ($1,b)";
    private static final String VAR_PATH_ROW_THREE = """
            rn
            3
            """;
    private static final String VAR_PATH_TABLE =
            "CREATE TABLE x AS (SELECT cast(x AS LONG) a, cast(100 AS LONG) b, cast(x AS INT) rn FROM long_sequence(5))";
    private static final String WIDTH_SPLIT_QUERY = "SELECT rn FROM x WHERE (a*b) IN ($1,$2)";

    @Test
    public void testBindVariables() throws Exception {
        bindVariableService.clear();
        bindVariableService.setLong(0, 4);
        bindVariableService.setLong(1, 2);
        assertQuery("select * from x where x in ($1,$2)")
                .ddl("create table x as (" +
                        "select x from long_sequence(10)" +
                        ")")
                .returns("""
                        x
                        2
                        4
                        """);
    }

    @Test
    public void testFewBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE bench AS (SELECT rnd_long(1, 1000, 0) l FROM long_sequence(1_000_000));");

            StringSink sink = new StringSink();
            Rnd rnd = new Rnd(123, 456);
            sink.put("SELECT DISTINCT l FROM bench WHERE l IN (");

            bindVariableService.clear();
            for (int i = 0; i < 5; i++) {
                bindVariableService.setLong(i, rnd.nextLong(1000));
                sink.put("$").put(i + 1).put(',');
            }

            sink.trimTo(sink.length() - 1);
            sink.put(") ORDER BY l LIMIT 5;");

            assertQuery(sink)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            l
                            69
                            143
                            280
                            291
                            683
                            """);

            if (engine.getConfiguration().getSqlJitMode() == SqlJitMode.JIT_MODE_ENABLED) {
                assertQuery(sink)
                        .noLeakCheck()
                        .assertsPlan("""
                                Long Top K lo: 5
                                  keys: [l asc]
                                    Async JIT Group By workers: 1
                                      keys: [l]
                                      filter: l in [69,143,280,291,683]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: bench
                                """);
            } else {
                assertQuery(sink)
                        .noLeakCheck()
                        .assertsPlan("""
                                Long Top K lo: 5
                                  keys: [l asc]
                                    Async Group By workers: 1
                                      keys: [l]
                                      filter: l in [69,143,280,291,683]
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: bench
                                """);
            }


            sink.clear();
        });
    }

    @Test
    public void testManyBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE bench AS (SELECT rnd_long(1, 1000, 0) l FROM long_sequence(1_000_000));");

            StringSink sink = new StringSink();
            Rnd rnd = new Rnd(123, 456);
            sink.put("SELECT DISTINCT l FROM bench WHERE l IN (");

            bindVariableService.clear();
            for (int i = 0; i < 200; i++) {
                bindVariableService.setLong(i, rnd.nextLong(1000));
                sink.put("$").put(i + 1).put(',');
            }

            sink.trimTo(sink.length() - 1);
            sink.put(") ORDER BY l LIMIT 5;");

            assertQuery(sink)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            l
                            2
                            5
                            9
                            18
                            20
                            """);

            // should be the same, JIT or no JIT
            assertQuery(sink)
                    .noLeakCheck()
                    .assertsPlan("""
                            Long Top K lo: 5
                              keys: [l asc]
                                Async Group By workers: 1
                                  keys: [l]
                                  filter: l in [2,5,9,18,20,22,36,42,43,54,58,61,63,65,69,73,76,80,87,92,101,103,108,115,116,122,125,126,128,129,143,144,145,148,151,168,172,173,177,199,208,210,212,223,237,251,254,259,271,274,280,281,282,283,291,292,296,298,299,300,302,303,305,321,322,332,335,359,361,367,372,378,380,384,394,400,402,403,406,417,426,430,440,444,466,468,471,474,476,477,479,489,490,494,499,500,515,520,531,532,540,541,549,551,553,554,558,580,582,584,591,594,600,601,603,605,613,620,628,646,647,650,667,669,674,675,683,690,692,695,710,722,727,729,743,746,778,779,780,787,788,789,793,798,802,811,815,818,821,822,832,835,836,839,842,843,847,852,860,862,866,875,877,892,897,898,900,907,908,909,920,925,934,937,948,963,969,977,979,982,995]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: bench
                            """);


            sink.clear();
        });
    }

    @Test
    public void testManyConst() throws Exception {
        assertQuery("select * from x where x in (7,5,3,1)")
                .ddl("create table x as (" +
                        "select x from long_sequence(10)" +
                        ")")
                .returns("""
                        x
                        1
                        3
                        5
                        7
                        """);
    }

    @Test
    public void testMixedListPreservesSourceOrderShortCircuit() throws Exception {
        assertQuery("SELECT k IN (1, substring(s, 1, n)) result FROM x")
                .ddl("CREATE TABLE x AS (SELECT 1::long k, 'x' s, -1 n)")
                .expectSize()
                .returns("""
                        result
                        true
                        """);
    }

    @Test
    public void testMergedPlanDeduplicatesCrossWidthValue() throws Exception {
        // An INT arithmetic key wraps mod 2^32 under getInt() where getLong() widens, so it is read
        // once per element width and the INT-width and long-width sets stay apart. A value present
        // in both (6 as an INT literal, 6::long as a LONG element) must still render once in
        // EXPLAIN, matching the single hash-set plan. Needs >= 3 elements so the const path builds
        // the sets (a 2-element list routes to the two-const form). Before the dedup the merge
        // rendered [6,6,8].
        assertQuery("select * from x where i * 2 in (6, 6::long, 8)")
                .ddl("create table x as (select x::int i from long_sequence(10))")
                .withPlanContaining("in [6,8]")
                .returns("""
                        i
                        3
                        4
                        """);
    }

    @Test
    public void testNarrowKeyMixedWidthBindVariables() throws Exception {
        // The runtime-const path allocates its sets from the element TYPES (all STRING here) but
        // fills them by VALUE, so which set actually holds anything is only known after init().
        // The key is a plain INT column: it reads the same number at both widths, so every element
        // lands in one set whatever its value, and the key is probed once per row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT cast(v AS INT) i32 FROM " +
                    "(SELECT 1 v UNION ALL SELECT 2 v UNION ALL SELECT -2_147_483_647 v UNION ALL SELECT null v))");

            bindVariableService.clear();
            bindVariableService.setStr("b0", "1");
            bindVariableService.setStr("b1", "5000000000");
            assertQuery("SELECT i32 FROM x WHERE i32 IN (:b0, :b1) ORDER BY i32")
                    .noLeakCheck()
                    .returns("""
                            i32
                            1
                            """);

            // Rebind to two INT-range values: the wide set stays empty for this cursor.
            bindVariableService.setStr("b0", "2");
            bindVariableService.setStr("b1", "-2147483647");
            assertQuery("SELECT i32 FROM x WHERE i32 IN (:b0, :b1) ORDER BY i32")
                    .noLeakCheck()
                    .returns("""
                            i32
                            -2147483647
                            2
                            """);
        });
    }

    @Test
    public void testNarrowKeyMixedWidthConstList() throws Exception {
        // A plain INT column reads the same number through getInt() and getLong(), so a mixed-width
        // IN list collapses into one set and one probe per row. The rows pin what that selects: a
        // NULL element matches the NULL row, a LONG element matches nothing an INT column holds.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT cast(v AS INT) i32 FROM " +
                    "(SELECT 1 v UNION ALL SELECT 2 v UNION ALL SELECT -2_147_483_647 v UNION ALL SELECT null v))");

            assertQuery("SELECT i32 FROM x WHERE i32 IN (1, 2, null) ORDER BY i32")
                    .noLeakCheck()
                    .returns("""
                            i32
                            null
                            1
                            2
                            """);
            assertQuery("SELECT i32 FROM x WHERE i32 IN (1, 2, 5_000_000_000) ORDER BY i32")
                    .noLeakCheck()
                    .returns("""
                            i32
                            1
                            2
                            """);
            // The plan renders one merged, sorted list either way.
            assertQuery("SELECT i32 FROM x WHERE i32 IN (1, 2, 5_000_000_000)")
                    .noLeakCheck()
                    .assertsPlanContaining("filter: i32 in [1,2,5000000000]");
        });
    }

    @Test
    public void testNarrowKeyMixedWidthVarList() throws Exception {
        // The var path: one element varies per row, so the list cannot be pre-hashed. The constant
        // elements next to it are still folded at construction rather than re-parsed per row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT cast(v AS INT) i32, cast(v + 1 AS INT) other FROM " +
                    "(SELECT 1 v UNION ALL SELECT 2 v UNION ALL SELECT -2_147_483_647 v))");

            assertQuery("SELECT i32 FROM x WHERE i32 IN (other, '5000000000', 2) ORDER BY i32")
                    .noLeakCheck()
                    .returns("""
                            i32
                            2
                            """);
            assertQuery("SELECT i32 FROM x WHERE i32 IN (other - 1, 5_000_000_000) ORDER BY i32")
                    .noLeakCheck()
                    .returns("""
                            i32
                            -2147483647
                            1
                            2
                            """);
        });
    }

    @Test
    public void testNarrowIntArithKeyNullElementMatchesEqNull() throws Exception {
        // An INT-arithmetic key is NULL exactly when its value is INT_NULL, which is what '=',
        // IS NULL and a projection of the same expression all report. Every IN form has to agree
        // with them, and with each other, whatever the element's declared type:
        //   - row 1 (65_536*32_768) and row 2 (-2^30 * 2) wrap onto INT_NULL, so they ARE null.
        //   - row 3 (2^30 * 8 * 2^30) has value 0, so it is NOT null - even though the product it
        //     would have had at 64 bits lands exactly on LONG_NULL. Nothing computes that product.
        // `null::long` agrees with the untyped null here, because a 64-bit read of the key
        // sign-extends its value and Numbers.intToLong(INT_NULL) is LONG_NULL.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE x AS (SELECT
                      cast(CASE WHEN x = 1 THEN 65_536 WHEN x = 2 THEN -1_073_741_824 WHEN x = 3 THEN 1_073_741_824 WHEN x = 4 THEN 3 ELSE null END AS INT) a,
                      cast(CASE WHEN x = 1 THEN 32_768 WHEN x = 2 THEN 2 WHEN x = 3 THEN 8 ELSE 3 END AS INT) b,
                      cast(CASE WHEN x = 3 THEN 1_073_741_824 ELSE 1 END AS INT) c,
                      cast(x AS INT) rn
                    FROM long_sequence(5))""");

            // assertQuery() does not touch the JIT mode and the suite default is enabled, so
            // running these once only ever exercised the compiled filter - the Java InLong path
            // this test is about was never called. Run every case under both modes.
            final int jitMode = sqlExecutionContext.getJitMode();
            try {
                for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    sqlExecutionContext.setJitMode(mode);

                    // The oracle: the value of the key expression, as the projection reports it.
                    assertQuery("SELECT rn, a*b prod, a*b*c prod3 FROM x")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    rn\tprod\tprod3
                                    1\tnull\tnull
                                    2\tnull\tnull
                                    3\t0\t0
                                    4\t9\t9
                                    5\tnull\tnull
                                    """);

                    // '=' and IS NULL agree with the projection: rows 1, 2 and 5 are the null ones.
                    assertQuery("SELECT rn FROM x WHERE (a*b) = null").noLeakCheck().returns(NULL_KEY_ROWS);
                    assertQuery("SELECT rn FROM x WHERE (a*b) IS NULL").noLeakCheck().returns(NULL_KEY_ROWS);

                    // IN (null) must select exactly the same rows, in every InLong form.
                    assertQuery("SELECT rn FROM x WHERE (a*b) IN (null)")                  // single const
                            .noLeakCheck().returns(NULL_KEY_ROWS);
                    assertQuery("SELECT rn FROM x WHERE (a*b) IN (null, 999)")             // two const
                            .noLeakCheck().returns(NULL_KEY_ROWS);
                    assertQuery("SELECT rn FROM x WHERE (a*b) IN (null, 999, 7)")          // const set
                            .noLeakCheck().returns(NULL_KEY_ROWS);
                    assertQuery("SELECT rn FROM x WHERE (a*b) IN (null, c - 1)")           // var: c-1 is 0 on rows 1,2,4,5
                            .noLeakCheck().returns("""
                                    rn
                                    1
                                    2
                                    5
                                    """);

                    // NOT IN inverts it: the non-null keys, and nothing else.
                    assertQuery("SELECT rn FROM x WHERE (a*b) NOT IN (null)")
                            .noLeakCheck().returns("""
                                    rn
                                    3
                                    4
                                    """);

                    // The deeper key: row 3's long-width product would land exactly on LONG_NULL while
                    // its value is 0. The key has one value now, so nothing computes that product and
                    // row 3 cannot match a null element at any width.
                    assertQuery("SELECT rn FROM x WHERE (a*b*c) = null").noLeakCheck().returns(NULL_KEY_ROWS);
                    assertQuery("SELECT rn FROM x WHERE (a*b*c) IN (null)").noLeakCheck().returns(NULL_KEY_ROWS);

                    // A LONG-typed null element now agrees with the untyped one, because the key has
                    // a single value and every 64-bit read of it sign-extends that value:
                    // Numbers.intToLong(INT_NULL) is LONG_NULL, so the same rows are null either way.
                    // Under the split-key regime these selected row 3 instead - a row whose value is 0.
                    assertQuery("SELECT rn FROM x WHERE (a*b*c) = null::long")
                            .noLeakCheck().returns(NULL_KEY_ROWS);
                    assertQuery("SELECT rn FROM x WHERE (a*b*c) IN (null::long)")
                            .noLeakCheck().returns(NULL_KEY_ROWS);

                    // Control: a plain INT column key is not split - getInt() and getLong() carry the same
                    // number - so only the genuinely-null row matches, at either width.
                    assertQuery("SELECT rn FROM x WHERE a IN (null)")
                            .noLeakCheck().returns("""
                                    rn
                                    5
                                    """);
                }
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
            }
        });
    }

    @Test
    public void testBindVariableBareKeyCompileThenLongRebind() throws Exception {
        // A BARE bind variable as the IN key, rather than an arithmetic expression holding one.
        // Its two widths are the same number, so it never needed the width split - but the link
        // function does not answer the width question and inherits Function's false, while this
        // PR made it report isRowStable(). isSplitKey therefore turns on for every bind-variable
        // key, and getBool reads it with getInt().
        //
        // Whether that read is legal depends on what the variable is bound to: LongFunction.getInt()
        // is final and throws. So re-binding a compiled factory from INT to LONG - the pattern
        // testBindVariableIntWidthCompileThenLongRebind establishes as supported, and which master
        // served because it only ever read the key at long width - fails with
        // UnsupportedOperationException instead of returning rows.
        assertMemoryLeak(() -> {
            // The defect is not JIT-specific - it fires from BooleanRuntimeConstFunction.init(),
            // before the JIT is consulted, and reproduces either way. The JIT is off here because it
            // freezes each bind variable's width into its IR at compile time and does not
            // re-serialize on a re-bind, so it answers a re-bound query at the compile-time width
            // whatever the new binding says. Pinning the Java path keeps the expected result sets
            // below exact, and it is the path the split key actually feeds.
            final int jitMode = sqlExecutionContext.getJitMode();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try {
                execute("CREATE TABLE x AS (SELECT cast(x AS INT) rn FROM long_sequence(3))");

                final String allRows = """
                        rn
                        1
                        2
                        3
                        """;
                final String rowThree = """
                        rn
                        3
                        """;
                // Each list shape reaches a different InLong form, and every one of them reads the
                // key itself: single const, two const, set, the var path, and the runtime-const one.
                assertQuery("SELECT rn FROM x WHERE rn > 0 AND $1 IN (3)")
                        .noLeakCheck()
                        .assertBinds(bareKeyRebindCases(allRows));
                assertQuery("SELECT rn FROM x WHERE rn > 0 AND $1 IN (3, 5_000_000_000)")
                        .noLeakCheck()
                        .assertBinds(bareKeyRebindCases(allRows));
                assertQuery("SELECT rn FROM x WHERE rn > 0 AND $1 IN (3, 5_000_000_000, 7)")
                        .noLeakCheck()
                        .assertBinds(bareKeyRebindCases(allRows));
                // A column element reaches InLongVarFunction; only row 3 equals the bound key.
                assertQuery("SELECT rn FROM x WHERE rn > 0 AND $1 IN (rn, 5_000_000_000)")
                        .noLeakCheck()
                        .assertBinds(bareKeyRebindCases(rowThree));
                // A bind-variable element resolves once per cursor rather than per row, which is the
                // fifth form - InLongRuntimeConstFunction - and it reads the key the same way.
                final ObjList<BindVarTuple> runtimeConstElement = new ObjList<>();
                runtimeConstElement.add(BindVarTuple.ok("int bind", allRows, bvs -> {
                    bvs.setInt(0, 3);
                    bvs.setLong(1, 5_000_000_000L);
                }));
                runtimeConstElement.add(BindVarTuple.ok("re-bound to a long that wraps onto 3", NO_ROWS, bvs -> {
                    bvs.setLong(0, 4_294_967_299L);
                    bvs.setLong(1, 5_000_000_000L);
                }));
                assertQuery("SELECT rn FROM x WHERE rn > 0 AND $1 IN (3, $2)")
                        .noLeakCheck()
                        .assertBinds(runtimeConstElement);

                // A positive control rather than a width assertion: the re-bound wide key still has
                // to match the wide element. It cannot fail for a width reason, because a LONG-typed
                // element reads the key with getLong() whether or not the key was split - see
                // isIntWidthElement. It guards the plain "does a wide re-bind still return its rows"
                // question that the wrapping cases above deliberately answer with nothing.
                final ObjList<BindVarTuple> wideMatch = new ObjList<>();
                wideMatch.add(BindVarTuple.ok("int bind", allRows, bvs -> bvs.setInt(0, 3)));
                wideMatch.add(BindVarTuple.ok("re-bound to the wide element", allRows, bvs -> bvs.setLong(0, 5_000_000_000L)));
                assertQuery("SELECT rn FROM x WHERE rn > 0 AND $1 IN (3, 5_000_000_000)")
                        .noLeakCheck()
                        .assertBinds(wideMatch);

                // SHORT and BYTE are narrow ints too, so a key bound at either width takes the same
                // split decision at compile time as an INT one, and crashed the same way.
                assertQuery("SELECT rn FROM x WHERE rn > 0 AND $1 IN (3, 5_000_000_000)")
                        .noLeakCheck()
                        .assertBinds(narrowKeyRebindCases(allRows, "short", bvs -> bvs.setShort(0, (short) 3)));
                assertQuery("SELECT rn FROM x WHERE rn > 0 AND $1 IN (3, 5_000_000_000)")
                        .noLeakCheck()
                        .assertBinds(narrowKeyRebindCases(allRows, "byte", bvs -> bvs.setByte(0, (byte) 3)));
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
            }
        });
    }

    @Test
    public void testBindVariableIntWidthCompileThenLongRebind() throws Exception {
        // The runtime-const form decided WHICH width sets exist from a compile-time type snapshot,
        // but init() re-partitions the elements by their RUNTIME type. Compiled with INT-width binds
        // it allocated only the int set, so re-binding the same factory with LONG values sent
        // parseToSets down the outLongSet.add() arm with a null set - an NPE reaching the user as
        // "unexpected filter error". Re-binding a compiled factory to a different type is an
        // established pattern: QueryAssertion.assertBinds compiles once and re-binds per case, and
        // IndexedParameterLinkFunction.init() refreshes its type for exactly this reason.
        //
        // Row 1's key overflows INT and wraps to 1_410_065_408, whatever width reads it, so only
        // the INT binding selects it. The re-bind still has to WORK - the point of this test is that
        // a factory compiled with INT binds and re-bound to LONG ones answers rather than throwing.
        // See the mirror in testBindVariableLongWidthCompileThenIntRebind.
        assertMemoryLeak(() -> {
            // The compiled filter freezes each bind variable's width into its IR at compile time and
            // does not re-serialize on a re-bind, so it answers this query at the compile-time width
            // whatever the new binding says. That divergence is pre-existing and independent of the
            // set-allocation bug covered here - it reproduces on IN ($1,5), which allocated both sets
            // all along. Pin the Java path, which is what parseToSets feeds.
            final int jitMode = sqlExecutionContext.getJitMode();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try {
                execute(WIDTH_SPLIT_TABLE);

                final ObjList<BindVarTuple> cases = new ObjList<>();
                cases.add(BindVarTuple.ok("int binds", MATCHED_ROW, bindVariableService -> {
                    bindVariableService.setInt(0, 1_410_065_408);
                    bindVariableService.setInt(1, 999);
                }));
                cases.add(BindVarTuple.ok("re-bound to long", NO_ROWS, bindVariableService -> {
                    // The key wraps, so the pre-wrap product matches nothing. The row this case
                    // guards is that the re-bind is served at all.
                    bindVariableService.setLong(0, 10_000_000_000L);
                    bindVariableService.setLong(1, 999);
                }));
                assertQuery(WIDTH_SPLIT_QUERY)
                        .noLeakCheck()
                        .assertBinds(cases);
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
            }
        });
    }

    @Test
    public void testBindVariableLinkKeyIsNotSplit() throws Exception {
        // NamedParameterLinkFunction cannot reach this contract from SQL. Its init() only asserts
        // base.getType() == type where the indexed sibling refreshes the field, so the
        // INT-compile-then-LONG-rebind sequence that exposes the width question dies on that
        // assertion under -ea, which surefire enables (core/pom.xml). Drive both link functions
        // through the factory directly instead, and count the width each key is read at.
        //
        // The element list mixes widths on purpose - that is what makes a split key probe the INT
        // set with getInt() and the LONG set with getLong(), reading the key twice. A bind variable
        // carries the same number at either width and must not be split: one read, at long width.
        // Key 11 misses both elements, so a split key falls through to its second width, while a
        // correctly single-width key still has to touch exactly one.
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            try {
                bindVariableService.setInt(0, 11);
                bindVariableService.setInt("k", 11);
                try (
                        CountingIndexedLink indexed = new CountingIndexedLink();
                        CountingNamedLink named = new CountingNamedLink()
                ) {
                    assertKeyReadOnceAtLongWidth(indexed);
                    assertKeyReadOnceAtLongWidth(named);
                }
            } finally {
                bindVariableService.clear();
            }
        });
    }

    @Test
    public void testBindVariableLongWidthCompileThenIntRebind() throws Exception {
        // The mirror of testBindVariableIntWidthCompileThenLongRebind: a factory compiled with
        // LONG-width binds must serve a re-bind to INT ones. Kept as its own test so each direction
        // reds on its own - run together, the first crash would hide the second.
        assertMemoryLeak(() -> {
            // The compiled filter freezes each bind variable's width into its IR at compile time and
            // does not re-serialize on a re-bind, so it answers this query at the compile-time width
            // whatever the new binding says. That divergence is pre-existing and independent of the
            // set-allocation bug covered here - it reproduces on IN ($1,5), which allocated both sets
            // all along. Pin the Java path, which is what parseToSets feeds.
            final int jitMode = sqlExecutionContext.getJitMode();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try {
                execute(WIDTH_SPLIT_TABLE);

                final ObjList<BindVarTuple> cases = new ObjList<>();
                cases.add(BindVarTuple.ok("long binds", NO_ROWS, bindVariableService -> {
                    // The key wraps, so the pre-wrap product matches nothing.
                    bindVariableService.setLong(0, 10_000_000_000L);
                    bindVariableService.setLong(1, 999);
                }));
                cases.add(BindVarTuple.ok("re-bound to int", MATCHED_ROW, bindVariableService -> {
                    bindVariableService.setInt(0, 1_410_065_408);
                    bindVariableService.setInt(1, 999);
                }));
                assertQuery(WIDTH_SPLIT_QUERY)
                        .noLeakCheck()
                        .assertBinds(cases);
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
            }
        });
    }

    @Test
    public void testBindVariableIntArithKeyMatchesEqNull() throws Exception {
        // A bind variable is non-deterministic ACROSS EXECUTIONS but perfectly stable within a row,
        // so an INT-arithmetic key holding one is still safe to read at both widths. Reading the
        // non-determinism flag instead disqualified the key from the width split and probed it at
        // long width only, so (a*$1) IN (null) missed the row whose product wraps to INT_NULL -
        // while the literal spelling (a*2), '=' and IS NULL all reported it as null.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE x AS (SELECT
                      cast(CASE WHEN x = 1 THEN 1_073_741_824 WHEN x = 2 THEN 3 ELSE null END AS INT) a,
                      cast(x AS INT) rn
                    FROM long_sequence(3))""");
            bindVariableService.clear();
            bindVariableService.setInt(0, 2);
            final String nullKeyRows = """
                    rn
                    1
                    3
                    """;
            final int jitMode = sqlExecutionContext.getJitMode();
            try {
                for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    sqlExecutionContext.setJitMode(mode);

                    // The oracles: row 1's product wraps to INT_NULL, row 3's operand is null.
                    assertQuery("SELECT rn FROM x WHERE (a*$1) IS NULL").noLeakCheck().returns(nullKeyRows);
                    assertQuery("SELECT rn FROM x WHERE (a*$1) = null").noLeakCheck().returns(nullKeyRows);
                    // The literal spelling of the same key already agreed.
                    assertQuery("SELECT rn FROM x WHERE (a*2) IN (null)").noLeakCheck().returns(nullKeyRows);

                    // Each list length reaches a different InLong form: single const, two const, set.
                    assertQuery("SELECT rn FROM x WHERE (a*$1) IN (null)").noLeakCheck().returns(nullKeyRows);
                    assertQuery("SELECT rn FROM x WHERE (a*$1) IN (null, 999)").noLeakCheck().returns(nullKeyRows);
                    assertQuery("SELECT rn FROM x WHERE (a*$1) IN (null, 999, 7)").noLeakCheck().returns(nullKeyRows);
                    // A non-constant element reaches InLongVarFunction.
                    assertQuery("SELECT rn FROM x WHERE (a*$1) IN (null, a-1)").noLeakCheck().returns(nullKeyRows);
                }
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
            }
        });
    }

    @Test
    public void testBindVariableVarPathLongCompileThenStringRebind() throws Exception {
        // Mirror of testBindVariableVarPathStringCompileThenLongRebind. A NON-numeric string is what
        // discriminates here: re-bound to "3" a stale KIND_LONG still lands on 3, because
        // StrFunction.getLong implicit-casts it. "abc" separates them - the stale kind throws
        // ImplicitCastException where the refreshed KIND_STR parses quietly to LONG_NULL and matches
        // nothing. Kept separate so each direction reds on its own.
        assertMemoryLeak(() -> {
            final int jitMode = sqlExecutionContext.getJitMode();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try {
                execute(VAR_PATH_TABLE);
                final ObjList<BindVarTuple> cases = new ObjList<>();
                cases.add(BindVarTuple.ok("long bind", VAR_PATH_ROW_THREE, bvs -> bvs.setLong(0, 3)));
                cases.add(BindVarTuple.ok("re-bound to non-numeric string", "rn\n", bvs -> bvs.setStr(0, "abc")));
                assertQuery(VAR_PATH_QUERY).noLeakCheck().assertBinds(cases);
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
            }
        });
    }

    @Test
    public void testBindVariableVarPathStringCompileThenLongRebind() throws Exception {
        // The var form (reached when an element is a column) froze each element's KIND in its
        // constructor from the compile-time type, then dispatched on it per row. Master read the
        // element type per row instead, so a bind variable re-bound to another type kept working.
        // A frozen KIND_STR sends a re-bound LONG element down func.getStrA(rec), which LongFunction
        // does not implement - UnsupportedOperationException, surfacing as "unexpected filter error".
        // init() must refresh the kinds after the link functions have refreshed their types.
        //
        // The JIT is off because the compiled filter binds each variable's type into its IR at
        // compile time and never re-serializes, so it rejects the re-bind before any of this is
        // reached. elementKinds only drives the Java path.
        assertMemoryLeak(() -> {
            final int jitMode = sqlExecutionContext.getJitMode();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try {
                execute(VAR_PATH_TABLE);
                final ObjList<BindVarTuple> cases = new ObjList<>();
                cases.add(BindVarTuple.ok("string bind", VAR_PATH_ROW_THREE, bvs -> bvs.setStr(0, "3")));
                cases.add(BindVarTuple.ok("re-bound to long", VAR_PATH_ROW_THREE, bvs -> bvs.setLong(0, 3)));
                assertQuery(VAR_PATH_QUERY).noLeakCheck().assertBinds(cases);
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
            }
        });
    }

    @Test
    public void testNullValuedStringElementMatchesUntypedNull() throws Exception {
        // A string element is probed at the width its VALUE would carry as a literal. A string that
        // does not parse carries Numbers.LONG_NULL, i.e. it IS null - but LONG_NULL is
        // Long.MIN_VALUE, which is outside INT range, so it used to be probed at LONG width while an
        // untyped null (and the var path's KIND_NONE default) probed at INT width. That was wrong in
        // both directions on a split INT-arithmetic key: it matched row 3, whose long-width product
        // overflows exactly onto LONG_NULL although its value is 0, and missed rows 1 and 2, which
        // wrap onto INT_NULL. A null-valued string must select exactly what IS NULL selects.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE x AS (SELECT
                      cast(CASE WHEN x = 1 THEN 65_536 WHEN x = 2 THEN -1_073_741_824 WHEN x = 3 THEN 1_073_741_824 WHEN x = 4 THEN 3 ELSE null END AS INT) a,
                      cast(CASE WHEN x = 1 THEN 32_768 WHEN x = 2 THEN 2 WHEN x = 3 THEN 8 ELSE 3 END AS INT) b,
                      cast(CASE WHEN x = 3 THEN 1_073_741_824 ELSE 1 END AS INT) c,
                      cast(x AS INT) rn,
                      cast('abc' AS STRING) s,
                      cast('abc' AS VARCHAR) v,
                      cast(null AS STRING) sn
                    FROM long_sequence(5))""");

            final int jitMode = sqlExecutionContext.getJitMode();
            try {
                for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    sqlExecutionContext.setJitMode(mode);
                    // The oracle for both keys.
                    assertQuery("SELECT rn FROM x WHERE (a*b*c) IS NULL").noLeakCheck().returns(NULL_KEY_ROWS);
                    assertQuery("SELECT rn FROM x WHERE (a*b) IS NULL").noLeakCheck().returns(NULL_KEY_ROWS);

                    // The deep key is the discriminating one: its long-width product lands on
                    // LONG_NULL for row 3, so a long-width probe returned 3 instead of 1, 2, 5.
                    assertQuery("SELECT rn FROM x WHERE (a*b*c) IN (s)").noLeakCheck().returns(NULL_KEY_ROWS);
                    assertQuery("SELECT rn FROM x WHERE (a*b*c) IN (v)").noLeakCheck().returns(NULL_KEY_ROWS);
                    assertQuery("SELECT rn FROM x WHERE (a*b*c) IN (sn)").noLeakCheck().returns(NULL_KEY_ROWS);
                    assertQuery("SELECT rn FROM x WHERE (a*b) IN (s)").noLeakCheck().returns(NULL_KEY_ROWS);

                    // It must agree with the untyped null spelling.
                    assertQuery("SELECT rn FROM x WHERE (a*b*c) IN (null)").noLeakCheck().returns(NULL_KEY_ROWS);

                    // A parseable string is unaffected: no key equals 7.
                    assertQuery("SELECT rn FROM x WHERE (a*b) IN (cast('7' AS STRING))").noLeakCheck().returns("rn\n");
                    // ... while one that does match still does.
                    assertQuery("SELECT rn FROM x WHERE (a*b) IN (cast('9' AS STRING))")
                            .noLeakCheck()
                            .returns("""
                                    rn
                                    4
                                    """);
                }
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
            }
        });
    }

    @Test
    public void testNonDeterministicSplitKeyEvaluatedOncePerRow() throws Exception {
        // A split key - an INT function that computes at long width under getLong() - is normally
        // read at both widths so an INT-width element wraps against getInt() and a LONG-width one
        // widens against getLong(). That is correct for a deterministic key, whose two width reads
        // carry consistent values. But when the key is non-deterministic (e.g. rnd_int() + 0),
        // getInt() and getLong() draw two different random values for the SAME row, so probing the
        // two width sets against those two draws produces incoherent results: a row can match on a
        // draw it never "really" had. The factory must therefore evaluate a non-deterministic split
        // key exactly once per row - reading it at a single width - instead of once per width.
        //
        // The list below mixes widths on purpose (an INT element and a LONG one), which is precisely
        // what makes a deterministic split key read both widths. The key value 11 misses both
        // elements, so a split key would fall through to the second width; a correctly single-read
        // key touches exactly one width. Assert the total number of key reads is 1, across every
        // list shape that reaches a distinct InLong function.
        assertMemoryLeak(() -> {
            try (CountingIntKey key = new CountingIntKey(11)) {
                key.nonDeterministic = true;

                // two-constant path -> InLongTwoConstFunction
                try (Function f = newInFunction(key, new IntConstant(7), new LongConstant(5_000_000_000L))) {
                    Assert.assertFalse(f.getBool(null));
                    Assert.assertEquals(1, key.intCalls + key.longCalls);
                    Assert.assertEquals(0, key.intCalls);
                }

                // constant-set path (>= 3 elements) -> InLongConstFunction
                key.reset();
                try (Function f = newInFunction(key, new IntConstant(7), new IntConstant(8), new LongConstant(5_000_000_000L))) {
                    Assert.assertFalse(f.getBool(null));
                    Assert.assertEquals(1, key.intCalls + key.longCalls);
                    Assert.assertEquals(0, key.intCalls);
                }

                // variable path (a non-constant element) -> InLongVarFunction
                key.reset();
                try (Function f = newInFunction(key, new NonConstIntElement(7), new LongConstant(5_000_000_000L))) {
                    Assert.assertFalse(f.getBool(null));
                    Assert.assertEquals(1, key.intCalls + key.longCalls);
                    Assert.assertEquals(0, key.intCalls);
                }
            }
        });
    }

    @Test
    public void testNonNumericStringConstElementThrows() throws Exception {
        // Every element the IN list resolves before the row loop - a literal, or a bind variable
        // resolved at cursor open - is a query error when it does not parse, reported at the
        // element's own position. Master threw from both, and "k = 'abc'" raises
        // ImplicitCastException for the same input. Reading it as LONG_NULL instead silently
        // matched every NULL key row, which is a wrong result rather than an error.
        //
        // Only the per-row path keeps parsing quietly: it re-reads its elements for every row and
        // has no position to report. That is the one form that still matches NULLs, and it is
        // pinned below so the boundary cannot drift.
        assertMemoryLeak(() -> {
            execute("create table t as (select x id, case when x = 3 then null else x end k, 'abc' s from long_sequence(10))");

            // All-constant list: reports the bad element at its own position.
            assertExceptionNoLeakCheck(
                    "select id from t where k in ('abc', 5)",
                    29,
                    "invalid LONG value [abc]",
                    sqlExecutionContext
            );

            // Runtime-constant list: a bind variable resolves once per cursor, so it throws too.
            // This is the half that silently returned the NULL rows when the strict parse was
            // applied to the compile-time path alone.
            bindVariableService.clear();
            bindVariableService.setStr("b0", "abc");
            assertExceptionNoLeakCheck(
                    "select id from t where k in (:b0)",
                    29,
                    "invalid LONG value [abc]",
                    sqlExecutionContext
            );
            bindVariableService.clear();

            // A column sibling routes the list to the per-row path, which reads 'abc' as LONG_NULL
            // and therefore matches the NULL key row (id=3) alongside k=5 (id=5).
            assertQuery("select id from t where k in (s, 5)")
                    .noLeakCheck()
                    .returns("""
                            id
                            3
                            5
                            """);
        });
    }

    @Test
    public void testSingleConst() throws Exception {
        assertQuery("select * from x where x in (1)")
                .ddl("create table x as (" +
                        "select x from long_sequence(5)" +
                        ")")
                .returns("""
                        x
                        1
                        """);
    }

    @Test
    public void testNarrowKeyInJsonExtractShortElement() throws Exception {
        // A json_extract(..)::short element carries one value: getShort (0 for an out-of-SHORT
        // payload, since SHORT has no null sentinel), and every wider read sign-extends it. So the
        // element contributes 0 to the IN list and matches a key of 0. This is the one narrow-int
        // element whose getInt() and getLong() used to disagree (the base class re-parsed each
        // width); JsonExtractShortFunction now derives both from getShort, which is why the IN
        // factory can read every narrow element at long width without a per-key width branch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k INT)");
            execute("INSERT INTO t VALUES (0), (5), (32768)");
            assertQuery("SELECT k FROM t WHERE k IN (json_extract('{\"a\":5000000000}','$.a')::short) ORDER BY k")
                    .noLeakCheck()
                    .returns("k\n0\n");
        });
    }

    @Test
    public void testNarrowKeyOverridingGetLongIsNotAssumedWidthStable() throws Exception {
        // json_extract(..)::int does not extend IntFunction, but JsonExtractIntFunction overrides
        // getLong() as Numbers.intToLong(getInt()), so its INT and long widths agree: an
        // out-of-INT-range number reads as INT_NULL at INT width and as LONG_NULL at long width.
        // IN probes the key with getLong(), so it agrees with both '=' and the projection.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (j VARCHAR)");
            execute("INSERT INTO t VALUES ('{\"x\":5000000000}')");

            // the key reads as null at INT width, and '=' agrees
            assertQuery("SELECT json_extract(j,'$.x')::int AS k FROM t")
                    .expectSize()
                    .returns("k\nnull\n");
            assertQuery("SELECT count(*) AS c FROM t WHERE json_extract(j,'$.x')::int = null")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n1\n");
            // ... so IN must agree too
            assertQuery("SELECT count(*) AS c FROM t WHERE json_extract(j,'$.x')::int IN (null)")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n1\n");
            assertQuery("SELECT count(*) AS c FROM t WHERE json_extract(j,'$.x')::int NOT IN (null)")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n0\n");
        });
    }

    @Test
    public void testIntArithKeyReadOnceAtLongWidth() throws Exception {
        // The key carries one value, so the IN list reads it exactly ONCE per row - at long width,
        // which for a narrow key is an exact sign extension of the value getInt() reports. Reading
        // it twice would matter for a non-deterministic key, whose second read is a fresh draw.
        //
        // The list mixes an INT element with a LONG one on purpose: an element's declared width
        // decides only that element's own value, never the key's.
        assertMemoryLeak(() -> {
            try (CountingIntKey key = new CountingIntKey(7)) {
                // two-constant path
                try (Function f = newInFunction(key, new IntConstant(7), new LongConstant(5_000_000_000L))) {
                    Assert.assertTrue(f.getBool(null));
                    Assert.assertEquals(0, key.intCalls);
                    Assert.assertEquals(1, key.longCalls);

                    // a miss reads the key no more often than a hit
                    key.reset();
                    key.value = 11;
                    Assert.assertFalse(f.getBool(null));
                    Assert.assertEquals(0, key.intCalls);
                    Assert.assertEquals(1, key.longCalls);
                }

                // variable path: a non-constant element forces it
                key.reset();
                key.value = 7;
                try (Function f = newInFunction(key, new NonConstIntElement(7), new LongConstant(5_000_000_000L))) {
                    Assert.assertTrue(f.getBool(null));
                    Assert.assertEquals(0, key.intCalls);
                    Assert.assertEquals(1, key.longCalls);

                    key.reset();
                    key.value = 11;
                    Assert.assertFalse(f.getBool(null));
                    Assert.assertEquals(0, key.intCalls);
                    Assert.assertEquals(1, key.longCalls);
                }
            }
        });
    }

    @Test
    public void testVarListConstantsCompareAtOneKeyWidth() throws Exception {
        // The var path hoists its constant elements into a set at construction so the per-row loop
        // probes them once instead of scanning every constant on every row. The key has one value,
        // so there is one set and one key read: an element's declared width decides that element's
        // own value and nothing about the key's.
        //
        // (a * 5) is 705_032_704 for row 1 - the wrap - at every width. An element naming the
        // pre-wrap 5_000_000_000 therefore matches nothing, whatever type it carries.
        assertMemoryLeak(() -> {
            // Take the JIT out of it: this is about the interpreted InLong path.
            final int jitMode = sqlExecutionContext.getJitMode();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try {
                execute("""
                        CREATE TABLE z AS (SELECT
                          cast(CASE WHEN x = 1 THEN 1_000_000_000 WHEN x = 2 THEN 2 ELSE null END AS INT) a,
                          cast(CASE WHEN x = 1 THEN 0 WHEN x = 2 THEN 7 ELSE 3 END AS INT) el
                        FROM long_sequence(3))""");

                // The oracle: the key expression carries the same number at both widths.
                assertQuery("SELECT a, (a * 5) narrow, (a * 5)::long wide FROM z")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                a\tnarrow\twide
                                1000000000\t705032704\t705032704
                                2\t10\t10
                                null\tnull\tnull
                                """);

                // The wrapped value selects the row through an INT element and a LONG one alike;
                // the pre-wrap product selects nothing.
                assertQuery("SELECT a FROM z WHERE (a * 5) IN (el, 705_032_704) ORDER BY a")
                        .noLeakCheck().returns("a\n1000000000\n");
                assertQuery("SELECT a FROM z WHERE (a * 5) IN (el, 705_032_704::long) ORDER BY a")
                        .noLeakCheck().returns("a\n1000000000\n");
                assertQuery("SELECT a FROM z WHERE (a * 5) IN (el, 5_000_000_000) ORDER BY a")
                        .noLeakCheck().returns("a\n");

                // A numeric string carries no declared width and follows the same rule.
                assertQuery("SELECT a FROM z WHERE (a * 5) IN (el, '705032704') ORDER BY a")
                        .noLeakCheck().returns("a\n1000000000\n");
                assertQuery("SELECT a FROM z WHERE (a * 5) IN (el, '5000000000') ORDER BY a")
                        .noLeakCheck().returns("a\n");

                // Mixed widths next to the dynamic element: the untyped null matches the null key.
                assertQuery("SELECT a FROM z WHERE (a * 5) IN (el, 705_032_704, 5_000_000_000, null) ORDER BY a")
                        .noLeakCheck().returns("""
                                a
                                null
                                1000000000
                                """);

                // The dynamic element still decides its own rows: el + 3 is the key on the a = 2 row.
                assertQuery("SELECT a FROM z WHERE (a * 5) IN (el + 3, 5_000_000_000) ORDER BY a")
                        .noLeakCheck().returns("a\n2\n");
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
            }
        });
    }

    @Test
    public void testTwoConst() throws Exception {
        assertQuery("select * from x where x in (2,1)")
                .ddl("create table x as (" +
                        "select x from long_sequence(5)" +
                        ")")
                .returns("""
                        x
                        1
                        2
                        """);
    }

    // The re-bound value is 3 + 2^32. Read at long width it matches no element of the lists this
    // feeds, so the case expects nothing back; a key read at INT width wraps it onto exactly 3 and
    // would match. The two cases therefore differ in their result, which pins the width the key is
    // read at rather than only the absence of a crash.
    private static ObjList<BindVarTuple> bareKeyRebindCases(String intBindExpected) {
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok("int bind", intBindExpected, bvs -> bvs.setInt(0, 3)));
        cases.add(BindVarTuple.ok("re-bound to a long that wraps onto 3", NO_ROWS, bvs -> bvs.setLong(0, 4_294_967_299L)));
        return cases;
    }

    // The first case decides the type the query compiles the key at, so this is how a SHORT or BYTE
    // key reaches the compile-time split decision. The LONG re-bind that follows is the read the
    // split key could not serve.
    private static ObjList<BindVarTuple> narrowKeyRebindCases(String expected, String description, BindVariableTestSetter firstBind) {
        final ObjList<BindVarTuple> cases = new ObjList<>();
        cases.add(BindVarTuple.ok(description + " bind", expected, firstBind));
        cases.add(BindVarTuple.ok("re-bound to a long that wraps onto 3", NO_ROWS, bvs -> bvs.setLong(0, 4_294_967_299L)));
        return cases;
    }

    private <T extends Function & WidthCounter> void assertKeyReadOnceAtLongWidth(T key) throws SqlException {
        // two-constant path -> InLongTwoConstFunction
        key.reset();
        try (Function f = newInFunction(key, new IntConstant(7), new LongConstant(5_000_000_000L))) {
            f.init(null, sqlExecutionContext);
            Assert.assertFalse(f.getBool(null));
            Assert.assertEquals(0, key.intCalls());
            Assert.assertEquals(1, key.longCalls());
        }

        // constant-set path (>= 3 elements) -> InLongConstFunction
        key.reset();
        try (Function f = newInFunction(key, new IntConstant(7), new IntConstant(8), new LongConstant(5_000_000_000L))) {
            f.init(null, sqlExecutionContext);
            Assert.assertFalse(f.getBool(null));
            Assert.assertEquals(0, key.intCalls());
            Assert.assertEquals(1, key.longCalls());
        }

        // variable path (a non-constant element) -> InLongVarFunction
        key.reset();
        try (Function f = newInFunction(key, new NonConstIntElement(7), new LongConstant(5_000_000_000L))) {
            f.init(null, sqlExecutionContext);
            Assert.assertFalse(f.getBool(null));
            Assert.assertEquals(0, key.intCalls());
            Assert.assertEquals(1, key.longCalls());
        }
    }

    private Function newInFunction(Function key, Function... elements) throws SqlException {
        final ObjList<Function> args = new ObjList<>();
        final IntList argPositions = new IntList();
        args.add(key);
        argPositions.add(0);
        for (Function element : elements) {
            args.add(element);
            argPositions.add(0);
        }
        return new InLongFunctionFactory().newInstance(0, args, argPositions, configuration, sqlExecutionContext);
    }

    /**
     * The real {@link IndexedParameterLinkFunction} as an IN key, counting the width each read lands
     * on. The counting has to happen in a subclass because the link function delegates every
     * accessor to a bind variable the test cannot substitute; everything the factory actually asks
     * about - {@code isIntWidthStable()}, {@code isRowStable()}, {@code getType()} - still comes
     * from the class under test.
     */
    private static class CountingIndexedLink extends IndexedParameterLinkFunction implements WidthCounter {
        private int intCalls;
        private long longCalls;

        CountingIndexedLink() {
            super(0, ColumnType.INT, 0);
        }

        @Override
        public int getInt(Record rec) {
            intCalls++;
            return super.getInt(rec);
        }

        @Override
        public long getLong(Record rec) {
            longCalls++;
            return super.getLong(rec);
        }

        @Override
        public int intCalls() {
            return intCalls;
        }

        @Override
        public long longCalls() {
            return longCalls;
        }

        @Override
        public void reset() {
            intCalls = 0;
            longCalls = 0;
        }
    }

    /**
     * Stands in for an INT arithmetic function: it computes at long width under {@code getLong()},
     * so its two reads disagree once the INT value wraps, and it is not int-width stable. Counts the
     * reads so a test can assert which widths a row actually evaluated.
     */
    private static class CountingIntKey extends IntFunction {
        int intCalls;
        long longCalls;
        boolean nonDeterministic;
        int value;

        CountingIntKey(int value) {
            this.value = value;
        }

        @Override
        public int getInt(Record rec) {
            intCalls++;
            return value;
        }

        @Override
        public long getLong(Record rec) {
            longCalls++;
            return value;
        }

        @Override
        public boolean isNonDeterministic() {
            return nonDeterministic;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        void reset() {
            intCalls = 0;
            longCalls = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("counting_int_key");
        }
    }

    /**
     * The named counterpart of {@link CountingIndexedLink}. This is the only route to the named link
     * function's width answer: an SQL re-bind that changes the variable's type trips the assertion in
     * {@code NamedParameterLinkFunction.init()} before the key is ever read.
     */
    private static class CountingNamedLink extends NamedParameterLinkFunction implements WidthCounter {
        private int intCalls;
        private long longCalls;

        CountingNamedLink() {
            // The name keeps its leading colon: FunctionParser passes the SQL token verbatim, and
            // BindVariableServiceImpl.getFunction() asserts on it before stripping it for lookup.
            super(":k", ColumnType.INT);
        }

        @Override
        public int getInt(Record rec) {
            intCalls++;
            return super.getInt(rec);
        }

        @Override
        public long getLong(Record rec) {
            longCalls++;
            return super.getLong(rec);
        }

        @Override
        public int intCalls() {
            return intCalls;
        }

        @Override
        public long longCalls() {
            return longCalls;
        }

        @Override
        public void reset() {
            intCalls = 0;
            longCalls = 0;
        }
    }

    /**
     * A non-constant INT element, which routes the IN list to its variable (per-row) function.
     */
    private static class NonConstIntElement extends IntFunction {
        private final int value;

        NonConstIntElement(int value) {
            this.value = value;
        }

        @Override
        public int getInt(Record rec) {
            return value;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("non_const_int_element");
        }
    }

    /**
     * Reports which width a test key was read at.
     */
    private interface WidthCounter {
        int intCalls();

        long longCalls();

        void reset();
    }
}
