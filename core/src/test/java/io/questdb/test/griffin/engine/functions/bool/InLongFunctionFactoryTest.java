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

import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.bool.InLongFunctionFactory;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class InLongFunctionFactoryTest extends AbstractCairoTest {
    // The rows whose INT-arithmetic key lands on LONG_NULL at long width: row 3's product
    // overflows exactly onto it (its value is 0), row 5 is genuinely null. See
    // testNarrowSplitKeyNullElementMatchesEqNull.
    private static final String LONG_NULL_KEY_ROWS = """
            rn
            3
            5
            """;
    // The rows whose INT-arithmetic key is NULL, i.e. whose getInt() carries INT_NULL.
    private static final String NULL_KEY_ROWS = """
            rn
            1
            2
            5
            """;

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
    public void testNarrowSplitKeyNullElementMatchesEqNull() throws Exception {
        // An untyped NULL element reads a narrow-int key at the width '=' reads it: INT.
        // A split key - INT arithmetic, whose getInt() wraps mod 2^32 while getLong() widens -
        // is NULL exactly when its getInt() carries INT_NULL, which is what '=', IS NULL and the
        // projection all report. Reading the key at long width against the NULL element instead
        // made IN (null) disagree with all three, in both directions:
        //   - row 1 (65_536*32_768) and row 2 (-2^30 * 2) wrap onto INT_NULL, so they ARE null;
        //     IN (null) missed them because their long-width products (+/-2^31) are not LONG_NULL.
        //   - row 3 (2^30 * 8 * 2^30) has value 0, but its long-width product overflows exactly
        //     onto LONG_NULL, so IN (null) matched a row whose value is not null.
        // A LONG-typed null element (null::long) keeps long width: it matches '= null::long',
        // which reads the key with getLong().
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

                    // The deeper key: row 3's long-width product lands exactly on LONG_NULL while its
                    // value is 0, so a long-width probe matched it. It is not null and must not match.
                    assertQuery("SELECT rn FROM x WHERE (a*b*c) = null").noLeakCheck().returns(NULL_KEY_ROWS);
                    assertQuery("SELECT rn FROM x WHERE (a*b*c) IN (null)").noLeakCheck().returns(NULL_KEY_ROWS);

                    // Control: a LONG-typed null element keeps long width on both sides, so IN (null::long)
                    // and '= null::long' agree with each other - and select row 3, unlike the untyped null.
                    assertQuery("SELECT rn FROM x WHERE (a*b*c) = null::long")
                            .noLeakCheck().returns(LONG_NULL_KEY_ROWS);
                    assertQuery("SELECT rn FROM x WHERE (a*b*c) IN (null::long)")
                            .noLeakCheck().returns(LONG_NULL_KEY_ROWS);

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
    public void testNonDeterministicSplitKeyEvaluatedOncePerRow() throws SqlException {
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
    }

    @Test
    public void testNonNumericStringConstElementReadsAsNull() throws Exception {
        // Harmonize the all-constant IN path with the runtime path: a non-numeric string element
        // reads as LONG_NULL (via parseLongQuiet) instead of throwing, so "k IN ('abc', 5)" agrees
        // with a list carrying a dynamic sibling ("k IN (s, 5)", which already read 'abc' as
        // LONG_NULL). Before the fix the all-constant form threw "invalid LONG value [abc]". The
        // LONG_NULL element then matches a NULL key row on BOTH paths (row x=3), alongside k=5.
        assertMemoryLeak(() -> {
            execute("create table t as (select x id, case when x = 3 then null else x end k, 'abc' s from long_sequence(10))");
            // All-constant path: 'abc' -> LONG_NULL matches the NULL key row (id=3); 5 matches k=5
            // (id=5). Project the non-null id so the match is asserted without depending on how the
            // NULL key renders. No throw; before the fix this threw "invalid LONG value [abc]".
            assertQuery("select id from t where k in ('abc', 5)")
                    .noLeakCheck()
                    .returns("""
                            id
                            3
                            5
                            """);
            // The same element via a dynamic (column) sibling routes to the runtime path and agrees.
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
    public void testNarrowKeyOverridingGetLongIsNotAssumedWidthStable() throws Exception {
        // json_extract(..)::int implements Function directly rather than extending IntFunction, and its
        // getInt() and getLong() are two independent native parses: an out-of-INT-range number reads as
        // INT_NULL at INT width and as its full value at long width. It never declared that, and
        // isIntWidthStable() defaulted to true, so IN probed the key with getLong() only and disagreed
        // with both '=' and the projection - exactly the divergence the split key exists to close.
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
    public void testSplitKeyReadsSecondWidthOnlyOnMiss() throws SqlException {
        // A split key - an INT function that computes at long width under getLong() - is the only key
        // whose two reads can disagree, so the IN list probes it at both widths. An element matching
        // on the first width must not make the row pay for the other: each width is evaluated only
        // when an element actually reaches it.
        //
        // The list below mixes widths on purpose: the INT element reads the key at INT width, the
        // LONG one at long width. The key carries 7, so the INT element hits and the long read is
        // never needed.
        try (CountingIntKey key = new CountingIntKey(7)) {
            // two-constant path
            try (Function f = newInFunction(key, new IntConstant(7), new LongConstant(5_000_000_000L))) {
                Assert.assertTrue(f.getBool(null));
                Assert.assertEquals(1, key.intCalls);
                Assert.assertEquals(0, key.longCalls);

                // a miss on the INT element does reach the long width
                key.reset();
                key.value = 11;
                Assert.assertFalse(f.getBool(null));
                Assert.assertEquals(1, key.intCalls);
                Assert.assertEquals(1, key.longCalls);
            }

            // variable path: a non-constant element forces it, and is reached first
            key.reset();
            key.value = 7;
            try (Function f = newInFunction(key, new NonConstIntElement(7), new LongConstant(5_000_000_000L))) {
                Assert.assertTrue(f.getBool(null));
                Assert.assertEquals(1, key.intCalls);
                Assert.assertEquals(0, key.longCalls);

                key.reset();
                key.value = 11;
                Assert.assertFalse(f.getBool(null));
                Assert.assertEquals(1, key.intCalls);
                Assert.assertEquals(1, key.longCalls);
            }
        }
    }

    @Test
    public void testSplitKeyVarListPartitionsConstantsByWidth() throws Exception {
        // The var path hoists its constant elements into width-specific sets at construction, so the
        // per-row loop probes them once each instead of scanning every constant on every row. A split
        // key - INT arithmetic, whose getInt() wraps mod 2^32 while getLong() widens - is the only key
        // that can tell the two sets apart, so it is what pins the partition: (a * 5) reads 705_032_704
        // at INT width and 5_000_000_000 at long width for the same row, and each constant must land in
        // the set whose width '=' would compare it at.
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

                // The oracle: what the key expression carries at each width.
                assertQuery("SELECT a, (a * 5) wrapped, (a * 5)::long widened FROM z")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                a\twrapped\twidened
                                1000000000\t705032704\t5000000000
                                2\t10\t10
                                null\tnull\tnull
                                """);

                // An INT-typed constant wraps the key; a LONG-typed one widens it. Each selects the
                // same row through a different set, and neither matches the other's value.
                assertQuery("SELECT a FROM z WHERE (a * 5) IN (el, 705_032_704) ORDER BY a")
                        .noLeakCheck().returns("a\n1000000000\n");
                assertQuery("SELECT a FROM z WHERE (a * 5) IN (el, 5_000_000_000) ORDER BY a")
                        .noLeakCheck().returns("a\n1000000000\n");
                assertQuery("SELECT a FROM z WHERE (a * 5) IN (el, 705_032_704::long) ORDER BY a")
                        .noLeakCheck().returns("a\n");

                // A numeric string carries no declared width, so it takes the one its value would have
                // as a literal: an INT-range value wraps the key, a wider one widens it.
                assertQuery("SELECT a FROM z WHERE (a * 5) IN (el, '705032704') ORDER BY a")
                        .noLeakCheck().returns("a\n1000000000\n");
                assertQuery("SELECT a FROM z WHERE (a * 5) IN (el, '5000000000') ORDER BY a")
                        .noLeakCheck().returns("a\n1000000000\n");

                // Both sets at once, next to the dynamic element: an untyped null joins the INT-width
                // set (as '=' resolves it on a narrow key), so the null row matches through it.
                assertQuery("SELECT a FROM z WHERE (a * 5) IN (el, 705_032_704, 5_000_000_000, null) ORDER BY a")
                        .noLeakCheck().returns("""
                                a
                                null
                                1000000000
                                """);

                // The dynamic element still decides its own rows: el + 3 is the key on the a = 2 row.
                assertQuery("SELECT a FROM z WHERE (a * 5) IN (el + 3, 5_000_000_000) ORDER BY a")
                        .noLeakCheck().returns("""
                                a
                                2
                                1000000000
                                """);
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
        public boolean isIntWidthStable() {
            return false;
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
}
