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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.std.Uuid;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Validates that wrapping a constant in {@code cast(<const>, <type>)} (including
 * the {@code <const>::TYPE} shorthand) preserves both JIT eligibility and result
 * semantics.
 *
 * <p>The fold in {@link io.questdb.jit.CompiledFilterIRSerializer} only unwraps a
 * cast when its target type tag matches the predicate's column type tag. The fuzz
 * covers two shapes:
 * <ul>
 *   <li><b>Matching cast.</b> {@code <col_T> op <const>::T}. The fold engages and
 *       JIT runs. The harness asserts: JIT cast form == bare-constant JIT form
 *       (proves the fold reduces to the bare path identically), and JIT cast form
 *       == Java cast form (proves end-to-end correctness against the user's
 *       semantics).</li>
 *   <li><b>Cross-type cast.</b> {@code <col_T> op <const>::U}, where U != T. The
 *       fold refuses (semantics could differ -- e.g. cross-type NULL sentinels),
 *       JIT bails out, and the Java filter handles the query as written. The
 *       harness asserts JIT does NOT engage.</li>
 * </ul>
 *
 * <p>Probes include the negated MIN boundary for byte/short/int/long, which
 * exercises both the truncate-after-sign path in {@code serializeNumber} and the
 * null-aware compare in the JIT C++ backend ({@code INT_MIN}/{@code LONG_MIN}
 * are QuestDB's NULL sentinels, and the default {@code enableJitNullChecks=true}
 * keeps SQL three-valued semantics intact).
 */
public class CompiledFilterCastFuzzTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(CompiledFilterCastFuzzTest.class);
    private static final int ROW_COUNT = 200;
    private final StringSink javaSink = new StringSink();
    private final StringSink jitBareSink = new StringSink();
    private final StringSink jitCastSink = new StringSink();

    @Override
    @Before
    public void setUp() {
        Assume.assumeTrue(JitUtil.isJitSupported());
        super.setUp();
    }

    @Test
    public void testFuzz() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (select " +
                            " rnd_boolean() abool," +
                            " rnd_byte() abyte," +
                            " rnd_short() ashort," +
                            " rnd_char() achar," +
                            " rnd_int() anint," +
                            " rnd_long() along," +
                            " rnd_float() afloat," +
                            " rnd_double() adouble," +
                            " rnd_uuid4() auuid," +
                            " rnd_symbol('A','B','C','D') asymbol," +
                            " rnd_date(to_date('2025', 'yyyy'), to_date('2027', 'yyyy'), 0) adate," +
                            " rnd_timestamp(" +
                            "   to_timestamp('2026-01-01', 'yyyy-MM-dd')," +
                            "   to_timestamp('2026-12-31', 'yyyy-MM-dd'), 0) ats2," +
                            " timestamp_sequence(400000000000, 500000000) ats" +
                            " from long_sequence(" + ROW_COUNT + ")) timestamp(ats)"
            );

            // Matching cast/column type pairs: must fold, must JIT, must agree with
            // Java. Probes include the negated MIN boundary for every numeric type:
            // INT_MIN/LONG_MIN coincide with QuestDB's NULL sentinels, but the JIT
            // backend's null-aware compare (default enableJitNullChecks=true) preserves
            // SQL three-valued semantics, so JIT and Java agree on those values.
            assertMatchingProbe("abool", "boolean", new String[]{"true", "false"});
            assertMatchingProbe("abyte", "byte", numericProbes(rnd, 8));
            assertMatchingProbe("ashort", "short", numericProbes(rnd, 16));
            assertMatchingProbe("anint", "int", numericProbes(rnd, 32));
            assertMatchingProbe("along", "long", numericProbes(rnd, 64));
            assertMatchingProbe("adouble", "double", floatProbes(rnd));
            assertMatchingProbe("achar", "char", charProbes(rnd));
            assertMatchingProbe("auuid", "uuid", uuidProbes(rnd));
            assertMatchingProbe("asymbol", "symbol", new String[]{"A", "B", "C", "D", "E"});
            assertMatchingProbe("adate", "date", dateProbes());
            assertMatchingProbe("ats2", "timestamp", timestampProbes());

            // No-op column-side casts: `col::T op const` where T == col's type.
            // The fold should engage and produce identical results to the bare column.
            // Coverage spans every supported type so the metadata-direct match check
            // in descend exercises each columnTypeCode entry.
            assertColumnSideCastProbe("abool", "boolean", new String[]{"true", "false"});
            assertColumnSideCastProbe("abyte", "byte", numericProbes(rnd, 8));
            assertColumnSideCastProbe("ashort", "short", numericProbes(rnd, 16));
            assertColumnSideCastProbe("achar", "char", charProbes(rnd));
            assertColumnSideCastProbe("anint", "int", numericProbes(rnd, 32));
            assertColumnSideCastProbe("along", "long", numericProbes(rnd, 64));
            assertColumnSideCastProbe("adouble", "double", floatProbes(rnd));
            assertColumnSideCastProbe("auuid", "uuid", uuidProbes(rnd));
            assertColumnSideCastProbe("asymbol", "symbol", new String[]{"A", "B", "C", "D", "E"});
            assertColumnSideCastProbe("adate", "date", dateProbes());
            assertColumnSideCastProbe("ats2", "timestamp", timestampProbes());

            // Cross-type casts (constant side): fold refuses, JIT bails, Java handles.
            assertCrossTypeProbe("abyte", "short", numericProbes(rnd, 7));
            assertCrossTypeProbe("abyte", "int", numericProbes(rnd, 7));
            assertCrossTypeProbe("abyte", "long", numericProbes(rnd, 7));
            assertCrossTypeProbe("ashort", "int", numericProbes(rnd, 15));
            assertCrossTypeProbe("ashort", "long", numericProbes(rnd, 15));
            assertCrossTypeProbe("anint", "long", numericProbes(rnd, 31));
            assertCrossTypeProbe("afloat", "double", floatProbes(rnd));

            // Cross-type column-side casts: same -- fold refuses, Java handles.
            assertCrossTypeColumnProbe("abyte", "long", numericProbes(rnd, 7));
            assertCrossTypeColumnProbe("anint", "long", numericProbes(rnd, 31));
        });
    }

    private void assertColumnSideCastProbe(String col, String castType, String[] values) throws Exception {
        // `col::T op const` is fold-equivalent to `col op const`. We assert end-to-end
        // correctness against Java and that the cast form actually JITs.
        for (String v : values) {
            String literal = needsQuoting(castType) ? "'" + v + "'" : v;
            String constLiteral = literal.startsWith("-") ? "(" + literal + ")" : literal;
            String bareQuery = "x where " + col + " = " + literal;
            String castQuery = "x where " + col + "::" + castType + " = " + constLiteral;

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            runIntoSink(castQuery, javaSink, false);

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_FORCE_SCALAR);
            runIntoSink(bareQuery, jitBareSink, true);
            runIntoSink(castQuery, jitCastSink, true);
            TestUtils.assertEquals(
                    "scalar JIT diverged from Java for column-side cast: " + castQuery,
                    javaSink, jitCastSink
            );
            TestUtils.assertEquals(
                    "scalar JIT diverged between bare and column-side cast: " + castQuery,
                    jitBareSink, jitCastSink
            );

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            runIntoSink(castQuery, jitCastSink, true);
            TestUtils.assertEquals(
                    "vectorized JIT diverged from Java for column-side cast: " + castQuery,
                    javaSink, jitCastSink
            );
        }
    }

    private void assertCrossTypeColumnProbe(String col, String castType, String[] values) throws Exception {
        for (String v : values) {
            String literal = needsQuoting(castType) ? "'" + v + "'" : v;
            String castQuery = "x where " + col + "::" + castType + " = " + literal;
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            try (RecordCursorFactory factory = select(castQuery)) {
                Assert.assertFalse(
                        "cross-type column-side cast must NOT JIT: " + castQuery,
                        factory.usesCompiledFilter()
                );
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory.getMetadata(), cursor, jitCastSink);
                }
            }
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            runIntoSink(castQuery, javaSink, false);
            TestUtils.assertEquals(
                    "Java result inconsistent across modes for: " + castQuery,
                    javaSink, jitCastSink
            );
        }
    }

    private void assertCrossTypeProbe(String col, String castType, String[] values) throws Exception {
        // Cross-type cast: the fold must refuse. Compares JIT-enabled (which bails
        // back to Java) against pure Java reference to confirm both produce the same
        // output -- a regression that lets cross-type casts JIT would surface here.
        for (String v : values) {
            String literal = needsQuoting(castType) ? "'" + v + "'" : v;
            String castLiteral = literal.startsWith("-") ? "(" + literal + ")" : literal;
            String castExpr = castLiteral + "::" + castType;
            for (String op : new String[]{"=", "<>"}) {
                String castQuery = "x where " + col + " " + op + " " + castExpr;
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                runIntoSink(castQuery, javaSink, false);
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                runIntoSink(castQuery, jitCastSink, false);
                TestUtils.assertEquals(
                        "JIT fallback diverged from Java reference for cross-type cast: " + castQuery,
                        javaSink, jitCastSink
                );
            }
        }
    }

    private void assertEqualBareAndCastJit(String col, String op, String literal, String castExpr) throws Exception {
        String bareQuery = "x where " + col + " " + op + " " + literal;
        String castQuery = "x where " + col + " " + op + " " + castExpr;

        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_FORCE_SCALAR);
        runIntoSink(bareQuery, jitBareSink, true);
        runIntoSink(castQuery, jitCastSink, true);
        TestUtils.assertEquals(
                "scalar JIT diverged between bare and cast form for: " + castQuery,
                jitBareSink, jitCastSink
        );

        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
        runIntoSink(bareQuery, jitBareSink, true);
        runIntoSink(castQuery, jitCastSink, true);
        TestUtils.assertEquals(
                "vectorized JIT diverged between bare and cast form for: " + castQuery,
                jitBareSink, jitCastSink
        );
    }

    private void assertEqualJavaAndCastJit(String col, String op, String castExpr) throws Exception {
        String castQuery = "x where " + col + " " + op + " " + castExpr;

        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        runIntoSink(castQuery, javaSink, false);

        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_FORCE_SCALAR);
        runIntoSink(castQuery, jitCastSink, true);
        TestUtils.assertEquals(
                "scalar JIT diverged from Java for cast form: " + castQuery,
                javaSink, jitCastSink
        );

        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
        runIntoSink(castQuery, jitCastSink, true);
        TestUtils.assertEquals(
                "vectorized JIT diverged from Java for cast form: " + castQuery,
                javaSink, jitCastSink
        );
    }

    private void assertMatchingProbe(String col, String castType, String[] values) throws Exception {
        for (String v : values) {
            String literal = needsQuoting(castType) ? "'" + v + "'" : v;
            String castLiteral = literal.startsWith("-") ? "(" + literal + ")" : literal;
            String castExpr = castLiteral + "::" + castType;
            String castAsExpr = "cast(" + literal + " AS " + castType + ")";

            for (String op : new String[]{"=", "<>"}) {
                assertEqualBareAndCastJit(col, op, literal, castExpr);
                assertEqualBareAndCastJit(col, op, literal, castAsExpr);
                assertEqualJavaAndCastJit(col, op, castExpr);
                assertEqualJavaAndCastJit(col, op, castAsExpr);
            }

            if (supportsOrdering(castType)) {
                for (String op : new String[]{"<", "<=", ">", ">="}) {
                    assertEqualBareAndCastJit(col, op, literal, castExpr);
                    assertEqualJavaAndCastJit(col, op, castExpr);
                }
            }
        }
    }

    private String[] charProbes(Rnd rnd) {
        return new String[]{
                "A", "Z", "a", "z", "0",
                String.valueOf((char) ('A' + rnd.nextPositiveInt() % 26)),
        };
    }

    private String[] dateProbes() {
        return new String[]{
                "2025-01-01", "2026-06-15", "2027-03-09",
        };
    }

    private String[] floatProbes(Rnd rnd) {
        return new String[]{
                "0.0", "1.5", "-3.25", "123.456",
                Double.toString(rnd.nextDouble() * 1000.0 - 500.0),
        };
    }

    private boolean needsQuoting(String castType) {
        return switch (castType) {
            case "char", "uuid", "symbol", "date", "timestamp" -> true;
            default -> false;
        };
    }

    private String[] numericProbes(Rnd rnd, int bits) {
        long max = bits == 64 ? Long.MAX_VALUE : (1L << (bits - 1)) - 1;
        long min = -max - 1;
        long midRange = Math.max(1, Math.min(Integer.MAX_VALUE, max / 2));
        return new String[]{
                "0", "1", "-1",
                Long.toString(max),
                Long.toString(min),       // negated MIN: NULL sentinel for INT/LONG
                Long.toString(min + 1),
                Long.toString(rnd.nextLong(midRange)),
                Long.toString(-rnd.nextLong(midRange)),
        };
    }

    private void runIntoSink(String query, StringSink target, boolean expectJit) throws Exception {
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertEquals(
                    (expectJit ? "expected JIT for: " : "expected NO JIT for: ") + query,
                    expectJit, factory.usesCompiledFilter()
            );
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                println(factory.getMetadata(), cursor, target);
            }
        }
    }

    private boolean supportsOrdering(String castType) {
        return switch (castType) {
            case "boolean", "uuid", "symbol" -> false;
            default -> true;
        };
    }

    private String[] timestampProbes() {
        return new String[]{
                "2026-01-01T00:00:00.000000Z",
                "2026-06-15T12:34:56.000000Z",
                "2027-12-31T23:59:59.999999Z",
        };
    }

    private String[] uuidProbes(Rnd rnd) {
        Uuid u = new Uuid(rnd.nextLong(), rnd.nextLong());
        StringSink s = new StringSink();
        u.toSink(s);
        return new String[]{
                "11111111-2222-3333-4444-555555555555",
                "00000000-0000-0000-0000-000000000000",
                s.toString(),
        };
    }
}
