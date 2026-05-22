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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.jit.JitUtil;
import io.questdb.std.Numbers;
import io.questdb.std.Uuid;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for advanced features and scenarios, such as col tops, bind variables,
 * random access, record behavior, and so on.
 */
public class CompiledFilterTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        // Disable the test suite on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());
        super.setUp();
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_PRETOUCH_THRESHOLD, "1.0");
    }

    @Test
    public void testAllBindVariableTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (select" +
                            " rnd_boolean() aboolean," +
                            " rnd_byte(2,50) abyte," +
                            " rnd_geohash(4) ageobyte," +
                            " rnd_short(10,1024) ashort," +
                            " rnd_geohash(12) ageoshort," +
                            " rnd_char() achar," +
                            " rnd_int() anint," +
                            " rnd_geohash(16) ageoint," +
                            " rnd_symbol(4,4,4,2) asymbol," +
                            " rnd_float(2) afloat," +
                            " rnd_long() along," +
                            " rnd_double(2) adouble," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) adate," +
                            " rnd_geohash(32) ageolong," +
                            " timestamp_sequence(400000000000, 500000000) atimestamp" +
                            " from long_sequence(100)) timestamp(atimestamp)"
            );

            bindVariableService.clear();
            bindVariableService.setBoolean("aboolean", false);
            bindVariableService.setByte("abyte", (byte) 28);
            bindVariableService.setGeoHash("ageobyte", 0, ColumnType.getGeoHashTypeWithBits(4));
            bindVariableService.setShort("ashort", (short) 243);
            bindVariableService.setGeoHash("ageoshort", 0b011011000010L, ColumnType.getGeoHashTypeWithBits(12));
            bindVariableService.setChar("achar", 'O');
            bindVariableService.setInt("anint", 2085282008);
            bindVariableService.setGeoHash("ageoint", 0b0101011010111101L, ColumnType.getGeoHashTypeWithBits(16));
            bindVariableService.setStr("asymbol", "HYRX");
            bindVariableService.setFloat("afloat", 0.48820507526397705f);
            bindVariableService.setLong("along", -4986232506486815364L);
            bindVariableService.setDouble("adouble", 0.42281342727402726);
            bindVariableService.setDate("adate", 1443479385706L);
            bindVariableService.setGeoHash("ageolong", 0b11010000001110101000110100011010L, ColumnType.getGeoHashTypeWithBits(32));
            bindVariableService.setTimestamp("atimestamp", 400500000000L);

            final String query = "select * from x where" +
                    " aboolean = :aboolean" +
                    " and abyte = :abyte" +
                    " and ageobyte = :ageobyte" +
                    " and ashort = :ashort" +
                    " and ageoshort = :ageoshort" +
                    " and achar = :achar" +
                    " and anint = :anint" +
                    " and ageoint = :ageoint" +
                    " and asymbol = :asymbol" +
                    " and afloat = :afloat" +
                    " and along = :along" +
                    " and adouble = :adouble" +
                    " and adate = :adate" +
                    " and ageolong = :ageolong" +
                    " and atimestamp = :atimestamp";
            final String expected = """
                    aboolean\tabyte\tageobyte\tashort\tageoshort\tachar\tanint\tageoint\tasymbol\tafloat\talong\tadouble\tadate\tageolong\tatimestamp
                    false\t28\t0000\t243\t011011000010\tO\t2085282008\t0101011010111101\tHYRX\t0.48820508\t-4986232506486815364\t0.42281342727402726\t2015-09-28T22:29:45.706Z\t11010000001110101000110100011010\t1970-01-05T15:15:00.000000Z
                    """;

            assertSql(expected, query);
            assertSqlRunWithJit(query);
        });
    }

    @Test
    public void testBindVariableCastPreservesJit() throws Exception {
        // PR #6413's `<col> = $1::TYPE` workaround: when the bind variable is bound
        // as the cast target type, the cast is a no-op and JIT can engage. When the
        // bound type differs, the cast does real coercion that the JIT IR can't
        // replicate, so we bail to Java. Covers every JIT-supported scalar type so
        // bindVariableTypeCode dispatch is exercised end-to-end.
        Uuid uuidValue = new Uuid();
        uuidValue.of("11111111-2222-3333-4444-555555555555");
        assertMemoryLeak(() -> {
            execute(
                    "create table x (" +
                            "abool BOOLEAN, abyte BYTE, ashort SHORT, achar CHAR, " +
                            "anint INT, along LONG, afloat FLOAT, adouble DOUBLE, " +
                            "auuid UUID, aipv4 IPv4, " +
                            "adate DATE, ats2 TIMESTAMP, ats TIMESTAMP" +
                            ") timestamp(ats)"
            );
            execute(
                    "insert into x values (true, 1, 10, 'A', 100, 1000, 1.5, 2.5, " +
                            "'11111111-2222-3333-4444-555555555555', " +
                            "'1.2.3.4', " +
                            "'2026-06-15T00:00:00.000Z', " +
                            "'2026-01-01T00:00:00.000000Z', '2026-01-01T00:00:00.000000Z')"
            );

            // Read back the row's date and timestamp so the bind values match without
            // magic epoch numbers.
            long rowDate, rowTs;
            try (RecordCursorFactory factory = select("select adate, ats2 from x");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                rowDate = cursor.getRecord().getDate(0);
                rowTs = cursor.getRecord().getTimestamp(1);
            }

            // Bind variables bound to types matching the cast target -> fold engages.
            // IPv4 covers the I4_TYPE-mapping `bindVariableTypeCode` branch shared with
            // INT/SYMBOL/etc. Parametrised types (GEOHASH(Nb), TIMESTAMP_NANO) aren't
            // covered here because their type literals are not bare CONSTANT nodes, so
            // `isCastTargetTypeLiteral` correctly refuses to fold them.
            bindVariableService.clear();
            bindVariableService.setBoolean(0, true);
            bindVariableService.setByte(1, (byte) 1);
            bindVariableService.setShort(2, (short) 10);
            bindVariableService.setChar(3, 'A');
            bindVariableService.setInt(4, 100);
            bindVariableService.setLong(5, 1000L);
            bindVariableService.setFloat(6, 1.5f);
            bindVariableService.setDouble(7, 2.5d);
            bindVariableService.setUuid(8, uuidValue.getLo(), uuidValue.getHi());
            bindVariableService.setIPv4(9, "1.2.3.4");
            bindVariableService.setDate(10, rowDate);
            bindVariableService.setTimestamp(11, rowTs);

            String[] queriesThatMustJit = {
                    "select * from x where abool = $1::boolean",
                    "select * from x where abyte = $2::byte",
                    "select * from x where ashort = $3::short",
                    "select * from x where achar = $4::char",
                    "select * from x where anint = $5::int",
                    "select * from x where along = $6::long",
                    "select * from x where afloat = cast($7 AS float)",
                    "select * from x where adouble = $8::double",
                    "select * from x where auuid = $9::uuid",
                    "select * from x where aipv4 = $10::ipv4",
                    "select * from x where adate = $11::date",
                    "select * from x where ats2 = $12::timestamp",
            };
            for (String q : queriesThatMustJit) {
                try (RecordCursorFactory factory = select(q)) {
                    Assert.assertTrue("expected JIT for: " + q, factory.usesCompiledFilter());
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue("expected at least one row for: " + q, cursor.hasNext());
                    }
                }
            }

            // NULL bind variable with matching cast: fold must engage and produce the
            // same output as the bare-bind form. We compare cast vs bare bind (rather
            // than vs Java) so the assertion proves only what's in scope here -- the
            // fold doesn't change semantics relative to the bare bind path.
            bindVariableService.clear();
            bindVariableService.setLong(0); // typed NULL LONG
            sink.clear();
            try (RecordCursorFactory factory = select("select * from x where along = $1");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(
                        "expected JIT for bare NULL LONG bind var",
                        factory.usesCompiledFilter()
                );
                println(factory.getMetadata(), cursor, sink);
            }
            String bareOut = sink.toString();
            try (RecordCursorFactory factory = select("select * from x where along = $1::long");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(
                        "expected JIT for NULL LONG bind var with ::long cast",
                        factory.usesCompiledFilter()
                );
                sink.clear();
                println(factory.getMetadata(), cursor, sink);
            }
            Assert.assertEquals("cast form must match bare bind form for NULL LONG", bareOut, sink.toString());

            // Mismatched bound vs cast target: cast performs real coercion that the
            // JIT IR can't replicate, so we bail to Java. STRING -> INT exercises the
            // boundType != castType path with a STRING bind var (PG's typical default
            // before binding); LONG -> INT covers two non-STRING types differing.
            bindVariableService.clear();
            bindVariableService.setStr(0, "100");
            try (RecordCursorFactory factory = select("select * from x where anint = $1::int")) {
                Assert.assertFalse(
                        "expected Java fallback for STRING bind var with ::int cast",
                        factory.usesCompiledFilter()
                );
            }
            bindVariableService.clear();
            bindVariableService.setLong(0, 100L);
            try (RecordCursorFactory factory = select("select * from x where anint = $1::int")) {
                Assert.assertFalse(
                        "expected Java fallback for LONG bind var with ::int cast",
                        factory.usesCompiledFilter()
                );
            }
        });
    }

    @Test
    public void testBindVariableNullCheckScalar() throws Exception {
        testBindVariableNullCheck(SqlJitMode.JIT_MODE_FORCE_SCALAR);
    }

    @Test
    public void testBindVariableNullCheckVectorized() throws Exception {
        testBindVariableNullCheck(SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testBindVariablesFilterWithColTops() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            bindVariableService.setLong(0, 3);

            final String query = "select * from t1 where x = $1";
            final String expected = """
                    x\tts\tj
                    3\t1970-01-01T00:00:02.000000Z\tnull
                    3\t1970-01-01T00:01:42.000000Z\t7746536061816329025
                    """;

            testFilterWithColTops(query, expected, SqlJitMode.JIT_MODE_ENABLED);
        });
    }

    @Test
    public void testColumnCastPreservesJit() throws Exception {
        // No-op casts wrapped around a column should fold to the bare column for JIT.
        // Cross-type column casts must still bail to Java since the JIT IR has no
        // cast op to perform the runtime conversion.
        assertMemoryLeak(() -> {
            execute(
                    "create table x (" +
                            "abyte BYTE, ashort SHORT, anint INT, along LONG, " +
                            "auuid UUID, asymbol SYMBOL, " +
                            "ats2 TIMESTAMP, ats TIMESTAMP" +
                            ") timestamp(ats)"
            );
            execute(
                    "insert into x values (1, 10, 100, 1000, " +
                            "'11111111-2222-3333-4444-555555555555', 'sym', " +
                            "'2026-01-01T00:00:00.000000Z', '2026-01-01T00:00:00.000000Z')"
            );

            String[] queriesThatMustJit = {
                    "select * from x where abyte::byte = 1",
                    "select * from x where ashort::short = 10",
                    "select * from x where anint::int = 100",
                    "select * from x where along::long = 1000",
                    "select * from x where auuid::uuid = '11111111-2222-3333-4444-555555555555'",
                    "select * from x where asymbol::symbol = 'sym'",
                    "select * from x where ats2::timestamp = '2026-01-01T00:00:00.000000Z'",
                    // Both sides cast (matching their own column types).
                    "select * from x where auuid::uuid = '11111111-2222-3333-4444-555555555555'::uuid",
            };
            String[] queriesThatMustNotJit = {
                    // Cross-type column cast: JIT can't perform the actual coercion.
                    "select * from x where anint::long = 100",
                    "select * from x where abyte::int = 1",
            };

            StringBuilder failures = new StringBuilder();
            for (String q : queriesThatMustJit) {
                try (RecordCursorFactory factory = select(q)) {
                    if (!factory.usesCompiledFilter()) {
                        failures.append("\n  expected JIT, got Java: ").append(q);
                    }
                }
            }
            for (String q : queriesThatMustNotJit) {
                try (RecordCursorFactory factory = select(q)) {
                    if (factory.usesCompiledFilter()) {
                        failures.append("\n  expected Java fallback, got JIT: ").append(q);
                    }
                }
            }
            if (failures.length() > 0) {
                Assert.fail("Column-cast-fold expectations not met:" + failures);
            }
        });
    }

    @Test
    public void testConstantCastPreservesJit() throws Exception {
        // Regression for the legacy `<col> = '...'::uuid` workaround from PR #6413
        // (and any other literal `'...'::TYPE`): the cast was tripping the JIT IR
        // serializer's "invalid operator: cast" rejection and silently falling back
        // to the Java filter. Asserts JIT engagement for matching cast/column types,
        // and asserts NO JIT for cross-type or PG-rewritten casts (where folding
        // would change semantics). Aggregates failures so a single run lists every
        // miss.
        assertMemoryLeak(() -> {
            execute(
                    "create table x (" +
                            "abool BOOLEAN, " +
                            "abyte BYTE, " +
                            "ashort SHORT, " +
                            "achar CHAR, " +
                            "anint INT, " +
                            "along LONG, " +
                            "afloat FLOAT, " +
                            "adouble DOUBLE, " +
                            "auuid UUID, " +
                            "asymbol SYMBOL, " +
                            "adate DATE, " +
                            // ats is the designated timestamp -- predicates on it are
                            // pushed down as interval scans, so we JIT against ats2.
                            "ats2 TIMESTAMP, " +
                            "ats TIMESTAMP" +
                            ") timestamp(ats)"
            );
            execute(
                    "insert into x values (" +
                            "true, 1, 10, 'A', 100, 1000, 1.5, 2.5, " +
                            "'11111111-2222-3333-4444-555555555555', " +
                            "'sym', " +
                            "'2026-01-01', " +
                            "'2026-01-01T00:00:00.000000Z', " +
                            "'2026-01-01T00:00:00.000000Z')"
            );

            // NB: in QuestDB, `::float` is rewritten to DOUBLE for PG compatibility,
            // so `afloat = 1.5::float` is semantically `afloat = 1.5::double`. The
            // type-tag-match guard correctly refuses to fold this, and the query
            // falls back to Java. Test FLOAT via the unambiguous `cast(... AS float)`
            // form instead.
            String[] queriesThatMustJit = {
                    // baseline: constant without cast already JITs
                    "select * from x where abool = true",
                    "select * from x where abyte = 1",
                    "select * from x where ashort = 10",
                    "select * from x where achar = 'A'",
                    "select * from x where anint = 100",
                    "select * from x where along = 1000",
                    "select * from x where afloat = 1.5",
                    "select * from x where adouble = 2.5",
                    "select * from x where auuid = '11111111-2222-3333-4444-555555555555'",
                    "select * from x where asymbol = 'sym'",
                    "select * from x where adate = '2026-01-01'",
                    "select * from x where ats2 = '2026-01-01T00:00:00.000000Z'",
                    // matching `::TYPE` cast on the constant side
                    "select * from x where abool = true::boolean",
                    "select * from x where abyte = 1::byte",
                    "select * from x where ashort = 10::short",
                    "select * from x where achar = 'A'::char",
                    "select * from x where anint = 100::int",
                    "select * from x where along = 1000::long",
                    "select * from x where afloat = cast(1.5 AS float)",
                    "select * from x where adouble = 2.5::double",
                    "select * from x where auuid = '11111111-2222-3333-4444-555555555555'::uuid",
                    "select * from x where asymbol = 'sym'::symbol",
                    "select * from x where adate = '2026-01-01'::date",
                    "select * from x where ats2 = '2026-01-01T00:00:00.000000Z'::timestamp",
                    // `cast(<const> AS TYPE)` form parses to the same FUNCTION/cast AST node
                    "select * from x where auuid = cast('11111111-2222-3333-4444-555555555555' AS uuid)",
                    "select * from x where ats2 = cast('2026-01-01T00:00:00.000000Z' AS timestamp)",
                    // mirrors the user-reported shape: AND-chain with literal casts on the constant side
                    "select * from x where auuid = '11111111-2222-3333-4444-555555555555'::uuid and adate = '2026-01-01'::date",
                    // negated-constant cast: exercises the OPERATION(-, CONSTANT) branch of
                    // isFoldableConstantArg AND the LONG_MIN/INT_MIN/SHORT_MIN/BYTE_MIN
                    // sign-truncate path in serializeNumber. The negative MIN values
                    // happen to coincide with INT/LONG NULL sentinels; the JIT's
                    // null-aware compare keeps Java semantics intact.
                    "select * from x where abyte > (-128)::byte",
                    "select * from x where ashort > (-32768)::short",
                    "select * from x where anint > (-2147483648)::int",
                    "select * from x where along > (-9223372036854775808)::long",
            };

            // Cross-type or PG-rewritten casts must NOT fold. They fall back to the Java
            // filter, which preserves the user's stated cast semantics. The Java path
            // produces correct results; the only assertion here is that JIT does not
            // engage (since folding would change semantics, e.g. INT NULL sentinel,
            // or different timestamp precision drivers for ::timestamp_ns vs ::timestamp).
            String[] queriesThatMustNotJit = {
                    "select * from x where afloat = 1.5::float",         // ::float -> DOUBLE
                    "select * from x where anint = 100::long",           // wider cast
                    "select * from x where abyte = 1::short",            // wider cast
                    // ::timestamp_ns and ::timestamp share the TIMESTAMP tag but use
                    // different precision drivers; the full-type guard must reject this.
                    "select * from x where ats2 = '2026-01-01T00:00:00.000000Z'::timestamp_ns",
            };

            StringBuilder failures = new StringBuilder();
            for (String q : queriesThatMustJit) {
                try (RecordCursorFactory factory = select(q)) {
                    if (!factory.usesCompiledFilter()) {
                        failures.append("\n  expected JIT, got Java: ").append(q);
                    }
                }
            }
            for (String q : queriesThatMustNotJit) {
                try (RecordCursorFactory factory = select(q)) {
                    if (factory.usesCompiledFilter()) {
                        failures.append("\n  expected Java fallback, got JIT: ").append(q);
                    }
                }
            }
            if (failures.length() > 0) {
                Assert.fail("Cast-fold expectations not met:" + failures);
            }

            // Result-correctness checks for load-bearing cases. The fuzz covers the
            // wide surface end-to-end; these explicit assertions verify that the
            // user-reported scenario and the negated-MIN boundaries (which exercise
            // parseSignedLong and the JIT null-aware compare) produce the same rows
            // as the Java filter would. A bug where JIT engages but emits incorrect
            // IR would surface here even when usesCompiledFilter() is true.
            assertSql(
                    "count\n1\n",
                    "select count() from x where auuid = '11111111-2222-3333-4444-555555555555'::uuid and adate = '2026-01-01'::date"
            );
            // BYTE_MIN/SHORT_MIN are NOT NULL sentinels in QuestDB, so `col > -MIN`
            // matches every non-null row.
            assertSql("count\n1\n", "select count() from x where abyte > (-128)::byte");
            assertSql("count\n1\n", "select count() from x where ashort > (-32768)::short");
            // INT_MIN and LONG_MIN ARE NULL sentinels, so the cast result is NULL and
            // `col > NULL` evaluates to NULL (no rows match).
            assertSql("count\n0\n", "select count() from x where anint > (-2147483648)::int");
            assertSql("count\n0\n", "select count() from x where along > (-9223372036854775808)::long");
        });
    }

    @Test
    public void testDeferredSymbolConstants() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " timestamp_sequence(400000000000, 500000000) ts," +
                    " x l," +
                    " rnd_symbol('A','B','C') sym" +
                    " from long_sequence(5)) timestamp(ts)");

            // The column order is important here, since we want
            // query and table column indexes to be different.
            final String query = "select sym, l, ts from x where sym = 'B' or sym = 'D' or sym = 'F'";
            final String expected = """
                    sym\tl\tts
                    B\t3\t1970-01-05T15:23:20.000000Z
                    """;

            assertSql(expected, query);
            assertSqlRunWithJit(query);

            execute("insert into x select " +
                    " timestamp_sequence(500000000000, 500000000) ts," +
                    " (x+5) l," +
                    " rnd_symbol('D','E','F') sym " +
                    "from long_sequence(5)");

            final String expected2 = """
                    sym\tl\tts
                    B\t3\t1970-01-05T15:23:20.000000Z
                    F\t6\t1970-01-06T18:53:20.000000Z
                    F\t7\t1970-01-06T19:01:40.000000Z
                    D\t9\t1970-01-06T19:18:20.000000Z
                    """;

            assertSql(expected2, query);
            assertSqlRunWithJit(query);
        });
    }

    @Test
    public void testFilteringOnSingleQuote() throws Exception {
        assertQueryAndPlan("Time\tSpread\tBid_Volume\task_volume\n",
                """
                        SELECT timestamp as Time,
                        avg(asks[1,1]-bids[1,1]) as Spread,
                        sum(bids[1,1]*bids[2,1]) as Bid_Volume,
                        sum(asks[1,1]*asks[2,1]) as ask_volume
                        FROM market_data
                        WHERE symbol = ''''
                        SAMPLE BY 1s
                        ORDER BY timestamp DESC
                        LIMIT 6;""",
                """
                        
                        CREATE TABLE 'market_data' (\s
                        \ttimestamp TIMESTAMP,
                        \tsymbol SYMBOL CAPACITY 16384 CACHE,
                        \tbids DOUBLE[][],
                        \tasks DOUBLE[][]
                        ) timestamp(timestamp);""",
                "Time###DESC",
                "INSERT INTO market_data (timestamp, symbol, bids, asks) " +
                        "VALUES " +
                        "(0, 'abc', array[[1d,2d],[3d,4d]], array[[2d,3d],[4d,5d]]), " +
                        "(10_000_000, '''', array[[10d,20d],[30d,40d]], array[[20d,30d],[40d,50d]]);",
                """
                        Time\tSpread\tBid_Volume\task_volume
                        1970-01-01T00:00:10.000000Z\t10.0\t300.0\t800.0
                        """,
                true,
                true,
                false,
                """
                        Long Top K lo: 6
                          keys: [Time desc]
                            Async JIT Group By workers: 1
                              keys: [Time]
                              keyFunctions: [timestamp_floor_utc('1s',timestamp)]
                              values: [avg(asks[1,1]-bids[1,1]),sum(bids[1,1]*bids[2,1]),sum(asks[1,1]*asks[2,1])]
                              filter: symbol='''
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: market_data
                        """);
    }

    @Test
    public void testIndexBindVariableReplacedContext() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " x l," +
                    " timestamp_sequence(400000000000, 500000000) ts" +
                    " from long_sequence(100)) timestamp(ts)");

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            indexBindVariableReplacedContext(false);

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            indexBindVariableReplacedContext(true);
        });
    }

    @Test
    public void testMixedSelectPreTouchEnabled() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table t1 as (select " +
                            " x," +
                            " timestamp_sequence(to_timestamp('1970-01-01', 'yyyy-MM-dd'), 100000L) ts " +
                            "from long_sequence(10)) timestamp(ts) partition by day"
            );

            final String query = "select /*+ ENABLE_PRE_TOUCH(t1) */ 1 as one, (4 + 2) as the_answer, ts as col_ts, x as col_x, sqrt(x) as root_x from t1 where x > 1";
            final String expected = """
                    one\tthe_answer\tcol_ts\tcol_x\troot_x
                    1\t6\t1970-01-01T00:00:00.100000Z\t2\t1.4142135623730951
                    1\t6\t1970-01-01T00:00:00.200000Z\t3\t1.7320508075688772
                    1\t6\t1970-01-01T00:00:00.300000Z\t4\t2.0
                    1\t6\t1970-01-01T00:00:00.400000Z\t5\t2.23606797749979
                    1\t6\t1970-01-01T00:00:00.500000Z\t6\t2.449489742783178
                    1\t6\t1970-01-01T00:00:00.600000Z\t7\t2.6457513110645907
                    1\t6\t1970-01-01T00:00:00.700000Z\t8\t2.8284271247461903
                    1\t6\t1970-01-01T00:00:00.800000Z\t9\t3.0
                    1\t6\t1970-01-01T00:00:00.900000Z\t10\t3.1622776601683795
                    """;

            assertSql(expected, query);
            assertSqlRunWithJit(query);
        });
    }

    @Test
    public void testMultiplePartitionsOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select " +
                    " x," +
                    " timestamp_sequence(to_timestamp('1970-01-01', 'yyyy-MM-dd'), 100000L) ts " +
                    "from long_sequence(1000)) timestamp(ts) partition by day");

            execute("insert into t1 select " +
                    " x," +
                    " timestamp_sequence(to_timestamp('1970-01-02', 'yyyy-MM-dd'), 100000L) ts " +
                    "from long_sequence(1000)");

            final String query = "select * from t1 where x < 3 order by ts desc";
            final String expected = """
                    x\tts
                    2\t1970-01-02T00:00:00.100000Z
                    1\t1970-01-02T00:00:00.000000Z
                    2\t1970-01-01T00:00:00.100000Z
                    1\t1970-01-01T00:00:00.000000Z
                    """;

            assertSql(expected, query);
            assertSqlRunWithJit(query);
        });
    }

    @Test
    public void testNameBindVariableReplacedContext() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " x l," +
                    " timestamp_sequence(400000000000, 500000000) ts" +
                    " from long_sequence(100)) timestamp(ts)");

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            namedBindVariableReplacedContext(false);

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            namedBindVariableReplacedContext(true);
        });
    }

    @Test
    public void testPageFrameMaxSize() throws Exception {
        int pageFrameMaxRows = 128;
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, pageFrameMaxRows);
        final long N = 8 * pageFrameMaxRows + 1;
        assertMemoryLeak(() -> {
            execute("create table t1 as (select " +
                    " x," +
                    " timestamp_sequence(to_timestamp('1970-01-01', 'yyyy-MM-dd'), 100000L) ts " +
                    "from long_sequence(" + N + ")) timestamp(ts) partition by day");

            final String query = "select * from t1 where x < 3";
            final String expected = """
                    x\tts
                    1\t1970-01-01T00:00:00.000000Z
                    2\t1970-01-01T00:00:00.100000Z
                    """;

            assertSql(expected, query);
            assertSqlRunWithJit(query);
        });
    }

    @Test
    public void testRandomAccessAfterToTop() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " x l," +
                    " timestamp_sequence(400000000000, 500000000) ts" +
                    " from long_sequence(5)) timestamp(ts)");

            final String query = "select * from x where l > 3";
            final String expected = """
                    l\tts
                    4\t1970-01-05T15:31:40.000000Z
                    5\t1970-01-05T15:40:00.000000Z
                    """;

            assertSql(expected, query);
            assertSqlRunWithJit(query);

            try (RecordCursorFactory factory = select(query)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    // 1. iteration
                    Assert.assertTrue(cursor.hasNext());
                    final long rowid = record.getRowId();
                    Assert.assertTrue(cursor.hasNext());
                    long l = record.getLong(factory.getMetadata().getColumnIndex("l"));
                    Assert.assertEquals(5, l);

                    // 2. reset iteration
                    cursor.toTop();

                    // 3. random access
                    cursor.recordAt(record, rowid);
                    l = record.getLong(factory.getMetadata().getColumnIndex("l"));
                    Assert.assertEquals(4, l);

                    // 4. iteration restarts
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(cursor.hasNext());
                    l = record.getLong(factory.getMetadata().getColumnIndex("l"));
                    Assert.assertEquals(5, l);
                }
            }
        });
    }

    @Test
    public void testRandomAccessWithColTops() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " x l," +
                    " timestamp_sequence(400000000000, 500000000) ts" +
                    " from long_sequence(5)) timestamp(ts)");

            execute("alter table x add column j long", sqlExecutionContext);

            execute("insert into x select " +
                    " (x+5) l," +
                    " timestamp_sequence(500000000000, 500000000) ts," +
                    " rnd_long() j " +
                    "from long_sequence(5)");

            final String query = "select * from x where l > 3 and j = null";
            final String expected = """
                    l\tts\tj
                    4\t1970-01-05T15:31:40.000000Z\tnull
                    5\t1970-01-05T15:40:00.000000Z\tnull
                    """;

            assertSql(expected, query);
            assertSqlRunWithJit(query);

            try (RecordCursorFactory factory = select(query)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record record = cursor.getRecord();
                    // 1. iteration
                    Assert.assertTrue(cursor.hasNext());
                    final long rowid = record.getRowId();
                    Assert.assertTrue(cursor.hasNext());
                    long l = record.getLong(factory.getMetadata().getColumnIndex("l"));
                    Assert.assertEquals(5, l);

                    // 2. random access
                    cursor.recordAt(record, rowid);
                    l = record.getLong(factory.getMetadata().getColumnIndex("l"));
                    Assert.assertEquals(4, l);

                    // 3. continue iteration
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testSelectAllBothPageFramesFilterWithColTopsPreTouchEnabled() throws Exception {
        testSelectAllBothPageFramesFilterWithColTops(SqlJitMode.JIT_MODE_ENABLED, true);
    }

    @Test
    public void testSelectAllBothPageFramesFilterWithColTopsScalar() throws Exception {
        testSelectAllBothPageFramesFilterWithColTops(SqlJitMode.JIT_MODE_FORCE_SCALAR, false);
    }

    @Test
    public void testSelectAllBothPageFramesFilterWithColTopsVectorized() throws Exception {
        testSelectAllBothPageFramesFilterWithColTops(SqlJitMode.JIT_MODE_ENABLED, false);
    }

    @Test
    public void testSelectAllFilterWithColTopsPreTouchEnabled() throws Exception {
        testSelectAllFilterWithColTops(SqlJitMode.JIT_MODE_ENABLED, true);
    }

    @Test
    public void testSelectAllFilterWithColTopsScalar() throws Exception {
        testSelectAllFilterWithColTops(SqlJitMode.JIT_MODE_FORCE_SCALAR, false);
    }

    @Test
    public void testSelectAllFilterWithColTopsVectorized() throws Exception {
        testSelectAllFilterWithColTops(SqlJitMode.JIT_MODE_ENABLED, false);
    }

    @Test
    public void testSelectAllTypesFromRecordPreTouchDisabled() throws Exception {
        testSelectAllTypesFromRecord(false);
    }

    @Test
    public void testSelectAllTypesFromRecordPreTouchEnabled() throws Exception {
        testSelectAllTypesFromRecord(true);
    }

    @Test
    public void testSelectSingleColumnFilterWithColTopsPreTouchEnabled() throws Exception {
        testSelectSingleColumnFilterWithColTops(SqlJitMode.JIT_MODE_ENABLED, true);
    }

    @Test
    public void testSelectSingleColumnFilterWithColTopsScalar() throws Exception {
        testSelectSingleColumnFilterWithColTops(SqlJitMode.JIT_MODE_FORCE_SCALAR, false);
    }

    @Test
    public void testSelectSingleColumnFilterWithColTopsVectorized() throws Exception {
        testSelectSingleColumnFilterWithColTops(SqlJitMode.JIT_MODE_ENABLED, false);
    }

    @Test
    public void testSingleBindVariableScalar() throws Exception {
        testSingleBindVariable(SqlJitMode.JIT_MODE_FORCE_SCALAR);
    }

    @Test
    public void testSingleBindVariableVectorized() throws Exception {
        testSingleBindVariable(SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testSymbolBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " rnd_symbol('A','B','C') sym," +
                    " timestamp_sequence(400000000000, 500000000) ts" +
                    " from long_sequence(5)) timestamp(ts)");

            bindVariableService.clear();
            bindVariableService.setStr("sym", "B");

            // The column order is important here, since we want
            // query and table column indexes to be different.
            final String query = "select ts, sym from x where sym = :sym";

            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue("JIT was not enabled for query: " + query, factory.usesCompiledFilter());

                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    println(factory, cursor);
                }
                TestUtils.assertEquals("""
                        ts\tsym
                        1970-01-05T15:23:20.000000Z\tB
                        """, sink);

                BindVariableServiceImpl bindService2 = new BindVariableServiceImpl(configuration);
                bindService2.setStr("sym", "C");
                try (
                        SqlExecutionContext context2 = TestUtils.createSqlExecutionCtx(engine, bindService2);
                        RecordCursor cursor = factory.getCursor(context2)
                ) {
                    println(factory, cursor);
                }
                TestUtils.assertEquals("""
                        ts\tsym
                        1970-01-05T15:31:40.000000Z\tC
                        1970-01-05T15:40:00.000000Z\tC
                        """, sink);
            }
        });
    }

    @Test
    public void testSymbolComparison() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (s symbol)");
            execute("insert into test values ('C'), ('B'), ('A')");

            assertSql("s\nB\nA\n", "select s from test where s <  'C'");
            assertSql("s\nC\nB\nA\n", "select s from test where s <= 'C'");
            assertSql("s\n", "select s from test where s >  'C'");
            assertSql("s\nC\n", "select s from test where s >= 'C'");

            assertSql("s\nA\n", "select s from test where s <  'B'");
            assertSql("s\nB\nA\n", "select s from test where s <= 'B'");
            assertSql("s\nC\n", "select s from test where s >  'B'");
            assertSql("s\nC\nB\n", "select s from test where s >= 'B'");

            assertSql("s\n", "select s from test where s <  'A'");
            assertSql("s\nA\n", "select s from test where s <= 'A'");
            assertSql("s\nC\nB\n", "select s from test where s >  'A'");
            assertSql("s\nC\nB\nA\n", "select s from test where s >= 'A'");

            assertSql("s\nC\nB\nA\n", "select s from test where s <  'Z'");
            assertSql("s\nC\nB\nA\n", "select s from test where s <= 'Z'");
            assertSql("s\n", "select s from test where s >  'Z'");
            assertSql("s\n", "select s from test where s >= 'Z'");

            assertSql("s\n", "select s from test where s <  null");
        });
    }

    @Test
    public void testUuidBindFollowedByOtherBinds() throws Exception {
        // Reproduces the case where a UUID bind variable precedes other bind
        // variables in the same JIT-compiled filter. The bind-var memory layout
        // has a 16-byte UUID slot, but the JIT addresses every slot at idx*8,
        // so subsequent bind variables would be read from the wrong offset.
        assertMemoryLeak(() -> {
            execute("""
                    create table x (\
                        u UUID, sym SYMBOL, ts TIMESTAMP\
                    ) timestamp(ts) partition by day""");

            Uuid uuid = new Uuid();
            uuid.of("10bb226e-b424-4e36-83b9-1ec970b04e78");

            execute("insert into x values ('10bb226e-b424-4e36-83b9-1ec970b04e78', '1m', '2023-01-01T00:00:00.000Z')");

            bindVariableService.clear();
            bindVariableService.setUuid(0, uuid.getLo(), uuid.getHi());
            bindVariableService.setStr(1, "1m");
            bindVariableService.setTimestamp(2, 0L);
            bindVariableService.setTimestamp(3, Long.MAX_VALUE);

            final String query = "select u from x where u = $1 and sym = $2 and ts >= $3 and ts <= $4";
            final String expected = """
                    u
                    10bb226e-b424-4e36-83b9-1ec970b04e78
                    """;

            assertSql(expected, query);
            assertSqlRunWithJit(query);
        });
    }

    @Test
    public void testUuid() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    create table x as (select\
                     rnd_uuid4() u,\
                     timestamp_sequence(400000000000, 500000000) ts\
                     from long_sequence(100)) timestamp(ts) partition by day""");

            Uuid uuid = new Uuid();
            uuid.of("10bb226e-b424-4e36-83b9-1ec970b04e78");

            bindVariableService.clear();
            bindVariableService.setUuid(0, uuid.getLo(), uuid.getHi());

            final String query = "select * from x where u = $1";
            final String expected = """
                    u\tts
                    10bb226e-b424-4e36-83b9-1ec970b04e78\t1970-01-05T19:25:00.000000Z
                    """;

            assertSql(expected, query);
            assertSqlRunWithJit(query);

            // check JIT uses both hi and lo for comparison
            // use a dummy lo
            bindVariableService.clear();
            bindVariableService.setUuid(0, 0, uuid.getHi());
            String expectedEmpty = """
                    u	ts
                    """;
            assertSql(expectedEmpty, query);
            assertSqlRunWithJit(query);

            // use a dummy hi
            bindVariableService.clear();
            bindVariableService.setUuid(0, uuid.getLo(), 0);
            assertSql(expectedEmpty, query);
            assertSqlRunWithJit(query);

            // switch hi and lo
            bindVariableService.clear();
            bindVariableService.setUuid(0, uuid.getHi(), uuid.getLo());
            assertSql(expectedEmpty, query);
            assertSqlRunWithJit(query);

            // null uuid
            execute("insert into x values (null, '2020')");
            bindVariableService.clear();
            bindVariableService.setUuid(0, Numbers.LONG_NULL, Numbers.LONG_NULL);
            String expectedWithNull = """
                    u	ts
                    	2020-01-01T00:00:00.000000Z
                    """;
            assertSql(expectedWithNull, query);
            assertSqlRunWithJit(query);
        });
    }

    private void indexBindVariableReplacedContext(boolean jit) throws SqlException {
        bindVariableService.clear();
        bindVariableService.setInt(0, 1);
        bindVariableService.setInt(1, 1000);

        final String query = "select $2 as a, l from x where l = $1";

        try (RecordCursorFactory factory = select(query)) {
            if (jit) {
                Assert.assertTrue("JIT was not enabled for query: " + query, factory.usesCompiledFilter());
            }

            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                println(factory, cursor);
            }
            TestUtils.assertEquals("""
                    a\tl
                    1000\t1
                    """, sink);

            BindVariableServiceImpl bindService2 = new BindVariableServiceImpl(configuration);
            bindService2.setInt(0, 2);
            bindService2.setInt(1, 1002);
            try (
                    SqlExecutionContext context2 = TestUtils.createSqlExecutionCtx(engine, bindService2);
                    RecordCursor cursor = factory.getCursor(context2)
            ) {
                println(factory, cursor);
            }
            TestUtils.assertEquals("""
                    a\tl
                    1002\t2
                    """, sink);
        }
    }

    private void namedBindVariableReplacedContext(boolean jit) throws SqlException {

        bindVariableService.clear();
        bindVariableService.setInt("v1", 1);
        bindVariableService.setInt("v2", 1000);

        final String query = "select :v2 as a, l from x where l = :v1";

        try (RecordCursorFactory factory = select(query)) {
            if (jit) {
                Assert.assertTrue("JIT was not enabled for query: " + query, factory.usesCompiledFilter());
            }

            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                println(factory, cursor);
            }
            TestUtils.assertEquals("""
                    a\tl
                    1000\t1
                    """, sink);

            BindVariableServiceImpl bindService2 = new BindVariableServiceImpl(configuration);
            bindService2.setInt("v1", 2);
            bindService2.setInt("v2", 1002);

            try (
                    SqlExecutionContext context2 = TestUtils.createSqlExecutionCtx(engine, bindService2);
                    RecordCursor cursor = factory.getCursor(context2)
            ) {
                println(factory, cursor);
            }
            TestUtils.assertEquals("""
                    a\tl
                    1002\t2
                    """, sink);
        }
    }

    private void testBindVariableNullCheck(int jitMode) throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setJitMode(jitMode);
            final long value = 42;
            execute("create table x as (select" +
                    " " + value + " l," +
                    " to_timestamp('1971', 'yyyy') ts" +
                    " from long_sequence(1)) timestamp(ts)");

            bindVariableService.clear();
            bindVariableService.setLong("l", Numbers.LONG_NULL);

            // Here we expect a NULL value on the left side of the predicate,
            // so no rows should be returned
            final String query = "select * from x where l + :l = " + (Numbers.LONG_NULL + value);
            final String expected = "l\tts\n";

            assertSql(expected, query);
            assertSqlRunWithJit(query);
        });
    }

    private void testFilterWithColTops(String query, String expected, int jitMode) throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setJitMode(jitMode);

            execute(
                    "create table t1 as (select " +
                            " x," +
                            " timestamp_sequence(0, 1000000) ts " +
                            "from long_sequence(20)) timestamp(ts)"
            );

            execute("alter table t1 add column j long");

            execute(
                    "insert into t1 select " +
                            " x," +
                            " timestamp_sequence(100000000, 1000000) ts," +
                            " rnd_long() j " +
                            "from long_sequence(20)"
            );

            assertSql(expected, query);
            assertSqlRunWithJit(query);
        });
    }

    private void testSelectAllBothPageFramesFilterWithColTops(int jitMode, boolean preTouch) throws Exception {
        final String query = "select " + (preTouch ? "/*+ ENABLE_PRE_TOUCH(t1) */" : "") + " * from t1 where x >= 3 and x <= 4";
        final String expected = """
                x\tts\tj
                3\t1970-01-01T00:00:02.000000Z\tnull
                4\t1970-01-01T00:00:03.000000Z\tnull
                3\t1970-01-01T00:01:42.000000Z\t7746536061816329025
                4\t1970-01-01T00:01:43.000000Z\t-6945921502384501475
                """;

        testFilterWithColTops(query, expected, jitMode);
    }

    private void testSelectAllFilterWithColTops(int jitMode, boolean preTouch) throws Exception {
        final String query = "select " + (preTouch ? "/*+ ENABLE_PRE_TOUCH(t1) */" : "") + " * from t1 where j < 0";
        final String expected = """
                x\tts\tj
                4\t1970-01-01T00:01:43.000000Z\t-6945921502384501475
                7\t1970-01-01T00:01:46.000000Z\t-7611843578141082998
                8\t1970-01-01T00:01:47.000000Z\t-5354193255228091881
                9\t1970-01-01T00:01:48.000000Z\t-2653407051020864006
                10\t1970-01-01T00:01:49.000000Z\t-1675638984090602536
                14\t1970-01-01T00:01:53.000000Z\t-7489826605295361807
                15\t1970-01-01T00:01:54.000000Z\t-4094902006239100839
                16\t1970-01-01T00:01:55.000000Z\t-4474835130332302712
                17\t1970-01-01T00:01:56.000000Z\t-6943924477733600060
                """;

        testFilterWithColTops(query, expected, jitMode);
    }

    private void testSelectAllTypesFromRecord(boolean preTouch) throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select " + (preTouch ? "/*+ ENABLE_PRE_TOUCH(x) */" : "") + " * from x where b = true and kk < 10";
            final String expected = """
                    kk\ta\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tcc\tl2\thash1b\thash2b\thash3b\thash1c\thash2c\thash4c\thash8c
                    2\t1637847416\ttrue\tV\t0.4900510449885239\t0.8258367\t553\t2015-12-28T22:25:40.934Z\t\t-7611030538224290496\t1970-01-05T15:15:00.000000Z\t37\t00000000 3e e3 f1 f1 1e ca 9c 1d 06 ac\tKGHVUVSDOTSED\tY\t0x772c8b7f9505620ebbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea61\t0\t10\t110\te\tsj\tfhcq\t35jvygt2
                    3\t844704299\ttrue\t\t0.3456897991538844\t0.24008358\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-05T15:23:20.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a
                    00000010 e7 0c 89\tLJUMLGLHMLLEO\tY\t0xabbcbeeddca3d4fe4f25a88863fc0f467f24de22c77acf93e983e65f5551d073\t0\t01\t000\tf\t33\teusj\tb5z6npxr
                    6\t-1501720177\ttrue\tP\t0.18158967304439033\t0.8196554\t501\t2015-06-08T17:20:46.703Z\tPEHN\t-4229502740666959541\t1970-01-05T15:48:20.000000Z\t19\t\tTNLEGP\tU\t0x79423d4d320d2649767a4feda060d4fb6923c0c7d965969da1b1140a2be25241\t1\t01\t010\tr\tc0\twhjh\trcqfw2hw
                    8\t526232578\ttrue\tE\t0.6379992093447574\t0.85148495\t850\t2015-08-19T05:52:05.329Z\tPEHN\t-5157086556591926155\t1970-01-05T16:05:00.000000Z\t42\t00000000 6d 8c d8 ac c8 46 3b 47 3c e1 72 3b 9d\tJSMKIXEYVTUPD\tH\t0x2337f7e6b82ebc2405c5c1b231cffa455a6e970fb8b80abcc4129ae493cc6076\t0\t11\t000\t5\ttp\tx578\ttdnxkw6d
                    """;
            final String ddl = "create table x as (select" +
                    " cast(x as int) kk," +
                    " rnd_int() a," +
                    " rnd_boolean() b," +
                    " rnd_str(1,1,2) c," +
                    " rnd_double(2) d," +
                    " rnd_float(2) e," +
                    " rnd_short(10,1024) f," +
                    " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                    " rnd_symbol(4,4,4,2) i," +
                    " rnd_long() j," +
                    " timestamp_sequence(400000000000, 500000000) k," +
                    " rnd_byte(2,50) l," +
                    " rnd_bin(10, 20, 2) m," +
                    " rnd_str(5,16,2) n," +
                    " rnd_char() cc," +
                    " rnd_long256() l2," +
                    " rnd_geohash(1) hash1b," +
                    " rnd_geohash(2) hash2b," +
                    " rnd_geohash(3) hash3b," +
                    " rnd_geohash(5) hash1c," +
                    " rnd_geohash(10) hash2c," +
                    " rnd_geohash(20) hash4c," +
                    " rnd_geohash(40) hash8c" +
                    " from long_sequence(100)) timestamp(k)";

            assertQueryNoLeakCheck(
                    expected,
                    query,
                    ddl,
                    "k",
                    true
            );
            assertSqlRunWithJit(query);
        });
    }

    private void testSelectSingleColumnFilterWithColTops(int jitMode, boolean preTouch) throws Exception {
        // The column order is important here, since we want
        // query and table column indexes to be different.
        final String query = "select " + (preTouch ? "/*+ ENABLE_PRE_TOUCH(t1) */" : "") + " j from t1 where j <> null and x < 3";
        final String expected = """
                j
                4689592037643856
                4729996258992366
                """;

        testFilterWithColTops(query, expected, jitMode);
    }

    private void testSingleBindVariable(int jitMode) throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setJitMode(jitMode);

            execute("create table x as (select" +
                    " rnd_long() l," +
                    " timestamp_sequence(400000000000, 500000000) ts" +
                    " from long_sequence(100)) timestamp(ts)");

            bindVariableService.clear();
            bindVariableService.setLong("l", 3614738589890112276L);

            final String query = "select * from x where l = :l";
            final String expected = """
                    l\tts
                    3614738589890112276\t1970-01-05T16:38:20.000000Z
                    """;

            assertSql(expected, query);
            assertSqlRunWithJit(query);
        });
    }
}
