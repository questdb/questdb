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

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TickCalendarServiceFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.join.AsyncWindowJoinFastRecordCursorFactory;
import io.questdb.jit.JitUtil;
import io.questdb.std.Numbers;
import io.questdb.std.Uuid;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.cairo.TestTickCalendarService;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.function.UnaryOperator;

/**
 * Tests for advanced features and scenarios, such as col tops, bind variables,
 * random access, record behavior, and so on.
 */
public class CompiledFilterTest extends AbstractCairoTest {
    // Three days in two intervals, since $yesterday and $today merge. The JIT compiles three range
    // checks for it and pads the third. Over createTemporalDaysTable(), it holds k = 7, 8 and 10 on
    // 2024-01-08, and k = 4, 5 and 7 on 2024-01-05.
    private static final String DAYS_AROUND_TODAY = "'[$yesterday, $today, $today + 2d]'";
    private static final String DAYS_AROUND_TODAY_GROUPS_5 = """
            s\tc\tsk
            a\t2\t12
            b\t1\t4
            """;
    private static final String DAYS_AROUND_TODAY_GROUPS_8 = """
            s\tc\tsk
            a\t1\t7
            b\t2\t18
            """;

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

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("atimestamp")
                    .returns(expected);
            assertSqlRunWithJit(query);
        });
    }

    @Test
    public void testBindVariableCountAroundVectorCacheCapacity() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x v, x::TIMESTAMP ts FROM long_sequence(103)) TIMESTAMP(ts) PARTITION BY DAY");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            for (int bindCount : new int[]{8, 9, 12}) {
                bindVariableService.clear();
                final StringBuilder where = new StringBuilder();
                for (int i = 1; i <= bindCount; i++) {
                    bindVariableService.setLong("b" + i, i);
                    if (i > 1) {
                        where.append(" AND ");
                    }
                    where.append("v <> :b").append(i);
                }
                final StringBuilder expected = new StringBuilder("v\n");
                for (int i = bindCount + 1; i <= 103; i++) {
                    expected.append(i).append('\n');
                }
                final String query = "SELECT v FROM t WHERE " + where;
                assertQuery(query).noLeakCheck().returns(expected.toString());
                assertQuery("SELECT count() FROM t WHERE " + where)
                        .noLeakCheck().noRandomAccess().expectSize().returns("count\n" + (103 - bindCount) + "\n");
                assertSqlRunWithJit(query);
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
    public void testByteIntArithmeticWidenedToLongInLongContext() throws Exception {
        assertMemoryLeak(() -> {
            // BYTE * INT can overflow int32 (e.g., 127 * 20_000_000), so the same
            // SX_I64 widening must apply to non-INT narrow operands as well when
            // the predicate has integer arithmetic and a LONG operand.
            execute("CREATE TABLE x (b BYTE, i INT, l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(127, 20000000, 0, '2024-01-01T00:00:00.000000Z')," +
                    " (-128, 20000000, 0, '2024-01-01T00:00:00.000001Z')," +
                    " (10, 5, 0, '2024-01-01T00:00:00.000002Z')");

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            // 127 * 20_000_000 = 2_540_000_000L (positive long, overflows int32);
            // -128 * 20_000_000 negative; 10*5=50 positive -> two rows match.
            assertQuery("SELECT count(*) FROM x WHERE (b * i) > l")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n2\n");

            try (RecordCursorFactory factory = select("SELECT count(*) FROM x WHERE (b * i) > l")) {
                Assert.assertTrue("BYTE*INT in LONG context must still JIT",
                        factory.usesCompiledFilter());
            }
        });
    }

    @Test
    public void testDateInBeforeEpochMatchesJava() throws Exception {
        // The JIT compares a DATE key in milliseconds and rounds the bounds of a micro interval
        // inward, as the Java filter matches a DATE d when d * 1000 lies in the interval. Before
        // 1970 the bounds are negative, where truncation rounds toward zero rather than inward:
        // the upper bound of 1969-12-31, -1 us, would include 1970-01-01T00:00:00.000 (k = 5).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k INT, dt DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '1969-12-30T23:59:59.999Z', '2024-01-01T00:00:00.000000Z'),
                    (2, '1969-12-31T00:00:00.000Z', '2024-01-01T01:00:00.000000Z'),
                    (3, '1969-12-31T23:59:59.998Z', '2024-01-01T02:00:00.000000Z'),
                    (4, '1969-12-31T23:59:59.999Z', '2024-01-01T03:00:00.000000Z'),
                    (5, '1970-01-01T00:00:00.000Z', '2024-01-01T04:00:00.000000Z'),
                    (6, NULL, '2024-01-01T05:00:00.000000Z')
                    """);
            assertFilterWithAndWithoutJit("dt IN '1969-12-31'", "k\n2\n3\n4\n", true);
            assertFilterWithAndWithoutJit("dt IN ('1969-12-31')", "k\n2\n3\n4\n", true);
            assertFilterWithAndWithoutJit("dt NOT IN '1969-12-31'", "k\n1\n5\n6\n", true);
            assertFilterWithAndWithoutJit("dt IN '1969-12-31' OR k = 1", "k\n1\n2\n3\n4\n", true);
            assertFilterWithAndWithoutJit("dt IN '1969-12-31T23:59:59.998'", "k\n3\n", true);
            // [-1500 us, -501 us] holds -1 ms only, and [-500 us, 499 us] holds 0 ms only.
            assertFilterWithAndWithoutJit("dt IN '1969-12-31T23:59:59.998500;1T'", "k\n4\n", true);
            assertFilterWithAndWithoutJit("dt IN '1969-12-31T23:59:59.999500;1T'", "k\n5\n", true);

            // CompiledFilterTickBounds rounds the bounds of a date variable the same way.
            final long dec31At10 = -Micros.DAY_MICROS + 10 * Micros.HOUR_MICROS;
            final long jan1At10 = 10 * Micros.HOUR_MICROS;
            assertFilterFollowsClock("dt IN '$today'", true, dec31At10, "k\n2\n3\n4\n", jan1At10, "k\n5\n");
            assertFilterFollowsClock("dt NOT IN '$today'", true, dec31At10, "k\n1\n5\n6\n", jan1At10, "k\n1\n2\n3\n4\n6\n");
            assertFilterFollowsClock("dt IN '$now - 500u;1T'", true, -Micros.MILLI_MICROS, "k\n4\n", 0, "k\n5\n");
        });
    }

    @Test
    public void testDateInIntervalCountThreshold() throws Exception {
        // An unparenthesised IN over a DATE key used to run on the Java filter, whose binary search
        // outpaces the JIT's range checks past a few dozen intervals. The IN list threshold (10 by
        // default) caps its interval count, as it caps the parenthesised form's. A TIMESTAMP or
        // TIMESTAMP_NS key has always compiled any interval count in this form.
        assertMemoryLeak(() -> {
            createTemporalInTable();
            for (String c : new String[]{"tm", "tn"}) {
                assertFilterWithAndWithoutJit(c + " IN '2024-01-02T12:00;1m;-1d;11'", "k\n1\n", true);
                assertFilterWithAndWithoutJit(c + " IN '2024-01-02T12:00;1m;-1d;40' OR k = 3", "k\n1\n3\n", true);
            }
            assertFilterWithAndWithoutJit("dt IN '2024-01-02'", "k\n1\n2\n", true);
            assertFilterWithAndWithoutJit("dt IN '2024-01-02T12:00;1m;-1d;10'", "k\n1\n", true);
            assertFilterWithAndWithoutJit("dt IN '2024-01-02T12:00;1m;-1d;11'", "k\n1\n", false);
            assertFilterWithAndWithoutJit("dt NOT IN '2024-01-02T12:00;1m;-1d;11'", "k\n2\n3\n4\n5\n", false);
            assertFilterWithAndWithoutJit("dt IN '2024-01-02T12:00;1m;-1d;40' OR k = 3", "k\n1\n3\n", false);
        });
    }

    @Test
    public void testDateVariableIntervalAboveInListThresholdFollowsClock() throws Exception {
        // Eleven intervals a day, one more than cairo.sql.jit.max.in.list.size.threshold allows by
        // default, so the Java filter runs it.
        final String times = "[$today]T[02:00,03:00,04:00,05:00,06:00,07:00,08:00,09:00,10:00,11:00,12:00]";
        assertMemoryLeak(() -> {
            createTemporalDaysTable();
            for (String c : new String[]{"dt", "tm", "tn"}) {
                assertFilterFollowsClock(c + " IN '" + times + "'", false, jan2024(5), keys(5), jan2024(6), keys(6));
                assertFilterFollowsClock(c + " IN ('" + times + "')", false, jan2024(5), keys(5), jan2024(6), keys(6));
                assertFilterFollowsClock(c + " NOT IN '" + times + "'", false, jan2024(5), allDayKeysExcept(5), jan2024(6), allDayKeysExcept(6));
            }
            assertFollowsClock("SELECT k FROM x WHERE ts IN '" + times + "'", false, jan2024(5), keys(5), jan2024(6), keys(6));
            assertFollowsClock("SELECT k FROM x WHERE ts IN '" + times + "' OR k = 1", false, jan2024(5), keys(1, 5), jan2024(6), keys(1, 6));
        });
    }

    @Test
    public void testDateVariableIntervalCountChangeFollowsClock() throws Exception {
        // The interval count of these expressions moves with the clock: $today#workday has no
        // interval at weekends, $today..$today+3bd splits at a weekend, and [$yesterday, 2024-01-02]
        // merges into one interval when $yesterday reaches 2024-01-02 or the day after it. The JIT
        // compiles a range check for each interval an expression can yield and pads the unused
        // ones. A business-day range has no such bound, and runs on the Java filter.
        assertMemoryLeak(() -> {
            createTemporalDaysTable();
            for (String c : new String[]{"dt", "tm", "tn"}) {
                // 2024-01-05 is a Friday, 2024-01-06 a Saturday and 2024-01-08 a Monday.
                assertFilterFollowsClock(c + " IN '$today#workday'", true, jan2024(5), keys(5), jan2024(6), keys());
                assertFilterFollowsClock(c + " IN '$today#workday'", true, jan2024(6), keys(), jan2024(8), keys(8));
                assertFilterFollowsClock(c + " IN ('$today#workday')", true, jan2024(5), keys(5), jan2024(6), keys());
                assertFilterFollowsClock(c + " NOT IN '$today#workday'", true, jan2024(5), allDayKeysExcept(5), jan2024(6), allDayKeysExcept());
                assertFilterFollowsClock("NOT " + c + " IN '$today#workday'", true, jan2024(6), allDayKeysExcept(), jan2024(5), allDayKeysExcept(5));
                assertFilterFollowsClock(c + " IN '$today#workday' OR k = 2", true, jan2024(6), keys(2), jan2024(5), keys(2, 5));
                assertFilterFollowsClock(c + " IN '$today#workday' AND k > 4", true, jan2024(6), keys(), jan2024(5), keys(5));
                // From a Monday, 3 business days stay in one week. From a Friday, they cross a weekend.
                assertFilterFollowsClock(c + " IN '$today..$today+3bd'", false, jan2024(8), keys(8, 9, 10, 11), jan2024(5), keys(5, 8, 9, 10));
                assertFilterFollowsClock(c + " IN ('$today..$today+3bd')", false, jan2024(8), keys(8, 9, 10, 11), jan2024(5), keys(5, 8, 9, 10));
                assertFilterFollowsClock(c + " NOT IN '$today..$today+3bd'", false, jan2024(8), allDayKeysExcept(8, 9, 10, 11), jan2024(5), allDayKeysExcept(5, 8, 9, 10));
                assertFilterFollowsClock(c + " IN '[$yesterday, 2024-01-02]'", true, jan2024(3), keys(2), jan2024(5), keys(2, 4));
                assertFilterFollowsClock(c + " IN '[$yesterday, 2024-01-02]'", true, jan2024(5), keys(2, 4), jan2024(4), keys(2, 3));
                assertFilterFollowsClock(c + " NOT IN '[$yesterday, 2024-01-02]'", true, jan2024(3), allDayKeysExcept(2), jan2024(5), allDayKeysExcept(2, 4));
            }
        });
    }

    @Test
    public void testDateVariableIntervalCountChangeOnDesignatedTimestampFollowsClock() throws Exception {
        assertMemoryLeak(() -> {
            createTemporalDaysTable();
            // Interval scan. It leaves only k > 4 to the filter, which the JIT compiles.
            assertFollowsClock("SELECT k FROM x WHERE ts IN '$today#workday'", false, jan2024(5), keys(5), jan2024(6), keys());
            assertFollowsClock("SELECT k FROM x WHERE ts NOT IN '$today#workday'", false, jan2024(5), allDayKeysExcept(5), jan2024(6), allDayKeysExcept());
            assertFollowsClock("SELECT k FROM x WHERE ts IN '$today#workday' AND k > 4", true, jan2024(6), keys(), jan2024(5), keys(5));
            assertFollowsClock("SELECT k FROM x WHERE ts IN '$today..$today+3bd'", false, jan2024(8), keys(8, 9, 10, 11), jan2024(5), keys(5, 8, 9, 10));
            assertFollowsClock("SELECT k FROM x WHERE ts NOT IN '$today..$today+3bd'", false, jan2024(8), allDayKeysExcept(8, 9, 10, 11), jan2024(5), allDayKeysExcept(5, 8, 9, 10));
            assertFollowsClock("SELECT k FROM x WHERE ts IN '[$yesterday, 2024-01-02]'", false, jan2024(3), keys(2), jan2024(5), keys(2, 4));
            // Filter.
            assertFollowsClock("SELECT k FROM x WHERE ts IN '$today#workday' OR k = 2", true, jan2024(6), keys(2), jan2024(5), keys(2, 5));
            assertFollowsClock("SELECT k FROM x WHERE NOT ts IN '$today#workday' OR k = 2", true, jan2024(6), allDayKeysExcept(), jan2024(5), allDayKeysExcept(5));
            assertFollowsClock("SELECT k FROM x WHERE ts IN '$today..$today+3bd' OR k = 2", false, jan2024(8), keys(2, 8, 9, 10, 11), jan2024(5), keys(2, 5, 8, 9, 10));
            assertFollowsClock("SELECT k FROM x WHERE ts NOT IN '$today..$today+3bd' OR k = 9", false, jan2024(8), allDayKeysExcept(8, 10, 11), jan2024(5), allDayKeysExcept(5, 8, 10));
            assertFollowsClock("SELECT k FROM x WHERE ts IN '[$yesterday, 2024-01-02]' OR k = 1", true, jan2024(3), keys(1, 2), jan2024(5), keys(1, 2, 4));
        });
    }

    @Test
    public void testDateVariableIntervalInFilterFollowsClock() throws Exception {
        // The JIT used to resolve date variables such as $today when it compiled the filter, so a
        // cached plan kept the bounds of the moment it was compiled. The JIT now reads the bounds
        // from bind variable slots, which the factory fills each time the cursor opens.
        assertMemoryLeak(() -> {
            createTemporalInTable();
            final long jan2 = Micros.toMicros(2024, 1, 2);
            final long hour = Micros.HOUR_MICROS;
            final long day = Micros.DAY_MICROS;
            for (String c : new String[]{"dt", "tm", "tn"}) {
                assertFilterFollowsClock(c + " IN '$today'", true, jan2 + hour, "k\n1\n2\n", jan2 + day + hour, "k\n3\n");
                assertFilterFollowsClock(c + " IN ('$today')", true, jan2 + hour, "k\n1\n2\n", jan2 + day + hour, "k\n3\n");
                assertFilterFollowsClock(c + " NOT IN '$today'", true, jan2 + hour, "k\n3\n4\n5\n", jan2 + day + hour, "k\n1\n2\n4\n5\n");
                assertFilterFollowsClock(c + " NOT IN ('$today')", true, jan2 + hour, "k\n3\n4\n5\n", jan2 + day + hour, "k\n1\n2\n4\n5\n");
                assertFilterFollowsClock(c + " IN '$yesterday'", true, jan2 + hour, "k\n5\n", jan2 + day + hour, "k\n1\n2\n");
                assertFilterFollowsClock(c + " IN '$tomorrow'", true, jan2 - day + hour, "k\n1\n2\n", jan2 + hour, "k\n3\n");
                // 2024-01-02 is a Tuesday and 2024-01-03 a Wednesday.
                assertFilterFollowsClock(c + " IN '[$today - 1bd]'", true, jan2 + day + hour, "k\n1\n2\n", jan2 + hour, "k\n5\n");
                assertFilterFollowsClock(c + " IN '[$yesterday, $today]'", true, jan2 + hour, "k\n1\n2\n5\n", jan2 + day + hour, "k\n1\n2\n3\n");
                // $now moves within the day.
                assertFilterFollowsClock(c + " IN '$now - 1h..$now'", true, jan2 + hour / 2, "k\n2\n5\n", jan2 + 12 * hour + hour / 2, "k\n1\n");
                assertFilterFollowsClock(c + " IN '$now - 1h..$now' AND k > 1", true, jan2 + hour / 2, "k\n2\n5\n", jan2 + 12 * hour + hour / 2, "k\n");
                assertFilterFollowsClock(c + " IN '$today' OR k = 5", true, jan2 + hour, "k\n1\n2\n5\n", jan2 + day + hour, "k\n3\n5\n");
                // A point in time, and a range of whole days.
                assertFilterFollowsClock(c + " IN '$now - 12h'", true, jan2 + day, "k\n1\n", jan2 + 13 * hour, "k\n");
                assertFilterFollowsClock(c + " IN '$today..$today+1d'", true, jan2 + hour, "k\n1\n2\n3\n", jan2 - day + hour, "k\n1\n2\n5\n");
                // Time overrides, one interval each.
                assertFilterFollowsClock(c + " IN '$todayT11:30;1h'", true, jan2 + hour, "k\n1\n", jan2 + day + hour, "k\n");
                assertFilterFollowsClock(c + " IN '[$today]T[00:00,12:00];1m'", true, jan2 + hour, "k\n1\n2\n", jan2 + day + hour, "k\n3\n");
                assertFilterFollowsClock(c + " NOT IN '[$today]T[00:00,12:00];1m'", true, jan2 + hour, "k\n3\n4\n5\n", jan2 + day + hour, "k\n1\n2\n4\n5\n");
                // Timezones. London keeps UTC in January.
                assertFilterFollowsClock(c + " IN '$today@+01:00'", true, jan2 + hour, "k\n1\n2\n5\n", jan2 + day + hour, "k\n3\n");
                assertFilterFollowsClock(c + " IN '$today@Europe/London'", true, jan2 + hour, "k\n1\n2\n", jan2 + day + hour, "k\n3\n");
                // A range of days in a named zone can split at a clock change, so its interval count
                // has no bound the JIT relies on.
                assertFilterFollowsClock(c + " IN '$today..$today+1d@Europe/London'", false, jan2 + hour, "k\n1\n2\n3\n", jan2 - day + hour, "k\n1\n2\n5\n");
            }
        });
    }

    @Test
    public void testDateVariableIntervalInGroupByFollowsClock() throws Exception {
        // AsyncFilterContext initializes the filter of GROUP BY and top K each time the cursor opens.
        assertMemoryLeak(() -> {
            createTemporalDaysTable();
            final String groupBy = "SELECT s, count() c, sum(k) sk FROM x WHERE tm IN ";
            final String topK = "SELECT k FROM x WHERE tm IN ";
            // The constant interval shows that the factory compiles its filter with the JIT.
            final String constant = "'[2024-01-05, 2024-01-08, 2024-01-09, 2024-01-10]'";
            final String expected5 = """
                    s\tc\tsk
                    a\t2\t14
                    b\t2\t18
                    """;
            final String expected8 = """
                    s\tc\tsk
                    a\t2\t20
                    b\t2\t18
                    """;
            assertFollowsClock(groupBy + constant + " ORDER BY s", true, QueryAssertion::expectSize, jan2024(8), expected5, jan2024(5), expected5);
            assertFollowsClock(groupBy + "'$today..$today+3bd' ORDER BY s", false, QueryAssertion::expectSize, jan2024(8), expected8, jan2024(5), expected5);
            assertFollowsClock(groupBy + DAYS_AROUND_TODAY + " ORDER BY s", true, QueryAssertion::expectSize, jan2024(8), DAYS_AROUND_TODAY_GROUPS_8, jan2024(5), DAYS_AROUND_TODAY_GROUPS_5);
            assertFollowsClock(topK + constant + " ORDER BY k DESC LIMIT 2", true, QueryAssertion::expectSize, jan2024(8), keys(10, 9), jan2024(5), keys(10, 9));
            assertFollowsClock(topK + "'$today..$today+3bd' ORDER BY k DESC LIMIT 2", false, QueryAssertion::expectSize, jan2024(8), keys(11, 10), jan2024(5), keys(10, 9));
            assertFollowsClock(topK + DAYS_AROUND_TODAY + " ORDER BY k DESC LIMIT 2", true, QueryAssertion::expectSize, jan2024(8), keys(10, 8), jan2024(5), keys(7, 5));
        });
    }

    @Test
    public void testDateVariableIntervalInHorizonJoinFollowsClock() throws Exception {
        assertMemoryLeak(() -> {
            createTemporalDaysTable();
            createDaysJoinSlaveTable();
            final String query = "SELECT t.s, count() c, sum(t.k) sk FROM x t HORIZON JOIN p ON (t.s = p.s) LIST (0s) AS h WHERE t.tm IN ";
            // The constant interval shows that the factory compiles its filter with the JIT.
            final String constant = "'[2024-01-05, 2024-01-08, 2024-01-09, 2024-01-10]'";
            final String expected5 = """
                    s\tc\tsk
                    a\t2\t14
                    b\t2\t18
                    """;
            final String expected8 = """
                    s\tc\tsk
                    a\t2\t20
                    b\t2\t18
                    """;
            assertFollowsClock(query + constant + " ORDER BY s", true, QueryAssertion::expectSize, jan2024(8), expected5, jan2024(5), expected5);
            assertFollowsClock(query + "'$today..$today+3bd' ORDER BY s", false, QueryAssertion::expectSize, jan2024(8), expected8, jan2024(5), expected5);
            assertFollowsClock(query + DAYS_AROUND_TODAY + " ORDER BY s", true, QueryAssertion::expectSize, jan2024(8), DAYS_AROUND_TODAY_GROUPS_8, jan2024(5), DAYS_AROUND_TODAY_GROUPS_5);
        });
    }

    @Test
    public void testDateVariableIntervalInLatestOnFollowsClock() throws Exception {
        // LatestByCompiledFilter initializes the filter each time the cursor opens.
        assertMemoryLeak(() -> {
            createTemporalDaysTable();
            final String query = "SELECT s, k FROM x WHERE tm IN ";
            // The constant interval shows that the factory compiles its filter with the JIT.
            final String constant = "'[2024-01-05, 2024-01-08, 2024-01-09, 2024-01-10]'";
            final String expected5 = """
                    s\tk
                    a\t9
                    b\t10
                    """;
            final String expected8 = """
                    s\tk
                    b\t10
                    a\t11
                    """;
            assertFollowsClock(query + constant + " LATEST ON ts PARTITION BY s", true, QueryAssertion::expectSize, jan2024(8), expected5, jan2024(5), expected5);
            assertFollowsClock(query + "'$today..$today+3bd' LATEST ON ts PARTITION BY s", false, QueryAssertion::expectSize, jan2024(8), expected8, jan2024(5), expected5);
            assertFollowsClock(
                    query + DAYS_AROUND_TODAY + " LATEST ON ts PARTITION BY s",
                    true,
                    QueryAssertion::expectSize,
                    jan2024(8),
                    """
                            s\tk
                            a\t7
                            b\t10
                            """,
                    jan2024(5),
                    """
                            s\tk
                            b\t4
                            a\t7
                            """
            );
        });
    }

    @Test
    public void testDateVariableIntervalInWindowJoinFollowsClock() throws Exception {
        // AsyncWindowJoinAtom initializes the filter each time the cursor opens. The factory does not
        // report whether it compiled the filter with the JIT through usesCompiledFilter(), so the
        // test asks its atom.
        assertMemoryLeak(() -> {
            createTemporalDaysTable();
            createDaysJoinSlaveTable();
            final String query = """
                    SELECT t.k, sum(p.v) sv
                    FROM x t
                    WINDOW JOIN p ON (t.s = p.s)
                    RANGE BETWEEN 1 minute PRECEDING AND 1 minute FOLLOWING INCLUDE PREVAILING
                    WHERE t.tm IN\s""";
            assertFollowsClock(
                    query + DAYS_AROUND_TODAY,
                    false,
                    QueryAssertion::noRandomAccess,
                    jan2024(8),
                    """
                            k\tsv
                            7\t1.0
                            8\t2.0
                            10\t2.0
                            """,
                    jan2024(5),
                    """
                            k\tsv
                            4\t2.0
                            5\t1.0
                            7\t1.0
                            """
            );
            assertWindowJoinUsesCompiledFilter(query + DAYS_AROUND_TODAY, true);
            assertFollowsClock(
                    query + "'$today..$today+3bd'",
                    false,
                    QueryAssertion::noRandomAccess,
                    jan2024(8),
                    """
                            k\tsv
                            8\t2.0
                            9\t1.0
                            10\t2.0
                            11\t1.0
                            """,
                    jan2024(5),
                    """
                            k\tsv
                            5\t1.0
                            8\t2.0
                            9\t1.0
                            10\t2.0
                            """
            );
            assertWindowJoinUsesCompiledFilter(query + "'$today..$today+3bd'", false);
        });
    }

    @Test
    public void testDateVariableIntervalOnDesignatedTimestampFollowsClock() throws Exception {
        // The interval scan takes an IN on the designated timestamp from an AND chain, and has
        // resolved date variables on every run since #6780. Under OR, the IN stays in the filter,
        // which the JIT used to compile with the bounds of the moment it was compiled.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE y (k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO y VALUES
                    (1, '2024-01-01T12:00:00.000000Z'),
                    (2, '2024-01-02T00:30:00.000000Z'),
                    (3, '2024-01-02T12:00:00.000000Z'),
                    (4, '2024-01-03T00:30:00.000000Z'),
                    (5, '2024-01-03T12:00:00.000000Z')
                    """);
            final long jan2 = Micros.toMicros(2024, 1, 2);
            final long hour = Micros.HOUR_MICROS;
            final long day = Micros.DAY_MICROS;
            final long t1 = jan2 + hour;
            final long t2 = jan2 + day + hour;
            // Interval scan. It leaves only k > 2 to the filter, which the JIT compiles.
            assertFollowsClock("SELECT k FROM y WHERE ts IN '$today'", false, t1, "k\n2\n3\n", t2, "k\n4\n5\n");
            assertFollowsClock("SELECT k FROM y WHERE ts IN ('$today')", false, t1, "k\n2\n3\n", t2, "k\n4\n5\n");
            assertFollowsClock("SELECT k FROM y WHERE ts NOT IN '$today'", false, t1, "k\n1\n4\n5\n", t2, "k\n1\n2\n3\n");
            assertFollowsClock("SELECT k FROM y WHERE ts IN '$yesterday'", false, t1, "k\n1\n", t2, "k\n2\n3\n");
            // A range end exactly at midnight counts as a whole day, so the $now ranges start off it.
            final long t3 = jan2 + 45 * Micros.MINUTE_MICROS;
            assertFollowsClock("SELECT k FROM y WHERE ts IN '$now - 1h..$now'", false, t3, "k\n2\n", jan2 + 12 * hour + hour / 2, "k\n3\n");
            assertFollowsClock("SELECT k FROM y WHERE ts IN '$today' AND k > 2", true, t1, "k\n3\n", t2, "k\n4\n5\n");
            // Filter.
            assertFollowsClock("SELECT k FROM y WHERE ts IN '$today' OR k = 1", true, t1, "k\n1\n2\n3\n", t2, "k\n1\n4\n5\n");
            assertFollowsClock("SELECT k FROM y WHERE ts IN ('$today') OR k = 1", true, t1, "k\n1\n2\n3\n", t2, "k\n1\n4\n5\n");
            assertFollowsClock("SELECT k FROM y WHERE ts NOT IN '$today' OR k = 3", true, t1, "k\n1\n3\n4\n5\n", t2, "k\n1\n2\n3\n");
            assertFollowsClock("SELECT k FROM y WHERE ts IN '$now - 1h..$now' OR k = 1", true, t3, "k\n1\n2\n", jan2 + 12 * hour + hour / 2, "k\n1\n3\n");
        });
    }

    @Test
    public void testDateVariableIntervalWithExchangeCalendarFollowsClock() throws Exception {
        // TestTickCalendarService holds the 2025 XNYS sessions: 14:30 to 21:00 UTC in January.
        final FactoryProvider previousFactoryProvider = factoryProvider;
        factoryProvider = new DefaultFactoryProvider() {
            @Override
            public @NotNull TickCalendarServiceFactory getTickCalendarServiceFactory() {
                return () -> TestTickCalendarService.INSTANCE;
            }
        };
        try {
            assertMemoryLeak(() -> {
                execute("CREATE TABLE x (k INT, dt DATE, tm TIMESTAMP, tn TIMESTAMP_NS, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
                execute("""
                        INSERT INTO x VALUES
                        (1, '2025-01-24T10:00:00.000Z', '2025-01-24T10:00:00.000000Z', '2025-01-24T10:00:00.000000000Z', '2025-01-24T10:00:00.000000Z'),
                        (2, '2025-01-24T15:00:00.000Z', '2025-01-24T15:00:00.000000Z', '2025-01-24T15:00:00.000000000Z', '2025-01-24T15:00:00.000000Z'),
                        (3, '2025-01-27T15:00:00.000Z', '2025-01-27T15:00:00.000000Z', '2025-01-27T15:00:00.000000000Z', '2025-01-27T15:00:00.000000Z'),
                        (4, NULL, NULL, NULL, '2025-01-27T22:00:00.000000Z')
                        """);
                // 2025-01-24 is a Friday, 2025-01-25 a Saturday and 2025-01-27 a Monday.
                final long fri = Micros.toMicros(2025, 1, 24, 1, 0);
                final long sat = Micros.toMicros(2025, 1, 25, 1, 0);
                final long mon = Micros.toMicros(2025, 1, 27, 1, 0);
                for (String c : new String[]{"dt", "tm", "tn"}) {
                    assertFilterFollowsClock(c + " IN '$today#XNYS'", false, fri, keys(2), mon, keys(3));
                    assertFilterFollowsClock(c + " IN ('$today#XNYS')", false, sat, keys(), mon, keys(3));
                    assertFilterFollowsClock(c + " NOT IN '$today#XNYS'", false, fri, keys(1, 3, 4), sat, keys(1, 2, 3, 4));
                    assertFilterFollowsClock(c + " IN '$today#XNYS' OR k = 1", false, sat, keys(1), fri, keys(1, 2));
                }
                assertFollowsClock("SELECT k FROM x WHERE ts IN '$today#XNYS'", false, fri, keys(2), mon, keys(3));
                assertFollowsClock("SELECT k FROM x WHERE ts IN '$today#XNYS' OR k = 1", false, sat, keys(1), fri, keys(1, 2));
            });
        } finally {
            factoryProvider = previousFactoryProvider;
        }
    }

    @Test
    public void testDateVariableIntervalWithManyElementsFollowsClock() throws Exception {
        // CompiledTickExpression used to keep the element count of a date variable expression in 8
        // bits, so the Java filter and the interval scan lost all but count % 256 of the elements.
        assertMemoryLeak(() -> {
            // One row a day at 10:00:30 through 2024.
            execute("CREATE TABLE x (k INT, dt DATE, tm TIMESTAMP, tn TIMESTAMP_NS, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT k, t::DATE, t, t::TIMESTAMP_NS, t
                    FROM (SELECT x::INT k, dateadd('d', (x - 1)::INT, '2024-01-01T10:00:30.000000Z'::TIMESTAMP) t FROM long_sequence(366))
                    """);
            // Each static element matches the row of its day. $today matches no row on 2025-06-01
            // and the row of 2024-12-30, which no static element holds, on that day.
            final String days336 = "2024-[01..12]-[01..28]T10:00";
            final String days252 = "2024-[01..09]-[01..28]T10:00";
            final String elements256 = "[$today, " + days252 + ", 2024-10-[01..03]T10:00]";
            final long jun2025 = Micros.toMicros(2025, 6, 1, 1, 0);
            final long dec2024 = Micros.toMicros(2024, 12, 30, 1, 0);
            for (String c : new String[]{"dt", "tm", "tn"}) {
                final String query = "SELECT count() FROM x WHERE " + c;
                // The IN list threshold caps a DATE key, so the Java filter runs the 336 DATE intervals;
                // TIMESTAMP keys keep the uncapped JIT path.
                final boolean isJitExpected = !"dt".equals(c);
                assertCountFollowsClock(query + " IN '[" + days336 + "]'", isJitExpected, jun2025, 336, dec2024, 336);
                assertCountFollowsClock(query + " IN '[$today, " + days336 + "]'", false, jun2025, 336, dec2024, 337);
                assertCountFollowsClock(query + " IN ('[$today, " + days336 + "]')", false, jun2025, 336, dec2024, 337);
                assertCountFollowsClock(query + " NOT IN '[$today, " + days336 + "]'", false, jun2025, 30, dec2024, 29);
                assertCountFollowsClock(query + " IN '" + elements256 + "'", false, jun2025, 255, dec2024, 256);
                assertCountFollowsClock(query + " IN '[$today, " + days252 + "]'", false, jun2025, 252, dec2024, 253);
            }
            // Interval scan.
            final String query = "SELECT count() FROM x WHERE ts";
            assertCountFollowsClock(query + " IN '[$today, " + days336 + "]'", false, jun2025, 336, dec2024, 337);
            assertCountFollowsClock(query + " NOT IN '[$today, " + days336 + "]'", false, jun2025, 30, dec2024, 29);
            assertCountFollowsClock(query + " IN '" + elements256 + "'", false, jun2025, 255, dec2024, 256);
            // Filter.
            assertCountFollowsClock(query + " IN '[" + days336 + "]' OR k = 0", true, jun2025, 336, dec2024, 336);
            assertCountFollowsClock(query + " IN '[$today, " + days336 + "]' OR k = 0", false, jun2025, 336, dec2024, 337);
            assertCountFollowsClock(query + " IN '" + elements256 + "' OR k = 0", false, jun2025, 255, dec2024, 256);
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

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
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

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected2);
            assertSqlRunWithJit(query);
        });
    }

    @Test
    public void testEmptyIntervalInFilterMatchesNoRow() throws Exception {
        // 2024-01-06 is a Saturday, so '2024-01-06#workday' holds no interval. The JIT used to push
        // no value for it: under OR or NOT the JVM crashed, and under AND or alone the filter
        // stored rows.
        assertMemoryLeak(() -> {
            createTemporalDaysTable();
            final String empty = " IN '2024-01-06#workday'";
            for (String c : new String[]{"dt", "tm", "tn"}) {
                // The IN function is the constant false, and the parser folds the tree around it to
                // a constant. The code generator then skips the scan or the filter, and with it the JIT.
                assertFilterWithAndWithoutJit(c + empty, keys(), false);
                assertFilterWithAndWithoutJit(c + " IN ('2024-01-06#workday')", keys(), false);
                assertFilterWithAndWithoutJit(c + empty + " AND k = 6", keys(), false);
                assertFilterWithAndWithoutJit("NOT " + c + empty, allDayKeysExcept(), false, QueryAssertion::expectSize);
                assertFilterWithAndWithoutJit(c + " NOT IN ('2024-01-06#workday')", allDayKeysExcept(), false, QueryAssertion::expectSize);
                assertFilterWithAndWithoutJit(c + empty + " AND " + c + " IN '2024-01'", keys(), false);
                // The Java filter folds to its other operand, while the JIT compiles the whole tree,
                // the IN as a comparison that is false on every row.
                assertFilterWithAndWithoutJit(c + empty + " OR k = 2", keys(2), true);
                assertFilterWithAndWithoutJit(c + " NOT" + empty + " AND k < 3", keys(0, 1, 2), true);
                assertFilterWithAndWithoutJit(c + empty + " OR " + c + " IN '2024-01-02'", keys(2), true);
            }
            // The interval scan takes an IN on the designated timestamp only from an AND chain.
            assertFilterWithAndWithoutJit("ts" + empty + " OR k = 2", keys(2), true);
            assertFilterWithAndWithoutJit("NOT ts" + empty + " OR k = 2", allDayKeysExcept(), false, QueryAssertion::expectSize);

            assertQuery("SELECT k FROM x WHERE tm" + empty + " AND k = 6").noLeakCheck().assertsPlan("""
                    SelectedRecord
                        Empty table
                    """);
            assertQuery("SELECT k FROM x WHERE tm NOT" + empty).noLeakCheck().assertsPlan("""
                    SelectedRecord
                        PageFrame
                            Row forward scan
                            Frame forward scan on: x
                    """);
        });
    }

    @Test
    public void testFilteringOnSingleQuote() throws Exception {
        assertQuery("""
                SELECT timestamp as Time,
                avg(asks[1,1]-bids[1,1]) as Spread,
                sum(bids[1,1]*bids[2,1]) as Bid_Volume,
                sum(asks[1,1]*asks[2,1]) as ask_volume
                FROM market_data
                WHERE symbol = ''''
                SAMPLE BY 1s
                ORDER BY timestamp DESC
                LIMIT 6;""")
                .withPlan("""
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
                        """)
                .ddl("""
                        
                        CREATE TABLE 'market_data' (\s
                        \ttimestamp TIMESTAMP,
                        \tsymbol SYMBOL CAPACITY 16384 CACHE,
                        \tbids DOUBLE[][],
                        \tasks DOUBLE[][]
                        ) timestamp(timestamp);""")
                .timestampDesc("Time")
                .mutateWith("INSERT INTO market_data (timestamp, symbol, bids, asks) " +
                        "VALUES " +
                        "(0, 'abc', array[[1d,2d],[3d,4d]], array[[2d,3d],[4d,5d]]), " +
                        "(10_000_000, '''', array[[10d,20d],[30d,40d]], array[[20d,30d],[40d,50d]]);")
                .expectSize()
                .returns("Time\tSpread\tBid_Volume\task_volume\n", """
                        Time\tSpread\tBid_Volume\task_volume
                        1970-01-01T00:00:10.000000Z\t10.0\t300.0\t800.0
                        """);
    }

    @Test
    public void testFloatColumnAgainstFoldedI8ConstantPicksScalarPath() throws Exception {
        assertMemoryLeak(() -> {
            // The descend()-time fold of 258558L * -259815L emits a single I8
            // IMM directly without observing the I8 type, so the predicate's
            // global TypesObserver only saw F4 (from c5). hasMixedSizes()
            // returned false, exec_hint went to SINGLE_SIZE_TYPE, and the
            // backend picked the AVX2 path. AVX2 has no convert from a
            // 4-element i64 vector to an 8-element f32 vector, and the I8 -
            // F4 sub fell through convert() unchanged into vpsubq, garbage
            // for non-NULL c5 and a false comparison for NULL c5 -- which
            // dropped the NULL rows that the Java filter included via
            // NaN < x = false / NOT(false) = true. The fold path now
            // observes I8 in both observers so getExecHint() reports mixed
            // sizes and the scalar path is used instead.
            execute("CREATE TABLE x (c5 FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // Eight rows so the AVX2 step (256 / (4 * 8) = 8) covers the
            // whole frame in a single vector iteration without falling back
            // to the scalar tail. Row 4 is NULL; the predicate is true for
            // all rows because the folded LHS is around -6.7e10 and c5 is
            // either a small float or NaN (NaN < x is false, NOT false is
            // true).
            execute("INSERT INTO x VALUES " +
                    "(0.50::FLOAT, '2024-01-01T00:00:00.000000Z')," +
                    " (0.51::FLOAT, '2024-01-01T00:00:01.000000Z')," +
                    " (0.52::FLOAT, '2024-01-01T00:00:02.000000Z')," +
                    " (0.53::FLOAT, '2024-01-01T00:00:03.000000Z')," +
                    " (NULL,         '2024-01-01T00:00:04.000000Z')," +
                    " (0.55::FLOAT, '2024-01-01T00:00:05.000000Z')," +
                    " (0.56::FLOAT, '2024-01-01T00:00:06.000000Z')," +
                    " (0.57::FLOAT, '2024-01-01T00:00:07.000000Z')");

            String sql = "SELECT count(*) FROM x WHERE NOT (c5 < ((258558L * -259815L) - (708206 - c5)))";

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n8\n");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n8\n");

            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertTrue("predicate must still JIT", factory.usesCompiledFilter());
            }
        });
    }

    @Test
    public void testInIntervalComparisonCountsEachInAgainstThreshold() throws Exception {
        // The IN list threshold (10 by default) caps the interval count of each IN. Two INs of six
        // intervals each under one comparison stay within it, so the JIT compiles the filter.
        assertMemoryLeak(() -> {
            createTwoTemporalKeysTable();
            for (String c : new String[]{"d", "t", "n"}) {
                assertFilterWithAndWithoutJit(
                        "(" + c + "1 IN ('2024-01-10T10:00;1m;1d;6')) = (" + c + "2 IN ('2024-02-01T10:00;1m;1d;6'))",
                        keys(4, 5),
                        true
                );
            }
        });
    }

    @Test
    public void testInIntervalComparisonOfTwoKeys() throws Exception {
        // `=` and `!=` hold both INs in one predicate. The JIT used to compile the second IN it
        // serialized, the left one, with the intervals of both.
        assertMemoryLeak(() -> {
            createTwoTemporalKeysTable();
            for (String c : new String[]{"d", "t", "n"}) {
                final String a = c + "1";
                final String b = c + "2";
                assertFilterWithAndWithoutJit("(" + a + " IN '2024-01') = (" + b + " IN '2024-02')", keys(2, 4, 5), true);
                assertFilterWithAndWithoutJit("(" + a + " IN '2024-01') != (" + b + " IN '2024-02')", keys(1, 3), true);
                assertFilterWithAndWithoutJit("(" + a + " IN '2024-01') <> (" + b + " IN '2024-02')", keys(1, 3), true);
                assertFilterWithAndWithoutJit("(" + a + " IN ('2024-01')) = (" + b + " IN ('2024-02'))", keys(2, 4, 5), true);
                assertFilterWithAndWithoutJit("(" + a + " IN ('2024-01')) != (" + b + " IN ('2024-02'))", keys(1, 3), true);
                assertFilterWithAndWithoutJit("(" + b + " IN '2024-02-02') = (" + b + " IN '2024-02-06')", keys(1, 4, 5), true);
                assertFilterWithAndWithoutJit("(" + b + " IN ('2024-02-02')) != (" + b + " IN ('2024-02-06'))", keys(2, 3), true);
                // 2024-01-06 is a Saturday, so the left IN holds no interval and matches no row.
                assertFilterWithAndWithoutJit("(" + a + " IN '2024-01-06#workday') = (" + b + " IN '2024-02')", keys(1, 4, 5), true);
            }
        });
    }

    @Test
    public void testInIntervalOverTwoKeysMatchesJava() throws Exception {
        assertMemoryLeak(() -> {
            createTwoTemporalKeysTable();
            // $today is 2024-02-06
            setCurrentMicros(Micros.toMicros(2024, 2, 6, 1, 0));
            for (String c : new String[]{"d", "t", "n"}) {
                final String a = c + "1";
                final String b = c + "2";
                assertFilterWithAndWithoutJit(a + " IN '2024-01'", keys(1, 2), true);
                assertFilterWithAndWithoutJit(b + " IN ('2024-02')", keys(2, 3), true);
                // AND and OR are not predicate roots, and the optimiser pushes NOT down to each IN, so each IN is a predicate of its own.
                assertFilterWithAndWithoutJit(a + " IN '2024-01' AND " + b + " IN '2024-02'", keys(2), true);
                assertFilterWithAndWithoutJit(a + " IN ('2024-01') OR " + b + " IN ('2024-02')", keys(1, 2, 3), true);
                assertFilterWithAndWithoutJit("NOT (" + a + " IN '2024-01' AND " + b + " IN '2024-02')", keys(1, 3, 4, 5), true);
                assertFilterWithAndWithoutJit(a + " IN '2024-01-06#workday' OR " + b + " IN '2024-02'", keys(2, 3), true);
                // A date variable compiles its own intervals.
                assertFilterWithAndWithoutJit("(" + a + " IN '2024-01') = (" + b + " IN '$today')", keys(4, 5), true);
                assertFilterWithAndWithoutJit("(" + a + " IN '$today') = (" + b + " IN '2024-02')", keys(1, 4, 5), true);
            }
        });
    }

    @Test
    public void testInShortCircuitDoesNotLeakColumnCacheAfterEndSc() throws Exception {
        assertMemoryLeak(() -> {
            // Multi-value IN() in a scalar AND chain emits BEGIN_SC, EQ pairs
            // gated by OR_SC, AND_SC and END_SC. Column reads for the IN
            // values populate the C++ ColumnValueCache, which is keyed on
            // (column, type) and persists across the OR_SC forward jump. If
            // OR_SC takes its jump to END_SC, the load that populated the
            // cache entry is skipped, but a later MEM read for the same
            // column would otherwise hit the cache and use a register that
            // was never written on the jumped-to path -- returning garbage.
            // Here c0 is loaded both inside the IN block (for c0 = c1) and
            // outside it (for c0 != null), and c1 IN (c0, c1) trips the
            // OR_SC short-circuit on the c1 = c1 leaf for every non-NULL
            // c1, so the bug surfaces as JIT including rows where c0 IS
            // NULL. The fix snapshots the cache size at BEGIN_SC and
            // truncates back at END_SC so entries from inside the block do
            // not leak past it.
            execute("CREATE TABLE x (c0 FLOAT, c1 DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(0.5::FLOAT, 0.7, '2024-01-01T00:00:00.000000Z')," +
                    " (NULL,       0.4, '2024-01-01T00:00:01.000000Z')");

            // Predicates sort by priority: IN (PRIORITY_OTHER=5) before
            // c0 != null (PRIORITY_OTHER_NEQ=6), so IN's END_SC sits right
            // before the NE predicate that re-reads c0.
            String sql = "SELECT count(*) FROM x WHERE NOT (c0 IS NULL) AND c1 IN (c0, c1)";

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");

            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertTrue("predicate must still JIT", factory.usesCompiledFilter());
            }
        });
    }

    @Test
    public void testInTimestampCompiledDayFilterBracketExpansionOverMaxIntervals() throws Exception {
        // A date variable compiles the list. Its static element holds 1,056 Mondays above the cap of
        // 1,024 with 20[03..24], and 1,008 with 20[04..24].
        assertMemoryLeak(() -> {
            createDayFilterTable();
            try {
                for (int jitMode : new int[]{SqlJitMode.JIT_MODE_DISABLED, SqlJitMode.JIT_MODE_FORCE_SCALAR, SqlJitMode.JIT_MODE_ENABLED}) {
                    sqlExecutionContext.setJitMode(jitMode);
                    final String sql = "SELECT k FROM x WHERE ts IN '[$today,20[03..24]-[01..12]-[01..28]]#mon'";
                    assertExceptionNoLeakCheck(sql, sql.indexOf("'["), "Bracket expansion produces too many intervals (max 1024)");
                    assertQuery("SELECT k FROM x WHERE ts IN '[$today,20[04..24]-[01..12]-[01..28]]#mon'")
                            .noLeakCheck()
                            .returns("""
                                    k
                                    3
                                    6
                                    """);
                }
            } finally {
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            }
        });
    }

    @Test
    public void testInTimestampDateListDayFilterRepeatedElements() throws Exception {
        // Each element holds 240 days, so together they exceed the list's incremental merge
        // threshold (256) when counted unfiltered. Each element filters its own days before any
        // union.
        assertMemoryLeak(() -> {
            createDayFilterTable();
            try {
                for (int jitMode : new int[]{SqlJitMode.JIT_MODE_DISABLED, SqlJitMode.JIT_MODE_ENABLED}) {
                    sqlExecutionContext.setJitMode(jitMode);
                    assertQuery("SELECT k FROM x WHERE ts IN '[2024-[01..12]-[01..20],2024-[01..12]-[01..20]]#mon'")
                            .noLeakCheck()
                            .returns("""
                                    k
                                    3
                                    6
                                    """);
                }
            } finally {
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            }
        });
    }

    @Test
    public void testInTimestampDateVariableRangeElementDayFilter() throws Exception {
        // the six days from $today, the Monday 2024-01-08, hold one Monday (row 3)
        assertMemoryLeak(() -> {
            createDayFilterTable();
            final String monday = """
                    k
                    3
                    """;
            bindVariableService.clear();
            bindVariableService.setStr("p", "[$today..$today+5d#mon]");
            assertQuery("SELECT k FROM x WHERE ts IN :p").noLeakCheck().returns(monday);
            assertQuery("SELECT k FROM x WHERE ts NOT IN :p").noLeakCheck().returns("""
                    k
                    1
                    2
                    4
                    5
                    6
                    """);
            // the element filter overrides the global one
            bindVariableService.setStr("p", "[$today..$today+5d#mon]#tue");
            assertQuery("SELECT k FROM x WHERE ts IN :p").noLeakCheck().returns(monday);
            bindVariableService.setStr("p", "[$today..$today+5d]#mon");
            assertQuery("SELECT k FROM x WHERE ts IN :p").noLeakCheck().returns(monday);
        });
    }

    @Test
    public void testInTimestampDayFilterAboveMergeThreshold() throws Exception {
        // 336 bracket values reach the incremental merge threshold (256) of the bracket expansion.
        // The row filter runs the IN on a timestamp that is not designated.
        assertMemoryLeak(() -> {
            createDayFilterTable();
            assertFilterWithAndWithoutJit("ts IN '2024-[01..12]-[01..28]#mon'", "k\n3\n6\n", true);
            assertFilterWithAndWithoutJit("ts NOT IN '2024-[01..12]-[01..28]#mon'", "k\n1\n2\n4\n5\n", true);
            // control below the merge threshold
            assertFilterWithAndWithoutJit("ts IN '2024-[01..09]-[01..28]#mon'", "k\n3\n6\n", true);
        });
    }

    @Test
    public void testInTimestampDayFilterBracketExpansionOverMaxIntervals() throws Exception {
        // 22 years x 12 months x 4 Mondays among days 1-28 make 1,056 intervals, above the cap of
        // 1,024; one year less makes 1,008
        assertMemoryLeak(() -> {
            createDayFilterTable();
            final String overCap = "[2003..2024]-[01..12]-[01..28]#mon";
            final String overCapError = "Bracket expansion produces too many intervals (max 1024)";
            try {
                for (int jitMode : new int[]{SqlJitMode.JIT_MODE_DISABLED, SqlJitMode.JIT_MODE_ENABLED}) {
                    sqlExecutionContext.setJitMode(jitMode);
                    bindVariableService.clear();
                    // the error points at the tick string literal
                    assertExceptionNoLeakCheck("SELECT k FROM x WHERE ts IN '" + overCap + "'", 28, overCapError);
                    assertExceptionNoLeakCheck("SELECT k FROM x WHERE ts NOT IN '" + overCap + "'", 32, overCapError);
                    assertQuery("SELECT k FROM x WHERE ts IN '[2004..2024]-[01..12]-[01..28]#mon'")
                            .noLeakCheck()
                            .returns("""
                                    k
                                    3
                                    6
                                    """);
                    // The row filter parses a bind string when the cursor opens, and any parse error
                    // fails the query at position 0.
                    bindVariableService.setStr("p", "2024-13-45");
                    assertExceptionNoLeakCheck("SELECT k FROM x WHERE ts IN :p", 0, "Invalid date: 2024-13-45");
                    bindVariableService.setStr("p", overCap);
                    assertExceptionNoLeakCheck("SELECT k FROM x WHERE ts IN :p", 0, overCapError);
                    assertExceptionNoLeakCheck("SELECT k FROM x WHERE ts NOT IN :p", 0, overCapError);
                    // the check after each day of a date variable range reports its own message
                    bindVariableService.setStr("p", "[$today..$today+5000d]T[00,12]:00#mon;1h");
                    assertExceptionNoLeakCheck("SELECT k FROM x WHERE ts IN :p", 0, "Date range expansion produces too many intervals (max 1024)");
                }
            } finally {
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            }
        });
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
    public void testIntArithmeticWrapsAtInt32InMixedLongDoubleContext() throws Exception {
        assertMemoryLeak(() -> {
            // The fuzzer hit a JIT/Java divergence on a predicate of shape
            //   ((d_const + d_const) - c6_long) > (c9_int * int_const)
            // where the int product overflows int32. serializeUntypedNumber
            // skipped the I4 emit path whenever the predicate had any LONG
            // operand and widened the integer constant to I8, which made
            // convert() promote c9 to i64 at MUL time and the JIT compute
            // c9 * 403251 at long width. The Java filter went through
            // IntFunction#getDouble = intToDouble(getInt) and wrapped at
            // int32, so the two paths disagreed for c9 large enough that
            // the inner product overflowed int32. After the fix the JIT
            // keeps the constant at I4 when any FLOAT / DOUBLE source is
            // present in the predicate, and int32_mul wraps on both sides.
            execute("CREATE TABLE x (c6 LONG, c9 INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(0,  5328, '2024-01-01T00:00:00.000000Z')," +
                    " (0,  6000, '2024-01-01T00:00:00.000001Z')," +
                    " (0,  1000, '2024-01-01T00:00:00.000002Z')," +
                    " (0, -6000, '2024-01-01T00:00:00.000003Z')," +
                    " (0,  -100, '2024-01-01T00:00:00.000004Z')");

            String sql = "SELECT count(*) FROM x WHERE ((0.983116 + 0.206995) - c6) > (c9 * 403251)";
            //  5328 *   403251 wraps to -2_146_445_968 at int32. 1.19 > -2.146e9 -> true.
            //  6000 *   403251 wraps to -1_875_461_296 at int32. 1.19 > -1.875e9 -> true.
            //  1000 *   403251 =        403_251_000  at int32.   1.19 > 4e8      -> false.
            // -6000 *   403251 wraps to  1_875_461_296 at int32. 1.19 > 1.875e9  -> false.
            //  -100 *   403251 =         -40_325_100  at int32.  1.19 > -4e7     -> true.
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            assertQuery(sql)
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n3\n");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertQuery(sql)
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n3\n");

            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertTrue("INT*INT in mixed LONG/DOUBLE context must still JIT",
                        factory.usesCompiledFilter());
            }
        });
    }

    @Test
    public void testIntColumnArithmeticInDoubleContextStaysAtIntWidth() throws Exception {
        assertMemoryLeak(() -> {
            // Negative case: in DOUBLE context the Java filter goes through
            // IntFunction#getDouble = intToDouble(getInt) at int width, so
            // the JIT must NOT widen INT operands to i64. NarrowI64WidenDetector
            // only triggers when a LONG operand is present and no FLOAT/DOUBLE
            // source is in the predicate, leaving the FLOAT / DOUBLE path at
            // int width (matching int32_mul / int32_add overflow semantics on
            // both sides).
            execute("CREATE TABLE x (i INT, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(46341, 0.0, '2024-01-01T00:00:00.000000Z')," +
                    " (46340, 0.0, '2024-01-01T00:00:00.000001Z')," +
                    " (10, 0.0, '2024-01-01T00:00:00.000002Z')");

            String sql = "SELECT count(*) FROM x WHERE (i * i) > d";
            // 46341 * 46341 = 2_147_488_281 -> int32 wraps to negative -> excluded.
            // 46340 * 46340 = 2_147_395_600 -> fits int32, positive -> included.
            // 10 * 10 = 100 -> included. So two rows match.
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            assertQuery(sql)
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n2\n");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertQuery(sql)
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n2\n");

            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertTrue("INT*INT in DOUBLE context must still JIT",
                        factory.usesCompiledFilter());
            }
        });
    }

    @Test
    public void testIntColumnArithmeticInFloatContextStaysAtFloatWidth() throws Exception {
        assertMemoryLeak(() -> {
            // INT-FLOAT arithmetic resolves to `-(FF)` (FloatFunctionFactory),
            // matching `+(FF)` / `*(FF)` / `/(FF)`. Both interpreter and JIT
            // therefore compute at f32 width and agree on rounding. Without
            // `-(FF)`, the parser's only viable match is `-(DD)` (since FLOAT
            // does not widen to INT or LONG), which would push the interpreter
            // to f64 while JIT stays at f32 -- the original divergence the
            // fuzzer found on `(c0 - (c0 - c5)) <= c5`.
            execute("CREATE TABLE x (c0 INT, c5 FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(999999, 0.123456, '2024-01-01T00:00:00.000000Z')," +
                    " (10, 0.5, '2024-01-01T00:00:00.000001Z')");

            // Row 1 is excluded under f32 rounding: `999999 - 0.123456` rounds
            // to 999998.875, so `999999 - 999998.875 = 0.125`, which is
            // strictly greater than 0.123456. Row 2 has small magnitudes that
            // fit exactly in f32 and matches.
            String sql = "SELECT count(*) FROM x WHERE (c0 - (c0 - c5)) <= c5";
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            assertQuery(sql)
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n1\n");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertQuery(sql)
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n1\n");

            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertTrue("INT-FLOAT arithmetic must still JIT", factory.usesCompiledFilter());
            }
        });
    }

    @Test
    public void testIntColumnArithmeticStaysAtIntWidthWhenFloatSuppressesWidening() throws Exception {
        assertMemoryLeak(() -> {
            // Inverse of testIntColumnArithmeticWidenedToLongInLongContext:
            // when the predicate also has a FLOAT/DOUBLE source, the int-
            // arithmetic subtree is consumed by IntFunction#getDouble, which
            // calls IntFunction.getInt() and wraps modulo 2^32. The IR
            // emitter must suppress widening when any F operand appears,
            // including lexical float CONSTANT tokens like 0.5.
            execute("CREATE TABLE x (a INT, b INT, l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(46341, 46341, 0, '2024-01-01T00:00:00.000000Z')," +
                    " (10, 5, 0, '2024-01-01T00:00:00.000001Z')");

            // 46341 * 46341 = 2_147_488_281L; wraps to -2_147_479_015 at
            // int32, so row 1's 0.5 * (a*b) is negative and only row 2
            // matches. Without the suppression the JIT keeps the long
            // product and the count would be 2.
            String sql = "SELECT count(*) FROM x WHERE ((l + 0.5) * (a * b)) > 0";

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");

            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertTrue("predicate must still JIT", factory.usesCompiledFilter());
            }
        });
    }

    @Test
    public void testIntColumnArithmeticWrapsInLongContext() throws Exception {
        assertMemoryLeak(() -> {
            // INT-INT arithmetic compared against a LONG column carries one value: the one its
            // four bytes hold. Both filters wrap it modulo 2^32 and only then sign-extend for
            // the comparison, so the JIT computes int32_* and the Java filter reads
            // AddInt / MulInt / SubInt at INT width, which is what IntFunction.getLong()
            // sign-extends. What this pins is that the two agree on the wrapped value.
            execute("CREATE TABLE x (a INT, b INT, l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(2147483640, 1, 0, '2024-01-01T00:00:00.000000Z')," +
                    " (-2147483640, -1, 0, '2024-01-01T00:00:00.000001Z')," +
                    " (46341, 46341, 0, '2024-01-01T00:00:00.000002Z')," +
                    " (10, 5, 0, '2024-01-01T00:00:00.000003Z')");

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            // a + b: 2147483640+1=2147483641 (positive long), -2147483640-1 negative,
            // 46341+46341=92682 positive, 10+5=15 positive -> three rows match.
            assertQuery("SELECT count(*) FROM x WHERE (a + b) > l")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n3\n");
            // a - b: 2147483640-1=positive, -2147483640-(-1)=negative,
            // 46341-46341=0 (not > 0), 10-5=5 positive -> two rows match.
            assertQuery("SELECT count(*) FROM x WHERE (a - b) > l")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n2\n");
            // a * b: 2147483640*1=positive, -2147483640*-1=positive,
            // 46341*46341 overflows int32 and wraps to -2_147_479_015 (not > 0),
            // 10*5=50 -> three rows match. The wrapping row is the discriminating one:
            // computing the product at long width would give 2_147_488_281 and a fourth row.
            assertQuery("SELECT count(*) FROM x WHERE (a * b) > l")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n3\n");

            try (RecordCursorFactory factory = select("SELECT count(*) FROM x WHERE (a + b) > l")) {
                Assert.assertTrue("INT-INT arithmetic in LONG context must still JIT",
                        factory.usesCompiledFilter());
            }
            try (RecordCursorFactory factory = select("SELECT count(*) FROM x WHERE (a * b) > l")) {
                Assert.assertTrue("INT*INT arithmetic in LONG context must still JIT",
                        factory.usesCompiledFilter());
            }
        });
    }

    @Test
    public void testIntegerArithmeticPreservesNullThroughVectorPath() throws Exception {
        assertMemoryLeak(() -> {
            // The AVX2 i64 mul kernel mutated its lhs vector while computing the
            // 64-bit product, then passed the now-clobbered vector into the
            // null-propagation blend. With a LONG null sentinel (Long.MIN_VALUE)
            // multiplied by an even constant, the low-32-bit product wraps to
            // zero, so the blend no longer recognised the lane as null and the
            // JIT result diverged from the scalar path.
            // The audit walked every AVX2 arithmetic kernel (add/sub/mul/div
            // for i32 and i64) and only i64 mul actually mutated its inputs.
            // This test covers the original bug and locks the audit in: each
            // predicate would return zero JIT rows (and diverge from the Java
            // filter) if any of these kernels stopped feeding pristine lhs/rhs
            // into blend_with_nulls.
            // Predicate shape: <col> = (<col> <op> <const>). Under QuestDB's
            // EqXxxFunctionFactory, NULL == NULL is true (both sides return
            // the type's null sentinel and the primitive == matches), so a
            // NULL row matches iff arithmetic on a NULL preserves NULL.
            //   col = NULL -> Java: <op> propagates NULL, NULL = NULL -> true.
            //   col != NULL -> any of these <op>s yield a different value,
            //                  so the predicate is false.
            // Need enough rows so each kernel's AVX2 main loop runs at least
            // once: i64 step=4, i32 step=8. Use 9 rows so both kernels exercise
            // a main iteration plus a scalar tail (the bug never reached the
            // scalar tail path, which preserves lhs).
            execute("CREATE TABLE x (l LONG, i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(NULL, NULL, '2024-01-01T00:00:00.000000Z')," +
                    " (1L, 1, '2024-01-01T00:00:01.000000Z')," +
                    " (2L, 2, '2024-01-01T00:00:02.000000Z')," +
                    " (3L, 3, '2024-01-01T00:00:03.000000Z')," +
                    " (4L, 4, '2024-01-01T00:00:04.000000Z')," +
                    " (5L, 5, '2024-01-01T00:00:05.000000Z')," +
                    " (6L, 6, '2024-01-01T00:00:06.000000Z')," +
                    " (7L, 7, '2024-01-01T00:00:07.000000Z')," +
                    " (8L, 8, '2024-01-01T00:00:08.000000Z')");

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);

            // i64 (LONG): the original failing shape plus the other operators.
            assertQuery("SELECT count(*) FROM x WHERE l = (l * 939722L)")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n1\n");
            assertQuery("SELECT count(*) FROM x WHERE l = (l + 1L)")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n1\n");
            assertQuery("SELECT count(*) FROM x WHERE l = (l - 1L)")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n1\n");
            assertQuery("SELECT count(*) FROM x WHERE l = (l / 2L)")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n1\n");

            // i32 (INT): all four operators.
            assertQuery("SELECT count(*) FROM x WHERE i = (i * 939722)")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n1\n");
            assertQuery("SELECT count(*) FROM x WHERE i = (i + 1)")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n1\n");
            assertQuery("SELECT count(*) FROM x WHERE i = (i - 1)")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n1\n");
            assertQuery("SELECT count(*) FROM x WHERE i = (i / 2)")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n1\n");

            // Sanity check that JIT actually compiled (otherwise the test
            // would silently pass via the Java filter).
            try (RecordCursorFactory factory = select("SELECT count(*) FROM x WHERE l = (l * 939722L)")) {
                Assert.assertTrue("JIT must compile this filter", factory.usesCompiledFilter());
            }
        });
    }

    @Test
    public void testLatestByDeferredSymbolConstants() throws Exception {
        assertQuery("SELECT v FROM latest_symbols WHERE filter_sym='B' OR filter_sym='D' LATEST ON ts PARTITION BY s,t")
                .ddl(
                        "CREATE TABLE latest_symbols (ts TIMESTAMP, v LONG, s SYMBOL, t SYMBOL, filter_sym SYMBOL) TIMESTAMP(ts)",
                        "INSERT INTO latest_symbols VALUES (1, 1, 'a', 'x', 'A'), (2, 2, 'a', 'x', 'B')"
                )
                .sizeMayVary().withPlanContaining("jit: true")
                .mutateWith("INSERT INTO latest_symbols VALUES (3, 3, 'a', 'x', 'D'), (4, 4, 'b', 'y', 'D')")
                .returns("v\n2\n", "v\n3\n4\n");
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

            assertQuery(query)
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns(expected);
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

            assertQuery(query)
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns(expected);
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
    public void testNarrowIntArithmeticPreservesNullInFloatContext() throws Exception {
        assertMemoryLeak(() -> {
            // Narrow int arithmetic (i8/i16/i32 add/sub/mul/div via int32_*)
            // runs at i32 width and can carry INT_NULL: either an INT operand
            // was INT_NULL, or division by zero produced INT_NULL via
            // int32_div. Before the fix the kernel returned the result tagged
            // with the narrower lhs dtype (e.g. i8 for BYTE * INT). The
            // downstream f32 / f64 conversion then went through
            // cvt_null_check(i8) = false and skipped the NaN substitution, so
            // INT_NULL flowed into the float comparison as -2_147_483_648.0.
            // Java's path (MulInt.getDouble = intToDouble(getInt)) substitutes
            // NaN whenever the int product was INT_NULL, and the negating `<=`
            // / `<` wrapper treats any NaN side as matching, so the two paths
            // diverged.
            //
            // Rows: b/s/i columns drive the narrow arithmetic; the (0, 0,
            // NULL) row covers div-by-zero and the i = NULL rows cover
            // INT_NULL propagation through mul / add / sub.
            execute("CREATE TABLE x (b BYTE, s SHORT, i INT, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1, 1, NULL, '2024-01-01T00:00:00.000000Z')," +
                    " (2, 2, NULL, '2024-01-01T00:00:00.000001Z')," +
                    " (5, 5, 7, '2024-01-01T00:00:00.000002Z')," +
                    " (0, 0, NULL, '2024-01-01T00:00:00.000003Z')");

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);

            // BYTE * INT (the original fuzzer divergence). Three of the four
            // rows have i = NULL, so the int product is INT_NULL and the
            // f64 cast returns NaN; the (b=5, i=7) row gives 35 and 1.74256
            // <= 35.0, so the predicate excludes every row.
            assertQuery("SELECT count(*) FROM x WHERE NOT ((0.348512 * b) <= (b * i))")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n0\n");
            // Same shape with addition (int32_add propagates INT_NULL).
            assertQuery("SELECT count(*) FROM x WHERE NOT ((0.348512 * b) <= (b + i))")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n0\n");
            // Subtraction: NaN rows still excluded; the (b=5, i=7) row gives
            // -2, so NOT(1.74256 <= -2.0) matches and the count is 1.
            assertQuery("SELECT count(*) FROM x WHERE NOT ((0.348512 * b) <= (b - i))")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n1\n");
            // SHORT * INT exercises the i16 narrow path.
            assertQuery("SELECT count(*) FROM x WHERE NOT ((0.348512 * s) <= (s * i))")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n0\n");
            // Division by zero on narrow operands: int32_div returns INT_NULL
            // when rhs is 0 even though both inputs are BYTE / SHORT, so the
            // i32 widening matters here too. The (0, 0, _) row divides by
            // zero -> NaN -> excluded; the (5, 5, _) row gives 1 and
            // NOT(1.74256 <= 1.0) matches, so the count is 1.
            assertQuery("SELECT count(*) FROM x WHERE NOT ((0.348512 * b) <= (b / s))")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n1\n");

            // JIT must still compile each predicate -- the fix is at the C++
            // kernel level (result dtype tagging), the IR side is unchanged.
            try (RecordCursorFactory factory = select(
                    "SELECT count(*) FROM x WHERE NOT ((0.348512 * b) <= (b * i))")) {
                Assert.assertTrue("BYTE * INT in DOUBLE context must still JIT",
                        factory.usesCompiledFilter());
            }
            try (RecordCursorFactory factory = select(
                    "SELECT count(*) FROM x WHERE NOT ((0.348512 * b) <= (b / s))")) {
                Assert.assertTrue("BYTE / SHORT in DOUBLE context must still JIT",
                        factory.usesCompiledFilter());
            }
        });
    }

    @Test
    public void testNarrowIntArithmeticWithIntLiteralInDoubleContext() throws Exception {
        assertMemoryLeak(() -> {
            // Same shape as the FLOAT case but with a DOUBLE column. F8 also
            // does not trigger the I8-IMM path -- IntFunction.getDouble keeps
            // the int math at int width regardless of whether the consumer is
            // FLOAT or DOUBLE.
            execute("CREATE TABLE x (s SHORT, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(32767, 0.0, '2024-01-01T00:00:00.000000Z')," +
                    " (-32768, 0.0, '2024-01-01T00:00:00.000001Z')," +
                    " (30000, 0.0, '2024-01-01T00:00:00.000002Z')," +
                    " (10, 0.0, '2024-01-01T00:00:00.000003Z')");

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertQuery("SELECT count(*) FROM x WHERE (s * 78941) > d")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n2\n");

            try (RecordCursorFactory factory = select("SELECT count(*) FROM x WHERE (s * 78941) > d")) {
                Assert.assertTrue("narrow * int literal vs double must still JIT",
                        factory.usesCompiledFilter());
            }
        });
    }

    @Test
    public void testNarrowIntArithmeticWithIntLiteralInFloatContext() throws Exception {
        assertMemoryLeak(() -> {
            // Predicate (s * 78941) > f compares a SHORT*INT product against a
            // FLOAT column. The Java filter consumes the IntFunction via
            // CastIntToDouble.getDouble = intToDouble(getInt), so the multiplication
            // stays at int width with overflow and the COMPARISON runs at f64.
            // The JIT reproduces both halves: it keeps the product at i32 (emitting
            // 78941 at I8 would force a long-width mul and change the wrap), then
            // emits SX_I64 after the multiply so convert() takes the (i64, f32) arm
            // instead of rounding the i32 product to f32.
            // 32767*78941 wraps to -1_708_307_549, whose nearest float is
            // -1_708_307_584, so the last row below sits exactly on the rounded
            // bound: the Java filter keeps it and an f32 comparison dropped it.
            execute("CREATE TABLE x (s SHORT, f FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(32767, 0.0, '2024-01-01T00:00:00.000000Z')," +
                    " (-32768, 0.0, '2024-01-01T00:00:00.000001Z')," +
                    " (30000, 0.0, '2024-01-01T00:00:00.000002Z')," +
                    " (10, 0.0, '2024-01-01T00:00:00.000003Z')," +
                    " (32767, -1.708307584E9, '2024-01-01T00:00:00.000004Z')");

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            // At int width: 32767*78941 wraps to -1_708_307_549,
            // 30000*78941 wraps to -1_926_737_296, both negative -- excluded by
            // "> 0.0". -32768*78941 wraps to +1_708_228_608 (positive int after wrap)
            // and 10*78941 = 789_410 -- both included. The fifth row compares the
            // same -1_708_307_549 against -1_708_307_584 and is included too, which
            // the f32 comparison dropped. So three rows match.
            assertQuery("SELECT count(*) FROM x WHERE (s * 78941) > f")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n3\n");

            try (RecordCursorFactory factory = select("SELECT count(*) FROM x WHERE (s * 78941) > f")) {
                Assert.assertTrue("narrow * int literal vs float must still JIT",
                        factory.usesCompiledFilter());
            }
        });
    }

    @Test
    public void testNarrowIntArithmeticWithIntLiteralInLongContext() throws Exception {
        assertMemoryLeak(() -> {
            // Predicate (s * 78941) > l mixes a SHORT column with an INT literal
            // and a LONG column. SHORT promotes to INT, so the product is an INT
            // expression and wraps modulo 2^32 on both paths: the JIT emits 78941
            // as an I4 IMM and dispatches int32_mul, and the Java filter reads
            // MulInt at INT width. The LONG peer decides the width of the
            // COMPARISON, never of the operand, so it does not promote the product.
            execute("CREATE TABLE x (s SHORT, l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(32767, 0, '2024-01-01T00:00:00.000000Z')," +
                    " (-32768, 0, '2024-01-01T00:00:00.000001Z')," +
                    " (30000, 0, '2024-01-01T00:00:00.000002Z')," +
                    " (10, 0, '2024-01-01T00:00:00.000003Z')");

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            // Wrapped: s = 32767 gives -1_708_307_549 and s = 30000 gives -1_926_737_296,
            // both negative; s = -32768 wraps the other way onto +1_708_228_608 and s = 10
            // stays at 789_410. So two rows match. Computing at long width would flip every
            // one of the four rows, which is what makes this discriminating.
            assertQuery("SELECT count(*) FROM x WHERE (s * 78941) > l")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n2\n");

            try (RecordCursorFactory factory = select("SELECT count(*) FROM x WHERE (s * 78941) > l")) {
                Assert.assertTrue("narrow * int literal vs long must still JIT",
                        factory.usesCompiledFilter());
            }
        });
    }

    @Test
    public void testNarrowIntColumnVsOutOfRangeLiteral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " rnd_byte() b," +
                    " rnd_short() s," +
                    " timestamp_sequence(0, 1_000_000) ts" +
                    " FROM long_sequence(1_000)) TIMESTAMP(ts) PARTITION BY DAY");

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);

            // SHORT range is [-32768, 32767]; literals outside this range must yield no rows.
            // Before the fix, JIT silently truncated the literal to the column's width
            // (e.g. (short) 346548 = 18868), letting matching values through and diverging
            // from the Java filter. The fix throws SqlException at IR serialization time so
            // SqlCodeGenerator falls back to the Java filter, which evaluates the comparison
            // at int width.
            assertQuery("SELECT count(*) FROM x WHERE s > 346548")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n0\n");
            assertQuery("SELECT count(*) FROM x WHERE s <= -897671")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n0\n");
            assertQuery("SELECT count(*) FROM x WHERE s = 100000")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n0\n");
            // != against an out-of-range literal is true for every row.
            assertQuery("SELECT count(*) FROM x WHERE s != 100000")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n1000\n");
            // BYTE range is [-128, 127].
            assertQuery("SELECT count(*) FROM x WHERE b > 200")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n0\n");
            assertQuery("SELECT count(*) FROM x WHERE b <= -300")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n0\n");
            assertQuery("SELECT count(*) FROM x WHERE b = 1000")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n0\n");

            // Sanity-check the fallback path.
            try (RecordCursorFactory factory = select("SELECT count(*) FROM x WHERE s > 346548")) {
                Assert.assertFalse("JIT must not compile out-of-range short comparison",
                        factory.usesCompiledFilter());
            }
            try (RecordCursorFactory factory = select("SELECT count(*) FROM x WHERE b <= -300")) {
                Assert.assertFalse("JIT must not compile out-of-range byte comparison",
                        factory.usesCompiledFilter());
            }

            // In-range literals must continue to JIT.
            try (RecordCursorFactory factory = select("SELECT count(*) FROM x WHERE s > 100")) {
                Assert.assertTrue("in-range short comparison must JIT",
                        factory.usesCompiledFilter());
            }
            try (RecordCursorFactory factory = select("SELECT count(*) FROM x WHERE b > 100")) {
                Assert.assertTrue("in-range byte comparison must JIT",
                        factory.usesCompiledFilter());
            }
        });
    }

    @Test
    public void testNarrowMixedWidthArithmetic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SHORT, i INT, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // s=200 -> s*s = 40_000 in int math, but overflows SHORT (low 16 bits = -25_536).
            // s*s + i = 40_000, matches d=40_000.0; the Java filter widens s to int via the
            // *.sql.Function classes and returns the row. A SIMD JIT computing s*s at SHORT
            // width would miss it. Same for s=-200; s=10 stays in range, so its row is
            // uncontroversial.
            execute("INSERT INTO x VALUES (200, 0, 40000.0, '2024-01-01T00:00:00.000000Z')," +
                    " (-200, 0, 40000.0, '2024-01-01T00:00:01.000000Z')," +
                    " (10, 0, 100.0, '2024-01-01T00:00:02.000000Z')");

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertQuery("SELECT count(*) FROM x WHERE d = (s * s) + i")
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n3\n");

            // Narrow arithmetic mixed with a wider operand must JIT in scalar mode --
            // SIMD would overflow at narrow width, but scalar upcasts to int.
            try (RecordCursorFactory factory = select("SELECT count(*) FROM x WHERE d = (s * s) + i")) {
                Assert.assertTrue("narrow arithmetic must JIT (scalar mode)",
                        factory.usesCompiledFilter());
            }
        });
    }

    @Test
    public void testNestedIntArithmeticWrapsInLongContext() throws Exception {
        assertMemoryLeak(() -> {
            // Predicate of shape c0 <= ((-732674 * c5) + -238927), c0 LONG and c5 INT. The
            // whole right-hand side is an INT expression, so the inner product wraps at int32
            // and the outer sum is taken on the wrapped value; only the finished INT result
            // sign-extends for the comparison against c0. Both filters have to agree on that,
            // which is why the test runs the same SQL with the JIT off and then on.
            execute("CREATE TABLE x (c0 LONG, c5 INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1_000_000_000, 10000, '2024-01-01T00:00:00.000000Z')," +
                    " (-7_326_979_000, 10000, '2024-01-01T00:00:00.000001Z')," +
                    " (2_000_000_000, 10000, '2024-01-01T00:00:00.000002Z')");

            // -732674 * 10000 is -7_326_740_000, which overflows int32 and wraps onto
            // 1_263_194_592; adding -238927 gives an rhs of 1_262_955_665. So both
            // c0 = -7_326_979_000 and c0 = 1_000_000_000 match, and c0 = 2_000_000_000 does
            // not: count 2. Computing at long width would give an rhs of -7_326_978_927 and
            // a count of 1, so the two rules are told apart by this table.
            String sql = "SELECT count(*) FROM x WHERE c0 <= ((-732674 * c5) + -238927)";

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n2\n");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n2\n");

            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertTrue("nested INT arithmetic in LONG context must still JIT",
                        factory.usesCompiledFilter());
            }

            // Same shape with subtraction, which reaches SubInt rather than AddInt. The rhs is
            // the same 1_262_955_665, so the count is the same 2; at long width it would be 1.
            String subSql = "SELECT count(*) FROM x WHERE c0 <= ((-732674 * c5) - 238927)";
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            assertQuery(subSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n2\n");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertQuery(subSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n2\n");

            // Same shape under unary minus, which reaches NegInt. The wrapped inner sum is
            // 1_263_433_519, so rhs = -1_263_433_519 and only c0 = -7_326_979_000 matches ->
            // count 1. At long width rhs would be 7_326_501_073 and all three rows would match.
            String negSql = "SELECT count(*) FROM x WHERE c0 <= -((-732674 * c5) + 238927)";
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            assertQuery(negSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertQuery(negSql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");
        });
    }

    @Test
    public void testNestedIntArithmeticWrapsInLongContextViaDivision() throws Exception {
        assertMemoryLeak(() -> {
            // Division sibling of testNestedIntArithmeticWrapsInLongContext, and the operator
            // that makes the width visible: the low 32 bits of a long-width quotient are NOT
            // the wrapped quotient, so a divide cannot be recovered after the fact. Both
            // filters therefore have to wrap the inner product BEFORE dividing.
            execute("CREATE TABLE x (c0 LONG, c5 INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(0, 10000, '2024-01-01T00:00:00.000000Z')," +
                    " (-200_000_000, 10000, '2024-01-01T00:00:00.000001Z')," +
                    " (2_000_000_000, 10000, '2024-01-01T00:00:00.000002Z')");

            // 732674 * 10000 is 7_326_740_000, which overflows int32 and wraps onto
            // -1_263_194_592; dividing by 7 gives an rhs of -180_456_370, so only
            // c0 = -200_000_000 matches -> count 1. At long width the quotient would be
            // 1_046_677_142 and c0 = 0 would match too, giving 2.
            String sql = "SELECT count(*) FROM x WHERE c0 <= ((732674 * c5) / 7)";
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n1\n");

            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertTrue("nested INT division in LONG context must still JIT",
                        factory.usesCompiledFilter());
            }
        });
    }

    @Test
    public void testNestedIntArithmeticWrapsOnLhsAgainstLongRhs() throws Exception {
        assertMemoryLeak(() -> {
            // (c4 INT * (536728 * c8 BYTE)) >= ((-432577L - c8) + c7). BYTE promotes to INT,
            // so the whole left-hand side is an INT expression and wraps at int32 on both
            // paths; the LONG constant on the right decides the width of the comparison
            // alone. The right-hand side is genuinely LONG and stays so.
            execute("CREATE TABLE x (c4 INT, c7 BYTE, c8 BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(-1000000, 0, 127, '2024-01-01T00:00:00.000000Z')," +
                    "  (100000, 0, 100, '2024-01-01T00:00:00.000001Z')," +
                    "      (10, 0,   5, '2024-01-01T00:00:00.000002Z')");

            // Wrapped LHS: 9.7e8, -1.4e9, 2.7e7; rhs ~= -4.3e5, so rows 1 and 3 match. At
            // long width the LHS would be -6.8e13, 5.4e12, 2.7e7 and rows 2 and 3 would match
            // instead - the same count from a different row set, which is why the bigSql case
            // below carries the count that tells the two rules apart.
            String sql = "SELECT count(*) FROM x WHERE (c4 * (536728 * c8)) >= ((-432577L - c8) + c7)";

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n2\n");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertQuery(sql)
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n2\n");

            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertTrue("nested INT*BYTE arithmetic in LONG context must still JIT",
                        factory.usesCompiledFilter());
            }

            // Double NOT around the comparison must not move the widening signal.
            String notSql = "SELECT count(*) FROM x WHERE NOT (NOT ((c4 * (536728 * c8)) >= ((-432577L - c8) + c7)))";
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            assertQuery(notSql)
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n2\n");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertQuery(notSql)
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n2\n");

            // Magnitude-only LONG (no L suffix, > INT_MAX). Every wrapped LHS fits in int32,
            // so none of the three clears 5e9 -> count 0. At long width row 2's 5.4e12 would
            // clear it and the count would be 1.
            String bigSql = "SELECT count(*) FROM x WHERE (c4 * (536728 * c8)) >= (5000000000 - c8 + c7)";
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            assertQuery(bigSql)
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n0\n");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertQuery(bigSql)
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n0\n");
        });
    }

    @Test
    public void testNullTokenDoesNotForceNarrowI64Widening() throws Exception {
        assertMemoryLeak(() -> {
            // null / true / false end in 'l' / 'e' and were folded into
            // a bogus I8 / F8 observation by longConstantTypeCode /
            // floatConstantTypeCode. In a predicate with narrow-int
            // arithmetic the spurious I8 flipped needsNarrowI64Widening,
            // emitted SX_I64 around the column read, and the JIT then
            // computed at long width while the Java filter wrapped at int
            // width.
            execute("CREATE TABLE x (c4 INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "( 1, '2024-01-01T00:00:00.000000Z')," +
                    "( 0, '2024-01-01T00:00:00.000001Z')," +
                    "(-1, '2024-01-01T00:00:00.000002Z')");

            // c4=1: sum overflows int and wraps to INT_NULL, so the row
            // is null in the Java path; the long-widened path stays at
            // 2_147_483_648 and is never null.
            String neqSql = "SELECT count(*) FROM x WHERE c4 + 2_147_483_647 != null";
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            assertQuery(neqSql)
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n2\n");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertQuery(neqSql)
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n2\n");

            try (RecordCursorFactory factory = select(neqSql)) {
                Assert.assertTrue("predicate must remain JIT-compiled after the fix",
                        factory.usesCompiledFilter());
            }

            // Same shape with =: only the wrap-to-null row matches.
            String eqSql = "SELECT count(*) FROM x WHERE c4 + 2_147_483_647 = null";
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            assertQuery(eqSql)
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n1\n");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertQuery(eqSql)
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n1\n");

            // Cover true / false in the same JIT'd filter as wrapping
            // int arithmetic to guard floatConstantTypeCode too.
            execute("CREATE TABLE y (c4 INT, b BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO y VALUES " +
                    "( 1, true,  '2024-01-01T00:00:00.000000Z')," +
                    "( 0, true,  '2024-01-01T00:00:00.000001Z')," +
                    "(-1, false, '2024-01-01T00:00:00.000002Z')");

            String orSql = "SELECT count(*) FROM y WHERE (c4 + 2_147_483_647 != 0) OR (b = true)";
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            assertQuery(orSql)
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n3\n");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            assertQuery(orSql)
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns("count\n3\n");
        });
    }

    @Test
    public void testNonEqualityNullCompareWithVarSizeColumnRejectedByJit() throws Exception {
        assertMemoryLeak(() -> {
            // The fuzzer surfaced JIT-vs-Java divergences on queries like
            //   WHERE null >= v AND d <= 0.7    (v VARCHAR)
            // The IR serializer's ensureOnlyVarSizeHeaderChecks() fired only when
            // both operands were var-size, but serializeNull() emitted the null
            // sentinel as an I8/I4 IMM. So <varsize_col> >= null was accepted by
            // the JIT path and the native kernel produced an arbitrary integer
            // comparison against the var-size header that diverged from the Java
            // filter's evaluation. Now JIT must reject any non-equality binary
            // operator that touches a var-size operand and fall back to the Java
            // filter, leaving IS NULL / IS NOT NULL (which lower to EQ/NE
            // against the NULL header IMM) on the JIT path.
            execute("CREATE TABLE x (v VARCHAR, s STRING, b BINARY, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES ('a', 'a', null, 0.1, '2024-01-01T00:00:00.000000Z')," +
                    " ('b', 'b', null, 0.2, '2024-01-01T00:00:01.000000Z')," +
                    " ('c', 'c', null, 0.3, '2024-01-01T00:00:02.000000Z')," +
                    " ('d', 'd', null, 0.4, '2024-01-01T00:00:03.000000Z')," +
                    " ('e', 'e', null, 0.5, '2024-01-01T00:00:04.000000Z')");

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);

            // Non-equality comparisons against NULL with a var-size column on
            // either side must not JIT.
            String[] nonJitQueries = {
                    "SELECT count(*) FROM x WHERE null >= v AND d <= 0.7",
                    "SELECT count(*) FROM x WHERE v >= null AND d <= 0.7",
                    "SELECT count(*) FROM x WHERE v < null",
                    "SELECT count(*) FROM x WHERE null > s",
            };
            for (String q : nonJitQueries) {
                try (RecordCursorFactory factory = select(q)) {
                    Assert.assertFalse("must not JIT: " + q, factory.usesCompiledFilter());
                }
            }

            // var-size IS NULL / IS NOT NULL must continue to JIT (these lower
            // to EQ/NE against the NULL header IMM, which is the only legitimate
            // var-size operand the JIT runtime supports). BINARY non-equality vs
            // NULL is rejected at SQL parse time before reaching the JIT path,
            // so only IS [NOT] NULL is meaningful to assert here for it.
            String[] jitQueries = {
                    "SELECT count(*) FROM x WHERE v IS NULL",
                    "SELECT count(*) FROM x WHERE v IS NOT NULL",
                    "SELECT count(*) FROM x WHERE s IS NULL",
                    "SELECT count(*) FROM x WHERE s IS NOT NULL",
                    "SELECT count(*) FROM x WHERE b IS NULL",
                    "SELECT count(*) FROM x WHERE b IS NOT NULL",
            };
            for (String q : jitQueries) {
                try (RecordCursorFactory factory = select(q)) {
                    Assert.assertTrue("must JIT: " + q, factory.usesCompiledFilter());
                }
            }
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

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
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

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
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

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
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
    public void testSingleStringInMatchesWholeInterval() throws Exception {
        // InTimestampTimestampFunctionFactory treats an IN whose only element is a string as an
        // interval search on a DATE, TIMESTAMP or TIMESTAMP_NS key. The JIT used to compare the
        // key with the start of the interval only.
        assertMemoryLeak(() -> {
            createTemporalInTable();
            for (String c : new String[]{"dt", "tm", "tn"}) {
                assertFilterWithAndWithoutJit(c + " IN ('2024-01-02')", "k\n1\n2\n", true);
                assertFilterWithAndWithoutJit(c + " IN '2024-01-02'", "k\n1\n2\n", true);
                assertFilterWithAndWithoutJit(c + " NOT IN ('2024-01-02')", "k\n3\n4\n5\n", true);
                assertFilterWithAndWithoutJit(c + " IN ('2024-01')", "k\n1\n2\n3\n5\n", true);
                assertFilterWithAndWithoutJit(c + " IN ('2024-01-02') AND k > 1", "k\n2\n", true);
                assertFilterWithAndWithoutJit(c + " IN ('2024-01-02') OR k = 3", "k\n1\n2\n3\n", true);
                assertFilterWithAndWithoutJit(c + " IN ('2024-01-02T00:00:00.000000Z')", "k\n2\n", true);
                assertFilterWithAndWithoutJit(c + " IN ('2024-01-02T00:00:00.000500')", "k\n", true);
                assertFilterWithAndWithoutJit(c + " IN ('2024-01-02T12:00;1m;1d;2')", "k\n1\n", true);
                assertFilterWithAndWithoutJit(c + " IN ('[2024-01-02,2024-01-03]')", "k\n1\n2\n3\n", true);
                // More intervals than the IN list threshold allows run on the Java filter.
                assertFilterWithAndWithoutJit(c + " IN ('2024-01-02T12:00;1m;-1d;11')", "k\n1\n", false);
                assertFilterWithAndWithoutJit(c + " IN (NULL)", "k\n4\n", true);
                // Two or more elements are points.
                assertFilterWithAndWithoutJit(c + " IN ('2024-01-02', '2024-01-03')", "k\n2\n3\n", true);
            }
            assertFilterWithAndWithoutJit("tm IN ('2024-01-02T12') OR dt IN ('2024-01-03') OR tn IN ('2024-01-01')", "k\n1\n3\n5\n", true);
            // The interval scan takes an IN on the designated timestamp only from an AND chain, so under
            // OR the JIT compiles it.
            assertFilterWithAndWithoutJit("ts IN ('2024-01-01T01') OR k = 5", "k\n2\n5\n", true);
            assertFilterWithAndWithoutJit("ts NOT IN ('2024-01-01T01') OR k = 2", "k\n1\n2\n3\n4\n5\n", true);
            // The Java filter runs an interval search over a timestamp expression, which the JIT used
            // to compile as an equality.
            assertFilterWithAndWithoutJit("(tm + 1) IN ('2024-01-02')", "k\n1\n2\n5\n", false);
            assertFilterWithAndWithoutJit("(tn + 1) IN ('2024-01-02')", "k\n1\n2\n5\n", false);
        });
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

            assertQuery("select s from test where s <  'C'")
                    .noLeakCheck()
                    .returns("s\nB\nA\n");
            assertQuery("select s from test where s <= 'C'")
                    .noLeakCheck()
                    .returns("s\nC\nB\nA\n");
            assertQuery("select s from test where s >  'C'")
                    .noLeakCheck()
                    .returns("s\n");
            assertQuery("select s from test where s >= 'C'")
                    .noLeakCheck()
                    .returns("s\nC\n");

            assertQuery("select s from test where s <  'B'")
                    .noLeakCheck()
                    .returns("s\nA\n");
            assertQuery("select s from test where s <= 'B'")
                    .noLeakCheck()
                    .returns("s\nB\nA\n");
            assertQuery("select s from test where s >  'B'")
                    .noLeakCheck()
                    .returns("s\nC\n");
            assertQuery("select s from test where s >= 'B'")
                    .noLeakCheck()
                    .returns("s\nC\nB\n");

            assertQuery("select s from test where s <  'A'")
                    .noLeakCheck()
                    .returns("s\n");
            assertQuery("select s from test where s <= 'A'")
                    .noLeakCheck()
                    .returns("s\nA\n");
            assertQuery("select s from test where s >  'A'")
                    .noLeakCheck()
                    .returns("s\nC\nB\n");
            assertQuery("select s from test where s >= 'A'")
                    .noLeakCheck()
                    .returns("s\nC\nB\nA\n");

            assertQuery("select s from test where s <  'Z'")
                    .noLeakCheck()
                    .returns("s\nC\nB\nA\n");
            assertQuery("select s from test where s <= 'Z'")
                    .noLeakCheck()
                    .returns("s\nC\nB\nA\n");
            assertQuery("select s from test where s >  'Z'")
                    .noLeakCheck()
                    .returns("s\n");
            assertQuery("select s from test where s >= 'Z'")
                    .noLeakCheck()
                    .returns("s\n");

            assertQuery("select s from test where s <  null")
                    .noLeakCheck()
                    .returns("s\n");
        });
    }

    @Test
    public void testSymbolConstantSpelling() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v INT, s SYMBOL) TIMESTAMP(ts)");
            assertSymbolFilter("s = 'TRUE'", "v\n1\n");
            assertSymbolFilter("s = 'False'", "v\n3\n");
            assertSymbolFilter("s = '''x'''", "v\n5\n");
            assertSymbolFilter("s != 'TRUE'", "v\n2\n3\n4\n5\n6\n");
            assertSymbolFilter("s IN ('TRUE', '''x''')", "v\n1\n5\n");
        });
    }

    @Test
    public void testSymbolInListExceedingBindVarMemoryFallsBackToJavaFilter() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_JIT_BIND_VARS_MEMORY_PAGE_SIZE, 1024);
            setProperty(PropertyKey.CAIRO_SQL_JIT_BIND_VARS_MEMORY_MAX_PAGES, 1);
            setProperty(PropertyKey.CAIRO_SQL_JIT_MAX_IN_LIST_SIZE_THRESHOLD, 128);
            execute("CREATE TABLE t AS (SELECT ('s' || x)::SYMBOL s, x::TIMESTAMP ts FROM long_sequence(66)) TIMESTAMP(ts) PARTITION BY DAY");
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            final StringBuilder symbols = new StringBuilder();
            for (int i = 1; i <= 65; i++) {
                if (i > 1) {
                    symbols.append(", ");
                }
                symbols.append("'s").append(i).append('\'');
            }
            assertQuery("SELECT count() FROM t WHERE s IN (" + symbols + ")")
                    .noRandomAccess().expectSize()
                    .withPlanContaining("Async Filter")
                    .returns("count\n65\n");
        });
    }

    @Test
    public void testTemporalInUnquotedIntegerMatchesJava() throws Exception {
        // InTimestampTimestampFunctionFactory reads an unquoted integer as a point in the key's
        // unit: 2024 is the 2024th microsecond to a TIMESTAMP, the 2024th nanosecond to a
        // TIMESTAMP_NS, and no DATE holds it, since a DATE compares in whole milliseconds. The JIT
        // used to parse it as a tick expression, where four digits are a year.
        assertMemoryLeak(() -> {
            createTemporalInTable();
            execute("""
                    INSERT INTO x VALUES
                    (6, '1970-01-01T00:00:02.024Z', '1970-01-01T00:00:00.002024Z', '1970-01-01T00:00:00.000002024Z', '2024-01-01T05:00:00.000000Z')
                    """);
            // A quoted string is an interval, and NULL a point, in every engine.
            for (String c : new String[]{"dt", "tm", "tn"}) {
                assertFilterWithAndWithoutJit(c + " IN '2024'", "k\n1\n2\n3\n5\n", true);
                assertFilterWithAndWithoutJit(c + " NOT IN '2024'", "k\n4\n6\n", true);
                assertFilterWithAndWithoutJit(c + " IN NULL", "k\n4\n", true);
            }
            // The parenthesised form compiles the same point as the Java filter.
            assertFilterWithAndWithoutJit("tm IN (2024)", "k\n6\n", true);
            assertFilterWithAndWithoutJit("tn IN (2024)", "k\n6\n", true);
            // A string bind variable runs on the Java filter.
            bindVariableService.clear();
            bindVariableService.setStr("s", "2024");
            assertFilterWithAndWithoutJit("tm IN :s", "k\n1\n2\n3\n5\n", false);

            assertFilterWithAndWithoutJit("dt IN 2024", "k\n", false);
            assertFilterWithAndWithoutJit("dt IN 1970", "k\n", false);
            assertFilterWithAndWithoutJit("NOT (dt IN 2024)", "k\n1\n2\n3\n4\n5\n6\n", false);
            assertFilterWithAndWithoutJit("dt NOT IN 2024", "k\n1\n2\n3\n4\n5\n6\n", false);
            assertFilterWithAndWithoutJit("dt IN 2024 OR k = 1", "k\n1\n", false);
            assertFilterWithAndWithoutJit("dt IN 2024_000", "k\n6\n", false);
            for (String c : new String[]{"tm", "tn"}) {
                assertFilterWithAndWithoutJit(c + " IN 2024", "k\n6\n", false);
                assertFilterWithAndWithoutJit(c + " IN 1970", "k\n", false);
                assertFilterWithAndWithoutJit("NOT (" + c + " IN 2024)", "k\n1\n2\n3\n4\n5\n", false);
                assertFilterWithAndWithoutJit(c + " NOT IN 2024", "k\n1\n2\n3\n4\n5\n", false);
                assertFilterWithAndWithoutJit(c + " IN 2024 OR k = 1", "k\n1\n6\n", false);
            }
            // The interval scan takes an IN on the designated timestamp only from an AND chain, so
            // under OR the IN stays in the filter.
            assertFilterWithAndWithoutJit("ts IN 2024 OR k = 1", "k\n1\n", false);
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

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            assertSqlRunWithJit(query);

            // check JIT uses both hi and lo for comparison
            // use a dummy lo
            bindVariableService.clear();
            bindVariableService.setUuid(0, 0, uuid.getHi());
            String expectedEmpty = """
                    u	ts
                    """;
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedEmpty);
            assertSqlRunWithJit(query);

            // use a dummy hi
            bindVariableService.clear();
            bindVariableService.setUuid(0, uuid.getLo(), 0);
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedEmpty);
            assertSqlRunWithJit(query);

            // switch hi and lo
            bindVariableService.clear();
            bindVariableService.setUuid(0, uuid.getHi(), uuid.getLo());
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedEmpty);
            assertSqlRunWithJit(query);

            // null uuid
            execute("insert into x values (null, '2020')");
            bindVariableService.clear();
            bindVariableService.setUuid(0, Numbers.LONG_NULL, Numbers.LONG_NULL);
            String expectedWithNull = """
                    u	ts
                    	2020-01-01T00:00:00.000000Z
                    """;
            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedWithNull);
            assertSqlRunWithJit(query);
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

            assertQuery(query)
                    .inferTimestamp()
                    .inferRandomAccess()
                    .sizeMayVary()
                    .noLeakCheck()
                    .returns(expected);
            assertSqlRunWithJit(query);
        });
    }

    private static String allDayKeysExcept(int... excluded) {
        final StringBuilder sb = new StringBuilder("k\n");
        for (int k = 0; k <= 14; k++) {
            boolean isExcluded = false;
            for (int e : excluded) {
                isExcluded |= e == k;
            }
            if (!isExcluded) {
                sb.append(k).append('\n');
            }
        }
        return sb.toString();
    }

    // 01:00 UTC on the given day of January 2024. 2024-01-01 is a Monday.
    private static long jan2024(int day) {
        return Micros.toMicros(2024, 1, day, 1, 0);
    }

    private static String keys(int... keys) {
        final StringBuilder sb = new StringBuilder("k\n");
        for (int k : keys) {
            sb.append(k).append('\n');
        }
        return sb.toString();
    }

    private void assertCountFollowsClock(String query, boolean isJitExpected, long micros1, long count1, long micros2, long count2) throws Exception {
        assertFollowsClock(query, isJitExpected, q -> q.expectSize().noRandomAccess(), micros1, "count\n" + count1 + "\n", micros2, "count\n" + count2 + "\n");
    }

    private void assertFilterFollowsClock(String filter, boolean isJitExpected, long micros1, String expected1, long micros2, String expected2) throws Exception {
        assertFollowsClock("SELECT k FROM x WHERE " + filter, isJitExpected, micros1, expected1, micros2, expected2);
    }

    private void assertFilterWithAndWithoutJit(String filter, String expected, boolean isJitExpected) throws Exception {
        assertFilterWithAndWithoutJit(filter, expected, isJitExpected, UnaryOperator.identity());
    }

    // The options configure the factory properties that QueryAssertion checks.
    private void assertFilterWithAndWithoutJit(
            String filter,
            String expected,
            boolean isJitExpected,
            UnaryOperator<QueryAssertion> options
    ) throws Exception {
        final String query = "SELECT k FROM x WHERE " + filter;
        try {
            for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_FORCE_SCALAR, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(mode);
                options.apply(assertQuery(query).noLeakCheck()).returns(expected);
                try (RecordCursorFactory factory = select(query)) {
                    Assert.assertEquals(query + ", jit mode " + mode, isJitExpected && mode != SqlJitMode.JIT_MODE_DISABLED, factory.usesCompiledFilter());
                }
            }
        } finally {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
        }
    }

    private void assertFollowsClock(String query, boolean isJitExpected, long micros1, String expected1, long micros2, String expected2) throws Exception {
        assertFollowsClock(query, isJitExpected, UnaryOperator.identity(), micros1, expected1, micros2, expected2);
    }

    // Compiles the query once at micros1 and runs it at micros1 and at micros2, as a plan from the
    // query cache runs. A filter that holds the bounds of the compile time fails the second run. It
    // does so in every JIT mode, and isJitExpected tells whether the factory compiles its filter with
    // the JIT when the mode allows it. The options configure the factory properties that
    // QueryAssertion checks.
    private void assertFollowsClock(
            String query,
            boolean isJitExpected,
            UnaryOperator<QueryAssertion> options,
            long micros1,
            String expected1,
            long micros2,
            String expected2
    ) throws Exception {
        try {
            for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_FORCE_SCALAR, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(mode);
                final String label = query + ", jit mode " + mode;
                try {
                    setCurrentMicros(micros1);
                    try (RecordCursorFactory factory = select(query)) {
                        Assert.assertEquals("uses compiled filter", isJitExpected && mode != SqlJitMode.JIT_MODE_DISABLED, factory.usesCompiledFilter());
                        options.apply(assertFactory(factory).withContext(sqlExecutionContext)).returns(expected1);
                        setCurrentMicros(micros2);
                        options.apply(assertFactory(factory).withContext(sqlExecutionContext)).returns(expected2);
                    }
                    // A plan compiled at micros2 returns the same rows.
                    options.apply(assertQuery(query).noLeakCheck()).returns(expected2);
                } catch (AssertionError e) {
                    throw new AssertionError(label, e);
                }
            }
        } finally {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
        }
    }

    private void assertSymbolFilter(String filter, String expected) throws Exception {
        final String query = "SELECT v FROM x WHERE " + filter;
        execute("TRUNCATE TABLE x");
        assertQuery(query).noLeakCheck().mutateWith("""
                INSERT INTO x VALUES
                (1, 1, 'TRUE'),
                (2, 2, 'true'),
                (3, 3, 'False'),
                (4, 4, 'false'),
                (5, 5, '''x'''),
                (6, 6, 'x')
                """).returns("v\n", expected);
        assertSqlRunWithJit(query);
    }

    private void assertWindowJoinUsesCompiledFilter(String query, boolean isJitExpected) throws Exception {
        try (RecordCursorFactory factory = select(query)) {
            RecordCursorFactory base = factory;
            while (!(base instanceof AsyncWindowJoinFastRecordCursorFactory windowJoin)) {
                base = base.getBaseFactory();
            }
            Assert.assertEquals(query, isJitExpected, windowJoin.getAtom().getCompiledMasterFilter() != null);
        }
    }

    // A timestamp that is not designated, so the row filter runs every IN on it. Row 1 is a Friday,
    // 2 a Saturday, 3 the Monday 2024-01-08, 4 a Tuesday, 5 a Sunday and 6 the Monday 2024-09-02.
    private void createDayFilterTable() throws SqlException {
        execute("CREATE TABLE x (k INT, ts TIMESTAMP)");
        execute("""
                INSERT INTO x VALUES
                (1, '2024-01-05T12:00:00.000000Z'),
                (2, '2024-01-06T12:00:00.000000Z'),
                (3, '2024-01-08T12:00:00.000000Z'),
                (4, '2024-01-09T04:30:00.000000Z'),
                (5, '2024-09-01T12:00:00.000000Z'),
                (6, '2024-09-02T12:00:00.000000Z')
                """);
        // $today is the Monday 2024-01-08
        setCurrentMicros(Micros.toMicros(2024, 1, 8, 1, 0));
    }

    private void createDaysJoinSlaveTable() throws SqlException {
        execute("CREATE TABLE p (s SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO p VALUES
                ('a', 1.0, '2023-12-31T00:00:00.000000Z'),
                ('b', 2.0, '2023-12-31T00:00:00.000000Z')
                """);
    }

    // One row a day at noon from Monday 2024-01-01 (k = 1) to Sunday 2024-01-14 (k = 14), with s = 'a'
    // for odd k and 'b' for even k, and a row of NULLs (k = 0) at the designated timestamp
    // 2023-12-31T12:00. The designated timestamp holds the same instants as dt, tm and tn.
    private void createTemporalDaysTable() throws SqlException {
        execute("CREATE TABLE x (k INT, s SYMBOL, dt DATE, tm TIMESTAMP, tn TIMESTAMP_NS, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO x
                SELECT
                    k,
                    CASE WHEN k = 0 THEN NULL WHEN k % 2 = 0 THEN 'b' ELSE 'a' END,
                    CASE WHEN k > 0 THEN t::DATE END,
                    CASE WHEN k > 0 THEN t END,
                    CASE WHEN k > 0 THEN t::TIMESTAMP_NS END,
                    t
                FROM (SELECT (x - 1)::INT k, dateadd('d', (x - 1)::INT, '2023-12-31T12:00:00.000000Z'::TIMESTAMP) t FROM long_sequence(15))
                """);
    }

    private void createTemporalInTable() throws SqlException {
        execute("CREATE TABLE x (k INT, dt DATE, tm TIMESTAMP, tn TIMESTAMP_NS, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO x VALUES
                (1, '2024-01-02T12:00:00.000Z', '2024-01-02T12:00:00.000000Z', '2024-01-02T12:00:00.000000000Z', '2024-01-01T00:00:00.000000Z'),
                (2, '2024-01-02T00:00:00.000Z', '2024-01-02T00:00:00.000000Z', '2024-01-02T00:00:00.000000000Z', '2024-01-01T01:00:00.000000Z'),
                (3, '2024-01-03T00:00:00.000Z', '2024-01-03T00:00:00.000000Z', '2024-01-03T00:00:00.000000000Z', '2024-01-01T02:00:00.000000Z'),
                (4, NULL, NULL, NULL, '2024-01-01T03:00:00.000000Z'),
                (5, '2024-01-01T23:59:59.999Z', '2024-01-01T23:59:59.999999Z', '2024-01-01T23:59:59.999999999Z', '2024-01-01T04:00:00.000000Z')
                """);
    }

    // Two keys of each temporal type, k1 and k2 for k in d (DATE), t (TIMESTAMP) and n (TIMESTAMP_NS).
    // Row 4 has a NULL second key, and row 5 two NULL keys.
    private void createTwoTemporalKeysTable() throws SqlException {
        execute("CREATE TABLE x (k INT, d1 DATE, d2 DATE, t1 TIMESTAMP, t2 TIMESTAMP, n1 TIMESTAMP_NS, n2 TIMESTAMP_NS, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO x VALUES
                (1, '2024-01-10T10:00:00.000Z', '2024-01-12T10:00:00.000Z', '2024-01-10T10:00:00.000000Z', '2024-01-12T10:00:00.000000Z',
                    '2024-01-10T10:00:00.000000000Z', '2024-01-12T10:00:00.000000000Z', '2024-01-10T10:00:00.000000Z'),
                (2, '2024-01-30T10:00:00.000Z', '2024-02-02T10:00:00.000Z', '2024-01-30T10:00:00.000000Z', '2024-02-02T10:00:00.000000Z',
                    '2024-01-30T10:00:00.000000000Z', '2024-02-02T10:00:00.000000000Z', '2024-01-30T10:00:00.000000Z'),
                (3, '2024-02-05T10:00:00.000Z', '2024-02-06T10:00:00.000Z', '2024-02-05T10:00:00.000000Z', '2024-02-06T10:00:00.000000Z',
                    '2024-02-05T10:00:00.000000000Z', '2024-02-06T10:00:00.000000000Z', '2024-02-05T10:00:00.000000Z'),
                (4, '2024-02-27T10:00:00.000Z', NULL, '2024-02-27T10:00:00.000000Z', NULL,
                    '2024-02-27T10:00:00.000000000Z', NULL, '2024-02-27T10:00:00.000000Z'),
                (5, NULL, NULL, NULL, NULL, NULL, NULL, '2024-02-28T10:00:00.000000Z')
                """);
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

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
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

            assertQuery(query)
                    .noLeakCheck()
                    .returnsOnce(expected);
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

            assertQuery(query)
                    .noLeakCheck()
                    .ddl(ddl)
                    .timestamp("k")
                    .returns(expected);
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

            assertQuery(query)
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected);
            assertSqlRunWithJit(query);
        });
    }
}
