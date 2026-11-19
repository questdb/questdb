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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.TimestampPrecisionUse;
import io.questdb.griffin.engine.functions.date.DateTruncFunctionFactory;
import io.questdb.griffin.engine.functions.date.DayOfMonthFunctionFactory;
import io.questdb.griffin.engine.functions.date.DayOfWeekFunctionFactory;
import io.questdb.griffin.engine.functions.date.DayOfWeekSundayFirstFunctionFactory;
import io.questdb.griffin.engine.functions.date.DaysPerMonthFunctionFactory;
import io.questdb.griffin.engine.functions.date.ExtractFromTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.date.HourOfDayFunctionFactory;
import io.questdb.griffin.engine.functions.date.IsEndOfMonthFunctionFactory;
import io.questdb.griffin.engine.functions.date.IsLeapYearFunctionFactory;
import io.questdb.griffin.engine.functions.date.MicrosOfMillsFunctionFactory;
import io.questdb.griffin.engine.functions.date.MillisOfSecondFunctionFactory;
import io.questdb.griffin.engine.functions.date.MinuteOfHourFunctionFactory;
import io.questdb.griffin.engine.functions.date.MonthOfYearFunctionFactory;
import io.questdb.griffin.engine.functions.date.NanosOfMicrosFunctionFactory;
import io.questdb.griffin.engine.functions.date.SecondOfMinuteFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampAddFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampAddWithTimezoneFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampCeilFunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampFloorFunctionFactory;
import io.questdb.griffin.engine.functions.date.ToTimezoneTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.date.ToUTCTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.date.WeekOfYearFunctionFactory;
import io.questdb.griffin.engine.functions.date.YearFunctionFactory;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.griffin.TimestampPrecisionUse.DEPENDENT;
import static io.questdb.griffin.TimestampPrecisionUse.INVARIANT;
import static io.questdb.griffin.TimestampPrecisionUse.PRESERVING;

/**
 * Pins how each timestamp factory answers {@link FunctionFactory#getTimestampPrecisionUse} for the bound first
 * argument of a call. The expectations follow the units the factories document: a calendar field of an instant is the
 * same at every precision (INVARIANT); truncation to a calendar unit, a ceiling and a time zone shift return the same
 * instant (PRESERVING); {@code dateadd()} shifts by its period, which every precision counts exactly except a
 * nanosecond ({@link TimestampPrecisionUse#ofShift}); {@code timestamp_floor()} floors to a grid of its stride and
 * unit, which is the same instant at microsecond and nanosecond precision except for a nanosecond stride that neither
 * divides nor is a multiple of a microsecond ({@link TimestampPrecisionUse#ofFloor}). A unit is the characters of a
 * STRING, VARCHAR, SYMBOL or CHAR constant; any other first argument leaves the answer DEPENDENT.
 */
public class TimestampPrecisionUseTest {
    private static final String FOUR_BYTE = new String(Character.toChars(0x1D11E));

    @Test
    public void testCalendarFieldsAreInvariant() {
        final FunctionFactory[] factories = {
                new DayOfMonthFunctionFactory(), new DayOfWeekFunctionFactory(), new DayOfWeekSundayFirstFunctionFactory(),
                new DaysPerMonthFunctionFactory(), new ExtractFromTimestampFunctionFactory(), new HourOfDayFunctionFactory(),
                new IsEndOfMonthFunctionFactory(), new IsLeapYearFunctionFactory(), new MicrosOfMillsFunctionFactory(),
                new MillisOfSecondFunctionFactory(), new MinuteOfHourFunctionFactory(), new MonthOfYearFunctionFactory(),
                new NanosOfMicrosFunctionFactory(), new SecondOfMinuteFunctionFactory(), new WeekOfYearFunctionFactory(),
                new YearFunctionFactory()
        };
        for (FunctionFactory factory : factories) {
            assertEveryArgument(factory, INVARIANT);
        }
    }

    @Test
    public void testDateAddPeriods() {
        // the period letters dateadd() accepts, in either case it accepts them
        final String preserving = "yMwdhHmsTUu";
        final FunctionFactory[] factories = {new TimestampAddFunctionFactory(), new TimestampAddWithTimezoneFunctionFactory()};
        for (FunctionFactory factory : factories) {
            for (int i = 0, n = preserving.length(); i < n; i++) {
                assertUnit(factory, String.valueOf(preserving.charAt(i)), PRESERVING);
            }
            // microsecond precision truncates a number of nanoseconds that is not a whole number of microseconds
            assertUnit(factory, "n", DEPENDENT);
            // only a single character spells a period
            assertText(factory, "dd", DEPENDENT);
            assertText(factory, "day", DEPENDENT);
            assertText(factory, "1d", DEPENDENT);
            assertText(factory, "", DEPENDENT);
            // a 4-byte character is two UTF-16 characters
            assertText(factory, FOUR_BYTE, DEPENDENT);
            // letters dateadd() rejects never bind, so the rewrite never asks about them; the period still follows
            // ofShift(): a letter other than 'n' shifts as a microsecond or a longer unit would
            assertUnit(factory, "N", PRESERVING);
            assertUnit(factory, "x", PRESERVING);
            assertUnit(factory, "é", PRESERVING);
            // a CHAR constant holds one character even when that is the NULL character
            assertUse(factory, "CHAR NULL", PRESERVING, new ConstantExpression().ofChar((char) 0, 0));
            assertNoUnit(factory);
        }
    }

    @Test
    public void testDefaultIsDependent() {
        final FunctionFactory factory = new FunctionFactory() {
            @Override
            public String getSignature() {
                return "f(N)";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
                throw new UnsupportedOperationException();
            }
        };
        assertEveryArgument(factory, DEPENDENT);
    }

    @Test
    public void testSameInstantAtEveryPrecision() {
        final FunctionFactory[] factories = {
                new DateTruncFunctionFactory(), new TimestampCeilFunctionFactory(), new ToTimezoneTimestampFunctionFactory(),
                new ToUTCTimestampFunctionFactory()
        };
        for (FunctionFactory factory : factories) {
            assertEveryArgument(factory, PRESERVING);
            // date_trunc() and timestamp_ceil() answer alike for every unit, the nanosecond included
            for (String unit : new String[]{"nanosecond", "microsecond", "day", "n", "U", "d", "M", "y"}) {
                assertText(factory, unit, PRESERVING);
            }
        }
    }

    @Test
    public void testTimestampFloorUnits() {
        final FunctionFactory factory = new TimestampFloorFunctionFactory();
        // every unit but the nanosecond floors to the same instant at every precision, with or without a stride
        final String units = "yMwdhmsTU";
        for (int i = 0, n = units.length(); i < n; i++) {
            final String unit = String.valueOf(units.charAt(i));
            assertUnit(factory, unit, PRESERVING);
            assertText(factory, "1" + unit, PRESERVING);
            assertText(factory, "7" + unit, PRESERVING);
            assertText(factory, "1500" + unit, PRESERVING);
        }
        // a nanosecond stride floors alike at microsecond and nanosecond precision when it divides a microsecond or is
        // a whole number of microseconds
        assertUnit(factory, "n", PRESERVING);
        for (String stride : new String[]{"1", "2", "4", "5", "8", "10", "100", "125", "250", "500", "1000", "2000", "1_000", "1000000", "2147483000"}) {
            assertText(factory, stride + "n", PRESERVING);
        }
        for (String stride : new String[]{"3", "7", "999", "1001", "1500", "2147483647"}) {
            assertText(factory, stride + "n", DEPENDENT);
        }
        // no positive stride: zero, a negative number, a stride that is not a number and one that overflows INT
        for (String stride : new String[]{"0", "-1", "-1000", "x", "1.5", "2147483648"}) {
            assertText(factory, stride + "n", DEPENDENT);
        }
        // the unit is the last character, so case tells a month (M) from a minute (m); units timestamp_floor()
        // rejects never bind, and their answer still follows ofFloor(): a unit other than 'n' floors as ofShift()
        assertUnit(factory, "N", PRESERVING);
        assertText(factory, "3N", PRESERVING);
        assertText(factory, "x", PRESERVING);
        // a CHAR constant holds one character even when that is the NULL character
        assertUse(factory, "CHAR NULL", PRESERVING, new ConstantExpression().ofChar((char) 0, 0));
        assertText(factory, "", DEPENDENT);
        assertNoUnit(factory);
    }

    // The same answer for every first argument, unit or not.
    private static void assertEveryArgument(FunctionFactory factory, TimestampPrecisionUse expected) {
        final String name = factory.getClass().getSimpleName();
        Assert.assertSame(name, expected, factory.getTimestampPrecisionUse(new ColumnExpression().of(1, ColumnType.TIMESTAMP_MICRO, 0)));
        Assert.assertSame(name, expected, factory.getTimestampPrecisionUse(new ColumnExpression().of(1, ColumnType.TIMESTAMP_NANO, 0)));
        Assert.assertSame(name, expected, factory.getTimestampPrecisionUse(new ColumnExpression().of(1, ColumnType.DATE, 0)));
        Assert.assertSame(name, expected, factory.getTimestampPrecisionUse(new ColumnExpression().of(1, ColumnType.STRING, 0)));
        Assert.assertSame(name, expected, factory.getTimestampPrecisionUse(bindVariable(ColumnType.TIMESTAMP_MICRO)));
        Assert.assertSame(name, expected, factory.getTimestampPrecisionUse(new ConstantExpression().ofTimestamp(0, ColumnType.TIMESTAMP_MICRO, 0)));
        Assert.assertSame(name, expected, factory.getTimestampPrecisionUse(new ConstantExpression().ofNull(0)));
        Assert.assertSame(name, expected, factory.getTimestampPrecisionUse(new ConstantExpression().ofString("n", 0)));
        Assert.assertSame(name, expected, factory.getTimestampPrecisionUse(new ConstantExpression().ofString("d", 0)));
        Assert.assertSame(name, expected, factory.getTimestampPrecisionUse(new ConstantExpression().ofChar('n', 0)));
        Assert.assertSame(name, expected, factory.getTimestampPrecisionUse(new ConstantExpression().ofInt(1, 0)));
    }

    // Every first argument that holds no unit: NULL text of each text type, the NULL literal, constants of other
    // types and arguments that are not constants.
    private static void assertNoUnit(FunctionFactory factory) {
        assertUse(factory, "STRING NULL", DEPENDENT, new ConstantExpression().ofString(null, 0));
        assertUse(factory, "VARCHAR NULL", DEPENDENT, new ConstantExpression().ofVarchar(null, 0));
        assertUse(factory, "SYMBOL NULL", DEPENDENT, new ConstantExpression().ofSymbol(null, 0));
        assertUse(factory, "NULL", DEPENDENT, new ConstantExpression().ofNull(0));
        assertUse(factory, "INT", DEPENDENT, new ConstantExpression().ofInt(1, 0));
        assertUse(factory, "LONG", DEPENDENT, new ConstantExpression().ofLong(1_000L, 0));
        assertUse(factory, "TIMESTAMP", DEPENDENT, new ConstantExpression().ofTimestamp(0, ColumnType.TIMESTAMP_MICRO, 0));
        assertUse(factory, "STRING column", DEPENDENT, new ColumnExpression().of(1, ColumnType.STRING, 0));
        assertUse(factory, "CHAR column", DEPENDENT, new ColumnExpression().of(1, ColumnType.CHAR, 0));
        assertUse(factory, "VARCHAR column", DEPENDENT, new ColumnExpression().of(1, ColumnType.VARCHAR, 0));
        assertUse(factory, "STRING bind variable", DEPENDENT, bindVariable(ColumnType.STRING));
        assertUse(factory, "CHAR bind variable", DEPENDENT, bindVariable(ColumnType.CHAR));
    }

    // The answer for the unit spelled as a STRING, VARCHAR or SYMBOL constant.
    private static void assertText(FunctionFactory factory, String unit, TimestampPrecisionUse expected) {
        assertUse(factory, "STRING '" + unit + '\'', expected, new ConstantExpression().ofString(unit, 0));
        assertUse(factory, "VARCHAR '" + unit + '\'', expected, new ConstantExpression().ofVarchar(new Utf8String(unit), 0));
        assertUse(factory, "SYMBOL '" + unit + '\'', expected, new ConstantExpression().ofSymbol(unit, 0));
    }

    // The answer for the one-character unit spelled as a text or CHAR constant.
    private static void assertUnit(FunctionFactory factory, String unit, TimestampPrecisionUse expected) {
        assertText(factory, unit, expected);
        assertUse(factory, "CHAR '" + unit + '\'', expected, new ConstantExpression().ofChar(unit.charAt(0), 0));
    }

    private static void assertUse(FunctionFactory factory, String argument, TimestampPrecisionUse expected, BoundExpression firstArgument) {
        Assert.assertSame(factory.getClass().getSimpleName() + '(' + argument + ')', expected, factory.getTimestampPrecisionUse(firstArgument));
    }

    private static BindVariableExpression bindVariable(int type) {
        return new BindVariableExpression().of("$1", type, 0, 0);
    }
}
