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

package io.questdb.test.griffin.engine.functions.date;

import io.questdb.cairo.ColumnType;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class ExtractFunctionTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public ExtractFunctionTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void test1997Millennium() throws Exception {
        assertQuery("select extract(millennium from '1997-04-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        2
                        """);

        assertQuery("select extract('millennium' from '1997-04-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        2
                        """);
    }

    @Test
    public void testBeforeEndOfYear() throws Exception {
        assertQuery("select extract(doy from '2022-12-30T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        364
                        """);

        assertQuery("select extract('doy' from '2022-12-30T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        364
                        """);
    }

    @Test
    public void testBeginningOfCentury() throws Exception {
        assertQuery("select extract(century from '2000-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        20
                        """);
    }

    @Test
    public void testCenturyNull() throws Exception {
        assertQuery("select extract(century from null)")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        null
                        """);
    }

    @Test
    public void testComma() throws Exception {
        assertQuery("select extract(hour, '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .fails(15, "Invalid column: hour");
    }

    @Test
    public void testDanglingArg() throws Exception {
        assertQuery("select extract(hour from to_timestamp('2022-03-11T22:00:30.555555Z') table)")
                .fails(69, "dangling literal");
    }

    @Test
    public void testDayNull() throws Exception {
        assertQuery("select extract(day from null)")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        null
                        """);
    }

    @Test
    public void testDecadeNull() throws Exception {
        assertQuery("select extract(decade from null)")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        null
                        """);
    }

    @Test
    public void testDowNull() throws Exception {
        assertQuery("select extract(dow from null)")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        null
                        """);
    }

    @Test
    public void testDoyNull() throws Exception {
        assertQuery("select extract(doy from null)")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        null
                        """);
    }

    @Test
    public void testEndOfLeapYear() throws Exception {
        assertQuery("select extract(doy from '2020-12-31T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        366
                        """);
    }

    @Test
    public void testEndOfYear() throws Exception {
        assertQuery("select extract(doy from '2022-12-31T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        365
                        """);
    }

    @Test
    public void testEpochNull() throws Exception {
        assertQuery("select extract(epoch from null)")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        null
                        """);
    }

    @Test
    public void testFirstCentury() throws Exception {
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertQuery("select extract(century from '0001-01-01T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        1
                        """);
    }

    @Test
    public void testHourNull() throws Exception {
        assertQuery("select extract(hour from null)")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        null
                        """);
    }

    @Test
    public void testIsoDowNull() throws Exception {
        assertQuery("select extract(isodow from null)")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        null
                        """);
    }

    @Test
    public void testIsoYearNull() throws Exception {
        assertQuery("select extract(isoyear from null)")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        null
                        """);
    }

    @Test
    public void testIsoYearWeek1Start() throws Exception {
        assertQuery("select extract(isoyear from '2022-01-03T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        2022
                        """);
    }

    @Test
    public void testIsoYearWeek1StartLeap() throws Exception {
        assertQuery("select extract(isoyear from '2019-12-31T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        2020
                        """);
    }

    @Test
    public void testIsoYearWeek52Start() throws Exception {
        assertQuery("select extract(isoyear from '2022-01-01T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        2021
                        """);
    }

    @Test
    public void testMicrosecondsNull() throws Exception {
        assertQuery("select extract(microseconds from null)")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        null
                        """);
    }

    @Test
    public void testMillenniumNull() throws Exception {
        assertQuery("select extract(millennium from null)")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        null
                        """);
    }

    @Test
    public void testMilliseconds() throws Exception {
        assertQuery("select extract(milliseconds from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        555
                        """);
    }

    @Test
    public void testMillisecondsNull() throws Exception {
        assertQuery("select extract(milliseconds from null)")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        null
                        """);
    }

    @Test
    public void testMillisecondsPreEpoch() throws Exception {
        assertQuery("select extract(milliseconds from '1905-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        555
                        """);
    }

    @Test
    public void testMinuteNull() throws Exception {
        assertQuery("select extract(minute from null)")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        null
                        """);
    }

    @Test
    public void testMissingPart() throws Exception {
        assertQuery("select extract(from to_timestamp('2022-03-11T22:00:30.555555Z'))")
                .fails(15, "Huh? What would you like to extract?");
    }

    @Test
    public void testMonthNull() throws Exception {
        assertQuery("select extract(month from null)")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        null
                        """);
    }

    @Test
    public void testMultipleFrom() throws Exception {
        assertQuery("select extract(hour from from to_timestamp('2022-03-11T22:00:30.555555Z'))")
                .fails(25, "Unnecessary `from`. Typo?");
    }

    @Test
    public void testNanosecondsNull() throws Exception {
        assertQuery("select extract(nanoseconds from null)")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        null
                        """);
    }

    @Test
    public void testNonLiteralPart() throws Exception {
        assertQuery("select extract(1+1 from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .fails(17, "we expect timestamp part here");
    }

    @Test
    public void testNotExtractFrom() throws Exception {
        assertQuery("select something(null from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .fails(22, "dangling literal");
    }

    @Test
    public void testNullFrom() throws Exception {
        assertQuery("select extract(null from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .fails(15, "unsupported timestamp part: null");
    }

    @Test
    public void testQuarterFirst1() throws Exception {
        assertQuery("select extract(quarter from '2022-01-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        1
                        """);
    }

    @Test
    public void testQuarterFirst2() throws Exception {
        assertQuery("select extract(quarter from '2022-04-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        2
                        """);
    }

    @Test
    public void testQuarterLast1() throws Exception {
        assertQuery("select extract(quarter from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        1
                        """);
    }

    @Test
    public void testQuarterLast4() throws Exception {
        assertQuery("select extract(quarter from '2022-12-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        4
                        """);
    }

    @Test
    public void testQuarterNull() throws Exception {
        assertQuery("select extract(quarter from null)")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        null
                        """);
    }

    @Test
    public void testSecondNull() throws Exception {
        assertQuery("select extract(second from null)")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        null
                        """);
    }

    @Test
    public void testSecondPreEpoch() throws Exception {
        assertQuery("select extract(second from '1812-03-11T22:45:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        30
                        """);
    }

    @Test
    public void testStartOfYear() throws Exception {
        assertQuery("select extract(doy from '2022-01-01T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        1
                        """);
    }

    @Test
    public void testUnsupported() throws Exception {
        assertQuery("select extract(timezone from '2022-12-30T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .fails(15, "unsupported timestamp part: timezone");
    }

    @Test
    public void testVanillaCentury() throws Exception {
        assertQuery("select extract(century from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        21
                        """);
    }

    @Test
    public void testVanillaDay() throws Exception {
        assertQuery("select extract(day from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        11
                        """);
    }

    @Test
    public void testVanillaDecade() throws Exception {
        assertQuery("select extract(decade from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        202
                        """);
    }

    @Test
    public void testVanillaDowSaturday() throws Exception {
        assertQuery("select extract(dow from '2022-04-16T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        6
                        """);
    }

    @Test
    public void testVanillaDowSunday() throws Exception {
        assertQuery("select extract(dow from '2022-04-10T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        0
                        """);
    }

    @Test
    public void testVanillaDoy() throws Exception {
        assertQuery("select extract(doy from '2022-04-22T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        112
                        """);
    }

    @Test
    public void testVanillaEpoch() throws Exception {
        assertQuery("select extract(epoch from '2022-04-22T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        1650664830
                        """);
    }

    @Test
    public void testVanillaHour() throws Exception {
        assertQuery("select extract(hour from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        22
                        """);
    }

    @Test
    public void testVanillaIsoDowMonday() throws Exception {
        assertQuery("select extract(isodow from '2022-04-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        1
                        """);
    }

    @Test
    public void testVanillaIsoDowSunday() throws Exception {
        assertQuery("select extract(isodow from '2022-04-10T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        7
                        """);
    }

    @Test
    public void testVanillaMicroseconds() throws Exception {
        assertQuery("select extract(microseconds from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        555555
                        """);
    }

    @Test
    public void testVanillaMicrosecondsPreEpoch() throws Exception {
        assertQuery("select extract(microseconds from '1917-03-11T22:00:40.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        555555
                        """);
    }

    @Test
    public void testVanillaMillennium() throws Exception {
        assertQuery("select extract(millennium from '2022-04-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        3
                        """);
    }

    @Test
    public void testVanillaMinute() throws Exception {
        assertQuery("select extract(minute from '2022-03-11T22:45:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        45
                        """);
    }

    @Test
    public void testVanillaMonth() throws Exception {
        assertQuery("select extract(month from '2022-03-11T22:45:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        3
                        """);
    }

    @Test
    public void testVanillaNanoseconds() throws Exception {
        assertQuery("select extract(nanoseconds from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("extract\n" +
                        (ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "555555000\n" : "555555123\n"));
    }

    @Test
    public void testVanillaNanosecondsPreEpoch() throws Exception {
        assertQuery("select extract(nanoseconds from '1917-03-11T22:00:40.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("extract\n" +
                        (ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? "555555000\n" : "555555123\n"));
    }

    @Test
    public void testVanillaSecond() throws Exception {
        assertQuery("select extract(second from '2022-03-11T22:45:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        30
                        """);
    }

    @Test
    public void testVanillaWeek() throws Exception {
        assertQuery("select extract(week from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        10
                        """);
    }

    @Test
    public void testVanillaYear() throws Exception {
        assertQuery("select extract(year from '2022-03-11T22:45:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        2022
                        """);
    }

    @Test
    public void testVanillaYearPreEpoch() throws Exception {
        assertQuery("select extract(year from '1908-03-11T22:45:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        1908
                        """);
    }

    @Test
    public void testWeek1Start() throws Exception {
        assertQuery("select extract(week from '2022-01-03T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        1
                        """);
    }

    @Test
    public void testWeek1StartLeap() throws Exception {
        assertQuery("select extract(week from '2019-12-31T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        1
                        """);
    }

    @Test
    public void testWeek2End() throws Exception {
        assertQuery("select extract(week from '2022-01-16T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        2
                        """);
    }

    @Test
    public void testWeek3Start() throws Exception {
        assertQuery("select extract(week from '2022-01-17T22:00:30.555555Z')")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        3
                        """);
    }

    @Test
    public void testWeek52Start() throws Exception {
        assertQuery("select extract(week from '2022-01-01T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        52
                        """);
    }

    @Test
    public void testWeekNull() throws Exception {
        assertQuery("select extract(week from null)")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        null
                        """);
    }

    @Test
    public void testYearNull() throws Exception {
        assertQuery("select extract(year from null)")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        null
                        """);
    }

    @Test
    public void testZeroCentury() throws Exception {
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertQuery("select extract(century from '0000-01-01T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")")
                .ddl(null)
                .expectSize()
                .returns("""
                        extract
                        -1
                        """);
    }
}
