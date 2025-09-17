/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
        assertQuery(
                "extract\n" +
                        "2\n",
                "select extract(millennium from '1997-04-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );

        assertQuery(
                "extract\n" +
                        "2\n",
                "select extract('millennium' from '1997-04-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testBeforeEndOfYear() throws Exception {
        assertQuery(
                "extract\n" +
                        "364\n",
                "select extract(doy from '2022-12-30T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );

        assertQuery(
                "extract\n" +
                        "364\n",
                "select extract('doy' from '2022-12-30T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testBeginningOfCentury() throws Exception {
        assertQuery(
                "extract\n" +
                        "20\n",
                "select extract(century from '2000-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testCenturyNull() throws Exception {
        assertQuery(
                "extract\n" +
                        "null\n",
                "select extract(century from null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testComma() throws Exception {
        assertException(
                "select extract(hour, '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                15,
                "Invalid column: hour"
        );
    }

    @Test
    public void testDanglingArg() throws Exception {
        assertException(
                "select extract(hour from to_timestamp('2022-03-11T22:00:30.555555Z') table)",
                69,
                "dangling literal"
        );
    }

    @Test
    public void testDayNull() throws Exception {
        assertQuery(
                "extract\n" +
                        "null\n",
                "select extract(day from null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testDecadeNull() throws Exception {
        assertQuery(
                "extract\n" +
                        "null\n",
                "select extract(decade from null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testDowNull() throws Exception {
        assertQuery(
                "extract\n" +
                        "null\n",
                "select extract(dow from null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testDoyNull() throws Exception {
        assertQuery(
                "extract\n" +
                        "null\n",
                "select extract(doy from null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testEndOfLeapYear() throws Exception {
        assertQuery(
                "extract\n" +
                        "366\n",
                "select extract(doy from '2020-12-31T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testEndOfYear() throws Exception {
        assertQuery(
                "extract\n" +
                        "365\n",
                "select extract(doy from '2022-12-31T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testEpochNull() throws Exception {
        assertQuery(
                "extract\n" +
                        "null\n",
                "select extract(epoch from null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testFirstCentury() throws Exception {
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertQuery(
                "extract\n" +
                        "1\n",
                "select extract(century from '0001-01-01T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testHourNull() throws Exception {
        assertQuery(
                "extract\n" +
                        "null\n",
                "select extract(hour from null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testIsoDowNull() throws Exception {
        assertQuery(
                "extract\n" +
                        "null\n",
                "select extract(isodow from null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testIsoYearNull() throws Exception {
        assertQuery(
                "extract\n" +
                        "null\n",
                "select extract(isoyear from null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testIsoYearWeek1Start() throws Exception {
        assertQuery(
                "extract\n" +
                        "2022\n",
                "select extract(isoyear from '2022-01-03T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testIsoYearWeek1StartLeap() throws Exception {
        assertQuery(
                "extract\n" +
                        "2020\n",
                "select extract(isoyear from '2019-12-31T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testIsoYearWeek52Start() throws Exception {
        assertQuery(
                "extract\n" +
                        "2021\n",
                "select extract(isoyear from '2022-01-01T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testMicrosecondsNull() throws Exception {
        assertQuery(
                "extract\n" +
                        "null\n",
                "select extract(microseconds from null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testMillenniumNull() throws Exception {
        assertQuery(
                "extract\n" +
                        "null\n",
                "select extract(millennium from null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testMilliseconds() throws Exception {
        assertQuery(
                "extract\n" +
                        "555\n",
                "select extract(milliseconds from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testMillisecondsNull() throws Exception {
        assertQuery(
                "extract\n" +
                        "null\n",
                "select extract(milliseconds from null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testMillisecondsPreEpoch() throws Exception {
        assertQuery(
                "extract\n" +
                        "555\n",
                "select extract(milliseconds from '1905-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testMinuteNull() throws Exception {
        assertQuery(
                "extract\n" +
                        "null\n",
                "select extract(minute from null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testMissingPart() throws Exception {
        assertException(
                "select extract(from to_timestamp('2022-03-11T22:00:30.555555Z'))",
                15,
                "Huh? What would you like to extract?"
        );
    }

    @Test
    public void testMonthNull() throws Exception {
        assertQuery(
                "extract\n" +
                        "null\n",
                "select extract(month from null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testMultipleFrom() throws Exception {
        assertException(
                "select extract(hour from from to_timestamp('2022-03-11T22:00:30.555555Z'))",
                25,
                "Unnecessary `from`. Typo?"
        );
    }

    @Test
    public void testNonLiteralPart() throws Exception {
        assertException(
                "select extract(1+1 from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                17,
                "we expect timestamp part here"
        );
    }

    @Test
    public void testNotExtractFrom() throws Exception {
        assertException(
                "select something(null from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                22,
                "dangling literal"
        );
    }

    @Test
    public void testNullFrom() throws Exception {
        assertException(
                "select extract(null from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                15,
                "unsupported timestamp part: null"
        );
    }

    @Test
    public void testQuarterFirst1() throws Exception {
        assertQuery(
                "extract\n" +
                        "1\n",
                "select extract(quarter from '2022-01-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testQuarterFirst2() throws Exception {
        assertQuery(
                "extract\n" +
                        "2\n",
                "select extract(quarter from '2022-04-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testQuarterLast1() throws Exception {
        assertQuery(
                "extract\n" +
                        "1\n",
                "select extract(quarter from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testQuarterLast4() throws Exception {
        assertQuery(
                "extract\n" +
                        "4\n",
                "select extract(quarter from '2022-12-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testQuarterNull() throws Exception {
        assertQuery(
                "extract\n" +
                        "null\n",
                "select extract(quarter from null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testSecondNull() throws Exception {
        assertQuery(
                "extract\n" +
                        "null\n",
                "select extract(second from null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testSecondPreEpoch() throws Exception {
        assertQuery(
                "extract\n" +
                        "30\n",
                "select extract(second from '1812-03-11T22:45:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testStartOfYear() throws Exception {
        assertQuery(
                "extract\n" +
                        "1\n",
                "select extract(doy from '2022-01-01T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testUnsupported() throws Exception {
        assertException(
                "select extract(timezone from '2022-12-30T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                15,
                "unsupported timestamp part: timezone"
        );
    }

    @Test
    public void testVanillaCentury() throws Exception {
        assertQuery(
                "extract\n" +
                        "21\n",
                "select extract(century from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanillaDay() throws Exception {
        assertQuery(
                "extract\n" +
                        "11\n",
                "select extract(day from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanillaDecade() throws Exception {
        assertQuery(
                "extract\n" +
                        "202\n",
                "select extract(decade from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanillaDowSaturday() throws Exception {
        assertQuery(
                "extract\n" +
                        "6\n",
                "select extract(dow from '2022-04-16T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanillaDowSunday() throws Exception {
        assertQuery(
                "extract\n" +
                        "0\n",
                "select extract(dow from '2022-04-10T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanillaDoy() throws Exception {
        assertQuery(
                "extract\n" +
                        "112\n",
                "select extract(doy from '2022-04-22T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanillaEpoch() throws Exception {
        assertQuery(
                "extract\n" +
                        "1650664830\n",
                "select extract(epoch from '2022-04-22T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanillaHour() throws Exception {
        assertQuery(
                "extract\n" +
                        "22\n",
                "select extract(hour from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanillaIsoDowMonday() throws Exception {
        assertQuery(
                "extract\n" +
                        "1\n",
                "select extract(isodow from '2022-04-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanillaIsoDowSunday() throws Exception {
        assertQuery(
                "extract\n" +
                        "7\n",
                "select extract(isodow from '2022-04-10T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanillaMicroseconds() throws Exception {
        assertQuery(
                "extract\n" +
                        "555555\n",
                "select extract(microseconds from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanillaMicrosecondsPreEpoch() throws Exception {
        assertQuery(
                "extract\n" +
                        "555555\n",
                "select extract(microseconds from '1917-03-11T22:00:40.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanillaMillennium() throws Exception {
        assertQuery(
                "extract\n" +
                        "3\n",
                "select extract(millennium from '2022-04-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanillaMinute() throws Exception {
        assertQuery(
                "extract\n" +
                        "45\n",
                "select extract(minute from '2022-03-11T22:45:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanillaMonth() throws Exception {
        assertQuery(
                "extract\n" +
                        "3\n",
                "select extract(month from '2022-03-11T22:45:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanillaSecond() throws Exception {
        assertQuery(
                "extract\n" +
                        "30\n",
                "select extract(second from '2022-03-11T22:45:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanillaWeek() throws Exception {
        assertQuery(
                "extract\n" +
                        "10\n",
                "select extract(week from '2022-03-11T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanillaYear() throws Exception {
        assertQuery(
                "extract\n" +
                        "2022\n",
                "select extract(year from '2022-03-11T22:45:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanillaYearPreEpoch() throws Exception {
        assertQuery(
                "extract\n" +
                        "1908\n",
                "select extract(year from '1908-03-11T22:45:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testWeek1Start() throws Exception {
        assertQuery(
                "extract\n" +
                        "1\n",
                "select extract(week from '2022-01-03T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testWeek1StartLeap() throws Exception {
        assertQuery(
                "extract\n" +
                        "1\n",
                "select extract(week from '2019-12-31T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testWeek2End() throws Exception {
        assertQuery(
                "extract\n" +
                        "2\n",
                "select extract(week from '2022-01-16T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testWeek3Start() throws Exception {
        assertQuery(
                "extract\n" +
                        "3\n",
                "select extract(week from '2022-01-17T22:00:30.555555Z')",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testWeek52Start() throws Exception {
        assertQuery(
                "extract\n" +
                        "52\n",
                "select extract(week from '2022-01-01T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testWeekNull() throws Exception {
        assertQuery(
                "extract\n" +
                        "null\n",
                "select extract(week from null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testYearNull() throws Exception {
        assertQuery(
                "extract\n" +
                        "null\n",
                "select extract(year from null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testZeroCentury() throws Exception {
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertQuery(
                "extract\n" +
                        "-1\n",
                "select extract(century from '0000-01-01T22:00:30.555555123Z'::" + timestampType.getTypeName() + ")",
                null,
                null,
                true,
                true
        );
    }
}
