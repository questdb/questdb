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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.maxDayOfMonth;
import static io.questdb.test.tools.TestUtils.putWithLeadingZeroIfNeeded;

public class WeekOfYearFunctionFactoryTest extends AbstractCairoTest {
    @Test
    public void testNull() throws Exception {
        assertQuery(
                "week_of_year\n" +
                        "null\n",
                "select week_of_year(null)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testPreEpoch() throws Exception {
        assertQuery(
                "week_of_year\n" +
                        "28\n",
                "select week_of_year('1901-07-11T22:00:30.555998Z'::timestamp)",
                null,
                null,
                true,
                true
        );

        assertQuery(
                "week_of_year\n" +
                        "28\n",
                "select week_of_year('1901-07-11T22:00:30.555998123Z'::timestamp_ns)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testVanilla() throws Exception {
        testVanilla(ColumnType.TIMESTAMP_MICRO);
    }

    @Test
    public void testVanillaOnTimestampNs() throws Exception {
        testVanilla(ColumnType.TIMESTAMP_NANO);
    }

    private void testVanilla(int timestampType) throws Exception {
        TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        StringSink weekSink = new StringSink();
        StringSink dateSink = new StringSink();
        dateSink.put("2023-");
        final int yearLen = dateSink.length();
        for (int month = 1; month < 13; month++) {
            putWithLeadingZeroIfNeeded(dateSink, yearLen, month).put('-');
            final int monthLen = dateSink.length(); // yyyy-MM-
            for (int day = 1, maxDay = maxDayOfMonth(month); day < maxDay + 1; day++) {
                putWithLeadingZeroIfNeeded(dateSink, monthLen, day);
                final String expectedDayFormatted = dateSink.toString(); // yyyy-MM-dd
                final String expectedWeekFormatted;
                final long timestamp = driver.parseFloorLiteral(expectedDayFormatted);
                int year = driver.getYear(timestamp);
                int week = driver.getWeek(timestamp);
                if (week == 52 && month == 1) {
                    year--;
                }
                weekSink.clear();
                weekSink.put(year).put("-W");
                putWithLeadingZeroIfNeeded(weekSink, weekSink.length(), week);
                expectedWeekFormatted = weekSink.toString();

                assertQuery(
                        "week_partition_dir_name\tweek\n" +
                                expectedWeekFormatted + '\t' + week + '\n',
                        "with timestamp as (select '" + expectedDayFormatted + "T23:59:59.999999Z'::" + ColumnType.nameOf(timestampType) + " as ts)\n" +
                                "  select to_str(ts, 'YYYY-Www') week_partition_dir_name, week_of_year(ts) week from timestamp",
                        null,
                        null,
                        true,
                        true
                );
            }
        }
    }
}
