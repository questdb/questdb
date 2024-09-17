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

import io.questdb.std.Interval;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class TodayTomorrowYesterdayTest extends AbstractCairoTest {

    @Test
    public void testTimestampInInterval() throws Exception {
        assertSql("result\n", "select true as result from long_sequence(1)\n" +
                "where now() in tomorrow()");
        assertSql("result\n" +
                "true\n", "select true as result from long_sequence(1)\n" +
                "where now() in today()");
        assertSql("result\n", "select true as result from long_sequence(1)\n" +
                "where now() in yesterday()");
    }

    @Test
    public void testToday() throws Exception {
        long todayStart = Timestamps.floorDD(Os.currentTimeMicros());
        long todayEnd = Timestamps.floorDD(Timestamps.addDays(Os.currentTimeMicros(), 1)) - 1;
        final Interval interval = new Interval(todayStart, todayEnd);
        assertSql("today\n" + interval + "\n", "select today()");
    }

    @Test
    public void testTomorrow() throws Exception {
        long tomorrowStart = Timestamps.floorDD(Timestamps.addDays(Os.currentTimeMicros(), 1));
        long tomorrowEnd = Timestamps.floorDD(Timestamps.addDays(Os.currentTimeMicros(), 2)) - 1;
        final Interval interval = new Interval(tomorrowStart, tomorrowEnd);
        assertSql("tomorrow\n" + interval + "\n", "select tomorrow()");
    }

    @Test
    public void testYesterday() throws Exception {
        long yesterdayStart = Timestamps.floorDD(Timestamps.addDays(Os.currentTimeMicros(), -1));
        long yesterdayEnd = Timestamps.floorDD(Os.currentTimeMicros()) - 1;
        final Interval interval = new Interval(yesterdayStart, yesterdayEnd);
        assertSql("yesterday\n" + interval + "\n", "select yesterday()");
    }

}
