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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class DateTruncWithTimezoneFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testDayTruncWithTimezone() throws Exception {
        // 2017-03-17T02:09:30.111111Z in America/Los_Angeles is 2017-03-16T19:09:30.111111
        // Truncating to day in local time gives 2017-03-16T00:00:00 local
        // Converting back to UTC: 2017-03-16T08:00:00Z (UTC-8 in March, DST not yet)
        // Actually, March 12 2017 DST started, so March 17 is UTC-7
        // 2017-03-17T02:09:30.111111Z -> 2017-03-16T19:09:30.111111 PDT
        // floor to day -> 2017-03-16T00:00:00 PDT -> 2017-03-16T07:00:00Z
        assertTimestamp(
                "SELECT DATE_TRUNC('day', TIMESTAMP '2017-03-17T02:09:30.111111Z', 'America/Los_Angeles') AS truncated",
                "2017-03-16T07:00:00.000000Z"
        );
    }

    @Test
    public void testDayTruncWithTimezoneNanos() throws Exception {
        assertTimestamp(
                "SELECT DATE_TRUNC('day', TIMESTAMP_NS '2017-03-17T02:09:30.111111111Z', 'America/Los_Angeles') AS truncated",
                "2017-03-16T07:00:00.000000000Z"
        );
    }

    @Test
    public void testDstTransition() throws Exception {
        // 2017-03-12 is the spring-forward day in America/New_York (EST->EDT)
        // 2017-03-12T06:30:00Z = 2017-03-12T01:30:00 EST
        // floor to day -> 2017-03-12T00:00:00 EST -> 2017-03-12T05:00:00Z
        assertTimestamp(
                "SELECT DATE_TRUNC('day', TIMESTAMP '2017-03-12T06:30:00.000000Z', 'America/New_York') AS truncated",
                "2017-03-12T05:00:00.000000Z"
        );
        // 2017-03-12T10:00:00Z = 2017-03-12T06:00:00 EDT (after spring forward)
        // floor to day -> 2017-03-12T00:00:00 EST -> 2017-03-12T05:00:00Z
        assertTimestamp(
                "SELECT DATE_TRUNC('day', TIMESTAMP '2017-03-12T10:00:00.000000Z', 'America/New_York') AS truncated",
                "2017-03-12T05:00:00.000000Z"
        );
    }

    @Test
    public void testHourTruncWithTimezone() throws Exception {
        // 2017-03-17T02:09:30.111111Z in Europe/Berlin is 2017-03-17T03:09:30.111111 CET (UTC+1)
        // Wait, March 17 2017 in Europe/Berlin is CET (UTC+1), DST starts March 26
        // So: 2017-03-17T02:09:30.111111Z -> 2017-03-17T03:09:30.111111 local
        // floor to hour -> 2017-03-17T03:00:00 local -> 2017-03-17T02:00:00Z
        assertTimestamp(
                "SELECT DATE_TRUNC('hour', TIMESTAMP '2017-03-17T02:09:30.111111Z', 'Europe/Berlin') AS truncated",
                "2017-03-17T02:00:00.000000Z"
        );
    }

    @Test
    public void testInvalidTimezone() throws Exception {
        assertException(
                "SELECT DATE_TRUNC('day', TIMESTAMP '2017-03-17T02:09:30.111111Z', 'Invalid/Zone') AS truncated",
                66,
                "invalid timezone"
        );
    }

    @Test
    public void testInvalidUnit() throws Exception {
        assertException(
                "SELECT DATE_TRUNC('invalid', TIMESTAMP '2017-03-17T02:09:30.111111Z', 'UTC') AS truncated",
                18,
                "invalid unit 'invalid'"
        );
    }

    @Test
    public void testMonthTruncWithTimezone() throws Exception {
        // 2017-03-01T02:00:00Z in Asia/Tokyo (UTC+9) is 2017-03-01T11:00:00 local
        // floor to month -> 2017-03-01T00:00:00 local -> 2017-02-28T15:00:00Z
        assertTimestamp(
                "SELECT DATE_TRUNC('month', TIMESTAMP '2017-03-01T02:00:00.000000Z', 'Asia/Tokyo') AS truncated",
                "2017-02-28T15:00:00.000000Z"
        );
    }

    @Test
    public void testNullTimestamp() throws Exception {
        assertTimestamp(
                "SELECT DATE_TRUNC('day', NULL::TIMESTAMP, 'UTC') AS truncated",
                ""
        );
    }

    @Test
    public void testPluralUnits() throws Exception {
        assertTimestamp(
                "SELECT DATE_TRUNC('days', TIMESTAMP '2017-03-17T02:09:30.111111Z', 'America/Los_Angeles') AS truncated",
                "2017-03-16T07:00:00.000000Z"
        );
        assertTimestamp(
                "SELECT DATE_TRUNC('hours', TIMESTAMP '2017-03-17T02:09:30.111111Z', 'Europe/Berlin') AS truncated",
                "2017-03-17T02:00:00.000000Z"
        );
    }

    @Test
    public void testUtcTimezone() throws Exception {
        // With UTC timezone, result should match plain date_trunc
        assertTimestamp(
                "SELECT DATE_TRUNC('day', TIMESTAMP '2017-03-17T02:09:30.111111Z', 'UTC') AS truncated",
                "2017-03-17T00:00:00.000000Z"
        );
        assertTimestamp(
                "SELECT DATE_TRUNC('hour', TIMESTAMP '2017-03-17T02:09:30.111111Z', 'UTC') AS truncated",
                "2017-03-17T02:00:00.000000Z"
        );
        assertTimestamp(
                "SELECT DATE_TRUNC('month', TIMESTAMP '2017-03-17T02:09:30.111111Z', 'UTC') AS truncated",
                "2017-03-01T00:00:00.000000Z"
        );
    }

    @Test
    public void testWeekTruncWithTimezone() throws Exception {
        // 2017-03-17T02:09:30.111111Z in America/Los_Angeles is 2017-03-16T19:09:30.111111 PDT
        // floor to week (Monday) -> 2017-03-13T00:00:00 PDT -> 2017-03-13T07:00:00Z
        assertTimestamp(
                "SELECT DATE_TRUNC('week', TIMESTAMP '2017-03-17T02:09:30.111111Z', 'America/Los_Angeles') AS truncated",
                "2017-03-13T07:00:00.000000Z"
        );
    }

    @Test
    public void testWithTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ts_test (ts TIMESTAMP, val INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO ts_test VALUES ('2017-03-17T02:09:30.111111Z', 1), ('2017-03-17T10:30:00.000000Z', 2), ('2017-03-18T01:00:00.000000Z', 3)");
            drainWalQueue();
            assertSql(
                    "truncated\tSUM\n" +
                            "2017-03-16T07:00:00.000000Z\t1\n" +
                            "2017-03-17T07:00:00.000000Z\t5\n",
                    "SELECT DATE_TRUNC('day', ts, 'America/Los_Angeles') AS truncated, SUM(val) FROM ts_test GROUP BY truncated ORDER BY truncated"
            );
        });
    }

    @Test
    public void testYearTruncWithTimezone() throws Exception {
        // 2017-01-01T02:00:00Z in Asia/Kolkata (UTC+5:30) is 2017-01-01T07:30:00 local
        // floor to year -> 2017-01-01T00:00:00 local -> 2016-12-31T18:30:00Z
        assertTimestamp(
                "SELECT DATE_TRUNC('year', TIMESTAMP '2017-01-01T02:00:00.000000Z', 'Asia/Kolkata') AS truncated",
                "2016-12-31T18:30:00.000000Z"
        );
    }

    private void assertTimestamp(String sql, String expected) throws Exception {
        assertMemoryLeak(() -> assertSql(
                "truncated\n" +
                        expected + "\n", sql
        ));
    }
}
