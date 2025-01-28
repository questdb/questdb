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

import io.questdb.cairo.CairoException;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ToTimezoneIntervalFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAreaName() throws Exception {
        assertToTimezoneInterval("select to_timezone(interval(0,0), 'Europe/Prague')",
                "('1970-01-01T01:00:00.000Z', '1970-01-01T01:00:00.000Z')\n");
    }

    @Test
    public void testDaylightSavings() throws Exception {
        assertToTimezoneInterval(
                "SELECT to_timezone(interval('2023-03-01T17:00:00.000000Z', '2023-03-15T17:00:00.000000Z'),'America/New_York')",
                "('2023-03-01T12:00:00.000Z', '2023-03-15T13:00:00.000Z')\n"
        );
    }

    @Test
    public void testInvalidConstantOffset() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("select to_timezone(interval(0,0), '25:40')");
            } catch (SqlException e) {
                Assert.assertEquals(34, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid timezone name");
            }
        });
    }

    @Test
    public void testInvalidConstantTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("select to_timezone(interval(0,0), 'UUU')");
            } catch (SqlException e) {
                Assert.assertEquals(34, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid timezone name");
            }
        });
    }

    @Test
    public void testNullConstantTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("select to_timezone(interval(0,0), null)");
            } catch (SqlException e) {
                Assert.assertEquals(34, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "timezone must not be null");
            }
        });
    }

    @Test
    public void testSanityCheckAgainstTimestamp() throws Exception {
        assertSql("column\ntrue\n", "select interval(to_timezone(now(), 'Antarctica/McMurdo'), to_timezone(now(), 'Antarctica/McMurdo')) = to_timezone(interval(now(), now()), 'Antarctica/McMurdo')");
    }

    @Test
    public void testTimeOffset() throws Exception {
        assertToTimezoneInterval(
                "select to_timezone(interval('2020-03-12T15:30:00.000000Z', '2020-03-12T17:30:00.000000Z'), '-07:40')",
                "('2020-03-12T07:50:00.000Z', '2020-03-12T09:50:00.000Z')\n"
        );
    }

    @Test
    public void testToTimezoneWithConstantTimezonePlan() throws Exception {
        assertMemoryLeak(() -> assertPlanNoLeakCheck(
                //Input query
                "select to_timezone(interval('2020-03-12T15:30:00.000000Z', '2020-03-12T17:30:00.000000Z'), '-07:40')",
                //Expected Plan
                "VirtualRecord\n" +
                        "  functions: [to_timezone(('2020-03-12T15:30:00.000Z', '2020-03-12T17:30:00.000Z'),'-07:40')]\n" +
                        "    long_sequence count: 1\n"
        ));
    }

    @Test
    public void testToTimezoneWithVarTimezonePlan() throws Exception {
        assertMemoryLeak(() -> assertPlanNoLeakCheck(
                //Input query
                "select to_timezone(interval('2020-03-12T15:30:00.000000Z', '2020-03-12T17:30:00.000000Z'), zone) from (select '-07:40' zone)",
                //Expected Plan
                "VirtualRecord\n" +
                        "  functions: [to_timezone(('2020-03-12T15:30:00.000Z', '2020-03-12T17:30:00.000Z'),zone)]\n" +
                        "    VirtualRecord\n" +
                        "      functions: ['-07:40']\n" +
                        "        long_sequence count: 1\n"
        ));
    }

    @Test
    public void testVarInvalidTimezone() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("select to_timezone(interval('2020-03-12T15:30:00.000000Z', '2020-03-12T17:30:00.000000Z'), zone) from (select 'XU' zone)");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid timezone name");
            }
        });
    }

    @Test
    public void testVarNullTimezone() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("select to_timezone(interval('2020-03-12T15:30:00.000000Z', '2020-03-12T17:30:00.000000Z'), zone) from (select null zone)");
            } catch (SqlException e) {
                Assert.assertEquals(91, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "timezone must not be null");
            }
        });
    }

    @Test
    public void testVarTimezone() throws Exception {
        assertToTimezoneInterval(
                "select to_timezone(interval('2020-03-12T15:30:00.000000Z', '2020-03-12T17:30:00.000000Z'), zone) from (select '-07:40' zone)",
                "('2020-03-12T07:50:00.000Z', '2020-03-12T09:50:00.000Z')\n"
        );
    }

    @Test
    public void testZoneName() throws Exception {
        assertToTimezoneInterval(
                "select to_timezone(interval('2020-03-12T15:30:00.000000Z', '2020-03-12T17:30:00.000000Z'), 'PST')",
                "('2020-03-12T08:30:00.000Z', '2020-03-12T10:30:00.000Z')\n"
        );
    }

    private void assertToTimezoneInterval(String sql, String expected) throws Exception {
        assertMemoryLeak(() -> assertSql("to_timezone\n" + expected, sql));
    }
}