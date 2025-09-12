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

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class ToUTCTimestampFunctionFactoryTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public ToUTCTimestampFunctionFactoryTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testAreaName() throws Exception {
        assertMemoryLeak(() -> assertToUTC(
                "to_utc\n" +
                        "1969-12-31T23:00:00.000000Z\n",
                "1970-01-01T00:00:00.000000Z",
                "Europe/Prague"
        ));
    }

    @Test
    public void testDst() throws Exception {
        assertMemoryLeak(() -> {
            // CET to CEST
            assertToUTC(
                    "to_utc\n" +
                            "2021-03-27T23:01:00.000000Z\n",
                    "2021-03-28T00:01:00.000000Z",
                    "Europe/Berlin"
            );
            assertToUTC(
                    "to_utc\n" +
                            "2021-03-28T00:01:00.000000Z\n",
                    "2021-03-28T01:01:00.000000Z",
                    "Europe/Berlin"
            );
            // non-existing local time (mapped to the "next" UTC hour)
            assertToUTC(
                    "to_utc\n" +
                            "2021-03-28T01:00:00.000000Z\n",
                    "2021-03-28T02:00:00.000000Z",
                    "Europe/Berlin"
            );
            assertToUTC(
                    "to_utc\n" +
                            "2021-03-28T01:01:00.000000Z\n",
                    "2021-03-28T02:01:00.000000Z",
                    "Europe/Berlin"
            );
            assertToUTC(
                    "to_utc\n" +
                            "2021-03-28T01:00:00.000000Z\n",
                    "2021-03-28T03:00:00.000000Z",
                    "Europe/Berlin"
            );
            assertToUTC(
                    "to_utc\n" +
                            "2021-03-28T01:01:00.000000Z\n",
                    "2021-03-28T03:01:00.000000Z",
                    "Europe/Berlin"
            );
            assertToUTC(
                    "to_utc\n" +
                            "2021-03-28T02:01:00.000000Z\n",
                    "2021-03-28T04:01:00.000000Z",
                    "Europe/Berlin"
            );

            // CEST to CET
            assertToUTC(
                    "to_utc\n" +
                            "2021-10-30T22:01:00.000000Z\n",
                    "2021-10-31T00:01:00.000000Z",
                    "Europe/Berlin"
            );
            assertToUTC(
                    "to_utc\n" +
                            "2021-10-30T23:01:00.000000Z\n",
                    "2021-10-31T01:01:00.000000Z",
                    "Europe/Berlin"
            );
            assertToUTC(
                    "to_utc\n" +
                            "2021-10-31T00:00:00.000000Z\n",
                    "2021-10-31T02:00:00.000000Z",
                    "Europe/Berlin"
            );
            assertToUTC(
                    "to_utc\n" +
                            "2021-10-31T00:01:00.000000Z\n",
                    "2021-10-31T02:01:00.000000Z",
                    "Europe/Berlin"
            );
            assertToUTC(
                    "to_utc\n" +
                            "2021-10-31T02:01:00.000000Z\n",
                    "2021-10-31T03:01:00.000000Z",
                    "Europe/Berlin"
            );
        });
    }

    @Test
    public void testInvalidConstantOffset() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("select to_utc(0, '25:40')");
            } catch (SqlException e) {
                Assert.assertEquals(17, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid timezone");
            }
        });
    }

    @Test
    public void testInvalidConstantTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("select to_utc(0, 'UUU')");
            } catch (SqlException e) {
                Assert.assertEquals(17, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid timezone");
            }
        });
    }

    @Test
    public void testNullConstantTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("select to_utc(0, null)");
            } catch (SqlException e) {
                Assert.assertEquals(17, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "timezone must not be null");
            }
        });
    }

    @Test
    public void testTimeOffset() throws Exception {
        assertMemoryLeak(() -> assertToUTC(
                "to_utc\n" +
                        "2020-03-12T23:10:00.000000Z\n",
                "2020-03-12T15:30:00.000000Z",
                "-07:40"
        ));
    }

    @Test
    public void testVarInvalidTimezone() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "to_utc\n" +
                        "2020-03-12T15:30:00.000000Z\n",
                "select to_utc(cast('2020-03-12T15:30:00.000000Z' as timestamp), zone) from (select 'XU' zone)"
        ));
    }

    @Test
    public void testVarNullTimezone() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("select to_utc(cast('2020-03-12T15:30:00.000000Z' as timestamp), zone) from (select null zone)");
            } catch (SqlException e) {
                Assert.assertEquals(64, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "timezone must not be null");
            }
        });
    }

    @Test
    public void testVarTimezone() throws Exception {
        assertMemoryLeak(() -> assertSql(
                replaceTimestampSuffix("to_utc\n" +
                        "2020-03-12T23:10:00.000000Z\n", timestampType.getTypeName()),
                "select to_utc(cast('2020-03-12T15:30:00.000000Z' as " + timestampType.getTypeName() + "), zone) from (select '-07:40' zone)"
        ));
    }

    @Test
    public void testZoneName() throws Exception {
        assertMemoryLeak(() -> assertToUTC(
                "to_utc\n" +
                        "2020-03-12T22:30:00.000000Z\n",
                "2020-03-12T15:30:00.000000Z",
                "PST"
        ));
    }

    private void assertToUTC(
            String expected,
            String timestamp,
            String timeZone
    ) throws SqlException {
        expected = replaceTimestampSuffix(expected, timestampType.getTypeName());
        timestamp = replaceTimestampSuffix(timestamp, timestampType.getTypeName());
        assertSql(
                expected,
                "select to_utc('" +
                        timestamp + "', " +
                        (timeZone != null ? "'" + timeZone + "'" : "null") +
                        ")"
        );

        bindVariableService.clear();
        bindVariableService.setStr("tz", timeZone);
        assertSql(
                expected,
                "select to_utc('" +
                        timestamp + "', " +
                        ":tz" +
                        ")"
        );
    }
}