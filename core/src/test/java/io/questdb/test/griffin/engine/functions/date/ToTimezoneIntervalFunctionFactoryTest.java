/*******************************************************************************
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
public class ToTimezoneIntervalFunctionFactoryTest extends AbstractCairoTest {
    private final TestTimestampType timestampType;

    public ToTimezoneIntervalFunctionFactoryTest(TestTimestampType timestampType) {
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
        assertMemoryLeak(() -> assertToTimezone(
                "('1970-01-01T01:00:00.000Z', '1970-01-01T01:30:00.000Z')",
                "1970-01-01T00:00:00.000Z",
                "1970-01-01T00:30:00.000Z",
                "Europe/Prague"
        ));
    }

    @Test
    public void testDstStraddle() throws Exception {
        // Europe/Berlin springs forward (CET +01:00 -> CEST +02:00) at 01:00 UTC on 2021-03-28.
        // The interval start falls before the transition and the end falls after it, so each
        // boundary is shifted by its own offset.
        assertMemoryLeak(() -> assertToTimezone(
                "('2021-03-28T01:30:00.000Z', '2021-03-28T03:30:00.000Z')",
                "2021-03-28T00:30:00.000Z",
                "2021-03-28T01:30:00.000Z",
                "Europe/Berlin"
        ));
    }

    @Test
    public void testNullInterval() throws Exception {
        assertMemoryLeak(() -> {
            final String tsName = timestampType.getTypeName();
            assertQuery("select to_timezone(interval(cast(null as " + tsName + "), cast(null as " + tsName + ")), 'UTC')")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            to_timezone

                            """);
        });
    }

    @Test
    public void testNullConstantTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("select to_timezone(interval('2020-03-12T15:30:00.000Z', '2020-03-12T16:30:00.000Z'), null)");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "timezone must not be null");
            }
        });
    }

    @Test
    public void testTimeOffset() throws Exception {
        assertMemoryLeak(() -> assertToTimezone(
                "('2020-03-12T07:50:00.000Z', '2020-03-12T08:50:00.000Z')",
                "2020-03-12T15:30:00.000Z",
                "2020-03-12T16:30:00.000Z",
                "-07:40"
        ));
    }

    @Test
    public void testVarInvalidTimezone() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                "select to_timezone(interval(cast('2020-03-12T15:30:00.000Z' as " + timestampType.getTypeName() +
                        "), cast('2020-03-12T16:30:00.000Z' as " + timestampType.getTypeName() + ")), zone) from (select 'XU' zone)")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        to_timezone
                        ('2020-03-12T15:30:00.000Z', '2020-03-12T16:30:00.000Z')
                        """));
    }

    @Test
    public void testVarNullTimezone() throws Exception {
        // A null projection folds to a constant null zone, so it is rejected at compile time.
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck(
                        "select to_timezone(interval(cast('2020-03-12T15:30:00.000Z' as " + timestampType.getTypeName() +
                                "), cast('2020-03-12T16:30:00.000Z' as " + timestampType.getTypeName() + ")), zone) from (select null zone)");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "timezone must not be null");
            }
        });
    }

    @Test
    public void testZoneName() throws Exception {
        // PST resolves with DST rules; 2020-03-12 is in Pacific Daylight Time (-07:00).
        assertMemoryLeak(() -> assertToTimezone(
                "('2020-03-12T08:30:00.000Z', '2020-03-12T09:30:00.000Z')",
                "2020-03-12T15:30:00.000Z",
                "2020-03-12T16:30:00.000Z",
                "PST"
        ));
    }

    private void assertToTimezone(
            String expectedInterval,
            String lo,
            String hi,
            String timeZone
    ) throws Exception {
        final String tsName = timestampType.getTypeName();
        final String intervalArg = "interval(cast('" + lo + "' as " + tsName + "), cast('" + hi + "' as " + tsName + "))";
        final String expected = "to_timezone\n" + expectedInterval + "\n";

        assertQuery("select to_timezone(" + intervalArg + ", '" + timeZone + "')")
                .noLeakCheck()
                .expectSize()
                .returns(expected);

        bindVariableService.clear();
        bindVariableService.setStr("tz", timeZone);
        assertQuery("select to_timezone(" + intervalArg + ", :tz)")
                .noLeakCheck()
                .expectSize()
                .returns(expected);
    }
}
