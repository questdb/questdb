/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.date;

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Test;

public class TimestampAtTimeZoneTest extends AbstractGriffinTest {

    @Test
    public void testVanilla() throws Exception {
        assertQuery(
                "to_timezone\n" +
                        "2022-03-11T22:00:30.555555Z\n",
                "select to_timestamp('2022-03-11T22:00:30.555555Z') at time zone 'UTC'",
                null,
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testValidAliasZone() throws Exception {
        assertQuery(
                "zone\n" +
                        "2022-03-11T22:00:30.555555Z\n",
                "select to_timestamp('2022-03-11T22:00:30.555555Z') zone",
                null,
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testValidAliasTime() throws Exception {
        assertQuery(
                "time\n" +
                        "2022-03-11T22:00:30.555555Z\n",
                "select to_timestamp('2022-03-11T22:00:30.555555Z') time",
                null,
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testFail1() throws Exception {
        assertFailure(
                "select to_timestamp('2022-03-11T22:00:30.555555Z') at 'UTC'",
                null,
                54,
                "',', 'from' or 'over' expected"
        );
    }

    @Test
    public void testFail2() throws Exception {
        assertFailure(
                "select to_timestamp('2022-03-11T22:00:30.555555Z') at time 'UTC'",
                null,
                59,
                "did you mean 'at time zone <tz>'?"
        );
    }

    @Test
    public void testFailDangling2() throws Exception {
        assertFailure(
                "select to_timestamp('2022-03-11T22:00:30.555555Z') at time",
                null,
                58,
                "did you mean 'at time zone <tz>'?"
        );
    }

    @Test
    public void testFailDangling3() throws Exception {
        assertFailure(
                "select to_timestamp('2022-03-11T22:00:30.555555Z') at time",
                null,
                58,
                "did you mean 'at time zone <tz>'?"
        );
    }

    @Test
    public void testArithmetic() throws Exception {
        assertQuery(
                "column\n" +
                        "2022-03-11T22:00:30.555560Z\n",
                "select to_timestamp('2022-03-11T22:00:30.555555Z') at time zone 'UTC' + 5",
                null,
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testCast() throws Exception {
        assertQuery(
                "cast\n" +
                        "1647018030555555\n",
                "select cast(to_timestamp('2022-03-11T22:00:30.555555Z') at time zone 'EST' as string)",
                null,
                null,
                true,
                false,
                true
        );
    }

    @Test
    public void testSwitch() throws Exception {
        assertQuery(
                "case\n" +
                        "abc\n",
                "select case " +
                        "   when to_timestamp('2022-03-11T22:00:30.555555Z') at time zone 'EST' > 0" +
                        "   then 'abc'" +
                        "   else 'cde'" +
                        "end",
                null,
                null,
                true,
                false,
                true
        );
    }

    @Test
    public void testFunctionArg() throws Exception {
        assertQuery(
                "date_trunc\n" +
                        "2022-03-11T00:00:00.000000Z\n",
                "select date_trunc('day', to_timestamp('2022-03-11T22:00:30.555555Z') at time zone 'UTC')",
                null,
                null,
                true,
                true,
                true
        );
    }
}
