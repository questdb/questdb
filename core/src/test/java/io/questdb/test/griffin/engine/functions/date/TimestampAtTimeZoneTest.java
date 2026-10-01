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

public class TimestampAtTimeZoneTest extends AbstractCairoTest {

    @Test
    public void testArithmetic() throws Exception {
        assertQuery("select '2022-03-11T22:00:30.555555Z'::timestamp at time zone 'UTC' + 5")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        column
                        2022-03-11T22:00:30.555560Z
                        """);

        assertQuery("select '2022-03-11T22:00:30.555555555Z'::timestamp_ns at time zone 'UTC' + 5")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        column
                        2022-03-11T22:00:30.555555560Z
                        """);
    }

    @Test
    public void testCast() throws Exception {
        assertQuery("select cast('2022-03-11T22:00:30.555555Z'::timestamp at time zone 'EST' as string)")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        cast
                        2022-03-11T17:00:30.555555Z
                        """);

        assertQuery("select cast('2022-03-11T22:00:30.555555555Z'::timestamp_ns at time zone 'EST' as string)")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        cast
                        2022-03-11T17:00:30.555555555Z
                        """);
    }

    @Test
    public void testFail1() throws Exception {
        assertQuery("select to_timestamp('2022-03-11T22:00:30.555555Z') at 'UTC'")
                .fails(54, "',', 'from' or 'over' expected");
    }

    @Test
    public void testFail2() throws Exception {
        assertQuery("select to_timestamp_ns('2022-03-11T22:00:30.555555555Z') at time 'UTC'")
                .fails(65, "did you mean 'at time zone <tz>'?");
    }

    @Test
    public void testFailDangling2() throws Exception {
        assertQuery("select to_timestamp('2022-03-11T22:00:30.555555Z') at time")
                .fails(58, "did you mean 'at time zone <tz>'?");
    }

    @Test
    public void testFailDangling3() throws Exception {
        assertQuery("select to_timestamp_ns('2022-03-11T22:00:30.555555555Z') at time")
                .fails(64, "did you mean 'at time zone <tz>'?");
    }

    @Test
    public void testFunctionArg() throws Exception {
        assertQuery("select date_trunc('day', '2022-03-11T22:00:30.555555Z'::timestamp at time zone 'UTC')")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        date_trunc
                        2022-03-11T00:00:00.000000Z
                        """);

        assertQuery("select date_trunc('day', '2022-03-11T22:00:30.555555555Z'::timestamp_ns at time zone 'UTC')")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        date_trunc
                        2022-03-11T00:00:00.000000000Z
                        """);
    }

    @Test
    public void testSwitch() throws Exception {
        assertQuery("select case " +
                "   when to_timestamp('2022-03-11T22:00:30.555555Z') at time zone 'EST' > 0" +
                "   then 'abc'" +
                "   else 'cde'" +
                "end")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        case
                        abc
                        """);

        assertQuery("select case " +
                "   when to_timestamp_ns('2022-03-11T22:00:30.555555555Z') at time zone 'EST' > 0" +
                "   then 'abc'" +
                "   else 'cde'" +
                "end")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        case
                        abc
                        """);
    }

    @Test
    public void testValidAliasTime() throws Exception {
        assertQuery("select '2022-03-11T22:00:30.555555Z'::timestamp time")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        time
                        2022-03-11T22:00:30.555555Z
                        """);

        assertQuery("select '2022-03-11T22:00:30.555555555Z'::timestamp_ns time")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        time
                        2022-03-11T22:00:30.555555555Z
                        """);
    }

    @Test
    public void testValidAliasZone() throws Exception {
        assertQuery("select '2022-03-11T22:00:30.555555Z'::timestamp zone")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        zone
                        2022-03-11T22:00:30.555555Z
                        """);

        assertQuery("select '2022-03-11T22:00:30.555555555Z'::timestamp_ns zone")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        zone
                        2022-03-11T22:00:30.555555555Z
                        """);
    }

    @Test
    public void testVanilla() throws Exception {
        assertQuery("select '2022-03-11T22:00:30.555555Z'::timestamp at time zone 'UTC'")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        cast
                        2022-03-11T22:00:30.555555Z
                        """);

        assertQuery("select '2022-03-11T22:00:30.555555555Z'::timestamp_ns at time zone 'UTC'")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        cast
                        2022-03-11T22:00:30.555555555Z
                        """);
    }
}
