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
import org.junit.Test;

public class TimestampAtTimeZoneTest extends AbstractCairoTest {

    @Test
    public void testArithmetic() throws Exception {
        assertSql(
                "column\n" +
                        "2022-03-11T22:00:30.555560Z\n",
                "select '2022-03-11T22:00:30.555555Z'::timestamp at time zone 'UTC' + 5"
        );

        assertSql(
                "column\n" +
                        "2022-03-11T22:00:30.555555560Z\n",
                "select '2022-03-11T22:00:30.555555555Z'::timestamp_ns at time zone 'UTC' + 5"
        );
    }

    @Test
    public void testCast() throws Exception {
        assertSql(
                "cast\n" +
                        "2022-03-11T17:00:30.555555Z\n",
                "select cast('2022-03-11T22:00:30.555555Z'::timestamp at time zone 'EST' as string)"
        );

        assertSql(
                "cast\n" +
                        "2022-03-11T17:00:30.555555555Z\n",
                "select cast('2022-03-11T22:00:30.555555555Z'::timestamp_ns at time zone 'EST' as string)"
        );
    }

    @Test
    public void testFail1() throws Exception {
        assertException(
                "select to_timestamp('2022-03-11T22:00:30.555555Z') at 'UTC'",
                54,
                "',', 'from' or 'over' expected"
        );
    }

    @Test
    public void testFail2() throws Exception {
        assertException(
                "select to_timestamp_ns('2022-03-11T22:00:30.555555555Z') at time 'UTC'",
                65,
                "did you mean 'at time zone <tz>'?"
        );
    }

    @Test
    public void testFailDangling2() throws Exception {
        assertException(
                "select to_timestamp('2022-03-11T22:00:30.555555Z') at time",
                58,
                "did you mean 'at time zone <tz>'?"
        );
    }

    @Test
    public void testFailDangling3() throws Exception {
        assertException(
                "select to_timestamp_ns('2022-03-11T22:00:30.555555555Z') at time",
                64,
                "did you mean 'at time zone <tz>'?"
        );
    }

    @Test
    public void testFunctionArg() throws Exception {
        assertSql(
                "date_trunc\n" +
                        "2022-03-11T00:00:00.000000Z\n",
                "select date_trunc('day', '2022-03-11T22:00:30.555555Z'::timestamp at time zone 'UTC')"
        );

        assertSql(
                "date_trunc\n" +
                        "2022-03-11T00:00:00.000000000Z\n",
                "select date_trunc('day', '2022-03-11T22:00:30.555555555Z'::timestamp_ns at time zone 'UTC')"
        );
    }

    @Test
    public void testSwitch() throws Exception {
        assertSql(
                "case\n" +
                        "abc\n",
                "select case " +
                        "   when to_timestamp('2022-03-11T22:00:30.555555Z') at time zone 'EST' > 0" +
                        "   then 'abc'" +
                        "   else 'cde'" +
                        "end"
        );

        assertSql(
                "case\n" +
                        "abc\n",
                "select case " +
                        "   when to_timestamp_ns('2022-03-11T22:00:30.555555555Z') at time zone 'EST' > 0" +
                        "   then 'abc'" +
                        "   else 'cde'" +
                        "end"
        );
    }

    @Test
    public void testValidAliasTime() throws Exception {
        assertSql(
                "time\n" +
                        "2022-03-11T22:00:30.555555Z\n",
                "select '2022-03-11T22:00:30.555555Z'::timestamp time"
        );

        assertSql(
                "time\n" +
                        "2022-03-11T22:00:30.555555555Z\n",
                "select '2022-03-11T22:00:30.555555555Z'::timestamp_ns time"
        );
    }

    @Test
    public void testValidAliasZone() throws Exception {
        assertSql(
                "zone\n" +
                        "2022-03-11T22:00:30.555555Z\n",
                "select '2022-03-11T22:00:30.555555Z'::timestamp zone"
        );

        assertSql(
                "zone\n" +
                        "2022-03-11T22:00:30.555555555Z\n",
                "select '2022-03-11T22:00:30.555555555Z'::timestamp_ns zone"
        );
    }

    @Test
    public void testVanilla() throws Exception {
        assertSql(
                "cast\n" +
                        "2022-03-11T22:00:30.555555Z\n",
                "select '2022-03-11T22:00:30.555555Z'::timestamp at time zone 'UTC'"
        );

        assertSql(
                "cast\n" +
                        "2022-03-11T22:00:30.555555555Z\n",
                "select '2022-03-11T22:00:30.555555555Z'::timestamp_ns at time zone 'UTC'"
        );
    }
}
