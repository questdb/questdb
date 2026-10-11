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
    public void testBareColumnOperand() throws Exception {
        assertMemoryLeak(() -> {
            createTzTable();
            assertQuery("SELECT ts AT TIME ZONE 'America/New_York' x FROM tz")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            2022-03-11T17:00:00.000000Z
                            2022-07-11T18:00:00.000000Z
                            """);
            assertQuery("SELECT tz.ts AT TIME ZONE 'EST' AT TIME ZONE 'UTC' x FROM tz")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            2022-03-11T17:00:00.000000Z
                            2022-07-11T18:00:00.000000Z
                            """);
            assertQuery("SELECT n AT TIME ZONE 'America/New_York' x FROM tz")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            2022-03-11T17:00:00.000000001Z
                            2022-07-11T18:00:00.000000001Z
                            """);
        });
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
    public void testColumnZone() throws Exception {
        assertMemoryLeak(() -> {
            createTzTable();
            assertQuery("SELECT ts AT TIME ZONE z x FROM tz")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            2022-03-11T17:00:00.000000Z
                            2022-07-11T18:00:00.000000Z
                            """);
            assertQuery("SELECT (ts) AT TIME ZONE z x FROM tz")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            2022-03-11T17:00:00.000000Z
                            2022-07-11T18:00:00.000000Z
                            """);
            assertQuery("SELECT ts AT TIME ZONE z + 1 x FROM tz")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            2022-03-11T17:00:00.000001Z
                            2022-07-11T18:00:00.000001Z
                            """);
            assertQuery("SELECT '2022-03-11T22:00:00.000000Z'::timestamp AT TIME ZONE z x FROM tz")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            2022-03-11T17:00:00.000000Z
                            2022-03-11T17:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testColumnZoneInWhere() throws Exception {
        assertMemoryLeak(() -> {
            createTzTable();
            assertQuery("SELECT ts FROM tz WHERE ts AT TIME ZONE z < '2022-03-11T18'")
                    .noLeakCheck()
                    .returns("""
                            ts
                            2022-03-11T22:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testDoubleColonAfterZoneCastsZone() throws Exception {
        assertMemoryLeak(() -> {
            createTzTable();
            // PostgreSQL rule: '::' binds tighter than AT TIME ZONE, so a cast written directly
            // after the zone applies to the zone; parentheses cast the converted timestamp
            assertQuery("SELECT (ts) AT TIME ZONE 'EST'::string x, typeOf((ts) AT TIME ZONE 'EST'::string) t FROM tz")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\tt
                            2022-03-11T17:00:00.000000Z\tTIMESTAMP
                            2022-07-11T18:00:00.000000Z\tTIMESTAMP
                            """);
            assertQuery("SELECT ts AT TIME ZONE 'EST'::string x, typeOf(ts AT TIME ZONE 'EST'::string) t FROM tz")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\tt
                            2022-03-11T17:00:00.000000Z\tTIMESTAMP
                            2022-07-11T18:00:00.000000Z\tTIMESTAMP
                            """);
            assertQuery("SELECT (ts AT TIME ZONE 'EST')::string x, typeOf((ts AT TIME ZONE 'EST')::string) t FROM tz")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\tt
                            2022-03-11T17:00:00.000000Z\tSTRING
                            2022-07-11T18:00:00.000000Z\tSTRING
                            """);
        });
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
    public void testPlusBindsLooserThanAtTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            createTzTable();
            assertQuery("SELECT 1 + ts AT TIME ZONE 'America/New_York' x FROM tz")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            1647018000000001
                            1657562400000001
                            """);
        });
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
                        to_timezone
                        2022-03-11T22:00:30.555555Z
                        """);

        assertQuery("select '2022-03-11T22:00:30.555555555Z'::timestamp_ns at time zone 'UTC'")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        to_timezone
                        2022-03-11T22:00:30.555555555Z
                        """);
    }

    private static void createTzTable() throws Exception {
        execute("CREATE TABLE tz (ts TIMESTAMP, z VARCHAR, n TIMESTAMP_NS)");
        execute("""
                INSERT INTO tz VALUES
                    ('2022-03-11T22:00:00.000000Z', 'EST', '2022-03-11T22:00:00.000000001Z'),
                    ('2022-07-11T22:00:00.000000Z', 'America/New_York', '2022-07-11T22:00:00.000000001Z')
                """);
    }
}
