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
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

public class TimestampFloorFromOffsetFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNulls() throws Exception {
        assertMemoryLeak(() -> assertTimestampFloor(
                """
                        timestamp_floor
                        2017-12-30T00:00:00.000000Z
                        """,
                "5d", "2018-01-01T00:00:00.000000Z", null, null, null
        ));

        assertMemoryLeak(() -> assertTimestampFloor(
                """
                        timestamp_floor
                        2017-12-30T00:00:00.000000000Z
                        """,
                "5d", "2018-01-01T00:00:00.000000001Z", null, null, null
        ));
    }

    @Test
    public void testDaysFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3d", null, null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3d", null, null, "00:00", "GMT+01:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3d", null, null, "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-08T00:00:00.000000Z
                            """,
                    "3d", "2016-02-10T16:18:22.862145Z", null, "00:00", "GMT+1"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-08T00:00:00.000000Z
                            """,
                    "3d", "2016-02-10T16:18:22.862145Z", null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-08T00:00:00.000000Z
                            """,
                    "3d", "2016-02-10T16:18:22.862145Z", null, "00:00", "GMT+01:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-08T00:00:00.000000Z
                            """,
                    "3d", "2016-02-10T16:18:22.862145Z", null, "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T00:00:00.000000Z
                            """,
                    "3d", "2016-02-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T00:00:00.000000Z
                            """,
                    "3d", "2016-02-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:00", "GMT+01:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T00:00:00.000000Z
                            """,
                    "3d", "2016-02-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-03-08T00:15:00.000000Z
                            """,
                    "3d", "2016-03-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:15", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-03-08T00:15:00.000000Z
                            """,
                    "3d", "2016-03-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:15", "GMT+02:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-03-08T00:15:00.000000Z
                            """,
                    "3d", "2016-03-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:15", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-08T00:00:00.000000000Z
                            """,
                    "3d", "2016-02-10T16:18:22.862145123Z", null, "00:00", "GMT+1"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-08T00:00:00.000000000Z
                            """,
                    "3d", "2016-02-10T16:18:22.862145123Z", null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-08T00:00:00.000000000Z
                            """,
                    "3d", "2016-02-10T16:18:22.862145123Z", null, "00:00", "GMT+01:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-08T00:00:00.000000000Z
                            """,
                    "3d", "2016-02-10T16:18:22.862145342Z", null, "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T00:00:00.000000000Z
                            """,
                    "3d", "2016-02-10T16:18:22.862145123Z", "2016-02-10T00:00:00Z", "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T00:00:00.000000000Z
                            """,
                    "3d", "2016-02-10T16:18:22.862145098Z", "2016-02-10T00:00:00Z", "00:00", "GMT+01:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T00:00:00.000000000Z
                            """,
                    "3d", "2016-02-10T16:18:22.862145123Z", "2016-02-10T00:00:00Z", "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-03-08T00:15:00.000000Z
                            """,
                    "3d", "2016-03-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:15", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-03-08T00:15:00.000000000Z
                            """,
                    "3d", "2016-03-10T16:18:22.862145123Z", "2016-02-10T00:00:00Z", "00:15", "GMT+02:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-03-08T00:15:00.000000000Z
                            """,
                    "3d", "2016-03-10T16:18:22.862145456Z", "2016-02-10T00:00:00Z", "00:15", "Europe/London"
            );
        });
    }

    @Test
    public void testExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp);");

            assertQuery("select timestamp_floor('3d', ts, null, '00:00', null) from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('3d',ts)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select timestamp_floor('3d', ts, null, '00:00', 'UTC+01:00') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('3d',ts,null,'00:00','UTC+01:00')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select timestamp_floor('3d', ts, '1980-01-01T16:00:00.000000001Z', null, 'UTC+12:00') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('3d',ts,'1980-01-01T16:00:00.000Z','00:00','UTC+12:00')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select timestamp_floor('3d', ts, '1980-01-01T16:00:00.000000Z', '00:15', null) from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('3d',ts,'1980-01-01T16:15:00.000Z')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select timestamp_floor('3d', ts, null, '00:00', 'Europe/Berlin') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('3d',ts,null,'00:00','Europe/Berlin')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select timestamp_floor('3d', ts, '1980-01-01T00:00:00.000000Z', null, 'Europe/Berlin') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('3d',ts,'1980-01-01T00:00:00.000Z','00:00','Europe/Berlin')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select timestamp_floor('33m', ts, '1980-01-01T16:00:00.000000Z', null, 'Europe/Berlin') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('33m',ts,'1980-01-01T16:00:00.000Z','00:00','Europe/Berlin')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select timestamp_floor('33m', ts, null, '00:00', 'Europe/Berlin') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('33m',ts,null,'00:00','Europe/Berlin')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select timestamp_floor('3d', ts, null, '12:00', 'Europe/Berlin') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('3d',ts,null,'12:00','Europe/Berlin')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select timestamp_floor('1d', ts, null, '00:01', null) from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,'1970-01-01T00:01:00.000Z')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("offset", "00:00");
            assertQuery("select timestamp_floor('d', ts, null, :offset, null) from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,null,:offset::string,null)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("offset", "00:00");
            assertQuery("select timestamp_floor('d', ts, '2016-02-10T16:00:00.000Z', :offset, 'UTC+00:00') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,'2016-02-10T16:00:00.000Z',:offset::string,'UTC+00:00')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("offset", "00:00");
            assertQuery("select timestamp_floor('d', ts, null, :offset, 'Europe/London') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,null,:offset::string,'Europe/London')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("offset", "00:00");
            assertQuery("select timestamp_floor('d', ts, '2022-02-02T01:00:00.000Z', :offset, 'Europe/London') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,'2022-02-02T01:00:00.000Z',:offset::string,'Europe/London')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("tz", "UTC");
            assertQuery("select timestamp_floor('d', ts, null, '00:00', :tz) from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,null,'00:00',:tz::string)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("tz", "UTC");
            assertQuery("select timestamp_floor('d', ts, '1980-01-01T00:00:00.000000Z', null, :tz) from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,'1980-01-01T00:00:00.000Z','00:00',:tz::string)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("tz", "Europe/Paris");
            assertQuery("select timestamp_floor('d', ts, null, '03:00', :tz) from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,null,'03:00',:tz::string)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("offset", "00:00");
            bindVariableService.setStr("tz", "UTC");
            assertQuery("select timestamp_floor('1d', ts, null, :offset, :tz) from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,null,:offset::string,:tz::string)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("offset", "00:00");
            bindVariableService.setStr("tz", "UTC");
            assertQuery("select timestamp_floor('1d', ts, '2016-02-10T16:18:22.862Z', :offset, :tz) from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,'2016-02-10T16:18:22.862Z',:offset::string,:tz::string)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);
        });
    }

    @Test
    public void testExplainPlanOnNanos() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp_ns);");

            assertQuery("select timestamp_floor('3d', ts, null, '00:00', null) from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('3d',ts)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select timestamp_floor('3d', ts, null, '00:00', 'UTC+01:00') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('3d',ts,null,'00:00','UTC+01:00')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select timestamp_floor('3d', ts, '1980-01-01T16:00:00.000000123Z', null, 'UTC+12:00') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('3d',ts,'1980-01-01T16:00:00.000Z','00:00','UTC+12:00')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select timestamp_floor('3d', ts, '1980-01-01T16:00:00.000000Z', '00:15', null) from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('3d',ts,'1980-01-01T16:15:00.000Z')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select timestamp_floor('3d', ts, null, '00:00', 'Europe/Berlin') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('3d',ts,null,'00:00','Europe/Berlin')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select timestamp_floor('3d', ts, '1980-01-01T00:00:00.000000000Z', null, 'Europe/Berlin') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('3d',ts,'1980-01-01T00:00:00.000Z','00:00','Europe/Berlin')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select timestamp_floor('33m', ts, '1980-01-01T16:00:00.000000098Z', null, 'Europe/Berlin') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('33m',ts,'1980-01-01T16:00:00.000Z','00:00','Europe/Berlin')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select timestamp_floor('33m', ts, null, '00:00', 'Europe/Berlin') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('33m',ts,null,'00:00','Europe/Berlin')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select timestamp_floor('3d', ts, null, '12:00', 'Europe/Berlin') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('3d',ts,null,'12:00','Europe/Berlin')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            assertQuery("select timestamp_floor('1d', ts, null, '00:01', null) from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,'1970-01-01T00:01:00.000Z')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("offset", "00:00");
            assertQuery("select timestamp_floor('d', ts, null, :offset, null) from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,null,:offset::string,null)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("offset", "00:00");
            assertQuery("select timestamp_floor('d', ts, '2016-02-10T16:00:00.000000Z', :offset, 'UTC+00:00') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,'2016-02-10T16:00:00.000Z',:offset::string,'UTC+00:00')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("offset", "00:00");
            assertQuery("select timestamp_floor('d', ts, null, :offset, 'Europe/London') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,null,:offset::string,'Europe/London')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("offset", "00:00");
            assertQuery("select timestamp_floor('d', ts, '2022-02-02T01:00:00.000Z', :offset, 'Europe/London') from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,'2022-02-02T01:00:00.000Z',:offset::string,'Europe/London')]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("tz", "UTC");
            assertQuery("select timestamp_floor('d', ts, null, '00:00', :tz) from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,null,'00:00',:tz::string)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("tz", "UTC");
            assertQuery("select timestamp_floor('d', ts, '1980-01-01T00:00:00.000000Z', null, :tz) from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,'1980-01-01T00:00:00.000Z','00:00',:tz::string)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("tz", "Europe/Paris");
            assertQuery("select timestamp_floor('d', ts, null, '03:00', :tz) from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,null,'03:00',:tz::string)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("offset", "00:00");
            bindVariableService.setStr("tz", "UTC");
            assertQuery("select timestamp_floor('1d', ts, null, :offset, :tz) from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,null,:offset::string,:tz::string)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);

            bindVariableService.clear();
            bindVariableService.setStr("offset", "00:00");
            bindVariableService.setStr("tz", "UTC");
            assertQuery("select timestamp_floor('1d', ts, '2016-02-10T16:18:22.862123123Z', :offset, :tz) from x")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [timestamp_floor('1d',ts,'2016-02-10T16:18:22.862Z',:offset::string,:tz::string)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: x
                            """);
        });
    }

    @Test
    public void testFloorDstGap() throws Exception {
        assertMemoryLeak(() -> {
            // verify that timestamp_floor() never returns timestamps from gaps

            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "1h", null, null, "00:10", "Europe/Berlin"
            );

            // 2021-03-28T02:00 - 2021-03-28T03:00 is a gap hour in local time
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2021-03-28T00:10:00.000000Z
                            """,
                    "1h", "2021-03-28T00:00:00.000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2021-03-28T00:10:00.000000Z
                            """,
                    "1h", "2021-03-28T00:01:00.000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2021-03-28T01:10:00.000000Z
                            """,
                    "1h", "2021-03-28T00:11:00.000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2021-03-28T01:10:00.000000Z
                            """,
                    "1h", "2021-03-28T01:00:00.000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2021-03-28T01:10:00.000000Z
                            """,
                    "1h", "2021-03-28T01:01:00.000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2021-03-28T03:10:00.000000Z
                            """,
                    "1h", "2021-03-28T01:11:00.000Z", null, "00:10", "Europe/Berlin"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2021-03-28T00:10:00.000000000Z
                            """,
                    "1h", "2021-03-28T00:00:00.000000000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2021-03-28T00:10:00.000000000Z
                            """,
                    "1h", "2021-03-28T00:01:00.000000000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2021-03-28T01:10:00.000000000Z
                            """,
                    "1h", "2021-03-28T00:11:00.000000000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2021-03-28T01:10:00.000000000Z
                            """,
                    "1h", "2021-03-28T01:00:00.000000000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2021-03-28T01:10:00.000000000Z
                            """,
                    "1h", "2021-03-28T01:01:00.000000000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2021-03-28T03:10:00.000000000Z
                            """,
                    "1h", "2021-03-28T01:11:00.000000000Z", null, "00:10", "Europe/Berlin"
            );

            // 1997-03-30T02:00 - 1997-03-30T03:00 is a gap hour in local time
            assertTimestampFloor(
                    """
                            timestamp_floor
                            1997-03-30T00:10:00.000000Z
                            """,
                    "1h", "1997-03-30T00:00:00.000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            1997-03-30T00:10:00.000000Z
                            """,
                    "1h", "1997-03-30T00:01:00.000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            1997-03-30T01:10:00.000000Z
                            """,
                    "1h", "1997-03-30T00:11:00.000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            1997-03-30T01:10:00.000000Z
                            """,
                    "1h", "1997-03-30T01:00:00.000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            1997-03-30T01:10:00.000000Z
                            """,
                    "1h", "1997-03-30T01:01:00.000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            1997-03-30T03:10:00.000000Z
                            """,
                    "1h", "1997-03-30T01:11:00.000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            1997-03-30T00:10:00.000000000Z
                            """,
                    "1h", "1997-03-30T00:00:00.000000000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            1997-03-30T00:10:00.000000000Z
                            """,
                    "1h", "1997-03-30T00:01:00.000000000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            1997-03-30T01:10:00.000000000Z
                            """,
                    "1h", "1997-03-30T00:11:00.000000000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            1997-03-30T01:10:00.000000000Z
                            """,
                    "1h", "1997-03-30T01:00:00.000000000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            1997-03-30T01:10:00.000000000Z
                            """,
                    "1h", "1997-03-30T01:01:00.000000000Z", null, "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            1997-03-30T03:10:00.000000000Z
                            """,
                    "1h", "1997-03-30T01:11:00.000000000Z", null, "00:10", "Europe/Berlin"
            );
        });
    }

    @Test
    public void testHoursFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3h", null, null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3h", null, null, "00:00", "GMT+01:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3h", null, null, "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T01:15:00.000000Z
                            """,
                    "3h", "2016-02-10T01:18:22.862145Z", "2016-02-10T00:00:00Z", "01:15", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T07:15:00.000000Z
                            """,
                    "3h", "2016-02-10T01:18:22.862145Z", "2016-02-10T00:00:00Z", "01:15", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T01:15:00.000000Z
                            """,
                    "3h", "2016-02-10T01:18:22.862145Z", "2016-02-10T00:00:00Z", "01:15", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:00:00.000000Z
                            """,
                    "1h", "2016-02-10T16:18:22.862145Z", null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T22:00:00.000000Z
                            """,
                    "1h", "2016-02-10T16:18:22.862145Z", null, "00:00", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T17:00:00.000000Z
                            """,
                    "1h", "2016-02-10T16:18:22.862145Z", null, "00:00", "Europe/Berlin"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T15:30:00.000000Z
                            """,
                    "3h", "2016-02-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:30", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T21:30:00.000000Z
                            """,
                    "3h", "2016-02-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:30", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T15:30:00.000000Z
                            """,
                    "3h", "2016-02-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:30", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T01:15:00.000000000Z
                            """,
                    "3h", "2016-02-10T01:18:22.862145123Z", "2016-02-10T00:00:00Z", "01:15", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T07:15:00.000000000Z
                            """,
                    "3h", "2016-02-10T01:18:22.862145123Z", "2016-02-10T00:00:00Z", "01:15", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T01:15:00.000000000Z
                            """,
                    "3h", "2016-02-10T01:18:22.862145123Z", "2016-02-10T00:00:00Z", "01:15", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:00:00.000000000Z
                            """,
                    "1h", "2016-02-10T16:18:22.862145123Z", null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T22:00:00.000000000Z
                            """,
                    "1h", "2016-02-10T16:18:22.862145123Z", null, "00:00", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T17:00:00.000000000Z
                            """,
                    "1h", "2016-02-10T16:18:22.862145123Z", null, "00:00", "Europe/Berlin"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T15:30:00.000000000Z
                            """,
                    "3h", "2016-02-10T16:18:22.862145123Z", "2016-02-10T00:00:00Z", "00:30", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T21:30:00.000000000Z
                            """,
                    "3h", "2016-02-10T16:18:22.862145123Z", "2016-02-10T00:00:00Z", "00:30", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T15:30:00.000000000Z
                            """,
                    "3h", "2016-02-10T16:18:22.862145123Z", "2016-02-10T00:00:00Z", "00:30", "Europe/London"
            );
        });
    }

    @Test
    public void testInvalidOffset() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select timestamp_floor('1h', '2018-02-10T21:00:00.000000001Z', null, 'foobar', null)")
                    .noLeakCheck()
                    .fails(69, "invalid offset: foobar");

            bindVariableService.clear();
            bindVariableService.setStr("offset", "foobar");
            assertQuery("select timestamp_floor('1h', '2018-02-10T21:00:00.000000Z', null, :offset, null)")
                    .noLeakCheck()
                    .fails(66, "invalid offset: foobar");

            bindVariableService.clear();
            bindVariableService.setStr("offset", "foobar");
            assertQuery("select timestamp_floor('1h', '2018-02-10T21:00:00.000000000Z', null, :offset, 'Europe/London')")
                    .noLeakCheck()
                    .fails(69, "invalid offset: foobar");

            bindVariableService.clear();
            bindVariableService.setStr("offset", "foobar");
            bindVariableService.setStr("tz", "Europe/London");
            assertQuery("select timestamp_floor('1h', '2018-02-10T21:00:00.000000012Z', null, :offset, :tz)")
                    .noLeakCheck()
                    .fails(69, "invalid offset: foobar");

            assertQuery("select timestamp_floor('1h', '2018-02-10T21:00:00.000000Z', null, rnd_str('foobar'), null)")
                    .noLeakCheck()
                    .fails(66, "const or runtime const expected");

            assertQuery("select timestamp_floor('1h', '2018-02-10T21:00:00.000000123Z', null, rnd_str('foobar'), 'UTC')")
                    .noLeakCheck()
                    .fails(69, "const or runtime const expected");

            assertQuery("select timestamp_floor('1h', '2018-02-10T21:00:00.000000000Z', null, rnd_str('foobar'), 'Europe/London')")
                    .noLeakCheck()
                    .fails(69, "const or runtime const expected");

            bindVariableService.clear();
            bindVariableService.setStr("tz", "Europe/London");
            assertQuery("select timestamp_floor('1h', '2018-02-10T21:00:00.000000123Z', null, rnd_str('foobar'), :tz)")
                    .noLeakCheck()
                    .fails(69, "const or runtime const expected");
        });
    }

    @Test
    public void testInvalidTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select timestamp_floor('1h', '2018-02-10T21:00:00.000000Z', null, '00:00', 'foobar')")
                    .noLeakCheck()
                    .fails(75, "invalid timezone: foobar");

            bindVariableService.clear();
            bindVariableService.setStr("tz", "foobar");
            assertQuery("select timestamp_floor('1h', '2018-02-10T21:00:00.000000123Z', null, '00:00', :tz)")
                    .noLeakCheck()
                    .fails(78, "invalid timezone: foobar");

            bindVariableService.clear();
            bindVariableService.setStr("tz", "foobar");
            assertQuery("select timestamp_floor('41m', '2016-02-10T16:18:22.862145232Z', '2016-02-10T00:00:00Z', '00:00', :tz)")
                    .noLeakCheck()
                    .fails(97, "invalid timezone: foobar");

            bindVariableService.clear();
            bindVariableService.setStr("offset", "00:00");
            bindVariableService.setStr("tz", "foobar");
            assertQuery("select timestamp_floor('1h', '2018-02-10T21:00:00.000000Z', null, :offset, :tz)")
                    .noLeakCheck()
                    .fails(75, "invalid timezone: foobar");

            bindVariableService.clear();
            bindVariableService.setStr("offset", "00:01");
            bindVariableService.setStr("tz", "foobar");
            assertQuery("select timestamp_floor('2d', '2016-02-10T16:18:22.862145000Z', '2016-02-10T00:00:00Z', :offset, :tz)")
                    .noLeakCheck()
                    .fails(96, "invalid timezone: foobar");

            assertQuery("select timestamp_floor('1h', '2018-02-10T21:00:00.000000123Z', null, '00:00', rnd_str('foobar'))")
                    .noLeakCheck()
                    .fails(78, "const or runtime const expected");
        });
    }

    @Test
    public void testInvalidUnit() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select timestamp_floor('z', '2018-02-10T21:00:00.000000Z', null, '00:00', null)")
                    .noLeakCheck()
                    .fails(23, "Invalid unit: z");

            assertQuery("select timestamp_floor('z', '2018-02-10T21:00:00.000000123Z', null, '00:00', null)")
                    .noLeakCheck()
                    .fails(23, "Invalid unit: z");
        });
    }

    @Test
    public void testMicrosecondsFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3U", null, null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3U", null, null, "00:00", "GMT+01:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3U", null, null, "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862143Z
                            """,
                    "3U", "2016-02-10T16:18:18.862144Z", "2016-02-10T13:18:18.123456Z", "00:30", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T22:18:18.862143Z
                            """,
                    "3U", "2016-02-10T16:18:18.862144Z", "2016-02-10T13:18:18.123456Z", "00:30", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862143Z
                            """,
                    "3U", "2016-02-10T16:18:18.862144Z", "2016-02-10T13:18:18.123456Z", "00:30", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862144Z
                            """,
                    "1U", "2016-02-10T16:18:18.862144Z", null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T22:18:18.862144Z
                            """,
                    "1U", "2016-02-10T16:18:18.862144Z", null, "00:00", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862144Z
                            """,
                    "1U", "2016-02-10T16:18:18.862144Z", null, "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862144Z
                            """,
                    "3U", "2016-02-10T16:18:18.862145Z", "2016-02-10T13:18:18.862144000Z", "00:01", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T22:18:18.862144Z
                            """,
                    "3U", "2016-02-10T16:18:18.862145Z", "2016-02-10T13:18:18.862144000Z", "00:01", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862144Z
                            """,
                    "3U", "2016-02-10T16:18:18.862145Z", "2016-02-10T13:18:18.862144123Z", "00:01", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862143000Z
                            """,
                    "3U", "2016-02-10T16:18:18.862144123Z", "2016-02-10T13:18:18.123456Z", "00:30", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T22:18:18.862143000Z
                            """,
                    "3U", "2016-02-10T16:18:18.862144123Z", "2016-02-10T13:18:18.123456Z", "00:30", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862143000Z
                            """,
                    "3U", "2016-02-10T16:18:18.862144123Z", "2016-02-10T13:18:18.123456Z", "00:30", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862144000Z
                            """,
                    "1U", "2016-02-10T16:18:18.862144123Z", null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T22:18:18.862144000Z
                            """,
                    "1U", "2016-02-10T16:18:18.862144123Z", null, "00:00", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862144000Z
                            """,
                    "1U", "2016-02-10T16:18:18.862144123Z", null, "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862144000Z
                            """,
                    "3U", "2016-02-10T16:18:18.862145123Z", "2016-02-10T13:18:18.862144Z", "00:01", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T22:18:18.862144000Z
                            """,
                    "3U", "2016-02-10T16:18:18.862145123Z", "2016-02-10T13:18:18.862144Z", "00:01", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862144000Z
                            """,
                    "3U", "2016-02-10T16:18:18.862145123Z", "2016-02-10T13:18:18.862144Z", "00:01", "Europe/London"
            );
        });
    }

    @Test
    public void testMillisecondsFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3T", null, null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3T", null, null, "00:00", "GMT+01:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3T", null, null, "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:19:22.850000Z
                            """,
                    "3T", "2016-02-10T16:18:22.862145Z", "2016-02-10T16:18:22.850000Z", "00:01", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T22:18:22.862000Z
                            """,
                    "3T", "2016-02-10T16:18:22.862145Z", "2016-02-10T16:18:22.850000Z", "00:01", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:19:22.850000Z
                            """,
                    "3T", "2016-02-10T16:18:22.862145Z", "2016-02-10T16:18:22.850000Z", "00:01", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:22.862000Z
                            """,
                    "1T", "2016-02-10T16:18:22.862145Z", null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T22:18:22.862000Z
                            """,
                    "1T", "2016-02-10T16:18:22.862145Z", null, "00:00", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:22.862000Z
                            """,
                    "1T", "2016-02-10T16:18:22.862145Z", null, "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:22.860000Z
                            """,
                    "3T", "2016-02-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:10", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T17:18:22.860000Z
                            """,
                    "3T", "2016-02-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:10", "GMT+01:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T17:18:22.860000Z
                            """,
                    "3T", "2016-02-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:10", "Europe/Berlin"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T17:18:22.860000000Z
                            """,
                    "3T", "2016-02-10T16:18:22.862145792Z", "2016-02-10T00:00:00Z", "00:10", "Europe/Berlin"
            );
        });
    }

    @Test
    public void testMinutesFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "6m", null, null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "6m", null, null, "00:00", "GMT+01:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "6m", null, null, "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:00.000000Z
                            """,
                    "6m", "2016-02-10T16:18:22.862145Z", "2016-02-10T15:00:00Z", "01:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T19:18:00.000000Z
                            """,
                    "6m", "2016-02-10T16:18:22.862145Z", "2016-02-10T15:00:00Z", "01:00", "GMT+03:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T17:18:00.000000Z
                            """,
                    "6m", "2016-02-10T16:18:22.862145Z", "2016-02-10T15:00:00Z", "01:00", "Europe/Paris"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:00.000000Z
                            """,
                    "1m", "2016-02-10T16:18:22.862145Z", null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T19:18:00.000000Z
                            """,
                    "1m", "2016-02-10T16:18:22.862145Z", null, "00:00", "GMT+03:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T17:18:00.000000000Z
                            """,
                    "1m", "2016-02-10T16:18:22.862145123Z", null, "00:00", "Europe/Paris"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:00:00.000000Z
                            """,
                    "6m", "2016-02-10T16:02:00.000000Z", "2016-02-10T16:00:00Z", "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T19:00:00.000000Z
                            """,
                    "6m", "2016-02-10T16:02:00.000000Z", "2016-02-10T16:00:00Z", "00:00", "GMT+03:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T17:00:00.000000000Z
                            """,
                    "6m", "2016-02-10T16:02:00.000000123Z", "2016-02-10T16:00:00Z", "00:00", "Europe/Paris"
            );
        });
    }

    @Test
    public void testMonthsFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3M", null, null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3M", null, null, "00:00", "GMT+01:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3M", null, null, "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T01:00:00.000000Z
                            """,
                    "3M", "2016-02-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "01:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T01:00:00.000000Z
                            """,
                    "3M", "2016-02-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "01:00", "GMT+02:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T01:00:00.000000Z
                            """,
                    "3M", "2016-02-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "01:00", "Europe/Sofia"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-01-01T00:00:00.000000Z
                            """,
                    "3M", "2016-02-10T16:18:22.862145Z", null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-01-01T00:00:00.000000Z
                            """,
                    "3M", "2016-02-10T16:18:22.862145Z", null, "00:00", "GMT+02:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-01-01T00:00:00.000000Z
                            """,
                    "3M", "2016-02-10T16:18:22.862145Z", null, "00:00", "Europe/Sofia"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-05-10T12:00:00.000000Z
                            """,
                    "3M", "2016-07-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "12:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-05-10T12:00:00.000000Z
                            """,
                    "3M", "2016-07-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "12:00", "GMT+02:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-05-10T12:00:00.000000Z
                            """,
                    "3M", "2016-07-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "12:00", "Europe/Sofia"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-05-10T12:00:00.000000000Z
                            """,
                    "3M", "2016-07-10T16:18:22.862145123Z", "2016-02-10T00:00:00Z", "12:00", "GMT+02:00"
            );
        });
    }

    @Test
    public void testNanoSecondsFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3n", null, null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3n", null, null, "00:00", "GMT+01:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3n", null, null, "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862143Z
                            """,
                    "3n", "2016-02-10T16:18:18.862144Z", "2016-02-10T13:18:18.123456Z", "00:30", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T22:18:18.862143Z
                            """,
                    "3n", "2016-02-10T16:18:18.862144Z", "2016-02-10T13:18:18.123456Z", "00:30", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862143Z
                            """,
                    "3n", "2016-02-10T16:18:18.862144Z", "2016-02-10T13:18:18.123456Z", "00:30", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862143Z
                            """,
                    "3n", "2016-02-10T16:18:18.862144Z", null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T22:18:18.862143Z
                            """,
                    "3n", "2016-02-10T16:18:18.862144Z", null, "00:00", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862143Z
                            """,
                    "3n", "2016-02-10T16:18:18.862144Z", null, "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862144Z
                            """,
                    "3n", "2016-02-10T16:18:18.862145Z", "2016-02-10T13:18:18.862144000Z", "00:01", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T22:18:18.862144Z
                            """,
                    "3n", "2016-02-10T16:18:18.862145Z", "2016-02-10T13:18:18.862144000Z", "00:01", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862144Z
                            """,
                    "3n", "2016-02-10T16:18:18.862145Z", "2016-02-10T13:18:18.862144123Z", "00:01", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:48:18.123456000Z
                            """,
                    "3n", "2016-02-10T16:18:18.862144123Z", "2016-02-10T16:18:18.123456Z", "00:30", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T22:18:18.862144122Z
                            """,
                    "3n", "2016-02-10T16:18:18.862144123Z", "2016-02-10T13:18:18.123456Z", "00:30", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862144122Z
                            """,
                    "3n", "2016-02-10T16:18:18.862144123Z", "2016-02-10T13:18:18.123456Z", "00:30", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862144122Z
                            """,
                    "3n", "2016-02-10T16:18:18.862144123Z", null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T22:18:18.862144122Z
                            """,
                    "3n", "2016-02-10T16:18:18.862144123Z", null, "00:00", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862144123Z
                            """,
                    "1n", "2016-02-10T16:18:18.862144123Z", null, "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:19:18.862144000Z
                            """,
                    "3n", "2016-02-10T16:18:18.862145123Z", "2016-02-10T16:18:18.862144Z", "00:01", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T22:18:18.862145122Z
                            """,
                    "3n", "2016-02-10T16:18:18.862145123Z", "2016-02-10T13:18:18.862144Z", "00:01", "GMT+06:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.862145122Z
                            """,
                    "3n", "2016-02-10T16:18:18.862145123Z", "2016-02-10T13:18:18.862144000Z", "00:01", "Europe/London"
            );
        });
    }

    @Test
    public void testSecondsFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "6s", null, null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "6s", null, null, "00:00", "GMT+01:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "6s", null, null, "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.000000Z
                            """,
                    "6s", "2016-02-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:01", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:19:18.000000Z
                            """,
                    "6s", "2016-02-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:01", "GMT+00:01"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:18.000000Z
                            """,
                    "6s", "2016-02-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:01", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:22.000000000Z
                            """,
                    "1s", "2016-02-10T16:18:22.862145123Z", null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:19:22.000000Z
                            """,
                    "1s", "2016-02-10T16:18:22.862145Z", null, "00:00", "GMT+00:01"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T16:18:22.000000Z
                            """,
                    "1s", "2016-02-10T16:18:22.862145Z", null, "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T00:02:00.000000Z
                            """,
                    "6s", "2016-02-10T00:00:04.862145Z", "2016-02-10T00:00:00Z", "00:02", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T00:02:00.000000Z
                            """,
                    "6s", "2016-02-10T00:00:04.862145Z", "2016-02-10T00:00:00Z", "00:02", "GMT+00:01"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T00:00:00.000000Z
                            """,
                    "6s", "2016-02-10T00:00:04.862145Z", "2016-02-10T00:00:00Z", "00:00", "Europe/London"
            );
        });
    }

    @Test
    public void testWeeksFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3w", null, null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3w", null, null, "00:00", "GMT+01:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3w", null, null, "00:00", "Europe/London"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "3w", null, null, null, "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T00:01:00.000000Z
                            """,
                    "3w", "2016-02-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:01", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T00:01:00.000000Z
                            """,
                    "3w", "2016-02-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:01", "GMT+03:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T00:01:00.000000Z
                            """,
                    "3w", "2016-02-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:01", "Europe/Prague"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-08T00:00:00.000000Z
                            """,
                    "1w", "2016-02-10T16:18:22.862145Z", null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-08T00:00:00.000000Z
                            """,
                    "1w", "2016-02-10T16:18:22.862145Z", null, "00:00", "GMT+03:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-08T00:00:00.000000Z
                            """,
                    "1w", "2016-02-10T16:18:22.862145Z", null, "00:00", "Europe/Prague"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-03-02T00:42:00.000000Z
                            """,
                    "3w", "2016-03-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:42", "GMT+03:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-03-02T00:42:00.000000Z
                            """,
                    "3w", "2016-03-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:42", "Europe/Prague"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-03-02T00:42:00.000000Z
                            """,
                    "3w", "2016-03-10T16:18:22.862145Z", "2016-02-10T00:00:00Z", "00:42", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-03-02T00:42:00.000000000Z
                            """,
                    "3w", "2016-03-10T16:18:22.862145123Z", "2016-02-10T00:00:00Z", "00:42", null
            );
        });
    }

    @Test
    public void testYearsFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "2y", null, null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "2y", null, null, "00:00", "GMT+01:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            
                            """,
                    "2y", null, null, "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T19:00:00.000000Z
                            """,
                    "2y", "2016-02-10T16:18:22.862145Z", "2016-02-10T16:00:00Z", "03:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T19:00:00.000000Z
                            """,
                    "2y", "2016-02-10T16:18:22.862145Z", "2016-02-10T16:00:00Z", "03:00", "GMT+09:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-02-10T19:00:00.000000Z
                            """,
                    "2y", "2016-02-10T16:18:22.862145Z", "2016-02-10T16:00:00Z", "03:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-01-01T00:00:00.000000Z
                            """,
                    "1y", "2016-02-10T16:18:22.862145Z", null, "00:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-01-01T00:00:00.000000Z
                            """,
                    "1y", "2016-02-10T16:18:22.862145Z", null, "00:00", "GMT+09:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2016-01-01T00:00:00.000000Z
                            """,
                    "1y", "2016-02-10T16:18:22.862145Z", null, "00:00", "Europe/London"
            );

            assertTimestampFloor(
                    """
                            timestamp_floor
                            2018-02-10T21:00:00.000000Z
                            """,
                    "2y", "2019-02-10T16:02:00.000000Z", "2016-02-10T16:00:00Z", "05:00", null
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2018-02-10T21:00:00.000000Z
                            """,
                    "2y", "2019-02-10T16:02:00.000000Z", "2016-02-10T16:00:00Z", "05:00", "GMT+09:00"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2018-02-10T21:00:00.000000Z
                            """,
                    "2y", "2019-02-10T16:02:00.000000Z", "2016-02-10T16:00:00Z", "05:00", "Europe/London"
            );
            assertTimestampFloor(
                    """
                            timestamp_floor
                            2018-02-10T21:00:00.000000000Z
                            """,
                    "2y", "2019-02-10T16:02:00.000000123Z", "2016-02-10T16:00:00Z", "05:00", "Europe/London"
            );
        });
    }

    private void assertTimestampFloor(
            String expected,
            String interval,
            @Nullable String timestamp,
            @Nullable String from,
            @Nullable String offset,
            @Nullable String timezone
    ) throws Exception {
        assertQuery("select timestamp_floor('" +
                interval + "', " +
                (timestamp != null ? "'" + timestamp + "'" : "null") + ", " +
                (from != null ? "'" + from + "'" : "null") + ", " +
                (offset != null ? "'" + offset + "'" : "null") + ", " +
                (timezone != null ? "'" + timezone + "'" : "null") +
                ")")
                .noLeakCheck()
                .expectSize()
                .returns(expected);

        // offset bind var only
        bindVariableService.clear();
        bindVariableService.setStr("offset", offset);
        assertQuery("select timestamp_floor('" +
                interval + "', " +
                (timestamp != null ? "'" + timestamp + "'" : "null") + ", " +
                (from != null ? "'" + from + "'" : "null") + ", " +
                ":offset, " +
                (timezone != null ? "'" + timezone + "'" : "null") +
                ")")
                .noLeakCheck()
                .expectSize()
                .returns(expected);

        // time zone bind var only
        bindVariableService.clear();
        bindVariableService.setStr("tz", timezone);
        assertQuery("select timestamp_floor('" +
                interval + "', " +
                (timestamp != null ? "'" + timestamp + "'" : "null") + ", " +
                (from != null ? "'" + from + "'" : "null") + ", " +
                (offset != null ? "'" + offset + "'" : "null") + ", " +
                ":tz" +
                ")")
                .noLeakCheck()
                .expectSize()
                .returns(expected);

        // both offset and time zone bind vars
        bindVariableService.clear();
        bindVariableService.setStr("offset", offset);
        bindVariableService.setStr("tz", timezone);
        assertQuery("select timestamp_floor('" +
                interval + "', " +
                (timestamp != null ? "'" + timestamp + "'" : "null") + ", " +
                (from != null ? "'" + from + "'" : "null") + ", " +
                ":offset, " +
                ":tz" +
                ")")
                .noLeakCheck()
                .expectSize()
                .returns(expected);
    }
}
