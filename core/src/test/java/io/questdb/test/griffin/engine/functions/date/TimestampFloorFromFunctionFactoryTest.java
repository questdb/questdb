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

public class TimestampFloorFromFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBasicFlooring() throws Exception {
        assertMemoryLeak(() -> {
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2017-12-30T00:00:00.000000Z\n",
                    "select timestamp_floor('5d', '2018-01-01T00:00:00.000000Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2018-01-01T00:00:00.000000Z\n",
                    "select timestamp_floor('5d', '2018-01-01T00:00:00.000000Z', '2018-01-01T00:00:00.000000Z')"
            );

            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2017-12-30T00:00:00.000000000Z\n",
                    "select timestamp_floor('5d', '2018-01-01T00:00:00.000000123Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2018-01-01T00:00:00.000000000Z\n",
                    "select timestamp_floor('5d', '2018-01-01T00:00:00.000000123Z', '2018-01-01T00:00:00.000000Z')"
            );
        });
    }

    @Test
    public void testDaysFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T00:00:00.000000Z\n",
                    "select timestamp_floor('3d', '2016-02-10T16:18:22.862145Z', '2016-02-10T00:00:00Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-03-08T00:00:00.000000Z\n",
                    "select timestamp_floor('3d', '2016-03-10T16:18:22.862145Z', '2016-02-10T00:00:00Z')"
            );
        });
    }

    @Test
    public void testHoursFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T00:00:00.000000Z\n",
                    "select timestamp_floor('3h', '2016-02-10T01:18:22.862145Z', '2016-02-10T00:00:00Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T15:00:00.000000Z\n",
                    "select timestamp_floor('3h', '2016-02-10T16:18:22.862145Z', '2016-02-10T00:00:00Z')"
            );
        });

        assertMemoryLeak(() -> {
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T00:00:00.000000000Z\n",
                    "select timestamp_floor('3h', '2016-02-10T01:18:22.862145123Z', '2016-02-10T00:00:00Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T15:00:00.000000000Z\n",
                    "select timestamp_floor('3h', '2016-02-10T16:18:22.862145123Z', '2016-02-10T00:00:00Z')"
            );
        });
    }

    @Test
    public void testMicrosecondsFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:18:18.862143Z\n",
                    "select timestamp_floor('3U', '2016-02-10T16:18:18.862144Z', '2016-02-10T16:18:18.123456Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:18:18.862144Z\n",
                    "select timestamp_floor('3U', '2016-02-10T16:18:18.862145Z', '2016-02-10T16:18:18.862144Z')"
            );

            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:18:18.862143789Z\n",
                    "select timestamp_floor('3U', '2016-02-10T16:18:18.862144123Z', '2016-02-10T16:18:18.123456789Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:18:18.862144789Z\n",
                    "select timestamp_floor('3U', '2016-02-10T16:18:18.862145123Z', '2016-02-10T16:18:18.862144789Z')"
            );
        });
    }

    @Test
    public void testMillisecondsFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:18:22.862000Z\n",
                    "select timestamp_floor('3T', '2016-02-10T16:18:22.862145Z', '2016-02-10T16:18:22.850000Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:18:22.860000Z\n",
                    "select timestamp_floor('3T', '2016-02-10T16:18:22.862145Z', '2016-02-10T00:00:00Z')"
            );

            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:18:22.862000000Z\n",
                    "select timestamp_floor('3T', '2016-02-10T16:18:22.862145123Z', '2016-02-10T16:18:22.850000Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:18:22.860000000Z\n",
                    "select timestamp_floor('3T', '2016-02-10T16:18:22.862145123Z', '2016-02-10T00:00:00Z')"
            );
        });
    }

    @Test
    public void testMinutesFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:18:00.000000Z\n",
                    "select timestamp_floor('6m', '2016-02-10T16:18:22.862145Z', '2016-02-10T16:00:00Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:00:00.000000Z\n",
                    "select timestamp_floor('6m', '2016-02-10T16:02:00.000000Z', '2016-02-10T16:00:00Z')"
            );

            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:18:00.000000000Z\n",
                    "select timestamp_floor('6m', '2016-02-10T16:18:22.862145123Z', '2016-02-10T16:00:00Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:00:00.000000000Z\n",
                    "select timestamp_floor('6m', '2016-02-10T16:02:00.000000123Z', '2016-02-10T16:00:00Z')"
            );
        });
    }

    @Test
    public void testMonthsFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T00:00:00.000000000Z\n",
                    "select timestamp_floor('3M', '2016-02-10T16:18:22.862145123Z', '2016-02-10T00:00:00Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-05-10T00:00:00.000000000Z\n",
                    "select timestamp_floor('3M', '2016-07-10T16:18:22.862145123Z', '2016-02-10T00:00:00Z')"
            );
        });
    }

    @Test
    public void testNanosecondsFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:18:18.862143Z\n",
                    "select timestamp_floor('3n', '2016-02-10T16:18:18.862144Z', '2016-02-10T16:18:18.123456Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:18:18.862144Z\n",
                    "select timestamp_floor('3n', '2016-02-10T16:18:18.862145Z', '2016-02-10T16:18:18.862144Z')"
            );

            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:18:18.862144122Z\n",
                    "select timestamp_floor('3n', '2016-02-10T16:18:18.862144123Z', '2016-02-10T16:18:18.123456789Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:18:18.862145122Z\n",
                    "select timestamp_floor('3n', '2016-02-10T16:18:18.862145123Z', '2016-02-10T16:18:18.862144789Z')"
            );

            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:18:18.862140000Z\n",
                    "select timestamp_floor('10000n', '2016-02-10T16:18:18.862145123Z', null)"
            );
        });
    }

    @Test
    public void testNullFrom() throws Exception {
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "timestamp_floor\n" +
                        "2017-12-30T00:00:00.000000Z\n",
                "select timestamp_floor('5d', '2018-01-01T00:00:00.000000Z', null)"
        ));

        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                "timestamp_floor\n" +
                        "2017-12-30T00:00:00.000000000Z\n",
                "select timestamp_floor('5d', '2018-01-01T00:00:00.000000000Z', null)"
        ));
    }

    @Test
    public void testSecondsFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:18:18.000000Z\n",
                    "select timestamp_floor('6s', '2016-02-10T16:18:22.862145Z', '2016-02-10T00:00:00Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T00:00:00.000000Z\n",
                    "select timestamp_floor('6s', '2016-02-10T00:00:04.862145Z', '2016-02-10T00:00:00Z')"
            );

            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:18:18.000000000Z\n",
                    "select timestamp_floor('6s', '2016-02-10T16:18:22.862145123Z', '2016-02-10T00:00:00Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T00:00:00.000000000Z\n",
                    "select timestamp_floor('6s', '2016-02-10T00:00:04.862145123Z', '2016-02-10T00:00:00Z')"
            );
        });
    }

    @Test
    public void testWeeksFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T00:00:00.000000Z\n",
                    "select timestamp_floor('3w', '2016-02-10T16:18:22.862145Z', '2016-02-10T00:00:00Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-03-02T00:00:00.000000Z\n",
                    "select timestamp_floor('3w', '2016-03-10T16:18:22.862145Z', '2016-02-10T00:00:00Z')"
            );

            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T00:00:00.000000000Z\n",
                    "select timestamp_floor('3w', '2016-02-10T16:18:22.862145123Z', '2016-02-10T00:00:00Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-03-02T00:00:00.000000000Z\n",
                    "select timestamp_floor('3w', '2016-03-10T16:18:22.862145123Z', '2016-02-10T00:00:00Z')"
            );
        });
    }

    @Test
    public void testYearsFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:00:00.000000Z\n",
                    "select timestamp_floor('2y', '2016-02-10T16:18:22.862145Z', '2016-02-10T16:00:00Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2018-02-10T16:00:00.000000Z\n",
                    "select timestamp_floor('2y', '2019-02-10T16:02:00.000000Z', '2016-02-10T16:00:00Z')"
            );

            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2016-02-10T16:00:00.000000000Z\n",
                    "select timestamp_floor('2y', '2016-02-10T16:18:22.862145123Z', '2016-02-10T16:00:00Z')"
            );
            assertQueryNoLeakCheck(
                    "timestamp_floor\n" +
                            "2018-02-10T16:00:00.000000000Z\n",
                    "select timestamp_floor('2y', '2019-02-10T16:02:00.000000123Z', '2016-02-10T16:00:00Z')"
            );
        });
    }
}
