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

public class TimestampFloorOffsetFunctionFactoryTest extends AbstractCairoTest {

//
//    @Test
//    public void testFloorEmptyStrKind() throws Exception {
//        assertMemoryLeak(() -> {
//            try {
//                assertExceptionNoLeakCheck("select timestamp_floor('', null)");
//            } catch (SqlException e) {
//                Assert.assertEquals(23, e.getPosition());
//                TestUtils.assertContains("invalid unit ''", e.getFlyweightMessage());
//            }
//        });
//    }
//
//    @Test
//    public void testFloorInvalidKind() throws Exception {
//        assertMemoryLeak(() -> {
//            try {
//                assertExceptionNoLeakCheck("select timestamp_floor('z', null)");
//            } catch (SqlException e) {
//                Assert.assertEquals(23, e.getPosition());
//                TestUtils.assertContains("invalid unit 'z'", e.getFlyweightMessage());
//            }
//        });
//    }
//
//    @Test
//    public void testFloorInvalidMinutesKind() throws Exception {
//        assertMemoryLeak(() -> {
//            try {
//                assertExceptionNoLeakCheck("select timestamp_floor('-3m', null)");
//            } catch (SqlException e) {
//                Assert.assertEquals(23, e.getPosition());
//                TestUtils.assertContains("invalid unit '-3m'", e.getFlyweightMessage());
//            }
//            try {
//                assertExceptionNoLeakCheck("select timestamp_floor('0Y', null)");
//            } catch (SqlException e) {
//                Assert.assertEquals(23, e.getPosition());
//                TestUtils.assertContains("invalid unit '0Y'", e.getFlyweightMessage());
//            }
//        });
//    }
//
//    @Test
//    public void testFloorNullKind() throws Exception {
//        assertMemoryLeak(() -> {
//            try {
//                assertExceptionNoLeakCheck("select timestamp_floor(null, null)");
//            } catch (SqlException e) {
//                Assert.assertEquals(23, e.getPosition());
//                TestUtils.assertContains("invalid unit 'null'", e.getFlyweightMessage());
//            }
//        });
//    }

    @Test
    public void testAAASimpleFloorWithStride() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "ts\tf_milli\tf_second\tf_minute\tf_hour\tf_day\tf_week\n" +
                        "2016-02-10T16:18:22.862145Z\t2016-02-10T16:18:22.850000Z\t2016-02-10T16:18:20.000000Z\t2016-02-10T16:15:00.000000Z\t2016-02-10T15:00:00.000000Z\t2016-02-10T00:00:00.000000Z\t2016-02-08T00:00:00.000000Z\n", "with t as (\n" +
                        "   select cast('2016-02-10T16:18:22.862145Z' as timestamp) ts\n" +
                        ")\n" +
                        "select\n" +
                        "  ts\n" +
                        "  , timestamp_floor_offset('25T', ts, '2016-02-10T00:00:00.123456Z') f_milli\n" +
//                        "  , timestamp_floor_offset('20s', ts, '2016-02-10T00:00:00Z') f_second\n" +
//                        "  , timestamp_floor_offset('5m', ts, '2016-02-10T00:00:00Z') f_minute\n" +
//                        "  , timestamp_floor_offset('9h', ts, '2016-02-10T00:00:00Z') f_hour\n" +
                        "  , timestamp_floor_offset('4d', ts, '2016-02-10T00:00:00Z') f_day\n" +
                        "  , timestamp_floor_offset('3w', ts, '2016-02-10T00:00:00Z') f_week\n" +
                        "  from t\n"
        ));
    }

    @Test
    public void testDaysFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
                    assertSql("timestamp_floor_offset\n" +
                                    "2016-02-10T00:00:00.000000Z\n",
                            "select timestamp_floor_offset('3d', '2016-02-10T16:18:22.862145Z', '2016-02-10T00:00:00Z')");
                    assertSql("timestamp_floor_offset\n" +
                                    "2016-03-08T00:00:00.000000Z\n",
                            "select timestamp_floor_offset('3d', '2016-03-10T16:18:22.862145Z', '2016-02-10T00:00:00Z')");
                }

        );
    }

    @Test
    public void testHoursFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
                    assertSql("timestamp_floor_offset\n" +
                                    "2016-02-10T00:00:00.000000Z\n",
                            "select timestamp_floor_offset('3h', '2016-02-10T01:18:22.862145Z', '2016-02-10T00:00:00Z')");
                    assertSql("timestamp_floor_offset\n" +
                                    "2016-02-10T15:00:00.000000Z\n",
                            "select timestamp_floor_offset('3h', '2016-02-10T16:18:22.862145Z', '2016-02-10T00:00:00Z')");
                }

        );
    }

    @Test
    public void testMillisecondsFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
                    assertSql("timestamp_floor_offset\n" +
                                    "2016-02-10T16:18:22.862000Z\n",
                            "select timestamp_floor_offset('3T', '2016-02-10T16:18:22.862145Z', '2016-02-10T16:18:22.850000Z')");
                    assertSql("timestamp_floor_offset\n" +
                                    "2016-02-10T16:18:22.860000Z\n",
                            "select timestamp_floor_offset('3T', '2016-02-10T16:18:22.862145Z', '2016-02-10T00:00:00Z')");
                }

        );
    }

    @Test
    public void testMonthsFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
                    assertSql("timestamp_floor_offset\n" +
                                    "2016-02-10T00:00:00.000000Z\n",
                            "select timestamp_floor_offset('3M', '2016-02-10T16:18:22.862145Z', '2016-02-10T00:00:00Z')");
                    assertSql("timestamp_floor_offset\n" +
                                    "2016-02-10T00:00:00.000000Z\n",
                            "select timestamp_floor_offset('3M', '2016-02-10T16:18:22.862145Z', '2016-02-10T00:00:00Z')");
                }

        );
    }

    @Test
    public void testSecondsFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
                    assertSql("timestamp_floor_offset\n" +
                                    "2016-02-10T16:18:18.000000Z\n",
                            "select timestamp_floor_offset('6s', '2016-02-10T16:18:22.862145Z', '2016-02-10T00:00:00Z')");
                    assertSql("timestamp_floor_offset\n" +
                                    "2016-02-10T00:00:00.000000Z\n",
                            "select timestamp_floor_offset('6s', '2016-02-10T00:00:04.862145Z', '2016-02-10T00:00:00Z')");
                }

        );
    }

    @Test
    public void testWeeksFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
                    assertSql("timestamp_floor_offset\n" +
                                    "2016-02-10T00:00:00.000000Z\n",
                            "select timestamp_floor_offset('3w', '2016-02-10T16:18:22.862145Z', '2016-02-10T00:00:00Z')");
                    assertSql("timestamp_floor_offset\n" +
                                    "2016-03-02T00:00:00.000000Z\n",
                            "select timestamp_floor_offset('3w', '2016-03-10T16:18:22.862145Z', '2016-02-10T00:00:00Z')");
                }

        );
    }
}
