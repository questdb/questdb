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
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TimestampCeilFloorFunctionFactoryTest extends AbstractCairoTest {
    @Test
    public void testCeilInvalidKind() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("select timestamp_ceil('o', null)");
            } catch (SqlException e) {
                Assert.assertEquals(22, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid unit 'o'");
            }
        });
    }

    @Test
    public void testCeilNullKind() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("select timestamp_ceil(null, null)");
            } catch (SqlException e) {
                Assert.assertEquals(22, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid unit 'null'");
            }
        });
    }

    @Test
    public void testFloorEmptyStrKind() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("select timestamp_floor('', null)");
            } catch (SqlException e) {
                Assert.assertEquals(23, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid unit ''");
            }
        });
    }

    @Test
    public void testFloorInvalidKind() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("select timestamp_floor('z', null)");
            } catch (SqlException e) {
                Assert.assertEquals(23, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid unit 'z'");
            }
        });
    }

    @Test
    public void testFloorInvalidMinutesKind() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("select timestamp_floor('-3m', null)");
            } catch (SqlException e) {
                Assert.assertEquals(23, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid unit '-3m'");
            }
            try {
                assertExceptionNoLeakCheck("select timestamp_floor('0Y', null)");
            } catch (SqlException e) {
                Assert.assertEquals(23, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid unit '0Y'");
            }
        });
    }

    @Test
    public void testFloorNullKind() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertExceptionNoLeakCheck("select timestamp_floor(null, null)");
            } catch (SqlException e) {
                Assert.assertEquals(23, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid unit 'null'");
            }
        });
    }

    @Test
    public void testSimple() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "ts\tc_nano\tc_micro\tc_milli\tc_second\tc_minute\tc_hour\tc_day\tc_month\tc_week\tc_year\tc_null\tf_nano\tf_micro\tf_milli\tf_second\tf_minute\tf_hour\tf_day\tf_month\tf_week\tf_year\tf_null\n" +
                                    "2016-02-10T16:18:22.862145Z\t2016-02-10T16:18:22.862145Z\t2016-02-10T16:18:22.862145Z\t2016-02-10T16:18:22.863000Z\t2016-02-10T16:18:23.000000Z\t2016-02-10T16:19:00.000000Z\t2016-02-10T17:00:00.000000Z\t2016-02-11T00:00:00.000000Z\t2016-03-01T00:00:00.000000Z\t2016-02-15T00:00:00.000000Z\t2017-01-01T00:00:00.000000Z\t\t2016-02-10T16:18:22.862145Z\t2016-02-10T16:18:22.862145Z\t2016-02-10T16:18:22.862000Z\t2016-02-10T16:18:22.000000Z\t2016-02-10T16:18:00.000000Z\t2016-02-10T16:00:00.000000Z\t2016-02-10T00:00:00.000000Z\t2016-02-01T00:00:00.000000Z\t2016-02-08T00:00:00.000000Z\t2016-01-01T00:00:00.000000Z\t\n",
                            "with t as (\n" +
                                    "   select cast('2016-02-10T16:18:22.862145Z' as timestamp) ts\n" +
                                    ")\n" +
                                    "select\n" +
                                    "  ts\n" +
                                    "  , timestamp_ceil('n', ts) c_nano\n" +
                                    "  , timestamp_ceil('U', ts) c_micro\n" +
                                    "  , timestamp_ceil('T', ts) c_milli\n" +
                                    "  , timestamp_ceil('s', ts) c_second\n" +
                                    "  , timestamp_ceil('m', ts) c_minute\n" +
                                    "  , timestamp_ceil('h', ts) c_hour\n" +
                                    "  , timestamp_ceil('d', ts) c_day\n" +
                                    "  , timestamp_ceil('M', ts) c_month\n" +
                                    "  , timestamp_ceil('w', ts) c_week\n" +
                                    "  , timestamp_ceil('y', ts) c_year\n" +
                                    "  , timestamp_ceil('y', null) c_null\n" +
                                    "  , timestamp_floor('n', ts) f_nano\n" +
                                    "  , timestamp_floor('U', ts) f_micro\n" +
                                    "  , timestamp_floor('T', ts) f_milli\n" +
                                    "  , timestamp_floor('s', ts) f_second\n" +
                                    "  , timestamp_floor('m', ts) f_minute\n" +
                                    "  , timestamp_floor('h', ts) f_hour\n" +
                                    "  , timestamp_floor('d', ts) f_day\n" +
                                    "  , timestamp_floor('M', ts) f_month\n" +
                                    "  , timestamp_floor('w', ts) f_week\n" +
                                    "  , timestamp_floor('y', ts) f_year\n" +
                                    "  , timestamp_floor('y', null) f_null\n" +
                                    "  from t\n"
                    );

                    assertSql(
                            "ts\tc_nano\tc_micro\tc_milli\tc_second\tc_minute\tc_hour\tc_day\tc_month\tc_week\tc_year\tc_null\tf_nano\tf_micro\tf_milli\tf_second\tf_minute\tf_hour\tf_day\tf_month\tf_week\tf_year\tf_null\n" +
                                    "2016-02-10T16:18:22.862145123Z\t2016-02-10T16:18:22.862145123Z\t2016-02-10T16:18:22.862146000Z\t2016-02-10T16:18:22.863000000Z\t2016-02-10T16:18:23.000000000Z\t2016-02-10T16:19:00.000000000Z\t2016-02-10T17:00:00.000000000Z\t2016-02-11T00:00:00.000000000Z\t2016-03-01T00:00:00.000000000Z\t2016-02-15T00:00:00.000000000Z\t2017-01-01T00:00:00.000000000Z\t\t2016-02-10T16:18:22.862145123Z\t2016-02-10T16:18:22.862145000Z\t2016-02-10T16:18:22.862000000Z\t2016-02-10T16:18:22.000000000Z\t2016-02-10T16:18:00.000000000Z\t2016-02-10T16:00:00.000000000Z\t2016-02-10T00:00:00.000000000Z\t2016-02-01T00:00:00.000000000Z\t2016-02-08T00:00:00.000000000Z\t2016-01-01T00:00:00.000000000Z\t\n",
                            "with t as (\n" +
                                    "   select cast('2016-02-10T16:18:22.862145123Z' as timestamp_ns) ts\n" +
                                    ")\n" +
                                    "select\n" +
                                    "  ts\n" +
                                    "  , timestamp_ceil('n', ts) c_nano\n" +
                                    "  , timestamp_ceil('U', ts) c_micro\n" +
                                    "  , timestamp_ceil('T', ts) c_milli\n" +
                                    "  , timestamp_ceil('s', ts) c_second\n" +
                                    "  , timestamp_ceil('m', ts) c_minute\n" +
                                    "  , timestamp_ceil('h', ts) c_hour\n" +
                                    "  , timestamp_ceil('d', ts) c_day\n" +
                                    "  , timestamp_ceil('M', ts) c_month\n" +
                                    "  , timestamp_ceil('w', ts) c_week\n" +
                                    "  , timestamp_ceil('y', ts) c_year\n" +
                                    "  , timestamp_ceil('y', null) c_null\n" +
                                    "  , timestamp_ceil('n', ts) f_nano\n" +
                                    "  , timestamp_floor('U', ts) f_micro\n" +
                                    "  , timestamp_floor('T', ts) f_milli\n" +
                                    "  , timestamp_floor('s', ts) f_second\n" +
                                    "  , timestamp_floor('m', ts) f_minute\n" +
                                    "  , timestamp_floor('h', ts) f_hour\n" +
                                    "  , timestamp_floor('d', ts) f_day\n" +
                                    "  , timestamp_floor('M', ts) f_month\n" +
                                    "  , timestamp_floor('w', ts) f_week\n" +
                                    "  , timestamp_floor('y', ts) f_year\n" +
                                    "  , timestamp_floor('y', null) f_null\n" +
                                    "  from t\n"
                    );
                });
    }

    @Test
    public void testSimpleFloorWithStride() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    "ts\tf_micro\tf_milli\tf_second\tf_minute\tf_hour\tf_day\tf_week\n" +
                            "2016-02-10T16:18:22.862145Z\t2016-02-10T16:18:22.862140Z\t2016-02-10T16:18:22.850000Z\t2016-02-10T16:18:20.000000Z\t2016-02-10T16:15:00.000000Z\t2016-02-10T15:00:00.000000Z\t2016-02-09T00:00:00.000000Z\t2016-02-08T00:00:00.000000Z\n",
                    "with t as (\n" +
                            "   select cast('2016-02-10T16:18:22.862145Z' as timestamp) ts\n" +
                            ")\n" +
                            "select\n" +
                            "  ts\n" +
                            "  , timestamp_floor('10U', ts) f_micro\n" +
                            "  , timestamp_floor('25T', ts) f_milli\n" +
                            "  , timestamp_floor('20s', ts) f_second\n" +
                            "  , timestamp_floor('5m', ts) f_minute\n" +
                            "  , timestamp_floor('9h', ts) f_hour\n" +
                            "  , timestamp_floor('4d', ts) f_day\n" +
                            "  , timestamp_floor('3w', ts) f_week\n" +
                            "  from t\n"
            );

            assertSql(
                    "ts\tf_micro\tf_milli\tf_second\tf_minute\tf_hour\tf_day\tf_week\n" +
                            "2016-02-10T16:18:22.862145123Z\t2016-02-10T16:18:22.862140000Z\t2016-02-10T16:18:22.850000000Z\t2016-02-10T16:18:20.000000000Z\t2016-02-10T16:15:00.000000000Z\t2016-02-10T15:00:00.000000000Z\t2016-02-09T00:00:00.000000000Z\t2016-02-08T00:00:00.000000000Z\n",
                    "with t as (\n" +
                            "   select cast('2016-02-10T16:18:22.862145123Z' as timestamp_ns) ts\n" +
                            ")\n" +
                            "select\n" +
                            "  ts\n" +
                            "  , timestamp_floor('10U', ts) f_micro\n" +
                            "  , timestamp_floor('25T', ts) f_milli\n" +
                            "  , timestamp_floor('20s', ts) f_second\n" +
                            "  , timestamp_floor('5m', ts) f_minute\n" +
                            "  , timestamp_floor('9h', ts) f_hour\n" +
                            "  , timestamp_floor('4d', ts) f_day\n" +
                            "  , timestamp_floor('3w', ts) f_week\n" +
                            "  from t\n"
            );
        });
    }
}
