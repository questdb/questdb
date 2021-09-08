/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.griffin.SqlException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TimestampCeilFloorFunctionFactoryTest extends AbstractGriffinTest {
    @Test
    public void testSimple() throws Exception {
        assertMemoryLeak(() -> TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                "with t as (\n" +
                        "   select cast('2016-02-10T16:18:22.862145Z' as timestamp) ts\n" +
                        ")\n" +
                        "select\n" +
                        "  ts\n" +
                        "  , timestamp_ceil('milli', ts) c_milli\n" +
                        "  , timestamp_ceil('second', ts) c_second\n" +
                        "  , timestamp_ceil('minute', ts) c_minute\n" +
                        "  , timestamp_ceil('hour', ts) c_hour\n" +
                        "  , timestamp_ceil('day', ts) c_day\n" +
                        "  , timestamp_ceil('month', ts) c_month\n" +
                        "  , timestamp_ceil('year', ts) c_year\n" +
                        "  , timestamp_ceil('year', null) c_null\n" +
                        "  , timestamp_floor('milli', ts) f_milli\n" +
                        "  , timestamp_floor('second', ts) f_second\n" +
                        "  , timestamp_floor('minute', ts) f_minute\n" +
                        "  , timestamp_floor('hour', ts) f_hour\n" +
                        "  , timestamp_floor('day', ts) f_day\n" +
                        "  , timestamp_floor('month', ts) f_month\n" +
                        "  , timestamp_floor('year', ts) f_year\n" +
                        "  , timestamp_floor('year', null) f_null\n" +
                        "  from t\n",
                sink,
                "ts\tc_milli\tc_second\tc_minute\tc_hour\tc_day\tc_month\tc_year\tc_null\tf_milli\tf_second\tf_minute\tf_hour\tf_day\tf_month\tf_year\tf_null\n" +
                        "2016-02-10T16:18:22.862145Z\t2016-02-10T16:18:22.862999Z\t2016-02-10T16:18:22.999999Z\t2016-02-10T16:18:59.999999Z\t2016-02-10T16:59:59.999999Z\t2016-02-10T23:59:59.999999Z\t2016-02-29T23:59:59.999999Z\t2016-12-31T23:59:59.999999Z\t\t2016-02-10T16:18:22.862000Z\t2016-02-10T16:18:22.000000Z\t2016-02-10T16:18:00.000000Z\t2016-02-10T16:00:00.000000Z\t2016-02-10T00:00:00.000000Z\t2016-02-01T00:00:00.000000Z\t2016-01-01T00:00:00.000000Z\t\n"
        ));
    }

    @Test
    public void testFloorNullKind() throws Exception {
        assertMemoryLeak(() -> {
            try {
                compiler.compile(
                        "select timestamp_floor(null, null)",
                         sqlExecutionContext
                );
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(23, e.getPosition());
                TestUtils.assertContains("invalid kind ''", e.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testFloorInvalidKind() throws Exception {
        assertMemoryLeak(() -> {
            try {
                compiler.compile(
                        "select timestamp_floor('hello', null)",
                        sqlExecutionContext
                );
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(23, e.getPosition());
                TestUtils.assertContains("invalid kind 'hello'", e.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testCeilNullKind() throws Exception {
        assertMemoryLeak(() -> {
            try {
                compiler.compile(
                        "select timestamp_ceil(null, null)",
                        sqlExecutionContext
                );
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(22, e.getPosition());
                TestUtils.assertContains("invalid kind ''", e.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testCeilInvalidKind() throws Exception {
        assertMemoryLeak(() -> {
            try {
                compiler.compile(
                        "select timestamp_ceil('hello', null)",
                        sqlExecutionContext
                );
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(22, e.getPosition());
                TestUtils.assertContains("invalid kind 'hello'", e.getFlyweightMessage());
            }
        });
    }
}
