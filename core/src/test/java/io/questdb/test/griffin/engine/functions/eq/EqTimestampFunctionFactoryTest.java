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

package io.questdb.test.griffin.engine.functions.eq;

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.eq.EqTimestampFunctionFactory;
import io.questdb.std.NumericException;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class EqTimestampFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testEquals() throws SqlException, NumericException {
        long t1 = MicrosTimestampDriver.floor("2020-12-31T23:59:59.000000Z");
        long t2 = MicrosTimestampDriver.floor("2020-12-31T23:59:59.000001Z");
        callBySignature("=(NN)", t1, t1).andAssert(true);
        callBySignature("=(NN)", t1, t2).andAssert(false);

        long t3 = NanosTimestampDriver.floor("2020-12-31T23:59:59.000000000Z");
        long t4 = NanosTimestampDriver.floor("2020-12-31T23:59:59.000000001Z");
        callBySignature("=(NN)", t3, t3).andAssert(true);
        callBySignature("=(NN)", t3, t4).andAssert(false);
    }

    @Test
    public void testMixedMicrosAndNanos() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select " +
                    "timestamp_sequence(1000000, 1000000) as ts, " +
                    "timestamp_sequence(0::timestamp_ns, 2000000000) as ts_ns " +
                    "from long_sequence(10)" +
                    ") timestamp(ts)");
            assertQuery("ts\tts_ns\tcolumn\n" +
                            "1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:00.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:02.000000000Z\ttrue\n" +
                            "1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:04.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:06.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:05.000000Z\t1970-01-01T00:00:08.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:06.000000Z\t1970-01-01T00:00:10.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:07.000000Z\t1970-01-01T00:00:12.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:08.000000Z\t1970-01-01T00:00:14.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:09.000000Z\t1970-01-01T00:00:16.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:10.000000Z\t1970-01-01T00:00:18.000000000Z\tfalse\n",
                    "select ts, ts_ns, ts = ts_ns from x");
            assertQuery("ts\tts_ns\tcolumn\n" +
                            "1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:00.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:02.000000000Z\ttrue\n" +
                            "1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:04.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:06.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:05.000000Z\t1970-01-01T00:00:08.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:06.000000Z\t1970-01-01T00:00:10.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:07.000000Z\t1970-01-01T00:00:12.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:08.000000Z\t1970-01-01T00:00:14.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:09.000000Z\t1970-01-01T00:00:16.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:10.000000Z\t1970-01-01T00:00:18.000000000Z\tfalse\n",
                    "select ts, ts_ns, ts_ns = ts from x");

            assertQuery("ts\tcolumn\tcolumn1\n" +
                            "1970-01-01T00:00:01.000000Z\tfalse\tfalse\n" +
                            "1970-01-01T00:00:02.000000Z\tfalse\tfalse\n" +
                            "1970-01-01T00:00:03.000000Z\ttrue\tfalse\n" +
                            "1970-01-01T00:00:04.000000Z\tfalse\tfalse\n" +
                            "1970-01-01T00:00:05.000000Z\tfalse\tfalse\n" +
                            "1970-01-01T00:00:06.000000Z\tfalse\tfalse\n" +
                            "1970-01-01T00:00:07.000000Z\tfalse\tfalse\n" +
                            "1970-01-01T00:00:08.000000Z\tfalse\tfalse\n" +
                            "1970-01-01T00:00:09.000000Z\tfalse\tfalse\n" +
                            "1970-01-01T00:00:10.000000Z\tfalse\tfalse\n",
                    "select ts, ts = '1970-01-01T00:00:03.000000000Z',ts = '1970-01-01T00:00:03.000000001Z'  from x");
            assertQuery("ts_ns\tcolumn\n" +
                            "1970-01-01T00:00:00.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:02.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:04.000000000Z\ttrue\n" +
                            "1970-01-01T00:00:06.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:08.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:10.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:12.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:14.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:16.000000000Z\tfalse\n" +
                            "1970-01-01T00:00:18.000000000Z\tfalse\n",
                    "select ts_ns, '1970-01-01T00:00:04.000000Z' = ts_ns  from x");
        });
    }

    @Test
    public void testNotEquals() throws SqlException, NumericException {
        long t1 = MicrosTimestampDriver.floor("2020-12-31T23:59:59.000000Z");
        long t2 = MicrosTimestampDriver.floor("2020-12-31T23:59:59.000001Z");
        callBySignature("<>(NN)", t1, t1).andAssert(false);
        callBySignature("<>(NN)", t1, t2).andAssert(true);

        long t3 = NanosTimestampDriver.floor("2020-12-31T23:59:59.000000000Z");
        long t4 = NanosTimestampDriver.floor("2020-12-31T23:59:59.000000001Z");
        callBySignature("<>(NN)", t3, t3).andAssert(false);
        callBySignature("<>(NN)", t3, t4).andAssert(true);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new EqTimestampFunctionFactory();
    }
}
