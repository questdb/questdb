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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.griffin.engine.functions.math.NegIntFunctionFactory;
import io.questdb.std.Numbers;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TimestampSequenceFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testIncrement2() throws SqlException {
        assertFunction(call(0L, 1000L).getFunction2());
    }

    @Test
    public void testInitCall() throws Exception {
        final String expected = "ts\n" +
                "2021-04-25T00:00:00.000000Z\n" +
                "2021-04-25T00:00:00.300000Z\n" +
                "2021-04-25T00:00:00.600000Z\n" +
                "2021-04-25T00:00:00.600000Z\n" +
                "2021-04-25T00:00:00.600000Z\n" +
                "2021-04-25T00:00:01.300000Z\n" +
                "2021-04-25T00:00:01.300000Z\n" +
                "2021-04-25T00:00:01.800000Z\n" +
                "2021-04-25T00:00:02.700000Z\n" +
                "2021-04-25T00:00:03.700000Z\n";

        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = compiler.compile("SELECT timestamp_sequence(\n" +
                            "         to_timestamp('2021-04-25T00:00:00', 'yyyy-MM-ddTHH:mm:ss'),\n" +
                            "         rnd_long(1,10,2) * 100000L\n" +
                            ") ts from long_sequence(10, 900, 800)",
                    sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true);
                    TestUtils.assertEquals(expected, sink);
                }
            } finally {
                sqlExecutionContext.setRandom(null);
            }
        });
    }

    @Test
    public void testTimestampSequenceWithSystimestampCall() throws Exception {
        final String expected = "ac\tts\n" +
                "1\t2020-06-05T13:27:54.334742Z\n" +
                "2\t2020-06-05T13:27:54.334795Z\n" +
                "3\t2020-06-05T13:27:54.334803Z\n" +
                "4\t2020-06-05T13:27:54.334810Z\n" +
                "5\t2020-06-05T13:27:54.334817Z\n" +
                "6\t2020-06-05T13:27:54.334823Z\n" +
                "7\t2020-06-05T13:27:54.334830Z\n" +
                "8\t2020-06-05T13:27:54.334836Z\n" +
                "9\t2020-06-05T13:27:54.334843Z\n" +
                "10\t2020-06-05T13:27:54.334859Z\n";

        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = compiler.compile("select x ac, timestamp_sequence(systimestamp(), 0) ts from long_sequence(10)",
                    sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true);
                    TestUtils.assertEquals(expected, sink);
                }
            } finally {
                sqlExecutionContext.setRandom(null);
            }
        });
    }

    @Override
    protected void addExtraFunctions() {
        functions.add(new NegIntFunctionFactory());
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new TimestampSequenceFunctionFactory();
    }

    private void assertFunction(Function function) {
        long next = 0;
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(next, function.getTimestamp(null));
            next += 1000L;
        }
    }
}