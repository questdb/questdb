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

package io.questdb.test.cutlass.http;

import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.arr.ArrayState;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.DirectArrayView;
import io.questdb.cairo.arr.NoopArrayState;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

public class ArrayBufferOverflowTest extends AbstractTest {
    private static final TestHttpClient testHttpClient = new TestHttpClient();

    @AfterClass
    public static void tearDownStatic() {
        testHttpClient.close();
        AbstractTest.tearDownStatic();
        assert Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN) == 0;
    }

    @Test
    public void testArrayToTextContinuity() {
        try (DirectArrayView arrayView = new DirectArrayView(new DefaultCairoConfiguration(root))) {
            var rnd = TestUtils.generateRandom(LOG);
            rnd.nextDoubleArray(2, arrayView, 1, 10, 0);
            var sinkActual = new StringSink();
            var sinkExpected = new StringSink();
            // we do not split values yet, such as double, so the sink should be
            // large enough for at least 1 value.
            var chunkSize = Math.max(31, rnd.nextInt(10000));
            var sinkInterrupted = new Utf16Sink() {
                private boolean bufOut = true;

                @Override
                public Utf16Sink put(char c) {
                    if (sinkActual.length() > 0 && (sinkActual.length() % chunkSize) == 0) {
                        if (bufOut) {
                            // interrupt the sinking on each chunk
                            // when we reach the chunk, we expect code retry to climb over
                            // this chunk, but fail on the next one
                            bufOut = false;
                            throw new RuntimeException("buf out");
                        } else {
                            bufOut = true;
                        }
                    }
                    return sinkActual.put(c);
                }
            };

            var statePrototype = new ArrayState() {
                final int[] contender = new int[STATE_MAX];
                final int[] target = new int[STATE_MAX];
                int flatIndex;
                int sinkLen = 0;

                @Override
                public void record(int flatIndex) {
                    this.sinkLen = sinkActual.length();
                    this.flatIndex = flatIndex;
                }

                @Override
                public boolean notRecorded(int flatIndex) {
                    return this.flatIndex <= flatIndex;
                }

                @Override
                public void putAsciiIfNotRecorded(int eventType, int eventDelta, CharSink<?> sink, char symbol) {
                    if ((contender[eventType] += eventDelta) > target[eventType]) {
                        sink.put(symbol);
                        this.sinkLen = sinkActual.length();
                        target[eventType] = contender[eventType];
                    }
                }
            };

            while (true) {
                try {
                    ArrayTypeDriver.arrayToJson(
                            arrayView,
                            sinkInterrupted,
                            ArrayTypeDriver::appendDoubleFromArrayToSink,
                            statePrototype
                    );
                    break;
                } catch (Throwable e) {
                    sinkActual.trimTo(statePrototype.sinkLen);
                    Arrays.fill(statePrototype.contender, 0);
                }
            }

            ArrayTypeDriver.arrayToJson(
                    arrayView,
                    sinkExpected,
                    ArrayTypeDriver::appendDoubleFromArrayToSink,
                    NoopArrayState.INSTANCE
            );

            TestUtils.assertEquals(sinkExpected, sinkActual);
        }
    }

    @Test
    @Ignore
    public void testSimple() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> testHttpClient.assertGet(
                "{\"query\":\"select rnd_double_array(6, 1, 0, 10, 10, 10, 10, 10, 10);\",\"columns\":[{\"name\":\"rnd_double_array\",\"type\":\"ARRAY\",\"dim\":6,\"elemType\":\"DOUBLE\"}],\"timestamp\":-1,\"dataset\":[[]],\"count\":1,\"error\":\"HTTP 400 (Bad request), response buffer is too small for the column value [columnName=rnd_double_array, columnIndex=0]\"}",
                "select rnd_double_array(6, 1, 0, 10, 10, 10, 10, 10, 10);"
        )));
    }
}
