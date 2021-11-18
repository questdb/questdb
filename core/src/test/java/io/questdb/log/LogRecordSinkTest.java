/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.log;

import io.questdb.std.*;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LogRecordSinkTest {

    private StringSink sink;

    @Before
    public void setUp() {
        sink = new StringSink();
    }

    @Test
    public void testEndToEnd() {
        String expected = "ﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺﾺ";
        int memorySize = Numbers.ceilPow2(20);
        long memoryPtr = Unsafe.malloc(memorySize, MemoryTag.NATIVE_DEFAULT);
        try {
            LogRecordSink recordSink = new LogRecordSink(memoryPtr, memorySize);
            recordSink.setLevel(LogLevel.ERROR);
            Assert.assertEquals(LogLevel.ERROR, recordSink.getLevel());
            Assert.assertEquals(memoryPtr, recordSink.getAddress());

            // fill it in with rubbish
            for (int i = 0; i < memorySize; i++) {
                recordSink.put('º'); // non ascii
            }
            recordSink.toSink(sink);
            Assert.assertEquals(memorySize, recordSink.length());
            Assert.assertEquals(memorySize, sink.length());
            Assert.assertEquals(expected, sink.toString());

            recordSink.clear(16);
            sink.clear();
            recordSink.put(expected, 16, 32);
            recordSink.toSink(sink);
            Assert.assertEquals(memorySize, recordSink.length());
            Assert.assertEquals(memorySize, sink.length());
            Assert.assertEquals(expected, sink.toString());

            recordSink.clear(0);
            sink.clear();
            recordSink.put(expected, 16, 32);
            recordSink.toSink(sink);
            Assert.assertEquals(16, recordSink.length());
            Assert.assertEquals(16, sink.length());
            Assert.assertEquals(expected.substring(16, 32), sink.toString());

            recordSink.clear();
            sink.clear();
            recordSink.put(expected.toCharArray(), 16, 16);
            recordSink.toSink(sink);
            Assert.assertEquals(16, recordSink.length());
            Assert.assertEquals(16, sink.length());
            Assert.assertEquals(expected.substring(16, 32), sink.toString());

        } finally {
            Unsafe.free(memoryPtr, memorySize, MemoryTag.NATIVE_DEFAULT);
        }

    }
}
