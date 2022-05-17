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

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
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
    public void testConvoluted() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String expected = "ππππππππππππππππππππ"; // len == 20
            final int len = expected.length();
            final int buffSize = len * 3;
            final long buffPtr = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);
            try {
                LogRecordSink recordSink = new LogRecordSink(buffPtr, buffSize);
                recordSink.setLevel(LogLevel.ERROR);
                Assert.assertEquals(LogLevel.ERROR, recordSink.getLevel());
                Assert.assertEquals(buffPtr, recordSink.getAddress());
                recordSink.encodeUtf8(expected);
                recordSink.toSink(sink);
                Assert.assertEquals(expected, sink.toString());
                Assert.assertEquals(recordSink.length(), sink.length() * 2);
                recordSink.clear();
                Assert.assertEquals(0, recordSink.length());
                sink.clear();
                recordSink.toSink(sink);
                Assert.assertEquals(0, recordSink.length());
                Assert.assertEquals("", sink.toString());
            } finally {
                Unsafe.free(buffPtr, buffSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSimpleMessage1() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String expected = "我能吞下玻璃而不傷身體";
            final int len = expected.length();
            final int buffSize = len * 3;
            final long buffPtr = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);
            try {
                LogRecordSink recordSink = new LogRecordSink(buffPtr, buffSize);
                recordSink.encodeUtf8(expected.substring(1, len - 1));
                recordSink.toSink(sink);
                TestUtils.assertEquals(expected.substring(1, len - 1), sink);
            } finally {
                Unsafe.free(buffPtr, buffSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSimpleMessage2() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String expected = "Я можу їсти скло, і воно мені не зашкодить.";
            final int len = expected.length();
            final int buffSize = len * 3;
            final long buffPtr = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);
            try {
                LogRecordSink recordSink = new LogRecordSink(buffPtr, buffSize);
                recordSink.encodeUtf8(expected, 2, len - 1);
                recordSink.toSink(sink);
                Assert.assertEquals(expected.substring(2, len - 1), sink.toString());
            } finally {
                Unsafe.free(buffPtr, buffSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSimpleMessage3() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String expected = "This is a simple message";
            final int len = expected.length();
            final int buffSize = len * 3;
            final long buffPtr = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);
            try {
                LogRecordSink recordSink = new LogRecordSink(buffPtr, buffSize);
                recordSink.put(expected, 2, len - 1);
                recordSink.toSink(sink);
                Assert.assertEquals(expected.substring(2, len - 1), sink.toString());
            } finally {
                Unsafe.free(buffPtr, buffSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }
}
