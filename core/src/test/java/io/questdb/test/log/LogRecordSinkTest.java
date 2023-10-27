/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.log;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogLevel;
import io.questdb.log.LogRecordSink;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;

import static io.questdb.log.LogRecordSink.EOL_LENGTH;

public class LogRecordSinkTest {

    private static final Log LOG = LogFactory.getLog(LogRecordSinkTest.class);

    private StringSink sink;

    /**
     * Test that we don't trim the log on an invalid UTF-8 sequence.
     */
    public void runTestUtf8LineTrimmingImpl(ScenarioConsumer scenarioConsumer) throws Exception {
        final String[] msgs = {
                "abcd",  // ASCII last char, 4 bytes total.
                "abcð",  // 2-byte UTF-8 last char, 5 bytes total.
                "abc嚜", // 3-byte UTF-8 last char, 6 bytes total.
                "abc\uD83D\uDCA9", // 4-byte UTF-8 last char, 7 bytes total.
        };

        // Expected byte length of each message when encoded as UTF-8.
        final int[] expEncLens = {4, 5, 6, 7};

        final int[][] scenarios = {
                // { msg string index, sink max len, expected written len }

                // ASCII
                {0, 4, 4},
                {0, 5, 4},
                {0, 3, 3},
                {0, 2, 2},
                {0, 1, 1},
                {0, 0, 0},

                // 2-byte UTF-8
                {1, 5, 5},
                {1, 6, 5},
                {1, 4, 3},
                {1, 3, 3},

                // 3-byte UTF-8
                {2, 6, 6},
                {2, 7, 6},
                {2, 5, 3},
                {2, 4, 3},
                {2, 3, 3},

                // 4-byte UTF-8
                {3, 7, 7},
                {3, 8, 7},
                {3, 6, 3},
                {3, 5, 3},
                {3, 4, 3},
                {3, 3, 3},
        };

        for (int[] scenario : scenarios) {
            final int msgIdx = scenario[0];
            final String msg = msgs[msgIdx];
            final long sinkMaxLen = scenario[1];
            final long expectedLen = scenario[2];
            // System.err.printf(
            //         "scenario: msgIdx: %d, msg: %s, sinkMaxLen: %d, expectedLen: %d\n",
            //         msgIdx, msg, sinkMaxLen, expectedLen);

            // Sanity-checking our test data.
            final byte[] msgBytes = msg.getBytes(Files.UTF_8);
            final int utf8ByteLen = msgBytes.length;
            Assert.assertEquals(utf8ByteLen, expEncLens[msgIdx]);

            TestUtils.assertMemoryLeak(() -> scenarioConsumer.accept(msg, utf8ByteLen, sinkMaxLen, expectedLen));
        }
    }

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
                Assert.assertEquals(buffPtr, recordSink.ptr());
                recordSink.put(expected);
                recordSink.toSink(sink);
                Assert.assertEquals(expected, sink.toString());
                Assert.assertEquals(recordSink.size(), sink.length() * 2);
                recordSink.clear();
                Assert.assertEquals(0, recordSink.size());
                sink.clear();
                recordSink.toSink(sink);
                Assert.assertEquals(0, recordSink.size());
                Assert.assertEquals("", sink.toString());
            } finally {
                Unsafe.free(buffPtr, buffSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testLoggerApi() {
        final String str = "abcde ðãµ¶ Āڜ 嚜꓂ \uD83D\uDCA9 \uD83E\uDD9E!";
        try (Utf8 msg = new Utf8(str)) {
            LOG.info().$utf8(msg.lo(), msg.hi()).$();
            Assert.assertEquals(str, msg.toString());
        }
    }

    /**
     * Test malformed UTF-8 sequences are handled correctly.
     */
    @Test
    public void testMalformedUtf8Seq() {
        // UTF-8 encoding for an illegal 5-byte sequence.
        final byte lead5 = (byte) 0xF8; // 1111 1000
        final byte inter = (byte) 0xBF; // 1011 1111
        final byte ascii = (byte) 'a';

        final byte[][] buffers = {
                {lead5, inter, inter},
                {inter, inter},
                {ascii, inter, inter},
                {lead5, inter, inter, inter, inter}
        };

        final String[] expectedMsgs = {
                "???",
                "??",
                "a??",
                ""
        };

        for (int bufIndex = 0; bufIndex < buffers.length; bufIndex++) {
            sink.clear();
            final byte[] msgBytes = buffers[bufIndex];
            final String expectedMsg = expectedMsgs[bufIndex];
            final int len = msgBytes.length;
            final long msgPtr = Unsafe.malloc(len + EOL_LENGTH, MemoryTag.NATIVE_DEFAULT);
            try {
                LogRecordSink logRecord = new LogRecordSink(msgPtr, len + EOL_LENGTH);
                for (int i = 0; i < len; i++) {
                    logRecord.put(msgBytes[i]);
                }
                logRecord.toSink(sink);
                Assert.assertEquals(expectedMsg, sink.toString());
            } finally {
                Unsafe.free(msgPtr, len + EOL_LENGTH, MemoryTag.NATIVE_DEFAULT);
            }
        }
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
                recordSink.put(expected.substring(1, len - 1));
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
                recordSink.put(expected, 2, len - 1);
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

    @Test
    public void testUtf8LineTrimming() throws Exception {
        runTestUtf8LineTrimmingImpl((msg, utf8ByteLen, sinkMaxLen, expectedLen) -> {
            final long msgPtr = Unsafe.malloc(utf8ByteLen + EOL_LENGTH, MemoryTag.NATIVE_DEFAULT);
            try {
                LogRecordSink logRecord = new LogRecordSink(msgPtr, sinkMaxLen + EOL_LENGTH);
                logRecord.put(msg);
                Assert.assertEquals(expectedLen, logRecord.size());
                if (sinkMaxLen > 0) {
                    // Now test that the log record can be cleared and reused.
                    logRecord.clear();
                    sink.clear();
                    logRecord.put("x");
                    logRecord.toSink(sink);
                    Assert.assertEquals("x", sink.toString());
                }
            } finally {
                Unsafe.free(msgPtr, utf8ByteLen + EOL_LENGTH, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testUtf8LineTrimmingFromString() throws Exception {
        runTestUtf8LineTrimmingImpl((msg, utf8ByteLen, sinkMaxLen, expectedLen) -> {
            final long msgPtr = Unsafe.malloc(utf8ByteLen + EOL_LENGTH, MemoryTag.NATIVE_DEFAULT);
            try {
                LogRecordSink logRecord = new LogRecordSink(msgPtr, sinkMaxLen + EOL_LENGTH);
                logRecord.put(msg);
                Assert.assertEquals(expectedLen, logRecord.size());
            } finally {
                Unsafe.free(msgPtr, utf8ByteLen + EOL_LENGTH, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testUtf8LineTrimmingFromUtf8Buf() throws Exception {
        runTestUtf8LineTrimmingImpl((msg, utf8ByteLen, sinkMaxLen, expectedLen) -> {
            final Utf8 utf8 = new Utf8(msg);
            final long msgPtr = Unsafe.malloc(utf8ByteLen + EOL_LENGTH, MemoryTag.NATIVE_DEFAULT);
            try {
                LogRecordSink logRecord = new LogRecordSink(msgPtr, sinkMaxLen + EOL_LENGTH);
                logRecord.put(utf8.lo(), utf8.hi());
                Assert.assertEquals(expectedLen, logRecord.size());
            } finally {
                Unsafe.free(msgPtr, utf8ByteLen + EOL_LENGTH, MemoryTag.NATIVE_DEFAULT);
                Misc.free(utf8);
            }
        });
    }

    @FunctionalInterface
    interface ScenarioConsumer {
        void accept(String msg, int utf8ByteLen, long sinkMaxLen, long expectedLen);
    }

    /**
     * A class that holds a UTF-8 string in native memory.
     */
    static final class Utf8 implements Closeable {
        private long hi;
        private long lo;

        public Utf8(String msg) {
            final byte[] buf = msg.getBytes(Files.UTF_8);
            lo = Unsafe.malloc(buf.length, MemoryTag.NATIVE_DEFAULT);
            hi = lo + buf.length;
            Unsafe.getUnsafe().copyMemory(buf, Unsafe.BYTE_OFFSET, null, lo, buf.length);
        }

        @Override
        public void close() {
            if (lo != 0) {
                Unsafe.free(lo, hi - lo, MemoryTag.NATIVE_DEFAULT);
                lo = hi = 0;
            }
        }

        public long hi() {
            return hi;
        }

        public long lo() {
            return lo;
        }

        @Override
        public String toString() {
            final StringSink sink = new StringSink();
            sink.putUtf8(lo, hi);
            return sink.toString();
        }
    }
}
