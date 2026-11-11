/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.log.HeapLogRecordUtf8Sink;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecordUtf8Sink;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class HeapLogRecordUtf8SinkTest {
    private static final Log LOG = LogFactory.getLog(HeapLogRecordUtf8SinkTest.class);
    // 1, 2, 3 and 4-byte UTF-8 characters
    private static final String[] CHARS = {"a", "Z", "7", " ", "\u03c0", "\u00e9", "\u20ac", "\u4e2d", "\ud83d\ude00"};

    @Test
    public void testCopyFromReplacesSlotContents() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int capacity = 64;
            final long slotPtr = Unsafe.malloc(capacity, MemoryTag.NATIVE_DEFAULT);
            try {
                final LogRecordUtf8Sink slot = new LogRecordUtf8Sink(slotPtr, capacity);
                slot.put("previous record, longer than the next one");

                final HeapLogRecordUtf8Sink staging = new HeapLogRecordUtf8Sink(capacity);
                staging.of(capacity);
                staging.put("next \u03c0 record").putEOL();

                slot.copyFrom(staging);
                Assert.assertEquals(staging.size(), slot.size());
                Assert.assertEquals(staging.toString(), slot.toString());
                Assert.assertEquals(capacity, slot.capacity());
            } finally {
                Unsafe.free(slotPtr, capacity, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testOfLimitsRecordWithoutGrowing() {
        final HeapLogRecordUtf8Sink staging = new HeapLogRecordUtf8Sink(16);
        Assert.assertEquals(16, staging.capacity());

        // a smaller slot limits the record to the slot size
        staging.of(8);
        Assert.assertEquals(8, staging.capacity());
        staging.put("0123456789");
        Assert.assertEquals(8 - LogRecordUtf8Sink.EOL_LENGTH, staging.size());

        // a larger slot caps the record at the buffer size
        staging.of(32);
        Assert.assertEquals(16, staging.capacity());
        Assert.assertEquals(0, staging.size());
        staging.put("0123456789abcdefghij");
        Assert.assertEquals(16 - LogRecordUtf8Sink.EOL_LENGTH, staging.size());
        staging.putEOL();
        Assert.assertEquals(16, staging.size());
    }

    @Test
    public void testTruncationMatchesNativeRecord() throws Exception {
        // HeapLogRecordUtf8Sink stages records for the log ring, so its truncation
        // must produce the exact bytes that LogRecordUtf8Sink produces.
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final int maxCapacity = 96;
            final long nativePtr = Unsafe.malloc(maxCapacity, MemoryTag.NATIVE_DEFAULT);
            final long srcPtr = Unsafe.malloc(maxCapacity * 4L, MemoryTag.NATIVE_DEFAULT);
            try {
                final HeapLogRecordUtf8Sink heap = new HeapLogRecordUtf8Sink(maxCapacity);
                final StringSink text = new StringSink();
                for (int iteration = 0; iteration < 10_000; iteration++) {
                    final int capacity = 4 + rnd.nextInt(maxCapacity - 4);
                    final LogRecordUtf8Sink expected = new LogRecordUtf8Sink(nativePtr, capacity);
                    heap.of(capacity);

                    final int opCount = 1 + rnd.nextInt(6);
                    for (int op = 0; op < opCount; op++) {
                        text.clear();
                        final int charCount = rnd.nextInt(24);
                        for (int i = 0; i < charCount; i++) {
                            text.put(CHARS[rnd.nextInt(CHARS.length)]);
                        }
                        final String str = text.toString();
                        switch (rnd.nextInt(6)) {
                            case 0 -> {
                                expected.put(str);
                                heap.put(str);
                            }
                            case 4 -> {
                                // ASCII only
                                final String ascii = str.replaceAll("[^\\x00-\\x7f]", "x");
                                expected.putAscii(ascii);
                                heap.putAscii(ascii);
                            }
                            case 5 -> {
                                final int lo = str.isEmpty() ? 0 : rnd.nextInt(str.length());
                                final int hi = lo + rnd.nextInt(str.length() - lo + 1);
                                expected.put(str, lo, hi);
                                heap.put(str, lo, hi);
                            }
                            case 1 -> {
                                final Utf8String utf8 = new Utf8String(str);
                                expected.put(utf8);
                                heap.put(utf8);
                            }
                            case 2 -> {
                                final byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
                                for (int i = 0; i < bytes.length; i++) {
                                    Unsafe.putByte(srcPtr + i, bytes[i]);
                                }
                                expected.putNonAscii(srcPtr, srcPtr + bytes.length);
                                heap.putNonAscii(srcPtr, srcPtr + bytes.length);
                            }
                            default -> {
                                final long value = rnd.nextLong();
                                expected.put(value);
                                heap.put(value);
                            }
                        }
                        assertSameBytes(expected, heap, iteration);
                    }
                    expected.putEOL();
                    heap.putEOL();
                    assertSameBytes(expected, heap, iteration);
                    Assert.assertTrue(Utf8s.validateUtf8(heap) > -1);
                }
            } finally {
                Unsafe.free(nativePtr, maxCapacity, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(srcPtr, maxCapacity * 4L, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void assertSameBytes(LogRecordUtf8Sink expected, HeapLogRecordUtf8Sink actual, int iteration) {
        Assert.assertEquals("iteration " + iteration, expected.toString(), actual.toString());
        Assert.assertEquals("iteration " + iteration, expected.size(), actual.size());
        for (int i = 0, n = expected.size(); i < n; i++) {
            Assert.assertEquals("iteration " + iteration + ", byte " + i, expected.byteAt(i), actual.byteAt(i));
        }
    }
}
