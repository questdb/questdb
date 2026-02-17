/*******************************************************************************
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

import io.questdb.log.HttpLogRecordUtf8Sink;
import io.questdb.log.LogRecordUtf8Sink;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8s;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Consumer;

public class HttpLogRecordUtf8SinkTest {

    private static final int bufferSize = 1024;

    @Test
    public void testContentLengthMarker() throws Exception {
        withHttpLogRecordSink(alertBuilder -> {
            alertBuilder.clear();
            alertBuilder.putContentLengthMarker();
            Assert.assertEquals(26, alertBuilder.$());
            Assert.assertEquals("Content-Length:       26\r\n", alertBuilder.toString());

            alertBuilder.clear();
            alertBuilder.putContentLengthMarker();
            Assert.assertEquals(38, alertBuilder.putAscii("clairvoyance").$());
            Assert.assertEquals("Content-Length:       38\r\nclairvoyance", alertBuilder.toString());

            alertBuilder.clear();
            alertBuilder.putContentLengthMarker();
            String message = "$Sîne klâwen durh die wolken sint geslagen,sîn vil manegiu tugent michz leisten hiez.$\r\n\";";
            Assert.assertEquals(119, alertBuilder.put(message).$());
            Assert.assertEquals("Content-Length:      119\r\n" + message, alertBuilder.toString());

            alertBuilder.clear();
            alertBuilder.putContentLengthMarker();
            message = "2021-11-26T19:22:47.8658077Z 2021-11-26T19:22:47.860908Z E i.q.c.BitmapIndexBwdReader cursor could not consistently read index header [corrupt?] [timeout=5000000ms]\n";
            Assert.assertEquals(191, alertBuilder.put(message).$());
            Assert.assertEquals("Content-Length:      191\r\n" + message, alertBuilder.toString());

            alertBuilder.clear();
            alertBuilder.putContentLengthMarker();
            Assert.assertEquals("Content-Length:#########\r\n", alertBuilder.toString());
            int limit = bufferSize - alertBuilder.size();
            for (int i = 0; i < limit; i++) {
                alertBuilder.put('Q');
            }
            alertBuilder.putEOL();
            alertBuilder.$();

            Assert.assertEquals(bufferSize, alertBuilder.size());
            String alertBuilderStr = alertBuilder.toString();
            Assert.assertTrue(alertBuilderStr, alertBuilderStr.startsWith("Content-Length:     1024\r\nQQQQQQQQQQQQQQQQQQQQQQ"));
            Assert.assertTrue(alertBuilderStr, alertBuilderStr
                    .endsWith("QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ" +
                            "QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ" +
                            "QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ" +
                            "QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ" +
                            "QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ\r\n"));
        });
    }

    @Test
    public void testContentLengthNoMarker() throws Exception {
        withHttpLogRecordSink(alertBuilder -> {
            alertBuilder.clear();
            Assert.assertEquals(0, alertBuilder.$());
            Assert.assertEquals("", alertBuilder.toString());

            alertBuilder.clear();
            Assert.assertEquals(12, alertBuilder.put("clairvoyance").$());
            Assert.assertEquals("clairvoyance", alertBuilder.toString());
        });
    }

    @Test
    public void testEmptyMessage() throws Exception {
        withHttpLogRecordSink(alertBuilder -> {
            alertBuilder.$(); // we are adding nothing, just finish the build
            Assert.assertEquals(
                    "POST /api/v1/alerts HTTP/1.1\r\n" +
                            "Host: localhost\r\n" +
                            "User-Agent: QuestDB/LogAlert\r\n" +
                            "Accept: */*\r\n" +
                            "Content-Type: application/json\r\n" +
                            "Content-Length:        0\r\n" +
                            "\r\n",
                    Utf8s.stringFromUtf8Bytes(alertBuilder.ptr(), alertBuilder.getMark())
            );
            Assert.assertEquals(150, alertBuilder.size());
            Assert.assertEquals("POST /api/v1/alerts HTTP/1.1\r\n" +
                    "Host: localhost\r\n" +
                    "User-Agent: QuestDB/LogAlert\r\n" +
                    "Accept: */*\r\n" +
                    "Content-Type: application/json\r\n" +
                    "Content-Length:        0\r\n" +
                    "\r\n", alertBuilder.toString());
        });
    }

    @Test
    public void testFilteringSimpleMessage() throws Exception {
        withHttpLogRecordSink(alertBuilder -> {
            final String msg = "\b\f\t$\"\\\r\n";
            final byte[] msgBytes = msg.getBytes(Files.UTF_8);
            final int len = msgBytes.length;
            final long msgPtr = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                LogRecordUtf8Sink logRecord = new LogRecordUtf8Sink(msgPtr, len);
                logRecord.put(msg);
                alertBuilder.put(logRecord).$();
                Assert.assertEquals(
                        "POST /api/v1/alerts HTTP/1.1\r\n" +
                                "Host: localhost\r\n" +
                                "User-Agent: QuestDB/LogAlert\r\n" +
                                "Accept: */*\r\n" +
                                "Content-Type: application/json\r\n" +
                                "Content-Length:        6\r\n" +
                                "\r\n" +
                                " \\$\\\"\\",
                        alertBuilder.toString()
                );
                Assert.assertEquals(156, alertBuilder.size());
            } finally {
                if (msgPtr != 0) {
                    Unsafe.free(msgPtr, len, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testSimpleMessage() throws Exception {
        withHttpLogRecordSink(alertBuilder -> {
            final String msg = "Hello, my name is Íñigo Montoya, you killed my father, prepare to ∑π¬µ∫√ç©!!";
            final byte[] msgBytes = msg.getBytes(Files.UTF_8);
            final int len = msgBytes.length + Misc.EOL.length();
            final long msgPtr = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                LogRecordUtf8Sink logRecord = new LogRecordUtf8Sink(msgPtr, len);
                logRecord.put(msg);
                alertBuilder.put(logRecord).$();
                Assert.assertEquals(
                        "POST /api/v1/alerts HTTP/1.1\r\n" +
                                "Host: localhost\r\n" +
                                "User-Agent: QuestDB/LogAlert\r\n" +
                                "Accept: */*\r\n" +
                                "Content-Type: application/json\r\n" +
                                "Content-Length:       89\r\n" +
                                "\r\n" +
                                "Hello, my name is Íñigo Montoya, you killed my father, prepare to ∑π¬µ∫√ç©!!",
                        alertBuilder.toString()
                );
                Assert.assertEquals(239, alertBuilder.size());

                String randomMsg = "Yup, this is a random message.";
                alertBuilder.rewindToMark().put(randomMsg, 5, randomMsg.length());
                alertBuilder.$();
                Assert.assertEquals(175, alertBuilder.size());
                Assert.assertEquals("POST /api/v1/alerts HTTP/1.1\r\n" +
                        "Host: localhost\r\n" +
                        "User-Agent: QuestDB/LogAlert\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Content-Length:       25\r\n" +
                        "\r\n" +
                        "this is a random message.", alertBuilder.toString());
            } finally {
                if (msgPtr != 0) {
                    Unsafe.free(msgPtr, len, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testSinkable() throws Exception {
        withHttpLogRecordSink(alertBuilder -> {
            final String msg = "test: ";
            final byte[] msgBytes = msg.getBytes(Files.UTF_8);
            final int len = msgBytes.length + Misc.EOL.length();
            final long msgPtr = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                LogRecordUtf8Sink logRecord = new LogRecordUtf8Sink(msgPtr, len);
                logRecord.put(msg);
                alertBuilder.put(logRecord).put(s -> s.put("Tres, Dos, Uno, Zero!!")).$();
                Assert.assertEquals(
                        "POST /api/v1/alerts HTTP/1.1\r\n" +
                                "Host: localhost\r\n" +
                                "User-Agent: QuestDB/LogAlert\r\n" +
                                "Accept: */*\r\n" +
                                "Content-Type: application/json\r\n" +
                                "Content-Length:       28\r\n" +
                                "\r\n" +
                                "test: Tres, Dos, Uno, Zero!!",
                        alertBuilder.toString()
                );
                Assert.assertEquals(178, alertBuilder.size());
            } finally {
                if (msgPtr != 0) {
                    Unsafe.free(msgPtr, len, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    private static void withHttpLogRecordSink(Consumer<HttpLogRecordUtf8Sink> consumer) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long bufferPtr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
            final HttpLogRecordUtf8Sink alertBuilder = new HttpLogRecordUtf8Sink(bufferPtr, bufferSize);
            alertBuilder.putHeader("localhost");
            alertBuilder.setMark();
            Assert.assertEquals(
                    "POST /api/v1/alerts HTTP/1.1\r\n" +
                            "Host: localhost\r\n" +
                            "User-Agent: QuestDB/LogAlert\r\n" +
                            "Accept: */*\r\n" +
                            "Content-Type: application/json\r\n" +
                            "Content-Length:#########\r\n" +
                            "\r\n",
                    Utf8s.stringFromUtf8Bytes(bufferPtr, alertBuilder.getMark())
            );
            Assert.assertEquals(150, alertBuilder.size());
            try {
                consumer.accept(alertBuilder);
            } finally {
                Unsafe.free(bufferPtr, bufferSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }
}
