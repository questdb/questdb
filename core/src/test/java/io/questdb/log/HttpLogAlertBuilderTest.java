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
import org.junit.*;

import java.io.UnsupportedEncodingException;

public class HttpLogAlertBuilderTest {

    private static final int bufferSize = 1024;
    private static long bufferPtr;
    private static long bufferLimit;
    private HttpLogAlertBuilder alertBuilder;

    @BeforeClass
    public static void classSetup() {
        bufferPtr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        bufferLimit = bufferPtr + bufferSize;
    }

    @AfterClass
    public static void classTeardown() {
        Unsafe.free(bufferPtr, bufferSize, MemoryTag.NATIVE_DEFAULT);
    }

    @Before
    public void setUp() {
        alertBuilder = new HttpLogAlertBuilder(bufferPtr, bufferLimit);
        alertBuilder.putHeader("localhost");
        alertBuilder.setFooter(s -> s.put("\nF.O.O.T.E.R"));
        alertBuilder.setMark();
        Assert.assertEquals(
                "POST /api/v1/alerts HTTP/1.1\r\n" +
                        "Host: localhost\r\n" +
                        "User-Agent: QuestDB/LogAlert\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Content-Length:#######\r\n" +
                        "\r\n",
                Chars.stringFromUtf8Bytes(bufferPtr, alertBuilder.getMark())
        );
        Assert.assertEquals(148, alertBuilder.length());
    }

    @Test
    public void testEmptyMessage() {
        alertBuilder.setFooter(null);
        alertBuilder.$(); // we are adding nothing, just finish the build
        Assert.assertEquals(
                "POST /api/v1/alerts HTTP/1.1\r\n" +
                        "Host: localhost\r\n" +
                        "User-Agent: QuestDB/LogAlert\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Content-Length:      0\r\n" +
                        "\r\n",
                Chars.stringFromUtf8Bytes(bufferPtr, alertBuilder.getMark())
        );
        Assert.assertEquals(148, alertBuilder.length());
        Assert.assertEquals("POST /api/v1/alerts HTTP/1.1\r\n" +
                "Host: localhost\r\n" +
                "User-Agent: QuestDB/LogAlert\r\n" +
                "Accept: */*\r\n" +
                "Content-Type: application/json\r\n" +
                "Content-Length:      0\r\n" +
                "\r\n", alertBuilder.toString());
    }

    @Test
    public void testSimpleMessage() throws UnsupportedEncodingException {
        final String msg = "Hello, my name is Íñigo Montoya, you killed my father, prepare to ∑π¬µ∫√ç©!!";
        final byte[] msgBytes = msg.getBytes(Files.UTF_8);
        final int len = msgBytes.length;
        final long msgPtr = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            LogRecordSink logRecord = new LogRecordSink(msgPtr, len);
            logRecord.put(msg);
            alertBuilder.setFooter(null);
            alertBuilder.put(logRecord).$();
            Assert.assertEquals(
                    "POST /api/v1/alerts HTTP/1.1\r\n" +
                            "Host: localhost\r\n" +
                            "User-Agent: QuestDB/LogAlert\r\n" +
                            "Accept: */*\r\n" +
                            "Content-Type: application/json\r\n" +
                            "Content-Length:     89\r\n" +
                            "\r\n" +
                            "Hello, my name is Íñigo Montoya, you killed my father, prepare to ∑π¬µ∫√ç©!!",
                    alertBuilder.toString()
            );
            Assert.assertEquals(237, alertBuilder.length());

            String randomMsg = "Yup, this is a random message.";
            alertBuilder.rewindToMark().put(randomMsg, 5, randomMsg.length()).$();
            Assert.assertEquals(173, alertBuilder.length());
            Assert.assertEquals("POST /api/v1/alerts HTTP/1.1\r\n" +
                    "Host: localhost\r\n" +
                    "User-Agent: QuestDB/LogAlert\r\n" +
                    "Accept: */*\r\n" +
                    "Content-Type: application/json\r\n" +
                    "Content-Length:     25\r\n" +
                    "\r\n" +
                    "this is a random message.", alertBuilder.toString());
        } finally {
            if (msgPtr != 0) {
                Unsafe.free(msgPtr, len, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testFilteringSimpleMessage() throws UnsupportedEncodingException {
        final String msg = "\b\f\t$\"\\\r\n";
        final byte[] msgBytes = msg.getBytes(Files.UTF_8);
        final int len = msgBytes.length;
        final long msgPtr = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            LogRecordSink logRecord = new LogRecordSink(msgPtr, len);
            logRecord.put(msg);
            alertBuilder.put(logRecord).$();
            Assert.assertEquals(
                    "POST /api/v1/alerts HTTP/1.1\r\n" +
                            "Host: localhost\r\n" +
                            "User-Agent: QuestDB/LogAlert\r\n" +
                            "Accept: */*\r\n" +
                            "Content-Type: application/json\r\n" +
                            "Content-Length:     18\r\n" +
                            "\r\n" +
                            " \\$\\\"\\" +
                            "\nF.O.O.T.E.R",
                    alertBuilder.toString()
            );
            Assert.assertEquals(166, alertBuilder.length());
        } finally {
            if (msgPtr != 0) {
                Unsafe.free(msgPtr, len, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testSinkable() {
        final String msg = "test: ";
        final byte[] msgBytes = msg.getBytes(Files.UTF_8);
        final int len = msgBytes.length;
        final long msgPtr = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            LogRecordSink logRecord = new LogRecordSink(msgPtr, len);
            logRecord.put(msg);
            alertBuilder.put(logRecord).put(s -> s.put("Tres, Dos, Uno, Zero!!")).$();
            Assert.assertEquals(
                    "POST /api/v1/alerts HTTP/1.1\r\n" +
                            "Host: localhost\r\n" +
                            "User-Agent: QuestDB/LogAlert\r\n" +
                            "Accept: */*\r\n" +
                            "Content-Type: application/json\r\n" +
                            "Content-Length:     40\r\n" +
                            "\r\n" +
                            "test: Tres, Dos, Uno, Zero!!" +
                            "\nF.O.O.T.E.R",
                    alertBuilder.toString()
            );
            Assert.assertEquals(188, alertBuilder.length());
        } finally {
            if (msgPtr != 0) {
                Unsafe.free(msgPtr, len, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testContentLengthNoMarker() {
        alertBuilder.clear();
        Assert.assertEquals(0, alertBuilder.$());
        Assert.assertEquals("", alertBuilder.toString());

        alertBuilder.clear();
        Assert.assertEquals(12, alertBuilder.put("clairvoyance").$());
        Assert.assertEquals("clairvoyance", alertBuilder.toString());
    }

    @Test
    public void testContentLengthMarker() {
        alertBuilder.clear();
        alertBuilder.putContentLengthMarker();
        Assert.assertEquals(24, alertBuilder.$());
        Assert.assertEquals("Content-Length:     24\r\n", alertBuilder.toString());

        alertBuilder.clear();
        alertBuilder.putContentLengthMarker();
        Assert.assertEquals(36, alertBuilder.put("clairvoyance").$());
        Assert.assertEquals("Content-Length:     36\r\nclairvoyance", alertBuilder.toString());

        alertBuilder.clear();
        alertBuilder.putContentLengthMarker();
        String message = "$Sîne klâwen durh die wolken sint geslagen,sîn vil manegiu tugent michz leisten hiez.$\r\n\";";
        Assert.assertEquals(117, alertBuilder.put(message).$());
        Assert.assertEquals("Content-Length:    117\r\n" + message, alertBuilder.toString());

        alertBuilder.clear();
        alertBuilder.putContentLengthMarker();
        message = "2021-11-26T19:22:47.8658077Z 2021-11-26T19:22:47.860908Z E i.q.c.BitmapIndexBwdReader cursor could not consistently read index header [corrupt?] [timeout=5000000μs]\n";
        Assert.assertEquals(190, alertBuilder.put(message).$());
        Assert.assertEquals("Content-Length:    190\r\n" + message, alertBuilder.toString());

        alertBuilder.clear();
        alertBuilder.putContentLengthMarker();
        Assert.assertEquals("Content-Length:#######\r\n", alertBuilder.toString());
        int limit = bufferSize - alertBuilder.length();
        for (int i = 0; i < limit; i++) {
            alertBuilder.put('Q');
        }
        alertBuilder.$();
        Assert.assertEquals(bufferSize, alertBuilder.length());
        Assert.assertTrue(alertBuilder
                .toString()
                .startsWith("Content-Length:   1024\r\nQQQQQQQQQQQQQQQQQQQQQQ"));
        Assert.assertTrue(alertBuilder
                .toString()
                .endsWith("QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ" +
                        "QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ" +
                        "QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ" +
                        "QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ" +
                        "QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ"));
    }
}
