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
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import org.junit.*;

import java.io.UnsupportedEncodingException;

public class HttpAlertBuilderTest {

    private static final long bufferSize = 1024;
    private static long bufferPtr;
    private static long bufferLimit;
    private HttpAlertBuilder alertBuilder;

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
        alertBuilder = new HttpAlertBuilder(bufferPtr, bufferLimit);
        alertBuilder.putHeader("localhost");
        alertBuilder.setMark();
        Assert.assertEquals(
                "POST /api/v1/alerts HTTP/1.1\r\n" +
                        "Host: localhost\r\n" +
                        "User-Agent: QuestDB/7.71.1\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Content-Length: ######\r\n" +
                        "\r\n",
                Chars.stringFromUtf8Bytes(bufferPtr, alertBuilder.getMark())
        );
        Assert.assertEquals(146, alertBuilder.length());
    }

    @Test
    public void testEmptyMessage() {
        alertBuilder.$(); // we are adding nothing, just finish the build
        Assert.assertEquals(
                "POST /api/v1/alerts HTTP/1.1\r\n" +
                        "Host: localhost\r\n" +
                        "User-Agent: QuestDB/7.71.1\r\n" +
                        "Accept: */*\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Content-Length:      0\r\n" +
                        "\r\n",
                Chars.stringFromUtf8Bytes(bufferPtr, alertBuilder.getMark())
        );
        Assert.assertEquals(146, alertBuilder.length());
        Assert.assertEquals("POST /api/v1/alerts HTTP/1.1\r\n" +
                "Host: localhost\r\n" +
                "User-Agent: QuestDB/7.71.1\r\n" +
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
            alertBuilder.put(logRecord).$();
            Assert.assertEquals(
                    "POST /api/v1/alerts HTTP/1.1\r\n" +
                            "Host: localhost\r\n" +
                            "User-Agent: QuestDB/7.71.1\r\n" +
                            "Accept: */*\r\n" +
                            "Content-Type: application/json\r\n" +
                            "Content-Length:     89\r\n" +
                            "\r\n" +
                            "Hello, my name is Íñigo Montoya, you killed my father, prepare to ∑π¬µ∫√ç©!!",
                    alertBuilder.toString()
            );
            Assert.assertEquals(235, alertBuilder.length());

            String randomMsg = "Yup, this is a random message.";
            alertBuilder.rewindToMark().put(randomMsg, 5, randomMsg.length()).$();
            Assert.assertEquals(171, alertBuilder.length());
            Assert.assertEquals("POST /api/v1/alerts HTTP/1.1\r\n" +
                    "Host: localhost\r\n" +
                    "User-Agent: QuestDB/7.71.1\r\n" +
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
                            "User-Agent: QuestDB/7.71.1\r\n" +
                            "Accept: */*\r\n" +
                            "Content-Type: application/json\r\n" +
                            "Content-Length:      7\r\n" +
                            "\r\n" +
                            " \\$\\\"\\\\",
                    alertBuilder.toString()
            );
            Assert.assertEquals(153, alertBuilder.length());
        } finally {
            if (msgPtr != 0) {
                Unsafe.free(msgPtr, len, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testSinkable() throws UnsupportedEncodingException {
        final String msg = "test: ";
        final byte[] msgBytes = msg.getBytes(Files.UTF_8);
        final int len = msgBytes.length;
        final long msgPtr = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            LogRecordSink logRecord = new LogRecordSink(msgPtr, len);
            logRecord.put(msg);

            Sinkable sinkable = sink1 -> sink1.put("Tres, Dos, Uno, Zero!!");
            alertBuilder.put(logRecord).put(sinkable).$();
            Assert.assertEquals(
                    "POST /api/v1/alerts HTTP/1.1\r\n" +
                            "Host: localhost\r\n" +
                            "User-Agent: QuestDB/7.71.1\r\n" +
                            "Accept: */*\r\n" +
                            "Content-Type: application/json\r\n" +
                            "Content-Length:     28\r\n" +
                            "\r\n" +
                            "test: Tres, Dos, Uno, Zero!!",
                    alertBuilder.toString()
            );
            Assert.assertEquals(174, alertBuilder.length());
        } finally {
            if (msgPtr != 0) {
                Unsafe.free(msgPtr, len, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }
}
