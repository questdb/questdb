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

import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

public class HttpAlertBuilderTest {

    @Test
    public void testEmptyMessage() {
        final long bufferSize = 1024;
        final long bufferPtr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        final long bufferLimit = bufferPtr + bufferSize;
        try {
            HttpAlertBuilder builder = new HttpAlertBuilder();
            builder.of(bufferPtr, bufferLimit, "localhost");
            builder.setMark();
            Assert.assertEquals(
                    "POST /api/v1/alerts HTTP/1.1\r\n" +
                            "Host: localhost\r\n" +
                            "User-Agent: QuestDB/7.71.1\r\n" +
                            "Accept: */*\r\n" +
                            "Content-Type: application/json\r\n" +
                            "Content-Length: ######\r\n" +
                            "\r\n",
                    Chars.stringFromUtf8Bytes(bufferPtr, builder.getMark())
            );
            Assert.assertEquals(146, builder.length());
            builder.$();
            Assert.assertEquals(
                    "POST /api/v1/alerts HTTP/1.1\r\n" +
                            "Host: localhost\r\n" +
                            "User-Agent: QuestDB/7.71.1\r\n" +
                            "Accept: */*\r\n" +
                            "Content-Type: application/json\r\n" +
                            "Content-Length:      0\r\n" +
                            "\r\n",
                    Chars.stringFromUtf8Bytes(bufferPtr, builder.getMark())
            );
            Assert.assertEquals(146, builder.length());
        } finally {
            if (bufferPtr != 0) {
                Unsafe.free(bufferPtr, bufferSize, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testSimpleMessage() throws UnsupportedEncodingException {
        final long bufferSize = 1024;
        final long bufferPtr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        final long bufferLimit = bufferPtr + bufferSize;
        final String msg = "My name is Inigo Montoya";
        final int len = msg.getBytes(StandardCharsets.UTF_8).length;
        final long msgPtr = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            LogRecordSink logRecord = new LogRecordSink(msgPtr, len);
            logRecord.put(msg);

            HttpAlertBuilder builder = new HttpAlertBuilder();
            builder.of(bufferPtr, bufferLimit, "localhost");
            builder.setMark();
            builder.put(logRecord);
            builder.$();
            Assert.assertEquals(
                    "POST /api/v1/alerts HTTP/1.1\r\n" +
                            "Host: localhost\r\n" +
                            "User-Agent: QuestDB/7.71.1\r\n" +
                            "Accept: */*\r\n" +
                            "Content-Type: application/json\r\n" +
                            "Content-Length:     24\r\n" +
                            "\r\n" +
                            "My name is Inigo Montoya",
                    builder.toString()
            );
            Assert.assertEquals(170, builder.length());
        } finally {
            if (msgPtr != 0) {
                Unsafe.free(msgPtr, len, MemoryTag.NATIVE_DEFAULT);
            }
            if (bufferPtr != 0) {
                Unsafe.free(bufferPtr, bufferSize, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }
}
