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

package io.questdb.cutlass.line.tcp;

import java.nio.charset.StandardCharsets;

import io.questdb.std.MemoryTag;
import org.junit.Assert;

import io.questdb.cutlass.line.LineProtoException;
import io.questdb.cutlass.line.LineProtoLexerTest;
import io.questdb.cutlass.line.tcp.NewLineProtoParser.ErrorCode;
import io.questdb.cutlass.line.tcp.NewLineProtoParser.ParseResult;
import io.questdb.cutlass.line.tcp.NewLineProtoParser.ProtoEntity;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;

public class NewLineProtocolParserLexerTest extends LineProtoLexerTest {
    private final NewLineProtoParser protoParser = new NewLineProtoParser();
    private ErrorCode lastErrorCode;
    private boolean onErrorLine;
    private long startOfLineAddr;

    @Override
    protected void assertThat(CharSequence expected, byte[] line) throws LineProtoException {
        final int len = line.length;
        final boolean endWithEOL = line[len - 1] == '\n' || line[len - 1] == '\r';
        long mem = Unsafe.malloc(line.length + 1, MemoryTag.NATIVE_DEFAULT);
        try {
            if (len < 10) {
                for (int i = 1; i < len; i++) {
                    for (int j = 0; j < len; j++) {
                        Unsafe.getUnsafe().putByte(mem + j, line[j]);
                    }
                    Unsafe.getUnsafe().putByte(mem + len, (byte) '\n');
                    sink.clear();
                    resetParser(mem);
                    boolean complete = parseMeasurement(mem + i);
                    Assert.assertFalse(complete);
                    complete = parseMeasurement(mem + len);
                    Assert.assertTrue(complete);
                    TestUtils.assertEquals(expected, sink);
                }
            } else {
                for (int i = 1; i < len - 10; i++) {
                    for (int j = 0; j < len; j++) {
                        Unsafe.getUnsafe().putByte(mem + j, line[j]);
                    }
                    Unsafe.getUnsafe().putByte(mem + len, (byte) '\n');
                    sink.clear();
                    resetParser(mem);
                    parseMeasurement(mem + i);
                    parseMeasurement(mem + i + 10);
                    boolean complete;
                    if (!endWithEOL) {
                        complete = parseMeasurement(mem + len + 1);
                    } else {
                        complete = parseMeasurement(mem + len);
                    }
                    Assert.assertTrue(complete);
                    TestUtils.assertEquals(expected, sink);
                }
            }

            // assert small buffer
            for (int j = 0; j < len; j++) {
                Unsafe.getUnsafe().putByte(mem + j, line[j]);
            }
            Unsafe.getUnsafe().putByte(mem + len, (byte) '\n');
            sink.clear();
            resetParser(mem);
            Assert.assertEquals(endWithEOL, parseMeasurement(mem + len));
            if (!endWithEOL) {
                Assert.assertTrue(parseMeasurement(mem + len + 1));
            }
            TestUtils.assertEquals(expected, sink);
        } finally {
            Unsafe.free(mem, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Override
    protected void assertError(CharSequence line, int state, int code, int position) throws LineProtoException {
        byte[] bytes = line.toString().getBytes(StandardCharsets.UTF_8);
        int len = bytes.length;
        final boolean endWithEOL = bytes[len - 1] == '\n' || bytes[len - 1] == '\r';
        if (!endWithEOL) {
            len++;
        }
        long mem = Unsafe.malloc(len + 1, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 1; i < len; i++) {
                for (int j = 0; j < bytes.length; j++) {
                    Unsafe.getUnsafe().putByte(mem + j, bytes[j]);
                }
                if (!endWithEOL) {
                    Unsafe.getUnsafe().putByte(mem + bytes.length, (byte) '\n');
                }
                sink.clear();
                resetParser(mem);
                parseMeasurement(mem + i);
                Assert.assertTrue(parseMeasurement(mem + len));
                Assert.assertNotNull(lastErrorCode);
            }
        } finally {
            Unsafe.free(mem, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void resetParser(long mem) {
        lastErrorCode = null;
        onErrorLine = false;
        startOfLineAddr = mem;
        protoParser.of(mem);
    }

    private boolean parseMeasurement(long bufHi) {
        while (protoParser.getBufferAddress() < bufHi) {
            ParseResult rc;
            if (!onErrorLine) {
                rc = protoParser.parseMeasurement(bufHi);
            } else {
                rc = protoParser.skipMeasurement(bufHi);
            }
            switch (rc) {
                case MEASUREMENT_COMPLETE:
                    startOfLineAddr = protoParser.getBufferAddress() + 1;
                    if (!onErrorLine) {
                        assembleLine();
                    } else {
                        onErrorLine = false;
                    }
                    protoParser.startNextMeasurement();
                    break;
                case BUFFER_UNDERFLOW:
                    return false;
                case ERROR:
                    Assert.assertFalse(onErrorLine);
                    onErrorLine = true;
                    lastErrorCode = protoParser.getErrorCode();
                    StringSink tmpSink = new StringSink();
                    if (Chars.utf8Decode(startOfLineAddr, protoParser.getBufferAddress(), tmpSink)) {
                        sink.put(tmpSink.toString());
                    }
                    sink.put("-- error --\n");
                    break;
            }
        }
        return true;
    }

    private void assembleLine() {
        int nEntities = protoParser.getnEntities();
        Chars.utf8Decode(protoParser.getMeasurementName().getLo(), protoParser.getMeasurementName().getHi(), sink);
        int n = 0;
        boolean tagsComplete = false;
        while (n < nEntities) {
            ProtoEntity entity = protoParser.getEntity(n++);
            if (!tagsComplete && entity.getType() != NewLineProtoParser.ENTITY_TYPE_TAG) {
                tagsComplete = true;
                sink.put(' ');
            } else {
                sink.put(',');
            }
            Chars.utf8Decode(entity.getName().getLo(), entity.getName().getHi(), sink);
            sink.put('=');
            switch (entity.getType()) {
                case NewLineProtoParser.ENTITY_TYPE_TAG:
                    Chars.utf8Decode(entity.getValue().getLo(), entity.getValue().getHi(), sink);
                    break;
                case NewLineProtoParser.ENTITY_TYPE_STRING:
                    sink.put('"');
                    Chars.utf8Decode(entity.getValue().getLo(), entity.getValue().getHi(), sink);
                    sink.put('"');
                    break;

                default:
                    sink.put(entity.getValue());
            }
        }

        if (protoParser.hasTimestamp()) {
            sink.put(' ');
            Numbers.append(sink, protoParser.getTimestamp());
        }
        sink.put('\n');
    }
}
