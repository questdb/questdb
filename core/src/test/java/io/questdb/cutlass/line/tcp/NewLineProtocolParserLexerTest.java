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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.cutlass.line.LineProtoLexerTest;
import org.junit.Assert;

import io.questdb.cutlass.line.LineProtoException;
import io.questdb.cutlass.line.tcp.NewLineProtoParser.ErrorCode;
import io.questdb.cutlass.line.tcp.NewLineProtoParser.ParseResult;
import io.questdb.cutlass.line.tcp.NewLineProtoParser.ProtoEntity;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class NewLineProtocolParserLexerTest extends LineProtoLexerTest {
    private final static Log LOG = LogFactory.getLog(NewLineProtocolParserLexerTest.class);
    private final NewLineProtoParser protoParser = new NewLineProtoParser();
    private ErrorCode lastErrorCode;
    private boolean onErrorLine;
    private long startOfLineAddr;

    @Override
    public void testNoTagValue1() {
        assertThat(
                "measurement,tag= field=x 10000\n",
                "measurement,tag= field=x 10000\n"
        );
    }

    @Override
    public void testNoTagValue2() {
        assertThat(
                "measurement,tag= field=x 10000\n",
                "measurement,tag=, field=x 10000\n"
        );
    }

    @Override
    public void testNoTagValue3() {
        assertThat(
                "measurement,tag=\n",
                "measurement,tag="
        );
    }

    @Override
    public void testNoTagValue4() {
        assertThat(
                "measurement,tag=\n",
                "measurement,tag=\n"
        );
    }

    @Override
    public void testSkipLine() {
        assertThat(
                "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "measurement,tag=value3,tag2=value2 field=,field2=\"ok\"\n" +
                        "measurement,tag=value4,tag2=value4 field=200i,field2=\"super\"\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "measurement,tag=value3,tag2=value2 field=,field2=\"ok\"\n" +
                        "measurement,tag=value4,tag2=value4 field=200i,field2=\"super\"\n"
        );
    }

    @Test
    public void testWithQuotedStringsWithSpaces() {
        assertThat(
                "measurement,tag=value,tag2=value field=10000i,field2=\"longstring\",fld3=\"short string\" 100000\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"longstring\",fld3=\"short string\" 100000\n"
        );
    }

    @Test
    public void testWithQuotedStringsWithEscapedQuotes() {
        assertThat(
                "measurement,tag=value,tag2=value field=10000i,field2=\"str\" escaped\\ end\" 100000\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str\\\" escaped\\\\ end\" 100000\n"
        );

        assertThat(
                "measurement field2=\"double escaped \\ \" and quoted\" 100000\n",
                "measurement field2=\"double escaped \\\\ \\\" and quoted\" 100000\n"
        );

        assertThat(
                "measurement field2=\"double escaped \\\" and quoted2\" 100000\n",
                "measurement field2=\"double escaped \\\\\\\" and quoted2\" 100000\n"
        );

        assertThat(
                "measurement,tag=value,tag2=value field=10000i,field2=\"str=special,end\" 100000\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str=special,end\" 100000\n"
        );

        assertThat(
                "measurement,tag=value,tag2=value field=10000i,field2=\"str=special,end\",field3=34 100000\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str=special,end\",field3=34 100000\n"
        );
    }

    @Test
    public void testWithQuotedStringsWithEscapedQuotesUnsuccessful() {
        assertThat(
                "-- error --\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str=special,lineend\n"
        );
    }

    @Test
    public void testWithEscapedTagValues() {
        assertThat(
                "measurement,tag=value with space,tag2=value field=10000i,field2=\"str=special,end\" 100000\n",
                "measurement,tag=value\\ with\\ space,tag2=value field=10000i,field2=\"str=special,end\" 100000\n"
        );

        assertThat(
                "measurement,tag=value\\with\\slash,tag2=value field=10000i,field2=\"str=special,end\\ \" 100000\n",
                "measurement,tag=value\\\\with\\\\slash,tag2=value field=10000i,field2=\"str=special,end\\\\ \" 100000\n"
        );
    }

    @Test
    public void testWithEscapedKeys() {
        assertThat(
                "measurement,t ag=value with space,tag2=value field=10000i,field 2=\"str=special,end\" 100000\n",
                "measurement,t\\ ag=value\\ with\\ space,tag2=value field=10000i,field\\ 2=\"str=special,end\" 100000\n"
        );

        assertThat(
                "measurement,t\"ag=value with space,tag2=value field=10000i,field 2=\"str=special,end\" 100000\n",
                "measurement,t\\\"ag=value\\ with\\ space,tag2=value field=10000i,field\\ 2=\"str=special,end\" 100000\n"
        );
    }

    @Test
    public void testSupportsUnquotedStrings() {
        assertThat(
                "measurement,t\"ag=value with space,tag2=value field=10000i,field 2=strend 100000\n",
                "measurement,t\\\"ag=value\\ with\\ space,tag2=value field=10000i,field\\ 2=strend 100000\n"
        );
    }

    @Test
    public void testSupportsUnquotedStringsWithQuoteInMiddle() {
        assertThat(
                "measurement,t\"ag=value with space,tag2=value field=10000i,field 2=str\"end 100000\n",
                "measurement,t\\\"ag=value\\ with\\ space,tag2=value field=10000i,field\\ 2=str\"end 100000\n"
        );

        assertThat(
                "measurement,t\"ag=value with space,tag2=value field=10000i,field 2=str\"end\" 100000\n",
                "measurement,t\\\"ag=value\\ with\\ space,tag2=value field=10000i,field\\ 2=str\"end\" 100000\n"
        );
    }

    @Test
    public void testSupportsUtf8Chars() {
        assertThat(
                "लаблअца,символ=значение1 поле=значение2,поле2=\"значение3\" 123\n",
                "लаблअца,символ=значение1 поле=значение2,поле2=\"значение3\" 123\n"
        );

        assertThat(
                "लаблअца,символ=значение2 161\n",
                "लаблअца,символ=значение2  161\n")
        ;
    }

    @Override
    public void testNoFieldValue2() {
        assertThat(
                "measurement,tag=x f= 10000\n",
                "measurement,tag=x f= 10000\n"
        );
    }

    @Override
    public void testNoFieldValue3() {
        assertThat(
                "measurement,tag=x f= 10000\n",
                "measurement,tag=x f=, 10000\n"
        );
    }

    @Override
    public void testDanglingCommaOnTag() {
        assertThat(
                "measurement,tag=value field=x 10000\n",
                "measurement,tag=value, field=x 10000\n"
        );
    }

    protected void assertThat(CharSequence expected, String lineStr) throws LineProtoException {
        assertThat(expected, lineStr, 1);
    }

    protected void assertThat(CharSequence expected, String lineStr, int start) throws LineProtoException {
        byte[] line = lineStr.getBytes(StandardCharsets.UTF_8);
        final int len = line.length;
        final boolean endWithEOL = line[len - 1] == '\n' || line[len - 1] == '\r';
        int fullLen = endWithEOL ? line.length : line.length + 1;
        long memFull = Unsafe.malloc(fullLen, MemoryTag.NATIVE_DEFAULT);
        long mem = Unsafe.malloc(fullLen, MemoryTag.NATIVE_DEFAULT);
        for (int j = 0; j < len; j++) {
            Unsafe.getUnsafe().putByte(memFull + j, line[j]);
        }
        if (!endWithEOL) {
            Unsafe.getUnsafe().putByte(memFull + len, (byte) '\n');
        }

        try {
            for (int i = start; i < len; i++) {
                for(int nextBreak = 0; nextBreak < len - i; nextBreak++) {
                    sink.clear();
                    resetParser(mem + fullLen);
                    parseMeasurement(memFull, mem, fullLen, i, 0);
                    if (nextBreak > 0) {
                        parseMeasurement(memFull, mem, fullLen, i + nextBreak, i);
                    }
                    boolean complete;
                    complete = parseMeasurement(memFull, mem, fullLen, fullLen, i + nextBreak);
                    Assert.assertTrue(complete);
                    if (!Chars.equals(expected, sink)) {
                        System.out.println(lineStr.substring(0, i));
                        if (nextBreak > 0) {
                            System.out.println(lineStr.substring(i, i + nextBreak));
                        }
                        System.out.println(lineStr.substring(i + nextBreak));
                        TestUtils.assertEquals("parse split " + i, expected, sink);
                    }
                }
            }
        } finally {
            Unsafe.free(mem, fullLen, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(memFull, fullLen, MemoryTag.NATIVE_DEFAULT);
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

    private boolean parseMeasurement(long fullBuffer, long parseBuffer, long buffersLen, long parseLen, long prevParseLen) {
        long shl = parseLen - prevParseLen;

        // This will copy ILP data from fullBuffer to parseBuffer so that the data ends at the end of the buffer
        long parseHi = parseBuffer + buffersLen;
        Vect.memmove(parseHi - parseLen, parseHi - prevParseLen, prevParseLen);
        Vect.memcpy(fullBuffer + prevParseLen, parseHi - shl, shl);

        // bufHi always the same, data alwasy ends at the end of the buffer
        // the only difference from iteration to iteration is where the data starts, which is set in shl
        protoParser.shl(shl);
        return parseMeasurement(parseHi);
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
                    case NewLineProtoParser.ENTITY_TYPE_INTEGER:
                case NewLineProtoParser.ENTITY_TYPE_LONG256:
                        sink.put(entity.getValue()).put('i');
                        break;
                default:
                    Chars.utf8Decode(entity.getValue().getLo(), entity.getValue().getHi(), sink);
                    break;
            }
        }

        if (protoParser.hasTimestamp()) {
            sink.put(' ');
            Numbers.append(sink, protoParser.getTimestamp());
        }
        sink.put('\n');
    }
}
