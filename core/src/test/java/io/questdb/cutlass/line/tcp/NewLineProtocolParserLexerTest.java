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
import io.questdb.cutlass.line.LineProtoLexer;
import io.questdb.cutlass.line.LineProtoParser;
import org.junit.Assert;

import io.questdb.cutlass.line.LineProtoException;
import io.questdb.cutlass.line.tcp.NewLineProtoParser.ErrorCode;
import io.questdb.cutlass.line.tcp.NewLineProtoParser.ParseResult;
import io.questdb.cutlass.line.tcp.NewLineProtoParser.ProtoEntity;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class NewLineProtocolParserLexerTest {
    private final static LineProtoLexer lexer = new LineProtoLexer(4096);
    private final NewLineProtoParser protoParser = new NewLineProtoParser();
    protected final StringSink sink = new StringSink();
    private ErrorCode lastErrorCode;
    private boolean onErrorLine;
    private long startOfLineAddr;

    @Before
    public void setUp() {
        lexer.clear();
    }

    @Test
    public void testCommaInTagName() {
        assertThat(
                "measurement,t,ag=value,tag2=value field=10000i,field2=\"str\" 100000\n",
                "measurement,t\\,ag=value,tag2=value field=10000i,field2=\"str\" 100000\n"
        );
    }

    @Test
    public void testCommaInTagValue() {
        assertThat("measurement,tag=value,tag2=va,lue field=10000i,field2=\"str\" 100000\n", "measurement,tag=value,tag2=va\\,lue field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testCorruptUtf8Sequence() {
        byte[] bytesA = "违法违,控网站漏洞风=不一定代,网站可能存在=комитета 的风险=10000i,вышел=\"险\" 100000\n".getBytes(StandardCharsets.UTF_8);
        byte[] bytesB = {-116, -76, -55, 55, -34, 0, -11, 15, 13};
        byte[] bytesC = "меморандум,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n".getBytes(StandardCharsets.UTF_8);

        byte[] bytes = new byte[bytesA.length + bytesB.length + bytesC.length];
        System.arraycopy(bytesA, 0, bytes, 0, bytesA.length);
        System.arraycopy(bytesB, 0, bytes, bytesA.length, bytesB.length);
        System.arraycopy(bytesC, 0, bytes, bytesA.length + bytesB.length, bytesC.length);
        assertThat("违法违,控网站漏洞风=不一定代,网站可能存在=комитета 的风险=10000i,вышел=\"险\" 100000\n" +
                        "-- error --\n" +
                        "меморандум,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n",
                bytes);
    }

    @Test
    public void testDanglingCommaOnTag() {
        assertThat("measurement,tag=value field=x 10000\n", "measurement,tag=value, field=x 10000\n");
    }

    @Test
    public void testEmptyLine() {
        assertThat("measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "measurement,tag=value3,tag2=value2 field=100i,field2=\"ok\"\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "\n" +
                        "measurement,tag=value3,tag2=value2 field=100i,field2=\"ok\"\n");
    }

    @Test
    public void testMissingFields() {
        assertThat("measurement,field=10000i,field2=str\n", "measurement,field=10000i,field2=str");
    }

    @Test
    public void testMissingFields2() {
        assertThat("measurement,field=10000i,field2=str\n", "measurement,field=10000i,field2=str\n");
    }

    @Test
    public void testMissingLineEnd() {
        assertThat("measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n", "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000");
    }

    @Test
    public void testMissingTags() {
        assertThat("measurement field=10000i,field2=\"str\"\n", "measurement field=10000i,field2=\"str\"");
    }

    @Test
    public void testMissingTimestamp() {
        assertThat("measurement,tag=value,tag2=value field=10000i,field2=\"str\"\n", "measurement,tag=value,tag2=value field=10000i,field2=\"str\"");
    }

    @Test
    public void testMultiLines() {
        assertThat("measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "measurement,tag=value3,tag2=value2 field=100i,field2=\"ok\"\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "measurement,tag=value3,tag2=value2 field=100i,field2=\"ok\"\n");
    }

    @Test
    public void testNoFieldName1() {
        assertError("measurement,tag=x f=10i,f2 10000", LineProtoParser.EVT_FIELD_NAME, LineProtoParser.ERROR_EXPECTED, 26);
    }

    @Test
    public void testNoFieldName2() {
        assertError("measurement,tag=x f=10i,=f2 10000", LineProtoParser.EVT_FIELD_NAME, LineProtoParser.ERROR_EMPTY, 24);
    }

    @Test
    public void testNoFieldName3() {
        assertError("measurement,tag=x =10i,=f2 10000", LineProtoParser.EVT_FIELD_NAME, LineProtoParser.ERROR_EMPTY, 18);
    }

    @Test
    public void testNoFieldValue1() {
        assertError("measurement,tag=x f 10000", LineProtoParser.EVT_FIELD_NAME, LineProtoParser.ERROR_EXPECTED, 19);
    }

    @Test
    public void testNoFieldValue2() {
        assertThat("measurement,tag=x f= 10000\n", "measurement,tag=x f= 10000\n");
    }

    @Test
    public void testNoFieldValue3() {
        assertThat("measurement,tag=x f= 10000\n", "measurement,tag=x f=, 10000\n");
    }

    @Test
    public void testNoFields1() {
        assertError("measurement  \n", LineProtoParser.EVT_FIELD_NAME, LineProtoParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testNoFields2() {
        assertError("measurement  ", LineProtoParser.EVT_FIELD_NAME, LineProtoParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testNoFields3() {
        assertError("measurement  10000", LineProtoParser.EVT_FIELD_NAME, LineProtoParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testNoFields4() {
        assertError("measurement,tag=x 10000", LineProtoParser.EVT_FIELD_NAME, LineProtoParser.ERROR_EXPECTED, 23);
    }

    @Test
    public void testNoMeasure1() {
        assertError("tag=value field=x 10000\n", LineProtoParser.EVT_MEASUREMENT, LineProtoParser.ERROR_EXPECTED, 3);
    }

    @Test
    public void testNoMeasure2() {
        assertError("tag=value field=x 10000\n", LineProtoParser.EVT_MEASUREMENT, LineProtoParser.ERROR_EXPECTED, 3);
    }

    @Test
    public void testNoTag4() {
        assertError("measurement, \n", LineProtoParser.EVT_TAG_NAME, LineProtoParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testNoTagEquals1() {
        assertError("measurement,tag field=x 10000\n", LineProtoParser.EVT_TAG_NAME, LineProtoParser.ERROR_EXPECTED, 15);
    }

    @Test
    public void testNoTagEquals2() {
        assertError("measurement,tag, field=x 10000\n", LineProtoParser.EVT_TAG_NAME, LineProtoParser.ERROR_EXPECTED, 15);
    }

    @Test
    public void testNoTagValue1() {
        assertThat("measurement,tag= field=x 10000\n", "measurement,tag= field=x 10000\n");
    }

    @Test
    public void testNoTagValue2() {
        assertThat("measurement,tag= field=x 10000\n", "measurement,tag=, field=x 10000\n");
    }

    @Test
    public void testNoTagValue3() {
        assertThat("measurement,tag=\n", "measurement,tag=");
    }

    @Test
    public void testNoTagValue4() {
        assertThat("measurement,tag=\n", "measurement,tag=\n");
    }

    @Test
    public void testNoTags1() {
        assertError("measurement,", LineProtoParser.EVT_TAG_NAME, LineProtoParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testNoTags2() {
        assertError("measurement,\n", LineProtoParser.EVT_TAG_NAME, LineProtoParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testNoTags3() {
        assertError("measurement, 100000\n", LineProtoParser.EVT_TAG_NAME, LineProtoParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testStringsInTag() {
        assertError("measurement,tag=\"james\" 100000\n", LineProtoParser.EVT_TAG_NAME, LineProtoParser.ERROR_EXPECTED, 12);
        assertError("measurement,tag=\"\" 100000\n", LineProtoParser.EVT_TAG_NAME, LineProtoParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testSimpleParse() {
        assertThat("measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testSkipLine() {
        assertThat("measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "measurement,tag=value3,tag2=value2 field=,field2=\"ok\"\n" +
                        "measurement,tag=value4,tag2=value4 field=200i,field2=\"super\"\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "measurement,tag=value3,tag2=value2 field=,field2=\"ok\"\n" +
                        "measurement,tag=value4,tag2=value4 field=200i,field2=\"super\"\n");
    }

    @Test
    public void testSpaceTagName() {
        assertThat("measurement,t ag=value,tag2=value field=10000i,field2=\"str\" 100000\n", "measurement,t\\ ag=value,tag2=value field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testSpaceTagValue() {
        assertThat("measurement,tag=value,tag2=valu e field=10000i,field2=\"str\" 100000\n", "measurement,tag=value,tag2=valu\\ e field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testTrailingSpace() {
        assertError("measurement,tag=value,tag2=value field=10000i,field2=\"str\" \n" +
                "measurement,tag=value3,tag2=value2 field=100i,field2=\"ok\"\n", LineProtoParser.EVT_TIMESTAMP, LineProtoParser.ERROR_EMPTY, 59);
    }

    @Test
    public void testUtf8() {
        assertThat("меморандум,кроме=никто,этом=комитета находился=10000i,вышел=\"Александр\" 100000\n", "меморандум,кроме=никто,этом=комитета находился=10000i,вышел=\"Александр\" 100000\n");
    }

    @Test
    public void testUtf8Measurement() {
        assertThat("меморандум,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n", "меморандум,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testUtf8ThreeBytes() {
        assertThat("违法违,控网站漏洞风=不一定代,网站可能存在=комитета 的风险=10000i,вышел=\"险\" 100000\n", "违法违,控网站漏洞风=不一定代,网站可能存在=комитета 的风险=10000i,вышел=\"险\" 100000\n");
    }

    protected void assertThat(CharSequence expected, CharSequence line) throws LineProtoException {
        assertThat(expected, line.toString().getBytes(StandardCharsets.UTF_8));
    }

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
                    case NewLineProtoParser.ENTITY_TYPE_INTEGER:
                case NewLineProtoParser.ENTITY_TYPE_LONG256:
                        sink.put(entity.getValue()).put('i');
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
