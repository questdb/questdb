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

package io.questdb.test.cutlass.line.udp;

import io.questdb.cutlass.line.LineException;
import io.questdb.cutlass.line.udp.CachedCharSequence;
import io.questdb.cutlass.line.udp.CharSequenceCache;
import io.questdb.cutlass.line.udp.LineUdpLexer;
import io.questdb.cutlass.line.udp.LineUdpParser;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class LineUdpLexerTest extends AbstractCairoTest {

    private final static LineUdpLexer lexer = new LineUdpLexer(4096);
    protected final StringSink sink = new StringSink();
    private final TestLineUdpParser lineAssemblingParser = new TestLineUdpParser();

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
        byte[] bytesA = "违法违,控网站漏洞风=不一定代,网站可能存在=комитета 的风险=10000i,вышел=\"险\" 100000\n".getBytes(Files.UTF_8);
        byte[] bytesB = {-116, -76, -55, 55, -34, 0, -11, 15, 13};
        byte[] bytesC = "меморандум,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n".getBytes(Files.UTF_8);

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
        assertError("measurement,tag=value, field=x 10000\n", LineUdpParser.EVT_TAG_NAME, LineUdpParser.ERROR_EXPECTED, 22);
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
        assertError("measurement,tag=x f=10i,f2 10000", LineUdpParser.EVT_FIELD_NAME, LineUdpParser.ERROR_EXPECTED, 26);
    }

    @Test
    public void testNoFieldName2() {
        assertError("measurement,tag=x f=10i,=f2 10000", LineUdpParser.EVT_FIELD_NAME, LineUdpParser.ERROR_EMPTY, 24);
    }

    @Test
    public void testNoFieldName3() {
        assertError("measurement,tag=x =10i,=f2 10000", LineUdpParser.EVT_FIELD_NAME, LineUdpParser.ERROR_EMPTY, 18);
    }

    @Test
    public void testNoFieldValue1() {
        assertError("measurement,tag=x f 10000", LineUdpParser.EVT_FIELD_NAME, LineUdpParser.ERROR_EXPECTED, 19);
    }

    @Test
    public void testNoFieldValue2() {
        assertError("measurement,tag=x f= 10000", 0, 0, 0);
        assertThat("measurement,tag=x f= 10000\n", "measurement,tag=x f= 10000\n");
    }

    @Test
    public void testNoFieldValue3() {
        assertError("measurement,tag=x f=, 10000", LineUdpParser.EVT_FIELD_NAME, LineUdpParser.ERROR_EXPECTED, 21);
    }

    @Test
    public void testNoFields1() {
        assertError("measurement  \n", LineUdpParser.EVT_FIELD_NAME, LineUdpParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testNoFields2() {
        assertError("measurement  ", LineUdpParser.EVT_FIELD_NAME, LineUdpParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testNoFields3() {
        assertError("measurement  10000", LineUdpParser.EVT_FIELD_NAME, LineUdpParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testNoMeasure1() {
        assertError("tag=value field=x 10000\n", LineUdpParser.EVT_MEASUREMENT, LineUdpParser.ERROR_EXPECTED, 3);
    }

    @Test
    public void testNoMeasure2() {
        assertError("tag=value field=x 10000\n", LineUdpParser.EVT_MEASUREMENT, LineUdpParser.ERROR_EXPECTED, 3);
    }

    @Test
    public void testNoTag4() {
        assertError("measurement, \n", LineUdpParser.EVT_TAG_NAME, LineUdpParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testNoTagEquals1() {
        assertError("measurement,tag field=x 10000\n", LineUdpParser.EVT_TAG_NAME, LineUdpParser.ERROR_EXPECTED, 15);
    }

    @Test
    public void testNoTagEquals2() {
        assertError("measurement,tag, field=x 10000\n", LineUdpParser.EVT_TAG_NAME, LineUdpParser.ERROR_EXPECTED, 15);
    }

    @Test
    public void testNoTagValue1() {
        assertError("measurement,tag= field=x 10000\n", LineUdpParser.EVT_TAG_VALUE, LineUdpParser.ERROR_EMPTY, 16);
    }

    @Test
    public void testNoTagValue2() {
        assertError("measurement,tag=, field=x 10000\n", LineUdpParser.EVT_TAG_VALUE, LineUdpParser.ERROR_EMPTY, 16);
    }

    @Test
    public void testNoTagValue3() {
        assertError("measurement,tag=", LineUdpParser.EVT_TAG_VALUE, LineUdpParser.ERROR_EMPTY, 16);
    }

    @Test
    public void testNoTagValue4() {
        assertError("measurement,tag=\n", LineUdpParser.EVT_TAG_VALUE, LineUdpParser.ERROR_EMPTY, 16);
    }

    @Test
    public void testNoTags1() {
        assertError("measurement,", LineUdpParser.EVT_TAG_NAME, LineUdpParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testNoTags2() {
        assertError("measurement,\n", LineUdpParser.EVT_TAG_NAME, LineUdpParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testNoTags3() {
        assertError("measurement, 100000\n", LineUdpParser.EVT_TAG_NAME, LineUdpParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testSimpleParse() {
        assertThat("measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n", "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testSkipLine() {
        assertThat("measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "measurement,tag=value3,tag2=value2 field=,field2=\"ok\"\n" +
                        "measurement,tag=value3,tag2=value2 field=-- error --\n" +
                        "measurement,tag=value4,tag2=value4 field=200i,field2=\"super\"\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "measurement,tag=value3,tag2=value2 field=,field2=\"ok\"\n" +
                        "measurement,tag=value3,tag2=value2 field= field2=\"not ok\"\n" +
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
                "measurement,tag=value3,tag2=value2 field=100i,field2=\"ok\"\n", LineUdpParser.EVT_TIMESTAMP, LineUdpParser.ERROR_EMPTY, 59);
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

    protected void assertError(CharSequence line, int state, int code, int position) throws LineException {
        byte[] bytes = line.toString().getBytes(Files.UTF_8);
        long mem = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            final int len = bytes.length;
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(mem + i, bytes[i]);
            }
            for (int i = 0; i < len; i++) {
                lineAssemblingParser.clear();
                lexer.clear();
                lexer.withParser(lineAssemblingParser);
                lexer.parse(mem, mem + i);
                lexer.parse(mem + i, mem + len);
                lexer.parseLast();
                Assert.assertEquals(state, lineAssemblingParser.errorState);
                Assert.assertEquals(code, lineAssemblingParser.errorCode);
                Assert.assertEquals(position, lineAssemblingParser.errorPosition);
            }
        } finally {
            Unsafe.free(mem, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    protected void assertThat(CharSequence expected, CharSequence line) throws LineException {
        assertThat(expected, line.toString().getBytes(Files.UTF_8));
    }

    protected void assertThat(CharSequence expected, byte[] line) throws LineException {
        final int len = line.length;
        long mem = Unsafe.malloc(line.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(mem + i, line[i]);
            }

            if (len < 10) {
                for (int i = 0; i < len; i++) {
                    lineAssemblingParser.clear();
                    lexer.clear();
                    lexer.withParser(lineAssemblingParser);
                    lexer.parse(mem, mem + i);
                    lexer.parse(mem + i, mem + len);
                    lexer.parseLast();
                    TestUtils.assertEquals(expected, sink);
                }
            } else {
                for (int i = 0; i < len - 10; i++) {
                    lineAssemblingParser.clear();
                    lexer.clear();
                    lexer.withParser(lineAssemblingParser);
                    lexer.parse(mem, mem + i);
                    lexer.parse(mem + i, mem + i + 10);
                    lexer.parse(mem + i + 10, mem + len);
                    lexer.parseLast();
                    TestUtils.assertEquals(expected, sink);
                }
            }

            // assert small buffer
            LineUdpLexer smallBufLexer = new LineUdpLexer(64);
            lineAssemblingParser.clear();
            smallBufLexer.withParser(lineAssemblingParser);
            smallBufLexer.parse(mem, mem + len);
            smallBufLexer.parseLast();
            TestUtils.assertEquals(expected, sink);
        } finally {
            Unsafe.free(mem, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private class TestLineUdpParser implements LineUdpParser {
        final HashMap<Long, String> tokens = new HashMap<>();
        int errorCode;
        int errorPosition;
        int errorState;
        boolean fields = false;

        @Override
        public void onError(int position, int state, int code) {
            this.errorCode = code;
            this.errorPosition = position;
            this.errorState = state;
            this.fields = false;
            sink.put("-- error --\n");
            tokens.clear();
        }

        @Override
        public void onEvent(CachedCharSequence token, int type, CharSequenceCache cache) {
            Assert.assertNull(tokens.put(token.getCacheAddress(), token.toString()));

            switch (type) {
                case EVT_TAG_NAME:
                    sink.put(',').put(token).put('=');
                    break;
                case EVT_FIELD_NAME:
                    if (fields) {
                        sink.put(',');
                    } else {
                        fields = true;
                        sink.put(' ');
                    }
                    sink.put(token).put('=');
                    break;
                case EVT_TAG_VALUE:
                case EVT_FIELD_VALUE:
                case EVT_MEASUREMENT:
                    sink.put(token);
                    break;
                case EVT_TIMESTAMP:
                    if (token.length() > 0) {
                        sink.put(' ').put(token);
                    }
                    break;
                default:
                    break;

            }
        }

        @Override
        public void onLineEnd(CharSequenceCache cache) {
            sink.put('\n');

            // assert that cached token match
            for (Map.Entry<Long, String> e : tokens.entrySet()) {
                TestUtils.assertEquals(e.getValue(), cache.get(e.getKey()));
            }

            tokens.clear();
            fields = false;
        }

        private void clear() {
            sink.clear();
            errorCode = 0;
            errorPosition = 0;
            tokens.clear();
        }
    }
}
