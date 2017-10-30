package com.questdb.parser.lp;

import com.questdb.std.str.AbstractCharSequence;
import com.questdb.std.str.ByteSequence;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class LineProtoLexerTest {

    private final static LineProtoLexer lexer = new LineProtoLexer(4096);
    private final StringSink sink = new StringSink();
    private final TestLineProtoParser lineAssemblingParser = new TestLineProtoParser();

    @AfterClass
    public static void tearDown() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        lexer.clear();
    }

    @Test
    public void testCommaInTagName() throws Exception {
        assertThat("measurement,t,ag=value,tag2=value field=10000i,field2=\"str\" 100000\n", "measurement,t\\,ag=value,tag2=value field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testCommaInTagValue() throws Exception {
        assertThat("measurement,tag=value,tag2=va,lue field=10000i,field2=\"str\" 100000\n", "measurement,tag=value,tag2=va\\,lue field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testCorruptUtf8Sequence() throws Exception {
        byte bytesA[] = "违法违,控网站漏洞风=不一定代,网站可能存在=комитета 的风险=10000i,вышел=\"险\" 100000\n".getBytes("UTF8");
        byte bytesB[] = {-116, -76, -55, 55, -34, 0, -11, 15, 13};
        byte bytesC[] = "меморандум,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n".getBytes("UTF8");

        byte bytes[] = new byte[bytesA.length + bytesB.length + bytesC.length];
        System.arraycopy(bytesA, 0, bytes, 0, bytesA.length);
        System.arraycopy(bytesB, 0, bytes, bytesA.length, bytesB.length);
        System.arraycopy(bytesC, 0, bytes, bytesA.length + bytesB.length, bytesC.length);
        assertThat("违法违,控网站漏洞风=不一定代,网站可能存在=комитета 的风险=10000i,вышел=\"险\" 100000\n" +
                        "-- error --\n" +
                        "меморандум,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n",
                bytes);
    }

    @Test
    public void testDanglingCommaOnTag() throws Exception {
        assertError("measurement,tag=value, field=x 10000\n", LineProtoParser.EVT_TAG_NAME, LineProtoParser.ERROR_EXPECTED, 22);
    }

    @Test
    public void testEmptyLine() throws Exception {
        assertThat("measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "measurement,tag=value3,tag2=value2 field=100i,field2=\"ok\"\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "\n" +
                        "measurement,tag=value3,tag2=value2 field=100i,field2=\"ok\"\n");
    }

    @Test
    public void testMissingFields() throws Exception {
        assertThat("measurement,field=10000i,field2=str\n", "measurement,field=10000i,field2=str");
    }

    @Test
    public void testMissingFields2() throws Exception {
        assertThat("measurement,field=10000i,field2=str\n", "measurement,field=10000i,field2=str\n");
    }

    @Test
    public void testMissingLineEnd() throws Exception {
        assertThat("measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n", "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000");
    }

    @Test
    public void testMissingTags() throws Exception {
        assertThat("measurement field=10000i,field2=\"str\"\n", "measurement field=10000i,field2=\"str\"");
    }

    @Test
    public void testMissingTimestamp() throws Exception {
        assertThat("measurement,tag=value,tag2=value field=10000i,field2=\"str\"\n", "measurement,tag=value,tag2=value field=10000i,field2=\"str\"");
    }

    @Test
    public void testMultiLines() throws Exception {
        assertThat("measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "measurement,tag=value3,tag2=value2 field=100i,field2=\"ok\"\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "measurement,tag=value3,tag2=value2 field=100i,field2=\"ok\"\n");
    }

    @Test
    public void testNoFieldName1() throws Exception {
        assertError("measurement,tag=x f=10i,f2 10000", LineProtoParser.EVT_FIELD_NAME, LineProtoParser.ERROR_EXPECTED, 26);
    }

    @Test
    public void testNoFieldName2() throws Exception {
        assertError("measurement,tag=x f=10i,=f2 10000", LineProtoParser.EVT_FIELD_NAME, LineProtoParser.ERROR_EMPTY, 24);
    }

    @Test
    public void testNoFieldName3() throws Exception {
        assertError("measurement,tag=x =10i,=f2 10000", LineProtoParser.EVT_FIELD_NAME, LineProtoParser.ERROR_EMPTY, 18);
    }

    @Test
    public void testNoFieldValue1() throws Exception {
        assertError("measurement,tag=x f 10000", LineProtoParser.EVT_FIELD_NAME, LineProtoParser.ERROR_EXPECTED, 19);
    }

    @Test
    public void testNoFieldValue2() throws Exception {
        assertError("measurement,tag=x f= 10000", LineProtoParser.EVT_FIELD_VALUE, LineProtoParser.ERROR_EMPTY, 20);
    }

    @Test
    public void testNoFieldValue3() throws Exception {
        assertError("measurement,tag=x f=, 10000", LineProtoParser.EVT_FIELD_VALUE, LineProtoParser.ERROR_EMPTY, 20);
    }

    @Test
    public void testNoFields1() throws Exception {
        assertError("measurement  \n", LineProtoParser.EVT_FIELD_NAME, LineProtoParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testNoFields2() throws Exception {
        assertError("measurement  ", LineProtoParser.EVT_FIELD_NAME, LineProtoParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testNoFields3() throws Exception {
        assertError("measurement  10000", LineProtoParser.EVT_FIELD_NAME, LineProtoParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testNoFields4() throws Exception {
        assertError("measurement,tag=x 10000", LineProtoParser.EVT_FIELD_NAME, LineProtoParser.ERROR_EXPECTED, 23);
    }

    @Test
    public void testNoMeasure1() throws Exception {
        assertError("tag=value field=x 10000\n", LineProtoParser.EVT_MEASUREMENT, LineProtoParser.ERROR_EXPECTED, 3);
    }

    @Test
    public void testNoMeasure2() throws Exception {
        assertError("tag=value field=x 10000\n", LineProtoParser.EVT_MEASUREMENT, LineProtoParser.ERROR_EXPECTED, 3);
    }

    @Test
    public void testNoTag4() throws Exception {
        assertError("measurement, \n", LineProtoParser.EVT_TAG_NAME, LineProtoParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testNoTagEquals1() throws Exception {
        assertError("measurement,tag field=x 10000\n", LineProtoParser.EVT_TAG_NAME, LineProtoParser.ERROR_EXPECTED, 15);
    }

    @Test
    public void testNoTagEquals2() throws Exception {
        assertError("measurement,tag, field=x 10000\n", LineProtoParser.EVT_TAG_NAME, LineProtoParser.ERROR_EXPECTED, 15);
    }

    @Test
    public void testNoTagValue1() throws Exception {
        assertError("measurement,tag= field=x 10000\n", LineProtoParser.EVT_TAG_VALUE, LineProtoParser.ERROR_EMPTY, 16);
    }

    @Test
    public void testNoTagValue2() throws Exception {
        assertError("measurement,tag=, field=x 10000\n", LineProtoParser.EVT_TAG_VALUE, LineProtoParser.ERROR_EMPTY, 16);
    }

    @Test
    public void testNoTagValue3() throws Exception {
        assertError("measurement,tag=", LineProtoParser.EVT_TAG_VALUE, LineProtoParser.ERROR_EMPTY, 16);
    }

    @Test
    public void testNoTagValue4() throws Exception {
        assertError("measurement,tag=\n", LineProtoParser.EVT_TAG_VALUE, LineProtoParser.ERROR_EMPTY, 16);
    }

    @Test
    public void testNoTags1() throws Exception {
        assertError("measurement,", LineProtoParser.EVT_TAG_NAME, LineProtoParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testNoTags2() throws Exception {
        assertError("measurement,\n", LineProtoParser.EVT_TAG_NAME, LineProtoParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testNoTags3() throws Exception {
        assertError("measurement, 100000\n", LineProtoParser.EVT_TAG_NAME, LineProtoParser.ERROR_EXPECTED, 12);
    }

    @Test
    public void testSimpleParse() throws Exception {
        assertThat("measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n", "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testSkipLine() throws Exception {
        assertThat("measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "measurement,tag=value3,tag2=value2 field=-- error --\n" +
                        "measurement,tag=value4,tag2=value4 field=200i,field2=\"super\"\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "measurement,tag=value3,tag2=value2 field=,field2=\"ok\"\n" +
                        "measurement,tag=value4,tag2=value4 field=200i,field2=\"super\"\n");
    }

    @Test
    public void testSpaceTagName() throws Exception {
        assertThat("measurement,t ag=value,tag2=value field=10000i,field2=\"str\" 100000\n", "measurement,t\\ ag=value,tag2=value field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testSpaceTagValue() throws Exception {
        assertThat("measurement,tag=value,tag2=valu e field=10000i,field2=\"str\" 100000\n", "measurement,tag=value,tag2=valu\\ e field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testTrailingSpace() throws Exception {
        assertError("measurement,tag=value,tag2=value field=10000i,field2=\"str\" \n" +
                "measurement,tag=value3,tag2=value2 field=100i,field2=\"ok\"\n", LineProtoParser.EVT_TIMESTAMP, LineProtoParser.ERROR_EMPTY, 59);
    }

    @Test
    public void testUtf8() throws Exception {
        assertThat("меморандум,кроме=никто,этом=комитета находился=10000i,вышел=\"Александр\" 100000\n", "меморандум,кроме=никто,этом=комитета находился=10000i,вышел=\"Александр\" 100000\n");
    }

    @Test
    public void testUtf8Measurement() throws Exception {
        assertThat("меморандум,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n", "меморандум,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testUtf8ThreeBytes() throws Exception {
        assertThat("违法违,控网站漏洞风=不一定代,网站可能存在=комитета 的风险=10000i,вышел=\"险\" 100000\n", "违法违,控网站漏洞风=不一定代,网站可能存在=комитета 的风险=10000i,вышел=\"险\" 100000\n");
    }

    private void assertError(CharSequence line, int state, int code, int position) throws LineProtoException, UnsupportedEncodingException {
        ByteArrayByteSequence bs = new ByteArrayByteSequence(line.toString().getBytes("UTF8"));
        final int len = bs.length();
        for (int i = 0; i < len; i++) {
            lineAssemblingParser.clear();
            lexer.clear();
            lexer.withParser(lineAssemblingParser);
            lexer.parse(bs.limit(0, i));
            lexer.parse(bs.limit(i, len - i));
            lexer.parseLast();
            Assert.assertEquals(state, lineAssemblingParser.errorState);
            Assert.assertEquals(code, lineAssemblingParser.errorCode);
            Assert.assertEquals(position, lineAssemblingParser.errorPosition);
        }
    }

    private void assertThat(CharSequence expected, CharSequence line) throws LineProtoException, UnsupportedEncodingException {
        assertThat(expected, line.toString().getBytes("UTF8"));
    }

    private void assertThat(CharSequence expected, byte[] line) throws LineProtoException, UnsupportedEncodingException {
        ByteArrayByteSequence bs = new ByteArrayByteSequence(line);
        final int len = bs.length();
        if (len < 10) {
            for (int i = 0; i < len; i++) {
                lineAssemblingParser.clear();
                lexer.clear();
                lexer.withParser(lineAssemblingParser);
                lexer.parse(bs.limit(0, i));
                lexer.parse(bs.limit(i, len - i));
                lexer.parseLast();
                TestUtils.assertEquals(expected, sink);
            }
        } else {
            for (int i = 0; i < len - 10; i++) {
                lineAssemblingParser.clear();
                lexer.clear();
                lexer.withParser(lineAssemblingParser);
                lexer.parse(bs.limit(0, i));
                lexer.parse(bs.limit(i, 10));
                lexer.parse(bs.limit(i + 10, len - i - 10));
                lexer.parseLast();
                TestUtils.assertEquals(expected, sink);
            }
        }

        // assert small buffer
        LineProtoLexer smallBufLexer = new LineProtoLexer(64);
        lineAssemblingParser.clear();
        smallBufLexer.withParser(lineAssemblingParser);
        smallBufLexer.parse(bs.limit(0, len));
        smallBufLexer.parseLast();
        TestUtils.assertEquals(expected, sink);
    }

    private static class ByteArrayByteSequence extends AbstractCharSequence implements ByteSequence {

        private final byte[] array;
        private int top = 0;
        private int len;

        public ByteArrayByteSequence(byte[] array) {
            this.array = array;
            this.len = array.length;
        }

        @Override
        public byte byteAt(int index) {
            return array[top + index];
        }

        @Override
        public int length() {
            return len;
        }

        @Override
        public char charAt(int index) {
            return (char) byteAt(index);
        }

        ByteArrayByteSequence limit(int top, int len) {
            this.top = top;
            this.len = len;
            return this;
        }
    }

    private class TestLineProtoParser implements LineProtoParser {
        boolean fields = false;
        int errorState;
        int errorCode;
        int errorPosition;
        HashMap<Long, String> tokens = new HashMap<>();

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
        public void onEvent(CachedCharSequence token, int type) {
            Assert.assertNull(tokens.put(token.getCacheAddress(), token.toString()));

            switch (type) {
                case LineProtoParser.EVT_MEASUREMENT:
                    sink.put(token);
                    break;
                case LineProtoParser.EVT_TAG_NAME:
                    sink.put(',').put(token).put('=');
                    break;
                case LineProtoParser.EVT_FIELD_NAME:
                    if (fields) {
                        sink.put(',');
                    } else {
                        try {
                            fields = true;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        sink.put(' ');
                    }
                    sink.put(token).put('=');
                    break;
                case LineProtoParser.EVT_TAG_VALUE:
                case LineProtoParser.EVT_FIELD_VALUE:
                    sink.put(token);
                    break;
                case LineProtoParser.EVT_TIMESTAMP:
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