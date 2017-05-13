package com.questdb.net.lp;

import com.questdb.misc.Unsafe;
import com.questdb.std.str.ByteSequence;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LineProtoLexerTest {

    private final static LineProtoLexer lexer = new LineProtoLexer();
    private final StringSink sink = new StringSink();
    private final LineProtoListener lineAssemblingListener = new LineProtoListener() {
        boolean fields = false;

        @Override
        public void onEvent(ByteSequence token, int type) {
            switch (type) {
                case LineProtoLexer.EVT_MEASUREMENT:
                    sink.put(token);
                    break;
                case LineProtoLexer.EVT_TAG_NAME:
                    sink.put(',').put(token).put('=');
                    break;
                case LineProtoLexer.EVT_FIELD_NAME:
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
                case LineProtoLexer.EVT_TAG_VALUE:
                case LineProtoLexer.EVT_FIELD_VALUE:
                    sink.put(token);
                    break;
                case LineProtoLexer.EVT_TIMESTAMP:
                    if (token.length() > 0) {
                        sink.put(' ').put(token);
                    }
                    break;
                case LineProtoLexer.EVT_END:
                    sink.put('\n');
                    fields = false;
                    break;
                default:
                    break;

            }

        }
    };

    @AfterClass
    public static void tearDown() throws Exception {
        lexer.close();
    }

    @Before
    public void setUp() throws Exception {
        lexer.clear();
    }

    @Test
    public void testCommaInTagName() throws Exception {
        assertThat("measurement,t\\,ag=value,tag2=value field=10000i,field2=\"str\" 100000\n", "measurement,t\\,ag=value,tag2=value field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testCommaInTagValue() throws Exception {
        assertThat("measurement,tag=value,tag2=va\\,lue field=10000i,field2=\"str\" 100000\n", "measurement,tag=value,tag2=va\\,lue field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testDanglingCommaOnTag() throws Exception {
        assertError("measurement,tag=value, field=x 10000\n");
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
        assertError("measurement,tag=x f=10i,f2 10000");
    }

    @Test
    public void testNoFieldName2() throws Exception {
        assertError("measurement,tag=x f=10i,=f2 10000");
    }

    @Test
    public void testNoFieldName3() throws Exception {
        assertError("measurement,tag=x =10i,=f2 10000");
    }

    @Test
    public void testNoFieldValue1() throws Exception {
        assertError("measurement,tag=x f 10000");
    }

    @Test
    public void testNoFieldValue2() throws Exception {
        assertError("measurement,tag=x f= 10000");
    }

    @Test
    public void testNoFieldValue3() throws Exception {
        assertError("measurement,tag=x f=, 10000");
    }

    @Test
    public void testNoFields1() throws Exception {
        assertError("measurement  \n");
    }

    @Test
    public void testNoFields2() throws Exception {
        assertError("measurement  ");
    }

    @Test
    public void testNoFields3() throws Exception {
        assertError("measurement  10000");
    }

    @Test
    public void testNoFields4() throws Exception {
        assertError("measurement,tag=x 10000");
    }

    @Test
    public void testNoMeasure1() throws Exception {
        assertError("tag=value field=x 10000\n");
    }

    @Test
    public void testNoMeasure2() throws Exception {
        assertError("tag=value field=x 10000\n");
    }

    @Test
    public void testNoTag4() throws Exception {
        assertError("measurement, \n");
    }

    @Test
    public void testNoTagEquals1() throws Exception {
        assertError("measurement,tag field=x 10000\n");
    }

    @Test
    public void testNoTagEquals2() throws Exception {
        assertError("measurement,tag, field=x 10000\n");
    }

    @Test
    public void testNoTagValue1() throws Exception {
        assertError("measurement,tag= field=x 10000\n");
    }

    @Test
    public void testNoTagValue2() throws Exception {
        assertError("measurement,tag=, field=x 10000\n");
    }

    @Test
    public void testNoTagValue3() throws Exception {
        assertError("measurement,tag=");
    }

    @Test
    public void testNoTagValue4() throws Exception {
        assertError("measurement,tag=\n");
    }

    @Test
    public void testNoTags1() throws Exception {
        assertError("measurement,");
    }

    @Test
    public void testNoTags2() throws Exception {
        assertError("measurement,\n");
    }

    @Test
    public void testNoTags3() throws Exception {
        assertError("measurement, 100000\n");
    }

    @Test
    public void testSimpleParse() throws Exception {
        assertThat("measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n", "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testSpaceTagName() throws Exception {
        assertThat("measurement,t\\ ag=value,tag2=value field=10000i,field2=\"str\" 100000\n", "measurement,t\\ ag=value,tag2=value field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testSpaceTagValue() throws Exception {
        assertThat("measurement,tag=value,tag2=valu\\ e field=10000i,field2=\"str\" 100000\n", "measurement,tag=value,tag2=valu\\ e field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testTrailingSpace() throws Exception {
        assertError("measurement,tag=value,tag2=value field=10000i,field2=\"str\" \n" +
                "measurement,tag=value3,tag2=value2 field=100i,field2=\"ok\"\n");
    }

    private void assertError(CharSequence line) throws LineProtoException {
        final int len = line.length();
        long ptr = TestUtils.toMemory(line);
        try {
            for (int i = 0; i < len; i++) {
                sink.clear();
                try {
                    lexer.clear();
                    lexer.parse(ptr, i, lineAssemblingListener);
                    lexer.parse(ptr + i, len - i, lineAssemblingListener);
                    lexer.parseLast(lineAssemblingListener);
                    Assert.fail();
                } catch (LineProtoException ignored) {

                }
            }
        } finally {
            Unsafe.free(ptr, len);
        }
    }

    private void assertThat(CharSequence expected, CharSequence line) throws LineProtoException {
        final int len = line.length();
        long ptr = TestUtils.toMemory(line);
        try {
            for (int i = 0; i < len; i++) {
                sink.clear();
                lexer.clear();
                lexer.parse(ptr, i, lineAssemblingListener);
                lexer.parse(ptr + i, len - i, lineAssemblingListener);
                lexer.parseLast(lineAssemblingListener);
                TestUtils.assertEquals(expected, sink);
            }
        } finally {
            Unsafe.free(ptr, len);
        }
    }

}