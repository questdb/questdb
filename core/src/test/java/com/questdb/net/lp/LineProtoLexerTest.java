package com.questdb.net.lp;

import com.questdb.misc.Unsafe;
import com.questdb.std.str.ByteSequence;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
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
        assertThatMovingBuffers("measurement,t\\,ag=value,tag2=value field=10000i,field2=\"str\" 100000\n", "measurement,t\\,ag=value,tag2=value field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testCommaInTagValue() throws Exception {
        assertThatMovingBuffers("measurement,tag=value,tag2=va\\,lue field=10000i,field2=\"str\" 100000\n", "measurement,tag=value,tag2=va\\,lue field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testEmptyLine() throws Exception {
        assertThatMovingBuffers("measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "measurement,tag=value3,tag2=value2 field=100i,field2=\"ok\"\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "\n" +
                        "measurement,tag=value3,tag2=value2 field=100i,field2=\"ok\"\n");
    }

    @Test
    public void testMissingFields() throws Exception {
        assertThatMovingBuffers("measurement,field=10000i,field2=str\n", "measurement,field=10000i,field2=str");
    }

    @Test
    public void testMissingFields2() throws Exception {
        assertThatMovingBuffers("measurement,field=10000i,field2=str\n", "measurement,field=10000i,field2=str\n");
    }

    @Test
    public void testMissingLineEnd() throws Exception {
        assertThatMovingBuffers("measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n", "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000");
    }

    @Test
    public void testMissingTags() throws Exception {
        assertThatMovingBuffers("measurement field=10000i,field2=\"str\"\n", "measurement field=10000i,field2=\"str\"");
    }

    @Test
    public void testMissingTimestamp() throws Exception {
        assertThatMovingBuffers("measurement,tag=value,tag2=value field=10000i,field2=\"str\"\n", "measurement,tag=value,tag2=value field=10000i,field2=\"str\"");
    }

    @Test
    public void testMultiLines() throws Exception {
        assertThatMovingBuffers("measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "measurement,tag=value3,tag2=value2 field=100i,field2=\"ok\"\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n" +
                        "measurement,tag=value3,tag2=value2 field=100i,field2=\"ok\"\n");
    }

    @Test
    public void testSimpleParse() throws Exception {
        assertThatMovingBuffers("measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n", "measurement,tag=value,tag2=value field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testSpaceTagName() throws Exception {
        assertThatMovingBuffers("measurement,t\\ ag=value,tag2=value field=10000i,field2=\"str\" 100000\n", "measurement,t\\ ag=value,tag2=value field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testSpaceTagValue() throws Exception {
        assertThatMovingBuffers("measurement,tag=value,tag2=valu\\ e field=10000i,field2=\"str\" 100000\n", "measurement,tag=value,tag2=valu\\ e field=10000i,field2=\"str\" 100000\n");
    }

    @Test
    public void testTrailingSpace() throws Exception {
        assertThatMovingBuffers("measurement,tag=value,tag2=value field=10000i,field2=\"str\"\n" +
                        "measurement,tag=value3,tag2=value2 field=100i,field2=\"ok\"\n",
                "measurement,tag=value,tag2=value field=10000i,field2=\"str\" \n" +
                        "measurement,tag=value3,tag2=value2 field=100i,field2=\"ok\"\n");
    }

    private void assertThatMovingBuffers(CharSequence expected, CharSequence line) {
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