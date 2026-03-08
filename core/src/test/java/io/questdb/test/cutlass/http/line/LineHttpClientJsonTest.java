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

package io.questdb.test.cutlass.http.line;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.std.bytes.DirectByteSlice;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.json.JsonParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlKeywords;
import io.questdb.std.*;
import io.questdb.std.histogram.org.HdrHistogram.Base64Helper;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.Base64;

public class LineHttpClientJsonTest extends AbstractBootstrapTest {

    public void assertSql(CairoEngine engine, CharSequence sql, CharSequence expectedResult) throws SqlException {
        StringSink sink = Misc.getThreadLocalSink();
        engine.print(sql, sink);
        if (!Chars.equals(sink, expectedResult)) {
            Assert.assertEquals(expectedResult, sink);
        }
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testIngestion() throws Exception {
        FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
        String pp = TestUtils.getTestResourcePath("/io/questdb/test/cutlass/line/interop/client-protocol-version-2-test.json");

        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                int port = serverMain.getHttpServerPort();
                try (
                        JsonLexer lexer = new JsonLexer(1024, 1024);
                        Sender sender = Sender.builder(Sender.Transport.HTTP)
                                .address("localhost:" + port)
                                .autoFlushRows(Integer.MAX_VALUE)
                                .autoFlushIntervalMillis(Integer.MAX_VALUE)
                                .build();
                        JsonTestSuiteParser parser = new JsonTestSuiteParser(sender, serverMain);
                        Path path = new Path().of(pp)) {
                    long fd = ff.openRO(path.$());
                    assert fd > 0;
                    final long memSize = 1024 * 1024;
                    final long mem = Unsafe.malloc(memSize, MemoryTag.NATIVE_DEFAULT);
                    try {
                        final long size = ff.length(fd);
                        long offset = 0;
                        while (offset < size) {
                            final long r = ff.read(fd, mem, memSize, offset);
                            lexer.parse(mem, mem + r, parser);
                            offset += r;
                        }
                    } finally {
                        Unsafe.free(mem, memSize, MemoryTag.NATIVE_DEFAULT);
                        ff.close(fd);
                    }
                }
            }
        });
    }

    private static class JsonTestSuiteParser implements JsonParser, QuietCloseable {

        public static final int TAG_ARRAY_ELEM_DATA = 24;
        public static final int TAG_ARRAY_ELEM_STRIDES = 23;
        public static final int TAG_ARRAY_ELEM_TYPE = 21;
        public static final int TAG_ARRAY_RANK = 22;
        public static final int TAG_ARRAY_SHAPE = 20;
        public static final int TAG_BUFFER = 9;
        public static final int TAG_BUFFER_EXPECT = 30;
        public static final int TAG_BUFFER_STATUS = 29;
        public static final int TAG_COLUMNS = 7;
        public static final int TAG_COLUMN_NAME = 4;
        public static final int TAG_COLUMN_TYPE = 8;
        public static final int TAG_COLUMN_VALUE = 5;
        public static final int TAG_QUERY = 25;
        public static final int TAG_QUERYS = 10;
        public static final int TAG_QUERY_ERROR = 28;
        public static final int TAG_QUERY_EXPECT = 27;
        public static final int TAG_QUERY_STATUS = 26;
        public static final int TAG_SYMBOLS = 6;
        public static final int TAG_SYMBOL_NAME = 2;
        public static final int TAG_SYMBOL_VALUE = 3;
        public static final int TAG_TABLE_NAME = 1;
        public static final int TAG_TEST_NAME = 0;
        private final Sender sender;
        private final TestServerMain serverMain;
        private final IntList shapes = new IntList(8);
        private final IntList strides = new IntList(8);
        private final StringSink stringSink = new StringSink();
        private DoubleArray array;
        private int arrayElementType = -1;
        private int columnType = -1;
        private boolean encounteredError;
        private boolean ignoreCurrentRow = false;
        private String name;
        private String queryError;
        private String queryExpected;
        private String queryStatus;
        private String queryText;
        private String tableName;
        private int tag1Type = -1;
        private int tag2Type = -1;

        public JsonTestSuiteParser(Sender sender, TestServerMain serverMain) {
            this.sender = sender;
            this.serverMain = serverMain;
        }

        private static CharSequence unescape(CharSequence tag, StringSink stringSink) {
            if (tag == null) {
                return null;
            }
            stringSink.clear();

            for (int i = 0, n = tag.length(); i < n; i++) {
                char sourceChar = tag.charAt(i);
                if (sourceChar != '\\') {
                    // happy-path, nothing to unescape
                    stringSink.put(sourceChar);
                } else {
                    // slow path. either there is a code unit sequence. think of this: foo\u0001bar
                    // or a simple escaping: \n, \r, \\, \", etc.
                    // in both cases we will consume more than 1 character from the input,
                    // so we have to adjust "i" accordingly

                    // malformed input could throw IndexOutOfBoundsException, but given we control
                    // the test data then we are OK.
                    char nextChar = tag.charAt(i + 1);
                    if (nextChar == 'u') {
                        // code unit sequence
                        char ch;
                        try {
                            ch = (char) Numbers.parseHexInt(tag, i + 2, i + 6);
                        } catch (NumericException e) {
                            throw new AssertionError("cannot parse code sequence in " + tag);
                        }
                        stringSink.put(ch);
                        i += 5;
                    } else if (nextChar == '\\') {
                        stringSink.put('\\');
                        i++;
                    } else if (nextChar == '\"') {
                        stringSink.put('\"');
                        i++;
                    } else if (nextChar == 'b') {
                        // backspace
                        stringSink.put('\b');
                        i++;
                    } else if (nextChar == 'f') {
                        // form-feed
                        stringSink.put('\f');
                        i++;
                    } else if (nextChar == 'n') {
                        // new line
                        stringSink.put('\n');
                        i++;
                    } else if (nextChar == 'r') {
                        // carriage return
                        stringSink.put('\r');
                        i++;
                    } else if (nextChar == 't') {
                        // tab
                        stringSink.put('\t');
                        i++;
                    } else {
                        throw new AssertionError("Unknown escaping sequence at " + tag);
                    }
                }
            }
            return stringSink.toString();
        }

        @Override
        public void close() {
            if (array != null) {
                array.close();
            }
        }

        @Override
        public void onEvent(int code, CharSequence tag, int position) throws JsonException {
            tag = unescape(tag, stringSink);

            if (ignoreCurrentRow && !(code == JsonLexer.EVT_NAME && Chars.equalsIgnoreCase(tag, "testname"))) {
                return;
            }
            switch (code) {
                case JsonLexer.EVT_NAME:
                    if (Chars.equalsIgnoreCase(tag, "testname")) {
                        tag1Type = TAG_TEST_NAME;
                        resetForNextTestCase();
                    } else if (Chars.equalsIgnoreCase(tag, "table")) {
                        tag1Type = TAG_TABLE_NAME;
                    } else if (Chars.equalsIgnoreCase(tag, "symbols")) {
                        tag2Type = TAG_SYMBOLS;
                    } else if (Chars.equalsIgnoreCase(tag, "columns")) {
                        tag2Type = TAG_COLUMNS;
                    } else if (Chars.equalsIgnoreCase(tag, "name")) {
                        if (tag2Type == TAG_SYMBOLS) {
                            tag1Type = TAG_SYMBOL_NAME;
                        } else {
                            tag1Type = TAG_COLUMN_NAME;
                        }
                    } else if (Chars.equalsIgnoreCase(tag, "value")) {
                        if (tag2Type == TAG_SYMBOLS) {
                            tag1Type = TAG_SYMBOL_VALUE;
                        } else {
                            tag1Type = TAG_COLUMN_VALUE;
                        }
                    } else if (Chars.equalsIgnoreCase(tag, "type")) {
                        tag1Type = TAG_COLUMN_TYPE;
                    } else if (Chars.equalsIgnoreCase(tag, "buffer")) {
                        tag2Type = TAG_BUFFER;
                    } else if (Chars.equalsIgnoreCase(tag, "querys")) {
                        tag2Type = TAG_QUERYS;
                    } else if (Chars.equalsIgnoreCase(tag, "status")) {
                        if (tag2Type == TAG_BUFFER) {
                            tag1Type = TAG_BUFFER_STATUS;
                        } else if (tag2Type == TAG_QUERYS) {
                            tag1Type = TAG_QUERY_STATUS;
                        }
                    } else if (Chars.equalsIgnoreCase(tag, "base64Content")) {
                        tag1Type = TAG_BUFFER_EXPECT;
                    } else if (Chars.equalsIgnoreCase(tag, "query")) {
                        tag1Type = TAG_QUERY;
                    } else if (Chars.equalsIgnoreCase(tag, "expected")) {
                        tag1Type = TAG_QUERY_EXPECT;
                    } else if (Chars.equalsIgnoreCase(tag, "error")) {
                        tag1Type = TAG_QUERY_ERROR;
                    } else if (Chars.equalsIgnoreCase(tag, "elemType")) {
                        tag1Type = TAG_ARRAY_ELEM_TYPE;
                    } else if (Chars.equalsIgnoreCase(tag, "rank")) {
                        tag1Type = TAG_ARRAY_RANK;
                    } else if (Chars.equalsIgnoreCase(tag, "shape")) {
                        tag1Type = TAG_ARRAY_SHAPE;
                    } else if (Chars.equalsIgnoreCase(tag, "strides")) {
                        tag1Type = TAG_ARRAY_ELEM_STRIDES;
                    } else if (Chars.equalsIgnoreCase(tag, "data")) {
                        tag1Type = TAG_ARRAY_ELEM_DATA;
                    }
                    break;
                case JsonLexer.EVT_VALUE:
                    switch (tag1Type) {
                        case TAG_TEST_NAME:
                            break;
                        case TAG_TABLE_NAME:
                            try {
                                tableName = Chars.toString(tag);
                                sender.table(tableName);
                            } catch (LineSenderException e) {
                                encounteredError = true;
                            }
                            break;
                        case TAG_SYMBOL_NAME:
                        case TAG_COLUMN_NAME:
                            name = Chars.toString(tag);
                            break;
                        case TAG_SYMBOL_VALUE:
                            if (encounteredError) {
                                break;
                            }
                            try {
                                sender.symbol(name, Chars.toString(tag));
                            } catch (LineSenderException e) {
                                encounteredError = true;
                            }
                            break;
                        case TAG_COLUMN_TYPE:
                            columnType = ColumnType.typeOf(tag);
                            break;
                        case TAG_COLUMN_VALUE:
                            if (encounteredError) {
                                break;
                            }
                            try {
                                switch (columnType) {
                                    case ColumnType.DOUBLE:
                                        try {
                                            sender.doubleColumn(name, Numbers.parseDouble(tag));
                                        } catch (NumericException e) {
                                            throw JsonException.$(position, "bad double");
                                        } catch (LineSenderException e) {
                                            encounteredError = true;
                                        }
                                        break;
                                    case ColumnType.LONG:
                                        try {
                                            sender.longColumn(name, Numbers.parseLong(tag));
                                        } catch (NumericException e) {
                                            throw JsonException.$(position, "bad long");
                                        } catch (LineSenderException e) {
                                            encounteredError = true;
                                        }
                                        break;
                                    case ColumnType.BOOLEAN:
                                        try {
                                            sender.boolColumn(name, SqlKeywords.isTrueKeyword(tag));
                                        } catch (LineSenderException e) {
                                            encounteredError = true;
                                        }
                                        break;
                                    case ColumnType.TIMESTAMP:
                                        try {
                                            sender.timestampColumn(name, Numbers.parseLong(tag), ChronoUnit.NANOS);
                                        } catch (NumericException e) {
                                            throw JsonException.$(position, "bad long");
                                        } catch (LineSenderException e) {
                                            encounteredError = true;
                                        }
                                        break;
                                    case ColumnType.STRING:
                                        try {
                                            sender.stringColumn(name, Chars.toString(tag));
                                        } catch (LineSenderException e) {
                                            encounteredError = true;
                                        }
                                        break;
                                    default:
                                        throw JsonException.$(position, "unexpected state");
                                }
                            } catch (LineSenderException e) {
                                encounteredError = true;
                            }
                            columnType = -1;
                            break;
                        case TAG_BUFFER_STATUS:
                            if (Chars.equals(tag, "SUCCESS")) {
                                Assert.assertFalse(encounteredError);
                            } else if (Chars.equals(tag, "ERROR")) {
                                if (!encounteredError) {
                                    // there was no error recorded yes. let's try to send the line now
                                    // that's the last chance to get an error. if there is no error
                                    // then the test-case must fail
                                    try {
                                        sender.at(100000000000L, ChronoUnit.MICROS);
                                        sender.flush();
                                        Assert.fail("Test case '" + name + "' should have failed, but it passed");
                                    } catch (LineSenderException e) {
                                        // expected
                                    }
                                }
                            } else {
                                throw new AssertionError("unknown status " + tag);
                            }
                            break;
                        case TAG_BUFFER_EXPECT:
                            Assert.assertFalse(encounteredError);
                            sender.at(100000000000L, ChronoUnit.MICROS);
                            assertSuccessfulLine(Base64Helper.parseBase64Binary(tag.toString()));
                            sender.flush();
                            break;
                        case TAG_QUERY_STATUS:
                            queryStatus = Chars.toString(tag);
                            break;
                        case TAG_QUERY:
                            queryText = Chars.toString(tag);
                            break;
                        case TAG_QUERY_EXPECT:
                            queryExpected = Chars.toString(tag);
                            break;
                        case TAG_QUERY_ERROR:
                            queryError = Chars.toString(tag);
                            break;
                        case TAG_ARRAY_ELEM_TYPE:
                            arrayElementType = ColumnType.typeOf(tag);
                            if (arrayElementType != ColumnType.DOUBLE) {
                                throw JsonException.$(position, "only support double array elements");
                            }
                    }
                    break;
                case JsonLexer.EVT_OBJ_END:
                    if (tag2Type == TAG_QUERYS) {
                        assertQuery();
                    }
                    if (tag1Type == TAG_ARRAY_ELEM_DATA && array != null) {
                        if (strides.size() != 0) {
                            sender.cancelRow();
                            ignoreCurrentRow = true;
                            array.close();
                            array = null;
                            break;
                        }
                        try {
                            sender.doubleArray(name, array);
                        } catch (LineSenderException e) {
                            encounteredError = true;
                        }
                        array.close();
                        array = null;
                    }
                    break;
                case JsonLexer.EVT_OBJ_START:
                    if (tag2Type == TAG_QUERYS) {
                        queryError = "";
                        queryExpected = "";
                        queryStatus = "";
                        queryText = "";
                    }
                    break;
                case JsonLexer.EVT_ARRAY_END:
                    if (tag2Type == TAG_QUERYS) {
                        tag2Type = -1;
                    }
                    if (tag1Type == TAG_ARRAY_SHAPE) {
                        int[] shapesArray = new int[shapes.size()];
                        for (int i = 0; i < shapesArray.length; i++) {
                            shapesArray[i] = shapes.get(i);
                        }
                        array = new DoubleArray(shapesArray);
                    }
                    break;
                case JsonLexer.EVT_ARRAY_START:
                    if (tag1Type == TAG_ARRAY_SHAPE) {
                        shapes.clear();
                    }
                    if (tag1Type == TAG_ARRAY_ELEM_STRIDES) {
                        strides.clear();
                    }
                    break;
                case JsonLexer.EVT_ARRAY_VALUE:
                    if (TAG_ARRAY_SHAPE == tag1Type) {
                        try {
                            shapes.add(Numbers.parseInt(tag));
                        } catch (NumericException ignored) {
                        }
                    }
                    if (tag1Type == TAG_ARRAY_ELEM_STRIDES) {
                        try {
                            strides.add(Numbers.parseInt(tag));
                        } catch (NumericException ignored) {
                        }
                    }
                    if (tag1Type == TAG_ARRAY_ELEM_DATA) {
                        try {
                            array.append(Numbers.parseFloat(tag));
                        } catch (NumericException ignored) {
                        }
                    }
            }
        }

        private void assertQuery() {
            serverMain.awaitTxn(tableName, 1);
            if (Chars.equals(queryStatus, "SUCCESS")) {
                serverMain.awaitTable(tableName);
                serverMain.assertSql(queryText, queryExpected);
            } else {
                try {
                    serverMain.getEngine().execute(queryText);
                    Assert.fail("Test case '" + queryText + "' should have failed, but it passed");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getMessage(), queryError);
                }
            }
        }

        private void assertSuccessfulLine(byte[] tag) {
            boolean equal = true;
            DirectByteSlice view = sender.bufferView();
            if (view.size() < tag.length - 1) {
                equal = false;
            } else {
                for (int i = 0, n = tag.length - 1; i < n; i++) {
                    if (tag[i] != view.byteAt(i)) {
                        equal = false;
                        break;
                    }
                }
            }
            if (!equal) {
                byte[] expected = new byte[view.size()];
                for (int i = 0, n = view.size(); i < n; i++) {
                    expected[i] = view.byteAt(i);
                }
                Assert.fail("sender buffer base64[" + Base64.getEncoder().encodeToString(expected) + "]");
            }
        }

        private void resetForNextTestCase() {
            encounteredError = false;
            ignoreCurrentRow = false;
            if (array != null) {
                array.close();
                array = null;
            }
            arrayElementType = -1;
            columnType = -1;
            tag1Type = -1;
            tag2Type = -1;
        }
    }
}
