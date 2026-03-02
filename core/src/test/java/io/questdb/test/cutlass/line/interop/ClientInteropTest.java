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

package io.questdb.test.cutlass.line.interop;

import io.questdb.cairo.ColumnType;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.LineTcpSenderV2;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.json.JsonParser;
import io.questdb.griffin.SqlKeywords;
import io.questdb.std.*;
import io.questdb.std.histogram.org.HdrHistogram.Base64Helper;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.cutlass.line.tcp.ByteChannel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

public class ClientInteropTest {

    @Test
    public void testInterop() throws Exception {
        FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
        String pp = TestUtils.getTestResourcePath("/io/questdb/test/cutlass/line/interop/ilp-client-interop-test.json");

        ByteChannel channel = new ByteChannel();
        try (JsonLexer lexer = new JsonLexer(1024, 1024);
             Path path = new Path().of(pp);
             Sender sender = new LineTcpSenderV2(channel, 1024, 127)) {
            JsonTestSuiteParser parser = new JsonTestSuiteParser(sender, channel);
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

    private static class JsonTestSuiteParser implements JsonParser {

        public static final int TAG_COLUMNS = 8;
        public static final int TAG_COLUMN_NAME = 4;
        public static final int TAG_COLUMN_TYPE = 9;
        public static final int TAG_COLUMN_VALUE = 5;
        public static final int TAG_EXPECTED_RESULT = 6;
        public static final int TAG_LINE = 10;
        public static final int TAG_SYMBOLS = 7;
        public static final int TAG_SYMBOL_NAME = 2;
        public static final int TAG_SYMBOL_VALUE = 3;
        public static final int TAG_TABLE_NAME = 1;
        public static final int TAG_TEST_NAME = 0;
        private final ByteChannel byteChannel;
        private final Sender sender;
        private final StringSink stringSink = new StringSink();
        private int columnType = -1;
        private boolean encounteredError;
        private String name;
        private int tag1Type = -1;
        private int tag2Type = -1;

        public JsonTestSuiteParser(Sender sender, ByteChannel channel) {
            this.sender = sender;
            this.byteChannel = channel;
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
        public void onEvent(int code, CharSequence tag, int position) throws JsonException {
            tag = unescape(tag, stringSink);
            switch (code) {
                case JsonLexer.EVT_NAME:
                    if (Chars.equalsIgnoreCase(tag, "testname")) {
                        tag1Type = TAG_TEST_NAME;
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
                    } else if (Chars.equalsIgnoreCase(tag, "status")) {
                        tag1Type = TAG_EXPECTED_RESULT;
                    } else if (Chars.equalsIgnoreCase(tag, "base64Line")) {
                        tag1Type = TAG_LINE;
                    } else {
                        tag1Type = -1;
                    }
                    break;
                case JsonLexer.EVT_VALUE:
                    switch (tag1Type) {
                        case TAG_TEST_NAME:
                            break;
                        case TAG_TABLE_NAME:
                            try {
                                sender.table(Chars.toString(tag));
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
                        case TAG_EXPECTED_RESULT:
                            if (Chars.equals(tag, "SUCCESS")) {
                                Assert.assertFalse(encounteredError);
                            } else if (Chars.equals(tag, "ERROR")) {
                                if (!encounteredError) {
                                    // there was no error recorded yes. let's try to send the line now
                                    // that's the last chance to get an error. if there is no error
                                    // then the test-case must fail
                                    try {
                                        sender.atNow();
                                        sender.flush();
                                        Assert.fail("Test case '" + name + "' should have failed, but it passed");
                                    } catch (LineSenderException e) {
                                        // expected
                                    }
                                }
                                resetForNextTestCase();
                            } else {
                                throw new AssertionError("unknown status " + tag);
                            }
                            break;
                        case TAG_LINE:
                            Assert.assertFalse(encounteredError);
                            sender.atNow();
                            sender.flush();
                            assertSuccessfulLine(Base64Helper.parseBase64Binary(tag.toString()));
                            resetForNextTestCase();
                            break;
                    }
                    break;
            }
        }

        private void assertSuccessfulLine(byte[] tag) {
            Assert.assertTrue("Produced line does not end with a new line char", byteChannel.endWith((byte) '\n'));
            Assert.assertTrue("buffer base64[" + byteChannel.encodeBase64String() + "]", byteChannel.equals(tag, 0, tag.length - 1));
        }

        private void resetForNextTestCase() {
            encounteredError = false;
            byteChannel.reset();
        }
    }
}
