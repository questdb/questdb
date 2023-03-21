/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.json.JsonParser;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.LineTcpSender;
import io.questdb.test.cutlass.line.tcp.StringChannel;
import io.questdb.griffin.SqlKeywords;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;

public class TestInterop {

    @Test
    public void testInterop() throws Exception {
        FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
        URL testCasesUrl = TestInterop.class.getResource("/io/questdb/test/cutlass/line/interop/ilp-client-interop-test.json");
        Assert.assertNotNull("interop test cases missing", testCasesUrl);
        String pp = testCasesUrl.getFile();
        if (Os.isWindows()) {
            // on Windows Java returns "/C:/dir/file". This leading slash is Java specific and doesn't bode well
            // with OS file open methods.
            pp = pp.substring(1);
        }

        StringChannel channel = new StringChannel();
        try (JsonLexer lexer = new JsonLexer(1024, 1024);
             Path path = new Path().of(pp).$();
             Sender sender = new LineTcpSender(channel, 1024)) {
            JsonTestSuiteParser parser = new JsonTestSuiteParser(sender, channel);
            int fd = ff.openRO(path);
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
        private final Sender sender;
        private final StringChannel stringChannel;
        private final StringSink stringSink = new StringSink();
        private int columnType = -1;
        private boolean encounteredError;
        private String name;
        private int tag1Type = -1;
        private int tag2Type = -1;

        public JsonTestSuiteParser(Sender sender, StringChannel channel) {
            this.sender = sender;
            this.stringChannel = channel;
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
                    } else if (Chars.equalsIgnoreCase(tag, "line")) {
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
                                            sender.timestampColumn(name, Numbers.parseLong(tag));
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
                            assertSuccessfulLine(tag);
                            resetForNextTestCase();
                            break;
                    }
                    break;
            }
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

        private void assertSuccessfulLine(CharSequence tag) {
            String s = stringChannel.toString();
            Assert.assertTrue("Produced line does not end with a new line char", s.endsWith("\n"));
            s = s.substring(0, s.length() - 1);
            Assert.assertTrue(Chars.equals(tag, s));
        }

        private void resetForNextTestCase() {
            encounteredError = false;
            stringChannel.reset();
        }
    }
}
