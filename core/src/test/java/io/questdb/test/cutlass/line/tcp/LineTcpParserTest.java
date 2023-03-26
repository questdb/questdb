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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.cutlass.line.tcp.LineTcpParser;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LineTcpParserTest extends BaseLineTcpContextTest {

    @Test
    public void testGetValueType() throws Exception {
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "null");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "NULL");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "NulL");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "skull");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "skulL");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "1.6x");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "aa\"aa");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "tre");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "''");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "oX");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "0x");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "a");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "i");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "aflse");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "aTTTT");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "aFFF");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "e");

        assertError(LineTcpParser.ENTITY_TYPE_TAG, "\"errt\"");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "\"errt\"", true, false);
        assertError(LineTcpParser.ENTITY_TYPE_SYMBOL, "errt");
        assertType(LineTcpParser.ENTITY_TYPE_SYMBOL, "errt", false, true);

        assertType(LineTcpParser.ENTITY_TYPE_BOOLEAN, "t");
        assertType(LineTcpParser.ENTITY_TYPE_BOOLEAN, "T");
        assertType(LineTcpParser.ENTITY_TYPE_BOOLEAN, "f");
        assertType(LineTcpParser.ENTITY_TYPE_BOOLEAN, "F");
        assertType(LineTcpParser.ENTITY_TYPE_BOOLEAN, "true");
        assertType(LineTcpParser.ENTITY_TYPE_BOOLEAN, "false");
        assertType(LineTcpParser.ENTITY_TYPE_BOOLEAN, "FalSe");
        assertType(LineTcpParser.ENTITY_TYPE_BOOLEAN, "tRuE");

        assertType(LineTcpParser.ENTITY_TYPE_STRING, "\"0x123a4\"");
        assertType(LineTcpParser.ENTITY_TYPE_STRING, "\"0x123a4 looks \\\" like=long256,\\\n but tis not!\"", "\"0x123a4 looks \" like=long256,\n but tis not!\"", LineTcpParser.ParseResult.MEASUREMENT_COMPLETE, false, false);
        assertType(LineTcpParser.ENTITY_TYPE_STRING, "\"0x123a4 looks like=long256, but tis not!\"");
        assertError(LineTcpParser.ENTITY_TYPE_NONE, "\"0x123a4 looks \\\" like=long256,\\\n but tis not!"); // missing closing '"'
        assertError(LineTcpParser.ENTITY_TYPE_TAG, "0x123a4 looks \\\" like=long256,\\\n but tis not!\""); // wanted to be a string, missing opening '"'

        assertType(LineTcpParser.ENTITY_TYPE_LONG256, "0x123i");
        assertType(LineTcpParser.ENTITY_TYPE_LONG256, "0x1i");

        assertType(LineTcpParser.ENTITY_TYPE_INTEGER, "123i");
        assertType(LineTcpParser.ENTITY_TYPE_INTEGER, "1i");

        assertType(LineTcpParser.ENTITY_TYPE_FLOAT, "1.45");
        assertType(LineTcpParser.ENTITY_TYPE_FLOAT, "1e-13");
        assertType(LineTcpParser.ENTITY_TYPE_FLOAT, "1.0");
        assertType(LineTcpParser.ENTITY_TYPE_FLOAT, "1");

        assertError(LineTcpParser.ENTITY_TYPE_NONE, "\"aaa");

        assertType(LineTcpParser.ENTITY_TYPE_TAG, "123a4i");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "oxi");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "xi");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "oXi");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "0xi");

        assertType(LineTcpParser.ENTITY_TYPE_TAG, "123a4");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "ox1");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "0x1");
        assertType(LineTcpParser.ENTITY_TYPE_TAG, "0x123a4");

        // in this edge case, type is guessed as best as possible, later the parser would fail, its a feature
        assertType(LineTcpParser.ENTITY_TYPE_LONG256, "0x123a4i");

        assertType(LineTcpParser.ENTITY_TYPE_INTEGER, "-9223372036854775808i");
        assertType(LineTcpParser.ENTITY_TYPE_INTEGER, "9223372036854775807i");
    }

    private static void assertError(int type, String value) throws Exception {
        assertType(type, value, value, LineTcpParser.ParseResult.ERROR, false, false);
    }

    private static void assertType(int type, String value) throws Exception {
        assertType(type, value, value, LineTcpParser.ParseResult.MEASUREMENT_COMPLETE, false, false);
    }

    private static void assertType(int type, String value, boolean stringAsTagSupported, boolean symbolAsFieldSupported) throws Exception {
        assertType(type, value, value, LineTcpParser.ParseResult.MEASUREMENT_COMPLETE, stringAsTagSupported, symbolAsFieldSupported);
    }

    private static void assertType(int type,
                                   String value,
                                   String expectedValue,
                                   LineTcpParser.ParseResult expectedParseResult,
                                   boolean stringAsTagSupported,
                                   boolean symbolAsFieldSupported) throws Exception {
        final LineTcpParser lineTcpParser = new LineTcpParser(stringAsTagSupported, symbolAsFieldSupported);
        TestUtils.assertMemoryLeak(() -> {
            sink.clear();
            sink.put(type == LineTcpParser.ENTITY_TYPE_TAG ? "t,v=" : "t v=").put(value).put('\n'); // SYMBOLS are in tag set, not field set
            byte[] bytes = sink.toString().getBytes(Files.UTF_8);
            final int len = bytes.length;
            long mem = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < bytes.length; i++) {
                    Unsafe.getUnsafe().putByte(mem + i, bytes[i]);
                }
                lineTcpParser.of(mem);
                Assert.assertEquals(expectedParseResult, lineTcpParser.parseMeasurement(mem + len));
                LineTcpParser.ProtoEntity entity = lineTcpParser.getEntity(0);
                Assert.assertEquals(type, entity.getType());
                Assert.assertEquals("v", entity.getName().toString());
                if (expectedParseResult == LineTcpParser.ParseResult.MEASUREMENT_COMPLETE) {
                    switch (type) {
                        case LineTcpParser.ENTITY_TYPE_STRING:
                            Assert.assertEquals(expectedValue, "\"" + entity.getValue().toString() + "\"");
                            break;
                        case LineTcpParser.ENTITY_TYPE_INTEGER:
                        case LineTcpParser.ENTITY_TYPE_LONG256:
                            Assert.assertEquals(expectedValue, entity.getValue().toString() + "i");
                            break;
                        default:
                            Assert.assertEquals(expectedValue, entity.getValue().toString());
                    }
                }
            } finally {
                Unsafe.free(mem, bytes.length, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }
}
