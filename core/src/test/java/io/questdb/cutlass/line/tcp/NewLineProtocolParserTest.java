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

import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class NewLineProtocolParserTest extends BaseLineTcpContextTest {
    private final static NewLineProtoParser protoParser = new NewLineProtoParser();

    @Test
    public void testGetValueType() throws Exception {
        assertType(NewLineProtoParser.ENTITY_TYPE_NULL, "");

        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "null");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "NULL");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "NulL");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "skull");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "skulL");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "1.6x");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "aa\"aa");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "tre");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "''");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "oX");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "0x");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "a");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "i");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "aflse");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "aTTTT");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "aFFF");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "e");

        assertType(NewLineProtoParser.ENTITY_TYPE_BOOLEAN, "t");
        assertType(NewLineProtoParser.ENTITY_TYPE_BOOLEAN, "T");
        assertType(NewLineProtoParser.ENTITY_TYPE_BOOLEAN, "f");
        assertType(NewLineProtoParser.ENTITY_TYPE_BOOLEAN, "F");
        assertType(NewLineProtoParser.ENTITY_TYPE_BOOLEAN, "true");
        assertType(NewLineProtoParser.ENTITY_TYPE_BOOLEAN, "false");
        assertType(NewLineProtoParser.ENTITY_TYPE_BOOLEAN, "FalSe");
        assertType(NewLineProtoParser.ENTITY_TYPE_BOOLEAN, "tRuE");

        assertType(NewLineProtoParser.ENTITY_TYPE_STRING, "\"0x123a4\"");
        assertType(NewLineProtoParser.ENTITY_TYPE_STRING, "\"0x123a4 looks \\\" like=long256,\\\n but tis not!\"");
        assertType(NewLineProtoParser.ENTITY_TYPE_STRING, "\"0x123a4 looks like=long256, but tis not!\"");
        assertType(NewLineProtoParser.ENTITY_TYPE_NONE, "\"0x123a4 looks \\\" like=long256,\\\n but tis not!",
                NewLineProtoParser.ParseResult.ERROR); // missing closing '"'
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "0x123a4 looks \\\" like=long256,\\\n but tis not!\"",
                NewLineProtoParser.ParseResult.ERROR); // wanted to be a string, missing opening '"'

        assertType(NewLineProtoParser.ENTITY_TYPE_LONG256, "0x123i");
        assertType(NewLineProtoParser.ENTITY_TYPE_LONG256, "0x1i");

        assertType(NewLineProtoParser.ENTITY_TYPE_INTEGER, "123i");
        assertType(NewLineProtoParser.ENTITY_TYPE_INTEGER, "1i");

        assertType(NewLineProtoParser.ENTITY_TYPE_FLOAT, "1.45");
        assertType(NewLineProtoParser.ENTITY_TYPE_FLOAT, "1e-13");
        assertType(NewLineProtoParser.ENTITY_TYPE_FLOAT, "1.0");
        assertType(NewLineProtoParser.ENTITY_TYPE_FLOAT, "1");

        assertType(NewLineProtoParser.ENTITY_TYPE_NONE, "aaa\"", NewLineProtoParser.ParseResult.ERROR);
        assertType(NewLineProtoParser.ENTITY_TYPE_NONE, "\"aaa", NewLineProtoParser.ParseResult.ERROR);

        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "123a4i");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "oxi");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "xi");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "oXi");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "0xi");

        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "123a4");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "ox1");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "0x1");
        assertType(NewLineProtoParser.ENTITY_TYPE_SYMBOL, "0x123a4");

        // in this edge case, type is guessed as best as possible, later the parser would fail, its a feature
        assertType(NewLineProtoParser.ENTITY_TYPE_LONG256, "0x123a4i");
    }

    private static void assertType(int type, String value) throws Exception {
        assertType(type, value, NewLineProtoParser.ParseResult.MEASUREMENT_COMPLETE);
    }

    private static void assertType(int type,
                                   String value,
                                   NewLineProtoParser.ParseResult expectedParseResult) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            sink.clear();
            sink.put("t v=").put(value).put('\n');
            byte[] bytes = sink.toString().getBytes(StandardCharsets.UTF_8);
            final int len = bytes.length;
            long mem = Unsafe.malloc(bytes.length);
            try {
                for (int i = 0; i < bytes.length; i++) {
                    Unsafe.getUnsafe().putByte(mem + i, bytes[i]);
                }
                protoParser.of(mem);
                Assert.assertEquals(expectedParseResult, protoParser.parseMeasurement(mem + len));
                NewLineProtoParser.ProtoEntity entity = protoParser.getEntity(0);
                Assert.assertEquals(type, entity.getType());
                Assert.assertEquals("v", entity.getName().toString());
                if (expectedParseResult == NewLineProtoParser.ParseResult.MEASUREMENT_COMPLETE) {
                    switch (type) {
                        case NewLineProtoParser.ENTITY_TYPE_STRING:
                            Assert.assertEquals(value, "\"" + entity.getValue().toString() + "\"");
                            break;
                        case NewLineProtoParser.ENTITY_TYPE_INTEGER:
                        case NewLineProtoParser.ENTITY_TYPE_LONG256:
                            Assert.assertEquals(value, entity.getValue().toString() + "i");
                            break;
                        default:
                            Assert.assertEquals(value, entity.getValue().toString());
                    }
                }
            } finally {
                Unsafe.free(mem, bytes.length);
            }
        });
    }
}
