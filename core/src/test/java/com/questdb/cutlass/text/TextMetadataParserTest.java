/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cutlass.text;

import com.questdb.cutlass.json.JsonException;
import com.questdb.cutlass.json.JsonLexer;
import com.questdb.cutlass.text.types.TypeManager;
import com.questdb.std.Unsafe;
import com.questdb.std.str.DirectCharSink;
import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateLocaleFactory;
import com.questdb.test.tools.TestUtils;
import org.junit.*;

public class TextMetadataParserTest {
    private static final JsonLexer LEXER = new JsonLexer(1024, 4096);
    private static TextMetadataParser textMetadataParser;
    private static TypeManager typeManager;
    private static DirectCharSink utf8Sink;
    private static JsonLexer jsonLexer;

    @BeforeClass
    public static void setUpClass() throws JsonException {
        utf8Sink = new DirectCharSink(1024);
        jsonLexer = new JsonLexer(1024, 1024);
        typeManager = new TypeManager(new DefaultTextConfiguration(), utf8Sink, jsonLexer);
        textMetadataParser = new TextMetadataParser(
                new DefaultTextConfiguration(),
                DateLocaleFactory.INSTANCE,
                new DateFormatFactory(),
                com.questdb.std.microtime.DateLocaleFactory.INSTANCE,
                new com.questdb.std.microtime.DateFormatFactory(),
                typeManager
        );
    }

    @AfterClass
    public static void tearDown() {
        LEXER.close();
        jsonLexer.close();
        utf8Sink.close();
        textMetadataParser.close();
    }

    @Before
    public void setUp() {
        LEXER.clear();
        textMetadataParser.clear();
        typeManager.clear();
    }

    @Test
    public void testArrayProperty() {
        assertFailure(
                "[\n" +
                        "{\"name\": \"x\", \"type\": \"DOUBLE\", \"pattern\":\"xyz\", \"locale\": []}\n" +
                        "]",
                62,
                "Unexpected array"
        );
    }

    @Test
    public void testCorrectSchema() throws Exception {
        String in = "[\n" +
                "{\"name\": \"x\", \"type\": \"INT\", \"pattern\":\"xyz\", \"locale\": \"en-US\"},\n" +
                "{\"name\": \"y\", \"type\": \"DATE\", \"pattern\":\"xyz\"}\n" +
                "]";

        long buf = TestUtils.toMemory(in);
        try {
            LEXER.parse(buf, in.length(), textMetadataParser);
            Assert.assertEquals(2, textMetadataParser.getColumnTypes().size());
            Assert.assertEquals(2, textMetadataParser.getColumnNames().size());
            Assert.assertEquals("[INT,DATE]", textMetadataParser.getColumnTypes().toString());
            Assert.assertEquals("[x,y]", textMetadataParser.getColumnNames().toString());
        } finally {
            Unsafe.free(buf, in.length());
        }
    }

    @Test
    public void testEmptyList() throws Exception {
        String in = "[]";

        long buf = TestUtils.toMemory(in);
        try {
            LEXER.parse(buf, in.length(), textMetadataParser);
            Assert.assertEquals(0, textMetadataParser.getColumnTypes().size());
            Assert.assertEquals(0, textMetadataParser.getColumnNames().size());
        } finally {
            Unsafe.free(buf, in.length());
        }
    }

    @Test
    public void testEmptyObject() {
        assertFailure("[{}]", 3, "Missing 'name' property");
    }

    @Test
    public void testMissingName() {
        String in = "[\n" +
                "{\"name\": \"x\", \"type\": \"INT\", \"pattern\":\"xyz\", \"locale\": \"en-GB\"},\n" +
                "{\"type\": \"DOUBLE\", \"pattern\":\"xyz\"}\n" +
                "]";

        assertFailure(in, 103, "Missing 'name' property");
    }

    @Test
    public void testMissingType() {
        String in = "[\n" +
                "{\"name\": \"x\", \"pattern\":\"xyz\", \"locale\": \"en-GB\"},\n" +
                "{\"name\": \"y\", \"type\": \"DOUBLE\", \"pattern\":\"xyz\"}\n" +
                "]";
        assertFailure(in, 51, "Missing 'type' property");
    }

    @Test
    public void testNonArray() {
        assertFailure("{}", 1, "Unexpected object");
    }

    @Test
    public void testNonObjectArrayMember() {
        assertFailure(
                "[2,\n" +
                        "{\"name\": \"x\", \"type\": \"DOUBLE\", \"pattern\":\"xyz\"}\n" +
                        "]",
                2,
                "Must be an object"
        );
    }

    @Test
    public void testWrongDateLocale() {
        assertFailure(
                "[\n" +
                        "{\"name\": \"x\", \"type\": \"DATE\", \"pattern\":\"xyz\", \"locale\": \"enk\"}\n" +
                        "]",
                61,
                "Invalid date locale"
        );
    }

    @Test
    public void testWrongTimestampLocale() {
        assertFailure(
                "[\n" +
                        "{\"name\": \"x\", \"type\": \"TIMESTAMP\", \"pattern\":\"xyz\", \"locale\": \"enk\"}\n" +
                        "]",
                66,
                "Invalid timestamp locale"
        );
    }

    @Test
    public void testWrongType() {
        assertFailure(
                "[\n" +
                        "{\"name\": \"y\", \"type\": \"ABC\", \"pattern\":\"xyz\"}\n" +
                        "]",
                26,
                "Invalid type"
        );
    }

    private void assertFailure(CharSequence schema, int position, CharSequence message) {
        long buf = TestUtils.toMemory(schema);
        try {
            LEXER.parse(buf, schema.length(), textMetadataParser);
            Assert.fail();
        } catch (JsonException e) {
            Assert.assertEquals(position, e.getPosition());
            TestUtils.assertContains(e.getMessage(), message);
        } finally {
            Unsafe.free(buf, schema.length());
        }
    }
}