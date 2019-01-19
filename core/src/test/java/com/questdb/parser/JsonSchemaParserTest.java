/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.parser;

import com.questdb.BootstrapEnv;
import com.questdb.cutlass.json.JsonException;
import com.questdb.cutlass.json.JsonLexer;
import com.questdb.std.ObjList;
import com.questdb.std.Unsafe;
import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateLocaleFactory;
import com.questdb.std.time.TimeZoneRuleFactory;
import com.questdb.test.tools.TestUtils;
import org.junit.*;

public class JsonSchemaParserTest {
    private static final JsonLexer LEXER = new JsonLexer(1024, 4096);
    private static JsonSchemaParser jsonSchemaParser;
    private static String defaultLocaleId;

    @BeforeClass
    public static void setUpClass() {
        BootstrapEnv env = new BootstrapEnv();
        env.dateFormatFactory = new DateFormatFactory();
        env.dateLocaleFactory = new DateLocaleFactory(new TimeZoneRuleFactory());
        defaultLocaleId = env.dateLocaleFactory.getDefaultDateLocale().getId();
        jsonSchemaParser = new JsonSchemaParser(env);
    }

    @AfterClass
    public static void tearDown() {
        LEXER.close();
    }

    @Before
    public void setUp() {
        LEXER.clear();
        jsonSchemaParser.clear();
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

        ObjList<ImportedColumnMetadata> metadata = parseMetadata(in);
        Assert.assertEquals(2, metadata.size());
        Assert.assertEquals("ImportedColumnMetadata{importedColumnType=INT, pattern=xyz, dateLocale=en-US, name=x}", metadata.get(0).toString());
        Assert.assertEquals("ImportedColumnMetadata{importedColumnType=DATE, pattern=xyz, dateLocale=" + defaultLocaleId + ", name=y}", metadata.get(1).toString());
    }

    @Test
    public void testEmptyList() throws Exception {
        Assert.assertEquals(0, parseMetadata("[]").size());
    }

    @Test
    public void testEmptyObject() {
        try {
            parseMetadata("[{}]");
            Assert.fail();
        } catch (JsonException e) {
            Assert.assertEquals(3, e.getPosition());
        }
    }

    @Test
    public void testMissingName() {
        assertFailure(
                "[\n" +
                        "{\"name\": \"x\", \"type\": \"INT\", \"pattern\":\"xyz\", \"locale\": \"en-GB\"},\n" +
                        "{\"type\": \"DOUBLE\", \"pattern\":\"xyz\"}\n" +
                        "]",
                103,
                "Missing 'name' property"
        );
    }

    @Test
    public void testMissingType() {
        assertFailure(
                "[\n" +
                        "{\"name\": \"x\", \"pattern\":\"xyz\", \"locale\": \"en-GB\"},\n" +
                        "{\"name\": \"y\", \"type\": \"DOUBLE\", \"pattern\":\"xyz\"}\n" +
                        "]",
                51,
                "Missing 'type' property"
        );
    }

    @Test
    public void testNonArray() {
        assertFailure(
                "{}",
                1,
                "Unexpected object"
        );
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
                        "{\"name\": \"x\", \"type\": \"DOUBLE\", \"pattern\":\"xyz\", \"locale\": \"enk\"}\n" +
                        "]",
                63,
                "Invalid date locale"
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

    private void assertFailure(String in, int position, CharSequence text) {
        try {
            parseMetadata(in);
            Assert.fail();
        } catch (JsonException e) {
            TestUtils.assertEquals(text, e.getFlyweightMessage());
            Assert.assertEquals(position, e.getPosition());
        }
    }

    private ObjList<ImportedColumnMetadata> parseMetadata(CharSequence in) throws JsonException {
        long buf = TestUtils.toMemory(in);
        try {
            LEXER.parse(buf, in.length(), jsonSchemaParser);
            return jsonSchemaParser.getMetadata();
        } finally {
            Unsafe.free(buf, in.length());
        }
    }
}