/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cutlass.text;

import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.text.DefaultTextConfiguration;
import io.questdb.cutlass.text.TextMetadataParser;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TextMetadataParserTest {
    private static final JsonLexer LEXER = new JsonLexer(1024, 4096);
    private static TextMetadataParser textMetadataParser;
    private static TypeManager typeManager;
    private static DirectUtf16Sink utf16Sink;

    @BeforeClass
    public static void setUpClass() {
        utf16Sink = new DirectUtf16Sink(1024);
        DirectUtf8Sink utf8Sink = new DirectUtf8Sink(1024);
        typeManager = new TypeManager(
                new DefaultTextConfiguration(),
                utf16Sink,
                utf8Sink
        );
        textMetadataParser = new TextMetadataParser(
                new DefaultTextConfiguration(),
                typeManager
        );
    }

    @AfterClass
    public static void tearDown() {
        LEXER.close();
        utf16Sink.close();
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
            LEXER.parse(buf, buf + in.length(), textMetadataParser);
            Assert.assertEquals(2, textMetadataParser.getColumnTypes().size());
            Assert.assertEquals(2, textMetadataParser.getColumnNames().size());
            Assert.assertEquals("[INT,DATE]", textMetadataParser.getColumnTypes().toString());
            Assert.assertEquals("[x,y]", textMetadataParser.getColumnNames().toString());
        } finally {
            Unsafe.free(buf, in.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEmptyList() throws Exception {
        String in = "[]";

        long buf = TestUtils.toMemory(in);
        try {
            LEXER.parse(buf, buf + in.length(), textMetadataParser);
            Assert.assertEquals(0, textMetadataParser.getColumnTypes().size());
            Assert.assertEquals(0, textMetadataParser.getColumnNames().size());
        } finally {
            Unsafe.free(buf, in.length(), MemoryTag.NATIVE_DEFAULT);
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
    public void testTimestampNanosType() throws Exception {
        String in = "[\n" +
                "{\"name\": \"x\", \"type\": \"INT\", \"pattern\":\"xyz\", \"locale\": \"en-US\"},\n" +
                "{\"name\": \"y\", \"type\": \"TIMESTAMP_NS\", \"pattern\":\"yyyy-MM-ddTHH:mm:ss.SSSSSSNNNZ\"}\n" +
                "]";

        long buf = TestUtils.toMemory(in);
        try {
            LEXER.parse(buf, buf + in.length(), textMetadataParser);
            Assert.assertEquals(2, textMetadataParser.getColumnTypes().size());
            Assert.assertEquals(2, textMetadataParser.getColumnNames().size());
            Assert.assertEquals("[INT,TIMESTAMP_NS]", textMetadataParser.getColumnTypes().toString());
            Assert.assertEquals("[x,y]", textMetadataParser.getColumnNames().toString());
        } finally {
            Unsafe.free(buf, in.length(), MemoryTag.NATIVE_DEFAULT);
        }
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
            LEXER.parse(buf, buf + schema.length(), textMetadataParser);
            Assert.fail();
        } catch (JsonException e) {
            Assert.assertEquals(position, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), message);
        } finally {
            Unsafe.free(buf, schema.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }
}