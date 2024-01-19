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

package io.questdb.test.cutlass.text.schema2;

import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.text.DefaultTextConfiguration;
import io.questdb.cutlass.text.schema2.SchemaV2;
import io.questdb.cutlass.text.schema2.SchemaV2Parser;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

public class SchemaV2ParserTest {
    private static final JsonLexer LEXER = new JsonLexer(1024, 4096);
    private static final StringSink sink = new StringSink();
    private static SchemaV2 schema;
    private static SchemaV2Parser schemaV2Parser;
    private static TypeManager typeManager;
    private static DirectUtf16Sink utf8Sink;

    @BeforeClass
    public static void setUpClass() {
        utf8Sink = new DirectUtf16Sink(1024);
        typeManager = new TypeManager(
                new DefaultTextConfiguration(),
                utf8Sink
        );
        schemaV2Parser = new SchemaV2Parser(
                new DefaultTextConfiguration(),
                typeManager
        );

        schema = new SchemaV2(new DefaultTextConfiguration());
        schemaV2Parser.withSchema(schema);
    }

    @AfterClass
    public static void tearDown() {
        LEXER.close();
        utf8Sink.close();
        schemaV2Parser.close();
    }

    @Before
    public void setUp() {
        LEXER.clear();
        schemaV2Parser.clear();
        typeManager.clear();
        schema.clear();
        sink.clear();
    }

    @Test
    public void testCorrectSchema() throws Exception {
        assertJson(
                "{\n" +
                        "  \"columns\": [\n" +
                        "    {\n" +
                        "      \"file_column_name\": \"x\",\n" +
                        "      \"file_column_index\": 0,\n" +
                        "      \"column_type\": \"TIMESTAMP\",\n" +
                        "      \"table_column_name\": \"x\",\n" +
                        "      \"formats\": [\n" +
                        "        {\n" +
                        "          \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "          \"locale\": null,\n" +
                        "          \"utf8\": false\n" +
                        "        }\n" +
                        "      ]\n" +
                        "    }\n" +
                        "  ],\n" +
                        "  \"formats\": {\n" +
                        "    \"TIMESTAMP\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": null,\n" +
                        "        \"utf8\": false\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": null,\n" +
                        "        \"utf8\": true\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"formats_action\": \"ADD\"\n" +
                        "}",
                "{\"columns\":[{\"file_column_name\":\"x\",\"file_column_index\":0,\"file_column_ignore\":false,\"column_type\":\"TIMESTAMP\",\"table_column_name\":\"x\",\"formats\":[{\"pattern\":\"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\"locale\":\"en\",\"utf8\":false}]}],\"formats\": {\"TIMESTAMP\": [{\"pattern\":\"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\"locale\":\"en\",\"utf8\":false},{\"pattern\":\"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\"locale\":\"en\",\"utf8\":true}]},\"formats_action\":\"ADD\"}"
        );
    }

    @Test
    public void testEmpty() throws JsonException {
        assertJson("{}", "{\"columns\":[],\"formats\": {},\"formats_action\":\"ADD\"}");
    }

    @Test
    public void testEmptyColumnsMultipleGlobalTypes() throws Exception {
        assertJson(
                "{\n" +
                        "  \"columns\": [],\n" +
                        "  \"formats\": {\n" +
                        "    \"TIMESTAMP\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": null,\n" +
                        "        \"utf8\": false\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"DATE\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"dd/MM/y\",\n" +
                        "        \"locale\": \"ja\",\n" +
                        "        \"utf8\": true\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"formats_action\": \"ADD\"\n" +
                        "}",
                "{\"columns\":[],\"formats\": {\"DATE\": [{\"pattern\":\"dd/MM/y\",\"locale\":\"ja\",\"utf8\":true}],\"TIMESTAMP\": [{\"pattern\":\"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\"locale\":\"en\",\"utf8\":false}]},\"formats_action\":\"ADD\"}"
        );
    }

    @Test
    public void testEmptyList() {
        assertException("[]", 1, "object expected");
    }

    @Test
    public void testFailBadColumnTimestampPattern() {
        assertException(
                "{\n" +
                        "  \"columns\": [\n" +
                        "    {\n" +
                        "      \"file_column_name\": \"x\",\n" +
                        "      \"file_column_index\": 0,\n" +
                        "      \"column_type\": \"TIMESTAMP\",\n" +
                        "      \"table_column_name\": \"x\",\n" +
                        "      \"formats\": [\n" +
                        "        {\n" +
                        "          \"pattern\": null,\n" +
                        "          \"locale\": null,\n" +
                        "          \"utf8\": false\n" +
                        "        }\n" +
                        "      ]\n" +
                        "    }\n" +
                        "  ],\n" +
                        "  \"formats\": {\n" +
                        "    \"DATE\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"dd/MM/y\",\n" +
                        "        \"locale\": \"ja\",\n" +
                        "        \"utf8\": true\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"TIMESTAMP\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": \"en\",\n" +
                        "        \"utf8\": false\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"formats_action\": \"Add\"\n" +
                        "}",
                201,
                "TIMESTAMP format pattern is required"
        );
    }

    @Test
    public void testFailColumnsAsObject() {
        assertException(
                "{\n" +
                        "  \"columns\": {},\n" +
                        "  \"formats\": {\n" +
                        "    \"DATE\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"dd/MM/y\",\n" +
                        "        \"locale\": \"ja\",\n" +
                        "        \"utf8\": true\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"TIMESTAMP\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": \"en\",\n" +
                        "        \"utf8\": false\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"formats_action\": \"Add\"\n" +
                        "}",
                16,
                "array expected"
        );
    }

    @Test
    public void testFailColumnsAsScalar() {
        assertException(
                "{\n" +
                        "  \"columns\": \"hello\",\n" +
                        "  \"formats\": {\n" +
                        "    \"DATE\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"dd/MM/y\",\n" +
                        "        \"locale\": \"ja\",\n" +
                        "        \"utf8\": true\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"TIMESTAMP\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": \"en\",\n" +
                        "        \"utf8\": false\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"formats_action\": \"Add\"\n" +
                        "}",
                17,
                "array expected"
        );
    }

    @Test
    public void testFailDuplicateColumnTypesInFormatsSection() {
        assertException(
                "{\n" +
                        "  \"columns\": [\n" +
                        "    {\n" +
                        "      \"file_column_name\": \"x\",\n" +
                        "      \"file_column_index\": 0,\n" +
                        "      \"column_type\": \"TIMESTAMP\",\n" +
                        "      \"table_column_name\": \"x\",\n" +
                        "      \"formats\": [\n" +
                        "        {\n" +
                        "          \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "          \"locale\": null,\n" +
                        "          \"utf8\": false\n" +
                        "        }\n" +
                        "      ]\n" +
                        "    }\n" +
                        "  ],\n" +
                        "  \"formats\": {\n" +
                        "    \"DATE\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"dd/MM/y\",\n" +
                        "        \"locale\": \"ja\",\n" +
                        "        \"utf8\": true\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"TIMESTAMP\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": \"en\",\n" +
                        "        \"utf8\": false\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"TIMESTAMP\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": \"en\",\n" +
                        "        \"utf8\": false\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"formats_action\": \"Add\"\n" +
                        "}",
                585,
                "duplicate formats column type [tag=TIMESTAMP]"
        );
    }

    @Test
    public void testFailFormatsAsArray() {
        assertException(
                "{\n" +
                        "  \"formats\": [],\n" +
                        "  \"formats_action\": \"Add\"\n" +
                        "}",
                16,
                "object expected"
        );
    }

    @Test
    public void testFailFormatsAsScalar() {
        assertException(
                "{\n" +
                        "  \"formats\": \"ok\",\n" +
                        "  \"formats_action\": \"Add\"\n" +
                        "}",
                17,
                "object expected"
        );
    }

    @Test
    public void testFailFormatsInvalidLocale() {
        assertException(
                "{\n" +
                        "  \"formats\": {\n" +
                        "    \"timestamp\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": \"where?\",\n" +
                        "        \"utf8\": false\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"formats_action\": \"Add\"\n" +
                        "}",
                116,
                "Invalid timestamp locale [tag=where?]"
        );
    }

    @Test
    public void testFailFormatsInvalidType() {
        assertException(
                "{\n" +
                        "  \"formats\": {\n" +
                        "    \"OK\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": \"en\",\n" +
                        "        \"utf8\": false\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"formats_action\": \"Add\"\n" +
                        "}",
                23,
                "Invalid column type [tag=OK]"
        );
    }

    @Test
    public void testFailFormatsInvalidUtf8Flag() {
        assertException(
                "{\n" +
                        "  \"formats\": {\n" +
                        "    \"timestamp\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": \"ja\",\n" +
                        "        \"utf8\": 3\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"formats_action\": \"Add\"\n" +
                        "}",
                137,
                "boolean value expected [tag=3]"
        );
    }

    @Test
    public void testFailFormatsTypeAsObject() {
        assertException(
                "{\n" +
                        "  \"formats\": {\n" +
                        "    \"DATE\": {},\n" +
                        "    \"TIMESTAMP\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": \"en\",\n" +
                        "        \"utf8\": false\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"formats_action\": \"Add\"\n" +
                        "}",
                30,
                "array expected"
        );
    }

    @Test
    public void testFailFormatsTypeAsScalar() {
        assertException(
                "{\n" +
                        "  \"formats\": {\n" +
                        "    \"DATE\": -1,\n" +
                        "    \"TIMESTAMP\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": \"en\",\n" +
                        "        \"utf8\": false\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"formats_action\": \"Add\"\n" +
                        "}",
                30,
                "array expected"
        );
    }

    @Test
    public void testFailFormatsUtf8AsArray() {
        assertException(
                "{\n" +
                        "  \"formats\": {\n" +
                        "    \"timestamp\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": \"ja\",\n" +
                        "        \"utf8\": []\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"formats_action\": \"Add\"\n" +
                        "}",
                137,
                "scalar value expected"
        );
    }

    @Test
    public void testFailMultipleColumnsSections() {
        assertException(
                "{\n" +
                        "  \"columns\": [\n" +
                        "    {\n" +
                        "      \"file_column_name\": \"x\",\n" +
                        "      \"file_column_index\": 0,\n" +
                        "      \"column_type\": \"TIMESTAMP\",\n" +
                        "      \"table_column_name\": \"x\",\n" +
                        "      \"formats\": [\n" +
                        "        {\n" +
                        "          \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "          \"locale\": null,\n" +
                        "          \"utf8\": false\n" +
                        "        }\n" +
                        "      ]\n" +
                        "    }\n" +
                        "  ],\n" +
                        "  \"columns\": [],\n" +
                        "  \"formats\": {\n" +
                        "    \"DATE\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"dd/MM/y\",\n" +
                        "        \"locale\": \"ja\",\n" +
                        "        \"utf8\": true\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"TIMESTAMP\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": \"en\",\n" +
                        "        \"utf8\": false\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"formats_action\": \"Replace\"\n" +
                        "}",
                316,
                "columns are already defined"
        );
    }

    @Test
    public void testFailMultipleFormatsActionProperties() {
        assertException(
                "{\n" +
                        "  \"columns\": [\n" +
                        "    {\n" +
                        "      \"file_column_name\": \"x\",\n" +
                        "      \"file_column_index\": 0,\n" +
                        "      \"column_type\": \"TIMESTAMP\",\n" +
                        "      \"table_column_name\": \"x\",\n" +
                        "      \"formats\": [\n" +
                        "        {\n" +
                        "          \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "          \"locale\": null,\n" +
                        "          \"utf8\": false\n" +
                        "        }\n" +
                        "      ]\n" +
                        "    }\n" +
                        "  ],\n" +
                        "  \"formats\": {\n" +
                        "    \"DATE\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"dd/MM/y\",\n" +
                        "        \"locale\": \"ja\",\n" +
                        "        \"utf8\": true\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"TIMESTAMP\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": \"en\",\n" +
                        "        \"utf8\": false\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"formats_action\": \"Replace\",\n" +
                        "  \"formats_action\": \"Add\"\n" +
                        "}",
                618,
                "formats_action is already defined"
        );
    }

    @Test
    public void testFailMultipleFormatsSections() {
        assertException(
                "{\n" +
                        "  \"columns\": [\n" +
                        "    {\n" +
                        "      \"file_column_name\": \"x\",\n" +
                        "      \"file_column_index\": 0,\n" +
                        "      \"column_type\": \"TIMESTAMP\",\n" +
                        "      \"table_column_name\": \"x\",\n" +
                        "      \"formats\": [\n" +
                        "        {\n" +
                        "          \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "          \"locale\": null,\n" +
                        "          \"utf8\": false\n" +
                        "        }\n" +
                        "      ]\n" +
                        "    }\n" +
                        "  ],\n" +
                        "  \"formats\": {\n" +
                        "    \"DATE\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"dd/MM/y\",\n" +
                        "        \"locale\": \"ja\",\n" +
                        "        \"utf8\": true\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"TIMESTAMP\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": \"en\",\n" +
                        "        \"utf8\": false\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"formats\": {},\n" +
                        "  \"formats_action\": \"Replace\"\n" +
                        "}",
                587,
                "formats are already defined"
        );
    }

    @Test
    public void testFormatsActionReplaceIgnoringCase() throws Exception {
        assertJson(
                "{\n" +
                        "  \"columns\": [],\n" +
                        "  \"formats\": {\n" +
                        "    \"DATE\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"dd/MM/y\",\n" +
                        "        \"locale\": \"ja\",\n" +
                        "        \"utf8\": true\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"TIMESTAMP\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": \"en\",\n" +
                        "        \"utf8\": false\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"formats_action\": \"Replace\"\n" +
                        "}",
                "{\"columns\":[],\"formats\": {\"DATE\": [{\"pattern\":\"dd/MM/y\",\"locale\":\"ja\",\"utf8\":true}],\"TIMESTAMP\": [{\"pattern\":\"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\"locale\":\"en\",\"utf8\":false}]},\"formats_action\":\"REPLACE\"}"
        );
    }

    @Test
    public void testFormatsTypeCaseInsensitive() throws JsonException {
        assertJson(
                "{\n" +
                        "  \"formats\": {\n" +
                        "    \"timeStamp\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": \"en\",\n" +
                        "        \"utf8\": false\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"formats_action\": \"Add\"\n" +
                        "}",
                "{\"columns\":[],\"formats\": {\"TIMESTAMP\": [{\"pattern\":\"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\"locale\":\"en\",\"utf8\":false}]},\"formats_action\":\"ADD\"}"
        );
    }

    @Test
    public void testMultipleColumnFormats() throws Exception {
        assertJson(
                "{\n" +
                        "  \"columns\": [\n" +
                        "    {\n" +
                        "      \"file_column_name\": \"x\",\n" +
                        "      \"file_column_index\": 0,\n" +
                        "      \"column_type\": \"TIMESTAMP\",\n" +
                        "      \"table_column_name\": \"x\",\n" +
                        "      \"formats\": [\n" +
                        "        {\n" +
                        "          \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "          \"locale\": null,\n" +
                        "          \"utf8\": false\n" +
                        "        },\n" +
                        "        {\n" +
                        "          \"pattern\": \"yyyy-MM-ddTHH:mm:ss.SSSUUUz\",\n" +
                        "          \"locale\": \"ja\",\n" +
                        "          \"utf8\": true\n" +
                        "        }\n" +
                        "      ]\n" +
                        "    }\n" +
                        "  ],\n" +
                        "  \"formats\": {\n" +
                        "    \"TIMESTAMP\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": null,\n" +
                        "        \"utf8\": false\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": null,\n" +
                        "        \"utf8\": false\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"formats_action\": \"ADD\"\n" +
                        "}",
                "{\"columns\":[{\"file_column_name\":\"x\",\"file_column_index\":0,\"file_column_ignore\":false,\"column_type\":\"TIMESTAMP\",\"table_column_name\":\"x\",\"formats\":[{\"pattern\":\"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\"locale\":\"en\",\"utf8\":false},{\"pattern\":\"yyyy-MM-ddTHH:mm:ss.SSSUUUz\",\"locale\":\"ja\",\"utf8\":true}]}],\"formats\": {\"TIMESTAMP\": [{\"pattern\":\"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\"locale\":\"en\",\"utf8\":false},{\"pattern\":\"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\"locale\":\"en\",\"utf8\":false}]},\"formats_action\":\"ADD\"}"
        );
    }

    @Test
    public void testNoColumns() throws Exception {
        assertJson(
                "{\n" +
                        "  \"formats\": {\n" +
                        "    \"DATE\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"dd/MM/y\",\n" +
                        "        \"locale\": \"ja\",\n" +
                        "        \"utf8\": true\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"TIMESTAMP\": [\n" +
                        "      {\n" +
                        "        \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\n" +
                        "        \"locale\": \"en\",\n" +
                        "        \"utf8\": false\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  },\n" +
                        "  \"formats_action\": \"Add\"\n" +
                        "}",
                "{\"columns\":[]," +
                        "\"formats\": " +
                        "{\"DATE\": [{\"pattern\":\"dd/MM/y\",\"locale\":\"ja\",\"utf8\":true}]," +
                        "\"TIMESTAMP\": [{\"pattern\":\"yyyy-MM-dd'T'HH:mm:ss*SSSZZZZ\",\"locale\":\"en\",\"utf8\":false}]}," +
                        "\"formats_action\":\"ADD\"}"
        );
    }

    private void assertException(CharSequence input, int pos, CharSequence contains) {
        long buf = TestUtils.toMemory(input);
        try {
            LEXER.parse(buf, buf + input.length(), schemaV2Parser);
            Assert.fail();
        } catch (JsonException e) {
            Assert.assertEquals(pos, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), contains);
        } finally {
            Unsafe.free(buf, input.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertJson(CharSequence input, CharSequence expected) throws JsonException {
        long buf = TestUtils.toMemory(input);
        try {
            LEXER.parse(buf, buf + input.length(), schemaV2Parser);
            schema.toSink(sink);
            TestUtils.assertEquals(expected, sink);
        } finally {
            Unsafe.free(buf, input.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }

}