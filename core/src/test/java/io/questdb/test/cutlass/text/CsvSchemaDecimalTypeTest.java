/*+*****************************************************************************
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

package io.questdb.test.cutlass.text;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.text.DefaultTextConfiguration;
import io.questdb.cutlass.text.TextConfiguration;
import io.questdb.cutlass.text.TextMetadataParser;
import io.questdb.cutlass.text.types.TypeAdapter;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.std.Decimal256;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractTest;
import io.questdb.test.cutlass.http.HttpQueryTestBuilder;
import io.questdb.test.cutlass.http.HttpServerConfigurationBuilder;
import io.questdb.test.cutlass.http.SendAndReceiveRequestBuilder;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CsvSchemaDecimalTypeTest extends AbstractTest {
    private static final String BOUNDARY = "------------------------27d997ca93d2689d";
    private JsonLexer lexer;
    private TextMetadataParser metadataParser;
    private TypeManager typeManager;
    private DirectUtf16Sink utf16Sink;
    private DirectUtf8Sink utf8Sink;

    @Before
    public void setUpParser() {
        final TextConfiguration configuration = new DefaultTextConfiguration();
        utf16Sink = new DirectUtf16Sink(1024);
        utf8Sink = new DirectUtf8Sink(1024);
        lexer = new JsonLexer(1024, 4096);
        typeManager = new TypeManager(configuration, utf16Sink, utf8Sink, new Decimal256());
        metadataParser = new TextMetadataParser(configuration, typeManager);
    }

    @After
    public void tearDownParser() {
        metadataParser.close();
        lexer.close();
        utf8Sink.close();
        utf16Sink.close();
    }

    @Test
    public void testBareDecimalIsRejected() {
        assertInvalidType("DECIMAL");
        assertInvalidType("decimal");
        assertInvalidType("Decimal");
    }

    @Test
    public void testImportRejectsBareDecimal() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    new SendAndReceiveRequestBuilder()
                            .withExpectReceiveDisconnect(true)
                            .withCompareLength(16)
                            .execute(
                                    importRequest("bare_dec", "DECIMAL", "0.125"),
                                    "HTTP/1.1 200 OK\r\n"
                            );
                    Assert.assertNull(engine.getTableTokenIfExists("bare_dec"));
                });
    }

    @Test
    public void testImportWithDecimalSchema() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    new SendAndReceiveRequestBuilder()
                            .withCompareLength(16)
                            .execute(
                                    importRequest("dec_schema", "DECIMAL(18,3)", "1.234", "-2.5", "0"),
                                    "HTTP/1.1 200 OK\r\n"
                            );

                    try (TableReader reader = engine.getReader("dec_schema")) {
                        final RecordMetadata metadata = reader.getMetadata();
                        Assert.assertEquals(
                                ColumnType.getDecimalType(18, 3),
                                metadata.getColumnType(metadata.getColumnIndex("amount"))
                        );
                    }

                    final StringSink sink = new StringSink();
                    TestUtils.assertEquals(
                            "name\tamount\nrow0\t1.234\nrow1\t-2.500\nrow2\t0.000\n",
                            TestUtils.printSqlToString(engine, sqlExecutionContext, "select * from dec_schema", sink)
                    );
                });
    }

    @Test
    public void testImportWithMaxPrecisionDecimalSchema() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    new SendAndReceiveRequestBuilder()
                            .withCompareLength(16)
                            .execute(
                                    importRequest("dec_full_scale", "DECIMAL(76,75)", "0.5", "-0.25"),
                                    "HTTP/1.1 200 OK\r\n"
                            );

                    try (TableReader reader = engine.getReader("dec_full_scale")) {
                        final RecordMetadata metadata = reader.getMetadata();
                        Assert.assertEquals(
                                ColumnType.getDecimalType(76, 75),
                                metadata.getColumnType(metadata.getColumnIndex("amount"))
                        );
                    }

                    final StringSink sink = new StringSink();
                    TestUtils.assertEquals(
                            "name\tamount\n" +
                                    "row0\t0.5" + "0".repeat(74) + "\n" +
                                    "row1\t-0.25" + "0".repeat(73) + "\n",
                            TestUtils.printSqlToString(engine, sqlExecutionContext, "select * from dec_full_scale", sink)
                    );
                });
    }

    @Test
    public void testScaleAbovePrecisionIsRejected() {
        assertInvalidType("DECIMAL(2,76)");
        assertInvalidType("decimal(2,76)");
        assertInvalidType("DECIMAL(1,2)");
        assertInvalidType("DECIMAL(18,19)");
        assertInvalidType("DECIMAL(75,76)");
    }

    @Test
    public void testValidDecimalIsAccepted() throws JsonException {
        assertColumnType("DECIMAL(18,3)", ColumnType.DECIMAL64, 18, 3);
        assertColumnType("DECIMAL(76,76)", ColumnType.DECIMAL256, 76, 76);
        assertColumnType("DECIMAL(1,1)", ColumnType.DECIMAL8, 1, 1);
        assertColumnType("DECIMAL(1,0)", ColumnType.DECIMAL8, 1, 0);
        assertColumnType("DECIMAL(76,0)", ColumnType.DECIMAL256, 76, 0);
        assertColumnType("decimal(9,2)", ColumnType.DECIMAL32, 9, 2);
    }

    private static String importRequest(String tableName, String decimalType, String... values) {
        final StringBuilder csv = new StringBuilder("name,amount\r\n");
        for (int i = 0; i < values.length; i++) {
            csv.append("row").append(i).append(',').append(values[i]).append("\r\n");
        }
        final String schema = "[\r\n" +
                "  {\"name\": \"name\", \"type\": \"STRING\"},\r\n" +
                "  {\"name\": \"amount\", \"type\": \"" + decimalType + "\"}\r\n" +
                "]\r\n";
        return "POST /imp?name=" + tableName + " HTTP/1.1\r\n" +
                "Host: localhost:9001\r\n" +
                "User-Agent: curl/7.64.0\r\n" +
                "Accept: */*\r\n" +
                "Content-Length: 437760673\r\n" +
                "Content-Type: multipart/form-data; boundary=" + BOUNDARY + "\r\n" +
                "Expect: 100-continue\r\n" +
                "\r\n" +
                "--" + BOUNDARY + "\r\n" +
                "Content-Disposition: form-data; name=\"schema\"; filename=\"schema.json\"\r\n" +
                "Content-Type: application/octet-stream\r\n" +
                "\r\n" +
                schema +
                "\r\n--" + BOUNDARY + "\r\n" +
                "Content-Disposition: form-data; name=\"data\"; filename=\"" + tableName + ".csv\"\r\n" +
                "Content-Type: application/octet-stream\r\n" +
                "\r\n" +
                csv +
                "\r\n--" + BOUNDARY + "--";
    }

    private static String schemaOf(String typeName) {
        return "[{\"name\": \"x\", \"type\": \"" + typeName + "\"}]";
    }

    private void assertColumnType(String typeName, short expectedTag, int precision, int scale) throws JsonException {
        parse(schemaOf(typeName));
        final ObjList<TypeAdapter> types = metadataParser.getColumnTypes();
        Assert.assertEquals(1, types.size());
        final int type = types.getQuick(0).getType();
        Assert.assertEquals(expectedTag, ColumnType.tagOf(type));
        Assert.assertEquals(precision, ColumnType.getDecimalPrecision(type));
        Assert.assertEquals(scale, ColumnType.getDecimalScale(type));
    }

    private void assertInvalidType(String typeName) {
        final String schema = schemaOf(typeName);
        try {
            parse(schema);
            Assert.fail("expected rejection of " + typeName);
        } catch (JsonException e) {
            // the error points at the offending type value
            Assert.assertEquals(schema.indexOf('"' + typeName + '"') + 2, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "Invalid type");
        }
    }

    private void parse(String schema) throws JsonException {
        lexer.clear();
        metadataParser.clear();
        typeManager.clear();
        final long buf = TestUtils.toMemory(schema);
        try {
            lexer.parse(buf, buf + schema.length(), metadataParser);
        } finally {
            Unsafe.free(buf, schema.length(), MemoryTag.NATIVE_DEFAULT);
        }
    }
}
