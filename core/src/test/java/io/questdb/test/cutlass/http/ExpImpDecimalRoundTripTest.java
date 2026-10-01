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

package io.questdb.test.cutlass.http;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ExpImpDecimalRoundTripTest extends AbstractTest {
    private static final String BOUNDARY = "------------------------27d997ca93d2689d";
    private static final String DECIMAL_COLUMNS = "d8 decimal(2,1), d16 decimal(4,2), d32 decimal(9,4), " +
            "d64 decimal(18,6), d128 decimal(38,10), d256 decimal(76,20)";
    private static final String EXPECTED_CSV = "\"d8\",\"d16\",\"d32\",\"d64\",\"d128\",\"d256\"\r\n" +
            "\"1.2\",\"12.34\",\"12345.6789\",\"123456789012.345678\",\"1234567890123456789012345678.9012345678\"," +
            "\"12345678901234567890123456789012345678901234567890123456.78901234567890123456\"\r\n" +
            "\"-1.2\",\"-12.34\",\"-12345.6789\",\"-123456789012.345678\",\"-1234567890123456789012345678.9012345678\"," +
            "\"-12345678901234567890123456789012345678901234567890123456.78901234567890123456\"\r\n" +
            ",,,,,\r\n";
    private static final String WIDE = "1234567890123456789012345678901234567890123456789012345678901234567890123456";

    @Test
    public void testExportEmitsEmptyFieldForNullDecimals() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            createSourceTable(engine);
            TestUtils.assertEquals(EXPECTED_CSV, export("select * from src"));
        });
    }

    @Test
    public void testPlainDecimalNumbersImportAsDouble() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            upload("plain", "small,big\r\n" +
                    "1.5,1234567890123456789012345678901234567890.5\r\n" +
                    "2.25,9876543210987654321098765432109876543210.25\r\n" +
                    "3.125,1111111111111111111111111111111111111111.125\r\n");

            Assert.assertEquals("DOUBLE", columnType(engine, "plain", "small"));
            Assert.assertEquals("DOUBLE", columnType(engine, "plain", "big"));
            TestUtils.assertEquals(
                    "small\n1.5\n2.25\n3.125\n",
                    TestUtils.printSqlToString(engine, sqlExecutionContext, "select small from plain", new StringSink())
            );
        });
    }

    @Test
    public void testReimportIsExactOnlyIntoPreCreatedTable() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            createSourceTable(engine);
            engine.execute("create table dst (" + DECIMAL_COLUMNS + ")");

            final String csv = export("select * from src");
            TestUtils.assertEquals(EXPECTED_CSV, csv);

            upload("dst", csv);
            Assert.assertEquals("DECIMAL(2,1)", columnType(engine, "dst", "d8"));
            Assert.assertEquals("DECIMAL(4,2)", columnType(engine, "dst", "d16"));
            Assert.assertEquals("DECIMAL(9,4)", columnType(engine, "dst", "d32"));
            Assert.assertEquals("DECIMAL(18,6)", columnType(engine, "dst", "d64"));
            Assert.assertEquals("DECIMAL(38,10)", columnType(engine, "dst", "d128"));
            Assert.assertEquals("DECIMAL(76,20)", columnType(engine, "dst", "d256"));
            TestUtils.assertEquals(
                    TestUtils.printSqlToString(engine, sqlExecutionContext, "select * from src", new StringSink()),
                    TestUtils.printSqlToString(engine, sqlExecutionContext, "select * from dst", new StringSink())
            );

            // an exported decimal is a plain number, so a new table auto-detects it as DOUBLE
            upload("detected", csv);
            Assert.assertEquals("DOUBLE", columnType(engine, "detected", "d8"));
            Assert.assertEquals("DOUBLE", columnType(engine, "detected", "d16"));
            Assert.assertEquals("DOUBLE", columnType(engine, "detected", "d32"));
            Assert.assertEquals("DOUBLE", columnType(engine, "detected", "d64"));
            Assert.assertEquals("DOUBLE", columnType(engine, "detected", "d128"));
            Assert.assertEquals("DOUBLE", columnType(engine, "detected", "d256"));
        });
    }

    @Test
    public void testSuffixedDecimalOutsideDetectedTypeStaysText() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            upload("fits", "id,amount\r\n1,1.234m\r\n2,2.345m\r\n");
            Assert.assertEquals("DECIMAL(18,3)", columnType(engine, "fits", "amount"));

            // more decimals than the detected type holds, the text is kept instead of being nulled
            upload("wide_scale", "id,amount\r\n1,1.2345m\r\n2,2.3456m\r\n");
            Assert.assertEquals("VARCHAR", columnType(engine, "wide_scale", "amount"));
            TestUtils.assertEquals(
                    "id\tamount\n1\t1.2345m\n2\t2.3456m\n",
                    TestUtils.printSqlToString(engine, sqlExecutionContext, "select * from wide_scale", new StringSink())
            );

            upload("wide_precision", "id,amount\r\n1,12345678901234567890.1m\r\n2,22345678901234567890.2m\r\n");
            Assert.assertEquals("VARCHAR", columnType(engine, "wide_precision", "amount"));
            TestUtils.assertEquals(
                    "id\tamount\n1\t12345678901234567890.1m\n2\t22345678901234567890.2m\n",
                    TestUtils.printSqlToString(engine, sqlExecutionContext, "select * from wide_precision", new StringSink())
            );
        });
    }

    @Test
    public void testWideDecimalLosesDigitsWhenAutoDetected() throws Exception {
        getSimpleTester().run((engine, sqlExecutionContext) -> {
            engine.execute("create table wide_src (id long, v decimal(76,0))");
            engine.execute("insert into wide_src values (1, " + WIDE + "::decimal(76,0))");
            engine.execute("insert into wide_src values (2, -" + WIDE + "::decimal(76,0))");
            engine.execute("create table wide_dst (id long, v decimal(76,0))");

            final String csv = export("select * from wide_src");
            TestUtils.assertEquals("\"id\",\"v\"\r\n1,\"" + WIDE + "\"\r\n2,\"-" + WIDE + "\"\r\n", csv);

            // an existing decimal column of the same width takes all 76 digits back unchanged
            upload("wide_dst", csv);
            Assert.assertEquals("DECIMAL(76,0)", columnType(engine, "wide_dst", "v"));
            TestUtils.assertEquals(
                    TestUtils.printSqlToString(engine, sqlExecutionContext, "select * from wide_src", new StringSink()),
                    TestUtils.printSqlToString(engine, sqlExecutionContext, "select * from wide_dst", new StringSink())
            );

            // auto-detection cannot size the column and settles on DOUBLE, which keeps 17 digits
            upload("wide_detected", csv);
            Assert.assertEquals("DOUBLE", columnType(engine, "wide_detected", "v"));
            TestUtils.assertEquals(
                    "id\tv\n1\t1.2345678901234569E75\n2\t-1.2345678901234569E75\n",
                    TestUtils.printSqlToString(engine, sqlExecutionContext, "select * from wide_detected", new StringSink())
            );
        });
    }

    private static String columnType(CairoEngine engine, String tableName, String columnName) {
        try (TableReader reader = engine.getReader(tableName)) {
            final RecordMetadata metadata = reader.getMetadata();
            return ColumnType.nameOf(metadata.getColumnType(metadata.getColumnIndex(columnName)));
        }
    }

    private static void createSourceTable(CairoEngine engine) throws SqlException {
        engine.execute("create table src (" + DECIMAL_COLUMNS + ")");
        engine.execute("insert into src values (" +
                "1.2::decimal(2,1), " +
                "12.34::decimal(4,2), " +
                "12345.6789::decimal(9,4), " +
                "123456789012.345678::decimal(18,6), " +
                "1234567890123456789012345678.9012345678::decimal(38,10), " +
                "12345678901234567890123456789012345678901234567890123456.78901234567890123456::decimal(76,20))");
        engine.execute("insert into src values (" +
                "-1.2::decimal(2,1), " +
                "-12.34::decimal(4,2), " +
                "-12345.6789::decimal(9,4), " +
                "-123456789012.345678::decimal(18,6), " +
                "-1234567890123456789012345678.9012345678::decimal(38,10), " +
                "-12345678901234567890123456789012345678901234567890123456.78901234567890123456::decimal(76,20))");
        engine.execute("insert into src values (null, null, null, null, null, null)");
    }

    private static String export(String sql) {
        try (TestHttpClient client = new TestHttpClient()) {
            final Utf8StringSink sink = new Utf8StringSink();
            client.toSink("/exp", sql, sink);
            return sink.toString();
        }
    }

    private static void upload(String tableName, String csv) {
        new SendAndReceiveRequestBuilder()
                .withCompareLength(15)
                .execute(
                        "POST /imp?name=" + tableName + " HTTP/1.1\r\n" +
                                "Host: localhost:9001\r\n" +
                                "User-Agent: curl/7.64.0\r\n" +
                                "Accept: */*\r\n" +
                                "Content-Length: 437760673\r\n" +
                                "Content-Type: multipart/form-data; boundary=" + BOUNDARY + "\r\n" +
                                "Expect: 100-continue\r\n" +
                                "\r\n" +
                                "--" + BOUNDARY + "\r\n" +
                                "Content-Disposition: form-data; name=\"data\"; filename=\"" + tableName + ".csv\"\r\n" +
                                "Content-Type: application/octet-stream\r\n" +
                                "\r\n" +
                                csv +
                                "\r\n--" + BOUNDARY + "--",
                        "HTTP/1.1 200 OK"
                );
    }
}
