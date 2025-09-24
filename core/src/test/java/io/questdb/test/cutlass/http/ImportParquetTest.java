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

package io.questdb.test.cutlass.http;

import io.questdb.cairo.TableReader;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;

public class ImportParquetTest extends AbstractCairoTest {
    @Before
    public void setUp() {
        super.setUp();
        inputRoot = root;
    }

    @Test
    public void testParquetImportAbsPath() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) id from long_sequence(10))");
            byte[] parquetData = createParquetFile("x");
            String fileName = Os.isWindows() ? root + "\\test.parquet" : "/test.parquet";
            byte[] parquetImportRequest = createParquetImportRequest(fileName, parquetData, "json", 0);

            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withCopyInputRoot(root)
                    .withWorkerCount(1)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        new SendAndReceiveRequestBuilder()
                                .execute(
                                        parquetImportRequest,
                                        "HTTP/1.1 200 OK\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: application/json; charset=utf-8\r\n" +
                                                "\r\n" +
                                                "3d\r\n" +
                                                "{\"status\":\"absolute path is not allowed in parquet filename\"}\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                    });
        });
    }

    @Test
    public void testParquetImportBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " cast(x as int) id," +
                    " rnd_str(4,4,4,2) as name," +
                    " rnd_double() as price," +
                    " rnd_timestamp('2023-01-01','2023-12-31',2) as timestamp," +
                    " rnd_symbol(4,4,4,2) as category" +
                    " from long_sequence(1000))");

            byte[] parquetData = createParquetFile("x");
            byte[] parquetImportRequest = createParquetImportRequest("test.parquet", parquetData, "text");

            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withCopyInputRoot(root)
                    .withWorkerCount(1)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        new SendAndReceiveRequestBuilder()
                                .execute(
                                        parquetImportRequest,
                                        "HTTP/1.1 200 OK\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: text/plain; charset=utf-8\r\n" +
                                                "\r\n" +
                                                "5d\r\n" +
                                                "Parquet file imported successfully\r\n" +
                                                "File: test.parquet\r\n" +
                                                "Size: 22902 bytes\r\n" +
                                                "Status: imported\r\n" +
                                                "\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                        assertQueryNoLeakCheck("id\tname\tprice\ttimestamp\tcategory\n" +
                                        "991\t\t0.3986091381481086\t2023-09-21T23:37:50.175745Z\tGPGW\n" +
                                        "992\t\t0.32981711763208754\t2023-08-10T03:53:28.805421Z\tIBBT\n" +
                                        "993\t\t0.17584294026220126\t2023-09-24T00:13:26.991347Z\t\n" +
                                        "994\tHYRX\t0.3774250796945574\t2023-03-24T04:23:20.021264Z\t\n" +
                                        "995\t\t0.49853085145025433\t2023-09-08T07:29:01.040677Z\t\n" +
                                        "996\t\t0.3762782783758658\t2023-12-03T19:05:41.071046Z\tGPGW\n" +
                                        "997\t\t0.17328491926011413\t2023-07-17T06:55:52.805956Z\tSXUX\n" +
                                        "998\t\t0.7198950859624534\t2023-09-06T07:48:58.863486Z\tSXUX\n" +
                                        "999\tCPSW\t0.7680389606555971\t2023-11-20T16:31:04.795595Z\tGPGW\n" +
                                        "1000\tVTJW\t0.35343989503439177\t2023-12-02T18:01:50.509085Z\t\n",
                                "select * from read_parquet('test.parquet') limit -10", null, null, true, true);
                    });
        });
    }

    @Test
    public void testParquetImportJsonResponse() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) id from long_sequence(100))");
            byte[] parquetData = createParquetFile("x");
            byte[] parquetImportRequest = createParquetImportRequest("test.parquet", parquetData, "json");
            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withCopyInputRoot(root)
                    .withWorkerCount(1)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        new SendAndReceiveRequestBuilder()
                                .execute(
                                        parquetImportRequest,
                                        "HTTP/1.1 200 OK\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: application/json; charset=utf-8\r\n" +
                                                "\r\n" +
                                                "53\r\n" +
                                                "{\"operation\":\"parquet_import\",\"file\":\"test.parquet\",\"size\":732,\"status\":\"imported\"}\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                        assertQueryNoLeakCheck("sum\n" +
                                        "5050\n",
                                "select sum(id) from read_parquet('test.parquet')", null, null, false, true);
                    });
        });
    }

    @Test
    public void testParquetImportLargeFile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table large_x as (select" +
                    " cast(x as int) id," +
                    " rnd_str('ABC', 'CDE', null, 'XYZ') as description," +
                    " rnd_double() as value1," +
                    " rnd_float() as value2," +
                    " rnd_timestamp('2023-01-01','2023-12-31',2) as created_at," +
                    " rnd_symbol('a','b','c') as status" +
                    " from long_sequence(100000))");

            byte[] parquetData = createParquetFile("large_x");
            String fileName = Os.isWindows() ? "dir\\dir1\\large_test.parquet" : "dir/dir1/large_test.parquet";
            byte[] parquetImportRequest = createParquetImportRequest(fileName, parquetData, "text");
            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withWorkerCount(2)
                    .withCopyInputRoot(root)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        new SendAndReceiveRequestBuilder()
                                .execute(
                                        parquetImportRequest,
                                        "HTTP/1.1 200 OK\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: text/plain; charset=utf-8\r\n" +
                                                "\r\n" +
                                                "6e\r\n" +
                                                "Parquet file imported successfully\r\n" +
                                                "File: " + fileName + "\r\n" +
                                                "Size: 2594145 bytes\r\n" +
                                                "Status: imported\r\n" +
                                                "\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                        String expected = "id\tdescription\tvalue1\tvalue2\tcreated_at\tstatus\n" +
                                "99991\tCDE\t0.8166478639285287\t0.4386441\t2023-05-16T21:22:48.854659Z\ta\n" +
                                "99992\tCDE\t0.17891236195840365\t0.34030402\t2023-08-21T17:24:04.807108Z\tb\n" +
                                "99993\tXYZ\t0.05557740758458862\t0.37863648\t2023-12-10T02:42:33.064423Z\tb\n" +
                                "99994\tCDE\t0.8353920305218698\t0.1985147\t2023-01-30T09:44:51.084677Z\tb\n" +
                                "99995\t\t0.4783108349434946\t0.079084635\t2023-11-01T02:22:06.043102Z\ta\n" +
                                "99996\t\t0.8686709902046369\t0.97213304\t2023-12-24T09:44:05.007638Z\ta\n" +
                                "99997\tXYZ\t0.4164018538135147\t0.29396755\t2023-03-25T16:40:31.769048Z\tc\n" +
                                "99998\tCDE\t0.7721854080413442\t0.35694885\t2023-07-15T20:57:24.754320Z\ta\n" +
                                "99999\tABC\t0.2478919218228035\t0.16465133\t\ta\n" +
                                "100000\tCDE\t0.1399738796495611\t0.17249799\t\tb\n";
                        assertQueryNoLeakCheck(expected,
                                "select * from read_parquet('" + fileName + "') limit -10", null, null, true, true);
                        assertQueryNoLeakCheck(expected,
                                "select * from read_parquet('large_x.parquet') limit -10", null, null, true, true);
                    });
        });
    }

    @Test
    public void testParquetImportLargeTotalSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) id from long_sequence(100))");
            byte[] parquetData = createParquetFile("x");
            byte[] parquetImportRequest = createParquetImportRequest("test.parquet", parquetData, "json", 1000);
            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withCopyInputRoot(root)
                    .withWorkerCount(1)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        new SendAndReceiveRequestBuilder()
                                .execute(
                                        parquetImportRequest,
                                        "HTTP/1.1 200 OK\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: application/json; charset=utf-8\r\n" +
                                                "\r\n" +
                                                "6a\r\n" +
                                                "{\"status\":\"parquet file transfer incomplete [expected=1000 bytes, received=732 bytes, file=test.parquet]\"}\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                    });
        });
    }

    @Test
    public void testParquetImportRejectsInvalidParquet() throws Exception {
        assertMemoryLeak(() -> {
            String invalidRequest = "POST /upload?total_size=" + 20 + " HTTP/1.1\r\n" +
                    "Host: localhost:9001\r\n" +
                    "User-Agent: curl/7.64.0\r\n" +
                    "Content-Type: multipart/form-data; boundary=boundary123\r\n" +
                    "Content-Length: 100\r\n" +
                    "\r\n" +
                    "--boundary123\r\n" +
                    "Content-Disposition: form-data; name=\"parquet\"; filename=\"test.parquet\"\r\n" +
                    "\r\n" +
                    "invalid parquet data\r\n" +
                    "--boundary123--";

            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withCopyInputRoot(root)
                    .withWorkerCount(1)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        new SendAndReceiveRequestBuilder()
                                .execute(
                                        invalidRequest.getBytes(),
                                        "HTTP/1.1 200 OK\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: text/plain; charset=utf-8\r\n" +
                                                "\r\n" +
                                                "87\r\n" +
                                                "error in PartitionDecoder.create: could not read parquet file with read size 20: File out of specification: The file must end with PAR1\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                    });
        });
    }

    @Test
    public void testParquetImportRelativePath() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) id from long_sequence(10))");
            byte[] parquetData = createParquetFile("x");
            String fileName = Os.isWindows() ? "..\\test.parquet" : "../test.parquet";
            byte[] parquetImportRequest = createParquetImportRequest(fileName, parquetData, "json", 0);

            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withCopyInputRoot(root)
                    .withWorkerCount(1)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        new SendAndReceiveRequestBuilder()
                                .execute(
                                        parquetImportRequest,
                                        "HTTP/1.1 200 OK\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: application/json; charset=utf-8\r\n" +
                                                "\r\n" +
                                                "3d\r\n" +
                                                "{\"status\":\"relative path is not allowed in parquet filename\"}\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                    });
        });
    }

    @Test
    public void testParquetImportSmallTotalSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) id from long_sequence(100))");
            byte[] parquetData = createParquetFile("x");
            byte[] parquetImportRequest = createParquetImportRequest("test.parquet", parquetData, "json", 100);
            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withCopyInputRoot(root)
                    .withWorkerCount(1)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        new SendAndReceiveRequestBuilder()
                                .execute(
                                        parquetImportRequest,
                                        "HTTP/1.1 200 OK\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: application/json; charset=utf-8\r\n" +
                                                "\r\n" +
                                                "65\r\n" +
                                                "{\"status\":\"parquet chunk exceeds expected file size [written=0, chunk=732, total=732, expected=100]\"}\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                    });
        });
    }

    @Test
    public void testParquetImportWithNegativeSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) id from long_sequence(10))");
            byte[] parquetData = createParquetFile("x");
            byte[] parquetImportRequest = createParquetImportRequest("test.parquet", parquetData, "json", -100);

            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withCopyInputRoot(root)
                    .withWorkerCount(1)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        new SendAndReceiveRequestBuilder()
                                .execute(
                                        parquetImportRequest,
                                        "HTTP/1.1 200 OK\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: application/json; charset=utf-8\r\n" +
                                                "\r\n" +
                                                "4e\r\n" +
                                                "{\"status\":\"parquet import requires positive 'total_size' URL parameter value\"}\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                    });
        });
    }

    @Test
    public void testParquetImportWithoutSqlCopyInputRoot() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) id from long_sequence(10))");
            byte[] parquetData = createParquetFile("x");

            byte[] parquetImportRequest = createParquetImportRequest("test.parquet", parquetData, "text");

            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withWorkerCount(1)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        new SendAndReceiveRequestBuilder()
                                .execute(
                                        parquetImportRequest,
                                        "HTTP/1.1 200 OK\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: text/plain; charset=utf-8\r\n" +
                                                "\r\n" +
                                                "49\r\n" +
                                                "parquet files can only be imported when sql.copy.input.root is configured\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                    });
        });
    }

    @Test
    public void testParquetImportWithoutTotalSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) id from long_sequence(10))");
            byte[] parquetData = createParquetFile("x");
            byte[] parquetImportRequest = createParquetImportRequest("test.parquet", parquetData, "json", 0);

            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withCopyInputRoot(root)
                    .withWorkerCount(1)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        new SendAndReceiveRequestBuilder()
                                .execute(
                                        parquetImportRequest,
                                        "HTTP/1.1 200 OK\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: application/json; charset=utf-8\r\n" +
                                                "\r\n" +
                                                "59\r\n" +
                                                "{\"status\":\"parquet import requires 'total_size' URL parameter to pre-allocate file size\"}\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                    });
        });
    }

    @Test
    public void testParquetImportWithoutTotalSizeNotNumber() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) id from long_sequence(10))");
            byte[] parquetData = createParquetFile("x");
            byte[] parquetImportRequest = createParquetImportRequest("test.parquet", parquetData, "json", Integer.MIN_VALUE);

            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withCopyInputRoot(root)
                    .withWorkerCount(1)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        new SendAndReceiveRequestBuilder()
                                .execute(
                                        parquetImportRequest,
                                        "HTTP/1.1 200 OK\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: application/json; charset=utf-8\r\n" +
                                                "\r\n" +
                                                "4d\r\n" +
                                                "{\"status\":\"parquet import requires valid numeric 'total_size' URL parameter\"}\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                    });
        });
    }

    private byte[] createParquetFile(String tableName) throws Exception {
        try (
                Path path = new Path();
                PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader(tableName)
        ) {
            path.of(root).concat(tableName).put(".parquet");
            PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
            PartitionEncoder.encode(partitionDescriptor, path);
            Assert.assertTrue("Parquet file should exist", Files.exists(path.$()));
            File parquetFile = new File(path.toString());
            byte[] parquetData = new byte[(int) parquetFile.length()];
            try (FileInputStream fis = new FileInputStream(parquetFile)) {
                int bytesRead = fis.read(parquetData);
                Assert.assertEquals("Should read entire file", parquetFile.length(), bytesRead);
            }
            return parquetData;
        }
    }

    private byte[] createParquetImportRequest(String filename, byte[] parquetData, String format) {
        return createParquetImportRequest(filename, parquetData, format, parquetData.length);
    }

    private byte[] createParquetImportRequest(String filename, byte[] parquetData, String format, int size) {
        String boundary = "------------------------boundary123";
        String header = "POST /upload?";
        if (size == Integer.MIN_VALUE) {
            header += "total_size=aaaaa&fmt=" + format + " HTTP/1.1\r\n" +
                    "Host: localhost:9001\r\n" +
                    "User-Agent: curl/7.64.0\r\n" +
                    "Accept: */*\r\n" +
                    "Content-Type: multipart/form-data; boundary=" + boundary + "\r\n" +
                    "\r\n" +
                    "--" + boundary + "\r\n" +
                    "Content-Disposition: form-data; name=\"parquet\"; filename=\"" + filename + "\"\r\n" +
                    "Content-Type: application/octet-stream\r\n" +
                    "\r\n";
        }
        if (size != 0) {
            header += "total_size=" + size + "&fmt=" + format + " HTTP/1.1\r\n" +
                    "Host: localhost:9001\r\n" +
                    "User-Agent: curl/7.64.0\r\n" +
                    "Accept: */*\r\n" +
                    "Content-Type: multipart/form-data; boundary=" + boundary + "\r\n" +
                    "\r\n" +
                    "--" + boundary + "\r\n" +
                    "Content-Disposition: form-data; name=\"parquet\"; filename=\"" + filename + "\"\r\n" +
                    "Content-Type: application/octet-stream\r\n" +
                    "\r\n";
        } else {
            header += "fmt=" + format + " HTTP/1.1\r\n" +
                    "Host: localhost:9001\r\n" +
                    "User-Agent: curl/7.64.0\r\n" +
                    "Accept: */*\r\n" +
                    "Content-Type: multipart/form-data; boundary=" + boundary + "\r\n" +
                    "\r\n" +
                    "--" + boundary + "\r\n" +
                    "Content-Disposition: form-data; name=\"parquet\"; filename=\"" + filename + "\"\r\n" +
                    "Content-Type: application/octet-stream\r\n" +
                    "\r\n";
        }
        String footer = "\r\n--" + boundary + "--";

        byte[] headerBytes = header.getBytes();
        byte[] footerBytes = footer.getBytes();

        int bodyLength = headerBytes.length + parquetData.length + footerBytes.length - header.split("\r\n\r\n")[0].length() - 4;
        header = header.replace("Content-Type:", "Content-Length: " + bodyLength + "\r\nContent-Type:");
        headerBytes = header.getBytes();
        byte[] requestBytes = new byte[headerBytes.length + parquetData.length + footerBytes.length];
        System.arraycopy(headerBytes, 0, requestBytes, 0, headerBytes.length);
        System.arraycopy(parquetData, 0, requestBytes, headerBytes.length, parquetData.length);
        System.arraycopy(footerBytes, 0, requestBytes, headerBytes.length + parquetData.length, footerBytes.length);
        return requestBytes;
    }
}