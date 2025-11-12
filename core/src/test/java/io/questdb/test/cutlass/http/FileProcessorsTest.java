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
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.AbstractTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FileProcessorsTest extends AbstractCairoTest {
    private static final TestHttpClient testHttpClient = new TestHttpClient();

    @AfterClass
    public static void tearDownStatic() {
        testHttpClient.close();
        AbstractTest.tearDownStatic();
        assert Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN) == 0;
    }

    @Before
    public void setUp() {
        super.setUp();
        inputRoot = root + Files.SEPARATOR + "import";
        exportRoot = root + Files.SEPARATOR + "export";
        Path path = Path.getThreadLocal(inputRoot);
        FilesFacade ff = engine.getConfiguration().getFilesFacade();
        if (ff.exists(path.$())) {
            ff.rmdir(path, false);
            Files.mkdirs(path, engine.getConfiguration().getMkDirMode());
        }
    }

    @Test
    public void testFileImportExistsError() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " cast(x as int) id," +
                    " rnd_str(4,4,4,2) as name," +
                    " rnd_double() as price," +
                    " rnd_timestamp('2023-01-01','2023-12-31',2) as timestamp," +
                    " rnd_symbol(4,4,4,2) as category" +
                    " from long_sequence(10))");
            byte[] parquetData = createParquetFile("x");
            byte[] parquetImportRequest = createMultipleParquetImportRequest(new String[]{"x.parquet"}, new byte[][]{parquetData}, false);
            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withCopyInputRoot(inputRoot)
                    .withWorkerCount(1)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        new SendAndReceiveRequestBuilder()
                                .execute(
                                        parquetImportRequest,
                                        "HTTP/1.1 409 Conflict\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: application/vnd.api+json\r\n" +
                                                "\r\n" +
                                                "d0\r\n" +
                                                "{\"errors\":[{\"status\":\"409\",\"title\":\"File Upload Error\",\"detail\":\"file already exists and overwriting is disabled\",\"meta\":{\"filename\":\"x.parquet\"}}],\"meta\":{\"totalFiles\":1,\"successfulFiles\":0,\"failedFiles\":1}}\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                    });
        });
    }

    @Test
    public void testFileImportOverwriting() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " cast(x as int) id," +
                    " rnd_str(4,4,4,2) as name," +
                    " rnd_double() as price," +
                    " rnd_timestamp('2023-01-01','2023-12-31',2) as timestamp," +
                    " rnd_symbol(4,4,4,2) as category" +
                    " from long_sequence(10))");
            byte[] parquetData = createParquetFile("x");
            byte[] parquetImportRequest = createMultipleParquetImportRequest(new String[]{"x.parquet"}, new byte[][]{parquetData}, true);
            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withCopyInputRoot(inputRoot)
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
                                                "Content-Type: application/vnd.api+json\r\n" +
                                                "\r\n" +
                                                "7d\r\n" +
                                                "{\"data\":[{\"type\":\"file\",\"id\":\"x.parquet\",\"attributes\":{\"filename\":\"x.parquet\",\"status\":\"201\"}}],\"meta\":{\"totalFiles\":1}}\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                    });
        });
    }

    @Test
    public void testGetAndDeleteExportFiles() throws Exception {
        String file1 = "file1.csv";
        String file2 = !Os.isWindows() ? "dir2/dir2/dir2/file2.txt" : "dir2\\dir2\\dir2\\file2.txt";
        String file3 = !Os.isWindows() ? "dir2/dir3/file3" : "dir2\\dir3\\file3";
        String dir1 = "dir2" + Files.SEPARATOR + "dir4";
        String dir2 = "dir5";
        createFile(exportRoot, file1, "file1 content".getBytes(), 1000);
        createFile(exportRoot, file2, "file2 content".getBytes(), 2000);
        createFile(exportRoot, file3, "file3 content".getBytes(), 3000);
        Path path = Path.getThreadLocal(exportRoot);
        path.concat(dir1);
        Files.mkdir(path.slash$(), configuration.getMkDirMode());
        path.trimTo(0);
        path.concat(exportRoot).concat(dir2);
        Files.mkdir(path.slash$(), configuration.getMkDirMode());

        assertMemoryLeak(() -> {
            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withWorkerCount(2)
                    .withCopyExportRoot(exportRoot)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        String file2JsonPath = Os.isWindows() ? "dir2\\\\dir2\\\\dir2\\\\file2.txt" : "dir2/dir2/dir2/file2.txt";
                        String file3JsonPath = Os.isWindows() ? "dir2\\\\dir3\\\\file3" : "dir2/dir3/file3";
                        String response = testHttpClient.getResponse("/api/v1/exports", "200");
                        Assert.assertTrue("Response should contain file2.txt",
                                response.contains("\"path\":\"" + file2JsonPath + "\""));
                        Assert.assertTrue("Response should contain file2.txt name",
                                response.contains("\"filename\":\"file2.txt\""));
                        Assert.assertTrue("Response should contain file3",
                                response.contains("\"path\":\"" + file3JsonPath + "\""));
                        Assert.assertTrue("Response should contain file3 name",
                                response.contains("\"filename\":\"file3\""));
                        Assert.assertTrue("Response should contain file1.csv",
                                response.contains("\"path\":\"" + file1 + "\""));
                        Assert.assertTrue("Response should contain file1.csv name",
                                response.contains("\"filename\":\"" + file1 + "\""));
                        Assert.assertTrue("Response should start with {\"data\":[", response.startsWith("{\"data\":["));
                        Assert.assertTrue("Response should contain totalFiles", response.contains("\"totalFiles\":3"));
                        int fileCount = response.split("\\{\"type\":\"file\"").length - 1;
                        Assert.assertEquals("Should have 3 files", 3, fileCount);
                        testHttpClient.assertGetBinary(
                                "/api/v1/exports/file1.csv",
                                "file1 content".getBytes(),
                                "200"
                        );
                        testHttpClient.assertGetBinary(
                                "/api/v1/exports/" + file2,
                                "file2 content".getBytes(),
                                "200"
                        );
                        testHttpClient.assertGetBinary(
                                "/api/v1/exports/" + dir1,
                                "{\"errors\":[{\"status\":\"400\",\"detail\":\"cannot download directory\"}]}".getBytes(),
                                "400"
                        );
                        testHttpClient.assertGetBinary(
                                "/api/v1/exports/fileNotExists.txt",
                                "{\"errors\":[{\"status\":\"404\",\"detail\":\"file not found\"}]}".getBytes(),
                                "404"
                        );
                        testHttpClient.assertDelete(
                                "/api/v1/exports",
                                "{\"errors\":[{\"status\":\"400\",\"detail\":\"missing required file path\"}]}",
                                "400"
                        );
                        testHttpClient.assertDelete(
                                "/api/v1/exports/" + file3,
                                "{\"message\":\"file(s) deleted successfully\",\"path\":\"" + (Os.isWindows() ? "dir2\\\\dir3\\\\file3" : "dir2/dir3/file3") + "\"}",
                                "200"
                        );
                        String responseAfterDelete = testHttpClient.getResponse("/api/v1/exports", "200");
                        Assert.assertTrue("Response should contain file2.txt after deletion",
                                responseAfterDelete.contains("\"path\":\"" + file2JsonPath + "\""));
                        Assert.assertTrue("Response should contain file1.csv after deletion",
                                responseAfterDelete.contains("\"path\":\"" + file1 + "\""));
                        Assert.assertFalse("Response should not contain deleted file3",
                                responseAfterDelete.contains("\"path\":\"" + file3JsonPath + "\""));
                        int fileCountAfterDelete = responseAfterDelete.split("\"path\":").length - 1;
                        Assert.assertEquals("Should have 2 files after deletion", 2, fileCountAfterDelete);
                        testHttpClient.assertDelete(
                                "/api/v1/exports/nonExists",
                                "{\"errors\":[{\"status\":\"404\",\"detail\":\"file(s) not found\"}]}",
                                "404"
                        );
                        testHttpClient.assertDelete(
                                "/api/v1/exports/dir2",
                                "{\"message\":\"file(s) deleted successfully\",\"path\":\"dir2\"}",
                                "200"
                        );
                        testHttpClient.assertDelete(
                                "/api/v1/exports/dir2",
                                "{\"errors\":[{\"status\":\"404\",\"detail\":\"file(s) not found\"}]}",
                                "404"
                        );
                        testHttpClient.assertDelete(
                                "/api/v1/exports/.." + Files.SEPARATOR,
                                "{\"errors\":[{\"status\":\"403\",\"detail\":\"traversal not allowed in file\"}]}",
                                "403"
                        );
                    });
        });
    }

    @Test
    public void testGetAndDeleteImportFiles() throws Exception {
        String file1 = "file1.csv";
        String file2 = !Os.isWindows() ? "dir2/dir2/dir2/file2.txt" : "dir2\\dir2\\dir2\\file2.txt";
        String file3 = !Os.isWindows() ? "dir2/dir3/file3" : "dir2\\dir3\\file3";
        String dir1 = "dir2" + Files.SEPARATOR + "dir4";
        String dir2 = "dir5";
        createFile(inputRoot, file1, "file1 content".getBytes(), 1000);
        createFile(inputRoot, file2, "file2 content".getBytes(), 2000);
        createFile(inputRoot, file3, "file3 content".getBytes(), 3000);
        Path path = Path.getThreadLocal(inputRoot);
        path.concat(dir1);
        Files.mkdir(path.slash$(), configuration.getMkDirMode());
        path.trimTo(0);
        path.concat(inputRoot).concat(dir2);
        Files.mkdir(path.slash$(), configuration.getMkDirMode());

        assertMemoryLeak(() -> {
            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withWorkerCount(2)
                    .withCopyInputRoot(inputRoot)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        String file2JsonPath = Os.isWindows() ? "dir2\\\\dir2\\\\dir2\\\\file2.txt" : "dir2/dir2/dir2/file2.txt";
                        String file3JsonPath = Os.isWindows() ? "dir2\\\\dir3\\\\file3" : "dir2/dir3/file3";

                        String response = testHttpClient.getResponse("/api/v1/imports", "200");
                        Assert.assertTrue("Response should contain file2.txt",
                                response.contains("\"path\":\"" + file2JsonPath + "\""));
                        Assert.assertTrue("Response should contain file2.txt name",
                                response.contains("\"filename\":\"file2.txt\""));
                        Assert.assertTrue("Response should contain file3",
                                response.contains("\"path\":\"" + file3JsonPath + "\""));
                        Assert.assertTrue("Response should contain file3 name",
                                response.contains("\"filename\":\"file3\""));
                        Assert.assertTrue("Response should contain file1.csv",
                                response.contains("\"path\":\"" + file1 + "\""));
                        Assert.assertTrue("Response should contain file1.csv name",
                                response.contains("\"filename\":\"" + file1 + "\""));
                        Assert.assertTrue("Response should start with {\"data\":[", response.startsWith("{\"data\":["));
                        Assert.assertTrue("Response should contain totalFiles", response.contains("\"totalFiles\":3"));
                        int fileCount = response.split("\\{\"type\":\"file\"").length - 1;
                        Assert.assertEquals("Should have 3 files", 3, fileCount);

                        testHttpClient.assertGetBinary(
                                "/api/v1/imports/file1.csv",
                                "file1 content".getBytes(),
                                "200"
                        );
                        testHttpClient.assertGetBinary(
                                "/api/v1/imports/" + file2,
                                "file2 content".getBytes(),
                                "200"
                        );
                        testHttpClient.assertGetBinary(
                                "/api/v1/imports/" + dir1,
                                "{\"errors\":[{\"status\":\"400\",\"detail\":\"cannot download directory\"}]}".getBytes(),
                                "400"
                        );
                        testHttpClient.assertGetBinary(
                                "/api/v1/imports/fileNotExists.txt",
                                "{\"errors\":[{\"status\":\"404\",\"detail\":\"file not found\"}]}".getBytes(),
                                "404"
                        );
                        testHttpClient.assertDelete(
                                "/api/v1/imports",
                                "{\"errors\":[{\"status\":\"400\",\"detail\":\"missing required file path\"}]}",
                                "400"
                        );
                        testHttpClient.assertDelete(
                                "/api/v1/imports/" + file3,
                                "{\"message\":\"file(s) deleted successfully\",\"path\":\"" + (Os.isWindows() ? "dir2\\\\dir3\\\\file3" : "dir2/dir3/file3") + "\"}",
                                "200"
                        );

                        String responseAfterDelete = testHttpClient.getResponse("/api/v1/imports", "200");
                        Assert.assertTrue("Response should contain file2.txt after deletion",
                                responseAfterDelete.contains("\"path\":\"" + file2JsonPath + "\""));
                        Assert.assertTrue("Response should contain file1.csv after deletion",
                                responseAfterDelete.contains("\"path\":\"" + file1 + "\""));
                        Assert.assertFalse("Response should not contain deleted file3",
                                responseAfterDelete.contains("\"path\":\"" + file3JsonPath + "\""));
                        int fileCountAfterDelete = responseAfterDelete.split("\"path\":").length - 1;
                        Assert.assertEquals("Should have 2 files after deletion", 2, fileCountAfterDelete);
                        testHttpClient.assertDelete(
                                "/api/v1/imports/nonExists",
                                "{\"errors\":[{\"status\":\"404\",\"detail\":\"file(s) not found\"}]}",
                                "404"
                        );
                        testHttpClient.assertDelete(
                                "/api/v1/imports/dir2",
                                "{\"message\":\"file(s) deleted successfully\",\"path\":\"dir2\"}",
                                "200"
                        );
                        testHttpClient.assertDelete(
                                "/api/v1/imports/dir2",
                                "{\"errors\":[{\"status\":\"404\",\"detail\":\"file(s) not found\"}]}",
                                "404"
                        );
                        testHttpClient.assertDelete(
                                "/api/v1/imports/.." + Files.SEPARATOR,
                                "{\"errors\":[{\"status\":\"403\",\"detail\":\"traversal not allowed in file\"}]}",
                                "403"
                        );
                    });
        });
    }

    @Test
    public void testImportAbsPath() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) id from long_sequence(10))");
            byte[] parquetData = createParquetFile("x");
            String fileName = Os.isWindows() ? root + "\\test.parquet" : "/test.parquet";
            byte[] parquetImportRequest = createMultipleParquetImportRequest(new String[]{fileName}, new byte[][]{parquetData}, false);

            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withCopyInputRoot(inputRoot)
                    .withWorkerCount(1)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        new SendAndReceiveRequestBuilder()
                                .execute(
                                        parquetImportRequest,
                                        "HTTP/1.1 403 Forbidden\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: application/vnd.api+json\r\n" +
                                                "\r\n" +
                                                "a7\r\n" +
                                                "{\"errors\":[{\"status\":\"403\",\"title\":\"File Upload Error\",\"detail\":\"path traversal not allowed in filename\"}],\"meta\":{\"totalFiles\":1,\"successfulFiles\":0,\"failedFiles\":1}}\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                    });
        });
    }

    @Test
    public void testImportBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " cast(x as int) id," +
                    " rnd_str(4,4,4,2) as name," +
                    " rnd_double() as price," +
                    " rnd_timestamp('2023-01-01','2023-12-31',2) as timestamp," +
                    " rnd_symbol(4,4,4,2) as category" +
                    " from long_sequence(1000))");
            byte[] parquetData = createParquetFile("x");
            byte[] parquetImportRequest = createMultipleParquetImportRequest(new String[]{"test.parquet"}, new byte[][]{parquetData}, false);

            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withCopyInputRoot(inputRoot)
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
                                                "Content-Type: application/vnd.api+json\r\n" +
                                                "\r\n" +
                                                "83\r\n" +
                                                "{\"data\":[{\"type\":\"file\",\"id\":\"test.parquet\",\"attributes\":{\"filename\":\"test.parquet\",\"status\":\"201\"}}],\"meta\":{\"totalFiles\":1}}\r\n" +
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
    public void testImportLargeFileAndGet() throws Exception {
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
            byte[] parquetImportRequest = createMultipleParquetImportRequest(new String[]{fileName}, new byte[][]{parquetData}, false);
            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withWorkerCount(2)
                    .withCopyInputRoot(inputRoot)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        new SendAndReceiveRequestBuilder()
                                .execute(
                                        parquetImportRequest,
                                        "HTTP/1.1 201 CREATED\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: application/vnd.api+json\r\n" +
                                                "\r\n" +
                                                (Os.isWindows() ? "a5\r\n" : "a1\r\n") +
                                                (Os.isWindows() ? "{\"data\":[{\"type\":\"file\",\"id\":\"dir\\\\dir1\\\\large_test.parquet\",\"attributes\":{\"filename\":\"dir\\\\dir1\\\\large_test.parquet\",\"status\":\"201\"}}],\"meta\":{\"totalFiles\":1}}\r\n" : "{\"data\":[{\"type\":\"file\",\"id\":\"dir/dir1/large_test.parquet\",\"attributes\":{\"filename\":\"dir/dir1/large_test.parquet\",\"status\":\"201\"}}],\"meta\":{\"totalFiles\":1}}\r\n") +
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

                        // testFileGet
                        testHttpClient.assertGetBinary(
                                "/api/v1/imports/" + fileName,
                                parquetData,
                                "200"
                        );
                        testHttpClient.assertGetBinary(
                                "/api/v1/imports/dir111",
                                "{\"errors\":[{\"status\":\"404\",\"detail\":\"file not found\"}]}".getBytes(),
                                "404"
                        );
                        testHttpClient.assertGetBinary(
                                "/api/v1/imports/dir",
                                "{\"errors\":[{\"status\":\"400\",\"detail\":\"cannot download directory\"}]}".getBytes(),
                                "400"
                        );
                    });
        });
    }

    @Test
    public void testImportRelativePath() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) id from long_sequence(10))");
            byte[] parquetData = createParquetFile("x");
            String fileName = Os.isWindows() ? "..\\test.parquet" : "../test.parquet";
            byte[] parquetImportRequest = createMultipleParquetImportRequest(new String[]{fileName}, new byte[][]{parquetData}, false);

            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withCopyInputRoot(inputRoot)
                    .withWorkerCount(1)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        new SendAndReceiveRequestBuilder()
                                .execute(
                                        parquetImportRequest,
                                        "HTTP/1.1 403 Forbidden\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: application/vnd.api+json\r\n" +
                                                "\r\n" +
                                                "a7\r\n" +
                                                "{\"errors\":[{\"status\":\"403\",\"title\":\"File Upload Error\",\"detail\":\"path traversal not allowed in filename\"}],\"meta\":{\"totalFiles\":1,\"successfulFiles\":0,\"failedFiles\":1}}\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                    });
        });
    }

    @Test
    public void testImportWithoutSqlCopyInputRoot() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) id from long_sequence(10))");
            byte[] parquetData = createParquetFile("x");

            byte[] parquetImportRequest = createMultipleParquetImportRequest(new String[]{"test.parquet"}, new byte[][]{parquetData}, false);

            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withWorkerCount(1)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        new SendAndReceiveRequestBuilder()
                                .execute(
                                        parquetImportRequest,
                                        "HTTP/1.1 400 Bad request\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: application/vnd.api+json\r\n" +
                                                "\r\n" +
                                                "a6\r\n" +
                                                "{\"errors\":[{\"status\":\"400\",\"title\":\"File Upload Error\",\"detail\":\"sql.copy.input.root is not configured\"}],\"meta\":{\"totalFiles\":1,\"successfulFiles\":0,\"failedFiles\":1}}\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                        testHttpClient.assertGetBinary(
                                "/api/v1/imports",
                                "{\"errors\":[{\"status\":\"400\",\"detail\":\"sql.copy.input.root is not configured\"}]}".getBytes(),
                                "400"
                        );
                    });
        });
    }

    @Test
    public void testListImportFiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select cast(x as int) id from long_sequence(10))");
            byte[] parquetData = createParquetFile("x");
            String[] fileNames = Os.isWindows() ?
                    new String[]{"x1.parquet", "x2.parquet", "dir1\\x3.parquet", "dir1\\dir2\\x4.parquet", "dir3\\special.parquet"} :
                    new String[]{"x1.parquet", "x2.parquet", "dir1/x3.parquet", "dir1/dir2/x4.parquet", "dir3/abc.parquet"};
            byte[] parquetImportRequest = createMultipleParquetImportRequest(fileNames, new byte[][]{parquetData, parquetData, parquetData, parquetData, parquetData}, false);

            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withCopyInputRoot(inputRoot)
                    .withWorkerCount(1)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        new SendAndReceiveRequestBuilder()
                                .execute(
                                        parquetImportRequest,
                                        "HTTP/1.1 201 Created\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: application/vnd.api+json\r\n" +
                                                "\r\n" +
                                                (Os.isWindows() ? "022d\r\n" : "0204\r\n") +
                                                (Os.isWindows() ? "{\"data\":[{\"type\":\"file\",\"id\":\"x1.parquet\",\"attributes\":{\"filename\":\"x1.parquet\",\"status\":\"201\"}},{\"type\":\"file\",\"id\":\"x2.parquet\",\"attributes\":{\"filename\":\"x2.parquet\",\"status\":\"201\"}},{\"type\":\"file\",\"id\":\"dir1\\\\x3.parquet\",\"attributes\":{\"filename\":\"dir1\\\\x3.parquet\",\"status\":\"201\"}},{\"type\":\"file\",\"id\":\"dir1\\\\dir2\\\\x4.parquet\",\"attributes\":{\"filename\":\"dir1\\\\dir2\\\\x4.parquet\",\"status\":\"201\"}},{\"type\":\"file\",\"id\":\"dir3\\\\special.parquet\",\"attributes\":{\"filename\":\"dir3\\\\special.parquet\",\"status\":\"201\"}}],\"meta\":{\"totalFiles\":5}}\r\n" : "{\"data\":[{\"type\":\"file\",\"id\":\"x1.parquet\",\"attributes\":{\"filename\":\"x1.parquet\",\"status\":\"201\"}},{\"type\":\"file\",\"id\":\"x2.parquet\",\"attributes\":{\"filename\":\"x2.parquet\",\"status\":\"201\"}},{\"type\":\"file\",\"id\":\"dir1/x3.parquet\",\"attributes\":{\"filename\":\"dir1/x3.parquet\",\"status\":\"201\"}},{\"type\":\"file\",\"id\":\"dir1/dir2/x4.parquet\",\"attributes\":{\"filename\":\"dir1/dir2/x4.parquet\",\"status\":\"201\"}},{\"type\":\"file\",\"id\":\"dir3/abc.parquet\",\"attributes\":{\"filename\":\"dir3/abc.parquet\",\"status\":\"201\"}}],\"meta\":{\"totalFiles\":5}}\r\n") +
                                                "00\r\n" +
                                                "\r\n"
                                );
                        String response = testHttpClient.getResponse("/api/v1/imports", "200");
                        LOG.info().$("========= Import files response=========: ").$(response).$();
                        final String jsonSep = Os.isWindows() ? "\\\\" : "/";
                        String expectedSpecialFile = Os.isWindows() ? "special.parquet" : "abc.parquet";
                        Assert.assertTrue("Response should contain dir3" + Files.SEPARATOR + expectedSpecialFile,
                                response.contains("\"path\":\"dir3" + jsonSep + expectedSpecialFile + "\""));
                        Assert.assertTrue("Response should contain dir3" + Files.SEPARATOR + expectedSpecialFile + " name",
                                response.contains("\"filename\":\"" + expectedSpecialFile + "\""));
                        Assert.assertTrue("Response should contain x2.parquet",
                                response.contains("\"path\":\"x2.parquet\""));
                        Assert.assertTrue("Response should contain x1.parquet",
                                response.contains("\"path\":\"x1.parquet\""));
                        Assert.assertTrue("Response should contain dir1" + Files.SEPARATOR + "dir2" + Files.SEPARATOR + "x4.parquet",
                                response.contains("\"path\":\"dir1" + jsonSep + "dir2" + jsonSep + "x4.parquet\""));
                        Assert.assertTrue("Response should contain dir1" + Files.SEPARATOR + "x3.parquet",
                                response.contains("\"path\":\"dir1" + jsonSep + "x3.parquet\""));
                        Assert.assertTrue("Response should contain x.parquet",
                                response.contains("\"path\":\"x.parquet\""));
                        Assert.assertTrue("Response should start with {\"data\":[", response.startsWith("{\"data\":["));
                        Assert.assertTrue("Response should contain totalFiles", response.contains("\"totalFiles\":6"));
                        int fileCount = response.split("\\{\"type\":\"file\"").length - 1;
                        Assert.assertEquals("Should have 6 files", 6, fileCount);
                    });
        });
    }

    @Test
    public void testMultiFilesImport() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " cast(x as int) id," +
                    " rnd_str(4,4,4,2) as name," +
                    " rnd_double() as price," +
                    " rnd_timestamp('2023-01-01','2023-12-31',2) as timestamp," +
                    " rnd_symbol(4,4,4,2) as category" +
                    " from long_sequence(10))");
            byte[] parquetData = createParquetFile("x");
            execute("create table y as (select" +
                    " cast(x as int) id," +
                    " rnd_str(4,4,4,2) as name," +
                    " rnd_double() as price," +
                    " rnd_timestamp('2023-01-01','2023-12-31',2) as timestamp," +
                    " rnd_symbol(4,4,4,2) as category" +
                    " from long_sequence(5))");
            byte[] parquetData1 = createParquetFile("y");
            byte[] parquetImportRequest = createMultipleParquetImportRequest(new String[]{"xx.parquet", "yy.parquet"}, new byte[][]{parquetData, parquetData1}, false);

            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withCopyInputRoot(inputRoot)
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
                                                "Content-Type: application/vnd.api+json\r\n" +
                                                "\r\n" +
                                                "dc\r\n" +
                                                "{\"data\":[{\"type\":\"file\",\"id\":\"xx.parquet\",\"attributes\":{\"filename\":\"xx.parquet\",\"status\":\"201\"}},{\"type\":\"file\",\"id\":\"yy.parquet\",\"attributes\":{\"filename\":\"yy.parquet\",\"status\":\"201\"}}],\"meta\":{\"totalFiles\":2}}\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                        assertQueryNoLeakCheck("id\tname\tprice\ttimestamp\tcategory\n" +
                                        "9\tPEHN\t0.0011075361080621349\t2023-11-22T22:30:14.635691Z\t\n" +
                                        "10\t\t0.8001121139739173\t2023-03-25T05:30:24.048489Z\tSXUX\n",
                                "select * from read_parquet('xx.parquet') limit -2", null, null, true, true);
                        assertQueryNoLeakCheck("id\tname\tprice\ttimestamp\tcategory\n" +
                                        "4\tEKGH\t0.8940917126581895\t\tDOTS\n" +
                                        "5\t\t0.6806873134626418\t2023-05-24T09:33:20.501727Z\t\n",
                                "select * from read_parquet('yy.parquet') limit -2", null, null, true, true);
                    });
        });
    }

    @Test
    public void testMultiFilesPartFail() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " cast(x as int) id," +
                    " rnd_str(4,4,4,2) as name," +
                    " rnd_double() as price," +
                    " rnd_timestamp('2023-01-01','2023-12-31',2) as timestamp," +
                    " rnd_symbol(4,4,4,2) as category" +
                    " from long_sequence(10))");
            byte[] parquetData = createParquetFile("x");
            execute("create table y as (select" +
                    " cast(x as int) id," +
                    " rnd_str(4,4,4,2) as name," +
                    " rnd_double() as price," +
                    " rnd_timestamp('2023-01-01','2023-12-31',2) as timestamp," +
                    " rnd_symbol(4,4,4,2) as category" +
                    " from long_sequence(5))");
            byte[] parquetData1 = createParquetFile("y");
            byte[] parquetImportRequest = createMultipleParquetImportRequest(new String[]{"xx.parquet", "y.parquet"}, new byte[][]{parquetData, parquetData1}, false);

            new HttpQueryTestBuilder()
                    .withTempFolder(root)
                    .withCopyInputRoot(inputRoot)
                    .withWorkerCount(1)
                    .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                    .withTelemetry(false)
                    .run((engine, sqlExecutionContext) -> {
                        new SendAndReceiveRequestBuilder()
                                .execute(
                                        parquetImportRequest,
                                        "HTTP/1.1 207 Multi status\r\n" +
                                                "Server: questDB/1.0\r\n" +
                                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                                "Transfer-Encoding: chunked\r\n" +
                                                "Content-Type: application/vnd.api+json\r\n" +
                                                "\r\n" +
                                                "0131\r\n" +
                                                "{\"data\":[{\"type\":\"file\",\"id\":\"xx.parquet\",\"attributes\":{\"filename\":\"xx.parquet\",\"status\":\"201\"}}],\"errors\":[{\"status\":\"409\",\"title\":\"File Upload Error\",\"detail\":\"file already exists and overwriting is disabled\",\"meta\":{\"filename\":\"y.parquet\"}}],\"meta\":{\"totalFiles\":2,\"successfulFiles\":1,\"failedFiles\":1}}\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                    });
        });
    }

    private void createFile(CharSequence rootPath, CharSequence filePath, byte[] data, long modifyTime) throws Exception {
        Path path = Path.getThreadLocal(rootPath);
        long fd = -1;
        long addr = -1;
        try {
            addr = Unsafe.malloc(data.length, MemoryTag.NATIVE_DEFAULT);
            for (int i = 0; i < data.length; i++) {
                Unsafe.getUnsafe().putByte(addr + i, data[i]);
            }
            path.concat(filePath);
            Files.mkdirs(path, engine.getConfiguration().getMkDirMode());
            fd = Files.openRW(path.$());
            Files.write(fd, addr, data.length, 0);
            Files.setLastModified(path.$(), modifyTime);
        } finally {
            if (fd != -1) {
                Files.close(fd);
            }
            if (addr != -1) {
                Unsafe.free(addr, data.length, MemoryTag.NATIVE_DEFAULT);
            }
            path.close();
        }
    }

    private byte[] createMultipleParquetImportRequest(String[] filenames, byte[][] parquetDataArray, boolean overwrite) {
        String boundary = "------------------------boundary123";
        String header = "POST /api/v1/imports?overwrite=" + overwrite;
        header += " HTTP/1.1\r\n" +
                "Host: localhost:9001\r\n" +
                "User-Agent: curl/7.64.0\r\n" +
                "Accept: */*\r\n" +
                "Content-Type: multipart/form-data; boundary=" + boundary + "\r\n" +
                "\r\n";

        StringSink bodySink = new StringSink();
        int totalDataLength = 0;

        for (int i = 0; i < filenames.length; i++) {
            bodySink.put("--").put(boundary).put("\r\n");
            bodySink.put("Content-Disposition: form-data; name=\"").put(filenames[i]).put("\"\r\n");
            bodySink.put("Content-Type: application/octet-stream\r\n");
            bodySink.put("\r\n");
            totalDataLength += parquetDataArray[i].length;
            if (i < filenames.length - 1) {
                bodySink.put("\r\n");
            }
        }

        String footer = "\r\n--" + boundary + "--";
        String bodyText = bodySink.toString();

        byte[] headerBytes = header.getBytes();
        byte[] bodyBytes = bodyText.getBytes();
        byte[] footerBytes = footer.getBytes();

        int bodyLength = bodyBytes.length + totalDataLength + footerBytes.length;
        header = header.replace("Content-Type:", "Content-Length: " + bodyLength + "\r\nContent-Type:");
        headerBytes = header.getBytes();

        byte[] requestBytes = new byte[headerBytes.length + bodyBytes.length + totalDataLength + footerBytes.length];
        int offset = 0;

        System.arraycopy(headerBytes, 0, requestBytes, offset, headerBytes.length);
        offset += headerBytes.length;

        for (int i = 0; i < filenames.length; i++) {
            String partHeader = "--" + boundary + "\r\n" +
                    "Content-Disposition: form-data; name=\"" + filenames[i] + "\"\r\n" +
                    "Content-Type: application/octet-stream\r\n" +
                    "\r\n";
            byte[] partHeaderBytes = partHeader.getBytes();
            System.arraycopy(partHeaderBytes, 0, requestBytes, offset, partHeaderBytes.length);
            offset += partHeaderBytes.length;

            System.arraycopy(parquetDataArray[i], 0, requestBytes, offset, parquetDataArray[i].length);
            offset += parquetDataArray[i].length;

            if (i < filenames.length - 1) {
                byte[] separator = "\r\n".getBytes();
                System.arraycopy(separator, 0, requestBytes, offset, separator.length);
                offset += separator.length;
            }
        }

        System.arraycopy(footerBytes, 0, requestBytes, offset, footerBytes.length);
        return requestBytes;
    }

    private byte[] createParquetFile(String tableName) throws Exception {
        try (
                Path path = new Path();
                PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader(tableName)
        ) {
            path.of(inputRoot).concat(tableName).put(".parquet");
            Files.mkdirs(path, engine.getConfiguration().getMkDirMode());
            PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
            PartitionEncoder.encode(partitionDescriptor, path);
            Assert.assertTrue("Parquet file should exist", Files.exists(path.$()));
            long fd = Files.openRO(path.$());
            int len = (int) Files.length(fd);
            long addr = Files.mmap(fd, len, 0, Files.MAP_RO, MemoryTag.MMAP_IMPORT);
            try {
                byte[] parquetData = new byte[len];
                for (int i = 0; i < len; i++) {
                    parquetData[i] = Unsafe.getUnsafe().getByte(addr + i);
                }
                return parquetData;
            } finally {
                Files.close(fd);
                Files.munmap(addr, len, MemoryTag.MMAP_IMPORT);
            }
        }
    }
}