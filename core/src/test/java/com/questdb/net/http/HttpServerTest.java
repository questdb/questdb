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

package com.questdb.net.http;

import com.questdb.BootstrapEnv;
import com.questdb.ServerConfiguration;
import com.questdb.ex.ResponseContentBufferTooSmallException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.net.ha.AbstractJournalTest;
import com.questdb.net.http.handlers.ImportHandler;
import com.questdb.net.http.handlers.StaticContentHandler;
import com.questdb.net.http.handlers.UploadHandler;
import com.questdb.parser.sql.QueryCompiler;
import com.questdb.parser.typeprobe.TypeProbeCollection;
import com.questdb.ql.RecordSource;
import com.questdb.ql.RecordSourcePrinter;
import com.questdb.std.ByteBuffers;
import com.questdb.std.Chars;
import com.questdb.std.Misc;
import com.questdb.std.Numbers;
import com.questdb.std.str.StringSink;
import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.std.time.DateLocaleFactory;
import com.questdb.store.ColumnType;
import com.questdb.store.Journal;
import com.questdb.store.JournalEntryWriter;
import com.questdb.store.JournalWriter;
import com.questdb.store.factory.FactoryEventListener;
import com.questdb.store.factory.WriterFactory;
import com.questdb.store.factory.configuration.JournalStructure;
import com.questdb.store.util.FileSink;
import com.questdb.test.tools.HttpTestUtils;
import com.questdb.test.tools.TestUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.net.ssl.SSLContext;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class HttpServerTest extends AbstractJournalTest {

    private final static Log LOG = LogFactory.getLog(HttpServerTest.class);
    private final static TypeProbeCollection TYPE_PROBE_COLLECTION = new TypeProbeCollection();
    private final static DateFormatFactory DATE_FORMAT_FACTORY = new DateFormatFactory();

    private final static String request = "GET /imp?x=1&z=2 HTTP/1.1\r\n" +
            "Host: localhost:80\r\n" +
            "Connection: keep-alive\r\n" +
            "Cache-Control: max-age=0\r\n" +
            "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
            "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
            "Accept-Language: en-US,en;q=0.8\r\n" +
            "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
            "\r\n";
    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    public static HttpResponse get(String url) throws IOException {
        HttpGet post = new HttpGet(url);
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            return client.execute(post);
        }
    }

    @Test
    public void testCompressedDownload() throws Exception {
        final ServerConfiguration configuration = new ServerConfiguration(new File(resourceFile("/site"), "conf/questdb.conf"));
        configuration.getSslConfig().setSecure(true);
        configuration.getSslConfig().setKeyStore(new FileInputStream(resourceFile("/keystore/singlekey.ks")), "changeit");

        BootstrapEnv env = new BootstrapEnv();
        env.configuration = configuration;
        env.matcher = new SimpleUrlMatcher() {{
            setDefaultHandler(new StaticContentHandler(env));
        }};

        HttpServer server = new HttpServer(env);
        server.start();
        try {
            HttpTestUtils.download(clientBuilder(true), "https://localhost:9000/upload.html", new File(temp.getRoot(), "upload.html"));
        } finally {
            server.halt();
        }
    }

    @Test
    public void testConcurrentImport() throws Exception {
        final BootstrapEnv env = new BootstrapEnv();
        env.configuration = new ServerConfiguration();
        env.factory = getFactory();
        env.typeProbeCollection = TYPE_PROBE_COLLECTION;
        env.matcher = new SimpleUrlMatcher() {{
            put("/imp", new ImportHandler(env));
        }};

        HttpServer server = new HttpServer(env);
        server.start();

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch latch = new CountDownLatch(2);
        try {

            new Thread(() -> {
                try {
                    barrier.await();
                    Assert.assertEquals(200, HttpTestUtils.upload("/csv/test-import.csv", "http://localhost:9000/imp", null, null));
                    latch.countDown();
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                }
            }).start();

            new Thread(() -> {
                try {
                    barrier.await();
                    Assert.assertEquals(200, HttpTestUtils.upload("/csv/test-import-nan.csv", "http://localhost:9000/imp", null, null));
                    latch.countDown();
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                }
            }).start();

            latch.await();

            try (Journal r = getFactory().reader("test-import.csv")) {
                Assert.assertEquals("First failed", 129, r.size());
            }

            try (Journal r = getFactory().reader("test-import-nan.csv")) {
                Assert.assertEquals("Second failed", 129, r.size());
            }
        } finally {
            server.halt();
        }
    }

    @Test
    public void testConnectionCount() throws Exception {
        final BootstrapEnv env = new BootstrapEnv();
        env.configuration = new ServerConfiguration(new File(resourceFile("/site"), "conf/questdb.conf"));
        env.configuration.setHttpMaxConnections(10);
        env.matcher = new SimpleUrlMatcher() {{
            setDefaultHandler(new StaticContentHandler(env));
        }};

        HttpServer server = new HttpServer(env);
        server.start();

        try {
            CloseableHttpClient c1 = clientBuilder(true).build();
            CloseableHttpClient c2 = clientBuilder(true).build();
            CloseableHttpClient c3 = clientBuilder(true).build();
            CloseableHttpClient c4 = clientBuilder(true).build();
            Assert.assertNotNull(c1.execute(new HttpGet("https://localhost:9000/upload.html")));
            Assert.assertEquals(1, server.getConnectionCount());

            Assert.assertNotNull(c2.execute(new HttpGet("https://localhost:9000/upload.html")));
            Assert.assertEquals(2, server.getConnectionCount());

            Assert.assertNotNull(c3.execute(new HttpGet("https://localhost:9000/upload.html")));
            Assert.assertEquals(3, server.getConnectionCount());

            Assert.assertNotNull(c4.execute(new HttpGet("https://localhost:9000/upload.html")));
            Assert.assertEquals(4, server.getConnectionCount());

            c1.close();
            c2.close();
            Thread.sleep(300);
            Assert.assertEquals(2, server.getConnectionCount());

            c3.close();
            c4.close();
            Thread.sleep(300);
            Assert.assertEquals(0, server.getConnectionCount());
        } finally {
            server.halt();
        }
    }

    @Test
    public void testDefaultDirIndex() throws Exception {
        final BootstrapEnv env = new BootstrapEnv();
        env.configuration = new ServerConfiguration(new File(resourceFile("/site"), "conf/questdb.conf"));
        env.configuration.getSslConfig().setSecure(false);
        env.typeProbeCollection = TYPE_PROBE_COLLECTION;
        env.matcher = new SimpleUrlMatcher() {{
            setDefaultHandler(new StaticContentHandler(env));
        }};

        HttpServer server = new HttpServer(env);

        server.start();
        try {
            HttpTestUtils.download(clientBuilder(false), "http://localhost:9000/", new File(temp.getRoot(), "index.html"));
        } finally {
            server.halt();
        }
    }

    @Test
    public void testFragmentedUrl() throws Exception {
        final long tick = DateFormatUtils.parseDateTime("2015-12-05T13:30:00.000Z");
        final BootstrapEnv env = new BootstrapEnv();
        env.configuration = new ServerConfiguration();
        env.matcher = new SimpleUrlMatcher();

        HttpServer server = new HttpServer(env);
        server.setClock(() -> tick);
        server.start();

        try (SocketChannel channel = openChannel()) {
            ByteBuffer buf = ByteBuffer.allocate(1024);
            ByteBuffer out = ByteBuffer.allocate(1024);

            int n = 5;
            for (int i = 0; i < n; i++) {
                buf.put((byte) request.charAt(i));
            }
            buf.flip();
            ByteBuffers.copy(buf, channel);

            buf.clear();
            for (int i = n; i < request.length(); i++) {
                buf.put((byte) request.charAt(i));
            }
            buf.flip();
            ByteBuffers.copy(buf, channel);

            channel.configureBlocking(false);

            ByteBuffers.copyGreedyNonBlocking(channel, out, 100000);

            final String expected = "HTTP/1.1 404 Not Found\r\n" +
                    "Server: questDB/1.0\r\n" +
                    "Date: Sat, 5 Dec 2015 13:30:00 GMT\r\n" +
                    "Transfer-Encoding: chunked\r\n" +
                    "Content-Type: text/html; charset=utf-8\r\n" +
                    "\r\n" +
                    "b\r\n" +
                    "Not Found\r\n" +
                    "\r\n" +
                    "0\r\n" +
                    "\r\n";

            Assert.assertEquals(expected.length(), out.remaining());

            for (int i = 0, k = expected.length(); i < k; i++) {
                Assert.assertEquals(expected.charAt(i), out.get());
            }
        } finally {
            server.halt();
        }
    }

    @Test
    public void testIdleTimeout() throws Exception {
        final BootstrapEnv env = new BootstrapEnv();
        env.configuration = new ServerConfiguration(new File(resourceFile("/site"), "conf/questdb.conf"));
        // 500ms timeout
        env.configuration.setHttpTimeout(500);
        env.configuration.getSslConfig().setSecure(false);
        env.matcher = new SimpleUrlMatcher() {{
            setDefaultHandler(new StaticContentHandler(env));
        }};

        HttpServer server = new HttpServer(env);
        server.start();

        try {
            Socket socket = new Socket("127.0.0.1", 9000);
            OutputStream os = socket.getOutputStream();
            Thread.sleep(600);

            try {
                os.write(request.getBytes());

                for (int i = 0; i < 4096; i++) {
                    os.write('c');
                }
                os.flush();

                Assert.fail("Expected exception due to connection timeout");
            } catch (SocketException ignored) {

            }
        } finally {
            server.halt();
        }
    }

    @Test
    public void testImportAppend() throws Exception {

        final AtomicInteger errors = new AtomicInteger();
        getFactory().setEventListener((factoryType, thread, name, event, segment, position) -> {
            if (event == FactoryEventListener.EV_UNEXPECTED_CLOSE) {
                errors.incrementAndGet();
            }
            return true;
        });

        BootstrapEnv env = new BootstrapEnv();
        env.configuration = new ServerConfiguration();
        env.factory = getFactory();
        env.typeProbeCollection = TYPE_PROBE_COLLECTION;
        env.matcher = new SimpleUrlMatcher() {{
            put("/imp", new ImportHandler(env));
        }};

        HttpServer server = new HttpServer(env);
        server.start();

        try {
            Assert.assertEquals(200, HttpTestUtils.upload("/csv/test-import.csv", "http://localhost:9000/imp?fmt=json", null, null));
            Assert.assertEquals(200, HttpTestUtils.upload("/csv/test-import.csv", "http://localhost:9000/imp", null, null));
            StringSink sink = new StringSink();
            RecordSourcePrinter printer = new RecordSourcePrinter(sink);
            QueryCompiler qc = new QueryCompiler(env);
            printer.print(qc.compile(getFactory(), "select count(StrSym), count(IntSym), count(IntCol), count(long), count() from 'test-import.csv'"), getFactory());
            TestUtils.assertEquals("252\t252\t256\t258\t258\n", sink);
        } finally {
            server.halt();
        }

        Assert.assertEquals(0, errors.get());
    }

    @Test
    public void testImportForcedHeader() throws Exception {

        BootstrapEnv env = new BootstrapEnv();
        env.configuration = new ServerConfiguration();
        env.factory = getFactory();
        env.typeProbeCollection = TYPE_PROBE_COLLECTION;
        env.matcher = new SimpleUrlMatcher() {{
            put("/imp", new ImportHandler(env));
        }};
        HttpServer server = new HttpServer(env);
        server.start();

        try {
            StringSink sink = new StringSink();
            RecordSourcePrinter printer = new RecordSourcePrinter(sink);
            QueryCompiler qc = new QueryCompiler(env);

            Assert.assertEquals(200, HttpTestUtils.upload("/csv/test-explicit-headers.csv", "http://localhost:9000/imp?name=test-import.csv&overwrite=true&durable=true&forceHeader=true", null, null));
            printer.print(qc.compile(getFactory(), "select count() from 'test-import.csv'"), getFactory());
            // expect first line to be treated as header
            TestUtils.assertEquals("2\n", sink);
        } finally {
            server.halt();
        }
    }

    @Test
    public void testImportIntoBusyJournal() throws Exception {
        try (JournalWriter ignored = getFactory().writer(new JournalStructure("test-import.csv").$int("x").$())) {
            BootstrapEnv env = new BootstrapEnv();
            env.configuration = new ServerConfiguration();
            env.factory = getFactory();
            env.typeProbeCollection = TYPE_PROBE_COLLECTION;
            env.matcher = new SimpleUrlMatcher() {{
                put("/imp", new ImportHandler(env));
            }};
            HttpServer server = new HttpServer(env);
            server.start();

            StringBuilder response = new StringBuilder();
            try {
                Assert.assertEquals(200, HttpTestUtils.upload("/csv/test-import.csv", "http://localhost:9000/imp?fmt=json", null, response));
                TestUtils.assertEquals("{\"status\":\"Journal exists and column count does not match\"}", response);
            } catch (IOException e) {
                Assert.assertTrue(e.getMessage().contains("Connection reset"));
            } finally {
                server.halt();
            }
        }
    }

    @Test
    public void testImportIntoBusyJournal2() throws Exception {
        WriterFactory f = getFactory();

        try (JournalWriter w = f.writer(new JournalStructure("small.csv").$int("X").$int("Y").$())) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putInt(0, 3);
            ew.putInt(1, 30);
            ew.append();
            w.commit();

            BootstrapEnv env = new BootstrapEnv();
            env.configuration = new ServerConfiguration();
            env.factory = getFactory();
            env.typeProbeCollection = TYPE_PROBE_COLLECTION;
            env.matcher = new SimpleUrlMatcher() {{
                put("/imp", new ImportHandler(env));
            }};

            HttpServer server = new HttpServer(env);
            server.start();

            StringBuilder response = new StringBuilder();
            try {
                Assert.assertEquals(200, HttpTestUtils.upload("/csv/small.csv", "http://localhost:9000/imp?fmt=json", null, response));
                Assert.assertTrue(Chars.startsWith(response, "{\"status\":\"com.questdb.ex.WriterBusyException\"}"));
            } catch (IOException e) {
                Assert.assertTrue(e.getMessage().contains("Connection reset"));
            } finally {
                server.halt();
            }
        }
    }

    @Test
    public void testImportNoHeader() throws Exception {
        final ServerConfiguration configuration = new ServerConfiguration();
        BootstrapEnv env = new BootstrapEnv();
        env.configuration = configuration;
        env.factory = getFactory();
        env.typeProbeCollection = TYPE_PROBE_COLLECTION;
        env.matcher = new SimpleUrlMatcher() {{
            put("/imp", new ImportHandler(env));
        }};

        HttpServer server = new HttpServer(env);
        server.start();

        try {
            StringSink sink = new StringSink();
            RecordSourcePrinter printer = new RecordSourcePrinter(sink);
            QueryCompiler qc = new QueryCompiler(env);

            Assert.assertEquals(200, HttpTestUtils.upload("/csv/test-explicit-headers.csv", "http://localhost:9000/imp?name=test-import.csv&overwrite=true&durable=true", null, null));
            printer.print(qc.compile(getFactory(), "select count() from 'test-import.csv'"), getFactory());
            TestUtils.assertEquals("3\n", sink);
        } finally {
            server.halt();
        }
    }

    @Test
    public void testImportNumberPrefixedColumn() throws Exception {
        final ServerConfiguration configuration = new ServerConfiguration();
        BootstrapEnv env = new BootstrapEnv();
        env.configuration = configuration;
        env.factory = getFactory();
        env.typeProbeCollection = TYPE_PROBE_COLLECTION;
        env.matcher = new SimpleUrlMatcher() {{
            put("/imp", new ImportHandler(env));
        }};

        HttpServer server = new HttpServer(env);
        server.start();

        try {
            Assert.assertEquals(200, HttpTestUtils.upload("/csv/test-import-num-prefix.csv", "http://localhost:9000/imp?fmt=json", null, null));
            StringSink sink = new StringSink();
            RecordSourcePrinter printer = new RecordSourcePrinter(sink);
            QueryCompiler qc = new QueryCompiler(env);
            try (RecordSource rs = qc.compile(env.factory, "select count(StrSym), count(IntSym), count(_1IntCol), count(long), count() from 'test-import-num-prefix.csv'")) {
                printer.print(rs, env.factory);
            }
            TestUtils.assertEquals("126\t126\t128\t129\t129\n", sink);
        } finally {
            server.halt();
        }
    }

    @Test
    public void testImportNumericHeader() throws Exception {
        BootstrapEnv env = new BootstrapEnv();
        env.configuration = new ServerConfiguration();
        env.factory = getFactory();
        env.typeProbeCollection = TYPE_PROBE_COLLECTION;
        env.matcher = new SimpleUrlMatcher() {{
            put("/imp", new ImportHandler(env));
        }};

        HttpServer server = new HttpServer(env);
        server.start();

        try {
            StringSink sink = new StringSink();
            RecordSourcePrinter printer = new RecordSourcePrinter(sink);
            QueryCompiler qc = new QueryCompiler(env);

            Assert.assertEquals(200, HttpTestUtils.upload("/csv/test-numeric-headers.csv", "http://localhost:9000/imp?name=test-import.csv&overwrite=true&durable=true&forceHeader=true", null, null));
            printer.print(qc.compile(getFactory(), "select count() from 'test-import.csv'"), getFactory());
            // expect first line to be treated as header
            TestUtils.assertEquals("2\n", sink);
        } finally {
            server.halt();
        }
    }

    @Test
    public void testImportOverwrite() throws Exception {
        final ServerConfiguration configuration = new ServerConfiguration();
        BootstrapEnv env = new BootstrapEnv();
        env.configuration = configuration;
        env.factory = getFactory();
        env.typeProbeCollection = TYPE_PROBE_COLLECTION;
        env.matcher = new SimpleUrlMatcher() {{
            put("/imp", new ImportHandler(env));
        }};

        HttpServer server = new HttpServer(env);
        server.start();

        try {
            StringSink sink = new StringSink();
            RecordSourcePrinter printer = new RecordSourcePrinter(sink);
            QueryCompiler qc = new QueryCompiler(env);
            Assert.assertEquals(200, HttpTestUtils.upload("/csv/test-import.csv", "http://localhost:9000/imp", null, null));
            printer.print(qc.compile(getFactory(), "select count() from 'test-import.csv'"), getFactory());
            TestUtils.assertEquals("129\n", sink);
            sink.clear();

            Assert.assertEquals(200, HttpTestUtils.upload("/csv/test-headers.csv", "http://localhost:9000/imp?name=test-import.csv&overwrite=true&durable=true", null, null));
            printer.print(qc.compile(getFactory(), "select count() from 'test-import.csv'"), getFactory());
            TestUtils.assertEquals("5\n", sink);
        } finally {
            server.halt();
        }
    }

    @Test
    public void testImportUnknownFormat() {
        BootstrapEnv env = new BootstrapEnv();
        env.configuration = new ServerConfiguration();
        env.factory = getFactory();
        env.matcher = new SimpleUrlMatcher() {{
            put("/imp", new ImportHandler(env));
        }};

        HttpServer server = new HttpServer(env);

        server.start();
        StringBuilder response = new StringBuilder();
        try {
            Assert.assertEquals(400, HttpTestUtils.upload("/com/questdb/std/AssociativeCache.class", "http://localhost:9000/imp", null, response));
            TestUtils.assertEquals("Unsupported Data Format", response);
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("Connection reset"));
        } finally {
            server.halt();
        }
    }

    @Test
    public void testImportUtf8() throws Exception {

        BootstrapEnv env = new BootstrapEnv();
        env.configuration = new ServerConfiguration();
        env.factory = getFactory();
        env.typeProbeCollection = TYPE_PROBE_COLLECTION;
        env.matcher = new SimpleUrlMatcher() {{
            put("/imp", new ImportHandler(env));
        }};
        HttpServer server = new HttpServer(env);
        server.start();

        try {
            StringSink sink = new StringSink();
            RecordSourcePrinter printer = new RecordSourcePrinter(sink);
            QueryCompiler qc = new QueryCompiler(env);

            Assert.assertEquals(200, HttpTestUtils.upload("/csv/test-import-utf8.csv", "http://localhost:9000/imp?overwrite=true&durable=true", null, null));
            printer.print(qc.compile(getFactory(), "'test-import-utf8.csv'"), getFactory());
            // expect first line to be treated as header
            TestUtils.assertEquals("авг\tавг\n", sink);
        } finally {
            server.halt();
        }
    }

    @Test
    public void testImportWrongType() throws Exception {
        BootstrapEnv env = new BootstrapEnv();
        env.configuration = new ServerConfiguration();
        env.factory = getFactory();
        env.typeProbeCollection = TYPE_PROBE_COLLECTION;
        env.dateLocaleFactory = DateLocaleFactory.INSTANCE;
        env.dateFormatFactory = DATE_FORMAT_FACTORY;
        env.matcher = new SimpleUrlMatcher() {{
            put("/imp", new ImportHandler(env));
        }};

        HttpServer server = new HttpServer(env);

        server.start();

        try {
            Assert.assertEquals(200, HttpTestUtils.upload("/csv/test-import.csv", "http://localhost:9000/imp?fmt=json", "[{\"name\":\"IsoDate\", \"type\":\"DATE\"}, {\"name\":\"IntCol\", \"type\":\"DOUBLE\"}]", null));
            Assert.assertEquals(200, HttpTestUtils.upload("/csv/test-import.csv", "http://localhost:9000/imp", "[{\"name\":\"IsoDate\", \"type\":\"DATE\"}]", null));
            StringSink sink = new StringSink();
            RecordSourcePrinter printer = new RecordSourcePrinter(sink);
            QueryCompiler qc = new QueryCompiler(env);
            RecordSource src1 = qc.compile(env.factory, "select count(StrSym), count(IntSym), count(IntCol), count(long), count() from 'test-import.csv'");
            try {
                printer.print(src1, env.factory);
                TestUtils.assertEquals("252\t252\t256\t258\t258\n", sink);
            } finally {
                Misc.free(src1);
            }

            RecordSource src2 = qc.compile(env.factory, "'test-import.csv'");
            try {
                Assert.assertEquals(ColumnType.DOUBLE, src2.getMetadata().getColumn("IntCol").getType());
            } finally {
                Misc.free(src2);
            }
        } finally {
            server.halt();
        }
    }

    @Test
    public void testImportWrongTypeStrictAtomicity() throws Exception {
        BootstrapEnv env = new BootstrapEnv();
        env.configuration = new ServerConfiguration();
        env.factory = getFactory();
        env.typeProbeCollection = TYPE_PROBE_COLLECTION;
        env.matcher = new SimpleUrlMatcher() {{
            put("/imp", new ImportHandler(env));
        }};
        env.dateLocaleFactory = DateLocaleFactory.INSTANCE;
        env.dateFormatFactory = DATE_FORMAT_FACTORY;

        HttpServer server = new HttpServer(env);

        server.start();

        try {
            Assert.assertEquals(400, HttpTestUtils.upload(
                    "/csv/test-import.csv",
                    "http://localhost:9000/imp?atomicity=strict",
                    "[{\"name\":\"IsoDate\", \"type\":\"DATE\"}, {\"name\":\"IntCol\", \"type\":\"DATE\"}]",
                    null));
            StringSink sink = new StringSink();
            RecordSourcePrinter printer = new RecordSourcePrinter(sink);
            QueryCompiler qc = new QueryCompiler(env);
            RecordSource src1 = qc.compile(env.factory, "select count() from 'test-import.csv'");
            try {
                printer.print(src1, env.factory);
                TestUtils.assertEquals("0\n", sink);
            } finally {
                Misc.free(src1);
            }
        } finally {
            server.halt();
        }
    }

    @Test
    public void testLargeChunkedPlainDownload() throws Exception {
        final int count = 3;
        final int sz = 16 * 1026 * 1024 - 4;
        ServerConfiguration conf = new ServerConfiguration();
        conf.setHttpBufRespContent(sz + 4);
        TestUtils.assertEquals(generateLarge(count, sz), downloadChunked(conf, count, sz, false, false));
    }

    @Test
    public void testLargeChunkedPlainGzipDownload() throws Exception {
        final int count = 3;
        final int sz = 16 * 1026 * 1024 - 4;
        ServerConfiguration conf = new ServerConfiguration();
        conf.setHttpBufRespContent(sz + 4);
        TestUtils.assertEquals(generateLarge(count, sz), downloadChunked(conf, count, sz, false, true));
    }

    @Test
    public void testLargeChunkedSSLDownload() throws Exception {
        final int count = 3;
        final int sz = 16 * 1026 * 1024 - 4;
        final ServerConfiguration configuration = new ServerConfiguration(new File(resourceFile("/site"), "conf/questdb.conf"));
        configuration.getSslConfig().setSecure(true);
        configuration.getSslConfig().setKeyStore(new FileInputStream(resourceFile("/keystore/singlekey.ks")), "changeit");
        configuration.setHttpBufRespContent(sz + 4);
        TestUtils.assertEquals(generateLarge(count, sz), downloadChunked(configuration, count, sz, true, false));
    }

    @Test
    public void testLargeChunkedSSLGzipDownload() throws Exception {
        final int count = 3;
        final int sz = 16 * 1026 * 1024 - 4;
        final ServerConfiguration configuration = new ServerConfiguration(new File(resourceFile("/site"), "conf/questdb.conf"));
        configuration.getSslConfig().setSecure(true);
        configuration.getSslConfig().setKeyStore(new FileInputStream(resourceFile("/keystore/singlekey.ks")), "changeit");
        configuration.setHttpBufRespContent(sz + 4);
        TestUtils.assertEquals(generateLarge(count, sz), downloadChunked(configuration, count, sz, true, true));
    }

    @Test
    public void testMaxConnections() throws Exception {
        final ServerConfiguration configuration = new ServerConfiguration(new File(resourceFile("/site"), "conf/questdb.conf"));
        BootstrapEnv env = new BootstrapEnv();
        env.configuration = configuration;
        env.configuration.setHttpMaxConnections(1);
        env.matcher = new SimpleUrlMatcher() {{
            setDefaultHandler(new StaticContentHandler(env));
        }};
        HttpServer server = new HttpServer(env);
        server.start();

        try {
            Assert.assertNotNull(clientBuilder(true).build().execute(new HttpGet("https://localhost:9000/upload.html")));
            try {
                clientBuilder(true).build().execute(new HttpGet("https://localhost:9000/upload.html"));
                Assert.fail("Expected server to reject connection");
            } catch (Exception ignored) {

            }
        } finally {
            server.halt();
        }
    }

    @Test
    public void testNativeConcurrentDownload() throws Exception {
        final ServerConfiguration configuration = new ServerConfiguration(new File(resourceFile("/site"), "conf/questdb.conf"));
        BootstrapEnv env = new BootstrapEnv();
        env.configuration = configuration;
        env.configuration.getSslConfig().setSecure(false);
        env.matcher = new SimpleUrlMatcher() {{
            setDefaultHandler(new StaticContentHandler(env));
        }};
        final MimeTypes mimeTypes = new MimeTypes(env.configuration.getMimeTypes());
        HttpServer server = new HttpServer(env);
        server.start();
        assertConcurrentDownload(mimeTypes, server, "http");
    }

    @Test
    public void testNativeNotModified() throws Exception {
        BootstrapEnv env = new BootstrapEnv();
        env.configuration = new ServerConfiguration(new File(resourceFile("/site"), "conf/questdb.conf"));
        env.configuration.getSslConfig().setSecure(false);
        env.matcher = new SimpleUrlMatcher() {{
            setDefaultHandler(new StaticContentHandler(env));
        }};

        HttpServer server = new HttpServer(env);
        assertNotModified(env.configuration, server);
    }

    @Test
    // this test intermittently fails on fedora 25
    // at the same tie it works consistently on OSX and windows
    // problem seems to be either with loopback interface and/or kernel
    // symptoms are: http client is in blocking write to socket, server epoll triggers fd, but
    // recv(fd) returns -1 and EWOULDBLOCK and this goes on forever with epoll keep firing fd that has nothing to show
    // fix is in server, which would allow limited number of this iterations after which connection is closed.
    @Ignore
    public void testRangesNative() throws Exception {
        BootstrapEnv env = new BootstrapEnv();
        env.configuration = new ServerConfiguration(new File(HttpServerTest.class.getResource("/site").getPath(), "conf/questdb.conf")) {
            @Override
            public File getHttpPublic() {
                return temp.getRoot();
            }
        };
        env.configuration.setHttpThreads(1);
        env.configuration.setHttpSoRetries(10);
        env.configuration.getSslConfig().setSecure(false);
        env.matcher = new SimpleUrlMatcher() {{
            put("/upload", new UploadHandler(env.configuration.getHttpPublic()));
            setDefaultHandler(new StaticContentHandler(env));
        }};

        assertRanges(new HttpServer(env));
    }

    @Test
    public void testSslConcurrentDownload() throws Exception {
        BootstrapEnv env = new BootstrapEnv();
        env.configuration = new ServerConfiguration(new File(resourceFile("/site"), "conf/questdb.conf"));
        env.configuration.getSslConfig().setSecure(true);
        env.configuration.getSslConfig().setKeyStore(new FileInputStream(resourceFile("/keystore/singlekey.ks")), "changeit");
        env.matcher = new SimpleUrlMatcher() {{
            setDefaultHandler(new StaticContentHandler(env));
        }};

        final MimeTypes mimeTypes = new MimeTypes(env.configuration.getMimeTypes());
        HttpServer server = new HttpServer(env);
        server.start();
        assertConcurrentDownload(mimeTypes, server, "https");
    }

    @Test
    public void testStartStop() {
        BootstrapEnv env = new BootstrapEnv();
        env.configuration = new ServerConfiguration();
        env.matcher = new SimpleUrlMatcher();
        HttpServer server = new HttpServer(env);
        server.start();
        server.halt();
    }

    @Test
    public void testUpload() throws Exception {
        final File dir = temp.newFolder();
        BootstrapEnv env = new BootstrapEnv();
        env.configuration = new ServerConfiguration();
        env.matcher = new SimpleUrlMatcher() {{
            put("/upload", new UploadHandler(dir));
        }};

        HttpServer server = new HttpServer(env);
        server.start();

        File expected = resourceFile("/csv/test-import.csv");
        File actual = new File(dir, "test-import.csv");
        upload(expected);

        TestUtils.assertEquals(expected, actual);
        server.halt();
    }

    private static HttpClientBuilder clientBuilder(boolean ssl) throws Exception {
        return (ssl ? createHttpClient_AcceptsUntrustedCerts() : HttpClientBuilder.create());
    }

    private static HttpClientBuilder createHttpClient_AcceptsUntrustedCerts() throws Exception {
        HttpClientBuilder b = HttpClientBuilder.create();

        // setup a Trust Strategy that allows all certificates.
        //
        SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, (arg0, arg1) -> true).build();

        b.setSSLContext(sslContext);

        // here's the special part:
        //      -- need to create an SSL Socket Factory, to use our weakened "trust strategy";
        //      -- and create a Registry, to register it.
        //
        SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext, (s, sslSession) -> true);
        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", sslSocketFactory)
                .build();

        // now, we create connection-manager using our Registry.
        //      -- allows multi-threaded use
        b.setConnectionManager(new PoolingHttpClientConnectionManager(socketFactoryRegistry));

        return b;
    }


    private static File resourceFile(String resource) {
        return new File(HttpServerTest.class.getResource(resource).getFile());
    }

    private static void upload(File file) throws IOException {
        HttpPost post = new HttpPost("http://localhost:9000/upload");
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            MultipartEntityBuilder b = MultipartEntityBuilder.create();
            b.addPart("data", new FileBody(file));
            post.setEntity(b.build());
            client.execute(post);
        }
    }

    private void assertConcurrentDownload(MimeTypes mimeTypes, HttpServer server, final String proto) throws InterruptedException, IOException {
        try {

            // ssl

            final File actual1 = new File(temp.getRoot(), "get.html");
            final File actual2 = new File(temp.getRoot(), "post.html");
            final File actual3 = new File(temp.getRoot(), "upload.html");

            final CyclicBarrier barrier = new CyclicBarrier(3);
            final CountDownLatch haltLatch = new CountDownLatch(3);

            final AtomicInteger counter = new AtomicInteger(0);

            new Thread(() -> {
                try {
                    barrier.await();
                    HttpTestUtils.download(clientBuilder("https".equals(proto)), proto + "://localhost:9000/get.html", actual1);
                } catch (Exception e) {
                    counter.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    haltLatch.countDown();
                }
            }).start();


            new Thread(() -> {
                try {
                    barrier.await();
                    HttpTestUtils.download(clientBuilder("https".equals(proto)), proto + "://localhost:9000/post.html", actual2);
                } catch (Exception e) {
                    counter.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    haltLatch.countDown();
                }
            }).start();

            new Thread(() -> {
                try {
                    barrier.await();
                    HttpTestUtils.download(clientBuilder("https".equals(proto)), proto + "://localhost:9000/upload.html", actual3);
                } catch (Exception e) {
                    counter.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    haltLatch.countDown();
                }
            }).start();

            haltLatch.await();

            Assert.assertEquals(0, counter.get());
            TestUtils.assertEquals(new File(HttpServerTest.class.getResource("/site/public/get.html").getPath()), actual1);
            TestUtils.assertEquals(new File(HttpServerTest.class.getResource("/site/public/post.html").getPath()), actual2);
            TestUtils.assertEquals(new File(HttpServerTest.class.getResource("/site/public/upload.html").getPath()), actual3);

        } finally {
            server.halt();
        }
    }

    private void assertNotModified(ServerConfiguration configuration, HttpServer server) throws IOException {
        server.start();
        try {
            File out = new File(temp.getRoot(), "get.html");
            HttpGet get = new HttpGet("http://localhost:9000/get.html");

            try (CloseableHttpClient client = HttpClients.createDefault()) {

                Header h;
                try (
                        CloseableHttpResponse r = client.execute(get);
                        FileOutputStream fos = new FileOutputStream(out)
                ) {
                    HttpTestUtils.copy(r.getEntity().getContent(), fos);
                    Assert.assertEquals(200, r.getStatusLine().getStatusCode());
                    h = HttpTestUtils.findHeader("ETag", r.getAllHeaders());
                }

                Assert.assertNotNull(h);
                get.addHeader("If-None-Match", h.getValue());

                try (CloseableHttpResponse r = client.execute(get)) {
                    Assert.assertEquals(304, r.getStatusLine().getStatusCode());
                }
            }
        } finally {
            server.halt();
        }
    }

    private void assertRanges(HttpServer server) throws IOException {
        server.start();
        try {

            HttpTestUtils.upload("/large.csv", "http://localhost:9000/upload", null, null);

            File out = new File(temp.getRoot(), "out.csv");

            HttpGet get = new HttpGet("http://localhost:9000/large.csv?attachment");
            get.addHeader("Range", "xyz");

            try (CloseableHttpClient client = HttpClients.createDefault()) {

                try (CloseableHttpResponse r = client.execute(get)) {
                    Assert.assertEquals(416, r.getStatusLine().getStatusCode());
                }

                File f = resourceFile("/large.csv");
                long size;

                try (FileInputStream is = new FileInputStream(f)) {
                    size = is.available();
                }

                long part = size / 2;


                try (FileOutputStream fos = new FileOutputStream(out)) {

                    // first part
                    get.addHeader("Range", "bytes=0-" + part);
                    try (CloseableHttpResponse r = client.execute(get)) {
                        HttpTestUtils.copy(r.getEntity().getContent(), fos);
                        Assert.assertEquals(206, r.getStatusLine().getStatusCode());
                    }

                    // second part
                    get.addHeader("Range", "bytes=" + part + "-");
                    try (CloseableHttpResponse r = client.execute(get)) {
                        HttpTestUtils.copy(r.getEntity().getContent(), fos);
                        Assert.assertEquals(206, r.getStatusLine().getStatusCode());
                    }
                }

                TestUtils.assertEquals(f, out);
            }
        } finally {
            server.halt();
        }
    }

    private File downloadChunked(ServerConfiguration conf, final int count, final int sz, boolean ssl, final boolean compressed) throws Exception {
        BootstrapEnv env = new BootstrapEnv();
        env.configuration = conf;
        env.matcher = new SimpleUrlMatcher() {{

            put("/test", new ContextHandler() {

                private int counter = -1;

                @Override
                public void handle(IOContext context) throws IOException {
                    ChunkedResponse r = context.chunkedResponse();
                    r.setCompressed(compressed);
                    r.status(200, "text/plain; charset=utf-8");
                    r.sendHeader();
                    counter = -1;
                    resume(context);
                }

                @Override
                public void resume(IOContext context) throws IOException {
                    ChunkedResponse r = context.chunkedResponse();
                    for (int i = counter + 1; i < count; i++) {
                        counter = i;
                        try {
                            for (int k = 0; k < sz; k++) {
                                Numbers.append(r, i);
                            }
                            r.put(Misc.EOL);
                        } catch (ResponseContentBufferTooSmallException ignore) {
                            // ignore, send as much as we can in one chunk
                            LOG.error().$("Response content buffer is too small").$();
                        }
                        r.sendChunk();
                    }
                    r.done();
                }

                @Override
                public void setupThread() {
                }
            });
        }};

        HttpServer server = new HttpServer(env);
        File out = temp.newFile();
        server.start();
        try {
            HttpTestUtils.download(clientBuilder(ssl), (ssl ? "https" : "http") + "://localhost:9000/test", out);
        } finally {
            server.halt();
        }

        return out;
    }

    private File generateLarge(int count, int sz) throws IOException {
        File file = temp.newFile();
        try (FileSink sink = new FileSink(file)) {
            for (int i = 0; i < count; i++) {
                for (int k = 0; k < sz; k++) {
                    Numbers.append(sink, i);
                }
                sink.put(Misc.EOL);
            }
        }
        return file;
    }

    private SocketChannel openChannel() throws IOException {
        InetSocketAddress address = new InetSocketAddress("localhost", 9000);
        SocketChannel channel = SocketChannel.open()
                .setOption(StandardSocketOptions.TCP_NODELAY, Boolean.TRUE);

        channel.configureBlocking(false);
        try {
            channel.connect(address);
            long t = System.currentTimeMillis();

            while (!channel.finishConnect()) {
                LockSupport.parkNanos(500000L);
                if (System.currentTimeMillis() - t > (long) 5000) {
                    throw new IOException("Connection timeout");
                }
            }

            channel.configureBlocking(true);
            return channel;
        } catch (IOException e) {
            channel.close();
            throw e;
        }
    }
}
