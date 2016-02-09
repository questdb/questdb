/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.net.http;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.nfsdb.Journal;
import com.nfsdb.JournalEntryWriter;
import com.nfsdb.JournalWriter;
import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.NumericException;
import com.nfsdb.ex.ResponseContentBufferTooSmallException;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.io.sink.FileSink;
import com.nfsdb.iter.clock.Clock;
import com.nfsdb.misc.*;
import com.nfsdb.net.ha.AbstractJournalTest;
import com.nfsdb.net.http.handlers.ImportHandler;
import com.nfsdb.net.http.handlers.JsonHandler;
import com.nfsdb.net.http.handlers.NativeStaticContentHandler;
import com.nfsdb.net.http.handlers.UploadHandler;
import com.nfsdb.test.tools.TestUtils;
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
import org.apache.http.ssl.TrustStrategy;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class HttpServerTest extends AbstractJournalTest {

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
        final HttpServerConfiguration configuration = new HttpServerConfiguration(new File(resourceFile("/site"), "conf/nfsdb.conf"));
        configuration.getSslConfig().setSecure(true);
        configuration.getSslConfig().setKeyStore(new FileInputStream(resourceFile("/keystore/singlekey.ks")), "changeit");


        final MimeTypes mimeTypes = new MimeTypes(configuration.getMimeTypes());
        HttpServer server = new HttpServer(configuration, new SimpleUrlMatcher() {{
            setDefaultHandler(new NativeStaticContentHandler(configuration.getHttpPublic(), mimeTypes));
        }});
        server.start();

        try {
            download(clientBuilder(true), "https://localhost:9000/upload.html", new File(temp.getRoot(), "upload.html"));
        } finally {
            server.halt();
        }
    }

    @Test
    public void testConcurrentImport() throws Exception {
        HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher() {{
            put("/imp", new ImportHandler(factory));
        }});
        server.start();

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch latch = new CountDownLatch(2);
        try {

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        barrier.await();
                        Assert.assertEquals(200, upload("/csv/test-import.csv", "http://localhost:9000/imp"));
                        latch.countDown();
                    } catch (Exception e) {
                        Assert.fail(e.getMessage());
                    }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        barrier.await();
                        Assert.assertEquals(200, upload("/csv/test-import-nan.csv", "http://localhost:9000/imp"));
                        latch.countDown();
                    } catch (Exception e) {
                        Assert.fail(e.getMessage());
                    }
                }
            }).start();

            latch.await();

            try (Journal r = factory.reader("test-import.csv")) {
                Assert.assertEquals("First failed", 129, r.size());
            }

            try (Journal r = factory.reader("test-import-nan.csv")) {
                Assert.assertEquals("Second failed", 129, r.size());
            }
        } finally {
            server.halt();
        }
    }

    @Test
    public void testConnectionCount() throws Exception {
        final HttpServerConfiguration configuration = new HttpServerConfiguration(new File(resourceFile("/site"), "conf/nfsdb.conf"));
        final MimeTypes mimeTypes = new MimeTypes(configuration.getMimeTypes());
        configuration.setHttpMaxConnections(10);
        HttpServer server = new HttpServer(configuration, new SimpleUrlMatcher() {{
            setDefaultHandler(new NativeStaticContentHandler(configuration.getHttpPublic(), mimeTypes));

        }});
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
            Thread.sleep(100);
            Assert.assertEquals(2, server.getConnectionCount());

            c3.close();
            c4.close();
            Thread.sleep(100);
            Assert.assertEquals(0, server.getConnectionCount());
        } finally {
            server.halt();
        }
    }

    @Test
    public void testFragmentedUrl() throws Exception {
        HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher());
        server.setClock(new Clock() {

            @Override
            public long getTicks() {
                try {
                    return Dates.parseDateTime("2015-12-05T13:30:00.000Z");
                } catch (NumericException ignore) {
                    throw new RuntimeException(ignore);
                }
            }
        });
        server.start();

        try (SocketChannel channel = openChannel("localhost", 9000, 5000)) {
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
                    "Server: nfsdb/0.1\r\n" +
                    "Date: Sat, 5 Dec 2015 13:30:0 GMT\r\n" +
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
        final HttpServerConfiguration configuration = new HttpServerConfiguration(new File(resourceFile("/site"), "conf/nfsdb.conf"));
        final MimeTypes mimeTypes = new MimeTypes(configuration.getMimeTypes());
        // 500ms timeout
        configuration.setHttpTimeout(500);
        configuration.getSslConfig().setSecure(false);
        HttpServer server = new HttpServer(configuration, new SimpleUrlMatcher() {{
            setDefaultHandler(new NativeStaticContentHandler(configuration.getHttpPublic(), mimeTypes));

        }});
        server.start();

        try {
            Socket socket = new Socket(InetAddress.getLocalHost(), 9000);
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
    public void testImportUnknownFormat() throws Exception {
        HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher() {{
            put("/imp", new ImportHandler(factory));
        }});
        server.start();
        try {
            Assert.assertEquals(400, upload("/com/nfsdb/std/AssociativeCache.class", "http://localhost:9000/imp"));
        } finally {
            server.halt();
        }
    }

    @Test
    public void testJsonChunkOverflow() throws Exception {
        int count = (int) 1E4;
        generateJournal(count);
        HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher() {{
            put("/js", new JsonHandler(factory));
        }});
        server.start();
        try {
            String query = "tab";
            QueryResponse queryResponse = download(query);
            Assert.assertEquals(count, queryResponse.result.length);
        } finally {
            server.halt();
        }
    }

    @Test
    public void testJsonEmpty() throws Exception {
        generateJournal();
        HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher() {{
            put("/js", new JsonHandler(factory));
        }});
        server.start();
        try {
            String query = "tab";
            QueryResponse queryResponse = download(query, 0, 0);
            Assert.assertEquals(0, queryResponse.result.length);
        } finally {
            server.halt();
        }
    }

    @Test
    public void testJsonEmpty0() throws Exception {
        generateJournal();
        HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher() {{
            put("/js", new JsonHandler(factory));
        }});
        server.start();
        try {
            String query = "tab where 1 = 2";
            QueryResponse queryResponse = download(query, 0, 0);
            Assert.assertEquals(0, queryResponse.result.length);
        } finally {
            server.halt();
        }
    }

    @Test
    public void testJsonEncodeControlChars() throws Exception {
        java.lang.StringBuilder allChars = new java.lang.StringBuilder();
        for (char c = Character.MIN_VALUE; c < 0xD800; c++) { //
            allChars.append(c);
        }

        String allCharString = allChars.toString();
        generateJournal(allCharString, 1.900232E-10, 2.598E20, Long.MAX_VALUE, Integer.MIN_VALUE, -102023);
        HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher() {{
            put("/js", new JsonHandler(factory));
        }});
        server.start();
        try {
            String query = "select id from tab \n limit 1";
            QueryResponse queryResponse = download(query);
            Assert.assertEquals(query, queryResponse.query);
            for (int i = 0; i < allCharString.length(); i++) {
                Assert.assertTrue("result len is less than " + i, i < queryResponse.result[0].id.length());
                Assert.assertEquals(i + "", allCharString.charAt(i), queryResponse.result[0].id.charAt(i));
            }
        } finally {
            server.halt();
        }
    }

    @Test
    public void testJsonEncodeNumbers() throws Exception {
        generateJournal(null, 1.900232E-10, Double.MAX_VALUE, Long.MAX_VALUE, Integer.MIN_VALUE, 10);
        HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher() {{
            put("/js", new JsonHandler(factory));
        }});
        server.start();
        try {
            String query = "tab limit 20";
            QueryResponse queryResponse = download(query);
            Assert.assertEquals(1.900232E-10, queryResponse.result[0].x, 1E-6);
            Assert.assertEquals(Double.MAX_VALUE, queryResponse.result[0].y, 1E-6);
            Assert.assertEquals(Long.MAX_VALUE, queryResponse.result[0].z);
            Assert.assertEquals(0, queryResponse.result[0].w);
            Assert.assertEquals(10, queryResponse.result[0].timestamp);
            Assert.assertEquals(false, queryResponse.moreExist);
        } finally {
            server.halt();
        }
    }

    @Test
    public void testJsonLimits() throws Exception {
        generateJournal();
        HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher() {{
            put("/js", new JsonHandler(factory));
        }});
        server.start();
        try {
            String query = "tab";
            QueryResponse queryResponse = download(query, 2, 4);
            Assert.assertEquals(2, queryResponse.result.length);
            Assert.assertEquals(true, queryResponse.moreExist);
            Assert.assertEquals("id2", queryResponse.result[0].id);
            Assert.assertEquals("id3", queryResponse.result[1].id);
        } finally {
            server.halt();
        }
    }

    @Test
    public void testJsonTakeLimit() throws Exception {
        generateJournal();
        HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher() {{
            put("/js", new JsonHandler(factory));
        }});
        server.start();
        try {
            String query = "tab limit 10";
            QueryResponse queryResponse = download(query, 2, -1);
            Assert.assertEquals(2, queryResponse.result.length);
            Assert.assertEquals(true, queryResponse.moreExist);
        } finally {
            server.halt();
        }
    }

    @Test
    public void testLargeChunkedPlainDownload() throws Exception {
        final int count = 3;
        final int sz = 16 * 1026 * 1024 - 4;
        HttpServerConfiguration conf = new HttpServerConfiguration();
        conf.setHttpBufRespContent(sz + 4);
        TestUtils.assertEquals(generateLarge(count, sz), downloadChunked(conf, count, sz, false, false));
    }

    @Test
    public void testLargeChunkedPlainGzipDownload() throws Exception {
        final int count = 3;
        final int sz = 16 * 1026 * 1024 - 4;
        HttpServerConfiguration conf = new HttpServerConfiguration();
        conf.setHttpBufRespContent(sz + 4);
        TestUtils.assertEquals(generateLarge(count, sz), downloadChunked(conf, count, sz, false, true));
    }

    @Test
    public void testLargeChunkedSSLDownload() throws Exception {
        final int count = 3;
        final int sz = 16 * 1026 * 1024 - 4;
        final HttpServerConfiguration configuration = new HttpServerConfiguration(new File(resourceFile("/site"), "conf/nfsdb.conf"));
        configuration.getSslConfig().setSecure(true);
        configuration.getSslConfig().setKeyStore(new FileInputStream(resourceFile("/keystore/singlekey.ks")), "changeit");
        configuration.setHttpBufRespContent(sz + 4);
        TestUtils.assertEquals(generateLarge(count, sz), downloadChunked(configuration, count, sz, true, false));
    }

    @Test
    public void testLargeChunkedSSLGzipDownload() throws Exception {
        final int count = 3;
        final int sz = 16 * 1026 * 1024 - 4;
        final HttpServerConfiguration configuration = new HttpServerConfiguration(new File(resourceFile("/site"), "conf/nfsdb.conf"));
        configuration.getSslConfig().setSecure(true);
        configuration.getSslConfig().setKeyStore(new FileInputStream(resourceFile("/keystore/singlekey.ks")), "changeit");
        configuration.setHttpBufRespContent(sz + 4);
        TestUtils.assertEquals(generateLarge(count, sz), downloadChunked(configuration, count, sz, true, true));
    }

    @Test
    public void testMaxConnections() throws Exception {
        final HttpServerConfiguration configuration = new HttpServerConfiguration(new File(resourceFile("/site"), "conf/nfsdb.conf"));
        final MimeTypes mimeTypes = new MimeTypes(configuration.getMimeTypes());
        configuration.setHttpMaxConnections(1);
        HttpServer server = new HttpServer(configuration, new SimpleUrlMatcher() {{
            setDefaultHandler(new NativeStaticContentHandler(configuration.getHttpPublic(), mimeTypes));

        }});
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
        final HttpServerConfiguration configuration = new HttpServerConfiguration(new File(resourceFile("/site"), "conf/nfsdb.conf"));
        configuration.getSslConfig().setSecure(false);
        final MimeTypes mimeTypes = new MimeTypes(configuration.getMimeTypes());
        HttpServer server = new HttpServer(configuration, new SimpleUrlMatcher() {{
            setDefaultHandler(new NativeStaticContentHandler(configuration.getHttpPublic(), mimeTypes));
        }});
        server.start();

        assertConcurrentDownload(mimeTypes, server, "http");
    }

    @Test
    public void testNativeNotModified() throws Exception {
        final HttpServerConfiguration configuration = new HttpServerConfiguration(new File(resourceFile("/site"), "conf/nfsdb.conf"));
        HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher() {{
            setDefaultHandler(new NativeStaticContentHandler(configuration.getHttpPublic(), new MimeTypes(configuration.getMimeTypes())));
        }});
        assertNotModified(configuration, server);
    }

    @Test
    public void testRangesNative() throws Exception {
        final HttpServerConfiguration configuration = new HttpServerConfiguration(new File(HttpServerTest.class.getResource("/site").getPath(), "conf/nfsdb.conf"));
        HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher() {{
            put("/upload", new UploadHandler(temp.getRoot()));
            setDefaultHandler(new NativeStaticContentHandler(temp.getRoot(), new MimeTypes(configuration.getMimeTypes())));
        }});
        assertRanges(configuration, server);
    }

    @Test
    public void testSimpleJson() throws Exception {
        generateJournal();
        HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher() {{
            put("/js", new JsonHandler(factory));
        }});
        server.start();
        try {
            QueryResponse queryResponse = download("select 1 z from tab limit 10");
            Assert.assertEquals(10, queryResponse.result.length);
        } finally {
            server.halt();
        }
    }

    @Test
    public void testSslConcurrentDownload() throws Exception {
        final HttpServerConfiguration configuration = new HttpServerConfiguration(new File(resourceFile("/site"), "conf/nfsdb.conf"));
        configuration.getSslConfig().setSecure(true);
        configuration.getSslConfig().setKeyStore(new FileInputStream(resourceFile("/keystore/singlekey.ks")), "changeit");


        final MimeTypes mimeTypes = new MimeTypes(configuration.getMimeTypes());
        HttpServer server = new HttpServer(configuration, new SimpleUrlMatcher() {{
            setDefaultHandler(new NativeStaticContentHandler(configuration.getHttpPublic(), mimeTypes));
        }});
        server.start();
        assertConcurrentDownload(mimeTypes, server, "https");
    }

    @Test
    public void testStartStop() throws Exception {
        HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher());
        server.start();
        server.halt();
    }

    @Test
    public void testUpload() throws Exception {
        final File dir = temp.newFolder();
        HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher() {{
            put("/upload", new UploadHandler(dir));
        }});
        server.start();

        File expected = resourceFile("/csv/test-import.csv");
        File actual = new File(dir, "test-import.csv");
        upload(expected, "http://localhost:9000/upload");

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
        SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
            public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
                return true;
            }
        }).build();

        b.setSSLContext(sslContext);

        // here's the special part:
        //      -- need to create an SSL Socket Factory, to use our weakened "trust strategy";
        //      -- and create a Registry, to register it.
        //
        SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext, new HostnameVerifier() {
            @Override
            public boolean verify(String s, SSLSession sslSession) {
                return true;
            }
        });
        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", sslSocketFactory)
                .build();

        // now, we create connection-manager using our Registry.
        //      -- allows multi-threaded use
        b.setConnectionManager(new PoolingHttpClientConnectionManager(socketFactoryRegistry));

        return b;
    }

    private static int upload(String resource, String url) throws IOException {
        return upload(resourceFile(resource), url);
    }

    private static File resourceFile(String resource) {
        return new File(HttpServerTest.class.getResource(resource).getFile());
    }

    private static int upload(File file, String url) throws IOException {
        HttpPost post = new HttpPost(url);
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            MultipartEntityBuilder b = MultipartEntityBuilder.create();
            b.addPart("data", new FileBody(file));
            post.setEntity(b.build());
            HttpResponse r = client.execute(post);
            return r.getStatusLine().getStatusCode();
        }
    }

    private static Header findHeader(String name, org.apache.http.Header[] headers) {
        for (Header h : headers) {
            if (name.equals(h.getName())) {
                return h;
            }
        }

        return null;
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

            new Thread() {
                @Override
                public void run() {
                    try {
                        barrier.await();
                        download(clientBuilder("https".equals(proto)), proto + "://localhost:9000/get.html", actual1);
                    } catch (Exception e) {
                        counter.incrementAndGet();
                        e.printStackTrace();
                    } finally {
                        haltLatch.countDown();
                    }
                }
            }.start();


            new Thread() {
                @Override
                public void run() {
                    try {
                        barrier.await();
                        download(clientBuilder("https".equals(proto)), proto + "://localhost:9000/post.html", actual2);
                    } catch (Exception e) {
                        counter.incrementAndGet();
                        e.printStackTrace();
                    } finally {
                        haltLatch.countDown();
                    }
                }
            }.start();

            new Thread() {
                @Override
                public void run() {
                    try {
                        barrier.await();
                        download(clientBuilder("https".equals(proto)), proto + "://localhost:9000/upload.html", actual3);
                    } catch (Exception e) {
                        counter.incrementAndGet();
                        e.printStackTrace();
                    } finally {
                        haltLatch.countDown();
                    }
                }
            }.start();

            haltLatch.await();

            Assert.assertEquals(0, counter.get());
            TestUtils.assertEquals(new File(HttpServerTest.class.getResource("/site/public/get.html").getPath()), actual1);
            TestUtils.assertEquals(new File(HttpServerTest.class.getResource("/site/public/post.html").getPath()), actual2);
            TestUtils.assertEquals(new File(HttpServerTest.class.getResource("/site/public/upload.html").getPath()), actual3);

        } finally {
            server.halt();
            mimeTypes.close();
        }
    }

    private void assertNotModified(HttpServerConfiguration configuration, HttpServer server) throws IOException, InterruptedException {
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
                    copy(r.getEntity().getContent(), fos);
                    Assert.assertEquals(200, r.getStatusLine().getStatusCode());
                    h = findHeader("ETag", r.getAllHeaders());
                }

                Assert.assertNotNull(h);
                get.addHeader("If-None-Match", h.getValue());

                try (CloseableHttpResponse r = client.execute(get)) {
                    Assert.assertEquals(304, r.getStatusLine().getStatusCode());
                }
            }
        } finally {
            server.halt();
            new MimeTypes(configuration.getMimeTypes()).close();
        }
    }

    private void assertRanges(HttpServerConfiguration configuration, HttpServer server) throws IOException, InterruptedException {
        server.start();
        try {

            upload("/large.csv", "http://localhost:9000/upload");

            File out = new File(temp.getRoot(), "out.csv");

            HttpGet get = new HttpGet("http://localhost:9000/large.csv");
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
                        copy(r.getEntity().getContent(), fos);
                        Assert.assertEquals(206, r.getStatusLine().getStatusCode());
                    }

                    // second part
                    get.addHeader("Range", "bytes=" + part + "-");
                    try (CloseableHttpResponse r = client.execute(get)) {
                        copy(r.getEntity().getContent(), fos);
                        Assert.assertEquals(206, r.getStatusLine().getStatusCode());
                    }
                }

                TestUtils.assertEquals(f, out);
            }
        } finally {
            server.halt();
            new MimeTypes(configuration.getMimeTypes()).close();
        }
    }

    private void copy(InputStream is, OutputStream os) throws IOException {
        byte[] buf = new byte[4096];
        int l;
        while ((l = is.read(buf)) > 0) {
            os.write(buf, 0, l);
        }
    }

    private QueryResponse download(String queryUrl) throws Exception {
        return download(queryUrl, -1, -1);
    }

    private QueryResponse download(String queryUrl, int limitFrom, int limitTo) throws Exception {
        File f = temp.newFile();
        String url = "http://localhost:9000/js?query=" + URLEncoder.encode(queryUrl, "UTF-8");
        if (limitFrom >= 0) {
            url += "&limit=" + limitFrom;
        }
        if (limitTo >= 0) {
            url += "," + limitTo;
        }
        download(clientBuilder(false), url, f);
        Gson gson = new GsonBuilder()
                .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").create();
        String body = Files.readStringFromFile(f);
        f.deleteOnExit();
        return gson.fromJson(body, QueryResponse.class);
    }

    private void download(HttpClientBuilder b, String url, File out) throws IOException {
        try (
                CloseableHttpClient client = b.build();
                CloseableHttpResponse r = client.execute(new HttpGet(url));
                FileOutputStream fos = new FileOutputStream(out)
        ) {
            copy(r.getEntity().getContent(), fos);
        }
    }

    private File downloadChunked(HttpServerConfiguration conf, final int count, final int sz, boolean ssl, final boolean compressed) throws Exception {
        HttpServer server = new HttpServer(conf, new SimpleUrlMatcher() {{

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
                            System.out.println("small");
                        }
                        r.sendChunk();
                    }
                    r.done();
                }
            });
        }});

        File out = temp.newFile();
        server.start();
        try {
            download(clientBuilder(ssl), (ssl ? "https" : "http") + "://localhost:9000/test", out);
        } finally {
            server.halt();
        }

        return out;
    }


    private void generateJournal(String id, double x, double y, long z, int w, long timestamp) throws JournalException, NumericException {
        QueryResponse.Tab record = new QueryResponse.Tab();
        record.id = id;
        record.x = x;
        record.y = y;
        record.z = z;
        record.w = w;
        record.timestamp = timestamp;
        generateJournal(new QueryResponse.Tab[]{record}, 1000);
    }

    private void generateJournal() throws JournalException, NumericException {
        generateJournal(new QueryResponse.Tab[0], 1000);
    }

    private void generateJournal(int count) throws JournalException, NumericException {
        generateJournal(new QueryResponse.Tab[0], count);
    }

    private void generateJournal(QueryResponse.Tab[] recs, int count) throws JournalException, NumericException {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $str("id").
                        $double("x").
                        $double("y").
                        $long("z").
                        $int("w").
                        $ts()

        );

        Rnd rnd = new Rnd();
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");

        for (int i = 0; i < count; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putStr(0, recs.length > i ? recs[i].id : "id" + i);
            ew.putDouble(1, recs.length > i ? recs[i].x : rnd.nextDouble());
            if (recs.length > i) {
                ew.putDouble(2, recs[i].y);
                ew.putLong(3, recs[i].z);
            } else {
                if (rnd.nextPositiveInt() % 10 == 0) {
                    ew.putNull(2);
                } else {
                    ew.putDouble(2, rnd.nextDouble());
                }
                if (rnd.nextPositiveInt() % 10 == 0) {
                    ew.putNull(3);
                } else {
                    ew.putLong(3, rnd.nextLong() % 500);
                }
            }
            ew.putInt(4, recs.length > i ? recs[i].w : rnd.nextInt() % 500);
            ew.putDate(5, recs.length > i ? recs[i].timestamp : t);
            t += 10;
            ew.append();
        }
        w.commit();
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

    private SocketChannel openChannel(String host, int port, long timeout) throws IOException {
        InetSocketAddress address = new InetSocketAddress(host, port);
        SocketChannel channel = SocketChannel.open()
                .setOption(StandardSocketOptions.TCP_NODELAY, Boolean.TRUE);

        channel.configureBlocking(false);
        try {
            channel.connect(address);
            long t = System.currentTimeMillis();

            while (!channel.finishConnect()) {
                LockSupport.parkNanos(500000L);
                if (System.currentTimeMillis() - t > timeout) {
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
