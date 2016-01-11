/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.net.http;

import com.nfsdb.Journal;
import com.nfsdb.exceptions.NumericException;
import com.nfsdb.iter.clock.Clock;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Dates;
import com.nfsdb.misc.Os;
import com.nfsdb.net.ha.AbstractJournalTest;
import com.nfsdb.net.http.handlers.ImportHandler;
import com.nfsdb.net.http.handlers.NativeStaticContentHandler;
import com.nfsdb.net.http.handlers.StaticContentHandler;
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
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class HttpServerTest extends AbstractJournalTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testConcurrentDownload() throws Exception {
        final HttpServerConfiguration configuration = new HttpServerConfiguration(new File(resourceFile("/site"), "conf/nfsdb.conf"));
        configuration.getSslConfig().setSecure(false);
        final MimeTypes mimeTypes = new MimeTypes(configuration.getMimeTypes());
        HttpServer server = new HttpServer(configuration, new SimpleUrlMatcher() {{
            setDefaultHandler(new StaticContentHandler(configuration.getHttpPublic(), mimeTypes));
        }});
        server.start();

        assertConcurrentDownload(mimeTypes, server, "http");
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
            final String request = "GET /imp?x=1&z=2 HTTP/1.1\r\n" +
                    "Host: localhost:80\r\n" +
                    "Connection: keep-alive\r\n" +
                    "Cache-Control: max-age=0\r\n" +
                    "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\r\n" +
                    "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48 Safari/537.36\r\n" +
                    "Accept-Language: en-US,en;q=0.8\r\n" +
                    "Cookie: textwrapon=false; textautoformat=false; wysiwyg=textarea\r\n" +
                    "\r\n";

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
    public void testImportUnknownFormat() throws Exception {
        HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher() {{
            put("/imp", new ImportHandler(factory));
        }});
        server.start();
        Assert.assertEquals(400, upload("/com/nfsdb/collections/AssociativeCache.class", "http://localhost:9000/imp"));
        server.halt();
    }

    @Test
    public void testNativeConcurrentDownload() throws Exception {
        if (Os.nativelySupported) {
            final HttpServerConfiguration configuration = new HttpServerConfiguration(new File(resourceFile("/site"), "conf/nfsdb.conf"));
            configuration.getSslConfig().setSecure(false);
            final MimeTypes mimeTypes = new MimeTypes(configuration.getMimeTypes());
            HttpServer server = new HttpServer(configuration, new SimpleUrlMatcher() {{
                setDefaultHandler(new NativeStaticContentHandler(configuration.getHttpPublic(), mimeTypes));
            }});
            server.start();

            assertConcurrentDownload(mimeTypes, server, "http");
        }
    }

    @Test
    public void testNativeNotModified() throws Exception {
        if (Os.nativelySupported) {
            final HttpServerConfiguration configuration = new HttpServerConfiguration(new File(resourceFile("/site"), "conf/nfsdb.conf"));
            HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher() {{
                setDefaultHandler(new NativeStaticContentHandler(configuration.getHttpPublic(), new MimeTypes(configuration.getMimeTypes())));
            }});
            assertNotModified(configuration, server);
        }
    }

    @Test
    public void testNotModified() throws Exception {
        final HttpServerConfiguration configuration = new HttpServerConfiguration(new File(resourceFile("/site"), "conf/nfsdb.conf"));
        HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher() {{
            setDefaultHandler(new StaticContentHandler(configuration.getHttpPublic(), new MimeTypes(configuration.getMimeTypes())));
        }});
        assertNotModified(configuration, server);
    }

    @Test
    public void testRanges() throws Exception {
        final HttpServerConfiguration configuration = new HttpServerConfiguration(new File(HttpServerTest.class.getResource("/site").getPath(), "conf/nfsdb.conf"));
        HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher() {{
            put("/upload", new UploadHandler(temp.getRoot()));
            setDefaultHandler(new StaticContentHandler(temp.getRoot(), new MimeTypes(configuration.getMimeTypes())));
        }});
        assertRanges(configuration, server);
    }

    @Test
    public void testRangesNative() throws Exception {
        if (Os.nativelySupported) {
            final HttpServerConfiguration configuration = new HttpServerConfiguration(new File(HttpServerTest.class.getResource("/site").getPath(), "conf/nfsdb.conf"));
            HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher() {{
                put("/upload", new UploadHandler(temp.getRoot()));
                setDefaultHandler(new NativeStaticContentHandler(temp.getRoot(), new MimeTypes(configuration.getMimeTypes())));
            }});
            assertRanges(configuration, server);
        }
    }

    @Test
    public void testSslConcurrentDownload() throws Exception {
        if (Os.nativelySupported) {
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

    private void download(HttpClientBuilder b, String url, File out) throws IOException {
        try (
                CloseableHttpClient client = b.build();
                CloseableHttpResponse r = client.execute(new HttpGet(url));
                FileOutputStream fos = new FileOutputStream(out)
        ) {
            copy(r.getEntity().getContent(), fos);
        }
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
