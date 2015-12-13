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

package com.nfsdb.http;

import com.nfsdb.Journal;
import com.nfsdb.exceptions.NumericException;
import com.nfsdb.ha.AbstractJournalTest;
import com.nfsdb.http.handlers.ImportHandler;
import com.nfsdb.http.handlers.UploadHandler;
import com.nfsdb.iter.clock.Clock;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Dates;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.LockSupport;

public class HttpServerTest extends AbstractJournalTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testConcurrentImport() throws Exception {
        HttpServer server = new HttpServer(new InetSocketAddress(9090), new SimpleUrlMatcher() {{
            put("/import", new ImportHandler(factory));
        }}, 2, 1024);
        server.start();

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch latch = new CountDownLatch(2);
        try {

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        barrier.await();
                        upload(
                                new File(HttpServerTest.this.getClass().getResource("/csv/test-import.csv").getFile()),
                                "http://localhost:9090/import"
                        );
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
                        upload(
                                new File(HttpServerTest.this.getClass().getResource("/csv/test-import-nan.csv").getFile()),
                                "http://localhost:9090/import"
                        );
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
    public void testGoogle() throws Exception {
        HttpServer server = new HttpServer(new InetSocketAddress(9000), new SimpleUrlMatcher(), 2, 1000);
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

            ByteBuffers.copyNonBlocking(channel, out, 100000);

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
    public void testStartStop() throws Exception {
        HttpServer server = new HttpServer(new InetSocketAddress(9090), new SimpleUrlMatcher(), 2, 1024);
        server.start();
        server.halt();
    }

    @Test
    public void testUpload() throws Exception {
        final File dir = temp.newFolder();
        HttpServer server = new HttpServer(new InetSocketAddress(9090), new SimpleUrlMatcher() {{
            put("/upload", new UploadHandler(dir));
        }}, 2, 1024);
        server.start();

        File expected = new File(this.getClass().getResource("/csv/test-import.csv").getFile());
        File actual = new File(dir, "test-import.csv");
        upload(expected, "http://localhost:9090/upload");

        TestUtils.assertEquals(expected, actual);
        server.halt();
    }

    private static void upload(File file, String url) throws IOException {
        String charset = "UTF-8";
        String param = "value";
        String boundary = Long.toHexString(System.currentTimeMillis()); // Just generate some unique random value.
        String CRLF = "\r\n"; // Line separator required by multipart/form-data.

        URLConnection connection = new URL(url).openConnection();
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);

        try (
                OutputStream output = connection.getOutputStream();
                PrintWriter writer = new PrintWriter(new OutputStreamWriter(output, charset), true)
        ) {
            // Send normal param.
            writer.append("--").append(boundary).append(CRLF);
            writer.append("Content-Disposition: form-data; name=\"param\"").append(CRLF);
            writer.append("Content-Type: text/plain; charset=").append(charset).append(CRLF);
            writer.append(CRLF).append(param).append(CRLF).flush();

            // Send text file.
            writer.append("--").append(boundary).append(CRLF);
            writer.append("Content-Disposition: form-data; name=\"data\"; filename=\"").append(file.getName()).append("\"").append(CRLF);
            writer.append("Content-Type: text/plain; charset=").append(charset).append(CRLF); // Text file itself must be saved in this charset!
            writer.append(CRLF).flush();
            Files.copy(file.toPath(), output);
            output.flush(); // Important before continuing with writer!
            writer.append(CRLF).flush(); // CRLF is important! It indicates end of boundary.

            // End of multipart/form-data.
            writer.append("--").append(boundary).append("--").append(CRLF).flush();
        }

        int response = ((HttpURLConnection) connection).getResponseCode();
        System.out.println("Response: " + response);
        Assert.assertEquals(200, response);
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
