/*******************************************************************************
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

package io.questdb.test.cutlass.http.line;

import io.questdb.*;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.client.Sender;
import io.questdb.client.std.bytes.DirectByteSlice;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientException;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPOutputStream;

import static io.questdb.PropertyKey.DEBUG_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE;
import static io.questdb.cairo.wal.WalUtils.EVENT_INDEX_FILE_NAME;
import static io.questdb.test.tools.TestUtils.assertEventually;
import static io.questdb.test.tools.TestUtils.assertResponse;
import static java.net.HttpURLConnection.HTTP_BAD_METHOD;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;

public class LineHttpFailureTest extends AbstractBootstrapTest {

    @NotNull
    private static AtomicInteger countWalWriterTakenFromPool(TestServerMain serverMain) {
        AtomicInteger walWriterTaken = new AtomicInteger();

        serverMain.getEngine().setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
            if (factoryType == PoolListener.SRC_WAL_WRITER && tableToken != null && tableToken.getTableName().equals("line")) {
                if (event == PoolListener.EV_GET || event == PoolListener.EV_CREATE) {
                    walWriterTaken.incrementAndGet();
                }
                if (event == PoolListener.EV_RETURN) {
                    walWriterTaken.decrementAndGet();
                }
            }
        });
        return walWriterTaken;
    }

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testChunkedDisconnect() throws Exception {
        assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    String line = "line,sym1=123 field1=123i 1234567890000000000\n";

                    for (int i = 0; i < 10; i++) {
                        HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                        request.POST()
                                .url("/write ")
                                .withChunkedContent()
                                .putAscii("\r\n")
                                .putAscii("\r\n")
                                .putAscii("500\r\n")
                                .putAscii(line)
                                .putAscii(line)
                                .sendPartialContent(1000, 5000);
                        Os.sleep(5);
                        httpClient.disconnect();
                    }

                    TableToken tt = serverMain.getEngine().getTableTokenIfExists("line");
                    Assert.assertNull(tt);
                }
            }
        });
    }

    @Test
    public void testChunkedEncodingMalformed() throws Exception {
        assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();
                String line = "line,sym1=123 field1=123i 1234567890000000000\n";

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                    try (
                            HttpClient.ResponseHeaders resp = request.POST()
                                    .url("/write ")
                                    .header("Transfer-Encoding", "chunked")
                                    .withContent()
                                    .putAscii(line)
                                    .putAscii(line)
                                    .send()
                    ) {
                        resp.await();
                        Assert.fail();
                    } catch (HttpClientException e) {
                        TestUtils.assertContains(e.getMessage(), "peer disconnect");
                    }
                }
            }
        });
    }

    @Test
    public void testChunkedRedundantBytes() throws Exception {
        assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();

                Rnd rnd = TestUtils.generateRandom(LOG);

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    String line = "line,sym1=123 field1=123i 1234567890000000000\n";

                    int count = 1 + rnd.nextInt(100);
                    HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                    request.POST()
                            .url("/write ")
                            .withChunkedContent();

                    String hexChunkLen = Integer.toHexString(line.length() * count);
                    if (rnd.nextBoolean()) {
                        hexChunkLen = hexChunkLen.toUpperCase();
                    } else {
                        hexChunkLen = hexChunkLen.toLowerCase();
                    }
                    request.putAscii(hexChunkLen).putEOL();

                    for (int i = 0; i < count; i++) {
                        request.putAscii(line);
                    }

                    request.putEOL().putAscii("0").putEOL().putEOL();

                    // This is not allowed
                    request.putEOL();

                    try (HttpClient.ResponseHeaders resp = request.send(5000)) {
                        resp.await();
                        Assert.fail();
                    } catch (HttpClientException e) {
                        TestUtils.assertContains(e.getMessage(), "peer disconnect");
                    }
                }

                serverMain.awaitTable("line");
                serverMain.assertSql("select count() from line", "count\n0\n");
            }
        });
    }

    @Test
    public void testClientDisconnectedBeforeCommitted() throws Exception {
        assertMemoryLeak(() -> {
            AtomicReference<HttpClient> httpClientRef = new AtomicReference<>();
            SOCountDownLatch ping = new SOCountDownLatch(1);
            SOCountDownLatch pong = new SOCountDownLatch(1);
            final FilesFacade filesFacade = new TestFilesFacadeImpl() {
                @Override
                public long openRW(LPSZ name, int opts) {
                    if (Utf8s.endsWithAscii(name, "field1.d") && Utf8s.containsAscii(name, "wal")) {
                        ping.await();
                        httpClientRef.get().disconnect();
                        pong.countDown();
                    }
                    return super.openRW(name, opts);
                }
            };

            final Bootstrap bootstrap = new Bootstrap(new DefaultBootstrapConfiguration() {
                @Override
                public FilesFacade getFilesFacade() {
                    return filesFacade;
                }
            }, Bootstrap.getServerMainArgs(root));

            try (final TestServerMain serverMain = new TestServerMain(bootstrap)) {
                serverMain.start();
                AtomicInteger walWriterTaken = countWalWriterTakenFromPool(serverMain);

                int count = 10;
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    httpClientRef.set(httpClient);
                    String line = ",sym1=123 field1=123i 1234567890000000000\n";

                    for (int i = 0; i < count; i++) {
                        ping.setCount(1);
                        pong.setCount(1);
                        HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                        String contentLine = "line" + i + line;
                        request.POST()
                                .url("/write ")
                                .withContent()
                                .putAscii(contentLine)
                                .putAscii(contentLine)
                                .send();
                        ping.countDown();
                        pong.await();
                    }
                }

                assertEventually(() -> {
                    // Assert no WAL writers are left in ILP http TUD cache
                    Assert.assertEquals(0, walWriterTaken.get());
                });
            }
        });
    }

    @Test
    public void testClientDisconnectedDuringCommit() throws Exception {
        assertMemoryLeak(() -> {
            AtomicReference<HttpClient> httpClientRef = new AtomicReference<>();
            SOCountDownLatch ping = new SOCountDownLatch(1);
            SOCountDownLatch pong = new SOCountDownLatch(1);
            AtomicInteger counter = new AtomicInteger(2);

            final FilesFacade filesFacade = new TestFilesFacadeImpl() {
                long addr = 0;

                @Override
                public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                    final long addr = super.mmap(fd, len, offset, flags, memoryTag);
                    if (fd == this.fd) {
                        this.addr = addr;
                    }
                    return addr;
                }

                @Override
                public void msync(long addr, long len, boolean async) {
                    if ((addr == this.addr) && (counter.decrementAndGet() == 0)) {
                        ping.await();
                        httpClientRef.get().disconnect();
                        pong.countDown();
                        // The longer is the sleep the more likely
                        // the disconnect happens during sending the response. But it also makes the test slower.
                        Os.sleep(10);
                    }
                    super.msync(addr, len, async);
                }

                @Override
                public long openRW(LPSZ name, int opts) {
                    long fd = super.openRW(name, opts);
                    if (Utf8s.endsWithAscii(name, Files.SEPARATOR + EVENT_INDEX_FILE_NAME)
                            && Utf8s.containsAscii(name, "second_table")) {
                        this.fd = fd;
                    }
                    return fd;
                }
            };

            final Map<String, String> env = new HashMap<>(System.getenv());
            env.put("QDB_CAIRO_COMMIT_MODE", "sync");

            final Bootstrap bootstrap = new Bootstrap(new DefaultBootstrapConfiguration() {
                @Override
                public Map<String, String> getEnv() {
                    return env;
                }

                @Override
                public FilesFacade getFilesFacade() {
                    return filesFacade;
                }
            }, Bootstrap.getServerMainArgs(root));

            try (final TestServerMain serverMain = new TestServerMain(bootstrap)) {
                serverMain.start();
                AtomicInteger walWriterTaken = countWalWriterTakenFromPool(serverMain);

                for (int i = 0; i < 10; i++) {
                    counter.set(2);
                    try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                        httpClientRef.set(httpClient);

                        HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                        request.POST()
                                .url("/write ")
                                .withContent()
                                .putAscii("first_table,ok=true allgood=true\n")
                                .putAscii("second_table,ok=true allgood=true\n")
                                .sendPartialContent(5000, 10000);
                        ping.countDown();
                        pong.await();
                    }
                }

                assertEventually(() -> {
                    // Assert no Wal Writers are left in ILP http TUD cache
                    Assert.assertEquals(0, walWriterTaken.get());
                });
            }
        });
    }

    @Test
    public void testClientDisconnectsMidRequest() throws Exception {
        assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    // Bombard server with partial content requests
                    String line = "line,sym1=123 field1=123i 1234567890000000000\n";
                    AtomicInteger walWriterTaken = countWalWriterTakenFromPool(serverMain);

                    for (int i = 0; i < 10; i++) {
                        HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                        request.POST()
                                .url("/write ")
                                .withContent()
                                .putAscii(line)
                                .putAscii(line)
                                .sendPartialContent(line.length() + 20, 1000);
                        Os.sleep(5);
                        httpClient.disconnect();
                    }

                    assertEventually(() -> {
                        // Table is create but no line should be committed
                        TableToken tt = serverMain.getEngine().getTableTokenIfExists("line");
                        Assert.assertNotNull(tt);
                        Assert.assertEquals(0, getSeqTxn(serverMain, tt));

                        // Assert no Wal Writers are left in ILP http TUD cache
                        Assert.assertEquals(0, walWriterTaken.get());
                    });

                    // Send good line, full request
                    try (
                            HttpClient.ResponseHeaders response = httpClient.newRequest("localhost", serverMain.getHttpServerPort()).POST()
                                    .url("/write ")
                                    .withContent()
                                    .putAscii(line)
                                    .send()
                    ) {
                        response.await();
                        TestUtils.assertEquals("204", response.getStatusCode());
                    }

                    serverMain.awaitTxn("line", 1);
                    serverMain.assertSql("select * from line",
                            "sym1\tfield1\ttimestamp\n" +
                                    "123\t123\t2009-02-13T23:31:30.000000Z\n");
                }
            }
        });
    }

    @Test
    public void testGzipBomb() throws Exception {
        assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "512",
                    PropertyKey.LINE_HTTP_MAX_RECV_BUFFER_SIZE.getEnvVarName(), "5M"
            )) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    // Create a gzip bomb: a small compressed payload that expands to a very large size
                    // This creates a 10KB buffer of 'a's that compresses to ~10KB bytes but will attempt to decompress to 10MB
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
                        byte[] raw = new byte[10 * 1024]; // 10KB of 'a's
                        Arrays.fill(raw, 0, raw.length, (byte) 'a');
                        for (int i = 0; i < 1000; i++) { // Write 1000 times = 10MB uncompressed
                            gzip.write(raw);
                        }
                    }
                    byte[] gzipBomb = out.toByteArray();

                    HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                    request.POST()
                            .url("/write")
                            .header("Content-Encoding", "gzip")
                            .withContent();

                    for (byte b : gzipBomb) {
                        request.put(b);
                    }

                    try (HttpClient.ResponseHeaders resp = request.send()) {
                        resp.await();
                        // Server should reject or handle the bomb gracefully
                        // Expecting 413 (Payload Too Large)
                        TestUtils.assertEquals("413", resp.getStatusCode());
                    }
                }

                // Verify no data was committed
                TableToken tt = serverMain.getEngine().getTableTokenIfExists("line");
                if (tt != null) {
                    Assert.assertEquals(0, getSeqTxn(serverMain, tt));
                }
            }
        });
    }

    @Test
    public void testGzipCorruptedData() throws Exception {
        assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    // Send corrupted gzip stream (valid header but corrupt data)
                    final byte[] corruptGzip = new byte[]{
                            0x1f, (byte) 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                            (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff
                    };

                    final HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort())
                            .POST()
                            .url("/write")
                            .header("Content-Encoding", "gzip")
                            .withContent();
                    for (byte b : corruptGzip) {
                        request.put(b);
                    }

                    try (HttpClient.ResponseHeaders resp = request.send()) {
                        resp.await();
                        TestUtils.assertEquals("415", resp.getStatusCode());
                    }
                }
            }
        });
    }

    @Test
    public void testGzipDisconnectDuringDecompression() throws Exception {
        assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    // Start a gzip header but disconnect mid-stream
                    final byte[] partialGzip = new byte[]{0x1f, (byte) 0x8b, 0x08, 0x00};

                    final HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort())
                            .POST()
                            .url("/write")
                            .header("Content-Encoding", "gzip")
                            .withContent();
                    for (byte b : partialGzip) {
                        request.put(b);
                    }

                    request.sendPartialContent(100, 500);
                    Os.sleep(100);
                    httpClient.disconnect();
                }

                // Verify no partial data was committed
                TableToken tt = serverMain.getEngine().getTableTokenIfExists("line");
                if (tt != null) {
                    Assert.assertEquals(0, getSeqTxn(serverMain, tt));
                }
            }
        });
    }

    @Test
    public void testGzipEmptyStream() throws Exception {
        assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());

                    try (
                            HttpClient.ResponseHeaders resp = request.POST()
                                    .url("/write")
                                    .header("Content-Encoding", "gzip")
                                    .withContent()
                                    .send()
                    ) {
                        resp.await();
                        TestUtils.assertEquals("204", resp.getStatusCode());
                    }
                }
            }
        });
    }

    @Test
    public void testGzipEncoding() throws Exception {
        assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();
                assertEventually(() -> Assert.assertTrue(serverMain.hasStarted()));

                try (Sender sender = Sender.fromConfig("http::addr=127.0.0.1:" + serverMain.getHttpServerPort() + ";protocol_version=1;auto_flush=off;")) {
                    sender.table("m1")
                            .symbol("tag1", "value1")
                            .doubleColumn("f1", 1)
                            .longColumn("x", 12)
                            .at(Instant.ofEpochSecond(123456));
                    DirectByteSlice rawBuffer = sender.bufferView();
                    byte[] b = new byte[rawBuffer.size()];
                    for (int i = 0; i < rawBuffer.size(); i++) {
                        b[i] = rawBuffer.byteAt(i);
                    }

                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    GZIPOutputStream strm = new GZIPOutputStream(out);
                    strm.write(b);
                    strm.finish();
                    byte[] outBytes = out.toByteArray();

                    try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
                        HttpClient.Request request = client
                                .newRequest("127.0.0.1", serverMain.getHttpServerPort()).POST()
                                .url("/write")
                                .header("User-Agent", "QuestDB/java/gzip_test")
                                .header("Content-Encoding", "gzip");
                        request.withContent();

                        for (byte outByte : outBytes) {
                            request.put(outByte);
                        }

                        try (HttpClient.ResponseHeaders response = request.send()) {
                            response.await();
                            Assert.assertEquals("204", response.getStatusCode().asAsciiCharSequence().toString());
                        }
                    }

                    serverMain.awaitTable("m1");
                    serverMain.assertSql(
                            "m1",
                            "tag1\tf1\tx\ttimestamp\n" +
                                    "value1\t1.0\t12\t1970-01-02T10:17:36.000000Z\n"
                    );
                }
            }
        });
    }

    @Test
    public void testPutAndGetAreNotSupported() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(DEBUG_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "5");
            }})) {
                serverMain.start();
                String line = "line,sym1=123 field1=123i 1234567890000000000\n";

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                    request.PUT()
                            .url("/write ")
                            .withContent()
                            .putAscii(line)
                            .putAscii(line);
                    assertResponse(request, HTTP_BAD_METHOD, "Method PUT not supported\r\n");
                }

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                    request.GET()
                            .url("/api/v2/write ")
                            .withContent()
                            .putAscii(line)
                            .putAscii(line);
                    assertResponse(request, HTTP_BAD_REQUEST, "GET request method cannot have content\r\n");
                }

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                    request.DELETE()
                            .url("/write ")
                            .withContent()
                            .putAscii(line)
                            .putAscii(line);
                    assertResponse(request, HTTP_BAD_METHOD, "Method DELETE not supported\r\n");
                }

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                    request.POST()
                            .url("/write ")
                            .putAscii(line)
                            .putAscii(line);
                    assertResponse(request, HTTP_BAD_REQUEST, "Content-length not specified for POST/PUT request\r\n");
                }
            }
        });
    }

    @Test
    public void testSlowPeerHeaderErrors() throws Exception {
        assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    DEBUG_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "20"
            )) {
                serverMain.start();
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                    try (
                            HttpClient.ResponseHeaders resp = request.POST()
                                    .url("/write?precision=ml ")
                                    .withContent()
                                    .putAscii("m1,tag1=value1 f1=1i,x=12i")
                                    .send()
                    ) {
                        resp.await();
                        TestUtils.assertEquals("400", resp.getStatusCode());
                    }
                }
            }
        });
    }

    @Test
    public void testUnsupportedPrecision() throws Exception {
        assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();
                String line = "line,sym1=123 field1=123i 1234567890000000000\n";

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                    try (
                            HttpClient.ResponseHeaders resp = request.POST()
                                    .url("/write?precision=ml ")
                                    .withContent()
                                    .putAscii(line)
                                    .putAscii(line)
                                    .send()
                    ) {
                        resp.await();
                        TestUtils.assertEquals("400", resp.getStatusCode());
                    }
                }
            }
        });
    }

    private long getSeqTxn(TestServerMain serverMain, TableToken tt) {
        return serverMain.getEngine().getTableSequencerAPI().getTxnTracker(tt).getSeqTxn();
    }
}
