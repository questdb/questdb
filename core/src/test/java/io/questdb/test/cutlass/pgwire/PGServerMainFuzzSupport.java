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

package io.questdb.test.cutlass.pgwire;

import io.questdb.Bootstrap;
import io.questdb.Metrics;
import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.std.Misc;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.TestOnly;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

final class PGServerMainFuzzSupport {
    private static final int CONNECT_TIMEOUT_MS = 250;
    private static final Object LOCK = new Object();
    private static final int MAX_OUTPUT_SIZE = 64 * 1024;
    private static final int READ_TIMEOUT_MS = 100;

    private static int port;
    private static Path root;
    private static boolean rootOwned;
    private static TestServerMain serverMain;
    private static boolean shutdownHookInstalled;

    private PGServerMainFuzzSupport() {
    }

    @TestOnly
    static void closeForTest() {
        synchronized (LOCK) {
            close();
        }
    }

    static void send(byte[] input) throws Exception {
        synchronized (LOCK) {
            ensureStarted();
        }
        sendToServer(input);
    }

    static void verifyAcceptsConnection() throws Exception {
        synchronized (LOCK) {
            ensureStarted();
        }
        try (Socket ignored = openSocket()) {
            // Opening a socket is enough to prove that the in-process pgwire
            // listener survived the previous fuzz input.
        }
    }

    private static void close() {
        serverMain = Misc.free(serverMain);
        port = 0;
        if (rootOwned && root != null) {
            TestUtils.removeTestPath(root.toString());
        }
        root = null;
        rootOwned = false;
    }

    private static void createConfiguration(Path root) throws IOException {
        final Path confDir = root.resolve("conf");
        Files.createDirectories(confDir);
        Files.createDirectories(root.resolve("db"));
        try (BufferedWriter writer = Files.newBufferedWriter(confDir.resolve("server.conf"), StandardCharsets.UTF_8)) {
            writeProperty(writer, PropertyKey.HTTP_ENABLED, "false");
            writeProperty(writer, PropertyKey.HTTP_MIN_ENABLED, "false");
            writeProperty(writer, PropertyKey.LINE_TCP_ENABLED, "false");
            writeProperty(writer, PropertyKey.LINE_HTTP_ENABLED, "false");
            writeProperty(writer, PropertyKey.LINE_UDP_ENABLED, "false");
            writeProperty(writer, PropertyKey.QWP_UDP_ENABLED, "false");
            writeProperty(writer, PropertyKey.PG_ENABLED, "true");
            writeProperty(writer, PropertyKey.PG_NET_BIND_TO, "127.0.0.1:0");
            writeProperty(writer, PropertyKey.PG_NET_CONNECTION_LIMIT, "8");
            writeProperty(writer, PropertyKey.PG_NET_ACCEPT_LOOP_TIMEOUT, "10");
            writeProperty(writer, PropertyKey.PG_SELECT_CACHE_ENABLED, "false");
            writeProperty(writer, PropertyKey.PG_INSERT_CACHE_ENABLED, "false");
            writeProperty(writer, PropertyKey.PG_WORKER_COUNT, "1");
            writeProperty(writer, PropertyKey.PG_RECV_BUFFER_SIZE, "65536");
            writeProperty(writer, PropertyKey.PG_SEND_BUFFER_SIZE, "65536");
            writeProperty(writer, PropertyKey.SHARED_WORKER_COUNT, "1");
            writeProperty(writer, PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, "false");
            writeProperty(writer, PropertyKey.METRICS_ENABLED, "false");
            writeProperty(writer, PropertyKey.TELEMETRY_ENABLED, "false");
            writeProperty(writer, PropertyKey.TELEMETRY_DISABLE_COMPLETELY, "true");
        }
        Files.writeString(confDir.resolve("mime.types"), "\n", StandardCharsets.UTF_8);
    }

    private static Path createRoot() throws IOException {
        final String configuredRoot = System.getProperty("questdb.pgwire.servermain.fuzz.root");
        if (configuredRoot != null && !configuredRoot.isEmpty()) {
            rootOwned = false;
            return Path.of(configuredRoot).toAbsolutePath();
        }
        rootOwned = true;
        return Files.createTempDirectory("questdb-pgwire-servermain-fuzz-").toAbsolutePath();
    }

    private static void drainOutput(Socket socket) throws IOException {
        final byte[] buffer = new byte[4096];
        final InputStream in = socket.getInputStream();
        int total = 0;
        while (total < MAX_OUTPUT_SIZE) {
            final int n = in.read(buffer, 0, Math.min(buffer.length, MAX_OUTPUT_SIZE - total));
            if (n < 0) {
                return;
            }
            total += n;
        }
    }

    private static void ensureStarted() throws Exception {
        if (serverMain != null) {
            return;
        }
        Metrics.ENABLED.clear();
        ColumnType.makeUtf8DefaultString();
        root = createRoot();
        createConfiguration(root);

        TestServerMain newServerMain = null;
        try {
            newServerMain = TestServerMain.createWithManualWalRun(Bootstrap.getServerMainArgs(root.toString()));
            newServerMain.start();
            serverMain = newServerMain;
            port = newServerMain.getPgWireServerPort();
            if (!shutdownHookInstalled) {
                Runtime.getRuntime().addShutdownHook(new Thread(PGServerMainFuzzSupport::close, "pgwire-servermain-fuzz-shutdown"));
                shutdownHookInstalled = true;
            }
        } catch (Throwable th) {
            Misc.free(newServerMain);
            close();
            throw th;
        }
    }

    private static Socket openSocket() throws IOException {
        final Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), CONNECT_TIMEOUT_MS);
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(READ_TIMEOUT_MS);
            return socket;
        } catch (IOException e) {
            Misc.free(socket);
            throw e;
        }
    }

    private static void sendToServer(byte[] input) throws IOException {
        try (Socket socket = openSocket()) {
            final OutputStream out = socket.getOutputStream();
            try {
                out.write(input);
                out.flush();
                socket.shutdownOutput();
                drainOutput(socket);
            } catch (SocketTimeoutException | SocketException ignored) {
                // Malformed peers commonly race server-side close or leave the
                // server waiting for more bytes. Both are acceptable outcomes.
            }
        }
    }

    private static void writeProperty(BufferedWriter writer, PropertyKey key, String value) throws IOException {
        writer.write(key.toString());
        writer.write('=');
        writer.write(value);
        writer.newLine();
    }
}
