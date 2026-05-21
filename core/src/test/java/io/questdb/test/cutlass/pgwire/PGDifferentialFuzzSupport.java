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

import io.questdb.std.Misc;
import org.jetbrains.annotations.TestOnly;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

final class PGDifferentialFuzzSupport {
    private static final int CONNECT_TIMEOUT_MS = 250;
    private static final String HOST_ENV = "QDB_PGWIRE_POSTGRES_HOST";
    private static final String HOST_PROPERTY = "questdb.pgwire.postgres.host";
    private static final int MAX_OUTPUT_SIZE = 64 * 1024;
    private static final String PORT_ENV = "QDB_PGWIRE_POSTGRES_PORT";
    private static final String PORT_PROPERTY = "questdb.pgwire.postgres.port";
    private static final int READ_TIMEOUT_MS = 100;

    private static volatile PostgresEndpoint postgresEndpoint;

    private PGDifferentialFuzzSupport() {
    }

    @TestOnly
    static void closeForTest() {
        PGServerMainFuzzSupport.closeForTest();
        postgresEndpoint = null;
    }

    @TestOnly
    static boolean isPostgresConfigured() {
        return configuredValue(HOST_PROPERTY, HOST_ENV) != null || configuredValue(PORT_PROPERTY, PORT_ENV) != null;
    }

    static void send(byte[] input) throws Exception {
        final PostgresEndpoint endpoint = getPostgresEndpoint();
        if (endpoint.enabled) {
            try {
                sendToEndpoint(endpoint, input);
                verifyEndpoint(endpoint);
            } catch (IOException e) {
                throw new IllegalStateException("PostgreSQL fuzz oracle became unavailable after input; this is classified as oracle-side failure, not a QuestDB crash", e);
            }
        }
        try {
            PGServerMainFuzzSupport.send(input);
            PGServerMainFuzzSupport.verifyAcceptsConnection();
        } catch (IOException e) {
            throw new IllegalStateException("QuestDB pgwire ServerMain fuzz target became unavailable after input", e);
        }
    }

    private static String configuredValue(String property, String env) {
        final String value = System.getProperty(property);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        final String envValue = System.getenv(env);
        if (envValue != null && !envValue.isEmpty()) {
            return envValue;
        }
        return null;
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

    private static PostgresEndpoint getPostgresEndpoint() throws IOException {
        PostgresEndpoint endpoint = postgresEndpoint;
        if (endpoint != null) {
            return endpoint;
        }
        synchronized (PGDifferentialFuzzSupport.class) {
            endpoint = postgresEndpoint;
            if (endpoint != null) {
                return endpoint;
            }
            final String host = configuredValue(HOST_PROPERTY, HOST_ENV);
            final String portText = configuredValue(PORT_PROPERTY, PORT_ENV);
            if (host == null && portText == null) {
                endpoint = PostgresEndpoint.disabled();
                postgresEndpoint = endpoint;
                return endpoint;
            }
            final int port = portText == null ? 5432 : parsePort(portText);
            endpoint = new PostgresEndpoint(host == null ? "127.0.0.1" : host, port);
            try {
                verifyEndpoint(endpoint);
            } catch (IOException e) {
                throw new IllegalStateException("PostgreSQL fuzz oracle is configured but unavailable before input", e);
            }
            postgresEndpoint = endpoint;
            return endpoint;
        }
    }

    private static Socket openSocket(PostgresEndpoint endpoint) throws IOException {
        final Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(endpoint.host, endpoint.port), CONNECT_TIMEOUT_MS);
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(READ_TIMEOUT_MS);
            return socket;
        } catch (IOException e) {
            Misc.free(socket);
            throw e;
        }
    }

    private static int parsePort(String portText) {
        final int port;
        try {
            port = Integer.parseInt(portText);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid PostgreSQL fuzz oracle port: " + portText, e);
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("PostgreSQL fuzz oracle port out of range: " + port);
        }
        return port;
    }

    private static void sendToEndpoint(PostgresEndpoint endpoint, byte[] input) throws IOException {
        try (Socket socket = openSocket(endpoint)) {
            final OutputStream out = socket.getOutputStream();
            try {
                out.write(input);
                out.flush();
                socket.shutdownOutput();
                drainOutput(socket);
            } catch (SocketTimeoutException | SocketException ignored) {
                // The PostgreSQL sidecar is an oracle for process liveness here,
                // not for exact response content or protocol acceptance.
            }
        }
    }

    private static void verifyEndpoint(PostgresEndpoint endpoint) throws IOException {
        try (Socket ignored = openSocket(endpoint)) {
        }
    }

    private static final class PostgresEndpoint {
        private final boolean enabled;
        private final String host;
        private final int port;

        private PostgresEndpoint(String host, int port) {
            this.enabled = true;
            this.host = host;
            this.port = port;
        }

        private PostgresEndpoint() {
            this.enabled = false;
            this.host = "";
            this.port = 0;
        }

        private static PostgresEndpoint disabled() {
            return new PostgresEndpoint();
        }
    }
}
