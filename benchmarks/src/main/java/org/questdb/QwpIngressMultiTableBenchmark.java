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

package org.questdb;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Unsafe;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Ingestion throughput of one QWP WebSocket connection when its frames carry rows
 * of several tables. The rows cycle through the tables in order, and each frame
 * holds a fixed number of them, so with one row per frame the frames alternate
 * between tables, and with as many rows per frame as tables every frame carries
 * one row of each table. Every invocation sends {@link #ROWS_PER_INVOCATION} rows
 * and then waits for the server to ack all of them, so the score counts rows the
 * server has committed.
 * <p>
 * Two clients send the rows:
 * <ul>
 *     <li>{@code raw}: replays pre-encoded, pre-masked QWP frames over a plain
 *     socket and reads the cumulative acks on a second thread. It spends almost no
 *     CPU, so it measures the server.</li>
 *     <li>{@code java}: the Java client, {@code auto_flush_rows} set to the rows per
 *     frame. With one-row frames its I/O thread saturates first, so it measures
 *     the client.</li>
 * </ul>
 * Workloads, as {@code <tables>T_<rows per frame>R}:
 * <ul>
 *     <li>{@code 1T_1R}: one table, one-row frames;</li>
 *     <li>{@code 2T_1R}: two tables, one-row frames alternating between them;</li>
 *     <li>{@code 10T_1R}, {@code 100T_1R}: 10 or 100 tables round-robin, one-row frames;</li>
 *     <li>{@code 2T_2R}: two tables, every frame carries one row of each;</li>
 *     <li>{@code 1T_1000R}, {@code 2T_1000R}: 1,000-row frames of one or two tables.</li>
 * </ul>
 * The benchmark expects a QuestDB server on localhost: HTTP (QWP) on port 9000 and
 * PostgreSQL wire on port 8812, or the ports that {@code -Dqdb.bench.http.port} and
 * {@code -Dqdb.bench.pg.port} name (pass them to the fork with {@code -jvmArgsAppend}).
 * Pin the server and the benchmark to separate cores, for example:
 * <pre>
 * taskset -c 0-11 java --add-exports java.base/jdk.internal.vm=io.questdb -p questdb.jar \
 *     -m io.questdb/io.questdb.ServerMain -d /dev/shm/qdb-bench
 * taskset -c 16-23 java -jar benchmarks.jar QwpIngressMultiTableBenchmark
 * </pre>
 * The server's commit mode decides what the score means, and the benchmark cannot set
 * it: run one server per mode, with {@code cairo.commit.mode} and, for ADAPTIVE,
 * {@code cairo.adaptive.commit.group.window} (microseconds) in its {@code server.conf}
 * or in the {@code QDB_CAIRO_COMMIT_MODE} and {@code QDB_CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW}
 * environment variables. A root on tmpfs keeps the disk out of a NOSYNC measurement,
 * but voids SYNC and ADAPTIVE ones, whose cost is the device flush: give those a root
 * on a real disk.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@Fork(1)
public class QwpIngressMultiTableBenchmark {
    private static final long DRAIN_TIMEOUT_MILLIS = 120_000;
    private static final String HOST = "localhost";
    private static final int HTTP_PORT = Integer.getInteger("qdb.bench.http.port", 9000);
    private static final int PG_PORT = Integer.getInteger("qdb.bench.pg.port", 8812);
    private static final int ROWS_PER_INVOCATION = 1_000_000;
    private static final String TABLE_PREFIX = "qwp_mt_bench_";
    @Param({"raw", "java"})
    public String client;
    @Param({"1T_1R", "2T_1R", "10T_1R", "100T_1R", "2T_2R", "1T_1000R", "2T_1000R"})
    public String workload;
    private RawQwpConnection rawConnection;
    private int rowsPerFrame;
    private Sender sender;
    private String[] tableNames;
    private long timestampMicros;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(QwpIngressMultiTableBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    @OperationsPerInvocation(ROWS_PER_INVOCATION)
    public void ingest() throws Exception {
        if (rawConnection != null) {
            rawConnection.sendInvocation();
            rawConnection.awaitAllAcked(DRAIN_TIMEOUT_MILLIS);
            return;
        }
        final String[] names = tableNames;
        final int tableCount = names.length;
        long ts = timestampMicros;
        for (int i = 0; i < ROWS_PER_INVOCATION; i++) {
            sender.table(names[i % tableCount])
                    .doubleColumn("price", i * 0.25)
                    .longColumn("qty", i)
                    .at(ts++, ChronoUnit.MICROS);
        }
        timestampMicros = ts;
        if (!sender.drain(DRAIN_TIMEOUT_MILLIS)) {
            throw new IllegalStateException("server did not ack every row within " + DRAIN_TIMEOUT_MILLIS + " ms");
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        final int separator = workload.indexOf("T_");
        final int tableCount = Integer.parseInt(workload.substring(0, separator));
        rowsPerFrame = Integer.parseInt(workload.substring(separator + 2, workload.length() - 1));
        if (ROWS_PER_INVOCATION % rowsPerFrame != 0
                || (rowsPerFrame % tableCount != 0 && tableCount % rowsPerFrame != 0)) {
            throw new IllegalArgumentException("unsupported workload: " + workload);
        }
        tableNames = new String[tableCount];
        try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
            for (int i = 0; i < tableCount; i++) {
                tableNames[i] = TABLE_PREFIX + i;
                statement.execute("DROP TABLE IF EXISTS " + tableNames[i]);
                statement.execute("CREATE TABLE " + tableNames[i]
                        + " (price DOUBLE, qty LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            }
        }
        timestampMicros = TimeUnit.DAYS.toMicros(20_000);
        if ("raw".equals(client)) {
            rawConnection = new RawQwpConnection(
                    encodeCycle(tableNames, rowsPerFrame, timestampMicros),
                    ROWS_PER_INVOCATION / rowsPerFrame
            );
        } else {
            sender = Sender.fromConfig("ws::addr=" + HOST + ":" + HTTP_PORT + ";auto_flush_rows=" + rowsPerFrame + ";");
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (rawConnection != null) {
            rawConnection.close();
            rawConnection = null;
        }
        if (sender != null) {
            sender.close();
            sender = null;
        }
        try (Connection connection = createConnection(); Statement statement = connection.createStatement()) {
            for (String tableName : tableNames) {
                statement.execute("DROP TABLE IF EXISTS " + tableName);
            }
        }
    }

    private static Connection createConnection() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "disable");
        return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:" + PG_PORT + "/qdb", properties);
    }

    /**
     * Encodes the shortest run of masked WebSocket frames after which the row cycle
     * through the tables starts over. Every row carries the same timestamp, so
     * replaying the run never sends a row out of order.
     */
    private static EncodedCycle encodeCycle(String[] tableNames, int rowsPerFrame, long timestampMicros) {
        final int tableCount = tableNames.length;
        final int cycleRows = Math.max(tableCount, rowsPerFrame);
        final int frameCount = cycleRows / rowsPerFrame;
        final ByteArrayOutputStream frames = new ByteArrayOutputStream();
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            encoder.setDeferCommit(false);
            for (int frame = 0; frame < frameCount; frame++) {
                final int firstRow = frame * rowsPerFrame;
                final int frameTableCount = Math.min(tableCount, rowsPerFrame);
                encoder.beginMessage(frameTableCount, new GlobalSymbolDictionary(), -1, -1);
                for (int t = 0; t < frameTableCount; t++) {
                    final String tableName = tableNames[(firstRow + t) % tableCount];
                    try (QwpTableBuffer buffer = new QwpTableBuffer(tableName)) {
                        var price = buffer.getOrCreateColumn("price", QwpConstants.TYPE_DOUBLE, false);
                        var qty = buffer.getOrCreateColumn("qty", QwpConstants.TYPE_LONG, false);
                        var ts = buffer.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP);
                        for (int row = t; row < rowsPerFrame; row += frameTableCount) {
                            price.addDouble((firstRow + row) * 0.25);
                            qty.addLong(firstRow + row);
                            ts.addLong(timestampMicros);
                            buffer.nextRow();
                        }
                        encoder.addTable(buffer);
                    }
                }
                final int length = encoder.finishMessage();
                final long address = encoder.getBuffer().getBufferPtr();
                final byte[] payload = new byte[length];
                for (int i = 0; i < length; i++) {
                    payload[i] = Unsafe.getUnsafe().getByte(address + i);
                }
                writeMaskedBinaryFrame(frames, payload);
            }
        }
        return new EncodedCycle(frames.toByteArray(), frameCount);
    }

    private static void writeMaskedBinaryFrame(ByteArrayOutputStream out, byte[] payload) {
        final byte[] mask = {0x12, 0x34, 0x56, 0x78};
        out.write(0x82); // FIN, BINARY
        if (payload.length < 126) {
            out.write(0x80 | payload.length);
        } else if (payload.length < 65_536) {
            out.write(0x80 | 126);
            out.write(payload.length >>> 8);
            out.write(payload.length & 0xFF);
        } else {
            out.write(0x80 | 127);
            for (int shift = 56; shift >= 0; shift -= 8) {
                out.write((int) ((long) payload.length >>> shift) & 0xFF);
            }
        }
        out.write(mask, 0, 4);
        for (int i = 0; i < payload.length; i++) {
            out.write(payload[i] ^ mask[i & 3]);
        }
    }

    private record EncodedCycle(byte[] bytes, int frameCount) {
    }

    /**
     * A QWP ingress connection that replays one encoded cycle of frames and tracks
     * the server's cumulative OK acks. Any other response fails the benchmark.
     */
    private static final class RawQwpConnection implements AutoCloseable {
        private final int framesPerInvocation;
        private final InputStream in;
        private final OutputStream out;
        private final Thread reader;
        private final byte[] sendBuffer;
        private final Socket socket;
        private volatile long ackedSeq = -1;
        private volatile String failure;
        private long sentFrames;

        RawQwpConnection(EncodedCycle cycle, int framesPerInvocation) throws IOException {
            if (framesPerInvocation % cycle.frameCount() != 0) {
                throw new IllegalArgumentException("an invocation must send whole cycles");
            }
            final int cycles = framesPerInvocation / cycle.frameCount();
            this.sendBuffer = new byte[cycles * cycle.bytes().length];
            for (int i = 0; i < cycles; i++) {
                System.arraycopy(cycle.bytes(), 0, sendBuffer, i * cycle.bytes().length, cycle.bytes().length);
            }
            this.framesPerInvocation = framesPerInvocation;
            this.socket = new Socket(HOST, HTTP_PORT);
            socket.setTcpNoDelay(true);
            this.out = socket.getOutputStream();
            this.in = socket.getInputStream();
            upgrade();
            this.reader = new Thread(this::readAcks, "qwp-bench-ack-reader");
            reader.setDaemon(true);
            reader.start();
        }

        @Override
        public void close() throws Exception {
            socket.close();
            reader.join(TimeUnit.SECONDS.toMillis(10));
        }

        void awaitAllAcked(long timeoutMillis) {
            final long deadline = System.currentTimeMillis() + timeoutMillis;
            final long target = sentFrames - 1;
            while (ackedSeq < target) {
                if (failure != null) {
                    throw new IllegalStateException(failure);
                }
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException("server did not ack every frame within " + timeoutMillis
                            + " ms [acked=" + ackedSeq + ", sent=" + sentFrames + ']');
                }
                Thread.onSpinWait();
            }
        }

        void sendInvocation() throws IOException {
            out.write(sendBuffer);
            sentFrames += framesPerInvocation;
        }

        private static long readLeLong(byte[] buf, int offset) {
            long v = 0;
            for (int i = 7; i >= 0; i--) {
                v = (v << 8) | (buf[offset + i] & 0xFFL);
            }
            return v;
        }

        private void readAcks() {
            final byte[] header = new byte[8];
            byte[] payload = new byte[1024];
            try {
                while (true) {
                    readFully(header, 2);
                    final int opcode = header[0] & 0x0F;
                    long length = header[1] & 0x7F;
                    if (length == 126) {
                        readFully(header, 2);
                        length = ((header[0] & 0xFF) << 8) | (header[1] & 0xFF);
                    } else if (length == 127) {
                        readFully(header, 8);
                        length = 0;
                        for (int i = 0; i < 8; i++) {
                            length = (length << 8) | (header[i] & 0xFF);
                        }
                    }
                    if (payload.length < length) {
                        payload = new byte[(int) length];
                    }
                    readFully(payload, (int) length);
                    if (opcode != 0x02) {
                        failure = "unexpected server frame [opcode=" + opcode + ']';
                        return;
                    }
                    if (payload[0] != QwpConstants.STATUS_OK) {
                        failure = "server rejected a frame [status=" + payload[0] + ", seq=" + readLeLong(payload, 1)
                                + ", message=" + new String(payload, 9, (int) length - 9, StandardCharsets.UTF_8) + ']';
                        return;
                    }
                    ackedSeq = Math.max(ackedSeq, readLeLong(payload, 1));
                }
            } catch (IOException e) {
                if (!socket.isClosed()) {
                    failure = "ack reader failed: " + e;
                }
            }
        }

        private void readFully(byte[] buf, int length) throws IOException {
            int pos = 0;
            while (pos < length) {
                final int n = in.read(buf, pos, length - pos);
                if (n < 0) {
                    throw new IOException("server closed the connection");
                }
                pos += n;
            }
        }

        private void upgrade() throws IOException {
            final byte[] key = new byte[16];
            for (int i = 0; i < key.length; i++) {
                key[i] = (byte) (i * 7 + 1);
            }
            final String request = "GET /write/v4 HTTP/1.1\r\n"
                    + "Host: " + HOST + ':' + HTTP_PORT + "\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Sec-WebSocket-Key: " + Base64.getEncoder().encodeToString(key) + "\r\n"
                    + "Sec-WebSocket-Version: 13\r\n"
                    + "\r\n";
            out.write(request.getBytes(StandardCharsets.US_ASCII));
            out.flush();
            final StringBuilder response = new StringBuilder();
            while (!response.toString().endsWith("\r\n\r\n")) {
                final int b = in.read();
                if (b < 0) {
                    throw new IOException("server closed the connection during the upgrade: " + response);
                }
                response.append((char) b);
            }
            if (!response.toString().startsWith("HTTP/1.1 101")) {
                throw new IOException("upgrade refused: " + response);
            }
        }
    }
}
