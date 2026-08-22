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

package org.questdb;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * JMH latency benchmark for QWP egress. Measures end-to-end wall time of a
 * single {@code SELECT id FROM latency_bench} query against a locally running
 * QuestDB, excluding connection setup (the {@link QwpQueryClient} is opened
 * once per trial and reused across every benchmarked invocation).
 * <p>
 * Runs two modes on each invocation:
 * <ul>
 *   <li>{@code SampleTime} -- reports p50/p90/p99/p99.9 percentiles per
 *       iteration. This is the main signal; interactive UX is gated by the
 *       tail, not the mean.</li>
 *   <li>{@code AverageTime} -- reports the arithmetic mean. Useful when
 *       comparing two builds: a smaller mean with an unchanged tail is usually
 *       the honest win (no outlier distortion).</li>
 * </ul>
 * <p>
 * Prerequisites:
 * <ul>
 *   <li>A QuestDB server listening on 9000 (HTTP/WS) and 8812 (PG wire).</li>
 * </ul>
 * <p>
 * Tune via system properties:
 * <ul>
 *   <li>{@code -Dskip.populate=true} to re-use an existing {@code latency_bench}
 *       table instead of dropping and recreating it in {@code @Setup}.</li>
 *   <li>{@code -Dsql=<text>} to override the benchmarked SQL. Default is
 *       {@code SELECT id FROM latency_bench} (one row from one LONG column).
 *       Use this to isolate specific latency contributors -- e.g. {@code SELECT 1}
 *       removes storage / cursor cost, leaving parse + protocol round-trip.</li>
 * </ul>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode({Mode.SampleTime, Mode.AverageTime})
public class QwpEgressLatencyBenchmark {

    private static final String HOST = "localhost";
    private static final int HTTP_PORT = 9000;
    private static final int PG_PORT = 8812;
    private static final String TABLE = "latency_bench";
    private static final boolean SKIP_POPULATE = Boolean.parseBoolean(System.getProperty("skip.populate", "false"));
    private static final String SQL = System.getProperty("sql", "SELECT 1");

    private QwpQueryClient client;
    private final QwpColumnBatchHandler handler = new QwpColumnBatchHandler() {
        @Override
        public void onBatch(QwpColumnBatch batch) {
            // Deliberately empty: we're measuring server-side + wire + decode
            // round-trip, not what the user app does with the row. A caller
            // doing real work (e.g. Blackhole.consume the row values) would
            // observe strictly greater latency, but the delta is user-app
            // code, not our responsibility to measure here.
        }

        @Override
        public void onEnd(long totalRows) {
        }

        @Override
        public void onError(byte status, String message) {
            throw new RuntimeException("query error [status=" + status + "]: " + message);
        }
    };

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(QwpEgressLatencyBenchmark.class.getSimpleName())
                // Five warmup iterations at two seconds each so the JIT gets
                // past C2 tiering and the OS page cache is hot before we
                // record samples.
                .warmupIterations(5)
                .warmupTime(TimeValue.seconds(2))
                .measurementIterations(10)
                .measurementTime(TimeValue.seconds(2))
                .threads(1)
                .forks(2)
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void selectSingleRow() {
        client.execute(SQL, handler);
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        if (!SKIP_POPULATE) {
            recreateTable();
            ingestOneRow();
        } else {
            System.out.println("skip.populate=true, re-using existing " + TABLE);
        }
        client = QwpQueryClient.fromConfig(
                "ws::addr=" + HOST + ":" + HTTP_PORT + ";client_id=qwp-latency-bench/1.0;");
        client.connect();
        // Prime the client's codec state: first execute() on a connection
        // allocates various scratch buffers and registers the schema in the
        // client's schemaRegistry. Running a throwaway query in setUp keeps
        // those one-time costs out of the measurement window.
        client.execute(SQL, handler);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    private static Connection createPgConnection() throws Exception {
        Properties p = new Properties();
        p.setProperty("user", "admin");
        p.setProperty("password", "quest");
        p.setProperty("sslmode", "disable");
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        return DriverManager.getConnection(
                String.format("jdbc:postgresql://%s:%d/qdb", HOST, PG_PORT), p);
    }

    private static void ingestOneRow() throws Exception {
        try (Sender sender = Sender.fromConfig("ws::addr=" + HOST + ":" + HTTP_PORT + ";")) {
            sender.table(TABLE)
                    .longColumn("id", 1)
                    .at(0, ChronoUnit.MICROS);
            sender.flush();
        }
        // Wait for WAL apply before returning from setUp -- otherwise the
        // first benchmarked query might fire while the table is still empty.
        try (Connection c = createPgConnection(); Statement st = c.createStatement()) {
            for (int attempt = 0; attempt < 600; attempt++) {
                try (ResultSet rs = st.executeQuery("SELECT count() FROM " + TABLE)) {
                    rs.next();
                    if (rs.getLong(1) == 1) {
                        return;
                    }
                }
                Thread.sleep(100);
            }
            throw new AssertionError("timed out waiting for WAL apply");
        }
    }

    private static void recreateTable() throws Exception {
        try (Connection c = createPgConnection(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + TABLE);
            st.execute("CREATE TABLE " + TABLE + " (id LONG, ts TIMESTAMP) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL");
        }
    }
}
