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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;

/**
 * JMH latency benchmark for QWP egress with a bind-variable query. Measures
 * end-to-end wall time of {@code SELECT x FROM long_sequence(10) WHERE x = $1}
 * where {@code $1} is a random LONG in {@code [1, 10]} per invocation.
 * <p>
 * The value randomises per iteration but the bind TYPE (LONG) does not, so the
 * server's select cache should hit on every call after the first. Comparing
 * this benchmark against {@link QwpEgressLatencyBenchmark} running the same
 * shape with a hardcoded literal ({@code WHERE x = 5}) measures the overhead
 * of bind encoding / decoding / cache lookup relative to a pure-literal query
 * that hits the same cache entry every call.
 * <p>
 * Uses {@code long_sequence(10)} as the row source so the benchmark needs no
 * table setup and no WAL-apply wait -- the measurement window is pure
 * parse / factory-lookup / cursor-open / one-row-stream on both sides of the
 * wire.
 * <p>
 * Prerequisites:
 * <ul>
 *   <li>A QuestDB server listening on port 9000 (HTTP/WS).</li>
 * </ul>
 * <p>
 * Tune via system properties:
 * <ul>
 *   <li>{@code -Dsql=<text>} to override the benchmarked SQL. The override must
 *       still reference exactly one placeholder {@code $1} typed as LONG.</li>
 * </ul>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode({Mode.SampleTime, Mode.AverageTime})
public class QwpEgressBindLatencyBenchmark {

    private static final String HOST = "localhost";
    private static final int HTTP_PORT = 9000;
    private static final String SQL = System.getProperty(
            "sql", "SELECT x FROM long_sequence(10) WHERE x = $1");

    private QwpQueryClient client;
    private final QwpColumnBatchHandler handler = new QwpColumnBatchHandler() {
        @Override
        public void onBatch(QwpColumnBatch batch) {
            // Deliberately empty -- see the matching note in
            // QwpEgressLatencyBenchmark. We measure round-trip, not user-app
            // consumption work.
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
                .include(QwpEgressBindLatencyBenchmark.class.getSimpleName())
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
    public void selectWhereBind() {
        final long x = ThreadLocalRandom.current().nextLong(1, 11);
        client.execute(SQL, binds -> binds.setLong(0, x), handler);
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        client = QwpQueryClient.fromConfig(
                "ws::addr=" + HOST + ":" + HTTP_PORT + ";client_id=qwp-bind-latency-bench/1.0;");
        client.connect();
        // Prime the client's codec state AND the server's select cache: the
        // first execute() on a connection allocates client-side scratch and
        // the first compile of this SQL primes the server's factory cache.
        // Without this priming step the first measurement iteration pays a
        // visible outlier for both.
        client.execute(SQL, binds -> binds.setLong(0, 1L), handler);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (client != null) {
            client.close();
        }
    }
}
