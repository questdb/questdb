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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogLevel;
import io.questdb.log.LogRecordUtf8Sink;
import io.questdb.log.LogWriter;
import io.questdb.log.LogWriterConfig;
import io.questdb.mp.CarrierIdentity;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.std.Os;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Measures the producer-side cost of a log call: formatting a message and
 * handing it to the log queue. A dedicated thread drains the queue in a busy
 * loop without writing anything, so the numbers exclude file I/O and do not
 * depend on how fast an idle logging worker wakes up.
 * <p>
 * The benchmark uses only the public logging API, so the same source compiles
 * against different logging implementations: to compare two implementations,
 * run it on each build, e.g.
 * {@code java -jar benchmarks/target/benchmarks.jar LogProducerBenchmark}.
 * <p>
 * Benchmark threads bind a carrier identity like QuestDB worker threads do, so
 * per-carrier state takes the same path as in production. Run with
 * {@code -t N} to measure contention between N producers.
 * <p>
 * The non-waiting benchmarks ({@code info()}) drop messages when the queue is
 * full; with several producers their numbers mix published and dropped calls.
 * The waiting benchmarks ({@code infoW()}, {@code errorW()}) publish every
 * message and are the better basis for comparison.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(2)
public class LogProducerBenchmark {
    private static final String QUERY = "SELECT symbol, avg(price), sum(amount) FROM trades WHERE timestamp IN today() SAMPLE BY 1h";
    private Thread consumer;
    private volatile DrainWriter drainWriter;
    private RuntimeException exception;
    private LogFactory factory;
    private volatile boolean isRunning;
    private Log log;
    private Log logWaiting;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(LogProducerBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setUp() {
        Os.init();
        factory = new LogFactory();
        // one writer for both levels: INFO and ERROR share a single queue
        factory.add(new LogWriterConfig(
                LogLevel.INFO | LogLevel.ERROR,
                (ring, seq, level) -> drainWriter = new DrainWriter(ring, seq)
        ));
        // no factory.startThread(): the consumer below replaces the logging worker
        factory.bind();
        isRunning = true;
        final DrainWriter writer = drainWriter;
        consumer = new Thread(() -> {
            while (isRunning) {
                if (!writer.drain()) {
                    Os.pause();
                }
            }
        }, "log-bench-consumer");
        consumer.setDaemon(true);
        consumer.start();
        log = factory.create("bench-log", false);
        logWaiting = factory.create("bench-log-waiting", true);
        exception = new RuntimeException("could not open file", new IllegalStateException("disk full"));
    }

    @TearDown(Level.Trial)
    public void tearDown() throws InterruptedException {
        isRunning = false;
        consumer.join();
        factory.close();
    }

    // DEBUG is not enabled, so this measures a disabled log call
    @Benchmark
    public void testDisabled(ProducerState state) {
        log.debug().$("brown fox jumped over ").$(state.counter++).$(" fence").$();
    }

    // exception with a cause; the stack trace comes from the JMH setup call chain
    @Benchmark
    public void testExceptionWaiting(ProducerState state) {
        logWaiting.errorW().$("could not process [id=").$(state.counter++).$(", e=").$(exception).I$();
    }

    // ~260 B: typical structured log line
    @Benchmark
    public void testMediumWaiting(ProducerState state) {
        final long n = state.counter++;
        logWaiting.infoW().$("query executed [table=").$("trades")
                .$(", fd=").$(n & 0xffff)
                .$(", rows=").$(n)
                .$(", elapsedNs=").$(n * 31)
                .$(", ts=").$ts(1_700_000_000_000_000L + n)
                .$(", query=").$safe(QUERY)
                .I$();
    }

    // ~80 B, non-waiting: drops the message when the queue is full
    @Benchmark
    public void testShort(ProducerState state) {
        log.info().$("brown fox jumped over ").$(state.counter++).$(" fence").$();
    }

    // ~80 B, waiting: publishes every message
    @Benchmark
    public void testShortWaiting(ProducerState state) {
        logWaiting.infoW().$("brown fox jumped over ").$(state.counter++).$(" fence").$();
    }

    @State(Scope.Thread)
    public static class ProducerState {
        long counter;

        @Setup(Level.Trial)
        public void setUp() {
            Os.init();
            // QuestDB worker threads are carrier-bound; mirror that
            CarrierIdentity.bind();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            CarrierIdentity.unbind();
        }
    }

    private static class DrainWriter implements LogWriter {
        private final RingQueue<LogRecordUtf8Sink> ring;
        private final SCSequence seq;
        private long bytes;

        DrainWriter(RingQueue<LogRecordUtf8Sink> ring, SCSequence seq) {
            this.ring = ring;
            this.seq = seq;
        }

        boolean drain() {
            return seq.consumeAll(ring, this::consume);
        }

        @Override
        public void bindProperties(LogFactory factory) {
        }

        @Override
        public boolean run(@NotNull WorkerContext workerContext) {
            return drain();
        }

        private void consume(LogRecordUtf8Sink record) {
            // read the record, so the consumer touches the published bytes
            bytes += record.size() + record.byteAt(0);
        }
    }
}
