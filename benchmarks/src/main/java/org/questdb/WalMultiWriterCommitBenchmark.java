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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Files;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8StringSink;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SP-C <b>multi-writer</b> commit-path benchmark — the concurrent-writer group-commit measurement the
 * single-writer {@link WalCommitModeBenchmark} could not make (SP-C spec §8/§9: "the most important
 * missing measurement for the W&gt;0 story"). Five WAL writers commit <em>concurrently</em>, each to its
 * OWN table, through ONE shared {@link CairoEngine}. The whole point is that adaptive's group-commit
 * flush registry ({@code WalGroupCommitFlushQueue}) is <b>engine-wide</b>, so under {@code W>0} the
 * deferred device flushes of many concurrent writers can amortize against one another (and against the
 * underlying device's write-cache flush) — a single-writer harness structurally cannot see this.
 *
 * <p><b>What is measured:</b> {@link Mode#AverageTime} us/op, aggregated by JMH across all 5 writer
 * threads. Compare the 5-writer per-op avgt at a given {@code (commitMode, groupWindowUs)} against the
 * single-writer avgt at the same point:
 * <ul>
 *   <li>avgt roughly FLAT going 1→5 writers ⇒ the commit path scales ~linearly (aggregate throughput
 *       {@code 5 / avgt} ≈ 5×) — the group-commit flush did NOT serialize the 5 writers at the device.</li>
 *   <li>avgt LOWER than single-writer ⇒ super-linear: concurrent deferred flushes coalesced (the
 *       amortization hypothesis).</li>
 *   <li>avgt approaching 5× single-writer ⇒ the device flush fully serialized the writers (no win).</li>
 * </ul>
 *
 * <p><b>How {@code W>0} is made faithful (studied from {@link WalCommitModeBenchmark}):</b> the deferred
 * batched device flush is driven by the <em>commit-driven trigger</em> in {@code WalWriter.commit0 ->
 * recordPendingDurable}: because every writer commits CONTINUOUSLY here, each writer's own subsequent
 * commit flushes its backlog once the oldest un-flushed commit is {@code >= W} old — no background
 * {@code WalPurgeJob} sweep is needed (that sweep only exists for the IDLE-tail case where commits stop).
 * This mirrors {@link WalCommitModeBenchmark} exactly, which likewise relies on the commit-driven trigger
 * and runs no purge job. The apply path is deliberately never driven (draining would force adaptive's
 * lazy apply and erase its advantage — and materializing the accumulated WAL at teardown would write GBs
 * for the fast modes); the WAL is simply discarded with the db-root at teardown.
 *
 * <p><b>Workload:</b> SMALL_BATCH only (5 rows/commit, 20-col schema, in-order) — the per-commit-latency
 * lens where the group window matters most (large-batch ingest already amortizes the fdatasync over the
 * batch and is nearly W-insensitive; SP-C 5.2). Deterministic rows (seeded {@link Rnd}), identical schema
 * to {@link WalCommitModeBenchmark}'s SMALL_BATCH so the single↔multi comparison is apples-to-apples.
 *
 * <p>DB root is on real disk ({@code /data}, xfs) so {@code fdatasync} is a real syscall. Numbers are for
 * RELATIVE comparison on one box (shared dev box: direction, not absolutes — SP-C §5/§8).
 *
 * <p>Run (lean core-jar classpath, NOT the shaded uber-jar — its FunctionFactory scan is ~2 min/combo):
 * <pre>
 * export JAVA_TOOL_OPTIONS="--sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED \
 *   --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
 *   --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"
 * export QDB_LOG_W_STDOUT_LEVEL=ERROR
 * mvn -q -pl benchmarks -am -DskipTests compile
 * mvn -q -pl benchmarks dependency:build-classpath -Dmdep.outputFile=/tmp/mw-deps.txt
 * CP="benchmarks/target/classes:$(cat /tmp/mw-deps.txt)"
 * java -cp "$CP" org.openjdk.jmh.Main WalMultiWriterCommitBenchmark -bm avgt -f 1 -w 1 -wi 1 -r 1 -i 2 \
 *   -p commitMode=NOSYNC,SYNC,ADAPTIVE -p groupWindowUs=0,5000,50000 -foe true
 * </pre>
 * KEEP JMH TIMES SHORT (never the 10s default) and SMALL_BATCH only — a fast, low-footprint run.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 2, time = 1)
@Fork(1)
@Threads(WalMultiWriterCommitBenchmark.WRITER_COUNT)
public class WalMultiWriterCommitBenchmark {

    /**
     * Concurrent writers == tables == JMH threads.
     */
    static final int WRITER_COUNT = 5;

    private static final long APPEND_PAGE_SIZE = 256 * 1024L;
    private static final int COLUMN_COUNT = 20;      // matches WalCommitModeBenchmark SMALL_BATCH
    private static final String DB_ROOT_PREFIX = "qdb-mwbench-";
    private static final int ROWS_PER_COMMIT = 5;    // SMALL_BATCH
    private static final String[] SYMBOLS = {"alpha", "beta", "gamma", "delta", "epsilon"};
    private static final String TABLE_PREFIX = "mwbench";

    @Param({"NOSYNC", "SYNC", "ADAPTIVE"})
    public String commitMode;

    /**
     * ADAPTIVE group-commit window in microseconds (the RPO knob; ignored by NOSYNC/SYNC). 0 = W=0,
     * zero-loss (fdatasync every commit). W&gt;0 batches the device flush per window (RPO up to W). Sweep
     * {@code 0,5000,50000} = W0 / W5ms / W50ms to trace the concurrent RPO ↔ throughput curve.
     */
    @Param({"0"})
    public long groupWindowUs;

    // ---- Benchmark-scope shared state (ONE engine, WRITER_COUNT tables) ----
    private String dbRoot;
    private CairoEngine engine;
    private int symbolColIndex;
    // Hands out a distinct 0..WRITER_COUNT-1 index to each JMH worker thread's @Setup.
    private final AtomicInteger threadIndexer = new AtomicInteger();
    private TableToken[] tokens;
    private int varcharColIndex;
    // Teardown barrier: each thread-scope tearDown() closes its writer then counts down; the
    // benchmark-scope tearDownTrial() awaits ALL WRITER_COUNT closes before engine.close(). JMH inserts
    // NO barrier between thread-scope and trial-scope teardown (verified in the generated jmhTest: each
    // worker runs writerstate.tearDown() then one runs tearDownTrial() with nothing forcing the former to
    // finish first), so without this latch engine.close() can race a still-open writer and throw
    // "table is left behind on pool shutdown" — which reproduced deterministically under ADAPTIVE W>0.
    private java.util.concurrent.CountDownLatch writersClosed;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(WalMultiWriterCommitBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .warmupTime(org.openjdk.jmh.runner.options.TimeValue.seconds(1))
                .measurementIterations(2)
                .measurementTime(org.openjdk.jmh.runner.options.TimeValue.seconds(1))
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void ingestAndCommit(WriterState st) {
        final WalWriter w = st.walWriter;
        final Rnd rnd = st.rnd;
        final Utf8StringSink varcharSink = st.varcharSink;
        final int varIdx = varcharColIndex;
        final int symIdx = symbolColIndex;
        final long base = st.ts;
        for (int i = 0; i < ROWS_PER_COMMIT; i++) {
            TableWriter.Row row = w.newRow(base + i);
            for (int c = 1; c < varIdx; c++) {
                row.putLong(c, rnd.nextLong());
            }
            int varLen = 8 + (rnd.nextPositiveInt() % 57);
            varcharSink.clear();
            rnd.nextUtf8AsciiStr(varLen, varcharSink);
            row.putVarchar(varIdx, varcharSink);
            row.putSym(symIdx, SYMBOLS[rnd.nextPositiveInt() % SYMBOLS.length]);
            row.append();
        }
        st.ts = base + ROWS_PER_COMMIT;
        w.commit();
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        final String baseDir = new java.io.File("/data").isDirectory() ? "/data" : System.getProperty("user.home");
        dbRoot = baseDir + "/" + DB_ROOT_PREFIX + System.nanoTime();
        new java.io.File(dbRoot).mkdirs();

        final int mode = parseCommitMode(commitMode);
        final CairoConfiguration cfg = new DefaultCairoConfiguration(dbRoot) {
            @Override
            public long getAdaptiveCommitGroupWindowUs() {
                return groupWindowUs; // W=0: zero-loss; W>0: batch commits per flush (RPO up to W)
            }

            @Override
            public int getCommitMode() {
                return mode;
            }

            @Override
            public long getDataAppendPageSize() {
                return APPEND_PAGE_SIZE;
            }

            @Override
            public long getMiscAppendPageSize() {
                return Files.ceilPageSize(APPEND_PAGE_SIZE);
            }
        };

        engine = new CairoEngine(cfg);

        // WRITER_COUNT identical WAL tables sharing the one engine (so they share the engine-wide
        // group-commit flush queue). Schema is identical to WalCommitModeBenchmark SMALL_BATCH.
        final int longCols = Math.max(0, COLUMN_COUNT - 2);
        varcharColIndex = 1 + longCols; // col 0 is ts
        symbolColIndex = varcharColIndex + 1;
        tokens = new TableToken[WRITER_COUNT];
        for (int t = 0; t < WRITER_COUNT; t++) {
            final String tableName = TABLE_PREFIX + t;
            final StringBuilder ddl = new StringBuilder("create table ").append(tableName).append(" (ts timestamp");
            for (int c = 0; c < longCols; c++) {
                ddl.append(", c").append(c).append(" long");
            }
            ddl.append(", v varchar, s symbol) timestamp(ts) partition by DAY wal");
            executeDdl(ddl.toString());
            tokens[t] = engine.verifyTableName(tableName);
        }
        threadIndexer.set(0);
        writersClosed = new java.util.concurrent.CountDownLatch(WRITER_COUNT);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        // Writers are closed by WriterState.tearDown (thread scope). Wait for ALL WRITER_COUNT of them to
        // finish (and thus return to the pool + deregister from the group-commit flush queue) before
        // closing the engine — JMH does not order thread-teardown before trial-teardown, so this barrier
        // is what prevents the "table is left behind on pool shutdown" race (see the field's comment).
        if (writersClosed != null) {
            try {
                if (!writersClosed.await(60, TimeUnit.SECONDS)) {
                    System.err.println("WalMultiWriterCommitBenchmark: timed out waiting for writers to close");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            writersClosed = null;
        }
        // Apply is deliberately NOT drained (see class javadoc): the WAL is discarded with the db-root.
        // Close the engine, then wipe the (distinctive) db-root.
        if (engine != null) {
            engine.close();
            engine = null;
        }
        if (dbRoot != null) {
            deleteDirectory(new java.io.File(dbRoot));
            // Final safety rm -rf on the distinctive prefix, belt-and-suspenders against any leftover fd
            // that briefly held a file open during the Java delete.
            if (dbRoot.contains(DB_ROOT_PREFIX)) {
                try {
                    new ProcessBuilder("rm", "-rf", dbRoot).start().waitFor();
                } catch (Exception ignored) {
                }
            }
            dbRoot = null;
        }
    }

    private static void deleteDirectory(java.io.File dir) {
        if (dir == null || !dir.exists()) return;
        java.io.File[] children = dir.listFiles();
        if (children != null) {
            for (java.io.File child : children) {
                if (child.isDirectory()) deleteDirectory(child);
                else child.delete();
            }
        }
        dir.delete();
    }

    private static int parseCommitMode(String name) {
        return switch (name) {
            case "NOSYNC" -> CommitMode.NOSYNC;
            case "ASYNC" -> CommitMode.ASYNC;
            case "SYNC" -> CommitMode.SYNC;
            case "ADAPTIVE" -> CommitMode.ADAPTIVE;
            default -> throw new IllegalArgumentException("Unknown commit mode: " + name);
        };
    }

    private void executeDdl(String ddl) {
        SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                .with(
                        engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null,
                        null,
                        -1,
                        null
                );
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            CairoEngine.execute(compiler, ddl, ctx, null);
        } catch (SqlException e) {
            throw new RuntimeException("DDL failed: " + ddl, e);
        }
    }

    /**
     * Per-thread writer state. Each of the {@link #WRITER_COUNT} JMH worker threads grabs a distinct table
     * index (0..WRITER_COUNT-1) at trial setup and holds that table's own {@link WalWriter} obtained from
     * the shared engine, so the 5 threads never contend on a writer object — only (deliberately) on the
     * shared engine and the underlying device.
     */
    @State(Scope.Thread)
    public static class WriterState {
        final Rnd rnd = new Rnd();
        final Utf8StringSink varcharSink = new Utf8StringSink();
        long ts;
        WalWriter walWriter;

        @Setup(Level.Trial)
        public void setup(WalMultiWriterCommitBenchmark bench) {
            final int idx = bench.threadIndexer.getAndIncrement() % WRITER_COUNT;
            walWriter = bench.engine.getWalWriter(bench.tokens[idx]);
            ts = 0;
            rnd.reset();
        }

        @TearDown(Level.Trial)
        public void tearDown(WalMultiWriterCommitBenchmark bench) {
            try {
                if (walWriter != null) {
                    try {
                        walWriter.commit();
                    } catch (Exception ignored) {
                    }
                    walWriter.close(); // returns this writer to the shared engine's pool
                    walWriter = null;
                }
            } finally {
                // Signal the trial-teardown barrier even if close threw, so engine.close() is never
                // blocked forever by a single failed writer.
                bench.writersClosed.countDown();
            }
        }
    }
}
