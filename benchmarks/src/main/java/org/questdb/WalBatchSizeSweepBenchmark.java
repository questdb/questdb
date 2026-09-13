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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * SP-C commit-path BATCH-SIZE sweep. Companion to {@link WalCommitModeBenchmark}: instead of sweeping
 * named workloads, this fixes the 20-column schema (identical row generation) and single-writer path
 * and sweeps only <b>batch size</b> (rows per commit) against the three durability-relevant modes
 * (NOSYNC / SYNC / ADAPTIVE at W=0, zero-loss).
 *
 * <p><b>What it proves.</b> Each commit's fsync is a fixed per-commit cost. At {@code rowsPerCommit=1}
 * that cost dominates and ADAPTIVE (W=0, fdatasync the WAL events+sequencer every commit) costs far more
 * than NOSYNC per commit. As the batch grows, the single fsync amortizes over more rows, so ADAPTIVE's
 * per-<em>row</em> cost should converge onto NOSYNC's — at large batches zero-loss durability becomes
 * nearly free. The {@code us/op} JMH reports is per-<em>commit</em>; divide by {@code rowsPerCommit} for
 * the per-row cost that actually converges.
 *
 * <p><b>Single writer.</b> {@code @State(Scope.Benchmark)} + {@code @Setup(Level.Trial)}, no
 * {@code @Threads} — one {@link WalWriter}, one engine, per param combo (each combo is its own {@code -f 1}
 * fork, so setup/teardown bracket exactly one combo). The WAL apply job is deliberately NOT drained —
 * draining would force adaptive's lazy apply eagerly and erase its advantage; we measure the commit path
 * only. The batch is buffered in the memory-mapped WAL segment (off-heap), so even the 1M-row commit does
 * not stress the Java heap; disk holds the accumulated WAL (cleaned at teardown).
 *
 * <p>DB root is on real disk (/data) so fdatasync is a real syscall. Numbers are for RELATIVE comparison
 * between modes at the same batch size on the same box.
 *
 * <p>Run FAST from the lean core-jar classpath (NOT the shaded uber-jar — its FunctionFactory scan is
 * ~2 min/combo):
 * <pre>
 *   export JAVA_TOOL_OPTIONS="--sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED \
 *     --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
 *     --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"
 *   export QDB_LOG_W_STDOUT_LEVEL=ERROR
 *   mvn -q -pl benchmarks -am -DskipTests compile
 *   mvn -q -pl benchmarks dependency:build-classpath -Dmdep.outputFile=/tmp/bs-deps.txt
 *   CP="benchmarks/target/classes:$(cat /tmp/bs-deps.txt)"
 *   java -cp "$CP" org.openjdk.jmh.Main WalBatchSizeSweepBenchmark -bm avgt -f 1 -w 1 -wi 1 -r 1 -i 2 -foe true
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class WalBatchSizeSweepBenchmark {

    private static final long APPEND_PAGE_SIZE = 256 * 1024L;
    private static final int COLUMN_COUNT = 20;
    private static final String[] SYMBOLS = {"alpha", "beta", "gamma", "delta", "epsilon"};
    private static final String TABLE_NAME = "walbench";

    /**
     * Durability-relevant modes only. ADAPTIVE stays at W=0 (zero-loss): fdatasync the WAL events +
     * sequencer every commit, defer column materialization to the durable epoch.
     */
    @Param({"NOSYNC", "SYNC", "ADAPTIVE"})
    public String commitMode;

    /**
     * Batch size sweep: rows appended per commit. The x-axis of the amortization curve.
     */
    @Param({"1", "10", "100", "1000", "10000", "100000", "1000000"})
    public int rowsPerCommit;

    private final Rnd rnd = new Rnd();
    private final Utf8StringSink varcharSink = new Utf8StringSink();
    private String dbRoot;
    private CairoEngine engine;
    private int symbolColIndex;
    private long ts;
    private int varcharColIndex;
    private WalWriter walWriter;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(WalBatchSizeSweepBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .warmupTime(org.openjdk.jmh.runner.options.TimeValue.seconds(1))
                .measurementIterations(2)
                .measurementTime(org.openjdk.jmh.runner.options.TimeValue.seconds(1))
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void ingestAndCommit() {
        final int varIdx = varcharColIndex;
        final int symIdx = symbolColIndex;
        final long base = ts;
        final int rows = rowsPerCommit;
        for (int i = 0; i < rows; i++) {
            TableWriter.Row row = walWriter.newRow(base + i);
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
        ts = base + rows;
        walWriter.commit();
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        final String baseDir = new java.io.File("/data").isDirectory() ? "/data" : System.getProperty("user.home");
        dbRoot = baseDir + "/qdb-batchsweep-" + System.nanoTime();
        new java.io.File(dbRoot).mkdirs();

        final int mode = parseCommitMode(commitMode);
        final CairoConfiguration cfg = new DefaultCairoConfiguration(dbRoot) {
            @Override
            public long getAdaptiveCommitGroupWindowUs() {
                return 0; // W=0: zero-loss (fdatasync every commit)
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

        final int longCols = Math.max(0, COLUMN_COUNT - 2);
        final StringBuilder ddl = new StringBuilder("create table ").append(TABLE_NAME).append(" (ts timestamp");
        for (int c = 0; c < longCols; c++) {
            ddl.append(", c").append(c).append(" long");
        }
        varcharColIndex = 1 + longCols; // col 0 is ts
        symbolColIndex = varcharColIndex + 1;
        ddl.append(", v varchar, s symbol) timestamp(ts) partition by DAY wal");
        executeDdl(ddl.toString());

        final TableToken token = engine.verifyTableName(TABLE_NAME);
        walWriter = engine.getWalWriter(token);

        ts = 0;
        rnd.reset();
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        // Do NOT drain apply — we measure the commit path only.
        if (walWriter != null) {
            walWriter.close();
            walWriter = null;
        }
        if (engine != null) {
            engine.close();
            engine = null;
        }
        if (dbRoot != null) {
            deleteDirectory(new java.io.File(dbRoot));
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
}
