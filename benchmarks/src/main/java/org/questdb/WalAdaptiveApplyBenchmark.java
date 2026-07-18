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
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
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
 * SP-C apply-path benchmark: isolates the cost of the ADAPTIVE durable EPOCH on the WAL apply worker.
 *
 * <p>Unlike {@link WalCommitModeBenchmark} (which never drains apply so it measures only the commit
 * path), this benchmark drains apply on every invocation, because the durable epoch fires on the apply
 * path ({@code ApplyWal2TableJob.maybeAdvanceDurableEpoch}), not on commit. Each invocation appends one
 * in-order batch, commits, then drains the WAL queue — so the measured op = ingest + apply (+ the epoch
 * fsync when the cadence fires).
 *
 * <p><b>Epoch-overhead axis</b> — {@code epochIntervalMs}:
 * <ul>
 *   <li>{@code -1} — epochs DISABLED (operator opt-out). ADAPTIVE apply is fully lazy: zero column
 *       msync/fdatasync on apply (proven by {@code AdaptiveWalDurabilityTest} tests (e)/(e2)).</li>
 *   <li>{@code 0} — epoch on EVERY apply batch. Each apply calls {@code fsyncMaterializedState()}
 *       (fsync columns + {@code _cv} + {@code _txn}), writes the {@code _snapshot} marker and the
 *       {@code .epoch} copies. This is the WORST-CASE epoch cadence; the delta vs {@code -1} is the
 *       per-epoch overhead.</li>
 * </ul>
 * The default production interval is 1000ms, which amortizes this cost across ~1s of apply batches, so
 * real epoch overhead on the hot path is a small fraction of the {@code 0} vs {@code -1} delta measured
 * here — this benchmark brackets the WORST case.
 *
 * <p>{@code commitMode=SYNC} is included as a reference: SYNC fsyncs the columns on EVERY apply
 * regardless of {@code epochIntervalMs}, so ADAPTIVE/{@code 0} (epoch every batch) ≈ SYNC apply cost,
 * while ADAPTIVE/{@code -1} is the lazy floor. ({@code SYNC} ignores {@code epochIntervalMs}, so its two
 * rows are duplicates.)
 *
 * <p>In-order only: batches advance forward so apply stays on the append fast path and the working
 * partition doesn't rewrite — keeping the measurement stable across a long run. (O3 apply correctness /
 * zero-column-sync is settled by {@code AdaptiveWalDurabilityTest.testAdaptiveO3ApplyIssuesZeroColumnSyncsOnApply};
 * O3 apply <em>cost</em> is a controlled-HW item — see the SP-C spec.)
 * <p>
 * DB root on real disk so fdatasync is a real syscall. Relative comparison only.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class WalAdaptiveApplyBenchmark {

    private static final long APPEND_PAGE_SIZE = 256 * 1024L;
    private static final int COLUMN_COUNT = 20;
    private static final int ROWS_PER_BATCH = 1000;
    private static final String[] SYMBOLS = {"alpha", "beta", "gamma", "delta", "epsilon"};
    private static final String TABLE_NAME = "walapplybench";

    @Param({"ADAPTIVE", "SYNC"})
    public String commitMode;

    /** Adaptive durable-epoch cadence: -1 = disabled (lazy floor), 0 = every apply batch (worst case). */
    @Param({"-1", "0"})
    public long epochIntervalMs;

    private final Rnd rnd = new Rnd();
    private final Utf8StringSink varcharSink = new Utf8StringSink();
    private ApplyWal2TableJob applyJob;
    private CheckWalTransactionsJob checkJob;
    private String dbRoot;
    private CairoEngine engine;
    private int symbolColIndex;
    private long ts;
    private int varcharColIndex;
    private WalWriter walWriter;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(WalAdaptiveApplyBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void ingestAndApply() {
        final int varIdx = varcharColIndex;
        final int symIdx = symbolColIndex;
        for (int i = 0; i < ROWS_PER_BATCH; i++) {
            TableWriter.Row row = walWriter.newRow(ts++);
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
        walWriter.commit();
        // Apply the batch we just committed. Under ADAPTIVE this fires the durable epoch per cadence
        // (epochIntervalMs); that epoch's fsyncMaterializedState is the overhead this benchmark isolates.
        applyJob.drain(0);
        if (checkJob.run()) {
            applyJob.drain(0);
        }
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        final String baseDir = new java.io.File("/data").isDirectory() ? "/data" : System.getProperty("user.home");
        dbRoot = baseDir + "/qdb-walapplybench-" + System.nanoTime();
        new java.io.File(dbRoot).mkdirs();

        final int mode = parseCommitMode(commitMode);
        final long epochMs = epochIntervalMs;
        final CairoConfiguration cfg = new DefaultCairoConfiguration(dbRoot) {
            @Override
            public long getAdaptiveCommitGroupWindowUs() {
                return 0; // W=0: isolate the APPLY/epoch path, not the group-commit window
            }

            @Override
            public long getAdaptiveEpochIntervalMs() {
                return epochMs;
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
        applyJob = new ApplyWal2TableJob(engine, 0);
        checkJob = new CheckWalTransactionsJob(engine);

        ts = 0;
        rnd.reset();
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        if (walWriter != null) {
            try {
                walWriter.commit();
            } catch (Exception ignored) {
            }
            walWriter.close();
            walWriter = null;
        }
        if (applyJob != null && engine != null) {
            applyJob.drain(0);
            if (checkJob.run()) {
                applyJob.drain(0);
            }
            applyJob.close();
            applyJob = null;
        }
        checkJob = null;
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
}
