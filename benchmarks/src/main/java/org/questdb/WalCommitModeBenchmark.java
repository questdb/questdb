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
 * Measures WAL commit overhead across NOSYNC / ASYNC / SYNC / ADAPTIVE commit modes — the path that
 * actually matters (adaptive is WAL-only). Each invocation appends {@code rowsPerCommit} rows to a
 * {@link WalWriter} and commits; the commit's durability is what the commit mode controls:
 * <ul>
 *   <li>NOSYNC — no device flush.</li>
 *   <li>SYNC   — msync/fdatasync the WAL segment + events every commit.</li>
 *   <li>ADAPTIVE (group window 0 = W=0, zero-loss) — fdatasync the smaller WAL <em>events</em> every
 *       commit; column materialization is deferred to the durable epoch (lazy apply).</li>
 * </ul>
 * The WAL apply job is deliberately NOT drained per invocation — draining it would force adaptive's
 * lazy apply eagerly and erase its advantage. Apply is drained once at teardown for a clean shutdown.
 * Numbers are for RELATIVE comparison between modes on the same box.
 * <p>
 * DB root is on real disk (ext4) so fdatasync is a real syscall.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class WalCommitModeBenchmark {

    private static final long APPEND_PAGE_SIZE = 256 * 1024L;
    private static final String[] SYMBOLS = {"alpha", "beta", "gamma", "delta", "epsilon"};
    private static final String TABLE_NAME = "walbench";

    @Param({"NOSYNC", "ASYNC", "SYNC", "ADAPTIVE"})
    public String commitMode;

    @Param({"1000"})
    public int rowsPerCommit;

    /** ts + (columnCount - 2) long columns + 1 varchar + 1 symbol. */
    @Param({"20"})
    public int columnCount;

    /**
     * ADAPTIVE group-commit window in microseconds (the RPO knob; ignored by other modes). 0 = W=0,
     * zero-loss (fdatasync every commit). Larger W batches commits per device flush → higher throughput at
     * the cost of an RPO of up to W. Sweep this to draw the RPO ↔ throughput curve.
     */
    @Param({"0"})
    public long groupWindowUs;

    private final Rnd rnd = new Rnd();
    private final Utf8StringSink varcharSink = new Utf8StringSink();
    private ApplyWal2TableJob applyJob;
    private String dbRoot;
    private CairoEngine engine;
    private int symbolColIndex;
    private long ts;
    private int varcharColIndex;
    private WalWriter walWriter;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(WalCommitModeBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        final String baseDir = new java.io.File("/data").isDirectory() ? "/data" : System.getProperty("user.home");
        dbRoot = baseDir + "/qdb-walcommitbench-" + System.nanoTime();
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

        final int longCols = Math.max(0, columnCount - 2);
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
        // Drain the accumulated WAL now (not per-invocation) so shutdown is clean.
        if (applyJob != null && engine != null) {
            CheckWalTransactionsJob check = new CheckWalTransactionsJob(engine);
            //noinspection StatementWithEmptyBody
            while (applyJob.run()) ;
            if (check.run()) {
                //noinspection StatementWithEmptyBody
                while (applyJob.run()) ;
            }
            applyJob.close();
            applyJob = null;
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

    @Benchmark
    public void ingestAndCommit() {
        final int varIdx = varcharColIndex;
        final int symIdx = symbolColIndex;
        for (int i = 0; i < rowsPerCommit; i++) {
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
