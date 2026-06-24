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

import io.questdb.MessageBusImpl;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.DefaultDdlListener;
import io.questdb.cairo.DefaultLifecycleManager;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
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
 * Measures commit overhead across NOSYNC / ASYNC / SYNC commit modes.
 *
 * Schema: bench (ts timestamp, l long, d double, v varchar, s symbol) partition by DAY.
 * Exercises the data+aux vector files (varchar) and symbol mem files on every commit.
 *
 * DB root is on real disk (ext4) so fsync/fdatasync are real syscalls.
 * getDataAppendPageSize = 256 KiB → file-extends (and thus the SYNC fdatasync path) happen frequently.
 *
 * Pattern follows TableWriterBenchmark: DDL via a temporary CairoEngine, then direct
 * TableWriter construction with the commit-mode-overridden configuration.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class CommitModeBenchmark {

    private static final String TABLE_NAME = "bench";
    private static final String[] SYMBOLS = {"alpha", "beta", "gamma", "delta", "epsilon"};
    /** 256 KiB page size → frequent file extends → frequent fdatasync on SYNC path */
    private static final long APPEND_PAGE_SIZE = 256 * 1024L;

    @Param({"NOSYNC", "ASYNC", "SYNC"})
    public String commitMode;

    @Param({"1", "1000"})
    public int rowsPerCommit;

    /**
     * Total data columns (excluding the designated timestamp). The flush-batching optimization replaces
     * N per-file device flushes with ~2 per commit, so the SYNC win is expected to GROW with this value.
     * The schema is: ts + (columnCount - 2) long columns + 1 varchar + 1 symbol.
     */
    @Param({"5", "25", "100"})
    public int columnCount;

    private CairoEngine writerEngine;
    private TableWriter writer;
    private final Rnd rnd = new Rnd();
    private final Utf8StringSink varcharSink = new Utf8StringSink();
    private long ts;
    private int varcharColIndex;
    private int symbolColIndex;
    private String dbRoot;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CommitModeBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        // Use a real (non-tmpfs) disk so msync/fdatasync are real device syscalls. Prefer /data when present
        // (ext4 with free space); otherwise fall back to $HOME.
        final String baseDir = new java.io.File("/data").isDirectory() ? "/data" : System.getProperty("user.home");
        dbRoot = baseDir + "/qdb-commitbench-" + System.nanoTime();
        new java.io.File(dbRoot).mkdirs();

        final int mode = parseCommitMode(commitMode);
        final CairoConfiguration cfg = new DefaultCairoConfiguration(dbRoot) {
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

        // Step 1: create table schema via a throw-away engine (same pattern as TableWriterBenchmark).
        // Build columnCount data columns: (columnCount - 2) longs, then 1 varchar + 1 symbol, so every
        // run exercises fixed-width data files plus a var-size (data+aux) column and a symbol (char/offset
        // + index). Widening columnCount is what scales the number of per-commit column flushes.
        final int longCols = Math.max(0, columnCount - 2);
        final StringBuilder ddl = new StringBuilder("create table ").append(TABLE_NAME).append(" (ts timestamp");
        for (int c = 0; c < longCols; c++) {
            ddl.append(", c").append(c).append(" long");
        }
        varcharColIndex = 1 + longCols;       // col 0 is ts
        symbolColIndex = varcharColIndex + 1;
        ddl.append(", v varchar, s symbol) timestamp(ts) partition by DAY bypass wal");
        executeDdl(ddl.toString(), cfg);

        // Step 2: open a direct TableWriter on the same config (bypasses WAL / sequencer overhead)
        // Table token must match what the DDL stored; tableId=0 is the bench table with no WAL.
        TableToken token = new TableToken(TABLE_NAME, TABLE_NAME, null, 0, false, false, false);

        writerEngine = new CairoEngine(cfg);
        writer = new TableWriter(
                cfg,
                token,
                null,
                new MessageBusImpl(cfg),
                true,
                DefaultLifecycleManager.INSTANCE,
                cfg.getDbRoot(),
                DefaultDdlListener.INSTANCE,
                writerEngine
        );

        ts = 0;
        rnd.reset();
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        if (writer != null) {
            try { writer.commit(); } catch (Exception ignored) {}
            writer.close();
            writer = null;
        }
        if (writerEngine != null) {
            writerEngine.close();
            writerEngine = null;
        }
        if (dbRoot != null) {
            deleteDirectory(new java.io.File(dbRoot));
            dbRoot = null;
        }
    }

    @Benchmark
    public void insertAndCommit() {
        final int varIdx = varcharColIndex;
        final int symIdx = symbolColIndex;
        for (int i = 0; i < rowsPerCommit; i++) {
            // ts monotonically increasing (append-only pattern)
            TableWriter.Row row = writer.newRow(ts++);

            // fixed-width long columns 1..varIdx-1
            for (int c = 1; c < varIdx; c++) {
                row.putLong(c, rnd.nextLong());
            }
            // varchar (8–64 bytes, varied length exercises data+aux vectors)
            int varLen = 8 + (rnd.nextPositiveInt() % 57);
            varcharSink.clear();
            rnd.nextUtf8AsciiStr(varLen, varcharSink);
            row.putVarchar(varIdx, varcharSink);
            // symbol (small dictionary)
            row.putSym(symIdx, SYMBOLS[rnd.nextPositiveInt() % SYMBOLS.length]);

            row.append();
        }
        writer.commit();
    }

    // ---- helpers ----

    private static int parseCommitMode(String name) {
        return switch (name) {
            case "NOSYNC" -> CommitMode.NOSYNC;
            case "ASYNC"  -> CommitMode.ASYNC;
            case "SYNC"   -> CommitMode.SYNC;
            default -> throw new IllegalArgumentException("Unknown commit mode: " + name);
        };
    }

    private static void executeDdl(String ddl, CairoConfiguration configuration) {
        try (CairoEngine engine = new CairoEngine(configuration)) {
            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(
                            configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null,
                            null,
                            -1,
                            null
                    );
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                // Must use CairoEngine.execute() — compiler.compile() only plans DDL,
                // it does NOT execute CREATE TABLE; execution requires op.execute().await().
                CairoEngine.execute(compiler, ddl, ctx, null);
            } catch (SqlException e) {
                throw new RuntimeException("DDL failed: " + ddl, e);
            }
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
}
