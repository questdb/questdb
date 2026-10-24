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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
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

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;

/**
 * DEDUP UPSERT O3 commits into a parquet partition of 1M rows in row groups of 100_000. With a BOOLEAN column
 * ({@code hasNoSentinelColumn}), an UPSERT commit first checks whether it replays stored rows unchanged: a replay
 * then leaves the partition as it is, or appends past its end. Other commits pay for the check before they merge,
 * apart from what it decoded of the first row group they merge, which the merge takes over. Without such a column
 * the check does not run.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ParquetDedupUpsertO3CommitBenchmark {
    // 2024-01-01T00:00:00Z
    private static final long DAY_START_MICROS = 1_704_067_200_000_000L;
    @Param({"true", "false"})
    public boolean hasNoSentinelColumn;
    @Param({"1000000"})
    public int partitionRows;
    private long appendTimestamp;
    private CairoEngine engine;
    private long newRowTimestamp;
    private Path root;
    private SqlExecutionContext sqlExecutionContext;
    private long updateValue;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ParquetDedupUpsertO3CommitBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public void append() throws SqlException {
        commit(newRow(appendTimestamp++));
    }

    @Benchmark
    public void newRow() throws SqlException {
        // a new key between the stored rows of the middle of the partition
        commit(newRow(newRowTimestamp++));
    }

    @Benchmark
    public void replay() throws SqlException {
        commit(storedRow(partitionRows / 2));
    }

    @Benchmark
    public void replayThenAppend() throws SqlException {
        commit(storedRow(partitionRows / 2) + ", " + newRow(appendTimestamp++));
    }

    @Setup(Level.Trial)
    public void setUp() throws IOException, SqlException {
        root = Files.createTempDirectory("parquet-dedup-upsert-o3-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(root.toString()) {
            @Override
            public long getPartitionEncoderParquetO3RewriteUnusedMaxBytes() {
                // keep the merge commits in place for the whole run
                return Long.MAX_VALUE;
            }

            @Override
            public double getPartitionEncoderParquetO3RewriteUnusedRatio() {
                return 1.0;
            }

            @Override
            public int getPartitionEncoderParquetRowGroupSize() {
                // the server default, DefaultCairoConfiguration leaves it unset
                return 100_000;
            }
        };
        engine = new CairoEngine(configuration);
        sqlExecutionContext = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null,
                null,
                -1,
                null
        );
        engine.execute(
                "CREATE TABLE pt (v LONG, d DOUBLE, " + (hasNoSentinelColumn ? "flag BOOLEAN" : "flag INT")
                        + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL DEDUP UPSERT KEYS(ts)",
                sqlExecutionContext
        );
        // one row per 10 ms from the start of the day, see storedRow()
        engine.execute(
                "INSERT INTO pt SELECT x, x * 0.5, " + (hasNoSentinelColumn ? "x % 2 = 0" : "(x % 2)::INT")
                        + ", " + DAY_START_MICROS + "::TIMESTAMP + (x - 1) * 10_000L FROM long_sequence(" + partitionRows + ")",
                sqlExecutionContext
        );
        drainWal();
        appendTimestamp = DAY_START_MICROS + partitionRows * 10_000L;
        newRowTimestamp = DAY_START_MICROS + (partitionRows / 2) * 10_000L + 1;
        updateValue = partitionRows;
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        engine.close();
        deleteRecursively(root);
    }

    @Benchmark
    public void valueUpdate() throws SqlException {
        // a new value for a stored key in the middle of the partition
        commit("(" + (++updateValue) + ", 0.5, " + flagOf(0) + ", " + timestampOf(partitionRows / 2) + "::TIMESTAMP)");
    }

    private static void deleteRecursively(Path dir) throws IOException {
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult postVisitDirectory(Path d, IOException exc) throws IOException {
                Files.delete(d);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static long timestampOf(long x) {
        return DAY_START_MICROS + (x - 1) * 10_000L;
    }

    private void commit(String rows) throws SqlException {
        engine.execute("INSERT INTO pt VALUES " + rows, sqlExecutionContext);
        drainWal();
    }

    private void drainWal() {
        try (ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0)) {
            //noinspection StatementWithEmptyBody
            while (walApplyJob.run()) ;
            if (new CheckWalTransactionsJob(engine).run()) {
                //noinspection StatementWithEmptyBody
                while (walApplyJob.run()) ;
            }
        }
    }

    private String flagOf(long x) {
        return hasNoSentinelColumn ? Boolean.toString(x % 2 == 0) : Long.toString(x % 2);
    }

    private String newRow(long timestampMicros) {
        return "(" + (++updateValue) + ", 1.5, " + flagOf(1) + ", " + timestampMicros + "::TIMESTAMP)";
    }

    // the row x of the partition with the values it stores
    private String storedRow(long x) {
        return "(" + x + ", " + (x * 0.5) + ", " + flagOf(x) + ", " + timestampOf(x) + "::TIMESTAMP)";
    }
}
