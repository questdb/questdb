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
 * DEDUP UPSERT commits that resend the last row of a parquet partition as stored, with a new row after it, as a
 * client that resends its last batch with new rows does. Each of the {@code days} partitions holds a row group of
 * 100_000 rows and a small last row group of {@code tailRows} rows, which takes both rows of the commit. The
 * commits go to the partitions in turn, so the small row groups grow by few rows over a run and the merge stays
 * the same size. With a BOOLEAN column ({@code hasNoSentinelColumn}), the identity check finds the resent row
 * identical, and the commit appends the new row and keeps the stored one, as native does. Without such a column
 * the check does not run and the commit merges as UPSERT.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ParquetDedupUpsertTailReplayBenchmark {
    private static final long DAY_MICROS = 86_400_000_000L;
    // 2024-01-01T00:00:00Z
    private static final long DAY_START_MICROS = 1_704_067_200_000_000L;
    private static final int ROW_GROUP_SIZE = 100_000;
    @Param({"16"})
    public int days;
    @Param({"true", "false"})
    public boolean hasNoSentinelColumn;
    @Param({"1000", "20000"})
    public int tailRows;
    private CairoEngine engine;
    // per partition, the row x of its last row, see row()
    private long[] lastRows;
    private int nextDay;
    private Path root;
    private SqlExecutionContext sqlExecutionContext;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ParquetDedupUpsertTailReplayBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public void replayTailThenAppend() throws SqlException {
        final int day = nextDay;
        nextDay = (nextDay + 1) % days;
        final long x = lastRows[day];
        engine.execute("INSERT INTO pt VALUES " + row(day, x) + ", " + row(day, x + 1), sqlExecutionContext);
        drainWal();
        lastRows[day] = x + 1;
    }

    @Setup(Level.Trial)
    public void setUp() throws IOException, SqlException {
        root = Files.createTempDirectory("parquet-dedup-upsert-tail-");
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
                return ROW_GROUP_SIZE;
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
        lastRows = new long[days];
        for (int day = 0; day < days; day++) {
            // a full row group, then the small last row group in a commit of its own
            insertRows(day, 0, ROW_GROUP_SIZE);
            insertRows(day, ROW_GROUP_SIZE, tailRows);
            lastRows[day] = ROW_GROUP_SIZE + tailRows;
        }
        nextDay = 0;
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        engine.close();
        deleteRecursively(root);
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

    private static long timestampOf(int day, long x) {
        // one row per 100 ms from the start of the day
        return DAY_START_MICROS + day * DAY_MICROS + (x - 1) * 100_000L;
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

    // rows x of the day in (lo, lo + count]
    private void insertRows(int day, long lo, long count) throws SqlException {
        engine.execute(
                "INSERT INTO pt SELECT x, x * 0.5, " + (hasNoSentinelColumn ? "x % 2 = 0" : "(x % 2)::INT")
                        + ", (" + timestampOf(day, 1) + " + (x - 1) * 100_000L)::TIMESTAMP"
                        + " FROM (SELECT x + " + lo + " x FROM long_sequence(" + count + "))",
                sqlExecutionContext
        );
        drainWal();
    }

    // the row x of the day with the values it stores
    private String row(int day, long x) {
        return "(" + x + ", " + (x * 0.5) + ", " + (hasNoSentinelColumn ? Boolean.toString(x % 2 == 0) : Long.toString(x % 2))
                + ", " + timestampOf(day, x) + "::TIMESTAMP)";
    }
}
