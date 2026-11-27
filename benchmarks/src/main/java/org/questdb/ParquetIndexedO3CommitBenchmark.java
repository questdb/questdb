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
 * Cost of one WAL commit into the last partition of a FORMAT PARQUET table with an indexed SYMBOL
 * column. Every such commit updates the parquet partition in place and maintains the index:
 * <ul>
 *     <li>append: one row after the partition's last row, which extends the live index;</li>
 *     <li>merge: one row into the middle of the partition, which rebuilds the index into a new
 *     column version.</li>
 * </ul>
 * The partition holds {@code partitionRows} rows in row groups of the default size.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ParquetIndexedO3CommitBenchmark {
    // 2024-01-01T00:00:00Z
    private static final long DAY_START_MICROS = 1_704_067_200_000_000L;
    @Param({"1000000"})
    public int partitionRows;
    private long appendTimestamp;
    private CairoEngine engine;
    private long mergeTimestamp;
    private Path root;
    private SqlExecutionContext sqlExecutionContext;
    private long value;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ParquetIndexedO3CommitBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public void append() throws SqlException {
        commit(appendTimestamp++);
    }

    @Benchmark
    public void merge() throws SqlException {
        commit(mergeTimestamp++);
    }

    @Setup(Level.Trial)
    public void setUp() throws IOException, SqlException {
        root = Files.createTempDirectory("parquet-indexed-o3-");
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
        engine.execute("CREATE TABLE pt (s SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY FORMAT PARQUET WAL", sqlExecutionContext);
        // one row per 10 ms from the start of the day
        engine.execute(
                "INSERT INTO pt SELECT rnd_symbol(100, 4, 8, 0), x, " + DAY_START_MICROS + "::TIMESTAMP + (x - 1) * 10_000L FROM long_sequence(" + partitionRows + ")",
                sqlExecutionContext
        );
        drainWal();
        appendTimestamp = DAY_START_MICROS + partitionRows * 10_000L;
        // between the existing rows of the middle of the partition
        mergeTimestamp = DAY_START_MICROS + (partitionRows / 2) * 10_000L + 1;
        value = partitionRows;
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

    private void commit(long timestampMicros) throws SqlException {
        engine.execute("INSERT INTO pt VALUES ('k', " + (++value) + ", " + timestampMicros + "::TIMESTAMP)", sqlExecutionContext);
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
}
