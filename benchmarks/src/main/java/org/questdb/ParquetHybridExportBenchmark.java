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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.parquet.CopyExportRequestJob;
import io.questdb.cutlass.parquet.ParquetExportMode;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Times a Parquet export (COPY TO) of a computed column over Parquet partitions, which takes the
 * PAGE_FRAME_BACKED export mode: the hybrid materializer decodes each Parquet row group, a frame,
 * into native buffers and holds them until the writer has written the frame's last row.
 * <p>
 * The table holds 600_000 rows in 6 daily Parquet partitions, in row groups of {@code frameRows}
 * rows, of an INT, a LONG, a DOUBLE, a VARCHAR of 400 bytes and the designated TIMESTAMP: a frame
 * decodes to 444 bytes a row, 44 MB for a frame of 100_000 rows, whose VARCHAR data vector is
 * larger than glibc's largest mmap threshold. The export writes row groups of 100_005 rows, which
 * no frame size divides, so each output row group ends inside a frame and the frames before it wait
 * in the writer until the flush: about 10_000 frames of 10 rows, 100 frames of 1_000 rows, or one
 * frame of 100_000 rows, which the export releases at once after the flush.
 * <p>
 * Run with:
 * <pre>
 * java -jar benchmarks/target/benchmarks.jar ParquetHybridExportBenchmark
 * </pre>
 * Logging goes to stdout at ERROR level unless the forked JVM gets {@code -Dout=<log.conf>}, for
 * example through {@code -jvmArgsAppend}.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
@Fork(value = 1, jvmArgsAppend = {"--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED"})
public class ParquetHybridExportBenchmark {
    private static final String EXPORT_QUERY = "SELECT a * 2 a2, b, c, s, ts FROM t";
    private static final String EXPORT_SQL = "COPY (" + EXPORT_QUERY + ") TO 'hybrid_export' WITH FORMAT PARQUET ROW_GROUP_SIZE 100005";
    @Param({"10", "1000", "100000"})
    public int frameRows;
    private CairoEngine engine;
    private CopyExportRequestJob job;
    private SqlExecutionContextImpl sqlExecutionContext;
    private Path tempRoot;

    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include(ParquetHybridExportBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public boolean export() throws SqlException {
        return runExport();
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        configureQuietLogging();
        tempRoot = Files.createTempDirectory("qdb-hybrid-export-bench");
        final Path dbRoot = Files.createDirectories(tempRoot.resolve("db"));
        final String exportRoot = Files.createDirectories(tempRoot.resolve("export")).toString();
        final int partitionRowGroupSize = frameRows;
        final DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(dbRoot.toString()) {
            @Override
            public int getPartitionEncoderParquetRowGroupSize() {
                return partitionRowGroupSize;
            }

            @Override
            public CharSequence getSqlCopyExportRoot() {
                return exportRoot;
            }

            @Override
            public CharSequence getSqlCopyInputRoot() {
                return exportRoot;
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
        engine.execute("CREATE TABLE t (a INT, b LONG, c DOUBLE, s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", sqlExecutionContext);
        // 100_000 rows a day over 6 days.
        engine.execute(
                "INSERT INTO t SELECT x::INT, x * 3, x / 7.0, rpad(x::VARCHAR, 400, 'v'), timestamp_sequence('2024-01-01', 864_000) FROM long_sequence(600_000)",
                sqlExecutionContext
        );
        engine.execute("INSERT INTO t VALUES (0, 0, 0, 'z', '2024-01-07T00:00:00.000000Z')", sqlExecutionContext);
        engine.execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-07'", sqlExecutionContext);
        try (RecordCursorFactory factory = engine.select(EXPORT_QUERY, sqlExecutionContext)) {
            final ParquetExportMode mode = ParquetExportMode.determineExportMode(factory, false, sqlExecutionContext);
            if (mode != ParquetExportMode.PAGE_FRAME_BACKED) {
                throw new IllegalStateException("the export must decode frames in the hybrid materializer [mode=" + mode + ']');
            }
        }
        job = new CopyExportRequestJob(engine);
        runExport();
        final String status = lastExportStatus();
        if (!"finished".equals(status)) {
            throw new IllegalStateException("the export must finish [status=" + status + ']');
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        job = Misc.free(job);
        sqlExecutionContext = Misc.free(sqlExecutionContext);
        engine = Misc.free(engine);
        if (tempRoot != null) {
            try (Stream<Path> paths = Files.walk(tempRoot)) {
                paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
            }
        }
    }

    // Keeps the INFO logs of every export out of the results. A user-supplied -Dout log configuration wins.
    private static void configureQuietLogging() throws IOException {
        if (System.getProperty(LogFactory.CONFIG_SYSTEM_PROPERTY) != null) {
            return;
        }
        final Path conf = Files.createTempFile("hybridExportBenchLog-", ".conf");
        conf.toFile().deleteOnExit();
        Files.writeString(conf, """
                writers=stdout
                w.stdout.class=io.questdb.log.LogConsoleWriter
                w.stdout.level=ERROR
                """);
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, conf.toString());
    }

    private String lastExportStatus() throws SqlException {
        try (
                RecordCursorFactory factory = engine.select(
                        "SELECT status FROM \"" + engine.getConfiguration().getSystemTableNamePrefix() + "copy_export_log\" LIMIT -1",
                        sqlExecutionContext
                );
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            return cursor.hasNext() ? String.valueOf(cursor.getRecord().getSymA(0)) : null;
        }
    }

    // Queues the export and runs it on this thread.
    private boolean runExport() throws SqlException {
        try (
                RecordCursorFactory factory = engine.select(EXPORT_SQL, sqlExecutionContext);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            if (!cursor.hasNext()) {
                throw new IllegalStateException("COPY must queue the export");
            }
        }
        return job.run();
    }
}
