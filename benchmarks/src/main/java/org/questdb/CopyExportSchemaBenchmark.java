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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.parquet.CopyExportRequestJob;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Measures whole-table COPY submission and synchronous export of mixed Parquet and native partitions.
 * Conversion leaves the active partition native; unchanged Parquet schemas are the byte-copy control.
 * Dropping b requires reconciliation of the older Parquet schemas.
 * Dataset generation, output cleanup and read-back validation are outside the timed operation.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = {
        "-ea", "--sun-misc-unsafe-memory-access=allow", "--enable-native-access=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"
})
public class CopyExportSchemaBenchmark {
    @Param({"10"})
    public int partitionCount;
    @Param({"1000000"})
    public int rowCount;
    @Param({"unchanged", "drop"})
    public String schema;
    private SqlExecutionContextImpl context;
    private CopyExportRequestJob copyJob;
    private CairoEngine engine;
    private Path exportRoot;
    private long originalParquetBytes;
    private int originalParquetPartitions;
    private Path outputDirectory;
    private Path root;
    private long sourceParquetBytes;
    private int sourceParquetPartitions;

    @Setup(Level.Invocation)
    public void clearOutput() throws Exception {
        // Keep the source partitions and start each measured COPY with an empty export root.
        try (var paths = Files.walk(exportRoot)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                if (!path.equals(exportRoot)) {
                    Files.delete(path);
                }
            }
        }
    }

    @Benchmark
    public boolean copyTable() throws Exception {
        try (
                RecordCursorFactory factory = engine.select("COPY t TO 'out' WITH FORMAT PARQUET", context);
                RecordCursor cursor = factory.getCursor(context)
        ) {
            if (!cursor.hasNext() || cursor.getRecord().getStrA(0) == null) {
                throw new AssertionError("COPY did not enqueue an export");
            }
        }
        if (!copyJob.run()) {
            throw new AssertionError("the export worker found no queued export");
        }
        return true;
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        LogFactory.init();
        root = Files.createTempDirectory("copy-export-schema-");
        Path dbRoot = Files.createDirectory(root.resolve("db"));
        exportRoot = Files.createDirectory(root.resolve("export"));
        outputDirectory = exportRoot.resolve("out");
        final String exportPath = exportRoot.toString();
        engine = new CairoEngine(new DefaultCairoConfiguration(dbRoot.toString()) {
            @Override
            public CharSequence getSqlCopyExportRoot() {
                return exportPath;
            }

            @Override
            public CharSequence getSqlCopyInputRoot() {
                return exportPath;
            }
        });
        engine.load();
        context = new SqlExecutionContextImpl(engine, 1).with(
                engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null,
                new Rnd(123, 456)
        );
        final long timestampStep = 86_400_000_000L * partitionCount / rowCount;
        engine.execute("CREATE TABLE t AS (SELECT rnd_int(1, 1_000_000, 0) a,"
                + " rnd_long(1, 1_000_000_000, 0) b, timestamp_sequence('2024-01-01', " + timestampStep + "L) ts"
                + " FROM long_sequence(" + rowCount + ")) TIMESTAMP(ts) PARTITION BY DAY", context);
        engine.execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts >= 0", context);
        try (var paths = Files.walk(dbRoot)) {
            final var parquetFiles = paths.filter(path -> path.getFileName().toString().equals("data.parquet"))
                    .toList();
            originalParquetPartitions = parquetFiles.size();
            if (originalParquetPartitions == 0) {
                throw new AssertionError("the source table has no converted Parquet partitions");
            }
            for (Path parquetFile : parquetFiles) {
                originalParquetBytes += Files.size(parquetFile);
            }
        }
        if (schema.equals("drop")) {
            engine.execute("ALTER TABLE t DROP COLUMN b", context);
        }
        try (TableReader reader = engine.getReader("t")) {
            if (reader.getPartitionCount() != partitionCount) {
                throw new AssertionError("the source table partition count differs from the requested count");
            }
            for (int i = 0; i < reader.getPartitionCount(); i++) {
                if (reader.getTxFile().isPartitionParquet(i)) {
                    sourceParquetPartitions++;
                    sourceParquetBytes += reader.getTxFile().getPartitionParquetFileSize(i);
                }
            }
        }
        if (sourceParquetPartitions != originalParquetPartitions) {
            throw new AssertionError("the source Parquet files differ from the transaction file's partition formats");
        }
        copyJob = new CopyExportRequestJob(engine);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        try {
            verifyOutput();
        } finally {
            copyJob = Misc.free(copyJob);
            context = Misc.free(context);
            engine = Misc.free(engine);
            io.questdb.std.str.Path.clearThreadLocals();
            try (var paths = Files.walk(root)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(path);
                }
            }
        }
    }

    private void verifyOutput() throws Exception {
        try (
                RecordCursorFactory factory = engine.select(
                        "SELECT status FROM \"" + engine.getConfiguration().getSystemTableNamePrefix()
                                + "copy_export_log\" LIMIT -1", context);
                RecordCursor cursor = factory.getCursor(context)
        ) {
            if (!cursor.hasNext() || !"finished".contentEquals(cursor.getRecord().getSymA(0))) {
                throw new AssertionError("the final export did not finish successfully");
            }
        }
        int exportedColumnsMin = Integer.MAX_VALUE;
        int exportedColumnsMax = 0;
        long exportedBytes = 0;
        long exportedRows = 0;
        try (var paths = Files.list(outputDirectory)) {
            final var parquetFiles = paths.filter(path -> path.getFileName().toString().endsWith(".parquet"))
                    .toList();
            if (parquetFiles.size() != partitionCount) {
                throw new AssertionError("the export partition count differs from the source table");
            }
            for (Path parquetFile : parquetFiles) {
                exportedBytes += Files.size(parquetFile);
                final String readQuery = "read_parquet('out/" + parquetFile.getFileName() + "')";
                try (RecordCursorFactory factory = engine.select("SELECT * FROM " + readQuery, context)) {
                    final int columnCount = factory.getMetadata().getColumnCount();
                    if ((schema.equals("unchanged") && columnCount != 3)
                            || (schema.equals("drop") && (columnCount < 2 || columnCount > 3))) {
                        throw new AssertionError("the exported partition has an unexpected column count");
                    }
                    exportedColumnsMin = Math.min(exportedColumnsMin, columnCount);
                    exportedColumnsMax = Math.max(exportedColumnsMax, columnCount);
                }
                try (
                        RecordCursorFactory factory = engine.select("SELECT count() FROM " + readQuery, context);
                        RecordCursor cursor = factory.getCursor(context)
                ) {
                    if (!cursor.hasNext()) {
                        throw new AssertionError("the exported partition has no row count");
                    }
                    exportedRows += cursor.getRecord().getLong(0);
                }
            }
        }
        if (exportedRows != rowCount) {
            throw new AssertionError("the export row count differs from the generated table");
        }
        // Report the baseline's stale schema rather than rejecting it, so both revisions can be timed.
        System.out.printf(Locale.ROOT,
                "copy_export_schema_data schema=%s partitions=%d rows=%d original_native_bytes=%d current_native_bytes=%d"
                        + " original_parquet_partitions=%d original_parquet_bytes=%d source_parquet_partitions=%d"
                        + " source_parquet_bytes=%d export_bytes=%d table_columns=%d export_columns_min=%d export_columns_max=%d%n",
                schema, partitionCount, rowCount, rowCount * 20L, rowCount * (schema.equals("drop") ? 12L : 20L),
                originalParquetPartitions, originalParquetBytes, sourceParquetPartitions, sourceParquetBytes,
                exportedBytes, schema.equals("drop") ? 2 : 3, exportedColumnsMin, exportedColumnsMax);
    }
}
