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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.griffin.engine.table.parquet.ParquetVersion;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectIntList;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
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

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/**
 * Measures the cost of the proposed VARCHAR_SLICE byte-accounting fix for the
 * Parquet decode cache (PageFrameMemoryPool.decode, the loop at the
 * {@code bytes += getChunkDataSize(s) + getChunkAuxSize(s)} site).
 * <p>
 * Under the common encodings (Plain, DeltaLengthByteArray, RLE/Plain dictionary)
 * a decoded VARCHAR_SLICE chunk keeps its string bytes in retained Rust page
 * buffers and leaves data_vec empty, so getChunkDataSize() returns 0. The fix
 * replaces that O(1) read with an O(rows) scan over the aux column that sums the
 * per-entry header lengths (sumVarcharStringBytes). This benchmark quantifies
 * that scan.
 * <p>
 * Four benchmarks:
 * <ul>
 *   <li>{@code decodeThenBaselineAccount} - decode a row group + the CURRENT O(1) accounting</li>
 *   <li>{@code decodeThenScanAccount}     - decode a row group + the PROPOSED aux scan
 *       <br>delta(scan - baseline) = marginal cost of the fix under realistic,
 *       post-decode cache state (aux was just written by the native decoder)</li>
 *   <li>{@code accountBaselineHot}        - CURRENT O(1) accounting over a pre-decoded buffer (cache-hot)</li>
 *   <li>{@code accountScanHot}            - PROPOSED aux scan over a pre-decoded buffer (cache-hot)
 *       <br>delta = compute-bound lower bound, and how the scan scales with row count</li>
 * </ul>
 * The frame has a single VARCHAR column, so the scan is the LARGEST possible
 * fraction of decode time; real multi-column frames make the relative cost
 * smaller. The scan reads only the 16-byte aux header per row, so its cost is
 * independent of string length - vary {@code stringLength} to confirm the scan
 * stays flat while decode grows.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class ParquetVarcharAccountingBenchmark {
    private static final Log LOG = LogFactory.getLog(ParquetVarcharAccountingBenchmark.class);
    private static final String PARQUET_FILE_NAME = "varchar_accounting.parquet";
    private static final String TABLE_NAME = "bench_varchar_accounting";
    private static final String VARCHAR_COLUMN_NAME = "v";

    // ParquetCompression COMPRESSION_* codec: 0 = UNCOMPRESSED, 5 = LZ4_RAW
    // (QuestDB's default partition codec), 4 = ZSTD (heavy). Mirrors production by
    // going through packCompressionCodecLevel() in createParquet().
    @Param({"0", "5", "4"})
    private int compressionCodec;

    @Param({"100000"})
    private int rowCount;

    @Param({"16"})
    private int stringLength;

    private DefaultCairoConfiguration configuration;
    private CairoEngine engine;
    private FilesFacade ff;
    private ParquetFileDecoder parquetDecoder;
    private long parquetFd = -1;
    private long parquetFileAddr;
    private long parquetFileSize;
    private String parquetPath;
    private DirectIntList sliceColumns;
    private RowGroupBuffers sliceRowGroupBuffers;  // re-decoded each invocation (realistic cache state)
    private SqlExecutionContextImpl sqlExecutionContext;
    private long staticAuxPtr;                      // captured once, scanned cache-hot
    private long staticAuxSize;
    private RowGroupBuffers staticRowGroupBuffers;  // decoded once in setup, never overwritten
    private java.nio.file.Path tempRoot;
    private int varcharColumnIndex;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ParquetVarcharAccountingBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public long accountBaselineHot() {
        // current behavior: O(1) field reads
        return staticRowGroupBuffers.getChunkDataSize(0) + staticRowGroupBuffers.getChunkAuxSize(0);
    }

    @Benchmark
    public long accountFieldHot() {
        // proposed Rust approach: the budget reads the O(1) page-buffers size field that
        // Rust populates at decode time (no per-row work on the Java side).
        return staticRowGroupBuffers.getChunkDataSize(0)
                + staticRowGroupBuffers.getChunkAuxSize(0)
                + staticRowGroupBuffers.getChunkPageBuffersSize(0);
    }

    @Benchmark
    public long accountScanHot() {
        // alternative (rejected) Java approach: O(rows) aux scan, cache-hot
        return sumVarcharStringBytes(staticAuxPtr, staticAuxSize) + staticAuxSize;
    }

    @Benchmark
    public long decodeThenBaselineAccount() {
        final int decoded = parquetDecoder.decodeRowGroup(sliceRowGroupBuffers, sliceColumns, 0, 0, rowCount);
        // current accounting: O(1) per slot
        return decoded + sliceRowGroupBuffers.getChunkDataSize(0) + sliceRowGroupBuffers.getChunkAuxSize(0);
    }

    @Benchmark
    public long decodeThenFieldAccount() {
        final int decoded = parquetDecoder.decodeRowGroup(sliceRowGroupBuffers, sliceColumns, 0, 0, rowCount);
        // proposed Rust approach: O(1) field reads; Rust computed the page-buffers size
        // during decode (folded into refresh_ptrs), so the budget adds no per-row work.
        return decoded + sliceRowGroupBuffers.getChunkDataSize(0)
                + sliceRowGroupBuffers.getChunkAuxSize(0)
                + sliceRowGroupBuffers.getChunkPageBuffersSize(0);
    }

    @Benchmark
    public long decodeThenScanAccount() {
        final int decoded = parquetDecoder.decodeRowGroup(sliceRowGroupBuffers, sliceColumns, 0, 0, rowCount);
        // alternative (rejected) Java approach: O(rows) aux scan for the varchar slot
        final long auxPtr = sliceRowGroupBuffers.getChunkAuxPtr(0);
        final long auxSize = sliceRowGroupBuffers.getChunkAuxSize(0);
        return decoded + sumVarcharStringBytes(auxPtr, auxSize) + auxSize;
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        tempRoot = java.nio.file.Files.createTempDirectory("qdb-varchar-accounting-bench");
        configuration = new DefaultCairoConfiguration(tempRoot.toString());
        ff = configuration.getFilesFacade();
        engine = new CairoEngine(configuration);
        sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                .with(
                        configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null,
                        null,
                        -1,
                        null
                );

        createParquet();
        openDecoder();

        sliceColumns = new DirectIntList(2, MemoryTag.NATIVE_DEFAULT);
        sliceColumns.add(varcharColumnIndex);
        sliceColumns.add(ColumnType.VARCHAR_SLICE);

        sliceRowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
        staticRowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);

        // Decode once into the static buffer for the cache-hot isolated scans.
        final int decoded = parquetDecoder.decodeRowGroup(staticRowGroupBuffers, sliceColumns, 0, 0, rowCount);
        staticAuxPtr = staticRowGroupBuffers.getChunkAuxPtr(0);
        staticAuxSize = staticRowGroupBuffers.getChunkAuxSize(0);

        // Validate (and demonstrate) the undercount: getChunkDataSize is what the
        // current budget counts for this slot; the aux scan is the true logical
        // string-byte total the fix would count instead.
        final long currentlyCountedDataBytes = staticRowGroupBuffers.getChunkDataSize(0);
        final long trueStringBytes = sumVarcharStringBytes(staticAuxPtr, staticAuxSize);
        // The Rust-side fix: page-buffers size now reports the retained string bytes the
        // old data_size missed. For compressed data this is non-zero (heap-retained pages);
        // for uncompressed it may be 0 (bytes borrowed from the mmap, not heap).
        final long pageBuffersSize = staticRowGroupBuffers.getChunkPageBuffersSize(0);
        LOG.info().$("varchar accounting bench setup [rows=").$(decoded)
                .$(", auxBytes=").$(staticAuxSize)
                .$(", currentlyCountedDataBytes=").$(currentlyCountedDataBytes)
                .$(", trueStringBytes=").$(trueStringBytes)
                .$(", pageBuffersSize=").$(pageBuffersSize)
                .$("]").$();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (parquetFileAddr != 0) {
            ff.munmap(parquetFileAddr, parquetFileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            parquetFileAddr = 0;
            parquetFileSize = 0;
        }
        if (parquetFd != -1) {
            ff.close(parquetFd);
            parquetFd = -1;
        }
        sliceColumns = Misc.free(sliceColumns);
        sliceRowGroupBuffers = Misc.free(sliceRowGroupBuffers);
        staticRowGroupBuffers = Misc.free(staticRowGroupBuffers);
        parquetDecoder = Misc.free(parquetDecoder);
        sqlExecutionContext = Misc.free(sqlExecutionContext);
        engine = Misc.free(engine);
        if (tempRoot != null && java.nio.file.Files.exists(tempRoot)) {
            try (java.util.stream.Stream<java.nio.file.Path> stream = java.nio.file.Files.walk(tempRoot)) {
                stream.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        java.nio.file.Files.deleteIfExists(path);
                    } catch (Exception ignore) {
                    }
                });
            }
        }
    }

    // Verbatim copy of PageFrameMemoryPool.sumVarcharStringBytes (the proposed fix),
    // so the measurement reflects the exact code that would run.
    private static long sumVarcharStringBytes(long auxBase, long auxSize) {
        if (auxSize <= 0) {
            return 0;
        }
        final long count = auxSize / VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
        long total = 0;
        for (long i = 0; i < count; i++) {
            final long entry = auxBase + i * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
            int hdr = Unsafe.getInt(entry);
            if ((hdr & VarcharTypeDriver.VARCHAR_HEADER_FLAG_NULL) != 0) {
                continue;
            }
            total += hdr >>> 4;
        }
        return total;
    }

    private String buildCreateTableSql() {
        // High-cardinality random varchars of fixed length => the writer picks
        // DeltaLengthByteArray (QuestDB's default for high-cardinality varchar), so every
        // row's bytes are retained in page_buffers. That is exactly the case the byte cap
        // undercounted before the fix (a low-cardinality dictionary column would instead
        // retain only a small dict page, which the fix correctly counts as small).
        return "create table " + TABLE_NAME + " as (" +
                "select rnd_varchar(" + stringLength + ", " + stringLength + ", 0) as " + VARCHAR_COLUMN_NAME +
                " from long_sequence(" + rowCount + ")" +
                ")";
    }

    private void createParquet() throws SqlException {
        parquetPath = tempRoot.resolve(PARQUET_FILE_NAME).toString();
        engine.execute("drop table if exists " + TABLE_NAME, sqlExecutionContext);
        engine.execute(buildCreateTableSql(), sqlExecutionContext);
        try (
                TableReader reader = engine.getReader(TABLE_NAME);
                PartitionDescriptor descriptor = new PartitionDescriptor();
                Path path = new Path()
        ) {
            path.of(parquetPath);
            PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
            // Mirror PropServerConfiguration's default-level rule and the production
            // call in TableUtils: pack the COMPRESSION_* codec with its level.
            final int level = compressionCodec == ParquetCompression.COMPRESSION_ZSTD ? 9 : 0;
            PartitionEncoder.encodeWithOptions(
                    descriptor,
                    path,
                    ParquetCompression.packCompressionCodecLevel(compressionCodec, level),
                    true,
                    false,
                    rowCount,        // one row group spanning the whole table
                    1024 * 1024,
                    ParquetVersion.PARQUET_VERSION_V1,
                    0.0
            );
        }
    }

    private void openDecoder() {
        parquetDecoder = new ParquetFileDecoder();
        try (Path path = new Path()) {
            path.of(parquetPath);
            parquetFd = TableUtils.openRO(ff, path.$(), LOG);
            parquetFileSize = ff.length(parquetFd);
            parquetFileAddr = TableUtils.mapRO(ff, parquetFd, parquetFileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            parquetDecoder.of(parquetFileAddr, parquetFileSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
        }
        varcharColumnIndex = parquetDecoder.metadata().getColumnIndex(VARCHAR_COLUMN_NAME);
        if (varcharColumnIndex < 0) {
            throw new IllegalStateException("varchar column not found in parquet metadata: " + VARCHAR_COLUMN_NAME);
        }
    }
}
