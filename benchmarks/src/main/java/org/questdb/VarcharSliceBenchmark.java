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
import io.questdb.griffin.engine.table.parquet.ParquetVersion;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectIntList;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8SplitString;
import io.questdb.std.str.Utf8s;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
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

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
public class VarcharSliceBenchmark {
    private static final Log LOG = LogFactory.getLog(VarcharSliceBenchmark.class);
    private static final String PARQUET_FILE_NAME = "dict_varchar.parquet";
    private static final String TABLE_NAME = "bench_dict_varchar";
    private static final String VARCHAR_COLUMN_NAME = "v";

    @Param({"256"})
    private int dictCardinality;

    @Param({"500000"})
    private int rowCount;

    @Param({"8", "50", "200"})
    private int stringLength;

    private long baseChecksum;
    private DirectIntList legacyColumns;
    private RowGroupBuffers legacyRowGroupBuffers;
    private Utf8SplitString legacyView;
    private DefaultCairoConfiguration configuration;
    private ParquetFileDecoder parquetDecoder;
    private long parquetFd = -1;
    private long parquetFileAddr;
    private long parquetFileSize;
    private String parquetPath;
    private FilesFacade ff;
    private DirectIntList sliceColumns;
    private RowGroupBuffers sliceRowGroupBuffers;
    private Utf8SplitString sliceView;
    private SqlExecutionContextImpl sqlExecutionContext;
    private CairoEngine engine;
    private java.nio.file.Path tempRoot;
    private int varcharColumnIndex;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(VarcharSliceBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        tempRoot = java.nio.file.Files.createTempDirectory("qdb-varchar-slice-bench");
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

        createDictParquet();
        openDecoder();
        prepareDecodeColumns();

        legacyView = new Utf8SplitString(() -> true);
        sliceView = new Utf8SplitString(() -> true);
        legacyRowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
        sliceRowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);

        long legacyChecksum = decodeLegacyChecksum();
        long sliceChecksum = decodeSliceChecksum();
        if (legacyChecksum != sliceChecksum) {
            throw new IllegalStateException("checksum mismatch [legacy=" + legacyChecksum + ", slice=" + sliceChecksum + "]");
        }
        baseChecksum = legacyChecksum;
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

        legacyColumns = Misc.free(legacyColumns);
        sliceColumns = Misc.free(sliceColumns);
        legacyRowGroupBuffers = Misc.free(legacyRowGroupBuffers);
        sliceRowGroupBuffers = Misc.free(sliceRowGroupBuffers);
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

    @Benchmark
    public long decodeDictVarcharLegacyChecksum() {
        return decodeLegacyChecksum();
    }

    @Benchmark
    public long decodeDictVarcharSliceChecksum() {
        return decodeSliceChecksum();
    }

    @Benchmark
    public int decodeDictVarcharLegacyDecodeOnly() {
        return decodeLegacyDecodeOnly();
    }

    @Benchmark
    public int decodeDictVarcharSliceDecodeOnly() {
        return decodeSliceDecodeOnly();
    }

    private String buildCreateTableSql() {
        StringBuilder dict = new StringBuilder(dictCardinality * (stringLength + 8));
        for (int i = 0; i < dictCardinality; i++) {
            if (i > 0) {
                dict.append(',');
            }
            String base = "dict_" + i;
            StringBuilder padded = new StringBuilder(stringLength);
            while (padded.length() < stringLength) {
                padded.append(base);
                padded.append('_');
            }
            padded.setLength(stringLength);
            dict.append('\'').append(padded).append('\'');
        }
        return "create table " + TABLE_NAME + " as (" +
                "select rnd_varchar(" + dict + ") as " + VARCHAR_COLUMN_NAME +
                " from long_sequence(" + rowCount + ")" +
                ")";
    }

    private static long mixChecksum(long checksum, Utf8Sequence value) {
        if (value == null) {
            return checksum * 31 + 1;
        }
        return checksum * 31 + Utf8s.hashCode(value);
    }

    private void createDictParquet() throws SqlException {
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
            PartitionEncoder.encodeWithOptions(
                    descriptor,
                    path,
                    ParquetCompression.COMPRESSION_UNCOMPRESSED,
                    true,
                    false,
                    rowCount,
                    1024 * 1024,
                    ParquetVersion.PARQUET_VERSION_V1,
                    0.0
            );
        }
    }

    private int decodeLegacyDecodeOnly() {
        int totalDecoded = 0;
        final int rowGroupCount = parquetDecoder.metadata().getRowGroupCount();
        for (int rowGroup = 0; rowGroup < rowGroupCount; rowGroup++) {
            final int rowGroupSize = parquetDecoder.metadata().getRowGroupSize(rowGroup);
            totalDecoded += parquetDecoder.decodeRowGroup(
                    legacyRowGroupBuffers,
                    legacyColumns,
                    rowGroup,
                    0,
                    rowGroupSize
            );
        }
        return totalDecoded;
    }

    private int decodeSliceDecodeOnly() {
        int totalDecoded = 0;
        final int rowGroupCount = parquetDecoder.metadata().getRowGroupCount();
        for (int rowGroup = 0; rowGroup < rowGroupCount; rowGroup++) {
            final int rowGroupSize = parquetDecoder.metadata().getRowGroupSize(rowGroup);
            totalDecoded += parquetDecoder.decodeRowGroup(
                    sliceRowGroupBuffers,
                    sliceColumns,
                    rowGroup,
                    0,
                    rowGroupSize
            );
        }
        return totalDecoded;
    }

    private long decodeLegacyChecksum() {
        long checksum = 0;
        final int rowGroupCount = parquetDecoder.metadata().getRowGroupCount();
        for (int rowGroup = 0; rowGroup < rowGroupCount; rowGroup++) {
            final int rowGroupSize = parquetDecoder.metadata().getRowGroupSize(rowGroup);
            final int decoded = parquetDecoder.decodeRowGroup(
                    legacyRowGroupBuffers,
                    legacyColumns,
                    rowGroup,
                    0,
                    rowGroupSize
            );
            final long auxPtr = legacyRowGroupBuffers.getChunkAuxPtr(0);
            final long dataPtr = legacyRowGroupBuffers.getChunkDataPtr(0);

            for (int row = 0; row < decoded; row++) {
                Utf8Sequence value = VarcharTypeDriver.getSplitValue(
                        auxPtr,
                        Long.MAX_VALUE,
                        dataPtr,
                        Long.MAX_VALUE,
                        row,
                        legacyView
                );
                checksum = mixChecksum(checksum, value);
            }
        }
        return checksum;
    }

    private long decodeSliceChecksum() {
        long checksum = 0;
        final int rowGroupCount = parquetDecoder.metadata().getRowGroupCount();
        for (int rowGroup = 0; rowGroup < rowGroupCount; rowGroup++) {
            final int rowGroupSize = parquetDecoder.metadata().getRowGroupSize(rowGroup);
            final int decoded = parquetDecoder.decodeRowGroup(
                    sliceRowGroupBuffers,
                    sliceColumns,
                    rowGroup,
                    0,
                    rowGroupSize
            );
            final long auxPtr = sliceRowGroupBuffers.getChunkAuxPtr(0);

            for (int row = 0; row < decoded; row++) {
                Utf8Sequence value = VarcharTypeDriver.getSliceValue(auxPtr, row, sliceView);
                checksum = mixChecksum(checksum, value);
            }
        }
        if (baseChecksum != 0 && checksum != baseChecksum) {
            throw new IllegalStateException("checksum mismatch [expected=" + baseChecksum + ", actual=" + checksum + "]");
        }
        return checksum;
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

    private void prepareDecodeColumns() {
        legacyColumns = new DirectIntList(2, MemoryTag.NATIVE_DEFAULT);
        legacyColumns.add(varcharColumnIndex);
        legacyColumns.add(ColumnType.VARCHAR);

        sliceColumns = new DirectIntList(2, MemoryTag.NATIVE_DEFAULT);
        sliceColumns.add(varcharColumnIndex);
        sliceColumns.add(ColumnType.VARCHAR_SLICE);
    }
}
