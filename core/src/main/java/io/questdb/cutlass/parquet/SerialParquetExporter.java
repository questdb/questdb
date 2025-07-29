/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cutlass.parquet;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cutlass.text.SerialCsvFileImporter;
import io.questdb.griffin.engine.table.parquet.MappedMemoryPartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;

import static io.questdb.cairo.TableUtils.createDirsOrFail;

public class SerialParquetExporter implements Closeable {

    private static final Log LOG = LogFactory.getLog(SerialCsvFileImporter.class);
    private final CairoEngine cairoEngine;
    private final CairoConfiguration configuration;
    private final CharSequence copyRoot;
    private final FilesFacade ff;
    ExecutionCircuitBreaker circuitBreaker;
    long copyId;
    CharSequence fileName;
    Path fromNative, toParquet;
    PhaseStatusReporter statusReporter;
    CharSequence tableName;
    TableReader tableReader;
    TableToken tableToken;

    public SerialParquetExporter(CairoEngine engine, Path path) {
        this.cairoEngine = engine;
        this.configuration = engine.getConfiguration();
        this.ff = this.configuration.getFilesFacade();
        this.copyRoot = this.configuration.getSqlCopyInputRoot();
        this.toParquet = path;
    }

    @Override
    public void close() throws IOException {

    }

    public void of(@NotNull CharSequence tableName,
                   @NotNull CharSequence fileName,
                   long copyId,
                   ExecutionCircuitBreaker circuitBreaker,
                   PhaseStatusReporter statusReporter) {
        this.tableName = tableName;
        this.fileName = fileName;
        this.copyId = copyId;
        this.circuitBreaker = circuitBreaker;
        this.statusReporter = statusReporter;
    }

    public void process(SecurityContext securityContext) {
        tableToken = cairoEngine.getTableTokenIfExists(tableName);
        final CharSequence inputRoot = configuration.getSqlCopyInputRoot();
        final int compressionCodec = configuration.getPartitionEncoderParquetCompressionCodec();
        final int compressionLevel = configuration.getPartitionEncoderParquetCompressionLevel();
        final int rowGroupSize = configuration.getPartitionEncoderParquetRowGroupSize();
        final int dataPageSize = configuration.getPartitionEncoderParquetDataPageSize();
        final boolean statisticsEnabled = configuration.isPartitionEncoderParquetStatisticsEnabled();
        final int parquetVersion = configuration.getPartitionEncoderParquetVersion();

        try (TableReader reader = cairoEngine.getReader(tableToken)) {
            final int partitionCount = reader.getPartitionCount();

            for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {

                reader.openPartition(partitionIndex);
                final int partitionBy = reader.getPartitionedBy();
                final long partitionTimestamp = reader.getPartitionTimestampByIndex(partitionIndex);

                try (PartitionDescriptor partitionDescriptor = new MappedMemoryPartitionDescriptor(ff)) {
                    PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, partitionIndex);

                    toParquet.trimTo(0).concat(inputRoot).concat(fileName);

                    PartitionBy.getPartitionDirFormatMethod(partitionBy)
                            .format(partitionTimestamp, DateFormatUtils.EN_LOCALE, null, toParquet.slash());

                    toParquet.put(".parquet");


                    createDirsOrFail(ff, toParquet, configuration.getMkDirMode());

                    PartitionEncoder.encodeWithOptions(
                            partitionDescriptor,
                            toParquet,
                            ParquetCompression.packCompressionCodecLevel(compressionCodec, compressionLevel),
                            statisticsEnabled,
                            rowGroupSize,
                            dataPageSize,
                            parquetVersion
                    );

                    long parquetFileLength = ff.length(toParquet.$());

                }
            }

        }
    }

    @FunctionalInterface
    public interface PhaseStatusReporter {
        void report(byte phase, byte status, @Nullable final CharSequence msg, long errors);
    }

}
