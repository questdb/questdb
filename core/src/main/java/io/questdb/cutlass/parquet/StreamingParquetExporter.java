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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;

import static io.questdb.cairo.TableUtils.createDirsOrFail;

public class StreamingParquetExporter implements Closeable {
    private static final Log LOG = LogFactory.getLog(StreamingParquetExporter.class);
    private final CairoEngine cairoEngine;
    private final CairoConfiguration configuration;
    private final CharSequence copyExportRoot;
    private final FilesFacade ff;
    ExecutionCircuitBreaker circuitBreaker;
    StringSink msgSink;
    RecordCursorFactory query;
    Path toParquet;

    public StreamingParquetExporter(CairoEngine engine, Path path) {
        this.cairoEngine = engine;
        this.configuration = engine.getConfiguration();
        this.ff = this.configuration.getFilesFacade();
        this.copyExportRoot = this.configuration.getSqlCopyExportRoot();
        this.toParquet = path;
        this.msgSink = new StringSink();
    }

    @Override
    public void close() throws IOException {
        Misc.free(toParquet);
    }

    public void process(SecurityContext securityContext) {

        // steps
        // get metadata
        // create column buffers based on row group size
        //
        query.getMetadata();


        statusReporter.report(CopyExportRequestTask.PHASE_CONVERTING_PARTITIONS, CopyExportRequestTask.STATUS_STARTED, null, Long.MIN_VALUE);

        final String tableName = task.getTableName();
        final String fileName = task.getFileName() != null ? task.getFileName() : tableName;
        @Nullable CharSequence toParquetCs;
        int lastSlash;

        tableToken = cairoEngine.getTableTokenIfExists(tableName);

        if (tableToken == null) {
            // check quoted
            throw CairoException.tableDoesNotExist(tableName);
        }

        securityContext.authorizeSelectOnAnyColumn(tableToken);

        final int compressionCodec = task.getCompressionCodec();
        final int compressionLevel = task.getCompressionLevel();
        final int rowGroupSize = task.getRowGroupSize();
        final int dataPageSize = task.getDataPageSize();
        final boolean statisticsEnabled = task.isStatisticsEnabled();
        final int parquetVersion = task.getParquetVersion();
        final boolean rawArrayEncoding = task.isRawArrayEncoding();

        try (TableReader reader = cairoEngine.getReader(tableToken)) {
            final int partitionCount = reader.getPartitionCount();
            final int partitionBy = reader.getPartitionedBy();

            if (partitionCount == 0) {
                // empty table
                throw CairoException.tableDoesNotExist(tableName);
            } else {
                try (PartitionDescriptor partitionDescriptor = new PartitionDescriptor()) {
                    for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                        if (circuitBreaker.checkIfTripped()) {
                            LOG.errorW().$("copy was cancelled [copyId=").$hexPadded(task.getCopyID()).$(']').$();
                            throw CairoException.queryCancelled();
                        }
                        final long partitionTimestamp = reader.getPartitionTimestampByIndex(partitionIndex);

                        if (reader.getPartitionFormat(partitionIndex) == PartitionFormat.PARQUET) {
                            // todo: copy the file directly
                            continue;
                        }

                        reader.openPartition(partitionIndex);
                        PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, partitionIndex);

                        // prepare output file path
                        toParquet.trimTo(0).concat(copyExportRoot).concat(fileName);
                        PartitionBy.getPartitionDirFormatMethod(partitionBy)
                                .format(partitionTimestamp, DateFormatUtils.EN_LOCALE, null, toParquet.slash());
                        toParquet.put(".parquet");
                        toParquetCs = toParquet.asAsciiCharSequence();
                        lastSlash = Chars.lastIndexOf(toParquetCs, 0, toParquetCs.length() - 1, '/');
                        CharSequence partitionName = toParquetCs.subSequence(lastSlash + 1, toParquetCs.length() - 8);

                        // log start
                        LOG.info().$("converting partition to parquet [table=").$(tableToken)
                                .$(", partition=").$(partitionName)
                                .$(", path=").$(toParquetCs).$(']').$();
                        statusReporter.report(CopyExportRequestTask.PHASE_CONVERTING_PARTITIONS, CopyExportRequestTask.STATUS_PENDING, toParquetCs, Long.MIN_VALUE);


                        createDirsOrFail(ff, toParquet, configuration.getMkDirMode());
                        PartitionEncoder.encodeWithOptions(
                                partitionDescriptor,
                                toParquet,
                                ParquetCompression.packCompressionCodecLevel(compressionCodec, compressionLevel),
                                statisticsEnabled,
                                rawArrayEncoding,
                                rowGroupSize,
                                dataPageSize,
                                parquetVersion
                        );

                        long parquetFileSize = ff.length(toParquet.$());
                        LOG.info().$("converted partition to parquet [table=").$(tableToken)
                                .$(", partition=").$(partitionName)
                                .$(", size=").$(parquetFileSize).$(']')
                                .$();
                    }
                } catch (CairoException e) {
                    LOG.errorW().$("could not populate table reader [msg=").$(e.getFlyweightMessage()).$(']').$();
                    throw CopyExportException.instance(CopyExportRequestTask.PHASE_CONVERTING_PARTITIONS, e.getFlyweightMessage(), e.getErrno());
                }
            }
        }
        LOG.info().$("finished parquet conversion [table=").$(tableToken).$(']').$();
        statusReporter.report(CopyExportRequestTask.PHASE_CONVERTING_PARTITIONS, CopyExportRequestTask.STATUS_FINISHED, null, Long.MIN_VALUE);
    }

    @FunctionalInterface
    public interface PhaseStatusReporter {
        void report(byte phase, byte status, @Nullable final CharSequence msg, long errors);
    }

}
