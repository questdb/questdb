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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.griffin.SqlException;
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
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;

import static io.questdb.cairo.CairoException.TABLE_DOES_NOT_EXIST;
import static io.questdb.cairo.TableUtils.createDirsOrFail;

public class SerialParquetExporter implements Closeable {
    private static final Log LOG = LogFactory.getLog(SerialParquetExporter.class);
    private final CairoEngine cairoEngine;
    private final CairoConfiguration configuration;
    private final CharSequence copyExportRoot;
    private final FilesFacade ff;
    private final Path fromParquet;
    private final Path toParquet;
    private ExecutionCircuitBreaker circuitBreaker;
    private PhaseStatusReporter statusReporter;
    private CopyExportRequestTask task;

    public SerialParquetExporter(CairoEngine engine) {
        this.cairoEngine = engine;
        this.configuration = engine.getConfiguration();
        this.ff = this.configuration.getFilesFacade();
        this.copyExportRoot = this.configuration.getSqlCopyExportRoot();
        this.toParquet = new Path();
        this.fromParquet = new Path();
    }

    @Override
    public void close() throws IOException {
        Misc.free(toParquet);
        Misc.free(fromParquet);
    }

    public void of(CopyExportRequestTask task,
                   ExecutionCircuitBreaker circuitBreaker,
                   PhaseStatusReporter statusReporter) {
        this.task = task;
        this.circuitBreaker = circuitBreaker;
        this.statusReporter = statusReporter;
    }

    public CopyExportRequestTask.Phase process(SecurityContext securityContext) {
        CopyExportRequestTask.Phase phase = CopyExportRequestTask.Phase.NONE;
        TableToken tableToken = null;
        try {
            if (task.getCreateOp() != null) {
                phase = CopyExportRequestTask.Phase.POPULATING_TEMP_TABLE;
                statusReporter.report(phase, CopyExportRequestTask.Status.STARTED, task, null, 0);
                LOG.infoW().$("starting to create temporary table and populate with data [table=").$(task.getTableName()).$(']').$();
                task.getCreateOp().execute(task.getExecutionContext(), null);
                LOG.infoW().$("completed creating temporary table and populating with data [table=").$(task.getTableName()).$(']').$();
                statusReporter.report(phase, CopyExportRequestTask.Status.FINISHED, task, null, 0);
            }

            phase = CopyExportRequestTask.Phase.CONVERTING_PARTITIONS;
            statusReporter.report(phase, CopyExportRequestTask.Status.STARTED, task, null, 0);
            final String tableName = task.getTableName();
            final String fileName = task.getFileName() != null ? task.getFileName() : tableName;
            @Nullable CharSequence toParquetCs;
            int lastSlash;
            tableToken = cairoEngine.getTableTokenIfExists(tableName);
            if (tableToken == null) {
                throw CopyExportException.instance(phase, TABLE_DOES_NOT_EXIST).put("table does not exist [table=").put(tableName).put(']');
            }
            if (task.getCreateOp() == null) {
                securityContext.authorizeSelectOnAnyColumn(tableToken);
            }

            final int compressionCodec = task.getCompressionCodec();
            final int compressionLevel = task.getCompressionLevel();
            final int rowGroupSize = task.getRowGroupSize();
            final int dataPageSize = task.getDataPageSize();
            final boolean statisticsEnabled = task.isStatisticsEnabled();
            final int parquetVersion = task.getParquetVersion();
            final boolean rawArrayEncoding = task.isRawArrayEncoding();
            final boolean userSpecifiedExportOptions = task.isUserSpecifiedExportOptions();

            if (circuitBreaker.checkIfTripped()) {
                LOG.errorW().$("copy was cancelled [copyId=").$hexPadded(task.getCopyID()).$(']').$();
                throw CopyExportException.instance(phase, -1).put("cancelled by user").setInterruption(true).setCancellation(true);
            }

            try (TableReader reader = cairoEngine.getReader(tableToken)) {
                final int partitionCount = reader.getPartitionCount();
                final int partitionBy = reader.getPartitionedBy();

                if (partitionCount == 0) {
                    throw CopyExportException.instance(phase, TABLE_DOES_NOT_EXIST).put("table does not exist [table=").put(tableName).put(']');
                } else {
                    int fromParquetBaseLen = 0;
                    final int toParquetBaseLen = toParquet.trimTo(0).concat(copyExportRoot).concat(fileName).size();

                    try (PartitionDescriptor partitionDescriptor = new PartitionDescriptor()) {
                        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                            if (circuitBreaker.checkIfTripped()) {
                                LOG.errorW().$("copy was cancelled [copyId=").$hexPadded(task.getCopyID()).$(']').$();
                                throw CopyExportException.instance(phase, -1).put("cancelled by user").setInterruption(true).setCancellation(true);
                            }
                            final long partitionTimestamp = reader.getPartitionTimestampByIndex(partitionIndex);

                            // skip parquet conversion if the partition is already in parquet format
                            if (reader.getPartitionFormat(partitionIndex) == PartitionFormat.PARQUET) {
                                if (userSpecifiedExportOptions) {
                                    LOG.infoW().$("ignoring user-specified export options for parquet partition, re-encoding not yet supported [table=").$(tableToken)
                                            .$(", partition=").$(partitionTimestamp)
                                            .$(", using direct file copy instead]").$();
                                }
                                if (fromParquetBaseLen == 0) {
                                    fromParquetBaseLen = fromParquet.trimTo(0).concat(configuration.getDbRoot()).concat(tableToken.getDirName()).size();
                                } else {
                                    fromParquet.trimTo(fromParquetBaseLen);
                                }
                                PartitionBy.getPartitionDirFormatMethod(partitionBy)
                                        .format(partitionTimestamp, DateFormatUtils.EN_LOCALE, null, fromParquet.slash());
                                fromParquet.concat("data.parquet");

                                toParquet.trimTo(toParquetBaseLen);
                                PartitionBy.getPartitionDirFormatMethod(partitionBy)
                                        .format(partitionTimestamp, DateFormatUtils.EN_LOCALE, null, toParquet.slash());
                                toParquet.put(".parquet");
                                createDirsOrFail(ff, toParquet, configuration.getMkDirMode());

                                // copy file directly
                                int copyResult = ff.copy(fromParquet.$(), toParquet.$());
                                if (copyResult != 0) {
                                    throw CopyExportException.instance(phase, copyResult)
                                            .put("failed to copy parquet file [from=").put(fromParquet)
                                            .put(", to=").put(toParquet).put(']');
                                }

                                long parquetFileSize = ff.length(toParquet.$());
                                LOG.info().$("copied parquet partition directly [table=").$(tableToken)
                                        .$(", partition=").$(partitionTimestamp)
                                        .$(", size=").$(parquetFileSize).$(']').$();

                                continue;
                            }

                            // native partition - convert to parquet
                            reader.openPartition(partitionIndex);
                            PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, partitionIndex);
                            toParquet.trimTo(toParquetBaseLen);
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
                            statusReporter.report(phase, CopyExportRequestTask.Status.PENDING, task, toParquetCs, 0);

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
                        throw CopyExportException.instance(CopyExportRequestTask.Phase.CONVERTING_PARTITIONS, e.getFlyweightMessage(), e.getErrno());
                    }
                }
            }

            LOG.info().$("finished parquet conversion [table=").$(tableToken).$(']').$();
            statusReporter.report(phase, CopyExportRequestTask.Status.FINISHED, task, null, 0);
        } catch (CopyExportException e) {
            LOG.errorW().$("parquet export failed [msg=").$(e.getFlyweightMessage()).$(']').$();
            throw e;
        } catch (SqlException e) {
            LOG.errorW().$("parquet export failed [msg=").$(e.getFlyweightMessage()).$(']').$();
            throw CopyExportException.instance(phase, e.getFlyweightMessage(), e.getErrorCode());
        } catch (CairoException e) {
            LOG.errorW().$("parquet export failed [msg=").$(e.getFlyweightMessage()).$(']').$();
            throw CopyExportException.instance(phase, e.getFlyweightMessage(), e.getErrno());
        } catch (Throwable e) {
            LOG.errorW().$("parquet export failed [msg=").$(e.getMessage()).$(']').$();
            throw CopyExportException.instance(phase, e.getMessage(), -1);
        } finally {
            if (tableToken != null && task.getCreateOp() != null) {
                phase = CopyExportRequestTask.Phase.DROPPING_TEMP_TABLE;
                statusReporter.report(phase, CopyExportRequestTask.Status.PENDING, task, null, 0);
                try {
                    fromParquet.trimTo(0);
                    cairoEngine.dropTableOrMatView(fromParquet, tableToken);
                    statusReporter.report(phase, CopyExportRequestTask.Status.FINISHED, task, null, 0);
                } catch (CairoException e) {
                    // drop failure doesn't affect task continuation - log and proceed
                    LOG.errorW().$("fail to drop temporary table [table=").$(tableToken).$(", msg=").$(e.getFlyweightMessage()).$(']').$();
                    statusReporter.report(phase, CopyExportRequestTask.Status.FAILED, task, null, 0);
                }
            }
        }
        return phase;
    }

    @FunctionalInterface
    public interface PhaseStatusReporter {
        void report(CopyExportRequestTask.Phase phase, CopyExportRequestTask.Status status, CopyExportRequestTask task, @Nullable final CharSequence msg, long errors);
    }

}
