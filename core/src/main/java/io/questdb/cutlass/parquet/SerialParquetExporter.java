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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.cutlass.text.CopyExportResult;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.CreateTableOperation;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import static io.questdb.cairo.CairoException.TABLE_DOES_NOT_EXIST;

public class SerialParquetExporter implements Closeable {
    private static final Log LOG = LogFactory.getLog(SerialParquetExporter.class);
    private final CairoConfiguration configuration;
    private final StringSink exportPath = new StringSink(128);
    private final FilesFacade ff;
    private final Path fromParquet;
    private final Utf8StringSink nameSink = new Utf8StringSink();
    private final SqlExecutionContextImpl sqlExecutionContext;
    private final Path tempPath;
    private final Path toParquet;
    private ExecutionCircuitBreaker circuitBreaker;
    private CharSequence copyExportRoot;
    private int numOfFiles;
    private PhaseStatusReporter statusReporter;
    private CopyExportRequestTask task;

    public SerialParquetExporter(SqlExecutionContextImpl context) {
        this.sqlExecutionContext = context;
        this.configuration = context.getCairoEngine().getConfiguration();
        this.ff = this.configuration.getFilesFacade();
        this.toParquet = new Path();
        this.fromParquet = new Path();
        this.tempPath = new Path();
    }

    public static void cleanupDir(FilesFacade ff, Path dir, int tempBaseDirLen) {
        try {
            dir.trimTo(tempBaseDirLen);
            if (ff.exists(dir.slash$())) {
                ff.rmdir(dir);
            }
        } catch (Throwable e) {
            LOG.error().$("error during temp directory cleanup [path=").$(dir).$(" error=").$(e).$(']').$();
        }
    }

    @Override
    public void close() throws IOException {
        Misc.free(toParquet);
        Misc.free(fromParquet);
        Misc.free(tempPath);
    }


    public void of(CopyExportRequestTask task,
                   SqlExecutionCircuitBreaker circuitBreaker,
                   PhaseStatusReporter statusReporter) {
        this.copyExportRoot = configuration.getSqlCopyExportRoot();
        this.task = task;
        this.circuitBreaker = circuitBreaker;
        this.statusReporter = statusReporter;
        this.exportPath.clear();
        numOfFiles = 0;
        sqlExecutionContext.with(task.getSecurityContext(), null, null, -1, circuitBreaker);
    }

    public CopyExportRequestTask.Phase process() {
        CopyExportRequestTask.Phase phase = CopyExportRequestTask.Phase.NONE;
        TableToken tableToken = null;
        int tempBaseDirLen = 0;
        CopyExportResult exportResult = task.getResult();
        CopyExportContext.ExportTaskEntry entry = task.getEntry();
        final CairoEngine cairoEngine = sqlExecutionContext.getCairoEngine();
        CreateTableOperation createOp = task.getCreateOp();

        try {
            if (createOp != null) {
                createOp.setInsertSelectProgressReporter((stage, rows) -> {
                    switch (stage) {
                        case Start:
                            entry.setTotalRowCount(rows);
                            break;
                        case InsertIng:
                            entry.setPopulatedRowCount(rows);
                            LOG.info().$("populating temporary table progress [id=").$hexPadded(task.getCopyID())
                                    .$(", table=")
                                    .$(task.getTableName())
                                    .$(", rows=")
                                    .$(rows).$(']')
                                    .$();
                            break;
                        case Finish:
                            entry.setPopulatedRowCount(rows);
                            break;
                    }
                });
                phase = CopyExportRequestTask.Phase.POPULATING_TEMP_TABLE;
                entry.setPhase(phase);
                statusReporter.report(phase, CopyExportRequestTask.Status.STARTED, task, null, Numbers.INT_NULL, null, 0);
                LOG.info().$("starting to create temporary table and populate with data [id=").$hexPadded(task.getCopyID()).$(", table=").$(task.getTableName()).$(']').$();
                createOp.execute(sqlExecutionContext, null);
                LOG.info().$("completed creating temporary table and populating with data [id=").$hexPadded(task.getCopyID()).$(", table=").$(task.getTableName()).$(']').$();
                statusReporter.report(phase, CopyExportRequestTask.Status.FINISHED, task, null, Numbers.INT_NULL, null, 0);
            }

            phase = CopyExportRequestTask.Phase.CONVERTING_PARTITIONS;
            entry.setPhase(phase);
            statusReporter.report(phase, CopyExportRequestTask.Status.STARTED, task, null, Numbers.INT_NULL, null, 0);
            final String tableName = task.getTableName();
            final String fileName = task.getFileName() != null ? task.getFileName() : tableName;
            tableToken = cairoEngine.getTableTokenIfExists(tableName);
            if (tableToken == null) {
                throw CopyExportException.instance(phase, TABLE_DOES_NOT_EXIST).put("table does not exist [table=").put(tableName).put(']');
            }
            if (createOp == null) {
                sqlExecutionContext.getSecurityContext().authorizeSelectOnAnyColumn(tableToken);
            }

            if (circuitBreaker.checkIfTripped()) {
                LOG.error().$("copy was cancelled [id=").$hexPadded(task.getCopyID()).$(']').$();
                throw CopyExportException.instance(phase, -1).put("cancelled by user").setInterruption(true).setCancellation(true);
            }
            try (TableReader reader = cairoEngine.getReader(tableToken)) {
                final int timestampType = reader.getMetadata().getTimestampType();
                final int partitionCount = reader.getPartitionCount();
                final int partitionBy = reader.getPartitionedBy();
                entry.setTotalPartitionCount(partitionCount);

                int fromParquetBaseLen = 0;
                // temporary directory path
                tempPath.trimTo(0).concat(copyExportRoot).concat("tmp_");
                Numbers.appendHex(tempPath, task.getCopyID(), true);
                tempBaseDirLen = tempPath.size();
                createDirsOrFail(ff, tempPath.slash(), configuration.getMkDirMode());

                try (PartitionDescriptor partitionDescriptor = new PartitionDescriptor()) {
                    for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                        if (circuitBreaker.checkIfTripped()) {
                            LOG.error().$("copy was cancelled [id=").$hexPadded(task.getCopyID()).$(']').$();
                            throw CopyExportException.instance(phase, -1).put("cancelled by user").setInterruption(true).setCancellation(true);
                        }
                        final long partitionTimestamp = reader.getPartitionTimestampByIndex(partitionIndex);
                        if (reader.openPartition(partitionIndex) <= 0) {
                            entry.setFinishedPartitionCount(partitionIndex + 1);
                            continue;
                        }
                        // skip parquet conversion if the partition is already in parquet format
                        if (reader.getPartitionFormat(partitionIndex) == PartitionFormat.PARQUET) {
                            numOfFiles++;
                            if (task.isUserSpecifiedExportOptions()) {
                                LOG.info().$("ignoring user-specified export options for parquet partition, re-encoding not yet supported [id=").$hexPadded(task.getCopyID()).$(", table=").$(tableToken)
                                        .$(", partition=").$(partitionTimestamp)
                                        .$(", using direct file copy instead]").$();
                            }
                            if (fromParquetBaseLen == 0) {
                                fromParquetBaseLen = fromParquet.trimTo(0).concat(configuration.getDbRoot()).size();
                            } else {
                                fromParquet.trimTo(fromParquetBaseLen);
                            }

                            fromParquet.concat(tableToken.getDirName());
                            TableUtils.setPathForParquetPartition(
                                    fromParquet, timestampType, partitionBy, partitionTimestamp, reader.getTxFile().getPartitionNameTxn(partitionIndex));
                            if (exportResult != null) {
                                exportResult.addFilePath(fromParquet, false, 0);
                                entry.setFinishedPartitionCount(partitionIndex + 1);
                                continue;
                            }

                            tempPath.trimTo(tempBaseDirLen);
                            nameSink.clear();
                            PartitionBy.getPartitionDirFormatMethod(timestampType, partitionBy)
                                    .format(partitionTimestamp, DateLocaleFactory.EN_LOCALE, null, nameSink);
                            tempPath.concat(nameSink).put(".parquet");

                            int copyResult = ff.copy(fromParquet.$(), tempPath.$());
                            if (copyResult < 0) {
                                throw CopyExportException.instance(phase, ff.errno())
                                        .put("failed to copy parquet file [from=").put(fromParquet)
                                        .put(", to=").put(tempPath).put(']');
                            }

                            long parquetFileSize = ff.length(tempPath.$());
                            LOG.info().$("copied parquet partition directly to temp [id=").$hexPadded(task.getCopyID()).$(", table=").$(tableToken)
                                    .$(", partition=").$(nameSink)
                                    .$(", size=").$(parquetFileSize).$(']').$();
                            entry.setFinishedPartitionCount(partitionIndex + 1);
                            continue;
                        }

                        // native partition - convert to parquet
                        numOfFiles++;
                        PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, partitionIndex);
                        nameSink.clear();
                        PartitionBy.getPartitionDirFormatMethod(timestampType, partitionBy)
                                .format(partitionTimestamp, DateLocaleFactory.EN_LOCALE, null, nameSink);
                        tempPath.trimTo(tempBaseDirLen);
                        tempPath.concat(nameSink).put(".parquet");
                        // log start
                        LOG.info().$("converting partition to parquet temp file [id=").$hexPadded(task.getCopyID()).$(", table=").$(tableToken)
                                .$(", partition=").$(nameSink).$();

                        PartitionEncoder.encodeWithOptions(
                                partitionDescriptor,
                                tempPath,
                                ParquetCompression.packCompressionCodecLevel(task.getCompressionCodec(), task.getCompressionLevel()),
                                task.isStatisticsEnabled(),
                                task.isRawArrayEncoding(),
                                task.getRowGroupSize(),
                                task.getDataPageSize(),
                                task.getParquetVersion()
                        );
                        long parquetFileSize = ff.length(tempPath.$());
                        LOG.info().$("converted partition to parquet temp [id=").$hexPadded(task.getCopyID()).$(", table=").$(tableToken)
                                .$(", partition=").$(nameSink)
                                .$(", size=").$(parquetFileSize).$(']')
                                .$();
                        if (exportResult != null) {
                            exportResult.addFilePath(tempPath, true, tempBaseDirLen);
                        }
                        entry.setFinishedPartitionCount(partitionIndex + 1);
                    }
                }
            }

            if (exportResult == null) {
                entry.setPhase(CopyExportRequestTask.Phase.MOVE_FILES);
                moveExportFiles(tempBaseDirLen, fileName);
            }
            LOG.info().$("finished parquet conversion to temp [id=").$hexPadded(task.getCopyID()).$(", table=").$(tableToken).$(']').$();
            statusReporter.report(phase, CopyExportRequestTask.Status.FINISHED, task, null, Numbers.INT_NULL, null, 0);
        } catch (CopyExportException e) {
            LOG.error().$("parquet export failed [id=").$hexPadded(task.getCopyID()).$(", msg=").$(e.getFlyweightMessage()).$(']').$();
            throw e;
        } catch (SqlException e) {
            LOG.error().$("parquet export failed [id=").$hexPadded(task.getCopyID()).$(", msg=").$(e.getFlyweightMessage()).$(']').$();
            throw CopyExportException.instance(phase, e.getFlyweightMessage(), e.getErrorCode());
        } catch (CairoException e) {
            LOG.error().$("parquet export failed [id=").$hexPadded(task.getCopyID()).$(", msg=").$(e.getFlyweightMessage()).$(']').$();
            throw CopyExportException.instance(phase, e.getFlyweightMessage(), e.getErrno());
        } catch (Throwable e) {
            LOG.error().$("parquet export failed [id=").$hexPadded(task.getCopyID()).$(", msg=").$(e.getMessage()).$(']').$();
            throw CopyExportException.instance(phase, e.getMessage(), -1);
        } finally {
            if (tableToken != null && createOp != null) {
                phase = CopyExportRequestTask.Phase.DROPPING_TEMP_TABLE;
                entry.setPhase(phase);
                statusReporter.report(phase, CopyExportRequestTask.Status.STARTED, task, null, Numbers.INT_NULL, null, 0);
                try {
                    fromParquet.trimTo(0);
                    cairoEngine.dropTableOrMatView(fromParquet, tableToken);
                    statusReporter.report(phase, CopyExportRequestTask.Status.FINISHED, task, null, Numbers.INT_NULL, null, 0);
                } catch (CairoException e) {
                    // drop failure doesn't affect task continuation - log and proceed
                    LOG.error().$("fail to drop temporary table [id=").$hexPadded(task.getCopyID()).$(", table=").$(tableToken).$(", msg=").$(e.getFlyweightMessage()).$(']').$();
                    statusReporter.report(phase, CopyExportRequestTask.Status.FAILED, task, null, Numbers.INT_NULL, null, 0);
                } catch (Throwable e) {
                    LOG.error().$("fail to drop temporary table [id=").$hexPadded(task.getCopyID()).$(", table=").$(tableToken).$(", msg=").$(e.getMessage()).$(']').$();
                    statusReporter.report(phase, CopyExportRequestTask.Status.FAILED, task, null, Numbers.INT_NULL, null, 0);
                }
            }
            if (exportResult == null) {
                cleanupDir(ff, tempPath, tempBaseDirLen);
            }
        }
        return phase;
    }

    private static void createDirsOrFail(FilesFacade ff, Path path, int mkDirMode) {
        synchronized (SerialParquetExporter.class) {
            if (ff.mkdirs(path, mkDirMode) != 0) {
                throw CairoException.critical(ff.errno()).put("could not create directories [file=").put(path).put(']');
            }
        }
    }

    private void moveAndOverwriteFiles() {
        int srcPlen = tempPath.size();
        int dstPlen = toParquet.size();
        ff.iterateDir(tempPath.$(), (long name, int type) -> {
            if (type == Files.DT_FILE) {
                tempPath.trimTo(srcPlen).concat(name);
                toParquet.trimTo(dstPlen).concat(name);
                if (ff.exists(toParquet.$())) {
                    LOG.info().$("overwriting existing file [path=").$(toParquet).$(']').$();
                    ff.remove(toParquet.$());
                }

                if (ff.rename(tempPath.$(), toParquet.$()) != Files.FILES_RENAME_OK) {
                    throw CopyExportException.instance(CopyExportRequestTask.Phase.CONVERTING_PARTITIONS, ff.errno())
                            .put("could not copy export file [from=")
                            .put(tempPath)
                            .put(", to=")
                            .put(toParquet)
                            .put(", errno=").put(ff.errno()).put(']');
                }
            }
        });
    }

    private void moveExportFiles(int tempBaseDirLen, String fileName) {
        if (numOfFiles == 0) {
            return;
        }
        if (numOfFiles == 1) {
            moveFile(fileName);
            return;
        }
        tempPath.trimTo(tempBaseDirLen);
        toParquet.trimTo(0).concat(copyExportRoot).concat(fileName);
        this.exportPath.put(toParquet.$()).put(File.separator);
        createDirsOrFail(ff, toParquet.slash(), configuration.getMkDirMode());
        boolean destExists = ff.exists(toParquet.$());
        if (!destExists) {
            int moveResult = ff.rename(tempPath.$(), toParquet.$());
            if (moveResult != Files.FILES_RENAME_OK) {
                throw CopyExportException.instance(CopyExportRequestTask.Phase.CONVERTING_PARTITIONS, ff.errno())
                        .put("could not rename export directory [from=")
                        .put(tempPath)
                        .put(", to=")
                        .put(toParquet)
                        .put(", errno=").put(ff.errno()).put(']');
            }
        } else {
            if (!ff.isDirOrSoftLinkDir(toParquet.$())) {
                ff.removeQuiet(toParquet.$());
                int moveResult = ff.rename(tempPath.$(), toParquet.$());
                if (moveResult != Files.FILES_RENAME_OK) {
                    throw CopyExportException.instance(CopyExportRequestTask.Phase.CONVERTING_PARTITIONS, ff.errno())
                            .put("could not rename export directory after file removal [from=")
                            .put(tempPath)
                            .put(", to=")
                            .put(toParquet)
                            .put(", errno=").put(ff.errno()).put(']');
                }
            } else {
                moveAndOverwriteFiles();
            }
        }
    }

    private void moveFile(String fileName) {
        if (!ff.exists(tempPath.$())) {
            return;
        }
        toParquet.trimTo(0).concat(copyExportRoot).concat(fileName);
        if (!Chars.endsWith(fileName, ".parquet")) {
            toParquet.put(".parquet");
        }
        this.exportPath.put(toParquet.$());
        createDirsOrFail(ff, toParquet, configuration.getMkDirMode());
        boolean destExists = ff.exists(toParquet.$());
        if (destExists) {
            ff.removeQuiet(toParquet.$());
        }
        int moveResult = ff.rename(tempPath.$(), toParquet.$());
        if (moveResult != Files.FILES_RENAME_OK) {
            throw CopyExportException.instance(CopyExportRequestTask.Phase.CONVERTING_PARTITIONS, ff.errno())
                    .put("could not rename export file [from=")
                    .put(tempPath)
                    .put(", to=")
                    .put(toParquet)
                    .put(", errno=").put(ff.errno()).put(']');
        }
    }

    CharSequence getExportPath() {
        return exportPath;
    }

    int getNumOfFiles() {
        return numOfFiles;
    }

    @FunctionalInterface
    public interface PhaseStatusReporter {
        void report(CopyExportRequestTask.Phase phase,
                    CopyExportRequestTask.Status status,
                    CopyExportRequestTask task,
                    CharSequence export_dir,
                    int numOfFiles,
                    CharSequence msg,
                    long errors);
    }
}
