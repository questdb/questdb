/*******************************************************************************
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

package io.questdb.cutlass.parquet;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.SqlException;
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

public class SQLSerialParquetExporter extends HTTPSerialParquetExporter implements Closeable {
    private static final Log LOG = LogFactory.getLog(SQLSerialParquetExporter.class);
    private final CairoConfiguration configuration;
    private final StringSink exportPath = new StringSink(128);
    private final FilesFacade ff;
    private final Path fromParquet;
    private final Utf8StringSink nameSink = new Utf8StringSink();
    private final Path tempPath;
    private final Path toParquet;
    private CharSequence copyExportRoot;
    private int numOfFiles;

    public SQLSerialParquetExporter(CairoEngine engine) {
        super(engine);
        this.configuration = engine.getConfiguration();
        this.ff = this.configuration.getFilesFacade();
        this.toParquet = new Path();
        this.fromParquet = new Path();
        this.tempPath = new Path();
    }

    @Override
    public void close() {
        Misc.free(toParquet);
        Misc.free(fromParquet);
        Misc.free(tempPath);
    }

    @Override
    public void of(CopyExportRequestTask task) {
        super.of(task);
        this.copyExportRoot = configuration.getSqlCopyExportRoot();
        this.exportPath.clear();
        numOfFiles = 0;
    }

    @Override
    public CopyExportRequestTask.Phase process() {
        if (copyExportRoot == null) {
            throw CairoException.nonCritical().put("parquet export is disabled ['cairo.sql.copy.export.root' is not set]");
        }

        sqlExecutionContext.setNowAndFixClock(task.getNow(), task.getNowTimestampType());
        CopyExportRequestTask.Phase phase = CopyExportRequestTask.Phase.NONE;
        TableToken tableToken = null;
        int tempBaseDirLen = 0;
        CopyExportContext.ExportTaskEntry entry = task.getEntry();
        final CairoEngine cairoEngine = sqlExecutionContext.getCairoEngine();
        CreateTableOperation createOp = task.getCreateOp();
        boolean success = false;

        try {
            if (createOp != null) {
                insertSelectReporter.of(circuitBreaker, entry, task.getCopyID(), task.getTableName());
                createOp.setCopyDataProgressReporter(insertSelectReporter);
                phase = CopyExportRequestTask.Phase.POPULATING_TEMP_TABLE;
                entry.setPhase(phase);
                copyExportContext.updateStatus(phase, CopyExportRequestTask.Status.STARTED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
                LOG.info().$("starting to create temporary table and populate with data [id=").$hexPadded(task.getCopyID()).$(", table=").$(task.getTableName()).$(']').$();
                createOp.execute(sqlExecutionContext, null);
                tableToken = cairoEngine.verifyTableName(task.getTableName());
                LOG.info().$("completed creating temporary table and populating with data [id=").$hexPadded(task.getCopyID()).$(", table=").$(tableToken).$(']').$();
                copyExportContext.updateStatus(phase, CopyExportRequestTask.Status.FINISHED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
            }

            phase = CopyExportRequestTask.Phase.CONVERTING_PARTITIONS;
            entry.setPhase(phase);
            copyExportContext.updateStatus(phase, CopyExportRequestTask.Status.STARTED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
            final String tableName = task.getTableName();
            // we can lift file name from the task as CharSequence (without materializing it) because this task was
            // and always is assigned to this instance of the exporter
            final CharSequence fileName = task.getFileName() != null ? task.getFileName() : tableName;
            tableToken = cairoEngine.verifyTableName(task.getTableName());
            if (createOp == null) {
                sqlExecutionContext.getSecurityContext().authorizeSelectOnAnyColumn(tableToken);
            }

            if (circuitBreaker.checkIfTripped()) {
                LOG.error().$("copy was cancelled [id=").$hexPadded(task.getCopyID()).$(']').$();
                throw CopyExportException.instance(phase, -1).put("cancelled by user").setInterruption(true).setCancellation(true);
            }

            try (TableReader reader = cairoEngine.getReader(tableToken)) {
                // Enable streaming mode to use MADV_SEQUENTIAL/DONTNEED hints,
                // releasing page cache after each partition is processed
                reader.setStreamingMode(true);
                final int timestampType = reader.getMetadata().getTimestampType();
                final int partitionCount = reader.getPartitionCount();
                final int partitionBy = reader.getPartitionedBy();
                entry.setTotalPartitionCount(partitionCount);

                if (partitionCount > 0) {
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
                            boolean emptyPartition = reader.openPartition(partitionIndex) <= 0;

                            // skip parquet conversion if the partition is already in parquet format
                            if (reader.getPartitionFormat(partitionIndex) == PartitionFormat.PARQUET) {
                                numOfFiles++;
                                if (fromParquetBaseLen == 0) {
                                    fromParquetBaseLen = fromParquet.trimTo(0).concat(configuration.getDbRoot()).size();
                                } else {
                                    fromParquet.trimTo(fromParquetBaseLen);
                                }

                                fromParquet.concat(tableToken.getDirName());
                                TableUtils.setPathForParquetPartition(
                                        fromParquet, timestampType, partitionBy, partitionTimestamp, reader.getTxFile().getPartitionNameTxn(partitionIndex));
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
                                reader.closePartitionByIndex(partitionIndex);
                                continue;
                            }

                            // native partition - convert to parquet
                            numOfFiles++;
                            if (emptyPartition) {
                                PartitionEncoder.populateEmptyPartition(reader, partitionDescriptor);
                            } else {
                                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, partitionIndex);
                            }
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
                            entry.setFinishedPartitionCount(partitionIndex + 1);
                            // Release page cache for processed partition
                            reader.closePartitionByIndex(partitionIndex);
                        }
                    }
                }
            }

            copyExportContext.updateStatus(CopyExportRequestTask.Phase.CONVERTING_PARTITIONS, CopyExportRequestTask.Status.FINISHED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
            phase = CopyExportRequestTask.Phase.MOVE_FILES;
            entry.setPhase(phase);
            copyExportContext.updateStatus(phase, CopyExportRequestTask.Status.STARTED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
            moveExportFiles(tempBaseDirLen, fileName);
            copyExportContext.updateStatus(phase, CopyExportRequestTask.Status.FINISHED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
            LOG.info().$("finished parquet conversion to temp [id=").$hexPadded(task.getCopyID()).$(", table=").$(tableToken).$(']').$();
            success = true;
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
            LOG.error().$("parquet export failed [id=").$hexPadded(task.getCopyID()).$(", msg=").$(e).$(']').$();
            throw CopyExportException.instance(phase, e.getMessage(), -1);
        } finally {
            if (createOp != null) {
                phase = CopyExportRequestTask.Phase.DROPPING_TEMP_TABLE;
                entry.setPhase(phase);
                copyExportContext.updateStatus(phase, CopyExportRequestTask.Status.STARTED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
                try {
                    if (tableToken == null) {
                        tableToken = cairoEngine.getTableTokenIfExists(task.getTableName());
                    }
                    if (tableToken != null) {
                        fromParquet.trimTo(0);
                        cairoEngine.dropTableOrViewOrMatView(fromParquet, tableToken);
                    }
                    copyExportContext.updateStatus(phase, CopyExportRequestTask.Status.FINISHED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
                } catch (CairoException e) {
                    // drop failure doesn't affect task continuation - log and proceed
                    LOG.error().$("fail to drop temporary table [id=").$hexPadded(task.getCopyID()).$(", table=").$(tableToken).$(", msg=").$(e.getFlyweightMessage()).$(']').$();
                    copyExportContext.updateStatus(phase, CopyExportRequestTask.Status.FAILED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
                } catch (Throwable e) {
                    LOG.error().$("fail to drop temporary table [id=").$hexPadded(task.getCopyID()).$(", table=").$(tableToken).$(", msg=").$(e.getMessage()).$(']').$();
                    copyExportContext.updateStatus(phase, CopyExportRequestTask.Status.FAILED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
                }
            }
            if (numOfFiles == 0 || !success) {
                tempPath.trimTo(tempBaseDirLen);
                TableUtils.cleanupDirQuiet(ff, tempPath, LOG);
            }
        }
        return phase;
    }

    private static void createDirsOrFail(FilesFacade ff, Path path, int mkDirMode) {
        synchronized (SQLSerialParquetExporter.class) {
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
                    throw CopyExportException.instance(CopyExportRequestTask.Phase.MOVE_FILES, ff.errno())
                            .put("could not copy export file [from=")
                            .put(tempPath)
                            .put(", to=")
                            .put(toParquet)
                            .put(", errno=").put(ff.errno()).put(']');
                }
            }
        });
    }

    private void moveExportFiles(int tempBaseDirLen, CharSequence fileName) {
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
                throw CopyExportException.instance(CopyExportRequestTask.Phase.MOVE_FILES, ff.errno())
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
                    throw CopyExportException.instance(CopyExportRequestTask.Phase.MOVE_FILES, ff.errno())
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

    private void moveFile(CharSequence fileName) {
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
            throw CopyExportException.instance(CopyExportRequestTask.Phase.MOVE_FILES, ff.errno())
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
}
