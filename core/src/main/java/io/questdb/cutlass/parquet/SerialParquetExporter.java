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
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cutlass.text.CopyExportResult;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
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
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import static io.questdb.cairo.CairoException.TABLE_DOES_NOT_EXIST;
import static io.questdb.cairo.TableUtils.createDirsOrFail;

public class SerialParquetExporter implements Closeable {
    private static final Log LOG = LogFactory.getLog(SerialParquetExporter.class);
    private final CairoEngine cairoEngine;
    private final CairoConfiguration configuration;
    private final CharSequence copyExportRoot;
    private final FilesFacade ff;
    private final Utf8StringSink files = new Utf8StringSink();
    private final Path fromParquet;
    private final Utf8StringSink nameSink = new Utf8StringSink();
    private final Path tempPath;
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
        this.tempPath = new Path();
    }

    public static void cleanupDir(FilesFacade ff, Path dir, int tempBaseDirLen) {
        try {
            dir.trimTo(tempBaseDirLen);
            if (ff.exists(dir.slash().$())) {
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

    public Utf8StringSink getFiles() {
        return files;
    }

    public void of(CopyExportRequestTask task,
                   ExecutionCircuitBreaker circuitBreaker,
                   PhaseStatusReporter statusReporter) {
        this.task = task;
        this.circuitBreaker = circuitBreaker;
        this.statusReporter = statusReporter;
    }

    public CopyExportRequestTask.Phase process(SqlExecutionContext sqlExecutionContext) {
        CopyExportRequestTask.Phase phase = CopyExportRequestTask.Phase.NONE;
        TableToken tableToken = null;
        int tempBaseDirLen = 0;
        CopyExportResult exportResult = task.getResult();

        try {
            if (task.getCreateOp() != null) {
                phase = CopyExportRequestTask.Phase.POPULATING_TEMP_TABLE;
                statusReporter.report(phase, CopyExportRequestTask.Status.STARTED, task, null, null, 0);
                LOG.info().$("starting to create temporary table and populate with data [table=").$(task.getTableName()).$(']').$();
                task.getCreateOp().execute(sqlExecutionContext, null);
                LOG.info().$("completed creating temporary table and populating with data [table=").$(task.getTableName()).$(']').$();
                statusReporter.report(phase, CopyExportRequestTask.Status.FINISHED, task, null, null, 0);
            }

            phase = CopyExportRequestTask.Phase.CONVERTING_PARTITIONS;
            statusReporter.report(phase, CopyExportRequestTask.Status.STARTED, task, null, null, 0);
            final String tableName = task.getTableName();
            final String fileName = task.getFileName() != null ? task.getFileName() : tableName;
            tableToken = cairoEngine.getTableTokenIfExists(tableName);
            if (tableToken == null) {
                throw CopyExportException.instance(phase, TABLE_DOES_NOT_EXIST).put("table does not exist [table=").put(tableName).put(']');
            }
            if (task.getCreateOp() == null) {
                sqlExecutionContext.getSecurityContext().authorizeSelectOnAnyColumn(tableToken);
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
                LOG.error().$("copy was cancelled [copyId=").$hexPadded(task.getCopyID()).$(']').$();
                throw CopyExportException.instance(phase, -1).put("cancelled by user").setInterruption(true).setCancellation(true);
            }
            files.clear();
            try (TableReader reader = cairoEngine.getReader(tableToken)) {
                final int timestampType = reader.getMetadata().getTimestampType();
                final int partitionCount = reader.getPartitionCount();
                final int partitionBy = reader.getPartitionedBy();

                int fromParquetBaseLen = 0;
                // temporary directory path
                tempPath.trimTo(0).concat(copyExportRoot).concat("tmp_");
                Numbers.appendHex(tempPath, task.getCopyID(), true);
                tempBaseDirLen = tempPath.size();

                try (PartitionDescriptor partitionDescriptor = new PartitionDescriptor()) {
                    for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                        if (circuitBreaker.checkIfTripped()) {
                            LOG.error().$("copy was cancelled [copyId=").$hexPadded(task.getCopyID()).$(']').$();
                            throw CopyExportException.instance(phase, -1).put("cancelled by user").setInterruption(true).setCancellation(true);
                        }
                        final long partitionTimestamp = reader.getPartitionTimestampByIndex(partitionIndex);

                        // skip parquet conversion if the partition is already in parquet format
                        if (reader.getPartitionFormat(partitionIndex) == PartitionFormat.PARQUET) {
                            if (userSpecifiedExportOptions) {
                                LOG.info().$("ignoring user-specified export options for parquet partition, re-encoding not yet supported [table=").$(tableToken)
                                        .$(", partition=").$(partitionTimestamp)
                                        .$(", using direct file copy instead]").$();
                            }
                            if (fromParquetBaseLen == 0) {
                                fromParquetBaseLen = fromParquet.trimTo(0).concat(configuration.getDbRoot()).size();
                            } else {
                                fromParquet.trimTo(fromParquetBaseLen);
                            }
                            fromParquet.concat(tableToken.getDirName());

                            // Find the directory with highest version number
                            nameSink.clear();
                            PartitionBy.getPartitionDirFormatMethod(timestampType, partitionBy)
                                    .format(partitionTimestamp, DateLocaleFactory.EN_LOCALE, null, nameSink);
                            CharSequence actualPartitionDir = findHighestVersionedPartitionDir(ff, fromParquet, nameSink.toString());
                            fromParquet.concat(actualPartitionDir).concat("data.parquet");
                            if (exportResult != null) {
                                exportResult.addFilePath(fromParquet, false, 0);
                                files.put(tableToken.getDirName()).put(File.separator).put(actualPartitionDir).put(".parquet");
                                if (partitionIndex < partitionCount - 1) {
                                    files.put(',');
                                }
                                continue;
                            }

                            tempPath.trimTo(tempBaseDirLen);
                            nameSink.clear();
                            PartitionBy.getPartitionDirFormatMethod(timestampType, partitionBy)
                                    .format(partitionTimestamp, DateLocaleFactory.EN_LOCALE, null, nameSink);
                            tempPath.concat(nameSink).put(".parquet");
                            createDirsOrFail(ff, tempPath, configuration.getMkDirMode());

                            int copyResult = ff.copy(fromParquet.$(), tempPath.$());
                            if (copyResult < 0) {
                                throw CopyExportException.instance(phase, copyResult)
                                        .put("failed to copy parquet file [from=").put(fromParquet)
                                        .put(", to=").put(tempPath).put(']');
                            }

                            files.put(fileName).put(File.separator).put(nameSink).put(".parquet");
                            if (partitionIndex < partitionCount - 1) {
                                files.put(',');
                            }
                            long parquetFileSize = ff.length(tempPath.$());
                            LOG.info().$("copied parquet partition directly to temp [table=").$(tableToken)
                                    .$(", partition=").$(nameSink)
                                    .$(", size=").$(parquetFileSize).$(']').$();

                            continue;
                        }

                        // native partition - convert to parquet
                        if (reader.openPartition(partitionIndex) <= 0) {
                            continue;
                        }
                        PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, partitionIndex);
                        nameSink.clear();
                        PartitionBy.getPartitionDirFormatMethod(timestampType, partitionBy)
                                .format(partitionTimestamp, DateLocaleFactory.EN_LOCALE, null, nameSink);
                        tempPath.trimTo(tempBaseDirLen);
                        tempPath.concat(nameSink).put(".parquet");
                        // log start
                        LOG.info().$("converting partition to parquet temp [table=").$(tableToken)
                                .$(", partition=").$(nameSink).$();

                        createDirsOrFail(ff, tempPath, configuration.getMkDirMode());
                        PartitionEncoder.encodeWithOptions(
                                partitionDescriptor,
                                tempPath,
                                ParquetCompression.packCompressionCodecLevel(compressionCodec, compressionLevel),
                                statisticsEnabled,
                                rawArrayEncoding,
                                rowGroupSize,
                                dataPageSize,
                                parquetVersion
                        );
                        long parquetFileSize = ff.length(tempPath.$());
                        LOG.info().$("converted partition to parquet temp [table=").$(tableToken)
                                .$(", partition=").$(nameSink)
                                .$(", size=").$(parquetFileSize).$(']')
                                .$();
                        files.put(fileName).put(File.separator).put(nameSink).put(".parquet");
                        if (partitionIndex < partitionCount - 1) {
                            files.put(',');
                        }
                        if (exportResult != null) {
                            exportResult.addFilePath(tempPath, true, tempBaseDirLen);
                        }
                    }
                }
            }

            if (exportResult == null) {
                moveExportFiles(tempBaseDirLen, fileName);
            }
            LOG.info().$("finished parquet conversion to temp [table=").$(tableToken).$(']').$();
            statusReporter.report(phase, CopyExportRequestTask.Status.FINISHED, task, null, null, 0);
        } catch (CopyExportException e) {
            LOG.error().$("parquet export failed [msg=").$(e.getFlyweightMessage()).$(']').$();
            throw e;
        } catch (SqlException e) {
            LOG.error().$("parquet export failed [msg=").$(e.getFlyweightMessage()).$(']').$();
            throw CopyExportException.instance(phase, e.getFlyweightMessage(), e.getErrorCode());
        } catch (CairoException e) {
            LOG.error().$("parquet export failed [msg=").$(e.getFlyweightMessage()).$(']').$();
            throw CopyExportException.instance(phase, e.getFlyweightMessage(), e.getErrno());
        } catch (Throwable e) {
            LOG.error().$("parquet export failed [msg=").$(e.getMessage()).$(']').$();
            throw CopyExportException.instance(phase, e.getMessage(), -1);
        } finally {
            if (tableToken != null && task.getCreateOp() != null) {
                phase = CopyExportRequestTask.Phase.DROPPING_TEMP_TABLE;
                statusReporter.report(phase, CopyExportRequestTask.Status.STARTED, task, null, null, 0);
                try {
                    fromParquet.trimTo(0);
                    cairoEngine.dropTableOrMatView(fromParquet, tableToken);
                    statusReporter.report(phase, CopyExportRequestTask.Status.FINISHED, task, null, null, 0);
                } catch (CairoException e) {
                    // drop failure doesn't affect task continuation - log and proceed
                    LOG.error().$("fail to drop temporary table [table=").$(tableToken).$(", msg=").$(e.getFlyweightMessage()).$(']').$();
                    statusReporter.report(phase, CopyExportRequestTask.Status.FAILED, task, null, null, 0);
                } catch (Throwable e) {
                    LOG.error().$("fail to drop temporary table [table=").$(tableToken).$(", msg=").$(e.getMessage()).$(']').$();
                    statusReporter.report(phase, CopyExportRequestTask.Status.FAILED, task, null, null, 0);
                }
            }
            if (exportResult == null) {
                cleanupDir(ff, tempPath, tempBaseDirLen);
            }

        }
        return phase;
    }

    private CharSequence findHighestVersionedPartitionDir(FilesFacade ff, Path basePath, CharSequence basePartitionName) {
        long maxVersion = -1;
        String bestMatch = null;
        nameSink.clear();
        int plimit = basePath.size();
        long p = ff.findFirst(basePath.$());
        if (p > 0) {
            try {
                do {
                    if (ff.isDirOrSoftLinkDirNoDots(basePath, plimit, ff.findName(p), ff.findType(p), nameSink)) {
                        CharSequence dirName = nameSink.asAsciiCharSequence();
                        int dirLength = dirName.length();
                        int partitionNameLength = basePartitionName.length();
                        if (Chars.startsWith(dirName, basePartitionName) &&
                                dirLength > partitionNameLength &&
                                dirName.charAt(partitionNameLength) == '.') {
                            try {
                                CharSequence versionStr = dirName.subSequence(partitionNameLength + 1, dirLength);
                                long version = Numbers.parseLong(versionStr);
                                if (version > maxVersion) {
                                    maxVersion = version;
                                    bestMatch = dirName.toString();
                                }
                            } catch (NumberFormatException ignored) {
                            }
                        }
                        nameSink.clear();
                    }
                } while (ff.findNext(p) > 0);
            } finally {
                basePath.trimTo(plimit).$();
                ff.findClose(p);
            }
        }

        if (bestMatch != null) {
            return bestMatch;
        }
        throw CairoException.nonCritical().put("no versioned parquet partition directory found for [basePartitionName=")
                .put(basePartitionName).put(']');
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
        tempPath.trimTo(tempBaseDirLen);
        if (!ff.exists(tempPath.$())) {
            return;
        }
        toParquet.trimTo(0).concat(copyExportRoot).concat(fileName);
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
            moveAndOverwriteFiles();
        }
    }

    @FunctionalInterface
    public interface PhaseStatusReporter {
        void report(CopyExportRequestTask.Phase phase, CopyExportRequestTask.Status status, CopyExportRequestTask task, Utf8Sequence files, @Nullable final CharSequence msg, long errors);
    }

}
