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

package io.questdb.cairo;

import io.questdb.cairo.idx.IndexFactory;
import io.questdb.cairo.idx.IndexWriter;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectIntList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FindVisitor;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.openSmallFile;
import static io.questdb.cairo.wal.WalUtils.TXNLOG_FILE_NAME;
import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;

/**
 * Shared helper class for restoring table files from checkpoint or backup.
 * Used by both DatabaseCheckpointAgent and BackupRestoreAgent.
 */
public class TableSnapshotRestore implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(TableSnapshotRestore.class);
    private final AtomicBoolean abortParallelTasks = new AtomicBoolean(false);
    private final CairoConfiguration configuration;
    private final ExecutorService executor;
    private final FilesFacade ff;
    private final ObjList<Future<?>> futures = new ObjList<>();
    private final Utf8StringSink utf8Sink = new Utf8StringSink();
    private ColumnVersionReader columnVersionReader;
    private MemoryCMARW memFile = Vm.getCMARWInstance();
    private Path partitionCleanPath;
    private DateFormat partitionDirFmt;
    private int pathTableLen;
    private TableReaderMetadata tableMetadata;
    private TxWriter txWriter;
    private final FindVisitor removePartitionDirsNotAttached = this::removePartitionDirsNotAttached;

    public TableSnapshotRestore(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        int threadCount = Math.max(
                configuration.getCheckpointRecoveryThreadpoolMin(),
                Math.min(configuration.getCheckpointRecoveryThreadpoolMax(), Runtime.getRuntime().availableProcessors())
        );
        this.executor = Executors.newFixedThreadPool(threadCount);
    }

    public void abortParallelTasks() {
        abortParallelTasks.set(true);
    }

    @Override
    public void close() {
        futures.clear();
        executor.shutdownNow();
        tableMetadata = Misc.free(tableMetadata);
        txWriter = Misc.free(txWriter);
        columnVersionReader = Misc.free(columnVersionReader);
        memFile = Misc.free(memFile);
    }

    /**
     * Copies all metadata files for a table from source to destination.
     * Includes: _meta, _name (optional), _txn, _cv, mat view state (optional), mat view definition (optional)
     *
     * @param srcPath            source path (will be modified)
     * @param dstPath            destination path (will be modified)
     * @param recoveredMetaFiles counter for recovered meta files
     */
    public void copyMetadataFiles(Path srcPath, Path dstPath, AtomicInteger recoveredMetaFiles) {
        int srcPathLen = srcPath.size();
        int dstPathLen = dstPath.size();
        try {
            copyFile(srcPath.trimTo(srcPathLen), dstPath.trimTo(dstPathLen), recoveredMetaFiles, TableUtils.META_FILE_NAME, false);
            // Name file is optional, it is included in newer checkpoints and backups but may not exist in old checkpoints
            copyFile(srcPath.trimTo(srcPathLen), dstPath.trimTo(dstPathLen), recoveredMetaFiles, TableUtils.TABLE_NAME_FILE, true);
            copyFile(srcPath.trimTo(srcPathLen), dstPath.trimTo(dstPathLen), recoveredMetaFiles, TableUtils.TXN_FILE_NAME, false);
            copyFile(srcPath.trimTo(srcPathLen), dstPath.trimTo(dstPathLen), recoveredMetaFiles, TableUtils.COLUMN_VERSION_FILE_NAME, false);
            copyFile(srcPath.trimTo(srcPathLen), dstPath.trimTo(dstPathLen), recoveredMetaFiles, MatViewState.MAT_VIEW_STATE_FILE_NAME, true);
            copyFile(srcPath.trimTo(srcPathLen), dstPath.trimTo(dstPathLen), recoveredMetaFiles, MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME, true);
        } finally {
            srcPath.trimTo(srcPathLen);
            dstPath.trimTo(dstPathLen);
        }
    }

    /**
     * Copies all view metadata files for a table from source to destination.
     * Includes: _meta, _name (optional), _txn, _view
     *
     * @param srcPath            source path (will be modified)
     * @param dstPath            destination path (will be modified)
     * @param recoveredMetaFiles counter for recovered meta files
     */
    public void copyViewMetadataFiles(Path srcPath, Path dstPath, AtomicInteger recoveredMetaFiles) {
        int srcPathLen = srcPath.size();
        int dstPathLen = dstPath.size();
        try {
            copyFile(srcPath.trimTo(srcPathLen), dstPath.trimTo(dstPathLen), recoveredMetaFiles, TableUtils.META_FILE_NAME, false);
            copyFile(srcPath.trimTo(srcPathLen), dstPath.trimTo(dstPathLen), recoveredMetaFiles, TableUtils.TABLE_NAME_FILE, true);
            copyFile(srcPath.trimTo(srcPathLen), dstPath.trimTo(dstPathLen), recoveredMetaFiles, TableUtils.TXN_FILE_NAME, false);
            copyFile(srcPath.trimTo(srcPathLen), dstPath.trimTo(dstPathLen), recoveredMetaFiles, ViewDefinition.VIEW_DEFINITION_FILE_NAME, false);
        } finally {
            srcPath.trimTo(srcPathLen);
            dstPath.trimTo(dstPathLen);
        }
    }


    public void finalizeParallelTasks() {
        if (futures.size() > 0) {
            LOG.info().$("awaiting ").$(futures.size()).$(" parallel tasks to complete").I$();
        }

        for (int i = 0, n = futures.size(); i < n; i++) {
            try {
                futures.getQuick(i).get();
            } catch (InterruptedException e) {
                LOG.error().$("parallel task interrupted ").$(e).I$();
                throw CairoException.critical(0).put("parallel task interrupted");
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause != null) {
                    LOG.critical().$("error in parallel task").$(cause).I$();
                } else {
                    LOG.critical().$("error in parallel task: ").$(e.getMessage()).I$();
                }
                final CairoException ex = CairoException.critical(0)
                        .put("error in parallel task")
                        .put(": ")
                        .put(cause != null ? cause.getMessage() : e.getMessage());
                ex.initCause(cause != null ? cause : e);
                throw ex;
            }
        }
    }

    // Used in enterprise edition
    @SuppressWarnings("unused")
    public MemoryMARW getMemFile() {
        return memFile;
    }

    public TableReaderMetadata getTableMetadata() {
        return tableMetadata;
    }

    // Used in enterprise edition
    @SuppressWarnings("unused")
    public TxReader getTableTxReader() {
        return txWriter;
    }

    /**
     * Processes WAL sequencer metadata - copies meta file and updates txnlog if needed.
     *
     * @param srcPath           source path (will be modified)
     * @param dstPath           destination path (will be modified)
     * @param recoveredWalFiles counter for recovered WAL files
     */
    public void processWalSequencerMetadata(
            Path srcPath,
            Path dstPath,
            AtomicInteger recoveredWalFiles,
            long lastSeqTxn
    ) {
        // Go inside SEQ_DIR
        srcPath.concat(WalUtils.SEQ_DIR);
        int srcSeqLen = srcPath.size();
        srcPath.concat(TableUtils.META_FILE_NAME);

        dstPath.concat(WalUtils.SEQ_DIR);
        int dstSeqLen = dstPath.size();
        dstPath.concat(TableUtils.META_FILE_NAME);

        if (ff.exists(srcPath.$())) {
            if (ff.copy(srcPath.$(), dstPath.$()) < 0) {
                throw CairoException.critical(ff.errno())
                        .put("Recovery failed. Could not copy meta file [src=").put(srcPath).put(", dst=").put(dstPath).put(']');
            } else {
                final long newMaxTxn;
                if (lastSeqTxn > -1) {
                    newMaxTxn = lastSeqTxn;
                } else {
                    // Read max txn from source checkpoint
                    openSmallFile(ff, srcPath.trimTo(srcSeqLen), srcSeqLen, memFile, TableUtils.CHECKPOINT_SEQ_TXN_FILE_NAME, MemoryTag.MMAP_TX_LOG);
                    newMaxTxn = memFile.getLong(0L);
                }

                if (newMaxTxn >= 0) {
                    dstPath.trimTo(dstSeqLen);
                    openSmallFile(ff, dstPath, dstSeqLen, memFile, TXNLOG_FILE_NAME, MemoryTag.MMAP_TX_LOG);
                    long oldMaxTxn = memFile.getLong(TableTransactionLogFile.MAX_TXN_OFFSET_64);
                    if (newMaxTxn < oldMaxTxn) {
                        memFile.putLong(TableTransactionLogFile.MAX_TXN_OFFSET_64, newMaxTxn);
                        LOG.info()
                                .$("updated ").$(TXNLOG_FILE_NAME).$(" file [path=").$(dstPath)
                                .$(", oldMaxTxn=").$(oldMaxTxn)
                                .$(", newMaxTxn=").$(newMaxTxn)
                                .I$();
                    }
                }

                recoveredWalFiles.incrementAndGet();
                LOG.info()
                        .$("recovered ").$(TableUtils.META_FILE_NAME).$(" file [src=").$(srcPath)
                        .$(", dst=").$(dstPath)
                        .I$();
            }
        }
    }

    /**
     * Rebuilds table files including symbol maps and optionally purges non-attached partitions.
     *
     * @param tablePath                     path to the table directory
     * @param recoveredSymbolFiles          counter for recovered symbol files
     * @param rebuildPartitionColumnIndexes whether to rebuild bitmap indexes for symbol columns in partitions
     */
    public void rebuildTableFiles(
            Path tablePath,
            AtomicInteger recoveredSymbolFiles,
            boolean rebuildPartitionColumnIndexes
    ) {
        pathTableLen = tablePath.size();
        try {
            if (tableMetadata == null) {
                tableMetadata = new TableReaderMetadata(configuration);
            }
            tableMetadata.loadMetadata(tablePath.concat(TableUtils.META_FILE_NAME).$());

            if (txWriter == null) {
                txWriter = new TxWriter(configuration.getFilesFacade(), configuration);
            }
            txWriter.ofRW(tablePath.trimTo(pathTableLen).concat(TableUtils.TXN_FILE_NAME).$(), tableMetadata.getTimestampType(), tableMetadata.getPartitionBy());
            txWriter.unsafeLoadAll();

            if (columnVersionReader == null) {
                columnVersionReader = new ColumnVersionReader();
            }
            tablePath.trimTo(pathTableLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME);
            columnVersionReader.ofRO(configuration.getFilesFacade(), tablePath.$());
            columnVersionReader.readUnsafe();

            // Symbols are not append-only data structures, they can be corrupt
            // when symbol files are copied while written to. We need to rebuild them.
            rebuildSymbolFiles(tablePath, recoveredSymbolFiles, pathTableLen);

            // Recreate the bitmap indexes for each indexed column in each partition
            if (rebuildPartitionColumnIndexes) {
                rebuildBitmapIndexes(tablePath, pathTableLen);
            }

            if (tableMetadata.isWalEnabled() && txWriter.getLagRowCount() > 0) {
                LOG.info().$("resetting WAL lag [table=").$(tablePath)
                        .$(", walLagRowCount=").$(txWriter.getLagRowCount())
                        .I$();
                // WAL Lag values is not strictly append-only data structures, it can be overwritten
                // while the snapshot was copied. Resetting it will re-apply data from copied WAL files
                txWriter.resetLagAppliedRows();
            }

            if (PartitionBy.isPartitioned(tableMetadata.getPartitionBy())) {
                // Remove non-attached partitions
                LOG.debug().$("purging non attached partitions [path=").$(tablePath.$()).I$();
                partitionCleanPath = tablePath; // parameter for `removePartitionDirsNotAttached`
                this.partitionDirFmt = PartitionBy.getPartitionDirFormatMethod(
                        tableMetadata.getTimestampType(),
                        tableMetadata.getPartitionBy()
                );
                ff.iterateDir(tablePath.$(), removePartitionDirsNotAttached);
            }
        } finally {
            tablePath.trimTo(pathTableLen);
        }
    }

    /**
     * Recovers all table files from source (checkpoint/backup) to destination.
     * Combines metadata file copying
     *
     * @param srcPath            source path (will be modified)
     * @param dstPath            destination path (will be modified)
     * @param recoveredMetaFiles counter for recovered meta files
     * @param recoveredWalFiles  counter for recovered WAL files
     * @param symbolFilesCount   counter for recovered symbol files
     */
    public void restoreTableFiles(
            Path srcPath,
            Path dstPath,
            AtomicInteger recoveredMetaFiles,
            AtomicInteger recoveredWalFiles,
            AtomicInteger symbolFilesCount,
            boolean rebuildPartitionColumnIndexes
    ) {
        int srcPathLen = srcPath.size();
        int dstPathLen = dstPath.size();

        // Check if this is a view (views have _view file but no _cv file)
        boolean isView = ff.exists(srcPath.trimTo(srcPathLen).concat(ViewDefinition.VIEW_DEFINITION_FILE_NAME).$());
        srcPath.trimTo(srcPathLen);

        if (isView) {
            copyViewMetadataFiles(srcPath, dstPath, recoveredMetaFiles);
        } else {
            // Copy metadata files from source to the destination table location
            copyMetadataFiles(srcPath, dstPath, recoveredMetaFiles);

            // Reset _todo_ file to prevent metadata restoration on table open
            TableUtils.resetTodoLog(ff, dstPath, dstPathLen, memFile);

            // Rebuild symbol files and other table-specific processing
            rebuildTableFiles(dstPath.trimTo(dstPathLen), symbolFilesCount, rebuildPartitionColumnIndexes);
        }

        // Handle WAL-specific processing
        processWalSequencerMetadata(srcPath.trimTo(srcPathLen), dstPath.trimTo(dstPathLen), recoveredWalFiles, -1);
    }

    /**
     * Restores the table name registry by removing existing files and copying from source.
     *
     * @param srcPath    source path (will be modified)
     * @param dstPath    destination path (will be modified)
     * @param srcRootLen length to trim srcPath to
     * @param dstRootLen length to trim dstPath to
     * @param nameSink   sink for finding registry file versions
     */
    public void restoreTableRegistry(
            Path srcPath,
            Path dstPath,
            int srcRootLen,
            int dstRootLen,
            StringSink nameSink
    ) {
        // First delete all table name registry files in dst.
        for (; ; ) {
            dstPath.trimTo(dstRootLen).$();
            int version = TableNameRegistryStore.findLastTablesFileVersion(ff, dstPath, nameSink);
            dstPath.trimTo(dstRootLen).concat(WalUtils.TABLE_REGISTRY_NAME_FILE).putAscii('.').put(version);
            LOG.info().$("removing table name registry file [dst=").$(dstPath).I$();
            if (!ff.removeQuiet(dstPath.$())) {
                throw CairoException.critical(ff.errno())
                        .put("Recovery failed. Could not remove registry file [file=").put(dstPath).put(']');
            }
            if (version == 0) {
                break;
            }
        }
        // Now copy the file name registry.
        srcPath.trimTo(srcRootLen).concat(WalUtils.TABLE_REGISTRY_NAME_FILE).putAscii(".0");
        dstPath.trimTo(dstRootLen).concat(WalUtils.TABLE_REGISTRY_NAME_FILE).putAscii(".0");
        if (ff.copy(srcPath.$(), dstPath.$()) < 0) {
            throw CairoException.critical(ff.errno())
                    .put("Recovery failed. Could not copy registry file [src=").put(srcPath).put(", dst=").put(dstPath).put(']');
        }
        LOG.info().$("restored table registry [src=").$(srcPath).$(", dst=").$(dstPath).I$();
    }

    /**
     * Check if column is a valid indexed symbol column that exists in parquet
     * and has valid data in this partition.
     *
     * @return parquet column index, or -1 if column should be skipped
     */
    private static int getIndexedParquetColumnIndex(
            RecordMetadata metadata,
            PartitionDecoder.Metadata parquetMetadata,
            ColumnVersionReader columnVersionReader,
            int columnIndex,
            long partitionTimestamp,
            long partitionRowCount
    ) {
        if (metadata.getColumnType(columnIndex) != ColumnType.SYMBOL || !metadata.isColumnIndexed(columnIndex)) {
            return -1;
        }
        if (metadata.getIndexValueBlockCapacity(columnIndex) < 0) {
            return -1;
        }

        // Check columnTop validity
        final int writerIndex = metadata.getWriterIndex(columnIndex);
        final long columnTop = columnVersionReader.getColumnTop(partitionTimestamp, writerIndex);
        // -1 means column doesn't exist in partition, see ColumnVersionReader.getColumnTop()
        if (columnTop < 0 || columnTop >= partitionRowCount) {
            return -1;
        }

        for (int idx = 0, cnt = parquetMetadata.getColumnCount(); idx < cnt; idx++) {
            if (parquetMetadata.getColumnId(idx) == columnIndex) {
                return idx;
            }
        }
        return -1;
    }

    /**
     * Copies a file from source to destination, optionally ignoring if file doesn't exist.
     */
    private void copyFile(
            Path srcPath,
            Path dstPath,
            AtomicInteger counter,
            CharSequence fileName,
            boolean optional
    ) {
        srcPath.concat(fileName);
        dstPath.concat(fileName);

        if (ff.copy(srcPath.$(), dstPath.$()) < 0) {
            if (optional && Files.isErrnoFileDoesNotExist(ff.errno())) {
                // File is optional and doesn't exist - this is expected
                return;
            }

            throw CairoException.critical(ff.errno())
                    .put("Recovery failed. Could not copy ")
                    .put(fileName)
                    .put(" file [src=")
                    .put(srcPath)
                    .put(", dst=")
                    .put(dstPath)
                    .put(']');
        } else {
            counter.incrementAndGet();
            LOG.info()
                    .$("recovered ").$(fileName).$(" file [src=").$(srcPath)
                    .$(", dst=").$(dstPath)
                    .I$();
        }
    }

    private void rebuildBitmapIndexForNativePartition(int pathTableLen, int columnCount, long partitionTimestamp, long partitionRowCount, long partitionNameTxn, String tablePathStr, int partitionBy, int timestampType) {
        for (int colIdx = 0; colIdx < columnCount; colIdx++) {
            // Skip non-indexed columns and non-symbol columns (deleted columns may still have indexed flag set)
            if (!tableMetadata.isColumnIndexed(colIdx) || !ColumnType.isSymbol(tableMetadata.getColumnType(colIdx))) {
                continue;
            }

            final int writerIndex = tableMetadata.getWriterIndex(colIdx);
            final long columnNameTxn = columnVersionReader.getColumnNameTxn(partitionTimestamp, writerIndex);
            final long columnTop = columnVersionReader.getColumnTop(partitionTimestamp, writerIndex);

            // -1 means column doesn't exist in partition, see ColumnVersionReader.getColumnTop()
            if (columnTop < 0 || columnTop >= partitionRowCount) {
                continue;
            }

            final String columnName = tableMetadata.getColumnName(colIdx);
            final int indexBlockCapacity = tableMetadata.getIndexBlockCapacity(colIdx);
            final byte indexType = tableMetadata.getColumnIndexType(colIdx);

            futures.add(executor.submit(() -> rebuildBitmapIndexForNativePartitionColumn(
                    tablePathStr,
                    pathTableLen,
                    columnName,
                    columnNameTxn,
                    indexBlockCapacity,
                    indexType,
                    partitionTimestamp,
                    partitionNameTxn,
                    partitionRowCount,
                    columnTop,
                    partitionBy,
                    timestampType
            )));
        }
    }

    private void rebuildBitmapIndexForNativePartitionColumn(
            String tablePathStr,
            int pathTableLen,
            String columnName,
            long columnNameTxn,
            int indexBlockCapacity,
            byte indexType,
            long partitionTimestamp,
            long partitionNameTxn,
            long partitionRowCount,
            long columnTop,
            int partitionBy,
            int timestampType
    ) {
        if (abortParallelTasks.get()) {
            return;
        }

        // Since we're using an executor, we can't use Path thread locals.
        try (
                Path path = new Path().put(tablePathStr);
                SymbolColumnIndexer indexer = new SymbolColumnIndexer(configuration, indexType)
        ) {
            path.trimTo(pathTableLen);

            // Set path to partition directory
            TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
            int partitionPathLen = path.size();

            // Check if partition exists
            if (!ff.exists(path.$())) {
                LOG.info().$("partition does not exist, skipping bitmap index rebuild [path=").$(path).I$();
                return;
            }

            LOG.info().$("rebuilding bitmap index [path=").$(path).$(", column=").$(columnName).I$();

            // Remove existing index files if they exist
            removeIndexFiles(ff, path, partitionPathLen, columnName, columnNameTxn, indexType);

            // Create new index files
            createIndexFiles(ff, path, partitionPathLen, columnName, columnNameTxn, indexBlockCapacity, indexType);

            // Open the .d file and rebuild the index
            TableUtils.dFile(path.trimTo(partitionPathLen), columnName, columnNameTxn);
            long columnDataFd = TableUtils.openRO(ff, path.$(), LOG);
            try {
                indexer.configureWriter(path.trimTo(partitionPathLen), columnName, columnNameTxn, columnTop);
                indexer.index(ff, columnDataFd, columnTop, partitionRowCount);
            } catch (CairoException e) {
                LOG.error().$("could not rebuild bitmap index [path=").$(path.trimTo(partitionPathLen))
                        .$(", column=").$(columnName)
                        .$(", errno=").$(e.getErrno())
                        .$(", msg=").$safe(e.getFlyweightMessage())
                        .I$();
                throw e;
            } finally {
                ff.close(columnDataFd);
            }

            LOG.info().$("rebuilt bitmap index [path=").$(path.trimTo(partitionPathLen))
                    .$(", column=").$(columnName)
                    .$(", rowCount=").$(partitionRowCount - columnTop)
                    .I$();
        }
    }

    private void rebuildBitmapIndexForParquetPartition(
            String tablePathStr,
            int pathTableLen,
            long partitionTimestamp,
            long partitionRowCount,
            long partitionNameTxn,
            long parquetSize,
            int partitionBy,
            int timestampType
    ) {
        if (abortParallelTasks.get()) {
            return;
        }

        try (
                Path path = new Path().put(tablePathStr);
                PartitionDecoder partitionDecoder = new PartitionDecoder();
                RowGroupBuffers rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                DirectIntList parquetColumns = new DirectIntList(32, MemoryTag.NATIVE_DEFAULT)
        ) {
            ObjList<IndexWriter> indexWriters = new ObjList<>();
            path.trimTo(pathTableLen);

            // Set path to parquet partition and mmap
            TableUtils.setPathForParquetPartition(path, timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
            path.concat(TableUtils.PARQUET_PARTITION_NAME).$();

            if (!ff.exists(path.$())) {
                LOG.info().$("parquet partition does not exist, skipping bitmap index rebuild [path=").$(path).I$();
                return;
            }

            long parquetAddr = TableUtils.mapRO(ff, path.$(), LOG, parquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            try {
                partitionDecoder.of(parquetAddr, parquetSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);

                // Set path to native partition directory (where index files go)
                path.trimTo(pathTableLen);
                TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
                int partitionPathLen = path.size();

                rebuildParquetPartitionIndexes(
                        ff,
                        configuration,
                        path,
                        partitionPathLen,
                        partitionDecoder,
                        rowGroupBuffers,
                        parquetColumns,
                        indexWriters,
                        tableMetadata,
                        columnVersionReader,
                        partitionTimestamp,
                        partitionRowCount
                );
            } catch (CairoException e) {
                LOG.error().$("could not rebuild bitmap indexes for parquet partition [path=").$(path)
                        .$(", errno=").$(e.getErrno())
                        .$(", msg=").$safe(e.getFlyweightMessage())
                        .I$();
                throw e;
            } finally {
                ff.munmap(parquetAddr, parquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
        }
    }

    private void rebuildBitmapIndexes(Path tablePath, int pathTableLen) {
        tablePath.trimTo(pathTableLen);

        final int partitionBy = tableMetadata.getPartitionBy();
        final boolean isPartitioned = PartitionBy.isPartitioned(partitionBy);
        final int timestampType = tableMetadata.getTimestampType();
        final int columnCount = tableMetadata.getColumnCount();
        final String tablePathStr = tablePath.toString();

        // Iterate through partitions (or single default partition for non-partitioned tables)
        int partitionCount = isPartitioned ? txWriter.getPartitionCount() : 1;

        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            final long partitionTimestamp;
            final long partitionRowCount;
            final long partitionNameTxn;

            if (isPartitioned) {
                partitionTimestamp = txWriter.getPartitionTimestampByIndex(partitionIndex);
                partitionRowCount = txWriter.getPartitionRowCountByTimestamp(partitionTimestamp);
                partitionNameTxn = txWriter.getPartitionNameTxnByPartitionTimestamp(partitionTimestamp);
            } else {
                partitionTimestamp = TxReader.DEFAULT_PARTITION_TIMESTAMP;
                partitionRowCount = txWriter.getTransientRowCount();
                partitionNameTxn = -1L;
            }

            if (partitionRowCount <= 0) {
                continue;
            }

            if (isPartitioned && txWriter.isPartitionParquet(partitionIndex)) {
                final long parquetSize = txWriter.getPartitionParquetFileSize(partitionIndex);

                futures.add(executor.submit(() -> rebuildBitmapIndexForParquetPartition(
                        tablePathStr,
                        pathTableLen,
                        partitionTimestamp,
                        partitionRowCount,
                        partitionNameTxn,
                        parquetSize,
                        partitionBy,
                        timestampType
                )));
            } else {
                rebuildBitmapIndexForNativePartition(pathTableLen, columnCount, partitionTimestamp, partitionRowCount, partitionNameTxn, tablePathStr, partitionBy, timestampType);
            }
        }
    }

    /**
     * Rebuilds symbol files for all symbol columns in the table.
     *
     * @param tablePath            path to the table directory
     * @param recoveredSymbolFiles counter for recovered symbol files
     * @param pathTableLen         length to trim tablePath to
     */
    private void rebuildSymbolFiles(
            Path tablePath,
            AtomicInteger recoveredSymbolFiles,
            int pathTableLen
    ) {
        tablePath.trimTo(pathTableLen);
        final String tablePathStr = tablePath.toString();

        for (int i = 0; i < tableMetadata.getColumnCount(); i++) {
            final int columnType = tableMetadata.getColumnType(i);
            if (ColumnType.isSymbol(columnType)) {
                final int cleanSymbolCount = txWriter.getSymbolValueCount(tableMetadata.getDenseSymbolIndex(i));
                final String columnName = tableMetadata.getColumnName(i);
                final int writerIndex = tableMetadata.getWriterIndex(i);
                final int indexKeyBlockCapacity = tableMetadata.getIndexBlockCapacity(i);
                final long columnNameTxn = columnVersionReader.getSymbolTableNameTxn(writerIndex);

                futures.add(executor.submit(() -> {
                    if (abortParallelTasks.get()) {
                        return;
                    }

                    LOG.info().$("rebuilding symbol files [table=").$(tablePathStr)
                            .$(", column=").$safe(columnName)
                            .$(", count=").$(cleanSymbolCount)
                            .I$();

                    SymbolMapUtil localSymbolMapUtil = new SymbolMapUtil();
                    try (Path localPath = new Path().of(tablePathStr)) {
                        localSymbolMapUtil.rebuildSymbolFiles(
                                configuration,
                                localPath,
                                columnName,
                                columnNameTxn,
                                cleanSymbolCount,
                                -1,
                                indexKeyBlockCapacity
                        );
                    }
                    recoveredSymbolFiles.incrementAndGet();
                }));
            }
        }
    }

    private void removePartitionDirsNotAttached(long pUtf8NameZ, int type) {
        // Do not remove detached partitions, they are probably about to be attached
        // Do not remove wal and sequencer directories either
        int checkedType = ff.typeDirOrSoftLinkDirNoDots(partitionCleanPath, pathTableLen, pUtf8NameZ, type, utf8Sink);
        if (checkedType != Files.DT_UNKNOWN &&
                !CairoKeywords.isDetachedDirMarker(pUtf8NameZ) &&
                !CairoKeywords.isWal(pUtf8NameZ) &&
                !CairoKeywords.isTxnSeq(pUtf8NameZ) &&
                !CairoKeywords.isSeq(pUtf8NameZ) &&
                !Utf8s.endsWithAscii(utf8Sink, configuration.getAttachPartitionSuffix())
        ) {
            try {
                long txn;
                int txnSep = Utf8s.indexOfAscii(utf8Sink, '.');
                if (txnSep < 0) {
                    txnSep = utf8Sink.size();
                    txn = -1;
                } else {
                    txn = Numbers.parseLong(utf8Sink, txnSep + 1, utf8Sink.size());
                }
                long dirTimestamp = partitionDirFmt.parse(utf8Sink.asAsciiCharSequence(), 0, txnSep, EN_LOCALE);
                if (txWriter.getPartitionNameTxnByPartitionTimestamp(dirTimestamp) == txn) {
                    return;
                }
                if (!ff.unlinkOrRemove(partitionCleanPath, LOG)) {
                    LOG.info()
                            .$("failed to purge unused partition version [path=").$(partitionCleanPath)
                            .$(", errno=").$(ff.errno())
                            .I$();
                } else {
                    LOG.info().$("purged unused partition version [path=").$(partitionCleanPath).I$();
                }
                partitionCleanPath.trimTo(pathTableLen).$();
            } catch (NumericException ignore) {
                // not a date?
                // ignore exception and leave the directory
                partitionCleanPath.trimTo(pathTableLen);
                partitionCleanPath.concat(pUtf8NameZ).$();
                LOG.error().$("invalid partition directory inside table folder: ").$(partitionCleanPath).$();
            } finally {
                partitionCleanPath.trimTo(pathTableLen);
            }
        }
    }

    static void createIndexFiles(FilesFacade ff, Path path, int partitionPathLen, CharSequence columnName, long columnNameTxn, int indexBlockCapacity, byte indexType) {
        // Create .k file with proper header
        try (MemoryCMARW mem = Vm.getCMARWInstance()) {
            LPSZ keyFileName = IndexFactory.keyFileName(indexType, path.trimTo(partitionPathLen), columnName, columnNameTxn);
            mem.smallFile(ff, keyFileName, MemoryTag.MMAP_INDEX_WRITER);
            IndexFactory.initKeyMemory(indexType, mem, indexBlockCapacity);
        } catch (CairoException e) {
            LOG.error().$("could not create index key file [path=").$(path).$(", column=").$(columnName).$(", errno=").$(e.getErrno()).I$();
            throw e;
        }

        // Create empty .v file
        LPSZ valueFileName = IndexFactory.valueFileName(indexType, path.trimTo(partitionPathLen), columnName, columnNameTxn);
        if (!ff.touch(valueFileName)) {
            int errno = ff.errno();
            LOG.error().$("could not create index value file [path=").$(path).$(", column=").$(columnName).$(", errno=").$(errno).I$();
            throw CairoException.critical(errno).put("could not create index value file [path=").put(path).put(']');
        }
    }

    /**
     * Rebuilds bitmap indexes for all indexed symbol columns in a parquet partition.
     * Decodes all indexed columns together in a single pass through row groups for efficiency.
     * This method is designed to be reusable from O3 logic.
     */
    static void rebuildParquetPartitionIndexes(
            FilesFacade ff,
            CairoConfiguration configuration,
            Path path,
            int partitionPathLen,
            PartitionDecoder partitionDecoder,
            RowGroupBuffers rowGroupBuffers,
            DirectIntList parquetColumns,
            ObjList<IndexWriter> indexWriters,
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp,
            long partitionRowCount
    ) {
        final PartitionDecoder.Metadata parquetMetadata = partitionDecoder.metadata();
        final int columnCount = metadata.getColumnCount();
        final StringSink columnNamesSink = new StringSink();

        // First pass: identify indexed columns and collect names for logging
        parquetColumns.clear();
        indexWriters.clear();

        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            if (getIndexedParquetColumnIndex(metadata, parquetMetadata, columnVersionReader, columnIndex, partitionTimestamp, partitionRowCount) == -1) {
                continue;
            }

            // Collect column names for logging
            if (!columnNamesSink.isEmpty()) {
                columnNamesSink.put(", ");
            }
            columnNamesSink.put(metadata.getColumnName(columnIndex));
        }

        if (columnNamesSink.isEmpty()) {
            return; // No indexed columns to process
        }

        LOG.info().$("rebuilding bitmap indexes for parquet partition [path=").$(path.trimTo(partitionPathLen))
                .$(", columns=").$(columnNamesSink)
                .I$();

        // Second pass: create index files, open writers, build parquetColumns list
        int indexedColumnCount = 0;
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            int parquetColumnIndex = getIndexedParquetColumnIndex(metadata, parquetMetadata, columnVersionReader, columnIndex, partitionTimestamp, partitionRowCount);
            if (parquetColumnIndex == -1) {
                continue;
            }

            final int writerIndex = metadata.getWriterIndex(columnIndex);
            final CharSequence columnName = metadata.getColumnName(columnIndex);
            final long columnNameTxn = columnVersionReader.getColumnNameTxn(partitionTimestamp, writerIndex);
            final int indexBlockCapacity = metadata.getIndexValueBlockCapacity(columnIndex);
            final byte indexType = metadata.getColumnIndexType(columnIndex);

            // Remove existing index files
            removeIndexFiles(ff, path, partitionPathLen, columnName, columnNameTxn, indexType);

            // Create new index files
            createIndexFiles(ff, path, partitionPathLen, columnName, columnNameTxn, indexBlockCapacity, indexType);

            // Open IndexWriter
            IndexWriter indexWriter = IndexFactory.createWriter(indexType, configuration);
            try {
                indexWriter.of(path.trimTo(partitionPathLen), columnName, columnNameTxn);
            } catch (CairoException e) {
                LOG.error().$("could not open index writer [path=").$(path.trimTo(partitionPathLen))
                        .$(", column=").$(columnName)
                        .$(", errno=").$(e.getErrno())
                        .I$();
                Misc.free(indexWriter);
                throw e;
            }
            indexWriters.add(indexWriter);

            // Add to parquet columns list for decoding
            parquetColumns.add(parquetColumnIndex);
            parquetColumns.add(ColumnType.SYMBOL);

            indexedColumnCount++;
        }

        // Third pass: decode row groups and populate all indexes together
        try {
            final int rowGroupCount = parquetMetadata.getRowGroupCount();

            // We need to track columnTop per indexed column - re-iterate to get them
            long[] columnTops = new long[indexedColumnCount];
            int colIdx = 0;
            for (int columnIndex = 0; columnIndex < columnCount && colIdx < indexedColumnCount; columnIndex++) {
                if (getIndexedParquetColumnIndex(metadata, parquetMetadata, columnVersionReader, columnIndex, partitionTimestamp, partitionRowCount) == -1) {
                    continue;
                }

                final int writerIndex = metadata.getWriterIndex(columnIndex);
                columnTops[colIdx++] = columnVersionReader.getColumnTop(partitionTimestamp, writerIndex);
            }

            long rowCount = 0;
            for (int rowGroupIndex = 0; rowGroupIndex < rowGroupCount; rowGroupIndex++) {
                final int rowGroupSize = parquetMetadata.getRowGroupSize(rowGroupIndex);

                // Check if any column needs data from this row group
                boolean needsDecode = false;
                for (int i = 0; i < indexedColumnCount; i++) {
                    if (rowCount + rowGroupSize > columnTops[i]) {
                        needsDecode = true;
                        break;
                    }
                }

                if (!needsDecode) {
                    rowCount += rowGroupSize;
                    continue;
                }

                // Decode all indexed columns for this row group
                try {
                    partitionDecoder.decodeRowGroup(rowGroupBuffers, parquetColumns, rowGroupIndex, 0, rowGroupSize);
                } catch (CairoException e) {
                    LOG.error().$("could not decode parquet row group [path=").$(path.trimTo(partitionPathLen))
                            .$(", rowGroupIndex=").$(rowGroupIndex)
                            .$(", errno=").$(e.getErrno())
                            .$(", msg=").$safe(e.getFlyweightMessage())
                            .I$();
                    throw e;
                }

                // Process each indexed column
                for (int i = 0; i < indexedColumnCount; i++) {
                    final long columnTop = columnTops[i];
                    if (rowCount + rowGroupSize <= columnTop) {
                        continue; // This column doesn't have data in this row group yet
                    }

                    final IndexWriter indexWriter = indexWriters.get(i);
                    final long startOffset = Math.max(0, columnTop - rowCount);
                    long rowId = Math.max(rowCount, columnTop);

                    final long addr = rowGroupBuffers.getChunkDataPtr(i);
                    final long size = rowGroupBuffers.getChunkDataSize(i);
                    for (long p = addr + startOffset * 4, lim = addr + size; p < lim; p += 4, rowId++) {
                        indexWriter.add(TableUtils.toIndexKey(Unsafe.getUnsafe().getInt(p)), rowId);
                    }
                }

                rowCount += rowGroupSize;
            }

            // Commit all writers and set max values
            for (int i = 0; i < indexedColumnCount; i++) {
                indexWriters.get(i).setMaxValue(partitionRowCount - 1);
                indexWriters.get(i).commit();
            }

            LOG.info().$("rebuilt bitmap indexes for parquet partition [path=").$(path.trimTo(partitionPathLen))
                    .$(", indexedColumns=").$(indexedColumnCount)
                    .I$();
        } finally {
            // Close all writers
            for (int i = 0, n = indexWriters.size(); i < n; i++) {
                Misc.free(indexWriters.get(i));
            }
            indexWriters.clear();
        }
    }

    static void removeFile(FilesFacade ff, LPSZ path) {
        if (!ff.removeQuiet(path)) {
            int errno = ff.errno();
            if (ff.exists(path)) {
                LOG.error().$("could not remove file [path=").$(path).$(", errno=").$(errno).I$();
                throw CairoException.critical(errno).put("could not remove file [path=").put(path).put(']');
            }
            // File didn't exist - proceed silently
        }
    }

    static void removeIndexFiles(FilesFacade ff, Path path, int partitionPathLen, CharSequence columnName, long columnNameTxn, byte indexType) {
        // Remove .k file
        removeFile(ff, IndexFactory.keyFileName(indexType, path.trimTo(partitionPathLen), columnName, columnNameTxn));

        // Remove .v file
        removeFile(ff, IndexFactory.valueFileName(indexType, path.trimTo(partitionPathLen), columnName, columnNameTxn));
    }
}