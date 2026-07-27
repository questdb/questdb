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

package io.questdb.cairo;

import io.questdb.cairo.idx.BitmapIndexUtils;
import io.questdb.cairo.idx.IndexFactory;
import io.questdb.cairo.idx.IndexWriter;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.griffin.engine.table.parquet.ParquetMetadataWriter;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectIntList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FindVisitor;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
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
    private final int threadCount;
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
        this.threadCount = Math.max(
                configuration.getCheckpointRecoveryThreadpoolMin(),
                Math.min(configuration.getCheckpointRecoveryThreadpoolMax(), Runtime.getRuntime().availableProcessors())
        );
        this.executor = Executors.newFixedThreadPool(threadCount);
    }

    public static void removeIndexFiles(FilesFacade ff, Path path, int partitionPathLen, CharSequence columnName, long columnNameTxn, byte indexType) {
        if (IndexType.isPosting(indexType)) {
            // POSTING leaves multiple sealed .pv.{txn} generations, plus a
            // .pci and one or more .pc<N>.*.* covering sidecars per index
            // instance. removeAllSealedFiles enumerates and removes every
            // such file across all sealTxn values. Without this, snapshot
            // restore leaves stale sidecars on disk that shadow the freshly
            // re-created .pk/.pv pair.
            PostingIndexUtils.removeAllSealedFiles(ff, path, partitionPathLen, columnName, columnNameTxn);
            // Remove .pk last — the helper above relies on its presence to
            // discover the sealTxn range.
            path.trimTo(partitionPathLen);
            removeFile(ff, IndexFactory.keyFileName(indexType, path, columnName, columnNameTxn));
            return;
        }

        // BITMAP keeps a single .v at columnVersion; no sealTxn axis.
        removeFile(ff, IndexFactory.keyFileName(indexType, path.trimTo(partitionPathLen), columnName, columnNameTxn));
        removeFile(ff, IndexFactory.valueFileName(indexType, path.trimTo(partitionPathLen), columnName, columnNameTxn, columnNameTxn));
    }

    public void abortParallelTasks() {
        abortParallelTasks.set(true);
    }

    @Override
    public void close() {
        // Backstop: drain tasks so freeing native-backed objects below cannot
        // race a still-running task.
        abortAndDrainParallelTasks();
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

    /**
     * Awaits every submitted parallel task and surfaces the first failure.
     * Returns or throws only after all tasks complete: tasks read the shared
     * native-backed {@code tableMetadata}, {@code columnVersionReader} and
     * {@code txWriter}, which callers reload for the next table, so abandoning
     * a running task would expose those reloads to concurrent readers. Resets
     * the abort flag before returning so the next table's tasks run normally.
     */
    public void finalizeParallelTasks() {
        if (futures.size() > 0) {
            LOG.info().$("awaiting ").$(futures.size()).$(" parallel tasks to complete").I$();
        }

        boolean failed = false;
        String firstErrorMessage = null;
        boolean interrupted = false;
        for (int i = 0, n = futures.size(); i < n; i++) {
            try {
                futures.getQuick(i).get();
            } catch (InterruptedException e) {
                // Keep draining: abandoning a running task risks a use-after-free
                // on the shared readers. get() cleared the interrupt status, so
                // retry (the abort flag bounds the wait) and restore it after.
                abortParallelTasks.set(true);
                interrupted = true;
                //noinspection AssignmentToForLoopParameter
                i--;
            } catch (Throwable e) {
                abortParallelTasks.set(true);
                Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
                if (cause == null) {
                    cause = e;
                }
                if (!failed) {
                    failed = true;
                    // submitParallelTask already logged the failure and replaced
                    // thread-local-reused exceptions with immutable carriers, so
                    // reading the message here cannot race the worker.
                    firstErrorMessage = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getName();
                }
            }
        }

        // All tasks done; reset the abort flag so the next table's tasks run
        // (enterprise restore continues after quarantining a failed table).
        abortParallelTasks.set(false);

        if (interrupted) {
            Thread.currentThread().interrupt();
            if (!failed) {
                LOG.error().$("parallel task await interrupted").I$();
                throw CairoException.critical(0).put("parallel task interrupted");
            }
        }
        if (failed) {
            // Deliberately no initCause(): Throwable.initCause() on the
            // thread-local-reused instance returned by critical() succeeds
            // only once per thread (clear() cannot reset the cause field), so
            // the next failed table would hit IllegalStateException. The
            // workers have already logged every failure with its cause.
            throw CairoException.critical(0)
                    .put("error in parallel task")
                    .put(": ")
                    .put(firstErrorMessage);
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

            // Validate restored parquet partitions and ensure each has a _pm
            // sidecar that resolves a footer at the committed parquet size:
            // generate one for pre-_pm backups, regenerate stale/torn/partial
            // captures, and (when requested) rebuild the parquet bitmap indexes.
            // All of this runs inside the parallel workers, mapping _pm once.
            // Passing the rebuild flag fuses validation with the index rebuild so
            // the sidecar is not mapped and CRC-verified twice on enterprise restore.
            //
            // Tradeoff: this no longer runs serially ahead of the symbol/bitmap
            // phases, so a truncated capture surfaces (with the same path-bearing
            // diagnostic) at the finalizeParallelTasks drain below rather than
            // before sibling work is submitted. The first failing worker trips the
            // shared abort latch (see submitParallelTask), so siblings bail at their
            // next item boundary; the restore still aborts and never feeds a
            // truncated file to ParquetMetadataWriter.generate. The cost is the
            // in-flight items already running -- wasted I/O on a doomed restore.
            prepareParquetPartitions(tablePath.trimTo(pathTableLen), pathTableLen, rebuildPartitionColumnIndexes);

            // Symbols are not append-only data structures, they can be corrupt
            // when symbol files are copied while written to. We need to rebuild them.
            rebuildSymbolFiles(tablePath, recoveredSymbolFiles, pathTableLen);

            // Recreate the bitmap indexes for each indexed native partition;
            // parquet partitions were already handled by prepareParquetPartitions.
            if (rebuildPartitionColumnIndexes) {
                rebuildBitmapIndexes(tablePath, pathTableLen);
            }

            // Drain all parallel tasks before going further: tableMetadata,
            // columnVersionReader and txWriter are reused across tables, so a
            // rebuild task from this table must not still be running when the
            // caller loads the next table into the same objects.
            finalizeParallelTasks();

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
        } catch (Throwable th) {
            // Any step above can throw with tasks still in flight. Reach quiescence
            // before propagating: the caller may quarantine-rename the table
            // directory the tasks write into and reload the shared readers.
            abortAndDrainParallelTasks();
            throw th;
        } finally {
            futures.clear();
            tablePath.trimTo(pathTableLen);
        }
    }

    /**
     * Releases the handles the restore holds on the current table's files (the
     * shared metadata, txn and column-version readers and the small memory
     * file). Callers that quarantine a failed table by renaming its directory
     * must call this first: Windows refuses to rename a directory while any
     * file inside it is open or mapped. The released objects are reopened
     * lazily, so the restore can continue with other tables.
     */
    public void releaseTableHandles() {
        // Backstop drain: freeing native-backed readers under a running task
        // would be a use-after-free, same as in close().
        abortAndDrainParallelTasks();
        futures.clear();
        tableMetadata = Misc.free(tableMetadata);
        txWriter = Misc.free(txWriter);
        columnVersionReader = Misc.free(columnVersionReader);
        // Keep the object: resetTodoLog/openSmallFile re-target it on a closed
        // instance; do not truncate.
        memFile.close(false);
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
            ParquetMetaFileReader parquetMetadata,
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
            if (parquetMetadata.getColumnId(idx) == writerIndex) {
                return idx;
            }
        }
        return -1;
    }

    /**
     * Awaits every submitted parallel task without surfacing task failures; used
     * on error paths that only need quiescence. Sets the abort flag for the drain
     * so not-yet-started tasks return immediately, resets it afterwards, and
     * restores the interrupt status if the draining thread is interrupted.
     */
    private void abortAndDrainParallelTasks() {
        abortParallelTasks.set(true);
        boolean interrupted = false;
        for (int i = 0, n = futures.size(); i < n; i++) {
            try {
                futures.getQuick(i).get();
            } catch (InterruptedException e) {
                // get() cleared the interrupt status; retry and restore it after.
                interrupted = true;
                //noinspection AssignmentToForLoopParameter
                i--;
            } catch (Throwable ignore) {
                // the task is done, which is all this path needs
            }
        }
        abortParallelTasks.set(false);
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
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

    /**
     * Validates restored parquet partitions and ensures each {@code _pm} sidecar
     * resolves a footer at the committed parquet size, regenerating it from
     * {@code data.parquet} when missing/stale/torn and -- when
     * {@code rebuildIndexes} is set -- rebuilding the partition's bitmap indexes
     * (see {@link #processParquetPartition}). Submits one worker per pool thread;
     * each {@link #processParquetPartitions} worker pulls partition indices from a
     * shared cursor (load balancing across skewed partitions) and reuses one
     * scratch set across every partition it handles. All per-partition syscalls
     * (including the {@code data.parquet} truncation check) run inside the workers,
     * so a ~100k-partition table no longer pays a serial stat per partition on the
     * calling thread. A truncated capture fails with a path-bearing diagnostic via
     * {@link #finalizeParallelTasks}.
     */
    private void prepareParquetPartitions(Path tablePath, int pathTableLen, boolean rebuildIndexes) {
        final int partitionBy = tableMetadata.getPartitionBy();
        if (!PartitionBy.isPartitioned(partitionBy)) {
            return;
        }
        final int timestampType = tableMetadata.getTimestampType();
        final int partitionCount = txWriter.getPartitionCount();
        if (partitionCount == 0) {
            return;
        }
        // Snapshot the table root: the workers build their own Path from this
        // string and never touch the shared tablePath owned by this thread.
        final String tablePathStr = tablePath.toString();

        // Shared work cursor: each worker pulls the next partition index until the
        // table is exhausted. txWriter's per-partition getters are pure reads over
        // the in-memory attached-partitions array, safe to call concurrently here.
        final AtomicInteger cursor = new AtomicInteger(0);
        final int workerCount = Math.min(threadCount, partitionCount);
        for (int w = 0; w < workerCount; w++) {
            futures.add(submitParallelTask(() -> processParquetPartitions(
                    tablePathStr,
                    pathTableLen,
                    partitionBy,
                    timestampType,
                    rebuildIndexes,
                    cursor,
                    partitionCount
            )));
        }
    }

    /**
     * Worker body for {@link #prepareParquetPartitions}: pulls parquet partition
     * indices from the shared {@code cursor} and processes each, reusing one set
     * of native-backed scratch objects instead of allocating per partition. The
     * rebuild-only buffers stay {@code null} on the validation-only (checkpoint
     * recovery) path. All scratch is freed in the {@code finally}.
     */
    private void processParquetPartitions(
            String tablePathStr,
            int pathTableLen,
            int partitionBy,
            int timestampType,
            boolean rebuildIndexes,
            AtomicInteger cursor,
            int partitionCount
    ) {
        final Path path = new Path();
        final ParquetMetaFileReader metaReader = new ParquetMetaFileReader();
        RowGroupBuffers rowGroupBuffers = null;
        DirectIntList parquetColumns = null;
        ParquetPartitionDecoder decoder = null;
        ObjList<IndexWriter> indexWriters = null;
        // Heap scratch reused across partitions, mirroring the native scratch
        // above (the per-partition StringSink/long[] rebuildParquetPartitionIndexes
        // used to allocate fold in here).
        StringSink columnNamesSink = null;
        LongList columnTops = null;
        try {
            if (rebuildIndexes) {
                rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                parquetColumns = new DirectIntList(32, MemoryTag.NATIVE_DEFAULT);
                decoder = configuration.newParquetPartitionDecoder();
                indexWriters = new ObjList<>();
                columnNamesSink = new StringSink();
                columnTops = new LongList();
            }
            int i;
            while (!abortParallelTasks.get() && (i = cursor.getAndIncrement()) < partitionCount) {
                if (!txWriter.isPartitionParquet(i)) {
                    continue;
                }
                if (txWriter.isPartitionRemotelyServed(i)) {
                    continue;
                }
                final long partitionTimestamp = txWriter.getPartitionTimestampByIndex(i);
                final long partitionNameTxn = txWriter.getPartitionNameTxn(i);
                // Read row count by index (O(1)) since i is in hand, not by
                // timestamp (O(log P)).
                final long partitionRowCount = txWriter.getPartitionSize(i);
                // Committed parquet size from _txn, not on-disk: a snapshot may
                // capture data.parquet mid-append, and bytes past the committed
                // size are not MVCC-visible.
                final long parquetFileSize = txWriter.getPartitionParquetFileSize(i);
                // An empty parquet partition still needs a valid _pm but no index
                // rebuild (rebuildBitmapIndexes skips rowCount<=0 too).
                final boolean doRebuild = rebuildIndexes && partitionRowCount > 0;
                try {
                    processParquetPartition(
                            path,
                            metaReader,
                            rowGroupBuffers,
                            parquetColumns,
                            decoder,
                            indexWriters,
                            columnNamesSink,
                            columnTops,
                            tablePathStr,
                            pathTableLen,
                            partitionTimestamp,
                            partitionRowCount,
                            partitionNameTxn,
                            parquetFileSize,
                            partitionBy,
                            timestampType,
                            doRebuild
                    );
                } finally {
                    // POSTING seal() retains Path thread-locals; clear per partition.
                    Path.clearThreadLocals();
                }
            }
        } finally {
            Misc.free(decoder);
            Misc.free(parquetColumns);
            Misc.free(rowGroupBuffers);
            metaReader.clear();
            path.close();
            Path.clearThreadLocals();
        }
    }

    /**
     * Regenerates the {@code _pm} sidecar from {@code data.parquet} at the
     * committed {@code parquetFileSize} (never {@code ff.length()}: bytes past it
     * are uncommitted MVCC state). Creates, writes and fsyncs the file, fsyncs the
     * partition directory on non-Windows so it survives a post-restore power loss,
     * and removes a partial {@code _pm} on failure. {@code path} must sit inside
     * the partition directory and is left trimmed to {@code partitionDirLen}. Takes
     * a per-task {@code path} as it runs on parallel executor threads.
     */
    private void regenerateParquetMetaFile(Path path, int partitionDirLen, long parquetFileSize) {
        path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_PARTITION_NAME).$();
        long parquetFd = ff.openRO(path.$());
        if (parquetFd < 0) {
            throw CairoException.critical(ff.errno()).put("cannot open parquet file for _pm generation [path=").put(path).put(']');
        }

        path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
        long parquetMetaFd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
        if (parquetMetaFd < 0) {
            int errno = ff.errno();
            ff.close(parquetFd);
            throw CairoException.critical(errno).put("cannot create _pm file [path=").put(path).put(']');
        }

        try {
            long parquetMetaAllocator = Unsafe.getNativeAllocator(MemoryTag.NATIVE_DEFAULT);
            long parquetMetaFileSize = ParquetMetadataWriter.generate(parquetMetaAllocator, Files.toOsFd(parquetFd), parquetFileSize, Files.toOsFd(parquetMetaFd));
            // Persist the brand-new _pm before snapshot restore returns. Otherwise
            // a power loss after restore but before the engine syncs would leave
            // the partition referenced by _txn but with no usable _pm sidecar.
            ff.fsync(parquetMetaFd);
            LOG.info().$("generated _pm for restored parquet partition [path=").$(path).$(", parquetMetaSize=").$(parquetMetaFileSize).I$();
        } catch (Throwable t) {
            // Remove partially written _pm file so a retry regenerates it.
            path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
            ff.remove(path.$());
            throw t;
        } finally {
            ff.close(parquetFd);
            ff.close(parquetMetaFd);
            if (!Os.isWindows()) {
                path.trimTo(partitionDirLen).$();
                final long dirFd = TableUtils.openRONoCache(ff, path.$(), LOG);
                if (dirFd != -1) {
                    ff.fsyncAndClose(dirFd);
                }
            }
        }
        path.trimTo(partitionDirLen);
    }

    /**
     * Ensures the {@code _pm} sidecar resolves a footer at the committed
     * {@code parquetFileSize}, regenerating it from {@code data.parquet} when
     * missing/stale/torn/zero-length. Resolving the footer runs the full-file CRC
     * exactly once on the returned live mapping: on success {@code taskReader} is
     * bound and resolved over it, and the caller owns the mapping and must
     * {@code munmap} it (capture {@code taskReader.getFileSize()} before
     * {@link ParquetMetaFileReader#clear()} resets it). The {@code data.parquet}
     * truncation check must already have passed. An existing sidecar is trusted
     * only when {@code onDiskSize == parquetFileSize}; a longer {@code data.parquet}
     * (an in-place O3 rewrite captured mid-flight) forces regeneration even when
     * the stale footer still resolves at the committed size. Takes per-task
     * {@code path}/{@code taskReader} as it runs on parallel executor threads.
     */
    private long mapResolvableParquetMeta(Path path, int partitionDirLen, long parquetFileSize, long onDiskSize, ParquetMetaFileReader taskReader) {
        path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
        final boolean exists = ff.exists(path.$());
        // Trust an existing _pm only when data.parquet is EXACTLY the committed
        // size. When the file is longer (onDiskSize > parquetFileSize), the
        // snapshot captured the partition mid in-place O3 rewrite: a later
        // generation appended row groups and a new footer past the committed point
        // and rewrote _pm to describe that later generation, while _txn still
        // records the earlier committed size. resolveFooter() can still resolve a
        // footer at the committed size, so the stale sidecar would be silently kept
        // and then mis-read at the committed size -- a column chunk of the later
        // generation lies past parquetFileSize, surfacing as "File out of
        // specification" on the first merge/read and suspending a replica that
        // replays over the restored partition. Regenerate from data.parquet at the
        // committed size, which the size check above has already validated.
        if (exists && onDiskSize == parquetFileSize) {
            long addr = 0;
            long size = 0;
            boolean resolved = false;
            try {
                addr = ParquetMetaFileReader.openAndMapRO(ff, path.$(), taskReader);
                if (addr != 0) {
                    size = taskReader.getFileSize();
                    resolved = taskReader.resolveFooter(parquetFileSize);
                }
            } catch (CairoException e) {
                // A torn copy whose header over-claims the length throws instead
                // of returning a resolve failure; treat both alike.
                LOG.info().$("restored _pm failed validation [path=").$(path)
                        .$(", msg=").$safe(e.getFlyweightMessage())
                        .I$();
                resolved = false;
            }
            if (resolved) {
                return addr;
            }
            // Drop the stale/torn mapping before regenerating in place.
            taskReader.clear();
            if (addr != 0) {
                ff.munmap(addr, size, MemoryTag.MMAP_PARQUET_METADATA_READER);
            }
        }
        if (exists) {
            // The sidecar does not resolve at the committed size, or data.parquet
            // is longer than committed (a stale _pm paired with an in-place
            // regenerated data.parquet, a torn copy, or a partial file from a
            // crashed restore). Trusting it would defer the failure to the first
            // read, so remove and regenerate it.
            path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
            if (!ff.removeQuiet(path.$())) {
                throw CairoException.critical(ff.errno()).put("cannot remove unresolvable _pm file [path=").put(path).put(']');
            }
            LOG.info().$("removed stale/unresolvable _pm of restored parquet partition for regeneration [path=").$(path)
                    .$(", committed=").$(parquetFileSize).$(", onDisk=").$(onDiskSize).I$();
        }

        regenerateParquetMetaFile(path, partitionDirLen, parquetFileSize);

        path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
        long addr = ParquetMetaFileReader.openAndMapRO(ff, path.$(), taskReader);
        try {
            if (addr == 0 || !taskReader.resolveFooter(parquetFileSize)) {
                throw CairoException.critical(0).put("regenerated _pm does not resolve at committed parquet size [path=").put(path).put(']');
            }
        } catch (Throwable t) {
            long size = taskReader.getFileSize();
            taskReader.clear();
            if (addr != 0) {
                ff.munmap(addr, size, MemoryTag.MMAP_PARQUET_METADATA_READER);
            }
            throw t;
        }
        return addr;
    }

    /**
     * Worker body for {@link #rebuildBitmapIndexes}: pulls packed
     * {@code (partitionIndex, colIdx)} items from the shared {@code cursor} and
     * rebuilds the bitmap/posting index of that one native column, reusing one
     * {@code Path} across every item instead of allocating per (partition, column).
     * Distributing individual items rather than a whole partition per worker
     * preserves column-level parallelism for non-partitioned and low-partition
     * tables. The {@link SymbolColumnIndexer} is still created fresh per column in
     * {@link #rebuildBitmapIndexForNativePartitionColumn}.
     */
    private void rebuildBitmapIndexesForNativePartitions(
            String tablePathStr,
            int pathTableLen,
            int partitionBy,
            int timestampType,
            boolean isPartitioned,
            AtomicInteger cursor,
            LongList nativeIndexWork,
            LongList nativeIndexColumnTops
    ) {
        final Path path = new Path();
        try {
            final int workCount = nativeIndexWork.size();
            int i;
            while (!abortParallelTasks.get() && (i = cursor.getAndIncrement()) < workCount) {
                final long item = nativeIndexWork.getQuick(i);
                final int partitionIndex = Numbers.decodeLowInt(item);
                final int colIdx = Numbers.decodeHighInt(item);

                // Resolve partition metadata per item: the shared cursor disperses
                // a worker's items across partitions, so a per-partition cache
                // mostly misses, and the by-index getters are O(1) array reads.
                final long partitionTimestamp;
                final long partitionRowCount;
                final long partitionNameTxn;
                if (isPartitioned) {
                    partitionTimestamp = txWriter.getPartitionTimestampByIndex(partitionIndex);
                    partitionRowCount = txWriter.getPartitionSize(partitionIndex);
                    partitionNameTxn = txWriter.getPartitionNameTxn(partitionIndex);
                } else {
                    partitionTimestamp = TxReader.DEFAULT_PARTITION_TIMESTAMP;
                    partitionRowCount = txWriter.getTransientRowCount();
                    partitionNameTxn = -1L;
                }

                final int writerIndex = tableMetadata.getWriterIndex(colIdx);
                final long columnNameTxn = columnVersionReader.getColumnNameTxn(partitionTimestamp, writerIndex);
                // columnTop was computed when the work list was built; read it
                // back from the index-aligned list.
                final long columnTop = nativeIndexColumnTops.getQuick(i);
                final String columnName = tableMetadata.getColumnName(colIdx);
                final int indexBlockCapacity = tableMetadata.getIndexBlockCapacity(colIdx);
                final byte indexType = tableMetadata.getColumnIndexType(colIdx);
                try {
                    rebuildBitmapIndexForNativePartitionColumn(
                            path,
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
                    );
                } finally {
                    // POSTING seal() retains Path thread-locals; clear per column.
                    Path.clearThreadLocals();
                }
            }
        } finally {
            path.close();
            Path.clearThreadLocals();
        }
    }

    private void rebuildBitmapIndexForNativePartitionColumn(
            Path path,
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
        // Reset the reused path to the table root.
        path.of(tablePathStr).trimTo(pathTableLen);

        // Set path to partition directory
        TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
        int partitionPathLen = path.size();

        // Check if partition exists
        if (!ff.exists(path.$())) {
            LOG.info().$("partition does not exist, skipping bitmap index rebuild [path=").$(path).I$();
            return;
        }

        LOG.info().$("rebuilding bitmap index [path=").$(path).$(", column=").$(columnName).I$();

        // Fresh per-column indexer: index type is fixed at construction.
        try (SymbolColumnIndexer indexer = new SymbolColumnIndexer(configuration, indexType)) {
            // Remove existing index files if they exist
            removeIndexFiles(ff, path, partitionPathLen, columnName, columnNameTxn, indexType);

            // Create new index files
            createIndexFiles(ff, path, partitionPathLen, columnName, columnNameTxn, indexBlockCapacity, indexType);

            // Open the .d file and rebuild the index
            TableUtils.dFile(path.trimTo(partitionPathLen), columnName, columnNameTxn);
            long columnDataFd = TableUtils.openRO(ff, path.$(), LOG);
            try {
                indexer.configureWriter(path.trimTo(partitionPathLen), columnName, columnNameTxn, columnTop, partitionTimestamp, partitionNameTxn);
                if (IndexType.isPosting(indexType)) {
                    // POSTING indexes need INCLUDE columns wired before index() so
                    // seal() can build covering sidecars. BITMAP has no covering
                    // and configureCoveringForPosting is a no-op for it.
                    configureCoveringForPosting(indexer.getWriter(), columnName, tableMetadata, columnVersionReader, partitionTimestamp);
                    // Tag the seal's chain entry with the committed _txn so a
                    // later recovery walk does not mis-classify the rebuilt
                    // index as abandoned.
                    indexer.getWriter().setNextTxnAtSeal(txWriter.getTxn());
                }
                indexer.index(ff, columnDataFd, columnTop, partitionRowCount);
                if (IndexType.isPosting(indexType)) {
                    // BITMAP is sealed-by-default; POSTING needs an explicit
                    // seal so the .pv.<sealTxn> sealed value file and the
                    // .pci/.pc<N> covering sidecars exist after restore.
                    indexer.seal();
                }
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

    /**
     * Processes one parquet partition, reusing the caller-owned scratch objects so a
     * worker amortizes their native allocation across partitions (see
     * {@link #processParquetPartitions}). Validates {@code data.parquet} against its
     * committed size (a stat, no fd), maps {@code _pm} once via {@code metaReader},
     * validates/regenerates it, then -- when {@code rebuildIndexes} is set -- reuses
     * that resolved mapping to rebuild the partition's bitmap indexes. Feeding the
     * resolved reader to the decoder via
     * {@link ParquetPartitionDecoder#of(ParquetMetaFileReader, long, long, int)}
     * shallow-copies its state, so the {@code _pm} footer is resolved and
     * CRC-verified once instead of three times. The caller owns {@code path}; the
     * rebuild-only buffers are non-null only when {@code rebuildIndexes} is set.
     */
    private void processParquetPartition(
            Path path,
            ParquetMetaFileReader metaReader,
            RowGroupBuffers rowGroupBuffers,
            DirectIntList parquetColumns,
            ParquetPartitionDecoder decoder,
            ObjList<IndexWriter> indexWriters,
            StringSink columnNamesSink,
            LongList columnTops,
            String tablePathStr,
            int pathTableLen,
            long partitionTimestamp,
            long partitionRowCount,
            long partitionNameTxn,
            long parquetFileSize,
            int partitionBy,
            int timestampType,
            boolean rebuildIndexes
    ) {
        // Reset the reused path to the table root.
        path.of(tablePathStr).trimTo(pathTableLen);

        // Set path to partition dir
        TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
        int partitionDirLen = path.size();

        // Validate data.parquet by size before touching _pm: regeneration reads
        // it at the committed size, so an undersized file must fail first. Runs
        // even when a valid _pm was restored -- an undersized data.parquet means
        // _txn was paired with a stale/truncated file. The size is read by path
        // (a stat, no fd) so it parallelizes across workers.
        path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_PARTITION_NAME).$();
        long onDiskSize = ff.length(path.$());
        if (onDiskSize < 0) {
            // Keep the full path in the message: one restore covers many parquet
            // partitions and the operator needs to know which one failed.
            throw CairoException.critical(ff.errno()).put("cannot read size of restored parquet file [path=").put(path).put(']');
        }
        if (onDiskSize < parquetFileSize) {
            throw CairoException.critical(0)
                    .put("restored parquet file is shorter than committed size [path=").put(path)
                    .put(", committed=").put(parquetFileSize)
                    .put(", onDisk=").put(onDiskSize)
                    .put(']');
        }

        long parquetMetaAddr = 0;
        long parquetMetaFileSize = 0;
        try {
            // Validate + (if needed) regenerate the sidecar, leaving it mapped and
            // its footer resolved. The only _pm map+CRC on this path.
            parquetMetaAddr = mapResolvableParquetMeta(path, partitionDirLen, parquetFileSize, onDiskSize, metaReader);
            parquetMetaFileSize = metaReader.getFileSize();

            if (!rebuildIndexes) {
                // Checkpoint-recovery default: validation/regeneration only.
                return;
            }

            final long parquetSize = metaReader.getParquetFileSize();

            // mmap data.parquet: existence and committed size were validated at
            // the top of this method.
            path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_PARTITION_NAME).$();
            if (!ff.exists(path.$())) {
                LOG.info().$("parquet partition does not exist, skipping bitmap index rebuild [path=").$(path).I$();
                return;
            }

            long parquetAddr = TableUtils.mapRO(ff, path.$(), LOG, parquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            try {
                // Reuse the resolved+verified reader: of(reader) shallow-copies
                // its footer state, skipping a redundant resolveFooter + CRC.
                decoder.of(metaReader, parquetAddr, parquetSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);

                // Set path to native partition directory (where index files go)
                path.trimTo(pathTableLen);
                TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
                int partitionPathLen = path.size();

                rebuildParquetPartitionIndexes(
                        ff,
                        configuration,
                        path,
                        partitionPathLen,
                        decoder,
                        rowGroupBuffers,
                        parquetColumns,
                        indexWriters,
                        columnNamesSink,
                        columnTops,
                        tableMetadata,
                        columnVersionReader,
                        partitionTimestamp,
                        partitionNameTxn,
                        partitionRowCount,
                        txWriter.getTxn()
                );
            } catch (CairoException e) {
                LOG.error().$("could not rebuild bitmap indexes for parquet partition [path=").$(path)
                        .$(", errno=").$(e.getErrno())
                        .$(", msg=").$safe(e.getFlyweightMessage())
                        .I$();
                throw e;
            } finally {
                // Destroy the decoder's context BEFORE munmap (its
                // clear-then-munmap contract); the next of() re-initializes it.
                decoder.close();
                ff.munmap(parquetAddr, parquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
        } finally {
            metaReader.clear();
            if (parquetMetaAddr != 0) {
                ff.munmap(parquetMetaAddr, parquetMetaFileSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
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

        // One default partition for non-partitioned tables, else every attached one.
        final int partitionCount = isPartitioned ? txWriter.getPartitionCount() : 1;
        if (partitionCount == 0) {
            return;
        }

        // Flatten the (partition, indexed symbol column) space into one work list,
        // skipping empty/parquet partitions and columns absent from a partition.
        // Each item packs (partitionIndex, colIdx); workers pull items from a
        // shared cursor and reuse one Path each. Distributing items rather than
        // whole partitions preserves column-level parallelism for low-partition
        // tables.
        final LongList nativeIndexWork = new LongList();
        // Index-aligned with nativeIndexWork: entry j holds the columnTop for work
        // item j, so the worker reads it back instead of re-running getColumnTop.
        final LongList nativeIndexColumnTops = new LongList();
        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            final long partitionTimestamp;
            final long partitionRowCount;
            if (isPartitioned) {
                partitionTimestamp = txWriter.getPartitionTimestampByIndex(partitionIndex);
                // Read by index (O(1)), not by timestamp (O(log P)).
                partitionRowCount = txWriter.getPartitionSize(partitionIndex);
            } else {
                partitionTimestamp = TxReader.DEFAULT_PARTITION_TIMESTAMP;
                partitionRowCount = txWriter.getTransientRowCount();
            }
            if (partitionRowCount <= 0) {
                continue;
            }
            if (isPartitioned && txWriter.isPartitionParquet(partitionIndex)) {
                // Handled by prepareParquetPartitions.
                continue;
            }
            for (int colIdx = 0; colIdx < columnCount; colIdx++) {
                // Skip non-indexed and non-symbol columns (deleted columns may
                // still carry the indexed flag).
                if (!tableMetadata.isColumnIndexed(colIdx) || !ColumnType.isSymbol(tableMetadata.getColumnType(colIdx))) {
                    continue;
                }
                final int writerIndex = tableMetadata.getWriterIndex(colIdx);
                final long columnTop = columnVersionReader.getColumnTop(partitionTimestamp, writerIndex);
                // -1 means the column is absent in this partition,
                // see ColumnVersionReader.getColumnTop().
                if (columnTop < 0 || columnTop >= partitionRowCount) {
                    continue;
                }
                nativeIndexWork.add(Numbers.encodeLowHighInts(partitionIndex, colIdx));
                nativeIndexColumnTops.add(columnTop);
            }
        }

        final int workCount = nativeIndexWork.size();
        if (workCount == 0) {
            return;
        }

        final AtomicInteger cursor = new AtomicInteger(0);
        final int workerCount = Math.min(threadCount, workCount);
        for (int w = 0; w < workerCount; w++) {
            futures.add(submitParallelTask(() -> rebuildBitmapIndexesForNativePartitions(
                    tablePathStr,
                    pathTableLen,
                    partitionBy,
                    timestampType,
                    isPartitioned,
                    cursor,
                    nativeIndexWork,
                    nativeIndexColumnTops
            )));
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

        final int columnCount = tableMetadata.getColumnCount();
        if (columnCount == 0) {
            return;
        }
        // Cursor over columns: each worker pulls column indices and rebuilds the
        // symbol files of the symbol columns, reusing one Path per worker instead
        // of allocating a Path (and queuing a future/lambda) per symbol column.
        final AtomicInteger cursor = new AtomicInteger(0);
        final int workerCount = Math.min(threadCount, columnCount);
        for (int w = 0; w < workerCount; w++) {
            futures.add(submitParallelTask(() -> rebuildSymbolFilesForColumns(
                    tablePathStr,
                    recoveredSymbolFiles,
                    cursor,
                    columnCount
            )));
        }
    }

    /**
     * Worker body for {@link #rebuildSymbolFiles}: pulls column indices from the
     * shared {@code cursor} and rebuilds the symbol files of each symbol column,
     * reusing one {@code Path} across all of them. {@link SymbolMapUtil} frees its
     * native memory internally per call, so a fresh instance per column is fine.
     */
    private void rebuildSymbolFilesForColumns(
            String tablePathStr,
            AtomicInteger recoveredSymbolFiles,
            AtomicInteger cursor,
            int columnCount
    ) {
        final Path path = new Path();
        try {
            int i;
            while (!abortParallelTasks.get() && (i = cursor.getAndIncrement()) < columnCount) {
                if (!ColumnType.isSymbol(tableMetadata.getColumnType(i))) {
                    continue;
                }
                final int cleanSymbolCount = txWriter.getSymbolValueCount(tableMetadata.getDenseSymbolIndex(i));
                final String columnName = tableMetadata.getColumnName(i);
                final int writerIndex = tableMetadata.getWriterIndex(i);
                final int indexKeyBlockCapacity = tableMetadata.getIndexBlockCapacity(i);
                final long columnNameTxn = columnVersionReader.getSymbolTableNameTxn(writerIndex);

                LOG.info().$("rebuilding symbol files [table=").$(tablePathStr)
                        .$(", column=").$safe(columnName)
                        .$(", count=").$(cleanSymbolCount)
                        .I$();

                final SymbolMapUtil symbolMapUtil = new SymbolMapUtil();
                symbolMapUtil.rebuildSymbolFiles(
                        configuration,
                        path.of(tablePathStr),
                        columnName,
                        columnNameTxn,
                        cleanSymbolCount,
                        -1,
                        indexKeyBlockCapacity
                );
                recoveredSymbolFiles.incrementAndGet();
            }
        } finally {
            path.close();
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

    /**
     * Submits a rebuild task; every parallel task must go through this method.
     * Worker failures arrive as {@link CairoException} and friends, which
     * reuse a per-thread instance with a mutable message, so only the owning
     * thread may read it: the worker overwrites the instance on its next
     * failure while the draining thread still holds the previous one. The
     * wrapper therefore logs the failure on the throwing thread and hands the
     * future an immutable {@link ParallelTaskException} carrying the
     * materialized message instead of the thread-local original.
     * <p>
     * On any failure the wrapper also trips {@code abortParallelTasks}, the shared
     * latch every worker loop polls between items, so siblings bail at their next
     * item boundary instead of running to completion on a doomed restore. This
     * cannot reorder the reported error: a worker that bails returns with nothing
     * to throw, so the earliest-submitted thrower is still the one reported
     * ({@link #finalizeParallelTasks} surfaces only the first failure).
     */
    private Future<?> submitParallelTask(Runnable task) {
        return executor.submit(() -> {
            try {
                task.run();
            } catch (Throwable e) {
                // Trip the shared latch so sibling workers short-circuit at their
                // next item check. finalizeParallelTasks resets it after the drain.
                abortParallelTasks.set(true);
                LOG.critical().$("error in parallel task").$(e).I$();
                if (e instanceof FlyweightMessageContainer) {
                    String message = e.getMessage();
                    throw new ParallelTaskException(message != null ? message : e.getClass().getName());
                }
                throw e;
            }
        });
    }

    /**
     * Mirrors TableWriter.configureCoveringIfNeeded for POSTING indexes during
     * snapshot restore. Pulls covering column names, txns, tops, and types
     * from metadata + columnVersionReader so the writer can open the
     * covered .d files and produce .pci / .pc&lt;N&gt; sidecars on seal. Shared
     * by the native and parquet rebuild paths.
     */
    static void configureCoveringForPosting(
            IndexWriter indexWriter,
            String columnName,
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp
    ) {
        int idxDenseIdx = metadata.getColumnIndexQuiet(columnName);
        if (idxDenseIdx < 0) {
            return;
        }
        IntList coveringCols = metadata.getColumnMetadata(idxDenseIdx).getCoveringColumnIndices();
        if (coveringCols == null || coveringCols.size() == 0) {
            return;
        }
        ObjList<CharSequence> names = new ObjList<>();
        LongList nameTxns = new LongList();
        LongList tops = new LongList();
        IntList shifts = new IntList();
        IntList indices = new IntList();
        IntList types = new IntList();
        int coverCount = coveringCols.size();
        int columnCount = metadata.getColumnCount();
        for (int i = 0; i < coverCount; i++) {
            int covWriterIdx = coveringCols.getQuick(i);
            if (covWriterIdx < 0) {
                names.add(null);
                nameTxns.add(TableUtils.COLUMN_NAME_TXN_NONE);
                tops.add(0);
                shifts.add(0);
                indices.add(-1);
                types.add(-1);
                continue;
            }
            // coveringCols stores writer indices, but getColumnType/getColumnName
            // are dense-keyed, and DROP COLUMN diverges the two. Resolve writer ->
            // dense first. Mirrors IndexBuilder.configureCovering.
            int covDenseIdx = -1;
            for (int k = 0; k < columnCount; k++) {
                if (metadata.getWriterIndex(k) == covWriterIdx) {
                    covDenseIdx = k;
                    break;
                }
            }
            if (covDenseIdx < 0) {
                names.add(null);
                nameTxns.add(TableUtils.COLUMN_NAME_TXN_NONE);
                tops.add(0);
                shifts.add(0);
                indices.add(-1);
                types.add(-1);
                continue;
            }
            int covType = metadata.getColumnType(covDenseIdx);
            names.add(metadata.getColumnName(covDenseIdx));
            nameTxns.add(columnVersionReader.getColumnNameTxn(partitionTimestamp, covWriterIdx));
            tops.add(Math.max(0, columnVersionReader.getColumnTop(partitionTimestamp, covWriterIdx)));
            shifts.add(ColumnType.pow2SizeOf(covType));
            indices.add(covWriterIdx);
            types.add(covType);
        }
        // PostingIndexWriter compares the timestamp parameter against
        // the writer-space coveredColumnIndices we just built, so
        // translate metadata.getTimestampIndex() (dense) to writer
        // space. After DROP COLUMN before the timestamp the two index
        // spaces diverge and the comparison would otherwise hit the
        // wrong column.
        int tsDense = metadata.getTimestampIndex();
        int tsWriter = tsDense >= 0 ? metadata.getWriterIndex(tsDense) : -1;
        indexWriter.configureCovering(names, nameTxns, tops, shifts, indices, types, tsWriter);
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

        // Create empty .v file. Fresh index: POSTING sealTxn starts at 0
        // (pre-seal state); BITMAP ignores the sealTxn arg.
        LPSZ valueFileName = IndexFactory.valueFileName(indexType, path.trimTo(partitionPathLen), columnName, columnNameTxn, 0L);
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
            ParquetPartitionDecoder partitionDecoder,
            RowGroupBuffers rowGroupBuffers,
            DirectIntList parquetColumns,
            ObjList<IndexWriter> indexWriters,
            StringSink columnNamesSink,
            LongList columnTops,
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp,
            long partitionNameTxn,
            long partitionRowCount,
            long currentTableTxn
    ) {
        final ParquetMetaFileReader parquetMetadata = partitionDecoder.metadata();
        final int columnCount = metadata.getColumnCount();

        // First pass: identify indexed columns and collect names for logging.
        // columnNamesSink/columnTops are caller-owned scratch reused across
        // partitions; reset them here instead of allocating.
        columnNamesSink.clear();
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
        try {
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                int parquetColumnIndex = getIndexedParquetColumnIndex(metadata, parquetMetadata, columnVersionReader, columnIndex, partitionTimestamp, partitionRowCount);
                if (parquetColumnIndex == -1) {
                    continue;
                }

                final int writerIndex = metadata.getWriterIndex(columnIndex);
                final String columnName = metadata.getColumnName(columnIndex);
                final long columnNameTxn = columnVersionReader.getColumnNameTxn(partitionTimestamp, writerIndex);
                final int indexBlockCapacity = metadata.getIndexValueBlockCapacity(columnIndex);
                final byte indexType = metadata.getColumnIndexType(columnIndex);

                // Remove existing index files
                removeIndexFiles(ff, path, partitionPathLen, columnName, columnNameTxn, indexType);

                // Create new index files
                createIndexFiles(ff, path, partitionPathLen, columnName, columnNameTxn, indexBlockCapacity, indexType);

                // Open IndexWriter. POSTING needs partitionTimestamp/partitionNameTxn
                // wired so seal() can produce .pv.<sealTxn> and .pc<N>.*.<sealTxn>
                // sidecars. BITMAP ignores those parameters.
                IndexWriter indexWriter = IndexFactory.createWriter(indexType, configuration);
                try {
                    indexWriter.of(path.trimTo(partitionPathLen), columnName, columnNameTxn, partitionTimestamp, partitionNameTxn);
                    if (IndexType.isPosting(indexType)) {
                        // Configure INCLUDE columns before any add() so seal() can
                        // build .pci/.pc<N> sidecars. Symmetric to the native
                        // partition rebuild path. removeIndexFiles above already
                        // wiped any existing sidecars.
                        configureCoveringForPosting(indexWriter, columnName, metadata, columnVersionReader, partitionTimestamp);
                    }
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
            final int rowGroupCount = parquetMetadata.getRowGroupCount();

            // We need to track columnTop per indexed column - re-iterate to get them
            columnTops.clear();
            for (int columnIndex = 0; columnIndex < columnCount && columnTops.size() < indexedColumnCount; columnIndex++) {
                if (getIndexedParquetColumnIndex(metadata, parquetMetadata, columnVersionReader, columnIndex, partitionTimestamp, partitionRowCount) == -1) {
                    continue;
                }

                final int writerIndex = metadata.getWriterIndex(columnIndex);
                columnTops.add(columnVersionReader.getColumnTop(partitionTimestamp, writerIndex));
            }

            long rowCount = 0;
            for (int rowGroupIndex = 0; rowGroupIndex < rowGroupCount; rowGroupIndex++) {
                final long rowGroupSize = parquetMetadata.getRowGroupSize(rowGroupIndex);

                // Check if any column needs data from this row group
                boolean needsDecode = false;
                for (int i = 0; i < indexedColumnCount; i++) {
                    if (rowCount + rowGroupSize > columnTops.getQuick(i)) {
                        needsDecode = true;
                        break;
                    }
                }

                if (!needsDecode) {
                    rowCount += rowGroupSize;
                    continue;
                }

                // Decode all indexed columns for this row group
                assert rowGroupSize <= Integer.MAX_VALUE;
                try {
                    partitionDecoder.decodeRowGroup(rowGroupBuffers, parquetColumns, rowGroupIndex, 0, (int) rowGroupSize);
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
                    final long columnTop = columnTops.getQuick(i);
                    if (rowCount + rowGroupSize <= columnTop) {
                        continue; // This column doesn't have data in this row group yet
                    }

                    final IndexWriter indexWriter = indexWriters.get(i);
                    final long startOffset = Math.max(0, columnTop - rowCount);
                    long rowId = Math.max(rowCount, columnTop);

                    final long addr = rowGroupBuffers.getChunkDataPtr(i);
                    final long size = rowGroupBuffers.getChunkDataSize(i);
                    if (size == 0) {
                        BitmapIndexUtils.addNullEntries(indexWriter, rowId, rowCount + rowGroupSize);
                    } else {
                        for (long p = addr + startOffset * 4, lim = addr + size; p < lim; p += 4, rowId++) {
                            indexWriter.add(TableUtils.toIndexKey(Unsafe.getInt(p)), rowId);
                        }
                    }
                }

                rowCount += rowGroupSize;
            }

            // Finalize each writer. POSTING calls seal() to produce sealed
            // .pv.<sealTxn> and .pci/.pc<N> covering sidecars (symmetric to
            // the native partition path); BITMAP keeps setMaxValue + commit.
            for (int i = 0; i < indexedColumnCount; i++) {
                final IndexWriter w = indexWriters.get(i);
                if (IndexType.isPosting(w.getIndexType())) {
                    // Tag the seal's chain entry with the committed _txn so a
                    // later recovery walk does not mis-classify the rebuilt
                    // index as abandoned.
                    w.setNextTxnAtSeal(currentTableTxn);
                    w.seal();
                } else {
                    w.setMaxValue(partitionRowCount - 1);
                    w.commit();
                }
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

    /**
     * Immutable carrier for a parallel task failure: submitParallelTask copies
     * the message of the worker's thread-local-reused exception into this carrier
     * before it reaches the {@code Future}, so the draining thread never reads a
     * mutable message cross-thread. Carries no stack trace or cause: the worker
     * has already logged the original failure.
     */
    private static class ParallelTaskException extends RuntimeException {
        ParallelTaskException(String message) {
            super(message, null, false, false);
        }
    }
}