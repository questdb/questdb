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

import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FindVisitor;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.QuietCloseable;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.openSmallFile;
import static io.questdb.cairo.wal.WalUtils.TXNLOG_FILE_NAME;
import static io.questdb.cairo.wal.WalUtils.TXNLOG_FILE_NAME_META_INX;
import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;

/**
 * Shared helper class for restoring table files from checkpoint or backup.
 * Used by both DatabaseCheckpointAgent and DatabaseRestoreAgent.
 */
public class TableSnapshotRestore implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(TableSnapshotRestore.class);
    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final SymbolMapUtil symbolMapUtil = new SymbolMapUtil();
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
    }

    @Override
    public void close() {
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
     * @param recoveredTxnFiles  counter for recovered txn files
     * @param recoveredCVFiles   counter for recovered CV files
     */
    public void copyMetadataFiles(
            Path srcPath,
            Path dstPath,
            AtomicInteger recoveredMetaFiles,
            AtomicInteger recoveredTxnFiles,
            AtomicInteger recoveredCVFiles
    ) {
        int srcPathLen = srcPath.size();
        int dstPathLen = dstPath.size();
        try {
            copyFile(srcPath.trimTo(srcPathLen), dstPath.trimTo(dstPathLen), recoveredMetaFiles, TableUtils.META_FILE_NAME, false);
            copyFile(srcPath.trimTo(srcPathLen), dstPath.trimTo(dstPathLen), recoveredMetaFiles, TableUtils.TABLE_NAME_FILE, true);
            copyFile(srcPath.trimTo(srcPathLen), dstPath.trimTo(dstPathLen), recoveredTxnFiles, TableUtils.TXN_FILE_NAME, false);
            copyFile(srcPath.trimTo(srcPathLen), dstPath.trimTo(dstPathLen), recoveredCVFiles, TableUtils.COLUMN_VERSION_FILE_NAME, false);
            copyFile(srcPath.trimTo(srcPathLen), dstPath.trimTo(dstPathLen), recoveredCVFiles, MatViewState.MAT_VIEW_STATE_FILE_NAME, true);
            copyFile(srcPath.trimTo(srcPathLen), dstPath.trimTo(dstPathLen), recoveredCVFiles, MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME, true);
        } finally {
            srcPath.trimTo(srcPathLen);
            dstPath.trimTo(dstPathLen);
        }
    }

    public MemoryMARW getMemFile() {
        return memFile;
    }

    public TableReaderMetadata getTableMetadata() {
        return tableMetadata;
    }

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

                dstPath.trimTo(dstSeqLen).concat(TableUtils.META_FILE_NAME);
                memFile.smallFile(ff, dstPath.$(), MemoryTag.MMAP_SEQUENCER_METADATA);
                dstPath.trimTo(dstSeqLen);
                openSmallFile(ff, dstPath, dstSeqLen, memFile, TXNLOG_FILE_NAME_META_INX, MemoryTag.MMAP_TX_LOG);

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
     * @param tablePath            path to the table directory
     * @param recoveredSymbolFiles counter for recovered symbol files
     */
    public void rebuildTableFiles(
            Path tablePath,
            AtomicInteger recoveredSymbolFiles
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
     * @param recoveredTxnFiles  counter for recovered txn files
     * @param recoveredCVFiles   counter for recovered CV files
     * @param recoveredWalFiles  counter for recovered WAL files
     * @param symbolFilesCount   counter for recovered symbol files
     */
    public void restoreTableFiles(
            Path srcPath,
            Path dstPath,
            AtomicInteger recoveredMetaFiles,
            AtomicInteger recoveredTxnFiles,
            AtomicInteger recoveredCVFiles,
            AtomicInteger recoveredWalFiles,
            AtomicInteger symbolFilesCount
    ) {
        int srcPathLen = srcPath.size();
        int dstPathLen = dstPath.size();

        // Copy metadata files from source to the destination table location
        copyMetadataFiles(srcPath, dstPath, recoveredMetaFiles, recoveredTxnFiles, recoveredCVFiles);

        // Reset _todo_ file to prevent metadata restoration on table open
        TableUtils.resetTodoLog(ff, dstPath, dstPathLen, memFile);

        // Rebuild symbol files and other table-specific processing
        rebuildTableFiles(dstPath.trimTo(dstPathLen), symbolFilesCount);

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
            if (optional && Files.isErrnoFileDoesNotExist(ff.errno()) && !ff.exists(srcPath.$())) {
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
        for (int i = 0; i < tableMetadata.getColumnCount(); i++) {
            final int columnType = tableMetadata.getColumnType(i);
            if (ColumnType.isSymbol(columnType)) {
                final int cleanSymbolCount = txWriter.getSymbolValueCount(tableMetadata.getDenseSymbolIndex(i));
                final String columnName = tableMetadata.getColumnName(i);
                LOG.info().$("rebuilding symbol files [table=").$(tablePath)
                        .$(", column=").$safe(columnName)
                        .$(", count=").$(cleanSymbolCount)
                        .I$();

                final int writerIndex = tableMetadata.getWriterIndex(i);
                final int indexKeyBlockCapacity = tableMetadata.getIndexBlockCapacity(i);
                symbolMapUtil.rebuildSymbolFiles(
                        configuration,
                        tablePath,
                        columnName,
                        columnVersionReader.getSymbolTableNameTxn(writerIndex),
                        cleanSymbolCount,
                        -1,
                        indexKeyBlockCapacity
                );
                recoveredSymbolFiles.incrementAndGet();
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
}