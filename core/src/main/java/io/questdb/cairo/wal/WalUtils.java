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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriterMetadata;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateReader;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.seq.TableSequencerImpl;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TableTransactionLogV1;
import io.questdb.cairo.wal.seq.TableTransactionLogV2;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;

import static io.questdb.cairo.wal.WalTxnType.MAT_VIEW_DATA;
import static io.questdb.cairo.wal.WalTxnType.MAT_VIEW_INVALIDATE;

public class WalUtils {
    public static final String CONVERT_FILE_NAME = "_convert";
    public static final int DROP_TABLE_STRUCTURE_VERSION = -2;
    public static final int DROP_TABLE_WAL_ID = -2;
    public static final String EVENT_FILE_NAME = "_event";
    public static final String EVENT_INDEX_FILE_NAME = "_event.i";
    public static final CharSequence INITIAL_META_FILE_NAME = "_meta.0";
    public static final int METADATA_WALID = -1;
    public static final int MIN_WAL_ID = DROP_TABLE_WAL_ID;
    // Per-table marker file written on the freshly-created table of an ALTER TABLE ... REBASE WAL. Lives
    // in the table dir and is permanent (survives restart, never removed). When the uploader first records
    // the table in the replication index it stats this marker and, if present, skips the empty seed txn and
    // records first_txn=2; a replica stalls instead of building the table from empty until a physical copy
    // arrives. This is handled entirely via the marker + first_txn, not via a getReplicationStatus byte
    // (that callback only ever returns ACTIVE/SUSPENDED/DISABLED).
    public static final String REBASE_NEW_FILE_NAME = "_rebase_new";
    // Per-table marker written on the OLD (source) dir of an ALTER TABLE ... REBASE WAL. The uploader
    // stats it as the source dir winds down and records the table in the replication index with the
    // rebase-source flag (high bit of last_txn) instead of a drop, so the object-store baseline is kept
    // for the rebased table and replicas treat the table as replication-disabled (they keep what they
    // have). The dir is purged locally by the normal WAL apply/purge jobs.
    public static final String REBASE_SOURCE_FILE_NAME = "_rebase_source";
    public static final int SEG_MIN_ID = 0;
    public static final int SEG_NONE_ID = Integer.MAX_VALUE >> 2;
    public static final int SEG_MAX_ID = SEG_NONE_ID - 1;
    public static final String SEQ_DIR = "txn_seq";
    public static final String SEQ_DIR_DEPRECATED = "seq";
    public static final long SEQ_META_COVERING_COLUMN_CHECKSUM_SALT = 0x434F5652; // COVR
    public static final long SEQ_META_INDEX_TYPE_CHECKSUM_SALT = 0x494E4458; // INDX
    public static final long SEQ_META_OFFSET_WAL_LENGTH = 0;
    public static final long SEQ_META_OFFSET_WAL_VERSION = SEQ_META_OFFSET_WAL_LENGTH + Integer.BYTES;
    public static final long SEQ_META_OFFSET_STRUCTURE_VERSION = SEQ_META_OFFSET_WAL_VERSION + Integer.BYTES;
    public static final long SEQ_META_OFFSET_COLUMN_COUNT = SEQ_META_OFFSET_STRUCTURE_VERSION + Long.BYTES;
    public static final long SEQ_META_OFFSET_TIMESTAMP_INDEX = SEQ_META_OFFSET_COLUMN_COUNT + Integer.BYTES;
    public static final long SEQ_META_TABLE_ID = SEQ_META_OFFSET_TIMESTAMP_INDEX + Integer.BYTES;
    public static final long SEQ_META_SUSPENDED = SEQ_META_TABLE_ID + Integer.BYTES;
    public static final long SEQ_META_OFFSET_COLUMNS = SEQ_META_SUSPENDED + Byte.BYTES;
    public static final long SEQ_META_REPLICA_ONLY_CHECKSUM_SALT = 0x52504C59; // RPLY
    public static final String TABLE_REGISTRY_NAME_FILE = "tables.d";
    public static final String TXNLOG_FILE_NAME = "_txnlog";
    public static final String TXNLOG_FILE_NAME_META_INX = "_txnlog.meta.i";
    public static final String TXNLOG_FILE_NAME_META_VAR = "_txnlog.meta.d";
    public static final String TXNLOG_PARTS_DIR = "_txn_parts";
    public static final int WALE_HEADER_SIZE = Integer.BYTES + Integer.BYTES;
    public static final long WALE_MAX_TXN_OFFSET_32 = 0L;
    // DEFAULT DEDUP mode means following the table definition. If the table has dedup enabled, then
    // the commit will deduplicate the data, otherwise it will not.
    public static final byte WAL_DEDUP_MODE_DEFAULT = 0;
    // NO_DEDUP mode means not deduplicating the data, even if the table definition has dedup enabled. For future use.
    public static final byte WAL_DEDUP_MODE_NO_DEDUP = WAL_DEDUP_MODE_DEFAULT + 1;
    // UPSERT_NEW mode means inserting new data when the keys match, this is the initially supported deduplication type.
    public static final byte WAL_DEDUP_MODE_UPSERT_NEW = WAL_DEDUP_MODE_NO_DEDUP + 1;
    // REPLACE_RANGE mode means replacing the existing range of data with the new data.
    public static final byte WAL_DEDUP_MODE_REPLACE_RANGE = WAL_DEDUP_MODE_UPSERT_NEW + 1;
    public static final byte WAL_DEDUP_MODE_MAX = WAL_DEDUP_MODE_REPLACE_RANGE;
    public static final int WAL_FORMAT_OFFSET_32 = Integer.BYTES;
    public static final short WAL_FORMAT_VERSION = 0;
    public static final short WALE_FORMAT_VERSION = WAL_FORMAT_VERSION;
    public static final short WALE_MAT_VIEW_FORMAT_VERSION = WALE_FORMAT_VERSION + 1;
    public static final short WALE_VIEW_FORMAT_VERSION = WALE_MAT_VIEW_FORMAT_VERSION + 1;
    public static final String WAL_INDEX_FILE_NAME = "_wal_index.d";
    public static final String WAL_NAME_BASE = "wal";
    public static final String WAL_PENDING_FS_MARKER = ".pending";
    public static final int WAL_SEQUENCER_FORMAT_VERSION_V1 = 0;
    public static final int WAL_SEQUENCER_FORMAT_VERSION_V2 = 1;
    public static long WAL_DEFAULT_BASE_TABLE_TXN = Long.MIN_VALUE;
    public static long WAL_DEFAULT_LAST_PERIOD_HI = Long.MIN_VALUE;
    public static long WAL_DEFAULT_LAST_REFRESH_TIMESTAMP = Long.MIN_VALUE;

    /**
     * Builds a complete rebased table in the staging directory {@code dstDir} for
     * {@code ALTER TABLE ... REBASE WAL}: clones the source table's data (hard-links the immutable
     * partition column files, copies the table-root files, excludes the sequencer {@code txn_seq} and
     * WAL segment dirs {@code wal*} plus transient markers), resets {@code _txn}/{@code _meta} to a fresh
     * table (seqTxn 0, new tableId, metadataVersion 0), and creates the sequencer files. The caller
     * positions {@code srcDir}/{@code dstDir} at the respective table dirs and must have created
     * {@code dstDir}; it then renames {@code dstDir} into place atomically. No in-memory sequencer is
     * registered and the WAL listener is not notified - the live sequencer opens lazily on first access.
     * When {@code markRebased} is set, the permanent {@code _rebase_new} marker is written too (see below).
     */
    public static void cloneTableDirForRebase(
            CairoConfiguration configuration,
            WalDirectoryPolicy walDirectoryPolicy,
            Path srcDir,
            Path dstDir,
            TableToken newToken,
            int newTableId,
            boolean markRebased,
            StringSink nameSink
    ) {
        final FilesFacade ff = configuration.getFilesFacade();
        final int dirMode = configuration.getMkDirMode();
        final int srcLen = srcDir.size();
        final int dstLen = dstDir.size();
        final long pFind = ff.findFirst(srcDir.$());
        if (pFind < 1) {
            throw CairoException.critical(ff.errno()).put("could not list table dir for rebase [path=").put(srcDir).put(']');
        }
        try {
            do {
                final long pName = ff.findName(pFind);
                if (!Files.notDots(pName)) {
                    continue;
                }
                final int type = ff.findType(pFind);
                nameSink.clear();
                Utf8s.utf8ToUtf16Z(pName, nameSink);
                if (type == Files.DT_FILE) {
                    if (isRebaseClonedRootFile(nameSink)) {
                        srcDir.trimTo(srcLen).concat(pName);
                        dstDir.trimTo(dstLen).concat(pName);
                        if (ff.copy(srcDir.$(), dstDir.$()) < 0) {
                            throw CairoException.critical(ff.errno()).put("could not clone table file [from=").put(srcDir).put(", to=").put(dstDir).put(']');
                        }
                    }
                } else if (!isRebaseExcludedDir(nameSink)) {
                    // A data partition directory: hard-link the immutable column files inside it.
                    srcDir.trimTo(srcLen).concat(pName);
                    dstDir.trimTo(dstLen).concat(pName);
                    ff.mkdir(dstDir.$(), dirMode);
                    if (ff.hardLinkDirRecursive(srcDir, dstDir, dirMode) < 0) {
                        throw CairoException.critical(ff.errno()).put("could not hard-link partition for rebase [from=").put(srcDir).put(", to=").put(dstDir).put(']');
                    }
                }
                srcDir.trimTo(srcLen);
                dstDir.trimTo(dstLen);
            } while (ff.findNext(pFind) > 0);
        } finally {
            ff.findClose(pFind);
            srcDir.trimTo(srcLen);
            dstDir.trimTo(dstLen);
        }

        // Reset _txn (seqTxn=0, lag, structure version=0) and _meta (new tableId, metadataVersion=0) in
        // the staging dir - exactly as WAL conversion does (TableConverter) - then create the sequencer
        // files so the rename carries a complete table into place.
        try (
                TxWriter txWriter = new TxWriter(ff, configuration);
                MemoryMARW metaMem = Vm.getCMARWInstance()
        ) {
            txWriter.ofRW(dstDir.concat(TableUtils.TXN_FILE_NAME).$());
            txWriter.resetLagValuesUnsafe();
            TableUtils.openSmallFile(ff, dstDir.trimTo(dstLen), dstLen, metaMem, TableUtils.META_FILE_NAME, MemoryTag.MMAP_TABLE_WRITER);
            metaMem.putInt(TableUtils.META_OFFSET_TABLE_ID, newTableId);
            metaMem.putLong(TableUtils.META_OFFSET_METADATA_VERSION, 0);
            txWriter.resetStructureVersionUnsafe();

            TableUtils.openSmallFile(ff, dstDir.trimTo(dstLen), dstLen, metaMem, TableUtils.META_FILE_NAME, MemoryTag.MMAP_TABLE_WRITER);
            try (TableWriterMetadata metadata = new TableWriterMetadata(newToken)) {
                metadata.reload(dstDir.trimTo(dstLen), metaMem);
                TableSequencerImpl.createSequencerFiles(configuration, walDirectoryPolicy, dstDir.trimTo(dstLen), metadata, newToken, newTableId);
            }
        }
        dstDir.trimTo(dstLen);

        // Mark the new table rebased while it is still invisible in the staging dir, so the permanent
        // _rebase_new marker is in place before the rename makes the table observable to the uploader.
        // The uploader's first poll stats this marker and, if present, skips the empty seed txn and records
        // first_txn=2 (otherwise it would lock first_txn=0 and a replica would build the table from an
        // incomplete baseline). The replica variant follows the primary's dir and must NOT mark rebased, so
        // markRebased is false there. No-op effect in OSS, which has no uploader to consume the marker.
        if (markRebased) {
            writeRebaseNewMarker(ff, dstDir);
        }
    }

    public static void createTxnLogFile(FilesFacade ff, MemoryMARW mem, Path txnSeqDirPath, long tableCreateDate, int chunkSize, int mkDirMode) {
        int rootLen = txnSeqDirPath.size();
        try {
            if (chunkSize < 1) {
                mem.smallFile(ff, txnSeqDirPath.concat(WalUtils.TXNLOG_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                mem.putInt(WAL_SEQUENCER_FORMAT_VERSION_V1);
                mem.putLong(0L);
                mem.putLong(tableCreateDate);
                mem.close();
            } else {
                mem.smallFile(ff, txnSeqDirPath.concat(WalUtils.TXNLOG_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                mem.putInt(WAL_SEQUENCER_FORMAT_VERSION_V2);
                mem.putLong(0L);
                mem.putLong(tableCreateDate);
                mem.putInt(chunkSize);
                mem.jumpTo(TableTransactionLogFile.HEADER_SIZE);
                mem.close(false);

                txnSeqDirPath.trimTo(rootLen).concat(WalUtils.TXNLOG_PARTS_DIR);
                if (!ff.exists(txnSeqDirPath.$())) {
                    ff.mkdir(txnSeqDirPath.$(), mkDirMode);
                }
            }
        } finally {
            txnSeqDirPath.trimTo(rootLen);
        }
    }

    /**
     * Whether the {@link #REBASE_NEW_FILE_NAME} marker exists in the table dir.
     *
     * @param tableDirPath path positioned at the table directory ({@code <dbRoot>/<dirName>}); restored on return.
     */
    public static boolean isRebaseNewMarkerPresent(FilesFacade ff, Path tableDirPath) {
        final int len = tableDirPath.size();
        try {
            return ff.exists(tableDirPath.concat(REBASE_NEW_FILE_NAME).$());
        } finally {
            tableDirPath.trimTo(len);
        }

        // Optional section: replica-only index flags (backward compatible), mirrors the index-type section.
        // Written last so older readers ignore the trailing bytes and newer readers find it after the
        // covering-columns section.
        metaMem.putLong(checkSum * 31 + SEQ_META_REPLICA_ONLY_CHECKSUM_SALT);
        metaMem.putInt(columnCount);
        for (int i = 0; i < columnCount; i++) {
            metaMem.putByte((byte) (metadata.getColumnMetadata(i).isReplicaOnlyIndex() ? 1 : 0));
        }
    }

    /**
     * Retrieves the last refresh state for a materialized view.
     * <p>
     * Note: This function is intended to be used during the initialization phase only.
     * It has been extracted into `WalUtils` solely to facilitate its usage in tests.
     *
     * @param tablePath          the path to the table
     * @param tableToken         the table token
     * @param configuration      the Cairo configuration
     * @param txnLogMemory       the memory to iterate over transaction logs
     * @param walEventReader     the reader to read WAL events
     * @param blockFileReader    the reader to read the state file
     * @param matViewStateReader the reader used to collect the state
     * @return true if the last mat view state was successfully retrieved, false otherwise
     */
    public static boolean readMatViewState(
            Path tablePath,
            TableToken tableToken,
            CairoConfiguration configuration,
            MemoryCMR txnLogMemory,
            WalEventReader walEventReader,
            BlockFileReader blockFileReader,
            MatViewStateReader matViewStateReader
    ) {
        try (MemoryCMR mem = txnLogMemory) {
            final int tablePathLen = tablePath.size();
            mem.smallFile(configuration.getFilesFacade(), tablePath.concat(SEQ_DIR).concat(TXNLOG_FILE_NAME).$(), MemoryTag.MMAP_TX_LOG);
            if (mem.size() < TableTransactionLogFile.HEADER_SIZE) {
                throw CairoException.critical(0).put("invalid transaction log file [path=")
                        .put(tablePath)
                        .put(", size=")
                        .put(mem.size())
                        .put(']');
            }
            final int formatVersion = mem.getInt(TableTransactionLogFile.TX_LOG_STRUCTURE_VERSION_OFFSET);
            if (formatVersion == WAL_SEQUENCER_FORMAT_VERSION_V1) {
                final long txnCount = mem.getLong(TableTransactionLogFile.MAX_TXN_OFFSET_64);
                if (txnCount > 0 && mem.size() >= TableTransactionLogFile.HEADER_SIZE + txnCount * TableTransactionLogV1.RECORD_SIZE) {
                    for (long txn = txnCount - 1; txn >= 0; txn--) {
                        final long offset = TableTransactionLogFile.HEADER_SIZE + txn * TableTransactionLogV1.RECORD_SIZE;
                        if (processTransaction(
                                mem,
                                offset,
                                tablePath,
                                tablePathLen,
                                walEventReader,
                                blockFileReader,
                                matViewStateReader,
                                tableToken
                        )) {
                            return true;
                        }
                    }
                }
            } else if (formatVersion == WAL_SEQUENCER_FORMAT_VERSION_V2) {
                final long txnCount = mem.getLong(TableTransactionLogFile.MAX_TXN_OFFSET_64);
                final long partSize = mem.getInt(TableTransactionLogFile.HEADER_SEQ_PART_SIZE_32);
                if (txnCount > 0 && partSize > 0) {
                    final long partCount = (txnCount + partSize - 1) / partSize;
                    try (MemoryCMR partMem = Vm.getCMRInstance(configuration.getBypassWalFdCache())) {
                        for (long part = partCount - 1; part >= 0; part--) {
                            tablePath.trimTo(tablePathLen).concat(SEQ_DIR).concat(TXNLOG_PARTS_DIR).slash().put(part);
                            partMem.smallFile(configuration.getFilesFacade(), tablePath.$(), MemoryTag.MMAP_TX_LOG);
                            final long partTxnCount = Math.min(partSize, txnCount - part * partSize);
                            for (long txn = partTxnCount - 1; txn >= 0; txn--) {
                                final long offset = txn * TableTransactionLogV2.RECORD_SIZE;
                                if (processTransaction(
                                        partMem,
                                        offset,
                                        tablePath,
                                        tablePathLen,
                                        walEventReader,
                                        blockFileReader,
                                        matViewStateReader,
                                        tableToken
                                )) {
                                    return true;
                                }
                            }
                        }
                    }
                }
            } else {
                throw new UnsupportedOperationException("Unsupported transaction log version: " + formatVersion);
            }
        }
        return false;
    }

    /**
     * Creates the {@link #REBASE_NEW_FILE_NAME} marker in the table dir. Idempotent.
     *
     * @param tableDirPath path positioned at the table directory; restored on return.
     */
    public static void writeRebaseNewMarker(FilesFacade ff, Path tableDirPath) {
        final int len = tableDirPath.size();
        try {
            if (!ff.exists(tableDirPath.concat(REBASE_NEW_FILE_NAME).$()) && !ff.touch(tableDirPath.$())) {
                throw CairoException.critical(ff.errno()).put("could not create rebase-new marker [path=").put(tableDirPath).put(']');
            }
        } finally {
            tableDirPath.trimTo(len);
        }
    }

    /**
     * Creates the {@link #REBASE_SOURCE_FILE_NAME} marker in the table dir. Idempotent.
     *
     * @param tableDirPath path positioned at the table directory; restored on return.
     */
    public static void writeRebaseSourceMarker(FilesFacade ff, Path tableDirPath) {
        final int len = tableDirPath.size();
        try {
            if (!ff.exists(tableDirPath.concat(REBASE_SOURCE_FILE_NAME).$()) && !ff.touch(tableDirPath.$())) {
                throw CairoException.critical(ff.errno()).put("could not create rebase-source marker [path=").put(tableDirPath).put(']');
            }
        } finally {
            tableDirPath.trimTo(len);
        }
    }

    public static void writeSequencerMetadataOptionalSections(
            MemoryMARW metaMem,
            int columnCount,
            long checkSum,
            RecordMetadata metadata,
            IntList readColumnOrder
    ) {
        metaMem.putLong(checkSum);
        if (readColumnOrder != null && readColumnOrder.size() > 0) {
            metaMem.putInt(readColumnOrder.size());
            for (int i = 0, n = readColumnOrder.size(); i < n; i++) {
                metaMem.putInt(readColumnOrder.getQuick(i));
            }
        } else {
            metaMem.putInt(columnCount);
            for (int i = 0; i < columnCount; i++) {
                metaMem.putInt(i);
            }
        }

        metaMem.putLong(checkSum * 31 + SEQ_META_INDEX_TYPE_CHECKSUM_SALT);
        metaMem.putInt(columnCount);
        for (int i = 0; i < columnCount; i++) {
            metaMem.putByte(metadata.getColumnMetadata(i).getIndexType());
        }

        metaMem.putLong(checkSum * 31 + SEQ_META_COVERING_COLUMN_CHECKSUM_SALT);
        int coveringColumnCount = 0;
        for (int i = 0; i < columnCount; i++) {
            IntList indices = metadata.getColumnMetadata(i).getCoveringColumnIndices();
            if (indices != null && indices.size() > 0) {
                coveringColumnCount++;
            }
        }
        metaMem.putInt(coveringColumnCount);
        for (int i = 0; i < columnCount; i++) {
            IntList indices = metadata.getColumnMetadata(i).getCoveringColumnIndices();
            if (indices != null && indices.size() > 0) {
                metaMem.putInt(i);
                metaMem.putInt(indices.size());
                for (int j = 0, n = indices.size(); j < n; j++) {
                    metaMem.putInt(indices.getQuick(j));
                }
            }
        }
    }

    // Whether a top-level file should be COPIED into a rebase clone (everything except transient markers).
    private static boolean isRebaseClonedRootFile(CharSequence name) {
        if (Chars.equals(name, TableUtils.TODO_FILE_NAME)
                || Chars.equals(name, CONVERT_FILE_NAME)
                || Chars.equals(name, REBASE_NEW_FILE_NAME)
                || Chars.equals(name, REBASE_SOURCE_FILE_NAME)
                || Chars.equals(name, TableUtils.TXN_SCOREBOARD_FILE_NAME)) {
            return false;
        }
        return !Chars.endsWith(name, WAL_PENDING_FS_MARKER);
    }

    // Whether a top-level directory entry should be EXCLUDED from a rebase clone (sequencer + WAL dirs).
    private static boolean isRebaseExcludedDir(CharSequence name) {
        return Chars.equals(name, SEQ_DIR)
                || Chars.equals(name, SEQ_DIR_DEPRECATED)
                || Chars.startsWith(name, WAL_NAME_BASE);
    }

    private static boolean processTransaction(
            MemoryCMR mem,
            long offset,
            Path tablePath,
            int tablePathLen,
            WalEventReader walEventReader,
            BlockFileReader blockFileReader,
            MatViewStateReader matViewStateReader,
            TableToken tableToken
    ) {
        final int walId = mem.getInt(offset + TableTransactionLogFile.TX_LOG_WAL_ID_OFFSET);
        final int segmentId = mem.getInt(offset + TableTransactionLogFile.TX_LOG_SEGMENT_OFFSET);
        final int segmentTxn = mem.getInt(offset + TableTransactionLogFile.TX_LOG_SEGMENT_TXN_OFFSET);
        // Only process valid WAL IDs
        if (walId > 0) {
            tablePath.trimTo(tablePathLen).concat(WAL_NAME_BASE).put(walId).slash().put(segmentId);
            // Since we are scanning the transaction log of a materialized view table,
            // we assume the last transaction is the one we are looking for (for the most cases).
            // As a result, fd and memory usage is not optimized.
            try (WalEventReader eventReader = walEventReader) {
                WalEventCursor walEventCursor = eventReader.of(tablePath, segmentTxn);
                if (walEventCursor.getType() == MAT_VIEW_DATA) {
                    matViewStateReader.of(walEventCursor.getMatViewDataInfo());
                    return true;
                }
                if (walEventCursor.getType() == MAT_VIEW_INVALIDATE) {
                    matViewStateReader.of(walEventCursor.getMatViewInvalidationInfo());
                    return true;
                }
            } catch (Throwable th) {
                // walEventReader may not be able to find/open the WAL-e files
                try (BlockFileReader blockReader = blockFileReader) {
                    tablePath.trimTo(tablePathLen).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME);
                    blockReader.of(tablePath.$());
                    matViewStateReader.of(blockReader, tableToken);
                    return true;
                } catch (Throwable ignored) {
                }
                return false;
            }
        }
        return false;
    }
}
