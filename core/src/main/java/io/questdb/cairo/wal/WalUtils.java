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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateReader;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TableTransactionLogV1;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;

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
    public static final int SEG_MIN_ID = 0;
    public static final int SEG_NONE_ID = Integer.MAX_VALUE >> 2;
    public static final int SEG_MAX_ID = SEG_NONE_ID - 1;
    public static final String SEQ_DIR = "txn_seq";
    public static final String SEQ_DIR_DEPRECATED = "seq";
    public static final long SEQ_META_OFFSET_WAL_LENGTH = 0;
    public static final long SEQ_META_OFFSET_WAL_VERSION = SEQ_META_OFFSET_WAL_LENGTH + Integer.BYTES;
    public static final long SEQ_META_OFFSET_STRUCTURE_VERSION = SEQ_META_OFFSET_WAL_VERSION + Integer.BYTES;
    public static final long SEQ_META_OFFSET_COLUMN_COUNT = SEQ_META_OFFSET_STRUCTURE_VERSION + Long.BYTES;
    public static final long SEQ_META_OFFSET_TIMESTAMP_INDEX = SEQ_META_OFFSET_COLUMN_COUNT + Integer.BYTES;
    public static final long SEQ_META_TABLE_ID = SEQ_META_OFFSET_TIMESTAMP_INDEX + Integer.BYTES;
    public static final long SEQ_META_SUSPENDED = SEQ_META_TABLE_ID + Integer.BYTES;
    public static final long SEQ_META_OFFSET_COLUMNS = SEQ_META_SUSPENDED + Byte.BYTES;
    public static final String TABLE_REGISTRY_NAME_FILE = "tables.d";
    public static final String TXNLOG_FILE_NAME = "_txnlog";
    public static final String TXNLOG_FILE_NAME_META_INX = "_txnlog.meta.i";
    public static final String TXNLOG_FILE_NAME_META_VAR = "_txnlog.meta.d";
    public static final String TXNLOG_PARTS_DIR = "_txn_parts";
    public static final int WALE_HEADER_SIZE = Integer.BYTES + Integer.BYTES;
    public static final long WALE_MAX_TXN_OFFSET_32 = 0L;
    public static final int WAL_FORMAT_OFFSET_32 = Integer.BYTES;
    public static final int WAL_FORMAT_VERSION = 0;
    public static final int WALE_FORMAT_VERSION = WAL_FORMAT_VERSION;
    public static final int WALE_MAT_VIEW_FORMAT_VERSION = WALE_FORMAT_VERSION + 1;
    public static final String WAL_INDEX_FILE_NAME = "_wal_index.d";
    public static final String WAL_NAME_BASE = "wal";
    public static final String WAL_PENDING_FS_MARKER = ".pending";
    public static final int WAL_SEQUENCER_FORMAT_VERSION_V1 = 0;
    public static final int WAL_SEQUENCER_FORMAT_VERSION_V2 = 1;

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
     * Retrieves the last refresh base transaction for a materialized view.
     *
     * @param tablePath          the path to the table
     * @param tableToken         the table token
     * @param configuration      the Cairo configuration
     * @param txnLogMemory       the memory to iterate over transaction logs
     * @param walEventReader     the reader to read WAL events
     * @param blockFileReader    the reader to read state file
     * @param matViewStateReader the POD to read materialized view state into
     * @return -1 if the transaction could not be extracted, -2 if the last WAL-E entry is an invalidation commit (MAT_VIEW_INVALIDATE),
     * or a transaction number greater than -1 if it is a valid last refresh transaction
     */
    public static long getMatViewLastRefreshBaseTxn(
            Path tablePath,
            TableToken tableToken,
            CairoConfiguration configuration,
            MemoryCMR txnLogMemory,
            WalEventReader walEventReader,
            BlockFileReader blockFileReader,
            MatViewStateReader matViewStateReader
    ) {
        long txnNotFound = -1;
        long txnInvalid = -2;
        try (MemoryCMR mem = txnLogMemory) {
            int tablePathLen = tablePath.size();
            mem.smallFile(configuration.getFilesFacade(), tablePath.concat(SEQ_DIR).concat(TXNLOG_FILE_NAME).$(), MemoryTag.MMAP_TX_LOG);
            long txnCount = mem.getLong(TableTransactionLogFile.MAX_TXN_OFFSET_64);
            if (txnCount > 0) {
                long fileSize = TableTransactionLogFile.HEADER_SIZE + txnCount * TableTransactionLogV1.RECORD_SIZE;
                if (mem.size() >= fileSize) {
                    for (long txn = txnCount - 1; txn >= 0; txn--) {
                        tablePath.trimTo(tablePathLen);
                        long offset = TableTransactionLogFile.HEADER_SIZE + txn * TableTransactionLogV1.RECORD_SIZE;
                        final int walId = mem.getInt(offset + TableTransactionLogFile.TX_LOG_WAL_ID_OFFSET);
                        final int segmentId = mem.getInt(offset + TableTransactionLogFile.TX_LOG_SEGMENT_OFFSET);
                        final int segmentTxn = mem.getInt(offset + TableTransactionLogFile.TX_LOG_SEGMENT_TXN_OFFSET);
                        if (walId > 0) {
                            tablePath.concat(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                            try (WalEventReader eventReader = walEventReader) {
                                WalEventCursor walEventCursor = eventReader.of(tablePath, segmentTxn);
                                if (walEventCursor.getType() == MAT_VIEW_DATA) {
                                    return walEventCursor.getDataInfoExt().getLastRefreshBaseTableTxn();
                                }
                                if (walEventCursor.getType() == MAT_VIEW_INVALIDATE) {
                                    return txnInvalid;
                                }
                            } catch (Throwable th) {
                                // walEventReader may not be able to find/open the WAL-e files
                                try (BlockFileReader blockReader = blockFileReader) {
                                    tablePath.trimTo(tablePathLen).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME);
                                    blockReader.of(tablePath.$());
                                    matViewStateReader.of(blockReader, tableToken);
                                    // read from state file if exists
                                    if (!matViewStateReader.isInvalid()) {
                                        return matViewStateReader.getLastRefreshBaseTxn();
                                    }
                                } catch (Throwable th2) {
                                }
                                return txnNotFound;
                            }
                        }
                    }
                }
            }
        }
        return txnNotFound;
    }
}
