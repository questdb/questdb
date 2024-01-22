///*******************************************************************************
// *     ___                  _   ____  ____
// *    / _ \ _   _  ___  ___| |_|  _ \| __ )
// *   | | | | | | |/ _ \/ __| __| | | |  _ \
// *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
// *    \__\_\\__,_|\___||___/\__|____/|____/
// *
// *  Copyright (c) 2014-2019 Appsicle
// *  Copyright (c) 2019-2023 QuestDB
// *
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *
// *  http://www.apache.org/licenses/LICENSE-2.0
// *
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// *
// ******************************************************************************/
//
//package io.questdb.cairo.wal.seq;
//
//import io.questdb.cairo.CairoConfiguration;
//import io.questdb.cairo.CairoException;
//import io.questdb.cairo.vm.Vm;
//import io.questdb.cairo.vm.api.MemoryCMARW;
//import io.questdb.cairo.wal.WalUtils;
//import io.questdb.std.FilesFacade;
//import io.questdb.std.MemoryTag;
//import io.questdb.std.str.Path;
//
//import java.util.concurrent.atomic.AtomicLong;
//
//import static io.questdb.cairo.TableUtils.openSmallFile;
//import static io.questdb.cairo.wal.WalUtils.*;
//
//public class TableTransactionLogV2 extends TableTransactionLog {
//    private final AtomicLong maxTxn = new AtomicLong();
//    private final MemoryCMARW txnChunkMem = Vm.getCMARWInstance();
//    private final MemoryCMARW txnMem = Vm.getCMARWInstance();
//    private int txnChunkTransactionCount;
//
//    TableTransactionLogV2(FilesFacade ff) {
//        super(ff);
//    }
//
//    @Override
//    public long addEntry(long structureVersion, int walId, int segmentId, int segmentTxn, long timestamp) {
//        return 0;
//    }
//
//    @Override
//    public void close() {
//        txnMem.close();
//        super.close();
//    }
//
//    public void create(Path path, long tableCreateTimestamp, int txnChunkTransactionCount) {
//        createTxnFile(path, tableCreateTimestamp);
//        jumpToTxnChunk(0);
//        creatMetaMem(path, tableCreateTimestamp);
//    }
//
//    @Override
//    public long endMetadataChangeEntry() {
//        return 0;
//    }
//
//    @Override
//    public boolean isDropped() {
//        long lastTxn = maxTxn.get();
//        if (lastTxn > 0) {
//            return WalUtils.DROP_TABLE_WALID == txnMem.getInt(HEADER_SIZE + (lastTxn - 1) * RECORD_SIZE + TX_LOG_WAL_ID_OFFSET);
//        }
//        return false;
//    }
//
//    @Override
//    public long lastTxn() {
//        return maxTxn.get();
//    }
//
//    @Override
//    public void open(Path path) {
//        this.rootPath.clear();
//        path.toSink(this.rootPath);
//
//        if (!txnMem.isOpen()) {
//            openTxnFile(path);
//            openMetadataFiles(path);
//        }
//
//        long formatVersion = txnMem.getLong(0);
//        if (formatVersion != WAL_FORMAT_VERSION_V2) {
//            throw CairoException.critical(0).put("invalid transaction log file version [expected=")
//                    .put(WAL_FORMAT_VERSION_V2).put(", actual=").put(formatVersion).put("]: ").put(path);
//        }
//        txnChunkTransactionCount = txnMem.getInt(TX_CHUNK_TRANSACTION_COUNT_OFFSET);
//        if (txnChunkTransactionCount == 0) {
//            throw CairoException.critical(0).put("invalid transaction log file, version is but the chunk is 0: ").put(path);
//        }
//
//        long lastTxn = txnMem.getLong(MAX_TXN_OFFSET);
//        maxTxn.set(lastTxn);
//
//        jumpToTxnChunk(lastTxn - 1);
//        long maxStructureVersion = txnChunkMem.getLong(getRecordChunkPosition(lastTxn - 1) + TX_LOG_STRUCTURE_VERSION_OFFSET);
//
//        jumpToTxnChunk(lastTxn);
//        openMeta(lastTxn, maxStructureVersion);
//    }
//
//    @Override
//    public boolean reload(Path path) {
//        txnMem.close(false);
//        txnChunkMem.close(false);
//        openSmallFile(ff, path, path.size(), txnMem, TXNLOG_FILE_NAME, MemoryTag.MMAP_TX_LOG);
//        return super.reloadMetadata(path);
//    }
//
//    public void sync() {
//        txnMem.sync(false);
//        syncMetadata();
//    }
//
//    private void createTxnFile(Path path, long tableCreateTimestamp) {
//        openTxnFile(path);
//        txnMem.jumpTo(0L);
//        txnMem.putInt(WAL_FORMAT_VERSION_V2);
//        txnMem.putLong(0L);
//        txnMem.putLong(tableCreateTimestamp);
//        txnMem.putInt(txnChunkTransactionCount);
//        txnMem.sync(false);
//    }
//
//    private long getRecordChunkPosition(long txn) {
//        return (txn % txnChunkTransactionCount) * RECORD_SIZE;
//    }
//
//    private void jumpToTxnChunk(long txn) {
//        long chunk = txn / txnChunkTransactionCount;
//    }
//
//    private void openTxnFile(Path path) {
//        final int pathLength = path.size();
//        try {
//            txnMem.of(ff, path.concat(TXNLOG_FILE_NAME).$(), HEADER_SIZE, CairoConfiguration.O_NONE, MemoryTag.MMAP_TX_LOG);
//        } finally {
//            path.trimTo(pathLength);
//        }
//    }
//}
