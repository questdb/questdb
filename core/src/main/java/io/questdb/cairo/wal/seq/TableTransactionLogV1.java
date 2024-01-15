/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.MemorySerializer;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.TableUtils.openSmallFile;
import static io.questdb.cairo.wal.WalUtils.TXNLOG_FILE_NAME;
import static io.questdb.cairo.wal.WalUtils.WAL_FORMAT_VERSION_V1;

public class TableTransactionLogV1 extends TableTransactionLog {
    private final AtomicLong maxTxn = new AtomicLong();
    private final MemoryCMARW txnMem = Vm.getCMARWInstance();

    TableTransactionLogV1(FilesFacade ff) {
        super(ff);
    }

    public long addEntry(long structureVersion, int walId, int segmentId, int segmentTxn, long timestamp) {
        txnMem.putLong(structureVersion);
        txnMem.putInt(walId);
        txnMem.putInt(segmentId);
        txnMem.putInt(segmentTxn);
        txnMem.putLong(timestamp);

        Unsafe.getUnsafe().storeFence();
        long maxTxn = this.maxTxn.incrementAndGet();
        txnMem.putLong(MAX_TXN_OFFSET, maxTxn);
        txnMem.sync(false);
        // Transactions are 1 based here
        return maxTxn;
    }

    @Override
    public void beginMetadataChangeEntry(long newStructureVersion, MemorySerializer serializer, Object instance, long timestamp) {
        txnMem.putLong(newStructureVersion);
        txnMem.putInt(STRUCTURAL_CHANGE_WAL_ID);
        txnMem.putInt(-1);
        txnMem.putInt(-1);
        txnMem.putLong(timestamp);

        super.beginMetadataChangeEntry(newStructureVersion, serializer, instance, timestamp);
    }

    @Override
    public void close() {
        txnMem.close(false);
        super.close();
    }

    public void create(Path path, long tableCreateTimestamp) {
        createTxnFile(path, tableCreateTimestamp);
        creatMetaMem(path, tableCreateTimestamp);
    }

    public boolean isDropped() {
        long lastTxn = maxTxn.get();
        if (lastTxn > 0) {
            return WalUtils.DROP_TABLE_WALID == txnMem.getInt(HEADER_SIZE + (lastTxn - 1) * RECORD_SIZE + TX_LOG_WAL_ID_OFFSET);
        }
        return false;
    }

    public long lastTxn() {
        return maxTxn.get();
    }

    @Override
    public void open(Path path) {
        this.rootPath.clear();
        path.toSink(this.rootPath);

        if (!txnMem.isOpen()) {
            txnMem.close(false);
            openSmallFile(ff, path, path.size(), txnMem, TXNLOG_FILE_NAME, MemoryTag.MMAP_TX_LOG);
            openMetadataFiles(path);
        }

        long lastTxn = txnMem.getLong(MAX_TXN_OFFSET);
        maxTxn.set(lastTxn);

        txnMem.jumpTo(HEADER_SIZE);

        long maxStructureVersion = txnMem.getLong(HEADER_SIZE + (lastTxn - 1) * RECORD_SIZE + TX_LOG_STRUCTURE_VERSION_OFFSET);
        txnMem.jumpTo(HEADER_SIZE + lastTxn * RECORD_SIZE);
        openMeta(lastTxn, maxStructureVersion);
    }

    public boolean reload(Path path) {
        txnMem.close(false);
        openSmallFile(ff, path, path.size(), txnMem, TXNLOG_FILE_NAME, MemoryTag.MMAP_TX_LOG);
        return super.reloadMetadata(path);
    }

    public void sync() {
        txnMem.sync(false);
        syncMetadata();
    }

    private void createTxnFile(Path path, long tableCreateTimestamp) {
        final int pathLength = path.size();
        openSmallFile(ff, path, pathLength, txnMem, TXNLOG_FILE_NAME, MemoryTag.MMAP_TX_LOG);

        txnMem.jumpTo(0L);
        txnMem.putInt(WAL_FORMAT_VERSION_V1);
        txnMem.putLong(0L);
        txnMem.putLong(tableCreateTimestamp);
        txnMem.putInt(0);
        txnMem.sync(false);
    }

    long endMetadataChangeEntry() {
        syncMetadata();

        Unsafe.getUnsafe().storeFence();

        // Transactions are 1 based here
        long nextTxn = maxTxn.incrementAndGet();
        txnMem.putLong(MAX_TXN_OFFSET, nextTxn);

        return nextTxn;
    }
}
