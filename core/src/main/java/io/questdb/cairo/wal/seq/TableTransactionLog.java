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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.MemorySerializer;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.MemoryFCRImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.lang.ThreadLocal;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.TableUtils.openSmallFile;
import static io.questdb.cairo.wal.WalUtils.*;

public class TableTransactionLog implements Closeable {
    public final static int HEADER_RESERVED = 7 * Long.BYTES;
    public static final long MAX_TXN_OFFSET = Integer.BYTES;
    public static final int STRUCTURAL_CHANGE_WAL_ID = -1;
    public static final long TABLE_CREATE_TIMESTAMP_OFFSET = MAX_TXN_OFFSET + Long.BYTES;
    public static final long HEADER_SIZE = TABLE_CREATE_TIMESTAMP_OFFSET + Long.BYTES + HEADER_RESERVED;
    private static final Log LOG = LogFactory.getLog(TableTransactionLog.class);
    private static final long TX_LOG_STRUCTURE_VERSION_OFFSET = 0L;
    private static final long TX_LOG_WAL_ID_OFFSET = TX_LOG_STRUCTURE_VERSION_OFFSET + Long.BYTES;
    private static final long TX_LOG_SEGMENT_OFFSET = TX_LOG_WAL_ID_OFFSET + Integer.BYTES;
    private static final long TX_LOG_SEGMENT_TXN_OFFSET = TX_LOG_SEGMENT_OFFSET + Integer.BYTES;
    private static final long TX_LOG_COMMIT_TIMESTAMP_OFFSET = TX_LOG_SEGMENT_TXN_OFFSET + Integer.BYTES;
    public static final long RECORD_SIZE = TX_LOG_COMMIT_TIMESTAMP_OFFSET + Long.BYTES;
    private static final ThreadLocal<AlterOperation> tlAlterOperation = new ThreadLocal<>();
    private static final ThreadLocal<TableMetadataChangeLogImpl> tlStructChangeCursor = new ThreadLocal<>();
    private static final ThreadLocal<TransactionLogCursorImpl> tlTransactionLogCursor = new ThreadLocal<>();
    private final FilesFacade ff;
    private final AtomicLong maxTxn = new AtomicLong();
    private final StringSink rootPath = new StringSink();
    private final MemoryCMARW txnMem = Vm.getCMARWInstance();
    private final MemoryCMARW txnMetaMem = Vm.getCMARWInstance();
    private final MemoryCMARW txnMetaMemIndex = Vm.getCMARWInstance();

    TableTransactionLog(FilesFacade ff) {
        this.ff = ff;
    }

    @Override
    public void close() {
        if (txnMem.isOpen()) {
            long maxTxnInFile = txnMem.getLong(MAX_TXN_OFFSET);
            if (maxTxnInFile != maxTxn.get()) {
                LOG.error().$("Max txn in the file ").$(maxTxnInFile).$(" but in memory is ").$(maxTxn.get()).$();
            }
            txnMem.close(false);
            txnMetaMem.close(false);
            txnMetaMemIndex.close(false);
        }
    }

    public boolean reload(Path path) {
        txnMem.close(false);
        txnMetaMem.close(false);
        txnMetaMemIndex.close(false);
        open(path);
        return true;
    }

    public void sync() {
        txnMetaMemIndex.sync(false);
        txnMetaMem.sync(false);
        txnMem.sync(false);
    }

    private static int openFileRO(final FilesFacade ff, final Path path, final String fileName) {
        final int rootLen = path.size();
        path.concat(fileName).$();
        try {
            return TableUtils.openRO(ff, path, LOG);
        } finally {
            path.trimTo(rootLen);
        }
    }

    @NotNull
    static TableMetadataChangeLog getTableMetadataChangeLog() {
        TableMetadataChangeLogImpl instance = tlStructChangeCursor.get();
        if (instance == null) {
            tlStructChangeCursor.set(instance = new TableMetadataChangeLogImpl());
        }
        return instance;
    }

    long addEntry(long structureVersion, int walId, int segmentId, int segmentTxn, long timestamp) {
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

    void beginMetadataChangeEntry(long newStructureVersion, MemorySerializer serializer, Object instance, long timestamp) {
        assert newStructureVersion == txnMetaMemIndex.getAppendOffset() / Long.BYTES;

        txnMem.putLong(newStructureVersion);
        txnMem.putInt(STRUCTURAL_CHANGE_WAL_ID);
        txnMem.putInt(-1);
        txnMem.putInt(-1);
        txnMem.putLong(timestamp);

        txnMetaMem.putInt(0);
        long varMemBegin = txnMetaMem.getAppendOffset();
        serializer.toSink(instance, txnMetaMem);
        int len = (int) (txnMetaMem.getAppendOffset() - varMemBegin);
        txnMetaMem.putInt(varMemBegin - Integer.BYTES, len);
        txnMetaMemIndex.putLong(varMemBegin + len);
    }

    void create(Path path, long tableCreateTimestamp) {

        openFiles(path);

        txnMem.jumpTo(0L);
        txnMem.putInt(WAL_FORMAT_VERSION);
        txnMem.putLong(0L);
        txnMem.putLong(tableCreateTimestamp);
        txnMem.sync(false);

        txnMetaMem.jumpTo(0L);
        txnMetaMem.sync(false); // empty

        txnMetaMemIndex.jumpTo(0L);
        txnMetaMemIndex.putLong(0L); // N + 1, first entry is 0.
        txnMetaMemIndex.sync(false);
    }

    long endMetadataChangeEntry() {
        sync();

        Unsafe.getUnsafe().storeFence();

        // Transactions are 1 based here
        long nextTxn = maxTxn.incrementAndGet();
        txnMem.putLong(MAX_TXN_OFFSET, nextTxn);

        return nextTxn;
    }

    TransactionLogCursor getCursor(long txnLo) {
        final Path path = Path.PATH.get().of(rootPath);
        TransactionLogCursorImpl cursor = tlTransactionLogCursor.get();
        if (cursor == null) {
            cursor = new TransactionLogCursorImpl(ff, txnLo, path);
            tlTransactionLogCursor.set(cursor);
            return cursor;
        }
        try {
            return cursor.of(ff, txnLo, path);
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @NotNull
    TableMetadataChangeLog getTableMetadataChangeLog(long structureVersionLo, MemorySerializer serializer) {
        final TableMetadataChangeLogImpl cursor = (TableMetadataChangeLogImpl) getTableMetadataChangeLog();
        cursor.of(ff, structureVersionLo, serializer, Path.getThreadLocal(rootPath));
        return cursor;
    }

    boolean isDropped() {
        long lastTxn = maxTxn.get();
        if (lastTxn > 0) {
            return WalUtils.DROP_TABLE_WALID == txnMem.getInt(HEADER_SIZE + (lastTxn - 1) * RECORD_SIZE + TX_LOG_WAL_ID_OFFSET);
        }
        return false;
    }

    long lastTxn() {
        return maxTxn.get();
    }

    void open(Path path) {
        this.rootPath.clear();
        path.toSink(this.rootPath);

        if (!txnMem.isOpen()) {
            openFiles(path);
        }

        long lastTxn = txnMem.getLong(MAX_TXN_OFFSET);
        maxTxn.set(lastTxn);

        txnMem.jumpTo(HEADER_SIZE);
        txnMetaMemIndex.jumpTo(8); // first entry is 0.
        txnMetaMem.jumpTo(0L);
        long maxStructureVersion = txnMem.getLong(HEADER_SIZE + (lastTxn - 1) * RECORD_SIZE + TX_LOG_STRUCTURE_VERSION_OFFSET);
        txnMem.jumpTo(HEADER_SIZE + lastTxn * RECORD_SIZE);
        long structureAppendOffset = maxStructureVersion * Long.BYTES;
        long txnMetaMemSize = txnMetaMemIndex.getLong(structureAppendOffset);
        txnMetaMemIndex.jumpTo(structureAppendOffset + Long.BYTES);
        txnMetaMem.jumpTo(txnMetaMemSize);
    }

    void openFiles(Path path) {
        final int pathLength = path.size();
        openSmallFile(ff, path, pathLength, txnMem, TXNLOG_FILE_NAME, MemoryTag.MMAP_TX_LOG);
        openSmallFile(ff, path, pathLength, txnMetaMem, TXNLOG_FILE_NAME_META_VAR, MemoryTag.MMAP_TX_LOG);
        openSmallFile(ff, path, pathLength, txnMetaMemIndex, TXNLOG_FILE_NAME_META_INX, MemoryTag.MMAP_TX_LOG);
    }

    AlterOperation readTableMetadataChangeLog(long structureVersion, MemorySerializer serializer) {
        long txnMetaOffset = txnMetaMemIndex.getLong(structureVersion * Long.BYTES);
        int recordSize = txnMetaMem.getInt(txnMetaOffset);
        if (recordSize < 0 || recordSize > Files.PAGE_SIZE) {
            throw CairoException.critical(0).put("invalid sequencer txn metadata [offset=").put(txnMetaOffset).put(", recordSize=").put(recordSize).put(']');
        }
        txnMetaOffset += Integer.BYTES;
        AlterOperation alterToDeserializeTo = tlAlterOperation.get();
        if (alterToDeserializeTo == null) {
            tlAlterOperation.set(alterToDeserializeTo = new AlterOperation());
        }
        serializer.fromSink(alterToDeserializeTo, txnMetaMem, txnMetaOffset, txnMetaOffset + recordSize);
        txnMetaMem.jumpTo(txnMetaOffset + recordSize);
        return alterToDeserializeTo;
    }

    private static class TableMetadataChangeLogImpl implements TableMetadataChangeLog {
        private final AlterOperation alterOp = new AlterOperation();
        private final MemoryFCRImpl txnMetaMem = new MemoryFCRImpl();
        private FilesFacade ff;
        private MemorySerializer serializer;
        private long txnMetaAddress;
        private long txnMetaOffset;
        private long txnMetaOffsetHi;

        @Override
        public void close() {
            if (txnMetaAddress > 0) {
                ff.munmap(txnMetaAddress, txnMetaOffsetHi, MemoryTag.MMAP_TX_LOG_CURSOR);
                txnMetaAddress = 0;
            }
            txnMetaOffset = 0;
            txnMetaOffsetHi = 0;
        }

        @Override
        public boolean hasNext() {
            return txnMetaOffset < txnMetaOffsetHi;
        }

        @Override
        public TableMetadataChange next() {
            int recordSize = txnMetaMem.getInt(txnMetaOffset);
            if (recordSize < 0 || recordSize > Files.PAGE_SIZE) {
                throw CairoException.critical(0).put("invalid sequencer txn metadata [offset=").put(txnMetaOffset).put(", recordSize=").put(recordSize).put(']');
            }
            txnMetaOffset += Integer.BYTES;
            serializer.fromSink(alterOp, txnMetaMem, txnMetaOffset, txnMetaOffset + recordSize);
            txnMetaOffset += recordSize;
            return alterOp;
        }

        public void of(
                FilesFacade ff,
                long structureVersionLo,
                MemorySerializer serializer,
                @Transient final Path path
        ) {
            // deallocates current state
            close();

            this.ff = ff;
            this.serializer = serializer;

            int txnFd = -1;
            int txnMetaFd = -1;
            int txnMetaIndexFd = -1;
            try {
                txnFd = openFileRO(ff, path, TXNLOG_FILE_NAME);
                txnMetaFd = openFileRO(ff, path, TXNLOG_FILE_NAME_META_VAR);
                txnMetaIndexFd = openFileRO(ff, path, TXNLOG_FILE_NAME_META_INX);
                long txnCount = ff.readNonNegativeLong(txnFd, MAX_TXN_OFFSET);
                if (txnCount > -1L) {
                    long maxStructureVersion = ff.readNonNegativeLong(txnFd, HEADER_SIZE + (txnCount - 1) * RECORD_SIZE + TX_LOG_STRUCTURE_VERSION_OFFSET);
                    if (maxStructureVersion > structureVersionLo) {
                        txnMetaOffset = ff.readNonNegativeLong(txnMetaIndexFd, structureVersionLo * Long.BYTES);
                        if (txnMetaOffset > -1L) {
                            txnMetaOffsetHi = ff.readNonNegativeLong(txnMetaIndexFd, maxStructureVersion * Long.BYTES);

                            if (txnMetaOffsetHi > txnMetaOffset) {
                                txnMetaAddress = ff.mmap(
                                        txnMetaFd,
                                        txnMetaOffsetHi,
                                        0L,
                                        Files.MAP_RO,
                                        MemoryTag.MMAP_TX_LOG_CURSOR
                                );
                                if (txnMetaAddress < 0) {
                                    txnMetaAddress = 0;
                                    close();
                                } else {
                                    txnMetaMem.of(txnMetaAddress, txnMetaOffsetHi);
                                    return;
                                }
                            }
                        }
                    } else {
                        // Set empty. This is not an error, it just means that there are no changes.
                        txnMetaOffset = txnMetaOffsetHi = 0;
                        return;
                    }
                }

                throw CairoException.critical(0).put("expected to read table structure changes but there is no saved in the sequencer [structureVersionLo=").put(structureVersionLo).put(']');
            } finally {
                ff.close(txnFd);
                ff.close(txnMetaFd);
                ff.close(txnMetaIndexFd);
            }
        }
    }

    private static class TransactionLogCursorImpl implements TransactionLogCursor {
        private long address;
        private int fd;
        private FilesFacade ff;
        private long txn;
        private long txnCount = -1;
        private long txnLo;
        private long txnOffset;

        public TransactionLogCursorImpl(FilesFacade ff, long txnLo, final Path path) {
            try {
                of(ff, txnLo, path);
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public void close() {
            if (fd > 0) {
                ff.close(fd);
            }
            if (txnCount > -1 && address > 0) {
                ff.munmap(address, getMappedLen(), MemoryTag.MMAP_TX_LOG_CURSOR);
                txnCount = 0;
                address = 0;
            }
        }

        @Override
        public long getCommitTimestamp() {
            return Unsafe.getUnsafe().getLong(address + txnOffset + TX_LOG_COMMIT_TIMESTAMP_OFFSET);
        }

        @Override
        public int getSegmentId() {
            return Unsafe.getUnsafe().getInt(address + txnOffset + TX_LOG_SEGMENT_OFFSET);
        }

        @Override
        public int getSegmentTxn() {
            return Unsafe.getUnsafe().getInt(address + txnOffset + TX_LOG_SEGMENT_TXN_OFFSET);
        }

        @Override
        public long getStructureVersion() {
            return Unsafe.getUnsafe().getLong(address + txnOffset + TX_LOG_STRUCTURE_VERSION_OFFSET);
        }

        @Override
        public long getTxn() {
            return txn;
        }

        @Override
        public int getWalId() {
            return Unsafe.getUnsafe().getInt(address + txnOffset + TX_LOG_WAL_ID_OFFSET);
        }

        @Override
        public boolean hasNext() {
            if (hasNext(getMappedLen())) {
                return true;
            }

            final long newTxnCount = ff.readNonNegativeLong(fd, MAX_TXN_OFFSET);
            if (newTxnCount > txnCount) {
                remap(newTxnCount);
                return hasNext(getMappedLen());
            }
            return false;
        }

        @Override
        public boolean setPosition() {
            final long newTxnCount = ff.readNonNegativeLong(fd, MAX_TXN_OFFSET);
            if (newTxnCount > txnCount) {
                remap(newTxnCount);

                this.txnLo = txn - 1;
                this.txnOffset -= RECORD_SIZE;
                return true;
            }
            return false;
        }

        @Override
        public void setPosition(long txn) {
            this.txnOffset = HEADER_SIZE + (txn - 1) * RECORD_SIZE;
            this.txn = txn;
        }

        @Override
        public void toTop() {
            if (txnCount > -1L) {
                this.txnOffset = HEADER_SIZE + (txnLo - 1) * RECORD_SIZE;
                this.txn = txnLo;
            }
        }

        private long getMappedLen() {
            return txnCount * RECORD_SIZE + HEADER_SIZE;
        }

        private boolean hasNext(long mappedLen) {
            if (txnOffset + 2 * RECORD_SIZE <= mappedLen) {
                txnOffset += RECORD_SIZE;
                txn++;
                return true;
            }
            return false;
        }

        @NotNull
        private TransactionLogCursorImpl of(FilesFacade ff, long txnLo, Path path) {
            this.ff = ff;
            this.fd = openFileRO(ff, path, TXNLOG_FILE_NAME);
            long newTxnCount = ff.readNonNegativeLong(fd, MAX_TXN_OFFSET);
            if (newTxnCount > -1L) {
                this.txnCount = newTxnCount;
                this.address = ff.mmap(fd, getMappedLen(), 0, Files.MAP_RO, MemoryTag.MMAP_TX_LOG_CURSOR);
                this.txnOffset = HEADER_SIZE + (txnLo - 1) * RECORD_SIZE;
            } else {
                throw CairoException.critical(ff.errno()).put("cannot read sequencer transactions [path=").put(path).put(']');
            }
            this.txnLo = txnLo;
            txn = txnLo;
            return this;
        }

        private void remap(long newTxnCount) {
            final long oldSize = getMappedLen();
            txnCount = newTxnCount;
            final long newSize = getMappedLen();
            address = ff.mremap(fd, address, oldSize, newSize, 0, Files.MAP_RO, MemoryTag.MMAP_TX_LOG_CURSOR);
        }
    }
}
