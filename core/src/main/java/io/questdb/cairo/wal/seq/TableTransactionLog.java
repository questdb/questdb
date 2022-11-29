/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.wal.WalUtils.WAL_FORMAT_VERSION;

public class TableTransactionLog implements Closeable {
    public final static int HEADER_RESERVED = 8 * Long.BYTES;
    public static final long MAX_TXN_OFFSET = Integer.BYTES;
    public static final long MAX_STRUCTURE_VERSION_OFFSET = MAX_TXN_OFFSET + Long.BYTES;
    public static final long TXN_META_SIZE_OFFSET = MAX_STRUCTURE_VERSION_OFFSET + Long.BYTES;
    public static final long HEADER_SIZE = TXN_META_SIZE_OFFSET + Long.BYTES + HEADER_RESERVED;
    public final static int RECORD_RESERVED = 15 * Integer.BYTES;
    public static final int RECORD_SIZE = Integer.BYTES + Integer.BYTES + Long.BYTES + RECORD_RESERVED;
    public static final int STRUCTURAL_CHANGE_WAL_ID = -1;
    private static final Log LOG = LogFactory.getLog(TableTransactionLog.class);
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
            assert maxTxnInFile == maxTxn.get() : "Max txn in the file " + maxTxnInFile + " but in memory is " + maxTxn.get();
            txnMem.close(false);
            txnMetaMem.close(false);
            txnMetaMemIndex.close(false);
        }
    }

    private static long openFileRO(final FilesFacade ff, final Path path, final String fileName) {
        final int rootLen = path.length();
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

    long addEntry(int walId, int segmentId, long segmentTxn) {
        txnMem.putInt(walId);
        txnMem.putInt(segmentId);
        txnMem.putLong(segmentTxn);
        txnMem.jumpTo(txnMem.getAppendOffset() + RECORD_RESERVED);

        Unsafe.getUnsafe().storeFence();
        txnMem.putLong(MAX_TXN_OFFSET, maxTxn.incrementAndGet());
        // Transactions are 1 based here
        return maxTxn.get();
    }

    long beginMetadataChangeEntry(long newStructureVersion, MemorySerializer serializer, Object instance) {
        assert newStructureVersion == txnMetaMemIndex.getAppendOffset() / Long.BYTES;

        txnMem.putInt(STRUCTURAL_CHANGE_WAL_ID);
        txnMem.putLong(newStructureVersion);
        txnMem.jumpTo((maxTxn.get() + 1) * RECORD_SIZE + HEADER_SIZE);

        txnMetaMem.putInt(0);
        long varMemBegin = txnMetaMem.getAppendOffset();
        serializer.toSink(instance, txnMetaMem);
        int len = (int) (txnMetaMem.getAppendOffset() - varMemBegin);
        txnMetaMem.putInt(varMemBegin - Integer.BYTES, len);
        txnMetaMemIndex.putLong(varMemBegin + len);

        return varMemBegin + len;
    }

    long endMetadataChangeEntry(long newStructureVersion, long offset) {
        Unsafe.getUnsafe().storeFence();
        txnMem.putLong(MAX_TXN_OFFSET, maxTxn.incrementAndGet());
        txnMem.putLong(MAX_STRUCTURE_VERSION_OFFSET, newStructureVersion);
        txnMem.putLong(TXN_META_SIZE_OFFSET, offset);

        // Transactions are 1 based here
        return maxTxn.get();
    }

    TransactionLogCursor getCursor(long txnLo) {
        final Path path = Path.PATH.get().of(rootPath);
        TransactionLogCursorImpl cursor = tlTransactionLogCursor.get();
        if (cursor == null) {
            cursor = new TransactionLogCursorImpl(ff, txnLo, path);
            tlTransactionLogCursor.set(cursor);
            return cursor;
        }
        return cursor.of(ff, txnLo, path);
    }

    @NotNull
    TableMetadataChangeLog getTableMetadataChangeLog(long structureVersionLo, MemorySerializer serializer) {
        final TableMetadataChangeLogImpl cursor = (TableMetadataChangeLogImpl) getTableMetadataChangeLog();
        cursor.of(ff, structureVersionLo, serializer, Path.getThreadLocal(rootPath));
        return cursor;
    }

    long lastTxn() {
        return maxTxn.get();
    }

    void open(Path path) {
        this.rootPath.clear();
        path.toSink(this.rootPath);

        final int pathLength = path.length();
        openSmallFile(ff, path, pathLength, txnMem, TXNLOG_FILE_NAME, MemoryTag.MMAP_TX_LOG);
        openSmallFile(ff, path, pathLength, txnMetaMem, TXNLOG_FILE_NAME_META_VAR, MemoryTag.MMAP_TX_LOG);
        openSmallFile(ff, path, pathLength, txnMetaMemIndex, TXNLOG_FILE_NAME_META_INX, MemoryTag.MMAP_TX_LOG);

        maxTxn.set(txnMem.getLong(MAX_TXN_OFFSET));
        long maxStructureVersion = txnMem.getLong(MAX_STRUCTURE_VERSION_OFFSET);
        long txnMetaMemSize = txnMem.getLong(TXN_META_SIZE_OFFSET);

        if (maxTxn.get() == 0) {
            txnMem.jumpTo(0L);
            txnMem.putInt(WAL_FORMAT_VERSION);
            txnMem.putLong(0L);
            txnMem.putLong(0L);
            txnMem.jumpTo(HEADER_SIZE);

            txnMetaMemIndex.jumpTo(0L);
            txnMetaMemIndex.putLong(0L); // N + 1, first entry is 0.
            txnMetaMem.jumpTo(0L);
        } else {
            txnMem.jumpTo(HEADER_SIZE + maxTxn.get() * RECORD_SIZE);
            long structureAppendOffset = (maxStructureVersion + 1) * Long.BYTES;
            txnMetaMemIndex.jumpTo(structureAppendOffset);
            txnMetaMem.jumpTo(txnMetaMemSize);
        }
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
                throw CairoException.critical(0).put("Invalid sequencer txn metadata [offset=").put(txnMetaOffset).put(", recordSize=").put(recordSize).put(']');
            }
            txnMetaOffset += Integer.BYTES;
            serializer.fromSink(alterOp, txnMetaMem, txnMetaOffset);
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

            long fdTxn = -1;
            long fdTxnMeta = -1;
            long fdTxnMetaIndex = -1;
            try {
                fdTxn = openFileRO(ff, path, TXNLOG_FILE_NAME);
                fdTxnMeta = openFileRO(ff, path, TXNLOG_FILE_NAME_META_VAR);
                fdTxnMetaIndex = openFileRO(ff, path, TXNLOG_FILE_NAME_META_INX);

                txnMetaOffset = ff.readNonNegativeLong(fdTxnMetaIndex, structureVersionLo * Long.BYTES);
                if (txnMetaOffset > -1L) {
                    txnMetaOffsetHi = ff.readNonNegativeLong(fdTxn, TXN_META_SIZE_OFFSET);
                    if (txnMetaOffsetHi > txnMetaOffset) {
                        txnMetaAddress = ff.mmap(
                                fdTxnMeta,
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
                } else {
                    throw CairoException.critical(0).put("expected to read table structure changes but there is no saved in the sequencer [structureVersionLo=").put(structureVersionLo).put(']');
                }
            } finally {
                if (fdTxn > -1) {
                    ff.close(fdTxn);
                }
                if (fdTxnMeta > -1) {
                    ff.close(fdTxnMeta);
                }
                if (fdTxnMetaIndex > -1) {
                    ff.close(fdTxnMetaIndex);
                }
            }
            // Set empty. This is not an error, it just means that there are no changes.
            txnMetaOffset = txnMetaOffsetHi = 0;
        }
    }

    private static class TransactionLogCursorImpl implements TransactionLogCursor {
        private static final long WAL_ID_OFFSET = 0;
        private static final long SEGMENT_ID_OFFSET = WAL_ID_OFFSET + Integer.BYTES;
        private static final long SEGMENT_TXN_OFFSET = SEGMENT_ID_OFFSET + Integer.BYTES;
        private long address;
        private long fd;
        private FilesFacade ff;
        private long txn;
        private long txnCount;
        private long txnOffset;

        public TransactionLogCursorImpl(FilesFacade ff, long txnLo, final Path path) {
            of(ff, txnLo, path);
        }

        @Override
        public void close() {
            if (fd > -1) {
                ff.close(fd);
            }
            ff.munmap(address, getMappedLen(), MemoryTag.NATIVE_DEFAULT);
        }

        @Override
        public int getSegmentId() {
            return Unsafe.getUnsafe().getInt(address + txnOffset + SEGMENT_ID_OFFSET);
        }

        @Override
        public long getSegmentTxn() {
            return Unsafe.getUnsafe().getLong(address + txnOffset + SEGMENT_TXN_OFFSET);
        }

        @Override
        public long getTxn() {
            return txn;
        }

        @Override
        public int getWalId() {
            return Unsafe.getUnsafe().getInt(address + txnOffset + WAL_ID_OFFSET);
        }

        @Override
        public boolean hasNext() {
            if (hasNext(getMappedLen())) {
                return true;
            }

            final long newTxnCount = ff.readNonNegativeLong(fd, MAX_TXN_OFFSET);
            if (newTxnCount > txnCount) {
                final long oldSize = getMappedLen();
                txnCount = newTxnCount;
                final long newSize = getMappedLen();
                address = ff.mremap(fd, address, oldSize, newSize, 0, Files.MAP_RO, MemoryTag.NATIVE_DEFAULT);

                return hasNext(newSize);
            }
            return false;
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
            this.txnCount = ff.readNonNegativeLong(fd, MAX_TXN_OFFSET);
            if (txnCount > -1L) {
                this.address = ff.mmap(fd, getMappedLen(), 0, Files.MAP_RO, MemoryTag.NATIVE_DEFAULT);
                this.txnOffset = HEADER_SIZE + (txnLo - 1) * RECORD_SIZE;
            }
            txn = txnLo;
            return this;
        }
    }
}
