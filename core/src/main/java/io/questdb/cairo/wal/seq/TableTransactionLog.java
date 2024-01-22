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
import static io.questdb.cairo.wal.WalUtils.TXNLOG_FILE_NAME_META_INX;
import static io.questdb.cairo.wal.WalUtils.TXNLOG_FILE_NAME_META_VAR;

public class TableTransactionLog implements Closeable {
    private static final Log LOG = LogFactory.getLog(TableTransactionLog.class);
    private static final ThreadLocal<AlterOperation> tlAlterOperation = new ThreadLocal<>();
    private static final ThreadLocal<TableMetadataChangeLogImpl> tlStructChangeCursor = new ThreadLocal<>();
    private final int defaultChunkSize;
    private final FilesFacade ff;
    private final AtomicLong maxMetadataVersion = new AtomicLong();
    private final StringSink rootPath = new StringSink();
    private final MemoryCMARW txnMetaMem = Vm.getCMARWInstance();
    private final MemoryCMARW txnMetaMemIndex = Vm.getCMARWInstance();
    private final TableTransactionLogFile txnLogFile;

    TableTransactionLog(FilesFacade ff, int defaultChunkSize) {
        this.ff = ff;
        this.defaultChunkSize = defaultChunkSize;
        this.txnLogFile = new TableTransactionLogV1(ff);
    }

    @Override
    public void close() {
        txnLogFile.close();
        txnMetaMem.close(false);
        txnMetaMemIndex.close(false);
    }

    public boolean reload(Path path) {
        close();
        open(path);
        return true;
    }

    public void sync() {
        txnMetaMemIndex.sync(false);
        txnMetaMem.sync(false);
        txnLogFile.sync();
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
        return txnLogFile.addEntry(structureVersion, walId, segmentId, segmentTxn, timestamp);
    }

    void beginMetadataChangeEntry(long newStructureVersion, MemorySerializer serializer, Object instance, long timestamp) {
        assert newStructureVersion == txnMetaMemIndex.getAppendOffset() / Long.BYTES;
        txnLogFile.beginMetadataChangeEntry(newStructureVersion, serializer, instance, timestamp);

        txnMetaMem.putInt(0);
        long varMemBegin = txnMetaMem.getAppendOffset();
        serializer.toSink(instance, txnMetaMem);
        int len = (int) (txnMetaMem.getAppendOffset() - varMemBegin);
        txnMetaMem.putInt(varMemBegin - Integer.BYTES, len);
        txnMetaMemIndex.putLong(varMemBegin + len);
    }

    void create(Path path, long tableCreateTimestamp) {
        assert defaultChunkSize < 1;

        txnLogFile.create(path, tableCreateTimestamp);
        openFiles(path);

        txnMetaMem.jumpTo(0L);
        txnMetaMem.sync(false); // empty

        txnMetaMemIndex.jumpTo(0L);
        txnMetaMemIndex.putLong(0L); // N + 1, first entry is 0.
        txnMetaMemIndex.sync(false);
    }

    long endMetadataChangeEntry() {
        sync();

        Unsafe.getUnsafe().storeFence();

        long txn = txnLogFile.endMetadataChangeEntry();
        maxMetadataVersion.incrementAndGet();
        return txn;
    }

    TransactionLogCursor getCursor(long txnLo) {
        return txnLogFile.getCursor(txnLo, Path.getThreadLocal(rootPath));
    }

    @NotNull
    TableMetadataChangeLog getTableMetadataChangeLog(long structureVersionLo, MemorySerializer serializer) {
        final TableMetadataChangeLogImpl cursor = (TableMetadataChangeLogImpl) getTableMetadataChangeLog();
        cursor.of(ff, structureVersionLo, serializer, Path.getThreadLocal(rootPath), maxMetadataVersion.get());
        return cursor;
    }

    boolean isDropped() {
        return txnLogFile.isDropped();
    }

    long lastTxn() {
        return txnLogFile.lastTxn();
    }

    void open(Path path) {
        this.rootPath.clear();
        path.toSink(this.rootPath);

        long maxStructureVersion = txnLogFile.open(path);

        if (!txnMetaMem.isOpen()) {
            openFiles(path);
        }
        maxMetadataVersion.set(maxStructureVersion);
        long structureAppendOffset = maxStructureVersion * Long.BYTES;
        long txnMetaMemSize = txnMetaMemIndex.getLong(structureAppendOffset);
        txnMetaMemIndex.jumpTo(structureAppendOffset + Long.BYTES);
        txnMetaMem.jumpTo(txnMetaMemSize);
    }

    void openFiles(Path path) {
        final int pathLength = path.size();
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
                @Transient final Path path,
                long maxStructureVersion
        ) {
            // deallocates current state
            close();

            this.ff = ff;
            this.serializer = serializer;

            int txnMetaFd = -1;
            int txnMetaIndexFd = -1;
            try {
                if (maxStructureVersion > structureVersionLo) {
                    txnMetaFd = openFileRO(ff, path, TXNLOG_FILE_NAME_META_VAR);
                    txnMetaIndexFd = openFileRO(ff, path, TXNLOG_FILE_NAME_META_INX);
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

                throw CairoException.critical(0).put("expected to read table structure changes but there is no saved in the sequencer [structureVersionLo=").put(structureVersionLo).put(']');
            } finally {
                ff.close(txnMetaFd);
                ff.close(txnMetaIndexFd);
            }
        }
    }
}
