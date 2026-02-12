/*******************************************************************************
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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.MemorySerializer;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.TableUtils.openRO;
import static io.questdb.cairo.wal.WalUtils.*;

/**
 * This class is used to read/write transactions to the disk.
 * This is V2 implementation of the sequencer transaction log storage.
 * <p>
 * File table_dir\\txn_seq\\_txnlog stores the header to be compatible with V1
 * Header: 76 bytes
 * <p>
 * Transactions are stored in table_dir\\txn_seq\\_txn_parts\\{partId} files. {PartId} starts from 0.
 * Each part file contains a fixed number of transactions. The number of transactions per part
 * is defined by the header Partition Size value.
 * <p>
 * To read a transaction one has to open the file partition where it's located.
 * To calculate the partition one can use the following formula:
 * {@code (txn - 1) / partTransactionCount}
 * <p>
 * Header and record is described in {@link TableTransactionLogFile}
 * <p>
 * Transaction record: 60 bytes
 */
public class TableTransactionLogV2 implements TableTransactionLogFile {
    public static final long MIN_TIMESTAMP_OFFSET = TX_LOG_COMMIT_TIMESTAMP_OFFSET + Long.BYTES;
    public static final long MAX_TIMESTAMP_OFFSET = MIN_TIMESTAMP_OFFSET + Long.BYTES;
    public static final long ROW_COUNT_OFFSET = MAX_TIMESTAMP_OFFSET + Long.BYTES;
    public static final long RESERVED_OFFSET = ROW_COUNT_OFFSET + Long.BYTES;
    public static final long RECORD_SIZE = RESERVED_OFFSET + Long.BYTES;
    private static final Log LOG = LogFactory.getLog(TableTransactionLogV2.class);
    private static final ThreadLocal<TransactionLogCursorImpl> tlTransactionLogCursor = new ThreadLocal<>();
    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final AtomicLong maxTxn = new AtomicLong();
    private final Path rootPath;
    private final MemoryCMARW txnMem = Vm.getCMARWInstance();
    private final MemoryCMARW txnPartMem = Vm.getCMARWInstance();
    private long partId = -1;
    private int partTransactionCount;

    public TableTransactionLogV2(CairoConfiguration configuration, int seqPartTransactionCount) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        this.partTransactionCount = seqPartTransactionCount;
        rootPath = new Path();
    }

    public static long readMaxStructureVersion(Path path, long logFileFd, FilesFacade ff) {
        long lastTxn = ff.readNonNegativeLong(logFileFd, TableTransactionLogFile.MAX_TXN_OFFSET_64);
        if (lastTxn < 0) {
            return -1;
        }
        int partTransactionCount = ff.readNonNegativeInt(logFileFd, HEADER_SEQ_PART_SIZE_32);
        if (partTransactionCount < 1) {
            return -1;
        }

        // Open last part and read last structure version
        if (lastTxn > 0L) {
            long prevTxn = lastTxn - 1;
            long part = prevTxn / partTransactionCount;
            int size = path.size();
            path.concat(TXNLOG_PARTS_DIR).slash().put(part);
            long partFd = -1;
            try {
                partFd = openRO(ff, path.$(), LOG);
                long fileReadOffset = (prevTxn % partTransactionCount) * RECORD_SIZE + TX_LOG_STRUCTURE_VERSION_OFFSET;
                return ff.readNonNegativeLong(partFd, fileReadOffset);
            } finally {
                if (partFd > -1) {
                    ff.close(partFd);
                }
                path.trimTo(size);
            }
        }
        return 0;
    }

    @Override
    public long addEntry(long structureVersion, int walId, int segmentId, int segmentTxn, long timestamp, long txnMinTimestamp, long txnMaxTimestamp, long txnRowCount) {
        openTxnPart();

        txnPartMem.putLong(structureVersion);
        txnPartMem.putInt(walId);
        txnPartMem.putInt(segmentId);
        txnPartMem.putInt(segmentTxn);
        txnPartMem.putLong(timestamp);
        txnPartMem.putLong(txnMinTimestamp);
        txnPartMem.putLong(txnMaxTimestamp);
        txnPartMem.putLong(txnRowCount);
        txnPartMem.putLong(0L);

        Unsafe.getUnsafe().storeFence();
        long maxTxn = this.maxTxn.incrementAndGet();
        txnMem.putLong(MAX_TXN_OFFSET_64, maxTxn);
        sync0();
        // Transactions are 1 based here
        return maxTxn;
    }

    @Override
    public void beginMetadataChangeEntry(long newStructureVersion, MemorySerializer serializer, Object instance, long timestamp) {
        openTxnPart();

        txnPartMem.putLong(newStructureVersion);
        txnPartMem.putInt(STRUCTURAL_CHANGE_WAL_ID);
        txnPartMem.putInt(-1);
        txnPartMem.putInt(-1);
        txnPartMem.putLong(timestamp);
        txnPartMem.putLong(serializer.getCommandType(instance));
        txnPartMem.putLong(0L);
        txnPartMem.putLong(0L);
        txnPartMem.putLong(0L);
    }

    @Override
    public void close() {
        if (txnMem.isOpen()) {
            long maxTxnInFile = txnMem.getLong(MAX_TXN_OFFSET_64);
            if (maxTxnInFile != maxTxn.get()) {
                LOG.info().$("Max txn in the file ").$(maxTxnInFile).$(" but in memory is ").$(maxTxn.get()).$();
            }
        }
        txnMem.close(false);
        txnPartMem.close(false);
        rootPath.close();
    }

    @Override
    public void create(Path path, long tableCreateTimestamp) {
        createTxnFile(path, tableCreateTimestamp);
        createPartsDir(configuration.getMkDirMode());
    }

    @Override
    public long endMetadataChangeEntry() {
        // Transactions are 1 based here
        long nextTxn = maxTxn.incrementAndGet();
        txnMem.putLong(MAX_TXN_OFFSET_64, nextTxn);
        return nextTxn;
    }

    @Override
    public void fullSync() {
        txnMem.sync(false);
    }

    @Override
    public TransactionLogCursor getCursor(long txnLo, @Transient Path path) {
        TransactionLogCursorImpl cursor = tlTransactionLogCursor.get();
        if (cursor == null) {
            cursor = new TransactionLogCursorImpl(ff, txnLo, path, partTransactionCount);
            tlTransactionLogCursor.set(cursor);
            return cursor;
        }
        try {
            return cursor.of(ff, txnLo, path, partTransactionCount);
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public boolean isDropped() {
        long lastTxn = maxTxn.get();
        if (lastTxn > 0) {
            long prevTxn = lastTxn - 1;
            openTxnPart(prevTxn);
            long lastPartTxn = (prevTxn) % partTransactionCount;
            return WalUtils.DROP_TABLE_WAL_ID == txnPartMem.getLong(lastPartTxn * RECORD_SIZE + TX_LOG_WAL_ID_OFFSET);
        }
        return false;
    }

    @Override
    public long lastTxn() {
        return maxTxn.get();
    }

    @Override
    public long open(Path path) {
        if (!txnMem.isOpen()) {
            txnMem.close(false);
            openTxnMem(path);
        }

        long lastTxn = txnMem.getLong(MAX_TXN_OFFSET_64);
        maxTxn.set(lastTxn);
        partTransactionCount = txnMem.getInt(HEADER_SEQ_PART_SIZE_32);
        if (partTransactionCount < 1) {
            throw new CairoException().put("invalid sequencer file part size [size=").put(partTransactionCount).put(", path=").put(path).put(']');
        }

        long maxStructureVersion = 0L;
        if (lastTxn > 0L) {
            long prevTxn = lastTxn - 1;
            openTxnPart(prevTxn);
            long lastPartTxn = (prevTxn) % partTransactionCount;
            maxStructureVersion = txnPartMem.getLong(lastPartTxn * RECORD_SIZE + TX_LOG_STRUCTURE_VERSION_OFFSET);
        }
        openTxnPart();
        // Open part can leave prev txn append position when part is the same
        setAppendPosition();
        return maxStructureVersion;
    }

    private void createPartsDir(int mkDirMode) {
        int rootLen = rootPath.size();
        rootPath.concat(TXNLOG_PARTS_DIR);
        try {
            if (!ff.exists(rootPath.$()) && ff.mkdir(rootPath.$(), mkDirMode) != 0) {
                throw CairoException.critical(ff.errno()).put("could not create directory [path='").put(rootPath).put("']");
            }
        } finally {
            rootPath.trimTo(rootLen);
        }
    }

    private void createTxnFile(Path path, long tableCreateTimestamp) {
        openTxnMem(path);

        txnMem.jumpTo(0L);
        txnMem.putInt(WAL_SEQUENCER_FORMAT_VERSION_V2);
        txnMem.putLong(0L);
        txnMem.putLong(tableCreateTimestamp);
        txnMem.putInt(partTransactionCount);
        sync0();
    }

    private void openTxnMem(Path path) {
        rootPath.of(path);
        int rootLen = rootPath.size();
        rootPath.concat(TXNLOG_FILE_NAME);
        try {
            txnMem.of(ff, rootPath.$(), HEADER_SIZE, HEADER_SIZE, MemoryTag.MMAP_TX_LOG);
        } finally {
            rootPath.trimTo(rootLen);
        }
    }

    private void openTxnPart(long txn) {
        long part = txn / partTransactionCount;
        if (partId != part) {
            int size = rootPath.size();
            try {
                rootPath.concat(TXNLOG_PARTS_DIR).slash().put(part);
                long partSize = partTransactionCount * RECORD_SIZE;
                txnPartMem.close(false);
                txnPartMem.of(ff, rootPath.$(), partSize, partSize, MemoryTag.MMAP_TX_LOG);
                txnPartMem.jumpTo((txn % partTransactionCount) * RECORD_SIZE);
                partId = part;
            } finally {
                rootPath.trimTo(size);
            }
        }
    }

    private void openTxnPart() {
        openTxnPart(this.maxTxn.get());
    }

    private void setAppendPosition() {
        txnPartMem.jumpTo((this.maxTxn.get() % partTransactionCount) * RECORD_SIZE);
    }

    private void sync0() {
        int commitMode = configuration.getCommitMode();
        if (commitMode != CommitMode.NOSYNC) {
            txnMem.sync(commitMode == CommitMode.ASYNC);
        }
    }

    private static class TransactionLogCursorImpl implements TransactionLogCursor {
        private final Path rootPath;
        private long address;
        private FilesFacade ff;
        private long headerFd;
        private long partFd = -1;
        private long partId = -1;
        private long partMapSize;
        private int partTransactionCount;
        private long txn = -2;
        private long txnCount = -1;
        private long txnLo;
        private long txnOffset;

        public TransactionLogCursorImpl(FilesFacade ff, long txnLo, final @Transient Path path, int partTransactionCount) {
            rootPath = new Path();
            try {
                of(ff, txnLo, path, partTransactionCount);
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public void close() {
            if (headerFd > 0) {
                ff.close(headerFd);
            }
            if (txnCount > -1 && address > 0) {
                ff.munmap(address, partMapSize, MemoryTag.MMAP_TX_LOG_CURSOR);
                txnCount = 0;
                address = 0;
            }
            closePart();
            rootPath.close();
        }

        @Override
        public boolean extend() {
            final long newTxnCount = ff.readNonNegativeLong(headerFd, MAX_TXN_OFFSET_64);
            boolean extended = newTxnCount > txnCount;
            if (extended) {
                txnCount = newTxnCount;
            }
            return extended;
        }

        @Override
        public long getCommitTimestamp() {
            assert address != 0;
            return Unsafe.getUnsafe().getLong(address + txnOffset + TX_LOG_COMMIT_TIMESTAMP_OFFSET);
        }

        @Override
        public long getMaxTxn() {
            return txnCount;
        }

        @Override
        public int getPartitionSize() {
            return partTransactionCount;
        }

        @Override
        public int getSegmentId() {
            assert address != 0;
            return Unsafe.getUnsafe().getInt(address + txnOffset + TX_LOG_SEGMENT_OFFSET);
        }

        @Override
        public int getSegmentTxn() {
            assert address != 0;
            return Unsafe.getUnsafe().getInt(address + txnOffset + TX_LOG_SEGMENT_TXN_OFFSET);
        }

        @Override
        public long getStructureVersion() {
            assert address != 0;
            return Unsafe.getUnsafe().getLong(address + txnOffset + TX_LOG_STRUCTURE_VERSION_OFFSET);
        }

        @Override
        public long getTxn() {
            return txn;
        }

        @Override
        public long getTxnMaxTimestamp() {
            assert address != 0;
            return Unsafe.getUnsafe().getLong(address + txnOffset + MAX_TIMESTAMP_OFFSET);
        }

        @Override
        public long getTxnMinTimestamp() {
            assert address != 0;
            return Unsafe.getUnsafe().getLong(address + txnOffset + MIN_TIMESTAMP_OFFSET);
        }

        @Override
        public long getTxnRowCount() {
            assert address != 0;
            return Unsafe.getUnsafe().getLong(address + txnOffset + ROW_COUNT_OFFSET);
        }

        @Override
        public int getVersion() {
            return WAL_SEQUENCER_FORMAT_VERSION_V2;
        }

        @Override
        public int getWalId() {
            assert address != 0;
            return Unsafe.getUnsafe().getInt(address + txnOffset + TX_LOG_WAL_ID_OFFSET);
        }

        @Override
        public boolean hasNext() {
            if (txn >= txnCount) {
                txnCount = ff.readNonNegativeLong(headerFd, MAX_TXN_OFFSET_64);
                if (txn >= txnCount) {
                    return false;
                }
            }

            openPart(txn);
            txn++;
            return true;
        }

        @Override
        public void setPosition(long txn) {
            this.txn = txn;
        }

        @Override
        public void toMinTxn() {
            int rootLen = rootPath.size();
            rootPath.concat(TXNLOG_PARTS_DIR).slash();
            long partId = txnLo / partTransactionCount;
            long minTxn = partId * partTransactionCount;

            int rootPathLen = rootPath.size();
            try {
                for (long part = partId - 1; part > -1L; part--) {
                    rootPath.trimTo(rootPathLen).put(part);
                    if (ff.exists(rootPath.$())) {
                        minTxn = part * partTransactionCount;
                    } else {
                        break;
                    }
                }
            } finally {
                rootPath.trimTo(rootLen);
            }
            openPart(minTxn);
            txn = txnLo = minTxn;
        }

        @Override
        public void toTop() {
            if (txnCount > -1L) {
                this.txn = txnLo;
            }
        }

        private void closePart() {
            if (partFd > -1) {
                if (address != 0) {
                    ff.munmap(address, partMapSize, MemoryTag.MMAP_TX_LOG_CURSOR);
                }
                ff.close(partFd);
                partFd = -1;
                partId = -2;
                address = 0;
            }
        }

        @NotNull
        private TransactionLogCursorImpl of(FilesFacade ff, long txnLo, @Transient Path path, int partTransactionCount) {
            this.partTransactionCount = partTransactionCount;
            partMapSize = partTransactionCount * RECORD_SIZE;
            this.ff = ff;
            this.headerFd = openRO(ff, path, WalUtils.TXNLOG_FILE_NAME, LOG);
            long newTxnCount = ff.readNonNegativeLong(headerFd, MAX_TXN_OFFSET_64);
            rootPath.of(path);

            if (newTxnCount > -1L) {
                this.txnCount = newTxnCount;
                assert txnLo > -1;
                this.txn = txnLo;
            } else {
                throw CairoException.critical(ff.errno()).put("cannot read sequencer transactions [path=").put(path).put(']');
            }
            this.txnLo = txnLo;
            return this;
        }

        private void openPart(long zeroBasedTxn) {
            long part = zeroBasedTxn / partTransactionCount;
            if (part != partId) {
                closePart();
                int size = rootPath.size();
                try {
                    rootPath.concat(TXNLOG_PARTS_DIR).slash().put(part);
                    partFd = openRO(ff, rootPath.$(), LOG);
                    long newAddr = ff.mmap(partFd, partMapSize, 0, Files.MAP_RO, MemoryTag.MMAP_TX_LOG_CURSOR);
                    if (newAddr == FilesFacade.MAP_FAILED) {
                        throw CairoException.critical(ff.errno())
                                .put("cannot mmap transaction log part [path=").put(rootPath)
                                .put(", mapSize=").put(partMapSize)
                                .put(']');
                    }
                    address = newAddr;
                    partId = part;
                } finally {
                    rootPath.trimTo(size);
                }
            }
            this.txnOffset = (zeroBasedTxn % partTransactionCount) * RECORD_SIZE;
        }
    }
}
