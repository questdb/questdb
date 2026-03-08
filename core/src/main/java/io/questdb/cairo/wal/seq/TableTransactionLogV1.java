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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.TableUtils.openSmallFile;
import static io.questdb.cairo.wal.WalUtils.TXNLOG_FILE_NAME;
import static io.questdb.cairo.wal.WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1;

/**
 * This class is used to read/write transactions to the disk.
 * This is V1 implementation of the sequencer transaction log storage, and it will be used
 * in parallel with the new V2 for backward compatibility.
 * <p>
 * All transactions are stored in the single file table_dir\\txn_seq\\_txnlog, the file structure is
 * <p>
 * Header: 76 bytes
 * Transaction record: 28 bytes
 * <p>
 * See the format of the header and transaction record in {@link TableTransactionLogFile}
 */
public class TableTransactionLogV1 implements TableTransactionLogFile {
    private static final Log LOG = LogFactory.getLog(TableTransactionLogV1.class);
    private static final ThreadLocal<TransactionLogCursorImpl> tlTransactionLogCursor = new ThreadLocal<>();
    public static long RECORD_SIZE = TX_LOG_COMMIT_TIMESTAMP_OFFSET + Long.BYTES;
    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final AtomicLong maxTxn = new AtomicLong();
    private final MemoryCMARW txnMem = Vm.getCMARWInstance();

    public TableTransactionLogV1(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
    }

    public static long readMaxStructureVersion(long logFileFd, FilesFacade ff) {
        long maxTxn = ff.readNonNegativeLong(logFileFd, TableTransactionLogFile.MAX_TXN_OFFSET_64);
        if (maxTxn < 0) {
            return -1;
        }
        long offset = TableTransactionLogFile.HEADER_SIZE + (maxTxn - 1) * RECORD_SIZE;
        return ff.readNonNegativeLong(logFileFd, offset);
    }

    @Override
    public long addEntry(
            long structureVersion,
            int walId,
            int segmentId,
            int segmentTxn,
            long timestamp,
            long txnMinTimestamp,
            long txnMaxTimestamp,
            long txnRowCount
    ) {
        txnMem.putLong(structureVersion);
        txnMem.putInt(walId);
        txnMem.putInt(segmentId);
        txnMem.putInt(segmentTxn);
        txnMem.putLong(timestamp);

        Unsafe.getUnsafe().storeFence();
        long maxTxn = this.maxTxn.incrementAndGet();
        txnMem.putLong(MAX_TXN_OFFSET_64, maxTxn);
        sync0();
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
    }

    @Override
    public void create(Path path, long tableCreateTimestamp) {
        final int pathLength = path.size();
        openSmallFile(ff, path, pathLength, txnMem, TXNLOG_FILE_NAME, MemoryTag.MMAP_TX_LOG);

        txnMem.jumpTo(0L);
        txnMem.putInt(WAL_SEQUENCER_FORMAT_VERSION_V1);
        txnMem.putLong(0L);
        txnMem.putLong(tableCreateTimestamp);
        txnMem.putInt(0);
        sync0();
        txnMem.jumpTo(HEADER_SIZE);
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
            cursor = new TransactionLogCursorImpl(configuration, txnLo, path);
            tlTransactionLogCursor.set(cursor);
            return cursor;
        }
        try {
            return cursor.of(ff, configuration.getBypassWalFdCache(), txnLo, path);
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public boolean isDropped() {
        long lastTxn = maxTxn.get();
        if (lastTxn > 0) {
            return WalUtils.DROP_TABLE_WAL_ID == txnMem.getInt(HEADER_SIZE + (lastTxn - 1) * RECORD_SIZE + TX_LOG_WAL_ID_OFFSET);
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
            openSmallFile(ff, path, path.size(), txnMem, TXNLOG_FILE_NAME, MemoryTag.MMAP_TX_LOG);
        }

        long lastTxn = txnMem.getLong(MAX_TXN_OFFSET_64);
        maxTxn.set(lastTxn);
        txnMem.jumpTo(HEADER_SIZE);
        long maxStructureVersion = txnMem.getLong(HEADER_SIZE + (lastTxn - 1) * RECORD_SIZE + TX_LOG_STRUCTURE_VERSION_OFFSET);
        txnMem.jumpTo(HEADER_SIZE + lastTxn * RECORD_SIZE);
        return maxStructureVersion;
    }

    private void sync0() {
        int commitMode = configuration.getCommitMode();
        if (commitMode != CommitMode.NOSYNC) {
            txnMem.sync(commitMode == CommitMode.ASYNC);
        }
    }

    private static class TransactionLogCursorImpl implements TransactionLogCursor {
        private long address;
        private long fd;
        private FilesFacade ff;
        private long txn;
        private long txnCount = -1;
        private long txnLo;
        private long txnOffset;

        public TransactionLogCursorImpl(CairoConfiguration configuration, long txnLo, final Path path) {
            try {
                of(configuration.getFilesFacade(), configuration.getBypassWalFdCache(), txnLo, path);
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public void close() {
            if (fd > 0) {
                ff.close(fd);
                fd = 0;
            }
            if (txnCount > -1 && address > 0) {
                ff.munmap(address, getMappedLen(), MemoryTag.MMAP_TX_LOG_CURSOR);
                txnCount = 0;
                address = 0;
            }
        }

        @Override
        public boolean extend() {
            final long newTxnCount = ff.readNonNegativeLong(fd, MAX_TXN_OFFSET_64);
            if (newTxnCount > txnCount) {
                remap(newTxnCount);

                this.txnLo = txn - 1;
                this.txnOffset -= RECORD_SIZE;
                return true;
            }
            return false;
        }

        @Override
        public long getCommitTimestamp() {
            return Unsafe.getUnsafe().getLong(address + txnOffset + TX_LOG_COMMIT_TIMESTAMP_OFFSET);
        }

        @Override
        public long getMaxTxn() {
            return txnCount;
        }

        @Override
        public int getPartitionSize() {
            return 0;
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
        public long getTxnMaxTimestamp() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTxnMinTimestamp() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTxnRowCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getVersion() {
            return WAL_SEQUENCER_FORMAT_VERSION_V1;
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

            final long newTxnCount = ff.readNonNegativeLong(fd, MAX_TXN_OFFSET_64);
            if (newTxnCount > txnCount) {
                remap(newTxnCount);
                return hasNext(getMappedLen());
            }
            return false;
        }

        @Override
        public void setPosition(long txn) {
            this.txnOffset = HEADER_SIZE + (txn - 1) * RECORD_SIZE;
            this.txn = txn;
        }

        @Override
        public void toMinTxn() {
            toTop();
        }

        @Override
        public void toTop() {
            if (txnCount > -1L) {
                this.txnOffset = HEADER_SIZE + (txnLo - 1) * RECORD_SIZE;
                this.txn = txnLo;
            }
        }

        private static long openFileRO(final FilesFacade ff, final Path path, boolean bypassFdCache) {
            return bypassFdCache
                    ? TableUtils.openRONoCache(ff, path, WalUtils.TXNLOG_FILE_NAME, LOG)
                    : TableUtils.openRO(ff, path, WalUtils.TXNLOG_FILE_NAME, LOG);
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
        private TransactionLogCursorImpl of(FilesFacade ff, boolean bypassFdCache, long txnLo, Path path) {
            this.ff = ff;
            close();
            this.fd = openFileRO(ff, path, bypassFdCache);
            long newTxnCount = ff.readNonNegativeLong(fd, MAX_TXN_OFFSET_64);
            if (newTxnCount > -1L) {
                this.txnCount = newTxnCount;
                long newAddr = ff.mmap(fd, getMappedLen(), 0, Files.MAP_RO, MemoryTag.MMAP_TX_LOG_CURSOR);
                if (newAddr == FilesFacade.MAP_FAILED) {
                    throw CairoException.critical(Os.errno()).put("cannot mmap transaction log [path=").put(path).put(']');
                }
                this.address = newAddr;
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
            long newAddr = ff.mremap(fd, address, oldSize, newSize, 0, Files.MAP_RO, MemoryTag.MMAP_TX_LOG_CURSOR);
            if (newAddr == FilesFacade.MAP_FAILED) {
                throw CairoException.critical(Os.errno()).put("cannot remap transaction log [fd=").put(fd).put(']');
            }
            address = newAddr;
        }
    }
}
