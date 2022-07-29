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

package io.questdb.cairo;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.CATALOG_FILE_NAME;
import static io.questdb.cairo.TableUtils.openSmallFile;

public class TxnCatalog implements Closeable {
    private final FilesFacade ff;
    private final MemoryCMARW txnListMem = Vm.getCMARWInstance();
    public static final int RECORD_SIZE = Integer.BYTES + Long.BYTES + Long.BYTES;
    public static final int HEADER_SIZE = Integer.BYTES + Long.BYTES;
    public static final long COUNT_OFFSET = Integer.BYTES;
    private final MemoryCMR txnCursorMem = Vm.getCMARWInstance();
    private long txnCount;

    TxnCatalog(FilesFacade ff) {
        this.ff = ff;
    }

    public void abortClose() {
        txnListMem.close(false);
    }

    @Override
    public void close() {
        Misc.free(txnListMem);
    }

    long addEntry(int walId, long segmentId, long segmentTxn) {
        txnListMem.putInt(walId);
        txnListMem.putLong(segmentId);
        txnListMem.putLong(segmentTxn);

        Unsafe.getUnsafe().storeFence();
        txnListMem.putLong(COUNT_OFFSET, ++txnCount);
        // Transactions are 1 based here
        return txnCount;
    }

    SequencerCursor getCursor(long txnLo) {
        return new SequencerCursorImpl(ff, txnLo, txnListMem.getFd());
    }

    // Can be used in parallel thread with no synchronisation between it and  addEntry
    long maxTxn() {
        Unsafe.getUnsafe().loadFence();
        // Transactions are 1 based here
        return txnListMem.getLong(COUNT_OFFSET);
    }

    void open(Path path) {
        openSmallFile(ff, path, path.length(), txnListMem, CATALOG_FILE_NAME, MemoryTag.MMAP_SEQUENCER);
        txnCount = txnListMem.getLong(COUNT_OFFSET);
        if (txnCount == 0) {
            txnListMem.jumpTo(0L);
            txnListMem.putInt(WalWriter.WAL_FORMAT_VERSION);
            txnListMem.putLong(0L);
        } else {
            txnListMem.jumpTo(HEADER_SIZE + txnCount * RECORD_SIZE);
        }
    }

    private static class SequencerCursorImpl implements SequencerCursor {
        private static final long WAL_ID_OFFSET = 0;
        private static final long SEGMENT_ID_OFFSET = WAL_ID_OFFSET + Integer.BYTES;
        private static final long SEGMENT_TXN_OFFSET = SEGMENT_ID_OFFSET + Long.BYTES;
        private final FilesFacade ff;
        private final long fd;
        private long txnOffset;
        private long txnCount;
        private long address;
        private long txn;

        public SequencerCursorImpl(FilesFacade ff, long txnLo, long fd) {
            this.ff = ff;
            this.fd = fd;
            this.txnCount = ff.readULong(fd, COUNT_OFFSET);
            if (txnCount > -1L) {
                this.address = ff.mmap(fd, getMappedLen(), 0, Files.MAP_RO, MemoryTag.MMAP_SEQUENCER);
                this.txnOffset = HEADER_SIZE + (txnLo - 1) * RECORD_SIZE;
            }
            txn = txnLo;
        }

        @Override
        public void close() {
            ff.munmap(address, getMappedLen(), MemoryTag.MMAP_SEQUENCER);
        }

        @Override
        public boolean hasNext() {
            if (this.txnOffset + 2 * RECORD_SIZE <= getMappedLen()) {
                this.txnOffset += RECORD_SIZE;
                this.txn++;
                return true;
            }

            long newTxnCount = ff.readULong(fd, COUNT_OFFSET);
            if (newTxnCount > txnCount) {
                long oldSize = getMappedLen();
                this.txnCount = newTxnCount;
                long newSize = getMappedLen();
                this.address = ff.mremap(fd, address, oldSize, newSize, 0, Files.MAP_RO, MemoryTag.MMAP_SEQUENCER);

                if (this.txnOffset + 2 * RECORD_SIZE <= newSize) {
                    this.txnOffset += RECORD_SIZE;
                    this.txn++;
                    return true;
                }
                return false;
            }
            return false;
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

        private long getMappedLen() {
            return txnCount * RECORD_SIZE + HEADER_SIZE;
        }
    }
}
