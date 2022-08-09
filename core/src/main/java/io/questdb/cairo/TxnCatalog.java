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

import io.questdb.cairo.vm.MemoryFCRImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;

import static io.questdb.cairo.TableUtils.*;

public class TxnCatalog implements Closeable {
    private static final int MEMORY_TAG = MemoryTag.MMAP_SEQUENCER;
    private final FilesFacade ff;
    private final MemoryCMARW txnMem = Vm.getCMARWInstance();
    private final MemoryCMARW txnMetaMem = Vm.getCMARWInstance();
    private final MemoryCMARW txnMetaMemIndex = Vm.getCMARWInstance();
    public final static int HEADER_RESERVED = 8 * Long.BYTES;
    public final static int RECORD_RESERVED = 11 * Integer.BYTES;
    public static final long MAX_TXN_OFFSET = Integer.BYTES;
    public static final long MAX_STRUCTURE_VERSION_OFFSET = MAX_TXN_OFFSET + Long.BYTES;
    public static final long TXN_META_SIZE_OFFSET = MAX_STRUCTURE_VERSION_OFFSET + Long.BYTES;
    public static final long HEADER_SIZE = TXN_META_SIZE_OFFSET + Long.BYTES + HEADER_RESERVED;
    public static final int RECORD_SIZE = Integer.BYTES + Long.BYTES + Long.BYTES + RECORD_RESERVED;
    public static final int METADATA_WALID = -1;
    private long maxTxn;

    TxnCatalog(FilesFacade ff) {
        this.ff = ff;
    }

    public void abortClose() {
        txnMem.close(false);
    }

    public long addMetadataChangeEntry(long newStructureVersion, MemorySerializer serializer, Object instance) {
        assert newStructureVersion == txnMetaMemIndex.getAppendOffset() / Long.BYTES;

        txnMem.putInt(METADATA_WALID);
        txnMem.putLong(newStructureVersion);
        txnMem.jumpTo(++maxTxn * RECORD_SIZE + HEADER_SIZE);

        long varMemBegin = txnMetaMem.getAppendOffset();
        txnMetaMem.putInt(0);
        serializer.toSink(instance, txnMetaMem);
        int len = (int)(txnMetaMem.getAppendOffset() - varMemBegin);
        txnMetaMem.putInt(varMemBegin, len);
        txnMetaMemIndex.putLong(varMemBegin + len);

        Unsafe.getUnsafe().storeFence();
        txnMem.putLong(MAX_TXN_OFFSET, maxTxn);
        txnMem.putLong(MAX_STRUCTURE_VERSION_OFFSET, newStructureVersion);
        txnMem.putLong(TXN_META_SIZE_OFFSET, varMemBegin + len);

        // Transactions are 1 based here
        return maxTxn;
    }

    @Override
    public void close() {
        Misc.free(txnMem);
        Misc.free(txnMetaMem);
        Misc.free(txnMetaMemIndex);
    }

    @NotNull
    public SequencerStructureChangeCursor getStructureChangeCursor(
            @Nullable SequencerStructureChangeCursor reusableCursor,
            int fromStructureVersion,
            MemorySerializer serializer
    ) {
        SequencerStructureChangeCursorImpl cursor = (SequencerStructureChangeCursorImpl) reusableCursor;
        if (cursor == null) {
            cursor = new SequencerStructureChangeCursorImpl();
        }
        cursor.of(fromStructureVersion, ff, serializer, txnMem.getFd(), txnMetaMemIndex.getFd(), txnMetaMem.getFd());
        return cursor;
    }

    long addEntry(int walId, long segmentId, long segmentTxn) {
        txnMem.putInt(walId);
        txnMem.putLong(segmentId);
        txnMem.putLong(segmentTxn);
        txnMem.jumpTo(txnMem.getAppendOffset() + RECORD_RESERVED);

        Unsafe.getUnsafe().storeFence();
        txnMem.putLong(MAX_TXN_OFFSET, ++maxTxn);
        // Transactions are 1 based here
        return maxTxn;
    }

    SequencerCursor getCursor(long txnLo) {
        return new SequencerCursorImpl(ff, txnLo, txnMem.getFd());
    }

    void open(Path path) {
        openSmallFile(ff, path, path.length(), txnMem, CATALOG_FILE_NAME, MEMORY_TAG);
        openSmallFile(ff, path, path.length(), txnMetaMem, CATALOG_FILE_NAME_META_VAR, MEMORY_TAG);
        openSmallFile(ff, path, path.length(), txnMetaMemIndex, CATALOG_FILE_NAME_META_INX, MEMORY_TAG);

        maxTxn = txnMem.getLong(MAX_TXN_OFFSET);
        long maxStructureVersion = txnMetaMem.getLong(MAX_STRUCTURE_VERSION_OFFSET);
        long txnMetaMemSize = txnMetaMem.getLong(TXN_META_SIZE_OFFSET);

        if (maxTxn == 0) {
            txnMem.jumpTo(0L);
            txnMem.putInt(WalWriter.WAL_FORMAT_VERSION);
            txnMem.putLong(0L);
            txnMem.putLong(0L);
            txnMem.jumpTo(HEADER_SIZE);

            txnMetaMemIndex.jumpTo(0L);
            txnMetaMemIndex.putLong(0L); // N + 1, first entry is 0.
            txnMetaMem.jumpTo(0L);
        } else {
            txnMem.jumpTo(HEADER_SIZE + maxTxn * RECORD_SIZE);
            long structureAppendOffset = (maxStructureVersion + 1) * Long.BYTES;
            txnMetaMemIndex.jumpTo(structureAppendOffset);
            txnMetaMem.jumpTo(txnMetaMemSize);
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
            this.txnCount = ff.readULong(fd, MAX_TXN_OFFSET);
            if (txnCount > -1L) {
                this.address = ff.mmap(fd, getMappedLen(), 0, Files.MAP_RO, MEMORY_TAG);
                this.txnOffset = HEADER_SIZE + (txnLo - 1) * RECORD_SIZE;
            }
            txn = txnLo;
        }

        @Override
        public void close() {
            ff.munmap(address, getMappedLen(), MEMORY_TAG);
        }

        @Override
        public boolean hasNext() {
            if (this.txnOffset + 2 * RECORD_SIZE <= getMappedLen()) {
                this.txnOffset += RECORD_SIZE;
                this.txn++;
                return true;
            }

            long newTxnCount = ff.readULong(fd, MAX_TXN_OFFSET);
            if (newTxnCount > txnCount) {
                long oldSize = getMappedLen();
                this.txnCount = newTxnCount;
                long newSize = getMappedLen();
                this.address = ff.mremap(fd, address, oldSize, newSize, 0, Files.MAP_RO, MEMORY_TAG);

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

    private static class SequencerStructureChangeCursorImpl implements SequencerStructureChangeCursor {
        private final AlterOperation alterOperation = new AlterOperation();
        private final MemoryFCRImpl txnMetaMem = new MemoryFCRImpl();
        private long txnMetaOffset;
        private long txnMetaOffsetHi;
        private long txnMetaAddress;
        private MemorySerializer serializer;
        private FilesFacade ff;

        @Override
        public void close() throws IOException {
            reset();
        }

        @Override
        public boolean hasNext() {
            return txnMetaOffset < txnMetaOffsetHi;
        }

        @Override
        public AlterOperation next() {
            int recordSize = txnMetaMem.getInt(txnMetaOffset);
            if (recordSize < 0 || recordSize > Files.PAGE_SIZE) {
                throw CairoException.instance(0).put("Invalid sequencer txn metadata [offset=").put(txnMetaOffset).put(", recordSize=").put(recordSize).put(']');
            }
            txnMetaOffset += Integer.BYTES;
            serializer.fromSink(alterOperation, txnMetaMem, txnMetaOffset);
            txnMetaOffset += recordSize;
            return alterOperation;
        }

        public void of(
                int fromStructureVersion,
                FilesFacade ff,
                MemorySerializer serializer,
                long fdTxn,
                long fdTxnMetaIndex,
                long fdTxnMeta) {
            reset();
            this.ff = ff;
            this.serializer = serializer;
            txnMetaOffset = ff.readULong(fdTxnMetaIndex, fromStructureVersion * Long.BYTES);
            if (txnMetaOffset > -1L) {
                txnMetaOffsetHi = ff.readULong(fdTxn, TXN_META_SIZE_OFFSET);
                if (txnMetaOffsetHi > txnMetaOffset) {
                    txnMetaAddress = ff.mmap(fdTxnMeta, txnMetaOffsetHi, 0L, Files.MAP_RO, MEMORY_TAG);
                    if (txnMetaAddress < 0) {
                        txnMetaAddress = 0;
                        reset();
                    } else {
                        txnMetaMem.of(txnMetaAddress, txnMetaOffsetHi);
                        return;
                    }
                }
            }
            throw CairoException.instance(0).put("expected to read table structure changes but there are no saved in the sequencer [fromStructureVersion=").put(fromStructureVersion).put(']');
        }

        public void reset() {
            if (txnMetaAddress > 0) {
                ff.munmap(txnMetaAddress, txnMetaOffsetHi, MEMORY_TAG);
                txnMetaAddress = 0;
            }
            txnMetaOffset = 0;
            txnMetaOffsetHi = 0;
        }
    }
}
