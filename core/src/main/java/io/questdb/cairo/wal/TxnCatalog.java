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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.MemorySerializer;
import io.questdb.cairo.vm.MemoryFCRImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.wal.WalUtils.WAL_FORMAT_VERSION;

public class TxnCatalog implements Closeable {
    public final static int HEADER_RESERVED = 8 * Long.BYTES;
    public final static int RECORD_RESERVED = 15 * Integer.BYTES;
    public static final long MAX_TXN_OFFSET = Integer.BYTES;
    public static final long MAX_STRUCTURE_VERSION_OFFSET = MAX_TXN_OFFSET + Long.BYTES;
    public static final long TXN_META_SIZE_OFFSET = MAX_STRUCTURE_VERSION_OFFSET + Long.BYTES;
    public static final long HEADER_SIZE = TXN_META_SIZE_OFFSET + Long.BYTES + HEADER_RESERVED;
    public static final int RECORD_SIZE = Integer.BYTES + Integer.BYTES + Long.BYTES + RECORD_RESERVED;
    public static final int METADATA_WALID = -1;
    private static final int MEMORY_TAG = MemoryTag.MMAP_SEQUENCER;
    private final FilesFacade ff;
    private final MemoryCMARW txnMem = Vm.getCMARWInstance();
    private final MemoryCMARW txnMetaMem = Vm.getCMARWInstance();
    private final MemoryCMARW txnMetaMemIndex = Vm.getCMARWInstance();
    private final Path rootPath = new Path();
    private long maxTxn;

    TxnCatalog(FilesFacade ff) {
        this.ff = ff;
    }

    public long beginMetadataChangeEntry(long newStructureVersion, MemorySerializer serializer, Object instance) {
        assert newStructureVersion == txnMetaMemIndex.getAppendOffset() / Long.BYTES;

        txnMem.putInt(METADATA_WALID);
        txnMem.putLong(newStructureVersion);
        txnMem.jumpTo(++maxTxn * RECORD_SIZE + HEADER_SIZE);

        txnMetaMem.putInt(0);
        long varMemBegin = txnMetaMem.getAppendOffset();
        serializer.toSink(instance, txnMetaMem);
        int len = (int) (txnMetaMem.getAppendOffset() - varMemBegin);
        txnMetaMem.putInt(varMemBegin - Integer.BYTES, len);
        txnMetaMemIndex.putLong(varMemBegin + len);

        return varMemBegin + len;
    }

    @Override
    public void close() {
        Misc.free(txnMem);
        Misc.free(txnMetaMem);
        Misc.free(txnMetaMemIndex);
        Misc.free(rootPath);
    }

    public long endMetadataChangeEntry(long newStructureVersion, long offset) {
        Unsafe.getUnsafe().storeFence();
        txnMem.putLong(MAX_TXN_OFFSET, maxTxn);
        txnMem.putLong(MAX_STRUCTURE_VERSION_OFFSET, newStructureVersion);
        txnMem.putLong(TXN_META_SIZE_OFFSET, offset);

        // Transactions are 1 based here
        return maxTxn;
    }

    @NotNull
    public SequencerStructureChangeCursor getStructureChangeCursor(
            @Nullable SequencerStructureChangeCursor reusableCursor,
            long fromStructureVersion,
            MemorySerializer serializer
    ) {
        SequencerStructureChangeCursorImpl cursor = (SequencerStructureChangeCursorImpl) reusableCursor;
        if (cursor == null) {
            cursor = new SequencerStructureChangeCursorImpl();
        }

        final Path path = Path.PATH.get().of(rootPath);
        cursor.of(fromStructureVersion, ff, serializer, path);
        return cursor;
    }

    private static long openFileRO(final FilesFacade ff, final Path path, final String fileName) {
        int rootLen = path.length();
        path.concat(fileName).$();
        try {
            long fd = ff.openRO(path);
            if (fd > -1) {
                return fd;
            }
            throw CairoException.critical(ff.errno()).put("could not open read-only [file=").put(path).put(']');
        } finally {
            path.trimTo(rootLen);
        }
    }

    long addEntry(int walId, int segmentId, long segmentTxn) {
        txnMem.putInt(walId);
        txnMem.putInt(segmentId);
        txnMem.putLong(segmentTxn);
        txnMem.jumpTo(txnMem.getAppendOffset() + RECORD_RESERVED);

        Unsafe.getUnsafe().storeFence();
        txnMem.putLong(MAX_TXN_OFFSET, ++maxTxn);
        // Transactions are 1 based here
        return maxTxn;
    }

    SequencerCursor getCursor(long seqTxn) {
        final Path path = Path.PATH.get().of(rootPath);
        final int rootLen = path.length();
        path.concat(CATALOG_FILE_NAME).$();
        try {
            return new SequencerCursorImpl(ff, seqTxn, path); //todo: dup fd
        } finally {
            path.trimTo(rootLen);
        }
    }

    long lastTxn() {
        return maxTxn;
    }

    void open(Path path) {
        this.rootPath.of(path);

        openSmallFile(ff, path, path.length(), txnMem, CATALOG_FILE_NAME, MEMORY_TAG);
        openSmallFile(ff, path, path.length(), txnMetaMem, CATALOG_FILE_NAME_META_VAR, MEMORY_TAG);
        openSmallFile(ff, path, path.length(), txnMetaMemIndex, CATALOG_FILE_NAME_META_INX, MEMORY_TAG);

        maxTxn = txnMem.getLong(MAX_TXN_OFFSET);
        long maxStructureVersion = txnMem.getLong(MAX_STRUCTURE_VERSION_OFFSET);
        long txnMetaMemSize = txnMem.getLong(TXN_META_SIZE_OFFSET);

        if (maxTxn == 0) {
            txnMem.jumpTo(0L);
            txnMem.putInt(WAL_FORMAT_VERSION);
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
        private static final long SEGMENT_TXN_OFFSET = SEGMENT_ID_OFFSET + Integer.BYTES;
        private final FilesFacade ff;
        private final long fd;
        private long txnOffset;
        private long txnCount;
        private long address;
        private long txn;

        public SequencerCursorImpl(FilesFacade ff, long seqTxn, final Path path) {
            this.ff = ff;
            this.fd = openFileRO(ff, path, CATALOG_FILE_NAME);
            this.txnCount = ff.readULong(fd, MAX_TXN_OFFSET);
            if (txnCount > -1L) {
                this.address = ff.mmap(fd, getMappedLen(), 0, Files.MAP_RO, MEMORY_TAG);
                this.txnOffset = HEADER_SIZE + (seqTxn - 1) * RECORD_SIZE;
            }
            txn = seqTxn;
        }

        @Override
        public void close() {
            if (fd > -1) {
                ff.close(fd);
            }
            ff.munmap(address, getMappedLen(), MEMORY_TAG);
        }

        @Override
        public boolean hasNext() {
            if (hasNext(getMappedLen())) {
                return true;
            }

            final long newTxnCount = ff.readULong(fd, MAX_TXN_OFFSET);
            if (newTxnCount > txnCount) {
                final long oldSize = getMappedLen();
                txnCount = newTxnCount;
                final long newSize = getMappedLen();
                address = ff.mremap(fd, address, oldSize, newSize, 0, Files.MAP_RO, MEMORY_TAG);

                return hasNext(newSize);
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

        private boolean hasNext(long mappedLen) {
            if (txnOffset + 2 * RECORD_SIZE <= mappedLen) {
                txnOffset += RECORD_SIZE;
                txn++;
                return true;
            }
            return false;
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
        public SequencerStructureChangeCursor empty() {
            txnMetaOffset = txnMetaOffsetHi = 0;
            return this;
        }

        @Override
        public boolean hasNext() {
            return txnMetaOffset < txnMetaOffsetHi;
        }

        @Override
        public AlterOperation next() {
            int recordSize = txnMetaMem.getInt(txnMetaOffset);
            if (recordSize < 0 || recordSize > Files.PAGE_SIZE) {
                throw CairoException.critical(0).put("Invalid sequencer txn metadata [offset=").put(txnMetaOffset).put(", recordSize=").put(recordSize).put(']');
            }
            txnMetaOffset += Integer.BYTES;
            serializer.fromSink(alterOperation, txnMetaMem, txnMetaOffset);
            txnMetaOffset += recordSize;
            return alterOperation;
        }

        public void reset() {
            if (txnMetaAddress > 0) {
                ff.munmap(txnMetaAddress, txnMetaOffsetHi, MEMORY_TAG);
                txnMetaAddress = 0;
            }
            txnMetaOffset = 0;
            txnMetaOffsetHi = 0;
        }

        public void of(
                long fromStructureVersion,
                FilesFacade ff,
                MemorySerializer serializer,
                final Path path) {
            reset();

            final long fdTxn = openFileRO(ff, path, CATALOG_FILE_NAME);
            final long fdTxnMeta = openFileRO(ff, path, CATALOG_FILE_NAME_META_VAR);
            final long fdTxnMetaIndex = openFileRO(ff, path, CATALOG_FILE_NAME_META_INX);

            this.ff = ff;
            this.serializer = serializer;
            try {
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
            throw CairoException.critical(0).put("expected to read table structure changes but there is no saved in the sequencer [fromStructureVersion=").put(fromStructureVersion).put(']');
        }
    }
}
