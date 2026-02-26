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

package io.questdb.cairo.idx;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.idx.BPBitmapIndexUtils.*;

/**
 * Delta + FoR64 BitPacking (BP) bitmap index writer.
 * <p>
 * Each commit appends one generation (covering all keys) to the value file.
 * No symbol table needed — encoding is purely arithmetic.
 */
public class BPBitmapIndexWriter implements IndexWriter {
    private static final int INITIAL_KEY_CAPACITY = 64;
    private static final Log LOG = LogFactory.getLog(BPBitmapIndexWriter.class);

    private final CairoConfiguration configuration;
    private final MemoryMARW keyMem = Vm.getCMARWInstance();
    private final MemoryMARW valueMem = Vm.getCMARWInstance();

    private int blockCapacity;
    private FilesFacade ff;
    private int genCount;
    private boolean hasPendingData;
    private int keyCapacity;
    private int keyCount;
    private long pendingCountsAddr;
    private long pendingValuesAddr;
    private long valueMemSize;

    public BPBitmapIndexWriter(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
    }

    @TestOnly
    public BPBitmapIndexWriter(CairoConfiguration configuration, Path path, CharSequence name, long columnNameTxn) {
        this(configuration);
        of(path, name, columnNameTxn, true);
    }

    public static void initKeyMemory(MemoryMA keyMem, int blockCapacity) {
        keyMem.jumpTo(0);
        keyMem.truncate();
        keyMem.putByte(SIGNATURE);
        keyMem.skip(7);
        keyMem.putLong(1); // SEQUENCE
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(0); // VALUE_MEM_SIZE
        keyMem.putInt(blockCapacity); // BLOCK_CAPACITY
        keyMem.putInt(0); // KEY_COUNT
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(1); // SEQUENCE_CHECK
        keyMem.putLong(-1); // MAX_VALUE
        keyMem.putInt(0); // GEN_COUNT
        keyMem.skip(KEY_FILE_RESERVED - keyMem.getAppendOffset());
    }

    @Override
    public void add(int key, long value) {
        if (key < 0) {
            throw CairoException.critical(0).put("index key cannot be negative [key=").put(key).put(']');
        }

        if (key >= keyCapacity) {
            growKeyBuffers(key + 1);
        }

        int count = Unsafe.getUnsafe().getInt(pendingCountsAddr + (long) key * Integer.BYTES);

        if (count >= blockCapacity) {
            throw CairoException.critical(0)
                    .put("too many values for key [key=").put(key)
                    .put(", count=").put(count)
                    .put(", blockCapacity=").put(blockCapacity).put(']');
        }

        if (count > 0) {
            long lastVal = Unsafe.getUnsafe().getLong(
                    pendingValuesAddr + ((long) key * blockCapacity + count - 1) * Long.BYTES);
            if (value < lastVal) {
                throw CairoException.critical(0)
                        .put("index values must be added in ascending order [lastValue=")
                        .put(lastVal).put(", newValue=").put(value).put(']');
            }
        }

        Unsafe.getUnsafe().putLong(
                pendingValuesAddr + ((long) key * blockCapacity + count) * Long.BYTES, value);
        Unsafe.getUnsafe().putInt(pendingCountsAddr + (long) key * Integer.BYTES, count + 1);

        if (key >= keyCount) {
            keyCount = key + 1;
        }
        hasPendingData = true;
    }

    @Override
    public void close() {
        seal();

        if (keyMem.isOpen()) {
            long keyFileSize = genCount > 0
                    ? BPBitmapIndexUtils.getGenDirOffset(genCount)
                    : KEY_FILE_RESERVED;
            keyMem.setSize(keyFileSize);
            Misc.free(keyMem);
        }
        if (valueMem.isOpen()) {
            if (valueMemSize > 0) {
                valueMem.setSize(valueMemSize);
            }
            Misc.free(valueMem);
        }

        freeNativeBuffers();
        keyCount = 0;
        valueMemSize = 0;
        genCount = 0;
        hasPendingData = false;
    }

    @Override
    public void commit() {
        flushAllPending();
        if (configuration.getCommitMode() != CommitMode.NOSYNC) {
            sync(configuration.getCommitMode() == CommitMode.ASYNC);
        }
    }

    /**
     * Seal the index: decode all generations, merge, re-encode into a single generation.
     * Simpler than FSST seal — no dictionary to retrain.
     */
    public void seal() {
        flushAllPending();

        if (genCount <= 1 || keyCount == 0) {
            return;
        }

        // Phase 1: Count total values per key across all generations
        long totalCountsSize = (long) keyCount * Integer.BYTES;
        long totalCountsAddr = Unsafe.malloc(totalCountsSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(totalCountsAddr, totalCountsSize, (byte) 0);

            long totalValueCount = 0;
            for (int gen = 0; gen < genCount; gen++) {
                long dirOffset = BPBitmapIndexUtils.getGenDirOffset(gen);
                long genFileOffset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
                long genAddr = valueMem.addressOf(genFileOffset);

                for (int key = 0; key < keyCount; key++) {
                    int count = Unsafe.getUnsafe().getInt(genAddr + (long) key * Integer.BYTES);
                    int existing = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                    Unsafe.getUnsafe().putInt(totalCountsAddr + (long) key * Integer.BYTES, existing + count);
                    totalValueCount += count;
                }
            }

            if (totalValueCount == 0) {
                return;
            }

            // Phase 2: Decode all values grouped by key into a flat buffer
            long allValuesAddr = Unsafe.malloc(totalValueCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            long keyWriteOffsetsSize = (long) keyCount * Long.BYTES;
            long keyWriteOffsetsAddr = Unsafe.malloc(keyWriteOffsetsSize, MemoryTag.NATIVE_DEFAULT);
            try {
                // Compute per-key write offsets
                long writeOffset = 0;
                for (int key = 0; key < keyCount; key++) {
                    Unsafe.getUnsafe().putLong(keyWriteOffsetsAddr + (long) key * Long.BYTES, writeOffset);
                    writeOffset += Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                }

                // Decode from each generation
                for (int gen = 0; gen < genCount; gen++) {
                    long dirOffset = BPBitmapIndexUtils.getGenDirOffset(gen);
                    long genFileOffset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
                    int genDataSize = keyMem.getInt(dirOffset + GEN_DIR_OFFSET_SIZE);
                    long genAddr = valueMem.addressOf(genFileOffset);
                    int headerSize = BPBitmapIndexUtils.genHeaderSize(keyCount);

                    for (int key = 0; key < keyCount; key++) {
                        int count = Unsafe.getUnsafe().getInt(genAddr + (long) key * Integer.BYTES);
                        if (count == 0) continue;

                        int dataOffset = Unsafe.getUnsafe().getInt(
                                genAddr + (long) keyCount * Integer.BYTES + (long) key * Integer.BYTES);
                        long encodedAddr = genAddr + headerSize + dataOffset;

                        long keyWriteOff = Unsafe.getUnsafe().getLong(
                                keyWriteOffsetsAddr + (long) key * Long.BYTES);
                        long destAddr = allValuesAddr + keyWriteOff * Long.BYTES;

                        // Decode values from BP-encoded data
                        long[] decoded = new long[count];
                        BPBitmapIndexUtils.decodeKey(encodedAddr, count, decoded);
                        for (int i = 0; i < count; i++) {
                            Unsafe.getUnsafe().putLong(destAddr + (long) i * Long.BYTES, decoded[i]);
                        }

                        Unsafe.getUnsafe().putLong(
                                keyWriteOffsetsAddr + (long) key * Long.BYTES, keyWriteOff + count);
                    }
                }

                // Phase 3: Re-encode into single generation
                int headerBufSize = BPBitmapIndexUtils.genHeaderSize(keyCount);
                long headerBuf = Unsafe.malloc(headerBufSize, MemoryTag.NATIVE_DEFAULT);

                int maxPerKey = 0;
                for (int key = 0; key < keyCount; key++) {
                    int c = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                    if (c > maxPerKey) maxPerKey = c;
                }
                int perKeyBufSize = BPBitmapIndexUtils.computeMaxEncodedSize(maxPerKey);
                long tmpBuf = Unsafe.malloc(perKeyBufSize, MemoryTag.NATIVE_DEFAULT);

                try {
                    Unsafe.getUnsafe().setMemory(headerBuf, headerBufSize, (byte) 0);

                    valueMem.jumpTo(0);
                    for (long i = 0; i + Long.BYTES <= headerBufSize; i += Long.BYTES) {
                        valueMem.putLong(0L);
                    }
                    for (long i = (headerBufSize / Long.BYTES) * Long.BYTES; i < headerBufSize; i++) {
                        valueMem.putByte((byte) 0);
                    }

                    long countsBase = headerBuf;
                    long offsetsBase = headerBuf + (long) keyCount * Integer.BYTES;
                    long readOffset = 0;
                    int encodedOffset = 0;

                    long[] keyValues = new long[maxPerKey];
                    for (int key = 0; key < keyCount; key++) {
                        int count = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);

                        Unsafe.getUnsafe().putInt(countsBase + (long) key * Integer.BYTES, count);
                        Unsafe.getUnsafe().putInt(offsetsBase + (long) key * Integer.BYTES, encodedOffset);

                        if (count > 0) {
                            for (int i = 0; i < count; i++) {
                                keyValues[i] = Unsafe.getUnsafe().getLong(
                                        allValuesAddr + (readOffset + i) * Long.BYTES);
                            }

                            int bytesWritten = BPBitmapIndexUtils.encodeKey(keyValues, count, tmpBuf);

                            int written = 0;
                            while (written + Long.BYTES <= bytesWritten) {
                                valueMem.putLong(Unsafe.getUnsafe().getLong(tmpBuf + written));
                                written += (int) Long.BYTES;
                            }
                            while (written < bytesWritten) {
                                valueMem.putByte(Unsafe.getUnsafe().getByte(tmpBuf + written));
                                written++;
                            }

                            encodedOffset += bytesWritten;
                            readOffset += count;
                        }
                    }

                    int totalGenSize = headerBufSize + encodedOffset;
                    valueMemSize = totalGenSize;

                    long headerAddr = valueMem.addressOf(0);
                    Unsafe.getUnsafe().copyMemory(headerBuf, headerAddr, headerBufSize);

                    genCount = 1;
                    long dirOffset = BPBitmapIndexUtils.getGenDirOffset(0);
                    keyMem.putLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET, 0);
                    keyMem.putInt(dirOffset + GEN_DIR_OFFSET_SIZE, totalGenSize);
                } finally {
                    Unsafe.free(headerBuf, headerBufSize, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(tmpBuf, perKeyBufSize, MemoryTag.NATIVE_DEFAULT);
                }

                keyMem.putInt(KEY_RESERVED_OFFSET_GEN_COUNT, genCount);
                updateHeaderAtomically();

            } finally {
                Unsafe.free(allValuesAddr, totalValueCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(keyWriteOffsetsAddr, keyWriteOffsetsSize, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            Unsafe.free(totalCountsAddr, totalCountsSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @TestOnly
    public RowCursor getCursor(int key) {
        flushAllPending();

        if (key >= keyCount || key < 0 || genCount == 0) {
            return EmptyRowCursor.INSTANCE;
        }

        LongList values = new LongList();
        for (int gen = 0; gen < genCount; gen++) {
            long dirOffset = BPBitmapIndexUtils.getGenDirOffset(gen);
            long genFileOffset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
            int genDataSize = keyMem.getInt(dirOffset + GEN_DIR_OFFSET_SIZE);

            long genAddr = valueMem.addressOf(genFileOffset);
            int headerSize = BPBitmapIndexUtils.genHeaderSize(keyCount);

            int count = Unsafe.getUnsafe().getInt(genAddr + (long) key * Integer.BYTES);
            if (count == 0) continue;

            int dataOffset = Unsafe.getUnsafe().getInt(genAddr + (long) keyCount * Integer.BYTES + (long) key * Integer.BYTES);
            long encodedAddr = genAddr + headerSize + dataOffset;

            long[] decoded = new long[count];
            BPBitmapIndexUtils.decodeKey(encodedAddr, count, decoded);
            for (int i = 0; i < count; i++) {
                values.add(decoded[i]);
            }
        }

        return new TestFwdCursor(values);
    }

    @Override
    public int getKeyCount() {
        return keyCount;
    }

    @TestOnly
    public long getValueMemSize() {
        return valueMemSize;
    }

    @Override
    public boolean isOpen() {
        return keyMem.isOpen();
    }

    @Override
    public byte getIndexType() {
        return IndexType.BP;
    }

    @Override
    public long getMaxValue() {
        return keyMem.getLong(KEY_RESERVED_OFFSET_MAX_VALUE);
    }

    @Override
    public void setMaxValue(long maxValue) {
        keyMem.putLong(KEY_RESERVED_OFFSET_MAX_VALUE, maxValue);
    }

    @Override
    public void of(Path path, CharSequence name, long columnNameTxn) {
        of(path, name, columnNameTxn, false);
    }

    @Override
    public void of(CairoConfiguration configuration, long keyFd, long valueFd, boolean init, int blockCapacity) {
        throw new UnsupportedOperationException("BP index does not support fd-based open");
    }

    @Override
    public void rollbackConditionally(long row) {
        throw new UnsupportedOperationException("BP index rollback not yet implemented");
    }

    @Override
    public void rollbackValues(long maxValue) {
        throw new UnsupportedOperationException("BP index rollback not yet implemented");
    }

    @Override
    public void sync(boolean async) {
        if (keyMem.isOpen()) {
            keyMem.sync(async);
        }
        if (valueMem.isOpen()) {
            valueMem.sync(async);
        }
    }

    @Override
    public void closeNoTruncate() {
        close();
    }

    @Override
    public void clear() {
        close();
    }

    public void of(Path path, CharSequence name, long columnNameTxn, boolean init) {
        close();
        final int plen = path.size();
        boolean kFdUnassigned = true;

        try {
            LPSZ keyFile = BPBitmapIndexUtils.keyFileName(path, name, columnNameTxn);

            if (init) {
                keyMem.of(ff, keyFile, configuration.getDataIndexKeyAppendPageSize(), 0L, MemoryTag.MMAP_INDEX_WRITER);
                this.blockCapacity = BLOCK_CAPACITY;
                initKeyMemory(keyMem, blockCapacity);
                kFdUnassigned = false;
            } else {
                if (!ff.exists(keyFile)) {
                    throw CairoException.critical(0).put("index does not exist [path=").put(path).put(']');
                }

                long keyFileSize = ff.length(keyFile);
                if (keyFileSize < KEY_FILE_RESERVED) {
                    throw CairoException.critical(0)
                            .put("Index file too short [expected>=").put(KEY_FILE_RESERVED)
                            .put(", actual=").put(keyFileSize).put(']');
                }

                keyMem.of(ff, keyFile, configuration.getDataIndexKeyAppendPageSize(), -1L, MemoryTag.MMAP_INDEX_WRITER);
                kFdUnassigned = false;

                byte sig = keyMem.getByte(KEY_RESERVED_OFFSET_SIGNATURE);
                if (sig != SIGNATURE) {
                    throw CairoException.critical(0)
                            .put("Unknown format: invalid BP index signature [expected=").put(SIGNATURE)
                            .put(", actual=").put(sig).put(']');
                }
            }

            this.valueMemSize = keyMem.getLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
            this.blockCapacity = keyMem.getInt(KEY_RESERVED_OFFSET_BLOCK_CAPACITY);
            this.keyCount = keyMem.getInt(KEY_RESERVED_OFFSET_KEY_COUNT);
            this.genCount = keyMem.getInt(KEY_RESERVED_OFFSET_GEN_COUNT);

            valueMem.of(
                    ff,
                    BPBitmapIndexUtils.valueFileName(path.trimTo(plen), name, columnNameTxn),
                    configuration.getDataIndexValueAppendPageSize(),
                    init ? 0 : valueMemSize,
                    MemoryTag.MMAP_INDEX_WRITER
            );

            if (!init && valueMemSize > 0) {
                valueMem.jumpTo(valueMemSize);
            }

            allocateNativeBuffers();
        } catch (Throwable e) {
            close();
            if (kFdUnassigned) {
                LOG.error().$("could not open BP index [path=").$(path).$(']').$();
            }
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    @Override
    public void truncate() {
        freeNativeBuffers();
        initKeyMemory(keyMem, blockCapacity);
        valueMem.truncate();
        keyCount = 0;
        valueMemSize = 0;
        genCount = 0;
        hasPendingData = false;
        allocateNativeBuffers();
    }

    private void allocateNativeBuffers() {
        keyCapacity = Math.max(INITIAL_KEY_CAPACITY, keyCount);
        long valBufSize = (long) keyCapacity * blockCapacity * Long.BYTES;
        long countBufSize = (long) keyCapacity * Integer.BYTES;

        pendingValuesAddr = Unsafe.malloc(valBufSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().setMemory(pendingValuesAddr, valBufSize, (byte) 0);

        pendingCountsAddr = Unsafe.malloc(countBufSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().setMemory(pendingCountsAddr, countBufSize, (byte) 0);
    }

    private void flushAllPending() {
        if (!hasPendingData || pendingCountsAddr == 0 || keyCount == 0) {
            return;
        }

        int totalValues = 0;
        for (int key = 0; key < keyCount; key++) {
            totalValues += Unsafe.getUnsafe().getInt(pendingCountsAddr + (long) key * Integer.BYTES);
        }

        if (totalValues == 0) {
            hasPendingData = false;
            return;
        }

        long maxValue = keyMem.getLong(KEY_RESERVED_OFFSET_MAX_VALUE);

        int headerSize = BPBitmapIndexUtils.genHeaderSize(keyCount);
        long headerBufSize = (long) keyCount * Integer.BYTES * 2;
        long headerBuf = Unsafe.malloc(headerBufSize, MemoryTag.NATIVE_DEFAULT);

        int perKeyBufSize = BPBitmapIndexUtils.computeMaxEncodedSize(blockCapacity);
        long tmpBuf = Unsafe.malloc(perKeyBufSize, MemoryTag.NATIVE_DEFAULT);

        try {
            Unsafe.getUnsafe().setMemory(headerBuf, headerBufSize, (byte) 0);
            long countsBase = headerBuf;
            long offsetsBase = headerBuf + (long) keyCount * Integer.BYTES;

            // Reserve header space
            long genOffset = valueMemSize;
            valueMem.jumpTo(genOffset);
            for (long i = 0; i + Long.BYTES <= headerBufSize; i += Long.BYTES) {
                valueMem.putLong(0L);
            }
            for (long i = (headerBufSize / Long.BYTES) * Long.BYTES; i < headerBufSize; i++) {
                valueMem.putByte((byte) 0);
            }

            // Encode each key's values
            int encodedOffset = 0;
            long[] keyValues = new long[blockCapacity];
            for (int key = 0; key < keyCount; key++) {
                int count = (key < keyCapacity)
                        ? Unsafe.getUnsafe().getInt(pendingCountsAddr + (long) key * Integer.BYTES)
                        : 0;

                Unsafe.getUnsafe().putInt(countsBase + (long) key * Integer.BYTES, count);
                Unsafe.getUnsafe().putInt(offsetsBase + (long) key * Integer.BYTES, encodedOffset);

                if (count > 0) {
                    long keyValuesAddr = pendingValuesAddr + (long) key * blockCapacity * Long.BYTES;

                    for (int i = 0; i < count; i++) {
                        keyValues[i] = Unsafe.getUnsafe().getLong(keyValuesAddr + (long) i * Long.BYTES);
                    }

                    int bytesWritten = BPBitmapIndexUtils.encodeKey(keyValues, count, tmpBuf);

                    int written = 0;
                    while (written + Long.BYTES <= bytesWritten) {
                        valueMem.putLong(Unsafe.getUnsafe().getLong(tmpBuf + written));
                        written += (int) Long.BYTES;
                    }
                    while (written < bytesWritten) {
                        valueMem.putByte(Unsafe.getUnsafe().getByte(tmpBuf + written));
                        written++;
                    }

                    encodedOffset += bytesWritten;

                    long lastVal = Unsafe.getUnsafe().getLong(keyValuesAddr + (long) (count - 1) * Long.BYTES);
                    if (lastVal > maxValue) {
                        maxValue = lastVal;
                    }
                }
            }

            int totalGenSize = headerSize + encodedOffset;
            valueMemSize = genOffset + totalGenSize;

            long headerAddr = valueMem.addressOf(genOffset);
            Unsafe.getUnsafe().copyMemory(headerBuf, headerAddr, headerBufSize);

            long dirOffset = BPBitmapIndexUtils.getGenDirOffset(genCount);
            keyMem.putLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET, genOffset);
            keyMem.putInt(dirOffset + GEN_DIR_OFFSET_SIZE, totalGenSize);
            genCount++;
        } finally {
            Unsafe.free(headerBuf, headerBufSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(tmpBuf, perKeyBufSize, MemoryTag.NATIVE_DEFAULT);
        }

        keyMem.putInt(KEY_RESERVED_OFFSET_GEN_COUNT, genCount);
        keyMem.putLong(KEY_RESERVED_OFFSET_MAX_VALUE, maxValue);
        updateHeaderAtomically();

        Unsafe.getUnsafe().setMemory(pendingCountsAddr, (long) keyCapacity * Integer.BYTES, (byte) 0);
        hasPendingData = false;
    }

    private void freeNativeBuffers() {
        if (pendingValuesAddr != 0) {
            Unsafe.free(pendingValuesAddr, (long) keyCapacity * blockCapacity * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            pendingValuesAddr = 0;
        }
        if (pendingCountsAddr != 0) {
            Unsafe.free(pendingCountsAddr, (long) keyCapacity * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            pendingCountsAddr = 0;
        }
        keyCapacity = 0;
    }

    private void growKeyBuffers(int minCapacity) {
        int newCapacity = Math.max(keyCapacity * 2, minCapacity);

        long oldValSize = (long) keyCapacity * blockCapacity * Long.BYTES;
        long newValSize = (long) newCapacity * blockCapacity * Long.BYTES;
        pendingValuesAddr = Unsafe.realloc(pendingValuesAddr, oldValSize, newValSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().setMemory(pendingValuesAddr + oldValSize, newValSize - oldValSize, (byte) 0);

        long oldCountSize = (long) keyCapacity * Integer.BYTES;
        long newCountSize = (long) newCapacity * Integer.BYTES;
        pendingCountsAddr = Unsafe.realloc(pendingCountsAddr, oldCountSize, newCountSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().setMemory(pendingCountsAddr + oldCountSize, newCountSize - oldCountSize, (byte) 0);

        keyCapacity = newCapacity;
    }

    private void updateHeaderAtomically() {
        long seq = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE) + 1;
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE, seq);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE, valueMemSize);
        keyMem.putInt(KEY_RESERVED_OFFSET_KEY_COUNT, keyCount);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE_CHECK, seq);
    }

    private static class TestFwdCursor implements RowCursor {
        private final LongList values;
        private int position;

        TestFwdCursor(LongList values) {
            this.values = values;
            this.position = 0;
        }

        @Override
        public boolean hasNext() {
            return position < values.size();
        }

        @Override
        public long next() {
            return values.getQuick(position++);
        }
    }
}
