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

import static io.questdb.cairo.idx.FSSTBitmapIndexUtils.*;

/**
 * FSST-compressed bitmap index writer.
 * <p>
 * One FSST symbol table per partition, trained once on first flush.
 * Each commit appends one generation (covering all keys) to the value file.
 * Per-key offsets enable O(1) random access on read.
 */
public class FSSTBitmapIndexWriter implements IndexWriter {
    private static final int INITIAL_KEY_CAPACITY = 64;
    private static final Log LOG = LogFactory.getLog(FSSTBitmapIndexWriter.class);

    private final CairoConfiguration configuration;
    private final MemoryMARW keyMem = Vm.getCMARWInstance();
    private final MemoryMARW valueMem = Vm.getCMARWInstance();

    private int blockValues;
    private FilesFacade ff;
    private int genCount;
    private boolean hasPendingData;
    private int keyCapacity;
    private int keyCount;
    private long pendingCountsAddr;
    private long pendingValuesAddr;
    private FSST.SymbolTable symbolTable;
    private long valueMemSize;

    public FSSTBitmapIndexWriter(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
    }

    @TestOnly
    public FSSTBitmapIndexWriter(CairoConfiguration configuration, Path path, CharSequence name, long columnNameTxn) {
        this(configuration);
        of(path, name, columnNameTxn, true);
    }

    public static void initKeyMemory(MemoryMA keyMem, int blockValues) {
        keyMem.jumpTo(0);
        keyMem.truncate();
        keyMem.putByte(SIGNATURE);
        keyMem.skip(7);
        keyMem.putLong(1); // SEQUENCE
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(0); // VALUE_MEM_SIZE
        keyMem.putInt(blockValues); // BLOCK_VALUES
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

        if (count >= blockValues) {
            // Buffer full for this key — flush all pending data to make room.
            flushAllPending();
            count = 0;
        }

        if (count > 0) {
            long lastVal = Unsafe.getUnsafe().getLong(
                    pendingValuesAddr + ((long) key * blockValues + count - 1) * Long.BYTES);
            if (value < lastVal) {
                throw CairoException.critical(0)
                        .put("index values must be added in ascending order [lastValue=")
                        .put(lastVal).put(", newValue=").put(value).put(']');
            }
        }

        Unsafe.getUnsafe().putLong(
                pendingValuesAddr + ((long) key * blockValues + count) * Long.BYTES, value);
        Unsafe.getUnsafe().putInt(pendingCountsAddr + (long) key * Integer.BYTES, count + 1);

        if (key >= keyCount) {
            keyCount = key + 1;
        }
        hasPendingData = true;
    }

    @Override
    public void close() {
        try {
            seal();
        } finally {
            try {
                if (keyMem.isOpen()) {
                    long keyFileSize = genCount > 0
                            ? FSSTBitmapIndexUtils.getGenDirOffset(genCount)
                            : KEY_FILE_RESERVED;
                    keyMem.setSize(keyFileSize);
                    Misc.free(keyMem);
                }
            } finally {
                try {
                    if (valueMem.isOpen()) {
                        if (valueMemSize > 0) {
                            valueMem.setSize(valueMemSize);
                        }
                        Misc.free(valueMem);
                    }
                } finally {
                    freeNativeBuffers();
                    keyCount = 0;
                    valueMemSize = 0;
                    genCount = 0;
                    if (symbolTable != null) {
                        symbolTable.close();
                        symbolTable = null;
                    }
                    hasPendingData = false;
                }
            }
        }
    }

    @Override
    public void commit() {
        flushAllPending();
        if (configuration.getCommitMode() != CommitMode.NOSYNC) {
            sync(configuration.getCommitMode() == CommitMode.ASYNC);
        }
    }

    /**
     * Seal the index: retrain the symbol table on all data and merge all
     * generations into a single generation. Called on partition close for
     * optimal compression and read performance.
     * <p>
     * After seal, the index has exactly 1 generation with a dictionary
     * trained on the full dataset.
     */
    public void seal() {
        flushAllPending();

        if (genCount <= 1 || symbolTable == null || keyCount == 0) {
            return; // nothing to optimize — 0 or 1 gen is already optimal
        }

        // Phase 1: Count total values per key across all generations
        long totalCountsSize = (long) keyCount * Integer.BYTES;
        long totalCountsAddr = Unsafe.malloc(totalCountsSize, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(totalCountsAddr, totalCountsSize, (byte) 0);

            long totalValueCount = 0;
            for (int gen = 0; gen < genCount; gen++) {
                long dirOffset = FSSTBitmapIndexUtils.getGenDirOffset(gen);
                long genFileOffset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
                int genKeyCount = keyMem.getInt(dirOffset + FSSTBitmapIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
                long genAddr = valueMem.addressOf(genFileOffset);

                for (int key = 0; key < genKeyCount; key++) {
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
                    long dirOffset = FSSTBitmapIndexUtils.getGenDirOffset(gen);
                    long genFileOffset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
                    int genDataSize = keyMem.getInt(dirOffset + GEN_DIR_OFFSET_SIZE);
                    int genKeyCount = keyMem.getInt(dirOffset + FSSTBitmapIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
                    long genAddr = valueMem.addressOf(genFileOffset);
                    int headerSize = FSSTBitmapIndexUtils.genHeaderSize(genKeyCount);

                    for (int key = 0; key < genKeyCount; key++) {
                        int count = Unsafe.getUnsafe().getInt(genAddr + (long) key * Integer.BYTES);
                        if (count == 0) continue;

                        int dataOffset = Unsafe.getUnsafe().getInt(
                                genAddr + (long) genKeyCount * Integer.BYTES + (long) key * Integer.BYTES);
                        int nextOffset;
                        if (key + 1 < genKeyCount) {
                            nextOffset = Unsafe.getUnsafe().getInt(
                                    genAddr + (long) genKeyCount * Integer.BYTES + (long) (key + 1) * Integer.BYTES);
                        } else {
                            nextOffset = genDataSize - headerSize;
                        }
                        int encodedLen = nextOffset - dataOffset;
                        long encodedAddr = genAddr + headerSize + dataOffset;

                        long keyWriteOff = Unsafe.getUnsafe().getLong(
                                keyWriteOffsetsAddr + (long) key * Long.BYTES);
                        FSST.decodeBulk(symbolTable, encodedAddr, encodedLen, count,
                                allValuesAddr + keyWriteOff * Long.BYTES);
                        Unsafe.getUnsafe().putLong(
                                keyWriteOffsetsAddr + (long) key * Long.BYTES, keyWriteOff + count);
                    }
                }

                // Phase 3: Retrain symbol table on all decoded values
                int trainCount = (int) Math.min(totalValueCount, Integer.MAX_VALUE);
                if (symbolTable != null) symbolTable.close();
                symbolTable = FSST.train(allValuesAddr, trainCount);

                // Serialize new symbol table to key file
                long stBuf = Unsafe.malloc(FSST.SERIALIZED_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
                try {
                    int stSize = FSST.serialize(symbolTable, stBuf);
                    for (int i = 0; i < stSize; i++) {
                        keyMem.putByte(SYMBOL_TABLE_OFFSET + i, Unsafe.getUnsafe().getByte(stBuf + i));
                    }
                } finally {
                    Unsafe.free(stBuf, FSST.SERIALIZED_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
                }

                // Phase 4: Re-encode into single generation and rewrite value file
                int headerBufSize = FSSTBitmapIndexUtils.genHeaderSize(keyCount);
                long headerBuf = Unsafe.malloc(headerBufSize, MemoryTag.NATIVE_DEFAULT);

                // Find max values for any single key to size the per-key encode buffer
                int maxPerKey = 0;
                for (int key = 0; key < keyCount; key++) {
                    int c = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);
                    if (c > maxPerKey) maxPerKey = c;
                }
                int perKeyBufSize = maxPerKey * 16; // worst case per key
                long tmpBuf = Unsafe.malloc(perKeyBufSize, MemoryTag.NATIVE_DEFAULT);

                try {
                    Unsafe.getUnsafe().setMemory(headerBuf, headerBufSize, (byte) 0);

                    // Write placeholder header to value file at offset 0 (rewriting from start)
                    valueMem.jumpTo(0);
                    for (long i = 0; i + Long.BYTES <= headerBufSize; i += Long.BYTES) {
                        valueMem.putLong(0L);
                    }
                    for (long i = (headerBufSize / Long.BYTES) * Long.BYTES; i < headerBufSize; i++) {
                        valueMem.putByte((byte) 0);
                    }

                    // Re-encode each key's values
                    long countsBase = headerBuf;
                    long offsetsBase = headerBuf + (long) keyCount * Integer.BYTES;
                    long readOffset = 0;
                    int encodedOffset = 0;

                    for (int key = 0; key < keyCount; key++) {
                        int count = Unsafe.getUnsafe().getInt(totalCountsAddr + (long) key * Integer.BYTES);

                        Unsafe.getUnsafe().putInt(countsBase + (long) key * Integer.BYTES, count);
                        Unsafe.getUnsafe().putInt(offsetsBase + (long) key * Integer.BYTES, encodedOffset);

                        if (count > 0) {
                            int bytesWritten = FSST.compress(symbolTable,
                                    allValuesAddr + readOffset * Long.BYTES, count, tmpBuf);

                            // Append encoded bytes to valueMem
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

                    // Write real header over placeholder
                    long headerAddr = valueMem.addressOf(0);
                    Unsafe.getUnsafe().copyMemory(headerBuf, headerAddr, headerBufSize);

                    // Reset generation directory to single entry
                    genCount = 1;
                    long dirOffset = FSSTBitmapIndexUtils.getGenDirOffset(0);
                    keyMem.putLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET, 0);
                    keyMem.putInt(dirOffset + GEN_DIR_OFFSET_SIZE, totalGenSize);
                    keyMem.putInt(dirOffset + FSSTBitmapIndexUtils.GEN_DIR_OFFSET_KEY_COUNT, keyCount);
                } finally {
                    Unsafe.free(headerBuf, headerBufSize, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(tmpBuf, perKeyBufSize, MemoryTag.NATIVE_DEFAULT);
                }

                updateHeaderAtomically(genCount, keyMem.getLong(KEY_RESERVED_OFFSET_MAX_VALUE));

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

        if (key >= keyCount || key < 0 || genCount == 0 || symbolTable == null) {
            return EmptyRowCursor.INSTANCE;
        }

        LongList values = new LongList();
        for (int gen = 0; gen < genCount; gen++) {
            long dirOffset = FSSTBitmapIndexUtils.getGenDirOffset(gen);
            long genFileOffset = keyMem.getLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET);
            int genDataSize = keyMem.getInt(dirOffset + GEN_DIR_OFFSET_SIZE);
            int genKeyCount = keyMem.getInt(dirOffset + FSSTBitmapIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);

            if (key >= genKeyCount) continue;

            long genAddr = valueMem.addressOf(genFileOffset);
            int headerSize = FSSTBitmapIndexUtils.genHeaderSize(genKeyCount);

            int count = Unsafe.getUnsafe().getInt(genAddr + (long) key * Integer.BYTES);
            if (count == 0) continue;

            int dataOffset = Unsafe.getUnsafe().getInt(genAddr + (long) genKeyCount * Integer.BYTES + (long) key * Integer.BYTES);
            int nextOffset;
            if (key + 1 < genKeyCount) {
                nextOffset = Unsafe.getUnsafe().getInt(genAddr + (long) genKeyCount * Integer.BYTES + (long) (key + 1) * Integer.BYTES);
            } else {
                nextOffset = genDataSize - headerSize;
            }
            int encodedLen = nextOffset - dataOffset;

            long encodedAddr = genAddr + headerSize + dataOffset;
            // Decode all values by streaming through the encoded bytes
            int off = 0;
            for (int v = 0; v < count; v++) {
                long result = 0;
                int bytePos = 0;
                while (bytePos < 8 && off < encodedLen) {
                    int code = Unsafe.getUnsafe().getByte(encodedAddr + off) & 0xFF;
                    off++;
                    if (code == FSST.ESCAPE) {
                        if (off < encodedLen) {
                            int literal = Unsafe.getUnsafe().getByte(encodedAddr + off) & 0xFF;
                            off++;
                            result |= ((long) literal) << (bytePos * 8);
                            bytePos++;
                        }
                    } else {
                        int len = symbolTable.getLen(code);
                        long sym = symbolTable.getSymbol(code);
                        for (int b = 0; b < len && bytePos < 8; b++) {
                            result |= ((sym >>> (b * 8)) & 0xFFL) << (bytePos * 8);
                            bytePos++;
                        }
                    }
                }
                values.add(result);
            }
        }

        return new TestBwdCursor(values);
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
        return IndexType.FSST;
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
        close();
        final FilesFacade ff = configuration.getFilesFacade();
        boolean kFdUnassigned = true;
        boolean vFdUnassigned = true;
        final long keyAppendPageSize = configuration.getDataIndexKeyAppendPageSize();
        final long valueAppendPageSize = configuration.getDataIndexValueAppendPageSize();

        try {
            if (init) {
                if (ff.truncate(keyFd, 0)) {
                    kFdUnassigned = false;
                    keyMem.of(ff, keyFd, false, null, keyAppendPageSize, keyAppendPageSize, MemoryTag.MMAP_INDEX_WRITER);
                    this.blockValues = DEFAULT_BLOCK_VALUES;
                    initKeyMemory(keyMem, blockValues);
                } else {
                    throw CairoException.critical(ff.errno()).put("Could not truncate [fd=").put(keyFd).put(']');
                }
            } else {
                final long keyFileSize = ff.length(keyFd);
                if (keyFileSize < KEY_FILE_RESERVED) {
                    throw CairoException.critical(0)
                            .put("Index file too short [fd=").put(keyFd)
                            .put(", expected>=").put(KEY_FILE_RESERVED)
                            .put(", actual=").put(keyFileSize).put(']');
                }

                kFdUnassigned = false;
                keyMem.of(ff, keyFd, null, keyFileSize, MemoryTag.MMAP_INDEX_WRITER);

                byte sig = keyMem.getByte(KEY_RESERVED_OFFSET_SIGNATURE);
                if (sig != SIGNATURE) {
                    throw CairoException.critical(0)
                            .put("Unknown format: invalid FSST index signature [fd=").put(keyFd)
                            .put(", expected=").put(SIGNATURE)
                            .put(", actual=").put(sig).put(']');
                }
            }

            this.valueMemSize = keyMem.getLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
            this.blockValues = keyMem.getInt(KEY_RESERVED_OFFSET_BLOCK_VALUES);
            this.keyCount = keyMem.getInt(KEY_RESERVED_OFFSET_KEY_COUNT);
            this.genCount = keyMem.getInt(KEY_RESERVED_OFFSET_GEN_COUNT);

            if (genCount > 0) {
                if (symbolTable != null) symbolTable.close();
                symbolTable = FSST.deserialize(keyMem.addressOf(SYMBOL_TABLE_OFFSET));
            }

            if (init) {
                if (ff.truncate(valueFd, 0)) {
                    vFdUnassigned = false;
                    valueMem.of(ff, valueFd, false, null, valueAppendPageSize, valueAppendPageSize, MemoryTag.MMAP_INDEX_WRITER);
                    valueMem.jumpTo(0);
                    valueMemSize = 0;
                } else {
                    throw CairoException.critical(ff.errno()).put("Could not truncate [fd=").put(valueFd).put(']');
                }
            } else {
                vFdUnassigned = false;
                valueMem.of(ff, valueFd, false, null, valueAppendPageSize, valueMemSize, MemoryTag.MMAP_INDEX_WRITER);
                if (valueMemSize > 0) {
                    valueMem.jumpTo(valueMemSize);
                }
            }

            allocateNativeBuffers();
        } catch (Throwable e) {
            close();
            if (kFdUnassigned) {
                ff.close(keyFd);
            }
            if (vFdUnassigned) {
                ff.close(valueFd);
            }
            throw e;
        }
    }

    @Override
    public void rollbackConditionally(long row) {
        final long currentMaxRow = getMaxValue();
        if (row >= 0 && (currentMaxRow < 1 || currentMaxRow >= row)) {
            if (row == 0) {
                truncate();
            } else {
                rollbackValues(row - 1);
            }
        }
    }

    @Override
    public void rollbackValues(long maxValue) {
        // Flush any pending data so all values are on disk before rollback.
        flushAllPending();

        if (genCount == 0 && keyCount == 0) {
            setMaxValue(maxValue);
            return;
        }

        // For generational FSST format, precise per-value rollback requires
        // decoding all generations, filtering, and re-encoding.
        // Truncate and let the caller re-add values as needed.
        LOG.info().$("rollback FSST index [maxValue=").$(maxValue).$(", genCount=").$(genCount).$(']').$();
        truncate();
        setMaxValue(maxValue);
    }

    @Override
    public void sync(boolean async) {
        // Flush pending data from native buffers to mmap'd files before syncing,
        // otherwise readers won't see buffered values.
        flushAllPending();
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
            LPSZ keyFile = FSSTBitmapIndexUtils.keyFileName(path, name, columnNameTxn);

            if (init) {
                keyMem.of(ff, keyFile, configuration.getDataIndexKeyAppendPageSize(), 0L, MemoryTag.MMAP_INDEX_WRITER);
                this.blockValues = DEFAULT_BLOCK_VALUES;
                initKeyMemory(keyMem, blockValues);
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
                            .put("Unknown format: invalid FSST index signature [expected=").put(SIGNATURE)
                            .put(", actual=").put(sig).put(']');
                }
            }

            this.valueMemSize = keyMem.getLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
            this.blockValues = keyMem.getInt(KEY_RESERVED_OFFSET_BLOCK_VALUES);
            this.keyCount = keyMem.getInt(KEY_RESERVED_OFFSET_KEY_COUNT);
            this.genCount = keyMem.getInt(KEY_RESERVED_OFFSET_GEN_COUNT);

            // Deserialize symbol table if present
            if (genCount > 0) {
                if (symbolTable != null) symbolTable.close();
                symbolTable = FSST.deserialize(keyMem.addressOf(SYMBOL_TABLE_OFFSET));
            }

            valueMem.of(
                    ff,
                    FSSTBitmapIndexUtils.valueFileName(path.trimTo(plen), name, columnNameTxn),
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
                LOG.error().$("could not open FSST index [path=").$(path).$(']').$();
            }
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    @Override
    public void truncate() {
        freeNativeBuffers();
        initKeyMemory(keyMem, blockValues);
        valueMem.truncate();
        keyCount = 0;
        valueMemSize = 0;
        genCount = 0;
        if (symbolTable != null) {
            symbolTable.close();
            symbolTable = null;
        }
        hasPendingData = false;
        allocateNativeBuffers();
    }

    private void allocateNativeBuffers() {
        keyCapacity = Math.max(INITIAL_KEY_CAPACITY, keyCount);
        long valBufSize = (long) keyCapacity * blockValues * Long.BYTES;
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

        // Train symbol table only on first flush
        if (symbolTable == null) {
            trainSymbolTable(totalValues);
        }

        long maxValue = keyMem.getLong(KEY_RESERVED_OFFSET_MAX_VALUE);

        // Streaming approach: allocate header buffer + small per-key encode buffer.
        // This avoids a monolithic buffer that can overflow int for high key counts.
        int headerSize = FSSTBitmapIndexUtils.genHeaderSize(keyCount);
        long headerBufSize = (long) keyCount * Integer.BYTES * 2;
        long headerBuf = Unsafe.malloc(headerBufSize, MemoryTag.NATIVE_DEFAULT);

        int perKeyBufSize = blockValues * 16; // worst case per key
        long tmpBuf = Unsafe.malloc(perKeyBufSize, MemoryTag.NATIVE_DEFAULT);

        try {
            Unsafe.getUnsafe().setMemory(headerBuf, headerBufSize, (byte) 0);
            long countsBase = headerBuf;
            long offsetsBase = headerBuf + (long) keyCount * Integer.BYTES;

            // Reserve header space in valueMem (write zeros as placeholder)
            long genOffset = valueMemSize;
            valueMem.jumpTo(genOffset);
            for (long i = 0; i + Long.BYTES <= headerBufSize; i += Long.BYTES) {
                valueMem.putLong(0L);
            }
            for (long i = (headerBufSize / Long.BYTES) * Long.BYTES; i < headerBufSize; i++) {
                valueMem.putByte((byte) 0);
            }

            // Encode each key's values and stream directly to valueMem
            int encodedOffset = 0;
            for (int key = 0; key < keyCount; key++) {
                int count = (key < keyCapacity)
                        ? Unsafe.getUnsafe().getInt(pendingCountsAddr + (long) key * Integer.BYTES)
                        : 0;

                Unsafe.getUnsafe().putInt(countsBase + (long) key * Integer.BYTES, count);
                Unsafe.getUnsafe().putInt(offsetsBase + (long) key * Integer.BYTES, encodedOffset);

                if (count > 0) {
                    long keyValuesAddr = pendingValuesAddr + (long) key * blockValues * Long.BYTES;
                    int bytesWritten = FSST.compress(symbolTable, keyValuesAddr, count, tmpBuf);

                    // Append encoded bytes to valueMem
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

            // Write real header (counts + offsets) over the placeholder via copyMemory
            long headerAddr = valueMem.addressOf(genOffset);
            Unsafe.getUnsafe().copyMemory(headerBuf, headerAddr, headerBufSize);

            // Write generation directory entry
            long dirOffset = FSSTBitmapIndexUtils.getGenDirOffset(genCount);
            keyMem.putLong(dirOffset + GEN_DIR_OFFSET_FILE_OFFSET, genOffset);
            keyMem.putInt(dirOffset + GEN_DIR_OFFSET_SIZE, totalGenSize);
            keyMem.putInt(dirOffset + FSSTBitmapIndexUtils.GEN_DIR_OFFSET_KEY_COUNT, keyCount);
            genCount++;
        } finally {
            Unsafe.free(headerBuf, headerBufSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(tmpBuf, perKeyBufSize, MemoryTag.NATIVE_DEFAULT);
        }

        updateHeaderAtomically(genCount, maxValue);

        Unsafe.getUnsafe().setMemory(pendingCountsAddr, (long) keyCapacity * Integer.BYTES, (byte) 0);
        hasPendingData = false;
    }

    private void trainSymbolTable(int totalValues) {
        int sampleSize = Math.min(totalValues, 16384);
        long sampleAddr = Unsafe.malloc((long) sampleSize * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            int sampled = 0;
            int step = Math.max(1, totalValues / sampleSize);
            int globalIdx = 0;
            outer:
            for (int key = 0; key < keyCount && sampled < sampleSize; key++) {
                int count = Unsafe.getUnsafe().getInt(pendingCountsAddr + (long) key * Integer.BYTES);
                for (int v = 0; v < count; v++) {
                    if (globalIdx % step == 0 && sampled < sampleSize) {
                        long val = Unsafe.getUnsafe().getLong(
                                pendingValuesAddr + ((long) key * blockValues + v) * Long.BYTES);
                        Unsafe.getUnsafe().putLong(sampleAddr + (long) sampled * Long.BYTES, val);
                        sampled++;
                        if (sampled >= sampleSize) break outer;
                    }
                    globalIdx++;
                }
            }
            symbolTable = FSST.train(sampleAddr, sampled);
        } finally {
            Unsafe.free(sampleAddr, (long) sampleSize * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }

        // Serialize symbol table to key file (once)
        long stBuf = Unsafe.malloc(FSST.SERIALIZED_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            int stSize = FSST.serialize(symbolTable, stBuf);
            long stFileOffset = SYMBOL_TABLE_OFFSET;
            for (int i = 0; i < stSize; i++) {
                keyMem.putByte(stFileOffset + i, Unsafe.getUnsafe().getByte(stBuf + i));
            }
        } finally {
            Unsafe.free(stBuf, FSST.SERIALIZED_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void freeNativeBuffers() {
        if (pendingValuesAddr != 0) {
            Unsafe.free(pendingValuesAddr, (long) keyCapacity * blockValues * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
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

        long oldValSize = (long) keyCapacity * blockValues * Long.BYTES;
        long newValSize = (long) newCapacity * blockValues * Long.BYTES;
        pendingValuesAddr = Unsafe.realloc(pendingValuesAddr, oldValSize, newValSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().setMemory(pendingValuesAddr + oldValSize, newValSize - oldValSize, (byte) 0);

        long oldCountSize = (long) keyCapacity * Integer.BYTES;
        long newCountSize = (long) newCapacity * Integer.BYTES;
        pendingCountsAddr = Unsafe.realloc(pendingCountsAddr, oldCountSize, newCountSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().setMemory(pendingCountsAddr + oldCountSize, newCountSize - oldCountSize, (byte) 0);

        keyCapacity = newCapacity;
    }

    private void updateHeaderAtomically(int genCount, long maxValue) {
        long seq = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE) + 1;
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE, seq);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE, valueMemSize);
        keyMem.putInt(KEY_RESERVED_OFFSET_KEY_COUNT, keyCount);
        keyMem.putInt(KEY_RESERVED_OFFSET_GEN_COUNT, genCount);
        keyMem.putLong(KEY_RESERVED_OFFSET_MAX_VALUE, maxValue);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE_CHECK, seq);
    }

    private static class TestBwdCursor implements RowCursor {
        private final LongList values;
        private int position;

        TestBwdCursor(LongList values) {
            this.values = values;
            this.position = values.size() - 1;
        }

        @Override
        public boolean hasNext() {
            return position >= 0;
        }

        @Override
        public long next() {
            return values.getQuick(position--);
        }
    }
}
