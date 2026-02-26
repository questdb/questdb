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
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

/**
 * Forward reader for FSST-compressed bitmap index.
 * <p>
 * Reads per-key offsets from each generation to decode only the requested
 * key's values — O(count) per key, no full-generation decompression needed.
 */
public class FSSTBitmapIndexFwdReader implements BitmapIndexReader {
    private static final String INDEX_CORRUPT = "cursor could not consistently read index header [corrupt?]";
    private static final Log LOG = LogFactory.getLog(FSSTBitmapIndexFwdReader.class);

    protected final MemoryMR keyMem = Vm.getCMRInstance();
    protected final MemoryMR valueMem = Vm.getCMRInstance();
    private final Cursor cursor = new Cursor();
    protected MillisecondClock clock;
    protected long columnTop;
    protected int keyCount;
    protected long spinLockTimeoutMs;
    private int blockValues;
    private long columnTxn;
    private int genCount;
    private int keyCountIncludingNulls;
    private long keyFileSequence = -1;
    private long partitionTxn;
    private FSST.SymbolTable symbolTable;
    private long valueMemSize = -1;

    public FSSTBitmapIndexFwdReader(
            CairoConfiguration configuration,
            Path path,
            CharSequence name,
            long columnNameTxn,
            long partitionTxn,
            long columnTop
    ) {
        of(configuration, path, name, columnNameTxn, partitionTxn, columnTop);
    }

    @Override
    public void close() {
        Misc.free(keyMem);
        Misc.free(valueMem);
    }

    @Override
    public long getColumnTop() {
        return columnTop;
    }

    @Override
    public long getColumnTxn() {
        return columnTxn;
    }

    @Override
    public RowCursor getCursor(boolean cachedInstance, int key, long minValue, long maxValue) {
        if (key >= keyCount) {
            updateKeyCount();
        }

        if (key < keyCount) {
            final Cursor c = cachedInstance ? cursor : new Cursor();
            c.of(key, minValue, maxValue);
            return c;
        }

        return EmptyRowCursor.INSTANCE;
    }

    @Override
    public long getKeyBaseAddress() {
        return keyMem.addressOf(0);
    }

    @Override
    public int getKeyCount() {
        return keyCountIncludingNulls;
    }

    @Override
    public long getKeyMemorySize() {
        return keyMem.size();
    }

    @Override
    public long getPartitionTxn() {
        return partitionTxn;
    }

    @Override
    public long getValueBaseAddress() {
        return valueMem.addressOf(0);
    }

    @Override
    public int getValueBlockCapacity() {
        return 0;
    }

    @Override
    public long getValueMemorySize() {
        return valueMem.size();
    }

    @Override
    public boolean isOpen() {
        return keyMem.getFd() != -1;
    }

    @Override
    public void of(
            CairoConfiguration configuration,
            @Transient Path path,
            CharSequence columnName,
            long columnNameTxn,
            long partitionTxn,
            long columnTop
    ) {
        this.columnTop = columnTop;
        this.columnTxn = columnNameTxn;
        this.partitionTxn = partitionTxn;
        final int plen = path.size();
        this.spinLockTimeoutMs = configuration.getSpinLockTimeout();

        try {
            FilesFacade ff = configuration.getFilesFacade();
            LPSZ name = FSSTBitmapIndexUtils.keyFileName(path, columnName, columnNameTxn);
            keyMem.of(
                    ff,
                    name,
                    ff.getMapPageSize(),
                    FSSTBitmapIndexUtils.KEY_FILE_RESERVED,
                    MemoryTag.MMAP_INDEX_READER,
                    CairoConfiguration.O_NONE,
                    -1
            );
            this.clock = configuration.getMillisecondClock();

            if (keyMem.getByte(FSSTBitmapIndexUtils.KEY_RESERVED_OFFSET_SIGNATURE) != FSSTBitmapIndexUtils.SIGNATURE) {
                LOG.error().$("unknown format [corrupt] ").$(path).$();
                throw CairoException.critical(0).put("Unknown format: ").put(path);
            }

            readIndexMetadataAtomically();

            this.valueMem.of(
                    configuration.getFilesFacade(),
                    FSSTBitmapIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn),
                    valueMemSize,
                    valueMemSize,
                    MemoryTag.MMAP_INDEX_READER
            );
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    @Override
    public void reloadConditionally() {
        long seq = keyMem.getLong(FSSTBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK);
        if (seq != keyFileSequence) {
            readIndexMetadataAtomically();
            long keyFileSize = FSSTBitmapIndexUtils.getGenDirOffset(genCount);
            this.keyMem.extend(keyFileSize);
            this.valueMem.extend(valueMemSize);
        }
    }

    public void updateKeyCount() {
        int keyCount;
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            long seq = keyMem.getLong(FSSTBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
            Unsafe.getUnsafe().loadFence();
            if (keyMem.getLong(FSSTBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {
                keyCount = keyMem.getInt(FSSTBitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
                Unsafe.getUnsafe().loadFence();
                if (seq == keyMem.getLong(FSSTBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE)) {
                    break;
                }
            }
            if (clock.getTicks() > deadline) {
                this.keyCount = 0;
                LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutMs).$("ms]").$();
                throw CairoException.critical(0).put(INDEX_CORRUPT);
            }
            Os.pause();
        }

        if (keyCount > this.keyCount) {
            this.keyCount = keyCount;
            this.keyCountIncludingNulls = columnTop > 0 ? keyCount + 1 : keyCount;
            this.genCount = keyMem.getInt(FSSTBitmapIndexUtils.KEY_RESERVED_OFFSET_GEN_COUNT);
            long keyFileSize = FSSTBitmapIndexUtils.getGenDirOffset(genCount);
            keyMem.extend(keyFileSize);
        }
    }

    private void readIndexMetadataAtomically() {
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            long seq = keyMem.getLong(FSSTBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
            Unsafe.getUnsafe().loadFence();
            if (keyMem.getLong(FSSTBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {
                int keyCount = keyMem.getInt(FSSTBitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
                long valueMemSize = keyMem.getLong(FSSTBitmapIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
                int blockValues = keyMem.getInt(FSSTBitmapIndexUtils.KEY_RESERVED_OFFSET_BLOCK_VALUES);
                int genCount = keyMem.getInt(FSSTBitmapIndexUtils.KEY_RESERVED_OFFSET_GEN_COUNT);

                Unsafe.getUnsafe().loadFence();
                if (keyMem.getLong(FSSTBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE) == seq) {
                    this.keyFileSequence = seq;
                    this.valueMemSize = valueMemSize;
                    this.keyCount = keyCount;
                    this.blockValues = blockValues;
                    this.genCount = genCount;
                    this.keyCountIncludingNulls = columnTop > 0 ? keyCount + 1 : keyCount;

                    // Deserialize symbol table
                    if (genCount > 0) {
                        long keyFileSize = FSSTBitmapIndexUtils.getGenDirOffset(genCount);
                        keyMem.extend(keyFileSize);
                        this.symbolTable = FSST.deserialize(keyMem.addressOf(FSSTBitmapIndexUtils.SYMBOL_TABLE_OFFSET));
                    }

                    long keyFileSize = FSSTBitmapIndexUtils.getGenDirOffset(genCount);
                    keyMem.extend(keyFileSize);
                    break;
                }
            }

            if (clock.getTicks() > deadline) {
                LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutMs).$("ms]").$();
                throw CairoException.critical(0).put(INDEX_CORRUPT);
            }
            Os.pause();
        }
    }

    private class Cursor implements RowCursor {
        protected long next;
        private int currentGen;
        private long encodedAddr;
        private int encodedLen;
        private int encodedOff;
        private long maxValue;
        private long minValue;
        private int requestedKey;
        private int roundIndex;
        private int valueCount;

        @Override
        public boolean hasNext() {
            while (true) {
                while (roundIndex < valueCount) {
                    long value = decodeNextValue();
                    roundIndex++;

                    if (value > maxValue) {
                        return false;
                    }
                    if (value >= minValue) {
                        this.next = value;
                        return true;
                    }
                }
                currentGen++;
                if (currentGen >= genCount) {
                    return false;
                }
                loadGeneration();
            }
        }

        @Override
        public long next() {
            return next;
        }

        void of(int key, long minValue, long maxValue) {
            if (keyCount == 0 || key < 0 || key >= keyCount || genCount == 0) {
                valueCount = 0;
                currentGen = genCount;
                return;
            }

            this.requestedKey = key;
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.currentGen = 0;
            loadGeneration();
        }

        private long decodeNextValue() {
            long result = 0;
            int bytePos = 0;
            while (bytePos < 8 && encodedOff < encodedLen) {
                int code = Unsafe.getUnsafe().getByte(encodedAddr + encodedOff) & 0xFF;
                encodedOff++;
                if (code == FSST.ESCAPE) {
                    if (encodedOff < encodedLen) {
                        int literal = Unsafe.getUnsafe().getByte(encodedAddr + encodedOff) & 0xFF;
                        encodedOff++;
                        result |= ((long) literal) << (bytePos * 8);
                        bytePos++;
                    }
                } else {
                    int len = symbolTable.lens[code];
                    long sym = symbolTable.symbols[code];
                    for (int b = 0; b < len && bytePos < 8; b++) {
                        result |= ((sym >>> (b * 8)) & 0xFFL) << (bytePos * 8);
                        bytePos++;
                    }
                }
            }
            return result;
        }

        private void loadGeneration() {
            long dirOffset = FSSTBitmapIndexUtils.getGenDirOffset(currentGen);
            keyMem.extend(dirOffset + FSSTBitmapIndexUtils.GEN_DIR_ENTRY_SIZE);
            long genFileOffset = keyMem.getLong(dirOffset + FSSTBitmapIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET);
            int genDataSize = keyMem.getInt(dirOffset + FSSTBitmapIndexUtils.GEN_DIR_OFFSET_SIZE);

            valueMem.extend(genFileOffset + genDataSize);
            long genAddr = valueMem.addressOf(genFileOffset);
            int headerSize = FSSTBitmapIndexUtils.genHeaderSize(keyCount);

            this.valueCount = Unsafe.getUnsafe().getInt(genAddr + (long) requestedKey * Integer.BYTES);
            if (valueCount == 0) {
                this.roundIndex = 0;
                return;
            }

            int dataOffset = Unsafe.getUnsafe().getInt(genAddr + (long) keyCount * Integer.BYTES + (long) requestedKey * Integer.BYTES);
            int nextOffset;
            if (requestedKey + 1 < keyCount) {
                nextOffset = Unsafe.getUnsafe().getInt(genAddr + (long) keyCount * Integer.BYTES + (long) (requestedKey + 1) * Integer.BYTES);
            } else {
                nextOffset = genDataSize - headerSize;
            }
            this.encodedLen = nextOffset - dataOffset;
            this.encodedAddr = genAddr + headerSize + dataOffset;
            this.encodedOff = 0;
            this.roundIndex = 0;
        }
    }
}
