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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;

/**
 * Forward reader for Delta + FoR64 BitPacking bitmap index.
 * <p>
 * Block-buffered decode: unpacks 64 values at a time from FoR64 blocks.
 * Generation iteration uses PostingGenLookup for tiered gen-to-key mapping.
 */
public class PostingIndexFwdReader extends AbstractPostingIndexReader {
    private final Cursor cursor = new Cursor();
    private ObjList<Cursor> extraCursors;

    @Override
    public void close() {
        cursor.close();
        if (extraCursors != null) {
            for (int i = 0, n = extraCursors.size(); i < n; i++) {
                extraCursors.getQuick(i).close();
            }
            extraCursors.clear();
        }
        super.close();
    }

    public PostingIndexFwdReader(
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
    public RowCursor getCursor(boolean cachedInstance, int key, long minValue, long maxValue) {
        if (key >= keyCount) {
            updateKeyCount();
        }

        if (key < keyCount) {
            final Cursor c;
            if (cachedInstance) {
                c = cursor;
            } else {
                // Non-cached: create a fresh cursor for concurrent use (IN-list).
                // Track it for cleanup in close().
                c = new Cursor();
                if (extraCursors == null) {
                    extraCursors = new ObjList<>();
                }
                extraCursors.add(c);
            }
            c.of(key, minValue, maxValue);
            return c;
        }

        return EmptyRowCursor.INSTANCE;
    }

    private class Cursor implements CoveringRowCursor {
        private long blockBufferAddr = Unsafe.malloc((long) PostingIndexUtils.PACKED_BATCH_SIZE * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        private long blockDeltasAddr = Unsafe.malloc((long) PostingIndexUtils.BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        private long next;
        private int blockBufferPos;
        private int blockBufferEnd;
        private int currentGen;
        private long encodedAddr;
        private int encodedBlockCount;
        private int currentBlock;
        private long maxValue;
        private long minValue;
        private int requestedKey;
        private int totalValueCount;
        private int metadataCapacity = 256;
        private long valueCountsAddr = Unsafe.malloc(256L * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
        private long firstValuesAddr = Unsafe.malloc(256L * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        private long minDeltasAddr = Unsafe.malloc(256L * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        private long bitWidthsAddr = Unsafe.malloc(256L * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
        private long packedDataAddr;
        private boolean flatMode;
        private int flatBitWidth;
        private long flatBaseValue;
        private long flatDataBase;
        private int flatStartIdx;
        private int flatRemaining;
        private int lookupPos;
        private int lookupEnd;
        private int sidecarOrdinal;
        private int sidecarStrideKeyStart;
        private int sealedGenKeyCount;
        private boolean isCurrentGenDense;
        private int[] currentGenSidecarOffsets; // per-column offset into .pc* for raw sidecar block
        private int denseVarKeyStartCount; // prefix count for this key in the stride (for var-sized columns)
        private double[][] decodedDoubles;
        private int[][] decodedInts;
        private long[][] decodedLongs;
        private long decodeWorkspaceAddr; // reusable native workspace for ALP decompress
        private int decodeWorkspaceCapacity;
        private int decodedStride = -1;

        @Override
        public byte getCoveredByte(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarByte(includeIdx);
            }
            int idx = sidecarValueIndex();
            if (idx < 0 || decodedInts == null || decodedInts[includeIdx] == null) {
                return 0;
            }
            return (byte) decodedInts[includeIdx][idx];
        }

        @Override
        public double getCoveredDouble(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarDouble(includeIdx);
            }
            int idx = sidecarValueIndex();
            if (idx < 0 || decodedDoubles == null || decodedDoubles[includeIdx] == null) {
                return Double.NaN;
            }
            return decodedDoubles[includeIdx][idx];
        }

        @Override
        public float getCoveredFloat(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarFloat(includeIdx);
            }
            int idx = sidecarValueIndex();
            if (idx < 0 || decodedInts == null || decodedInts[includeIdx] == null) {
                return Float.NaN;
            }
            return Float.intBitsToFloat(decodedInts[includeIdx][idx]);
        }

        @Override
        public int getCoveredInt(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarInt(includeIdx);
            }
            int idx = sidecarValueIndex();
            if (idx < 0 || decodedInts == null || decodedInts[includeIdx] == null) {
                return Integer.MIN_VALUE;
            }
            return decodedInts[includeIdx][idx];
        }

        @Override
        public long getCoveredLong(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarLong(includeIdx);
            }
            int idx = sidecarValueIndex();
            if (idx < 0 || decodedLongs == null || decodedLongs[includeIdx] == null) {
                return Long.MIN_VALUE;
            }
            return decodedLongs[includeIdx][idx];
        }

        @Override
        public short getCoveredShort(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarShort(includeIdx);
            }
            int idx = sidecarValueIndex();
            if (idx < 0 || decodedInts == null || decodedInts[includeIdx] == null) {
                return 0;
            }
            return (short) decodedInts[includeIdx][idx];
        }

        @Override
        public boolean hasCovering() {
            return coverCount > 0 && sidecarMems != null;
        }

        // Raw sidecar readers for sparse (unsealed) gens.
        // Format: [valueCount: 4B][raw values: valueCount × elemSize]
        // sidecarOrdinal tracks position within this gen's block.
        // Element sizes (Double.BYTES, Integer.BYTES, etc.) match the writer's
        // writeSidecarValue() which uses 1 << shift for each column type.
        // Each .pc{N} file contains a single column type, so there is no
        // cross-type confusion even with multiple covered columns.

        private double getRawSidecarDouble(int includeIdx) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return Double.NaN;
            long addr = sidecarMems[includeIdx].addressOf(currentGenSidecarOffsets[includeIdx] + 4 + (long) (sidecarOrdinal - 1) * Double.BYTES);
            return Unsafe.getUnsafe().getDouble(addr);
        }

        private float getRawSidecarFloat(int includeIdx) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return Float.NaN;
            long addr = sidecarMems[includeIdx].addressOf(currentGenSidecarOffsets[includeIdx] + 4 + (long) (sidecarOrdinal - 1) * Float.BYTES);
            return Unsafe.getUnsafe().getFloat(addr);
        }

        private int getRawSidecarInt(int includeIdx) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return Integer.MIN_VALUE;
            long addr = sidecarMems[includeIdx].addressOf(currentGenSidecarOffsets[includeIdx] + 4 + (long) (sidecarOrdinal - 1) * Integer.BYTES);
            return Unsafe.getUnsafe().getInt(addr);
        }

        private long getRawSidecarLong(int includeIdx) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return Long.MIN_VALUE;
            long addr = sidecarMems[includeIdx].addressOf(currentGenSidecarOffsets[includeIdx] + 4 + (long) (sidecarOrdinal - 1) * Long.BYTES);
            return Unsafe.getUnsafe().getLong(addr);
        }

        private short getRawSidecarShort(int includeIdx) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return 0;
            long addr = sidecarMems[includeIdx].addressOf(currentGenSidecarOffsets[includeIdx] + 4 + (long) (sidecarOrdinal - 1) * Short.BYTES);
            return Unsafe.getUnsafe().getShort(addr);
        }

        private byte getRawSidecarByte(int includeIdx) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return 0;
            long addr = sidecarMems[includeIdx].addressOf(currentGenSidecarOffsets[includeIdx] + 4 + (long) (sidecarOrdinal - 1) * Byte.BYTES);
            return Unsafe.getUnsafe().getByte(addr);
        }

        // Var-sized sidecar readers. Both per-gen and per-stride var-sized blocks use
        // the same format: [count:4B][offsets:(count+1)×4B][concatenated bytes].
        // The offset pair [offsets[ordinal], offsets[ordinal+1]] defines the byte range.
        // Zero-length range means NULL.

        private final DirectUtf8String varcharViewA = new DirectUtf8String();
        private final DirectUtf8String varcharViewB = new DirectUtf8String();
        private final DirectString stringViewA = new DirectString();
        private final DirectString stringViewB = new DirectString();

        @Override
        public Utf8Sequence getCoveredVarcharA(int includeIdx) {
            return getVarSidecarUtf8(includeIdx, varcharViewA);
        }

        @Override
        public Utf8Sequence getCoveredVarcharB(int includeIdx) {
            return getVarSidecarUtf8(includeIdx, varcharViewB);
        }

        @Override
        public CharSequence getCoveredStrA(int includeIdx) {
            return getVarSidecarStr(includeIdx, stringViewA);
        }

        @Override
        public CharSequence getCoveredStrB(int includeIdx) {
            return getVarSidecarStr(includeIdx, stringViewB);
        }

        private Utf8Sequence getVarSidecarUtf8(int includeIdx, DirectUtf8String view) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return null;
            // For dense (sealed): ordinal = prefix count for this key + per-key ordinal
            // For sparse (unsealed): use gen-global ordinal
            int ordinal = isCurrentGenDense
                    ? denseVarKeyStartCount + sidecarOrdinal - 1
                    : sidecarOrdinal - 1;
            long blockBase = isCurrentGenDense
                    ? findDenseVarBlockBase(includeIdx)
                    : currentGenSidecarOffsets[includeIdx];
            if (blockBase < 0) return null;
            MemoryMR mem = sidecarMems[includeIdx];
            int count = Unsafe.getUnsafe().getInt(mem.addressOf(blockBase));
            if (ordinal >= count) return null;
            long offsetsAddr = mem.addressOf(blockBase + 4);
            int lo = Unsafe.getUnsafe().getInt(offsetsAddr + (long) ordinal * Integer.BYTES);
            int hi = Unsafe.getUnsafe().getInt(offsetsAddr + (long) (ordinal + 1) * Integer.BYTES);
            if (lo == hi) return null; // NULL
            long dataBase = blockBase + 4 + (long) (count + 1) * Integer.BYTES;
            long dataAddr = mem.addressOf(dataBase + lo);
            return view.of(dataAddr, dataAddr + (hi - lo));
        }

        private CharSequence getVarSidecarStr(int includeIdx, DirectString view) {
            if (sidecarMems == null || sidecarMems[includeIdx] == null) return null;
            int ordinal = isCurrentGenDense
                    ? denseVarKeyStartCount + sidecarOrdinal - 1
                    : sidecarOrdinal - 1;
            long blockBase = isCurrentGenDense
                    ? findDenseVarBlockBase(includeIdx)
                    : currentGenSidecarOffsets[includeIdx];
            if (blockBase < 0) return null;
            MemoryMR mem = sidecarMems[includeIdx];
            int count = Unsafe.getUnsafe().getInt(mem.addressOf(blockBase));
            if (ordinal >= count) return null;
            long offsetsAddr = mem.addressOf(blockBase + 4);
            int lo = Unsafe.getUnsafe().getInt(offsetsAddr + (long) ordinal * Integer.BYTES);
            int hi = Unsafe.getUnsafe().getInt(offsetsAddr + (long) (ordinal + 1) * Integer.BYTES);
            if (lo == hi) return null; // NULL
            long dataBase = blockBase + 4 + (long) (count + 1) * Integer.BYTES;
            long dataAddr = mem.addressOf(dataBase + lo);
            // STRING sidecar stores [len:4B][UTF-16 chars]
            int len = Unsafe.getUnsafe().getInt(dataAddr);
            if (len < 0) return null;
            return view.of(dataAddr + Integer.BYTES, len);
        }

        /**
         * Computes per-column sidecar offsets for the given gen by scanning
         * through previous gen blocks in each sidecar file. Each gen block
         * starts with [count:4B]; block size depends on column type.
         */
        /**
         * Computes per-column sidecar offsets for the given sparse gen by
         * scanning through previous SPARSE gen blocks in each sidecar file.
         * Dense (sealed) gens use stride-indexed format in the sidecar, which
         * is not scannable as raw blocks. For gens after a dense gen 0, the
         * scan starts from the first sparse gen's position.
         */
        private void computePerColumnSidecarOffsets(int gen) {
            if (sidecarMems == null || sidecarColumnTypes == null || coverCount == 0) {
                return;
            }
            if (currentGenSidecarOffsets == null || currentGenSidecarOffsets.length < coverCount) {
                currentGenSidecarOffsets = new int[coverCount];
            }

            // Find the first sparse gen (skip dense gens which have stride-indexed sidecars)
            int firstSparseGen = 0;
            while (firstSparseGen < gen && genLookup.getGenKeyCount(firstSparseGen) >= 0) {
                firstSparseGen++;
            }
            // per-column offset computation follows

            for (int c = 0; c < coverCount; c++) {
                if (sidecarMems[c] == null) {
                    currentGenSidecarOffsets[c] = 0;
                    continue;
                }

                long offset;
                if (firstSparseGen == 0) {
                    // No dense gens before this one — scan from file start
                    offset = 0;
                } else if (c == 0) {
                    // Column 0: use gen dir sidecar offset (exact for column 0)
                    offset = genLookup.getGenSidecarOffset(firstSparseGen);
                } else {
                    // Other columns: compute sealed data size from stride index sentinel.
                    // Each column's sealed sidecar has different sizes due to compression.
                    offset = computeSealedSidecarSize(sidecarMems[c], sealedGenKeyCount);
                }

                int colType = sidecarColumnTypes[c];
                boolean isVar = ColumnType.isVarSize(colType);
                int elemSize = isVar ? 0 : ColumnType.sizeOf(colType);

                // Scan through sparse gen blocks from firstSparseGen to gen-1
                for (int g = firstSparseGen; g < gen; g++) {
                    if (genLookup.getGenKeyCount(g) >= 0) {
                        continue; // skip dense gens (shouldn't happen after firstSparseGen)
                    }
                    if (offset + 4 > sidecarMems[c].size()) {
                        break;
                    }
                    int count = Unsafe.getUnsafe().getInt(sidecarMems[c].addressOf(offset));
                    if (isVar) {
                        if (count == 0) {
                            offset += 4;
                        } else {
                            long sentinelPos = offset + 4 + (long) count * Integer.BYTES;
                            int dataSize = Unsafe.getUnsafe().getInt(sidecarMems[c].addressOf(sentinelPos));
                            offset += 4 + (long) (count + 1) * Integer.BYTES + dataSize;
                        }
                    } else {
                        offset += 4 + (long) count * elemSize;
                    }
                }
                currentGenSidecarOffsets[c] = (int) offset;
            }
        }

        /**
         * Computes the total size of sealed (stride-indexed) sidecar data by
         * reading the stride index sentinel. Format: [strideIndex: (sc+1)×4B]
         * [stride data]. The sentinel at strideIndex[sc] gives total data size.
         */
        private long computeSealedSidecarSize(MemoryMR mem, int keyCount) {
            if (keyCount <= 0 || mem == null || mem.size() < 4) {
                return 0;
            }
            int sc = PostingIndexUtils.strideCount(keyCount);
            int siSize = PostingIndexUtils.strideIndexSize(keyCount);
            long sentinelPos = (long) sc * Integer.BYTES;
            if (sentinelPos + Integer.BYTES > mem.size()) {
                return 0;
            }
            int totalStrideDataSize = Unsafe.getUnsafe().getInt(mem.addressOf(sentinelPos));
            // siSize + totalStrideDataSize = total sealed bytes in this sidecar file
            return siSize + totalStrideDataSize;
        }

        private long findDenseVarBlockBase(int includeIdx) {
            // For dense (sealed) gens, the sidecar file has a stride index at
            // the beginning. Each stride's var-block is at siSize + strideOffset.
            if (sidecarMems == null || sidecarMems[includeIdx] == null || sealedGenKeyCount <= 0) {
                return -1;
            }
            int stride = requestedKey / PostingIndexUtils.DENSE_STRIDE;
            int siSize = PostingIndexUtils.strideIndexSize(sealedGenKeyCount);
            MemoryMR mem = sidecarMems[includeIdx];
            if ((long) stride * Integer.BYTES + Integer.BYTES > mem.size()) {
                return -1;
            }
            int strideOff = Unsafe.getUnsafe().getInt(mem.addressOf((long) stride * Integer.BYTES));
            return siSize + strideOff;
        }

        @Override
        public long seekToLast() {
            long lastRowId = -1;
            while (hasNext()) {
                lastRowId = next();
            }
            return lastRowId;
        }

        private int sidecarValueIndex() {
            // sidecarStrideKeyStart tracks values skipped by block-skip (delta mode)
            // or binary search (flat mode) within this key's decoded sidecar block.
            // sidecarOrdinal counts values iterated (both emitted and filtered).
            return sidecarStrideKeyStart + sidecarOrdinal - 1;
        }

        private void decodeSidecarKey(int stride, int localKey) {
            if (sidecarMems == null || coverCount == 0 || sealedGenKeyCount <= 0) {
                return;
            }
            int sc = PostingIndexUtils.strideCount(sealedGenKeyCount);
            if (stride >= sc) {
                return;
            }
            int siSize = PostingIndexUtils.strideIndexSize(sealedGenKeyCount);
            int ks = PostingIndexUtils.keysInStride(sealedGenKeyCount, stride);
            if (localKey >= ks) {
                return;
            }

            if (decodedDoubles == null) {
                decodedDoubles = new double[coverCount][];
                decodedInts = new int[coverCount][];
                decodedLongs = new long[coverCount][];
            }

            for (int c = 0; c < coverCount; c++) {
                MemoryMR mem = sidecarMems[c];
                if (mem == null || mem.size() == 0) {
                    continue;
                }
                // Get stride data offset from stride index
                long strideIdxOffset = (long) stride * Integer.BYTES;
                if (strideIdxOffset + Integer.BYTES > mem.size()) {
                    continue;
                }
                int strideOff = mem.getInt(strideIdxOffset);
                long strideDataStart = siSize + strideOff;
                if (strideDataStart >= mem.size()) {
                    continue;
                }

                // Per-key layout: [key_offsets: ks × 4B][key_0_block][key_1_block]...
                long keyOffsetsAddr = mem.addressOf(strideDataStart);
                int keyBlockOff = Unsafe.getUnsafe().getInt(keyOffsetsAddr + (long) localKey * Integer.BYTES);
                long keyBlockAddr = mem.addressOf(strideDataStart + (long) ks * Integer.BYTES + keyBlockOff);
                int count = Unsafe.getUnsafe().getInt(keyBlockAddr);
                if (count == 0) {
                    continue;
                }
                int colType = sidecarColumnTypes[c];

                // Var-sized columns (VARCHAR, STRING) use offset-based format
                // in the sidecar, not ALP compression. Skip ALP decoding —
                // getCoveredVarcharA/getCoveredStrA reads directly from the
                // sidecar memory using the stride's var-block position.
                if (ColumnType.isVarSize(colType)) {
                    continue;
                }

                switch (ColumnType.tagOf(colType)) {
                    case ColumnType.DOUBLE: {
                        if (decodedDoubles[c] == null || decodedDoubles[c].length < count) {
                            decodedDoubles[c] = new double[count];
                        }
                        ensureDecodeWorkspaceCapacity(count);
                        AlpCompression.decompressDoubles(keyBlockAddr, decodedDoubles[c], decodeWorkspaceAddr);
                        break;
                    }
                    case ColumnType.LONG:
                    case ColumnType.TIMESTAMP:
                    case ColumnType.DATE:
                    case ColumnType.GEOLONG: {
                        if (decodedLongs[c] == null || decodedLongs[c].length < count) {
                            decodedLongs[c] = new long[count];
                        }
                        ensureDecodeWorkspaceCapacity(count);
                        AlpCompression.decompressLongs(keyBlockAddr, decodedLongs[c], decodeWorkspaceAddr);
                        break;
                    }
                    case ColumnType.INT:
                    case ColumnType.FLOAT:
                    case ColumnType.GEOINT:
                    case ColumnType.SYMBOL: {
                        if (decodedInts[c] == null || decodedInts[c].length < count) {
                            decodedInts[c] = new int[count];
                        }
                        ensureDecodeWorkspaceCapacity(count);
                        AlpCompression.decompressInts(keyBlockAddr, decodedInts[c], decodeWorkspaceAddr);
                        break;
                    }
                    case ColumnType.SHORT:
                    case ColumnType.GEOSHORT: {
                        if (decodedInts[c] == null || decodedInts[c].length < count) {
                            decodedInts[c] = new int[count];
                        }
                        long rawAddr = keyBlockAddr + 4; // skip count header
                        for (int i = 0; i < count; i++) {
                            decodedInts[c][i] = Unsafe.getUnsafe().getShort(rawAddr + (long) i * Short.BYTES);
                        }
                        break;
                    }
                    case ColumnType.BYTE:
                    case ColumnType.BOOLEAN:
                    case ColumnType.GEOBYTE: {
                        if (decodedInts[c] == null || decodedInts[c].length < count) {
                            decodedInts[c] = new int[count];
                        }
                        long rawAddr = keyBlockAddr + 4; // skip count header
                        for (int i = 0; i < count; i++) {
                            decodedInts[c][i] = Unsafe.getUnsafe().getByte(rawAddr + i);
                        }
                        break;
                    }
                    default:
                        throw new UnsupportedOperationException("unsupported sidecar column type: " + ColumnType.nameOf(colType));
                }
            }
            decodedStride = stride;
        }

        @Override
        public boolean hasNext() {
            while (true) {
                // Serve from block buffer first
                while (blockBufferPos < blockBufferEnd) {
                    long value = Unsafe.getUnsafe().getLong(blockBufferAddr + (long) blockBufferPos * Long.BYTES);
                    blockBufferPos++;

                    if (value > maxValue) {
                        return false;
                    }
                    if (value >= minValue) {
                        this.next = value;
                        if (coverCount > 0) sidecarOrdinal++;
                        return true;
                    }
                    if (coverCount > 0) sidecarOrdinal++;
                }

                // Try to decode next block in current generation
                if (currentBlock < encodedBlockCount) {
                    decodeNextBlock();
                    continue;
                }

                // Flat mode: decode next batch if remaining
                if (flatMode && flatRemaining > 0) {
                    decodeNextFlatBatch();
                    continue;
                }

                // Move to next relevant generation
                if (!advanceToNextRelevantGen()) {
                    return false;
                }
            }
        }

        @Override
        public long next() {
            return next - minValue;
        }

        void of(int key, long minValue, long maxValue) {
            if (keyCount == 0 || key < 0 || key >= keyCount || genCount == 0) {
                totalValueCount = 0;
                currentGen = genCount;
                encodedBlockCount = 0;
                currentBlock = 0;
                blockBufferPos = 0;
                blockBufferEnd = 0;
                return;
            }

            ensureGenLookup();

            this.requestedKey = key;
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.currentGen = -1; // will be advanced by first advanceToNextRelevantGen()

            // Set up inverted index range for this key (Tier 1)
            if (genLookup.isPerKeyMode() && key < genLookup.getKeyCount()) {
                this.lookupPos = genLookup.getEntryStart(key);
                this.lookupEnd = genLookup.getEntryEnd(key);
            } else {
                this.lookupPos = 0;
                this.lookupEnd = 0;
            }

            // Kick off iteration
            this.encodedBlockCount = 0;
            this.currentBlock = 0;
            this.blockBufferPos = 0;
            this.blockBufferEnd = 0;
            this.sidecarOrdinal = 0;
            this.sidecarStrideKeyStart = 0;
            this.decodedStride = -1;
            advanceToNextRelevantGen();
        }

        private boolean advanceToNextRelevantGen() {
            int lookupTier = genLookup.getTier();

            if (lookupTier == PostingGenLookup.TIER_PER_KEY) {
                return advanceWithPerKeyLookup();
            } else if (lookupTier == PostingGenLookup.TIER_SBBF) {
                return advanceWithSbbfLookup();
            } else {
                return advanceWithBinarySearch();
            }
        }

        /**
         * Tier 1: Direct jumps via inverted index — O(hitGens) per key.
         */
        private boolean advanceWithPerKeyLookup() {
            while (true) {
                if (lookupPos < lookupEnd) {
                    int nextSparseGen = genLookup.getGenIndex(lookupPos);

                    // Check dense gens before this sparse gen
                    currentGen++;
                    while (currentGen < nextSparseGen) {
                        if (genLookup.getGenKeyCount(currentGen) >= 0) {
                            loadDenseGenerationCached(currentGen);
                            return true;
                        }
                        currentGen++;
                    }

                    // Load sparse gen directly
                    int posInGen = genLookup.getPosInGen(lookupPos);
                    lookupPos++;
                    currentGen = nextSparseGen;
                    loadSparseGenDirect(currentGen, posInGen);
                    return true;
                }

                // No more sparse hits — check remaining dense gens
                currentGen++;
                while (currentGen < genCount) {
                    if (genLookup.getGenKeyCount(currentGen) >= 0) {
                        loadDenseGenerationCached(currentGen);
                        return true;
                    }
                    currentGen++;
                }
                return false;
            }
        }

        /**
         * Tier 2: SBBF probe per gen — skip gens where key definitely absent.
         */
        private boolean advanceWithSbbfLookup() {
            currentGen++;
            while (currentGen < genCount) {
                int gkc = genLookup.getGenKeyCount(currentGen);
                if (gkc >= 0) {
                    // Dense gen
                    loadDenseGenerationCached(currentGen);
                    return true;
                }

                // Sparse gen: check min/max bounds first
                if (requestedKey < genLookup.getGenMinKey(currentGen) ||
                        requestedKey > genLookup.getGenMaxKey(currentGen)) {
                    currentGen++;
                    continue;
                }

                // SBBF probe
                if (!genLookup.mightContainKey(currentGen, requestedKey)) {
                    currentGen++;
                    continue;
                }

                // SBBF says "maybe" — fall back to binary search within this gen
                loadSparseGenWithBinarySearch(currentGen);
                if (totalValueCount > 0 || encodedBlockCount > 0 || flatMode) {
                    return true;
                }
                // False positive — key not actually in this gen
                currentGen++;
            }
            return false;
        }

        /**
         * Tier 3: Binary search + min/max bounds check per gen.
         */
        private boolean advanceWithBinarySearch() {
            currentGen++;
            while (currentGen < genCount) {
                int gkc = genLookup.getGenKeyCount(currentGen);
                if (gkc >= 0) {
                    loadDenseGenerationCached(currentGen);
                    return true;
                }

                // Min/max bounds check from cached metadata
                if (requestedKey < genLookup.getGenMinKey(currentGen) ||
                        requestedKey > genLookup.getGenMaxKey(currentGen)) {
                    currentGen++;
                    continue;
                }

                loadSparseGenWithBinarySearch(currentGen);
                if (totalValueCount > 0 || encodedBlockCount > 0 || flatMode) {
                    return true;
                }
                currentGen++;
            }
            return false;
        }

        private void clearBlockState() {
            this.encodedBlockCount = 0;
            this.currentBlock = 0;
            this.blockBufferPos = 0;
            this.blockBufferEnd = 0;
        }

        private void decodeNextBlock() {
            int b = currentBlock;
            int count = Unsafe.getUnsafe().getInt(valueCountsAddr + (long) b * Integer.BYTES);
            int bitWidth = Unsafe.getUnsafe().getInt(bitWidthsAddr + (long) b * Integer.BYTES);
            int numDeltas = count - 1;

            long cumulative = Unsafe.getUnsafe().getLong(firstValuesAddr + (long) b * Long.BYTES);
            Unsafe.getUnsafe().putLong(blockBufferAddr, cumulative);

            if (numDeltas > 0) {
                long minD = Unsafe.getUnsafe().getLong(minDeltasAddr + (long) b * Long.BYTES);
                if (bitWidth == 0) {
                    // Constant delta: direct cumulative sum without intermediate buffer.
                    // Avoids writing 63 deltas to blockDeltasAddr then reading them back.
                    for (int i = 0; i < numDeltas; i++) {
                        cumulative += minD;
                        Unsafe.getUnsafe().putLong(blockBufferAddr + (long) (i + 1) * Long.BYTES, cumulative);
                    }
                } else {
                    BitpackUtils.unpackAllValues(packedDataAddr, numDeltas, bitWidth, minD, blockDeltasAddr);
                    packedDataAddr += BitpackUtils.packedDataSize(numDeltas, bitWidth);
                    for (int i = 0; i < numDeltas; i++) {
                        cumulative += Unsafe.getUnsafe().getLong(blockDeltasAddr + (long) i * Long.BYTES);
                        Unsafe.getUnsafe().putLong(blockBufferAddr + (long) (i + 1) * Long.BYTES, cumulative);
                    }
                }
            }

            blockBufferPos = 0;
            blockBufferEnd = count;
            currentBlock++;
        }

        private void decodeNextFlatBatch() {
            int batch = Math.min(flatRemaining, PostingIndexUtils.PACKED_BATCH_SIZE);
            BitpackUtils.unpackValuesFrom(flatDataBase, flatStartIdx, batch, flatBitWidth, flatBaseValue, blockBufferAddr);
            flatStartIdx += batch;
            flatRemaining -= batch;
            blockBufferPos = 0;
            blockBufferEnd = batch;
        }

        private void ensureMetadataCapacity(int needed) {
            if (needed > metadataCapacity) {
                int newCapacity = Math.max(needed, metadataCapacity * 2);
                long newFirstAddr = Unsafe.malloc((long) newCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                long newMinAddr = Unsafe.malloc((long) newCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                Unsafe.free(firstValuesAddr, (long) metadataCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                Unsafe.free(minDeltasAddr, (long) metadataCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                firstValuesAddr = newFirstAddr;
                minDeltasAddr = newMinAddr;
                long newValueCountsAddr = Unsafe.malloc((long) newCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                long newBitWidthsAddr;
                try {
                    newBitWidthsAddr = Unsafe.malloc((long) newCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                } catch (Throwable e) {
                    Unsafe.free(newValueCountsAddr, (long) newCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                    throw e;
                }
                if (valueCountsAddr != 0) {
                    Unsafe.free(valueCountsAddr, (long) metadataCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                if (bitWidthsAddr != 0) {
                    Unsafe.free(bitWidthsAddr, (long) metadataCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                valueCountsAddr = newValueCountsAddr;
                bitWidthsAddr = newBitWidthsAddr;
                metadataCapacity = newCapacity;
            }
        }

        private void ensureDecodeWorkspaceCapacity(int count) {
            if (count > decodeWorkspaceCapacity) {
                if (decodeWorkspaceAddr != 0) {
                    Unsafe.free(decodeWorkspaceAddr, (long) decodeWorkspaceCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                decodeWorkspaceCapacity = count;
                decodeWorkspaceAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
        }

        void close() {
            if (blockBufferAddr != 0) {
                Unsafe.free(blockBufferAddr, (long) PostingIndexUtils.PACKED_BATCH_SIZE * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                blockBufferAddr = 0;
            }
            if (blockDeltasAddr != 0) {
                Unsafe.free(blockDeltasAddr, (long) PostingIndexUtils.BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                blockDeltasAddr = 0;
            }
            if (firstValuesAddr != 0) {
                Unsafe.free(firstValuesAddr, (long) metadataCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                firstValuesAddr = 0;
            }
            if (minDeltasAddr != 0) {
                Unsafe.free(minDeltasAddr, (long) metadataCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                minDeltasAddr = 0;
            }
            if (valueCountsAddr != 0) {
                Unsafe.free(valueCountsAddr, (long) metadataCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                valueCountsAddr = 0;
            }
            if (bitWidthsAddr != 0) {
                Unsafe.free(bitWidthsAddr, (long) metadataCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                bitWidthsAddr = 0;
            }
            if (decodeWorkspaceAddr != 0) {
                Unsafe.free(decodeWorkspaceAddr, (long) decodeWorkspaceCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                decodeWorkspaceAddr = 0;
            }
        }

        /**
         * Loads a dense generation using cached metadata from PostingGenLookup.
         */
        private void loadDenseGenerationCached(int gen) {
            this.isCurrentGenDense = true;
            this.sidecarOrdinal = 0;
            long genFileOffset = genLookup.getGenFileOffset(gen);
            long genDataSize = genLookup.getGenDataSize(gen);
            int genKeyCount = genLookup.getGenKeyCount(gen);

            if (requestedKey >= genKeyCount) {
                clearBlockState();
                return;
            }

            valueMem.extend(genFileOffset + genDataSize);
            Unsafe.getUnsafe().loadFence();
            long genAddr = valueMem.addressOf(genFileOffset);

            this.flatMode = false;
            this.sealedGenKeyCount = genKeyCount;

            int stride = requestedKey / PostingIndexUtils.DENSE_STRIDE;
            int localKey = requestedKey % PostingIndexUtils.DENSE_STRIDE;
            decodeSidecarKey(stride, localKey);
            int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
            int strideOff = Unsafe.getUnsafe().getInt(genAddr + (long) stride * Integer.BYTES);
            long strideAddr = genAddr + siSize + strideOff;
            int ks = PostingIndexUtils.keysInStride(genKeyCount, stride);
            byte mode = Unsafe.getUnsafe().getByte(strideAddr);

            if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                int bitWidth = Unsafe.getUnsafe().getByte(strideAddr + 1) & 0xFF;
                long baseValue = Unsafe.getUnsafe().getLong(strideAddr + PostingIndexUtils.STRIDE_FLAT_BASE_OFFSET);
                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                int startCount = Unsafe.getUnsafe().getInt(prefixAddr + (long) localKey * Integer.BYTES);
                int endCount = Unsafe.getUnsafe().getInt(prefixAddr + (long) (localKey + 1) * Integer.BYTES);
                int count = endCount - startCount;
                this.denseVarKeyStartCount = startCount;

                if (count == 0) {
                    clearBlockState();
                    return;
                }

                int flatHeaderSize = PostingIndexUtils.strideFlatHeaderSize(ks);
                long dataAddr = strideAddr + flatHeaderSize;

                // Binary search to skip values below minValue.
                // Flat values are monotonically increasing (value - baseValue),
                // so we can use O(1) random access via unpackValue.
                int effectiveStart = startCount;
                int effectiveCount = count;
                if (minValue > 0 && bitWidth > 0 && count > 1) {
                    int lo = startCount, hi = endCount - 1;
                    while (lo < hi) {
                        int mid = (lo + hi) >>> 1;
                        long val = BitpackUtils.unpackValue(dataAddr, mid, bitWidth, baseValue);
                        if (val < minValue) {
                            lo = mid + 1;
                        } else {
                            hi = mid;
                        }
                    }
                    effectiveStart = lo;
                    effectiveCount = endCount - effectiveStart;
                }

                // Also trim values above maxValue from the end
                if (maxValue < Long.MAX_VALUE && effectiveCount > 1) {
                    int lo = effectiveStart, hi = effectiveStart + effectiveCount - 1;
                    while (lo < hi) {
                        int mid = (lo + hi + 1) >>> 1;
                        long val = BitpackUtils.unpackValue(dataAddr, mid, bitWidth, baseValue);
                        if (val > maxValue) {
                            hi = mid - 1;
                        } else {
                            lo = mid;
                        }
                    }
                    effectiveCount = lo - effectiveStart + 1;
                }

                if (effectiveCount <= 0) {
                    clearBlockState();
                    return;
                }

                this.flatMode = true;
                this.flatBitWidth = bitWidth;
                this.flatBaseValue = baseValue;
                this.flatDataBase = dataAddr;
                this.encodedBlockCount = 0;
                this.currentBlock = 0;
                this.sidecarStrideKeyStart = effectiveStart - startCount;
                this.sidecarOrdinal = 0;

                int batch = Math.min(effectiveCount, PostingIndexUtils.PACKED_BATCH_SIZE);
                BitpackUtils.unpackValuesFrom(dataAddr, effectiveStart, batch, bitWidth, baseValue, blockBufferAddr);
                this.blockBufferPos = 0;
                this.blockBufferEnd = batch;
                this.flatStartIdx = effectiveStart + batch;
                this.flatRemaining = effectiveCount - batch;
                return;
            }

            // Delta mode
            long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
            this.totalValueCount = Unsafe.getUnsafe().getInt(countsAddr + (long) localKey * Integer.BYTES);
            this.sidecarStrideKeyStart = 0;
            this.sidecarOrdinal = 0;
            if (coverCount > 0) {
                int deltaKeyStartCount = 0;
                for (int k = 0; k < localKey; k++) {
                    deltaKeyStartCount += Unsafe.getUnsafe().getInt(countsAddr + (long) k * Integer.BYTES);
                }
                this.denseVarKeyStartCount = deltaKeyStartCount;
            }
            long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) localKey * Integer.BYTES);
            int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(ks);
            this.encodedAddr = strideAddr + deltaHeaderSize + dataOffset;

            readDeltaBlockMetadata();
        }

        /**
         * Loads a sparse generation using the known position from the inverted index,
         * bypassing binary search entirely.
         */
        private void loadSparseGenDirect(int gen, int idx) {
            this.isCurrentGenDense = false;
            computePerColumnSidecarOffsets(gen);
            long genFileOffset = genLookup.getGenFileOffset(gen);
            long genDataSize = genLookup.getGenDataSize(gen);
            int genKeyCount = genLookup.getGenKeyCount(gen);
            int activeKeyCount = -genKeyCount;

            valueMem.extend(genFileOffset + genDataSize);
            Unsafe.getUnsafe().loadFence();
            long genAddr = valueMem.addressOf(genFileOffset);

            this.flatMode = false;

            long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;

            // Compute sidecar base only when covering columns exist.
            // O(idx) scan is expensive for high-cardinality indexes but only
            // runs when covered values are actually needed.
            if (coverCount > 0) {
                int sidecarBase = 0;
                for (int i = 0; i < idx; i++) {
                    sidecarBase += Unsafe.getUnsafe().getInt(countsBase + (long) i * Integer.BYTES);
                }
                this.sidecarOrdinal = sidecarBase;
            } else {
                this.sidecarOrdinal = 0;
            }

            int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
            long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
            this.totalValueCount = Unsafe.getUnsafe().getInt(countsBase + (long) idx * Integer.BYTES);
            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) idx * Integer.BYTES);
            this.encodedAddr = genAddr + headerSize + dataOffset;

            readDeltaBlockMetadata();
        }

        /**
         * Loads a sparse generation using binary search (Tier 2/3 fallback).
         */
        private void loadSparseGenWithBinarySearch(int gen) {
            this.isCurrentGenDense = false;
            computePerColumnSidecarOffsets(gen);
            long genFileOffset = genLookup.getGenFileOffset(gen);
            long genDataSize = genLookup.getGenDataSize(gen);
            int genKeyCount = genLookup.getGenKeyCount(gen);
            int activeKeyCount = -genKeyCount;

            valueMem.extend(genFileOffset + genDataSize);
            Unsafe.getUnsafe().loadFence();
            long genAddr = valueMem.addressOf(genFileOffset);

            int idx = PostingIndexUtils.binarySearchKeyId(genAddr, activeKeyCount, requestedKey);
            if (idx < 0) {
                clearBlockState();
                totalValueCount = 0;
                this.flatMode = false;
                return;
            }

            this.flatMode = false;

            long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;

            // Compute sidecar base only when covering columns exist.
            // O(idx) scan is expensive for high-cardinality indexes but only
            // runs when covered values are actually needed.
            if (coverCount > 0) {
                int sidecarBase = 0;
                for (int i = 0; i < idx; i++) {
                    sidecarBase += Unsafe.getUnsafe().getInt(countsBase + (long) i * Integer.BYTES);
                }
                this.sidecarOrdinal = sidecarBase;
            } else {
                this.sidecarOrdinal = 0;
            }

            int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
            long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
            this.totalValueCount = Unsafe.getUnsafe().getInt(countsBase + (long) idx * Integer.BYTES);
            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) idx * Integer.BYTES);
            this.encodedAddr = genAddr + headerSize + dataOffset;

            readDeltaBlockMetadata();
        }

        private void readDeltaBlockMetadata() {
            if (totalValueCount == 0) {
                clearBlockState();
                return;
            }

            long pos = encodedAddr;
            int blockCount = Unsafe.getUnsafe().getInt(pos);
            if (blockCount < 0 || blockCount > (totalValueCount + PostingIndexUtils.BLOCK_CAPACITY - 1) / PostingIndexUtils.BLOCK_CAPACITY) {
                throw CairoException.critical(0).put("corrupt posting index: invalid block count [blockCount=")
                        .put(blockCount).put(", totalValues=").put(totalValueCount).put(']');
            }
            pos += 4;

            ensureMetadataCapacity(blockCount);

            for (int b = 0; b < blockCount; b++) {
                Unsafe.getUnsafe().putInt(valueCountsAddr + (long) b * Integer.BYTES, Unsafe.getUnsafe().getByte(pos + b) & 0xFF);
            }
            pos += blockCount;

            for (int b = 0; b < blockCount; b++) {
                Unsafe.getUnsafe().putLong(firstValuesAddr + (long) b * Long.BYTES,
                        Unsafe.getUnsafe().getLong(pos + (long) b * Long.BYTES));
            }
            pos += (long) blockCount * Long.BYTES;

            for (int b = 0; b < blockCount; b++) {
                Unsafe.getUnsafe().putLong(minDeltasAddr + (long) b * Long.BYTES,
                        Unsafe.getUnsafe().getLong(pos + (long) b * Long.BYTES));
            }
            pos += (long) blockCount * Long.BYTES;

            for (int b = 0; b < blockCount; b++) {
                Unsafe.getUnsafe().putInt(bitWidthsAddr + (long) b * Integer.BYTES, Unsafe.getUnsafe().getByte(pos + b) & 0xFF);
            }
            pos += blockCount;

            // pos now points to the start of packed data for block 0
            long packedDataStart = pos;

            // Skip blocks whose values are entirely below minValue.
            // firstValues[] stores the first (lowest) absolute value per block.
            // Since values are monotonically increasing, if firstValues[b+1] <= minValue
            // then all values in block b are < minValue and can be skipped.
            int startBlock = 0;
            if (minValue > 0 && blockCount > 1) {
                // Binary search: find the last block whose firstValue <= minValue.
                // That block (or the one before it) is where we need to start decoding.
                int lo = 0, hi = blockCount - 1;
                while (lo < hi) {
                    int mid = (lo + hi + 1) >>> 1;
                    if (Unsafe.getUnsafe().getLong(firstValuesAddr + (long) mid * Long.BYTES) <= minValue) {
                        lo = mid;
                    } else {
                        hi = mid - 1;
                    }
                }
                // lo is the last block with firstValues[lo] <= minValue.
                // This block might contain values >= minValue, so start here.
                startBlock = lo;
            }

            // Advance packedDataAddr past skipped blocks and account for
            // skipped values in the sidecar offset calculation
            int skippedValueCount = 0;
            for (int b = 0; b < startBlock; b++) {
                int vc = Unsafe.getUnsafe().getInt(valueCountsAddr + (long) b * Integer.BYTES);
                int numDeltas = vc - 1;
                packedDataStart += BitpackUtils.packedDataSize(numDeltas, Unsafe.getUnsafe().getInt(bitWidthsAddr + (long) b * Integer.BYTES));
                skippedValueCount += vc;
            }
            this.sidecarStrideKeyStart += skippedValueCount;

            // Trim trailing blocks that are entirely above maxValue
            int endBlock = blockCount;
            if (maxValue < Long.MAX_VALUE && blockCount > 0) {
                // Find first block whose firstValue > maxValue — all subsequent blocks can be skipped
                for (int b = startBlock; b < blockCount; b++) {
                    if (Unsafe.getUnsafe().getLong(firstValuesAddr + (long) b * Long.BYTES) > maxValue) {
                        endBlock = b;
                        break;
                    }
                }
            }

            this.encodedBlockCount = endBlock;
            this.packedDataAddr = packedDataStart;
            this.currentBlock = startBlock;
            this.blockBufferPos = 0;
            this.blockBufferEnd = 0;
        }
    }
}
