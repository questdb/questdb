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
        private int blockBufferCapacity = PostingIndexUtils.PACKED_BATCH_SIZE;
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
        private long packedDataAddr;
        private boolean flatMode;
        private int flatBitWidth;
        private long flatBaseValue;
        private long flatDataBase;
        private int flatStartIdx;
        private int flatRemaining;
        // Streaming constant-delta mode: compute values on-the-fly as
        // currentValue += step, eliminating buffer writes entirely.
        // Active when all blocks in a full-scan have bitWidth=0.
        private boolean constantDeltaMode;
        private long constantDeltaValue;   // current value (additive tracking)
        private long constantDeltaStep;    // constant delta per value
        private int constantDeltaRemaining; // values left in current block
        private int constantDeltaBlock;    // current block index
        private int constantDeltaBlockCount; // total blocks
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
        private int cachedSidecarIdx; // cached sidecarValueIndex() — valid after hasNext() returns true
        // FSST decompression state for var-sized sidecar blocks
        private long fsstDecompBufAddr;
        private int fsstDecompBufCapacity;
        // Cached FSST symbol table to avoid deserializing per row
        private FSST.SymbolTable fsstCachedTable;
        private long fsstCachedBlockBase = -1;
        private int fsstCachedIncludeIdx = -1;

        @Override
        public byte getCoveredByte(int includeIdx) {
            if (!isCurrentGenDense) {
                return getRawSidecarByte(includeIdx);
            }
            int idx = cachedSidecarIdx;
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
            int idx = cachedSidecarIdx;
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
            int idx = cachedSidecarIdx;
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
            int idx = cachedSidecarIdx;
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
            int idx = cachedSidecarIdx;
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
            int idx = cachedSidecarIdx;
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
            int ordinal = isCurrentGenDense
                    ? denseVarKeyStartCount + sidecarOrdinal - 1
                    : sidecarOrdinal - 1;
            long blockBase = isCurrentGenDense
                    ? findDenseVarBlockBase(includeIdx)
                    : currentGenSidecarOffsets[includeIdx];
            if (blockBase < 0) return null;
            MemoryMR mem = sidecarMems[includeIdx];
            int rawCount = Unsafe.getUnsafe().getInt(mem.addressOf(blockBase));
            boolean fsst = (rawCount & FSST.FSST_BLOCK_FLAG) != 0;
            int count = rawCount & ~FSST.FSST_BLOCK_FLAG;
            if (ordinal >= count) return null;

            if (fsst) {
                return decompressFsstUtf8(mem, blockBase, count, ordinal, includeIdx, view);
            }

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
            int rawCount = Unsafe.getUnsafe().getInt(mem.addressOf(blockBase));
            boolean fsst = (rawCount & FSST.FSST_BLOCK_FLAG) != 0;
            int count = rawCount & ~FSST.FSST_BLOCK_FLAG;
            if (ordinal >= count) return null;

            if (fsst) {
                return decompressFsstStr(mem, blockBase, count, ordinal, includeIdx, view);
            }

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
         * Resolves the FSST symbol table for the given block, caching it to avoid
         * re-deserializing on every row access within the same block.
         */
        private FSST.SymbolTable resolveFsstTable(MemoryMR mem, long blockBase, int includeIdx) {
            if (fsstCachedBlockBase == blockBase && fsstCachedIncludeIdx == includeIdx && fsstCachedTable != null) {
                return fsstCachedTable;
            }
            long pos = blockBase + 4;
            int tableLen = Unsafe.getUnsafe().getShort(mem.addressOf(pos)) & 0xFFFF;
            long tableAddr = mem.addressOf(pos + 2);
            fsstCachedTable = FSST.deserialize(tableAddr);
            fsstCachedBlockBase = blockBase;
            fsstCachedIncludeIdx = includeIdx;
            return fsstCachedTable;
        }

        /**
         * Reads the compressed byte range for ordinal from an FSST block and
         * decompresses it into the reusable fsstDecompBuf.
         *
         * @return decompressed byte count, or -1 if NULL
         */
        private int decompressFsstValue(MemoryMR mem, long blockBase, int count, int ordinal, int includeIdx) {
            FSST.SymbolTable table = resolveFsstTable(mem, blockBase, includeIdx);
            long pos = blockBase + 4;
            int tableLen = Unsafe.getUnsafe().getShort(mem.addressOf(pos)) & 0xFFFF;

            long offsetsAddr = mem.addressOf(pos + 2 + tableLen);
            int lo = Unsafe.getUnsafe().getInt(offsetsAddr + (long) ordinal * Integer.BYTES);
            int hi = Unsafe.getUnsafe().getInt(offsetsAddr + (long) (ordinal + 1) * Integer.BYTES);
            if (lo == hi) return -1; // NULL

            long dataBase = pos + 2 + tableLen + (long) (count + 1) * Integer.BYTES;
            long compAddr = mem.addressOf(dataBase + lo);
            int compLen = hi - lo;

            int needed = compLen * 8;
            if (fsstDecompBufAddr == 0 || fsstDecompBufCapacity < needed) {
                if (fsstDecompBufAddr != 0) {
                    Unsafe.free(fsstDecompBufAddr, fsstDecompBufCapacity, MemoryTag.NATIVE_INDEX_READER);
                }
                fsstDecompBufCapacity = needed;
                fsstDecompBufAddr = Unsafe.malloc(needed, MemoryTag.NATIVE_INDEX_READER);
            }
            return FSST.decompressBytes(table, compAddr, compLen, fsstDecompBufAddr);
        }

        /**
         * Decompress a single FSST-encoded VARCHAR value from an FSST block.
         * Block format: [count|flag:4B][tableLen:2B][FSST table][offsets:(count+1)×4B][compressed data]
         */
        private Utf8Sequence decompressFsstUtf8(MemoryMR mem, long blockBase, int count, int ordinal, int includeIdx, DirectUtf8String view) {
            int decompLen = decompressFsstValue(mem, blockBase, count, ordinal, includeIdx);
            if (decompLen < 0) return null;
            return view.of(fsstDecompBufAddr, fsstDecompBufAddr + decompLen);
        }

        /**
         * Decompress a single FSST-encoded STRING value from an FSST block.
         */
        private CharSequence decompressFsstStr(MemoryMR mem, long blockBase, int count, int ordinal, int includeIdx, DirectString view) {
            int decompLen = decompressFsstValue(mem, blockBase, count, ordinal, includeIdx);
            if (decompLen < 0) return null;
            int len = Unsafe.getUnsafe().getInt(fsstDecompBufAddr);
            if (len < 0) return null;
            return view.of(fsstDecompBufAddr + Integer.BYTES, len);
        }

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

        private int sidecarValueIdx() {
            return sidecarStrideKeyStart + sidecarOrdinal - 1;
        }

        private int sidecarValueIndex() {
            return cachedSidecarIdx;
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
                if (mem == null) {
                    continue;
                }
                // Ensure the sidecar mapping covers the full file
                mem.growToFileSize();
                if (mem.size() == 0) {
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
                long keyOffsetsEnd = strideDataStart + (long) ks * Integer.BYTES;
                if (keyOffsetsEnd > mem.size()) {
                    continue;
                }
                long keyOffsetsAddr = mem.addressOf(strideDataStart);
                int keyBlockOff = Unsafe.getUnsafe().getInt(keyOffsetsAddr + (long) localKey * Integer.BYTES);
                long keyBlockStart = keyOffsetsEnd + keyBlockOff;
                if (keyBlockStart + 4 > mem.size()) {
                    continue;
                }
                long keyBlockAddr = mem.addressOf(keyBlockStart);
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
                    case ColumnType.IPv4:
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
                    case ColumnType.CHAR:
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
            // Fast path: streaming constant-delta (full scan, all bitWidth=0).
            // Tiny method body for JIT inlining — no min/max checks needed
            // because streaming mode is only active when minValue=0 and
            // maxValue=Long.MAX_VALUE (set by decodeAllBlocksBulk).
            if (constantDeltaRemaining > 0) {
                next = constantDeltaValue;
                constantDeltaValue += constantDeltaStep;
                constantDeltaRemaining--;
                sidecarOrdinal++;
                cachedSidecarIdx = sidecarValueIdx();
                return true;
            }
            return hasNextComplex();
        }

        /**
         * Slow path for hasNext(): handles block transitions, buffer reads,
         * flat mode, and generation advancement. Called ~1.6% of the time
         * for constant-delta data (only at block boundaries).
         */
        private boolean hasNextComplex() {
            while (true) {
                // Streaming mode: advance to next block
                if (constantDeltaMode) {
                    if (constantDeltaBlock < constantDeltaBlockCount) {
                        advanceConstantDeltaBlock();
                        if (constantDeltaRemaining > 0) {
                            next = constantDeltaValue;
                            constantDeltaValue += constantDeltaStep;
                            constantDeltaRemaining--;
                            sidecarOrdinal++;
                            cachedSidecarIdx = sidecarValueIdx();
                            return true;
                        }
                        continue;
                    }
                    constantDeltaMode = false;
                }

                // Serve from block buffer
                while (blockBufferPos < blockBufferEnd) {
                    long value = Unsafe.getUnsafe().getLong(blockBufferAddr + (long) blockBufferPos * Long.BYTES);
                    blockBufferPos++;

                    if (value > maxValue) {
                        return false;
                    }
                    if (value >= minValue) {
                        this.next = value;
                        if (coverCount > 0) {
                            sidecarOrdinal++;
                            cachedSidecarIdx = sidecarValueIdx();
                        }
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

                // New gen may have set up streaming mode — serve first value
                if (constantDeltaRemaining > 0) {
                    next = constantDeltaValue;
                    constantDeltaValue += constantDeltaStep;
                    constantDeltaRemaining--;
                    sidecarOrdinal++;
                    cachedSidecarIdx = sidecarValueIdx();
                    return true;
                }
            }
        }

        @Override
        public long next() {
            return next - minValue;
        }

        void of(int key, long minValue, long maxValue) {
            // Re-allocate fixed buffers if freed by close()
            if (blockBufferAddr == 0) {
                blockBufferCapacity = PostingIndexUtils.PACKED_BATCH_SIZE;
                blockBufferAddr = Unsafe.malloc((long) blockBufferCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
            if (blockDeltasAddr == 0) {
                blockDeltasAddr = Unsafe.malloc((long) PostingIndexUtils.BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
            if (keyCount == 0 || key < 0 || key >= keyCount || genCount == 0) {
                totalValueCount = 0;
                currentGen = genCount;
                encodedBlockCount = 0;
                currentBlock = 0;
                blockBufferPos = 0;
                blockBufferEnd = 0;
                constantDeltaMode = false;
                constantDeltaRemaining = 0;
                flatMode = false;
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
            this.constantDeltaMode = false;
            this.constantDeltaRemaining = 0;
            this.flatMode = false;
            this.sidecarOrdinal = 0;
            this.sidecarStrideKeyStart = 0;
            this.decodedStride = -1;
            advanceToNextRelevantGen();
        }

        @Override
        public int getCoveredColumnCount() {
            return coverCount;
        }

        @Override
        public int getCoveredColumnType(int includeIdx) {
            return sidecarColumnTypes != null && includeIdx < sidecarColumnTypes.length
                    ? sidecarColumnTypes[includeIdx] : -1;
        }

        @Override
        public int getCoveredValueCount() {
            if (sidecarMems == null || coverCount == 0) {
                return -1;
            }
            // Sum counts across all gens for this key.
            // Dense gens: read count from sidecar key block header.
            // Sparse gens: read count from raw sidecar block header.
            int total = 0;
            for (int g = 0; g < genCount; g++) {
                int genKeyCount = genLookup.getGenKeyCount(g);
                if (genKeyCount >= 0) {
                    // Dense gen — read count from sidecar key block
                    total += getDenseGenCount(g, genKeyCount);
                } else {
                    // Sparse gen — read count from raw sidecar block
                    total += getSparseGenCount(g);
                }
            }
            return total;
        }

        @Override
        public int decodeCoveredColumnsToAddr(long[] outputAddrs) {
            if (sidecarMems == null || coverCount == 0) {
                return -1;
            }
            ensureDecodeWorkspaceCapacity(65536); // pre-allocate workspace

            int totalWritten = 0;
            for (int g = 0; g < genCount; g++) {
                int genKeyCount = genLookup.getGenKeyCount(g);
                if (genKeyCount >= 0) {
                    totalWritten += decodeDenseGenToAddr(g, genKeyCount, outputAddrs, totalWritten);
                } else {
                    totalWritten += decodeSparseGenToAddr(g, outputAddrs, totalWritten);
                }
            }
            return totalWritten;
        }

        private int getDenseGenCount(int gen, int genKeyCount) {
            // Find the first non-var-size sidecar column to read count from
            int memIdx = -1;
            for (int c = 0; c < coverCount; c++) {
                if (sidecarMems[c] != null && sidecarMems[c].size() > 0
                        && !ColumnType.isVarSize(sidecarColumnTypes[c])) {
                    memIdx = c;
                    break;
                }
            }
            if (memIdx < 0) return 0;

            int stride = requestedKey / PostingIndexUtils.DENSE_STRIDE;
            int localKey = requestedKey % PostingIndexUtils.DENSE_STRIDE;
            int sc = PostingIndexUtils.strideCount(genKeyCount);
            if (stride >= sc) return 0;
            int ks = PostingIndexUtils.keysInStride(genKeyCount, stride);
            if (localKey >= ks) return 0;

            MemoryMR mem = sidecarMems[memIdx];
            int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
            long strideIdxOffset = (long) stride * Integer.BYTES;
            if (strideIdxOffset + Integer.BYTES > mem.size()) return 0;
            int strideOff = mem.getInt(strideIdxOffset);
            long strideDataStart = siSize + strideOff;
            if (strideDataStart >= mem.size()) return 0;
            long keyOffsetsAddr = mem.addressOf(strideDataStart);
            int keyBlockOff = Unsafe.getUnsafe().getInt(keyOffsetsAddr + (long) localKey * Integer.BYTES);
            long keyBlockAddr = mem.addressOf(strideDataStart + (long) ks * Integer.BYTES + keyBlockOff);
            return Unsafe.getUnsafe().getInt(keyBlockAddr);
        }

        private int getSparseGenCount(int gen) {
            // Sparse gen raw sidecar format: [count:4B][values...]
            // Use column 0's offset to find the count.
            if (sidecarMems[0] == null) {
                return 0;
            }
            // Compute offset for this gen in column 0's sidecar
            int[] offsets = new int[coverCount];
            computeSparseOffsets(gen, offsets);
            long addr = sidecarMems[0].addressOf(offsets[0]);
            return Unsafe.getUnsafe().getInt(addr);
        }

        private void computeSparseOffsets(int gen, int[] offsets) {
            // Same logic as computePerColumnSidecarOffsets but for a specific gen
            int firstSparseGen = 0;
            int denseKeyCount = 0;
            while (firstSparseGen < gen && genLookup.getGenKeyCount(firstSparseGen) >= 0) {
                denseKeyCount = genLookup.getGenKeyCount(firstSparseGen);
                firstSparseGen++;
            }
            for (int c = 0; c < coverCount; c++) {
                if (sidecarMems[c] == null) {
                    offsets[c] = 0;
                    continue;
                }
                long offset;
                if (firstSparseGen == 0) {
                    offset = 0;
                } else if (c == 0) {
                    offset = genLookup.getGenSidecarOffset(firstSparseGen);
                } else {
                    offset = computeSealedSidecarSize(sidecarMems[c], denseKeyCount);
                }
                int colType = sidecarColumnTypes[c];
                boolean isVar = ColumnType.isVarSize(colType);
                int elemSize = isVar ? 0 : ColumnType.sizeOf(colType);
                for (int g = firstSparseGen; g < gen; g++) {
                    if (genLookup.getGenKeyCount(g) >= 0) continue;
                    if (offset + 4 > sidecarMems[c].size()) break;
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
                offsets[c] = (int) offset;
            }
        }

        private int decodeDenseGenToAddr(int gen, int genKeyCount, long[] outputAddrs, int writeOffset) {
            int stride = requestedKey / PostingIndexUtils.DENSE_STRIDE;
            int localKey = requestedKey % PostingIndexUtils.DENSE_STRIDE;
            int sc = PostingIndexUtils.strideCount(genKeyCount);
            if (stride >= sc) return 0;
            int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
            int ks = PostingIndexUtils.keysInStride(genKeyCount, stride);
            if (localKey >= ks) return 0;

            int count = 0;
            for (int c = 0; c < coverCount; c++) {
                if (outputAddrs[c] == 0) continue; // column not requested
                MemoryMR mem = sidecarMems[c];
                if (mem == null || mem.size() == 0) continue;
                int colType = sidecarColumnTypes[c];
                if (ColumnType.isVarSize(colType)) continue;

                long strideIdxOffset = (long) stride * Integer.BYTES;
                if (strideIdxOffset + Integer.BYTES > mem.size()) continue;
                int strideOff = mem.getInt(strideIdxOffset);
                long strideDataStart = siSize + strideOff;
                if (strideDataStart >= mem.size()) continue;

                long keyOffsetsAddr = mem.addressOf(strideDataStart);
                int keyBlockOff = Unsafe.getUnsafe().getInt(keyOffsetsAddr + (long) localKey * Integer.BYTES);
                long keyBlockAddr = mem.addressOf(strideDataStart + (long) ks * Integer.BYTES + keyBlockOff);
                int keyCount = Unsafe.getUnsafe().getInt(keyBlockAddr);
                if (keyCount == 0) continue;
                count = keyCount;

                int elemSize = ColumnType.sizeOf(colType);
                long outAddr = outputAddrs[c] + (long) writeOffset * elemSize;
                ensureDecodeWorkspaceCapacity(keyCount);

                switch (ColumnType.tagOf(colType)) {
                    case ColumnType.DOUBLE ->
                            AlpCompression.decompressDoublesToAddr(keyBlockAddr, outAddr, decodeWorkspaceAddr);
                    case ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE, ColumnType.GEOLONG ->
                            AlpCompression.decompressLongsToAddr(keyBlockAddr, outAddr, decodeWorkspaceAddr);
                    case ColumnType.INT, ColumnType.IPv4, ColumnType.FLOAT, ColumnType.GEOINT, ColumnType.SYMBOL ->
                            AlpCompression.decompressIntsToAddr(keyBlockAddr, outAddr, decodeWorkspaceAddr);
                    case ColumnType.CHAR, ColumnType.SHORT, ColumnType.GEOSHORT -> {
                        long rawAddr = keyBlockAddr + 4;
                        for (int i = 0; i < keyCount; i++) {
                            Unsafe.getUnsafe().putShort(outAddr + (long) i * Short.BYTES,
                                    Unsafe.getUnsafe().getShort(rawAddr + (long) i * Short.BYTES));
                        }
                    }
                    case ColumnType.BYTE, ColumnType.BOOLEAN, ColumnType.GEOBYTE -> {
                        long rawAddr = keyBlockAddr + 4;
                        Unsafe.getUnsafe().copyMemory(rawAddr, outAddr, keyCount);
                    }
                }
            }
            return count;
        }

        private int decodeSparseGenToAddr(int gen, long[] outputAddrs, int writeOffset) {
            int[] offsets = new int[coverCount];
            computeSparseOffsets(gen, offsets);

            int count = 0;
            for (int c = 0; c < coverCount; c++) {
                if (outputAddrs[c] == 0) continue; // column not requested
                MemoryMR mem = sidecarMems[c];
                if (mem == null) continue;
                int colType = sidecarColumnTypes[c];
                if (ColumnType.isVarSize(colType)) continue;

                int elemSize = ColumnType.sizeOf(colType);
                long blockAddr = mem.addressOf(offsets[c]);
                int genCount = Unsafe.getUnsafe().getInt(blockAddr);
                if (genCount == 0) continue;
                count = genCount;

                // Raw sidecar: [count:4B][value0][value1]... — memcpy directly
                long srcAddr = blockAddr + 4;
                long dstAddr = outputAddrs[c] + (long) writeOffset * elemSize;
                Unsafe.getUnsafe().copyMemory(srcAddr, dstAddr, (long) genCount * elemSize);
            }
            return count;
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

        private void advanceConstantDeltaBlock() {
            int b = constantDeltaBlock;
            int count = Unsafe.getUnsafe().getByte(srcValueCountsAddr + b) & 0xFF;
            long firstValue = Unsafe.getUnsafe().getLong(srcFirstValuesAddr + (long) b * Long.BYTES);
            long minD = Unsafe.getUnsafe().getLong(srcMinDeltasAddr + (long) b * Long.BYTES);
            constantDeltaValue = firstValue;
            constantDeltaStep = minD;
            constantDeltaRemaining = count;
            constantDeltaBlock++;
        }

        private void clearBlockState() {
            this.encodedBlockCount = 0;
            this.currentBlock = 0;
            this.blockBufferPos = 0;
            this.blockBufferEnd = 0;
            this.constantDeltaMode = false;
            this.constantDeltaRemaining = 0;
        }

        private void decodeNextBlock() {
            int b = currentBlock;
            // Read metadata directly from mmap — no intermediate copy.
            int count = Unsafe.getUnsafe().getByte(srcValueCountsAddr + b) & 0xFF;
            int bitWidth = Unsafe.getUnsafe().getByte(srcBitWidthsAddr + b) & 0xFF;
            int numDeltas = count - 1;

            long cumulative = Unsafe.getUnsafe().getLong(srcFirstValuesAddr + (long) b * Long.BYTES);
            Unsafe.getUnsafe().putLong(blockBufferAddr, cumulative);

            if (numDeltas > 0) {
                long minD = Unsafe.getUnsafe().getLong(srcMinDeltasAddr + (long) b * Long.BYTES);
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

        private void ensureDecodeWorkspaceCapacity(int count) {
            if (count > decodeWorkspaceCapacity) {
                if (decodeWorkspaceAddr != 0) {
                    Unsafe.free(decodeWorkspaceAddr, (long) decodeWorkspaceCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                    decodeWorkspaceAddr = 0;
                }
                decodeWorkspaceCapacity = count;
                decodeWorkspaceAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
        }

        void close() {
            if (blockBufferAddr != 0) {
                Unsafe.free(blockBufferAddr, (long) blockBufferCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                blockBufferAddr = 0;
                blockBufferCapacity = 0;
            }
            if (blockDeltasAddr != 0) {
                Unsafe.free(blockDeltasAddr, (long) PostingIndexUtils.BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                blockDeltasAddr = 0;
            }
            if (decodeWorkspaceAddr != 0) {
                Unsafe.free(decodeWorkspaceAddr, (long) decodeWorkspaceCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                decodeWorkspaceAddr = 0;
                decodeWorkspaceCapacity = 0;
            }
            if (fsstDecompBufAddr != 0) {
                Unsafe.free(fsstDecompBufAddr, fsstDecompBufCapacity, MemoryTag.NATIVE_INDEX_READER);
                fsstDecompBufAddr = 0;
                fsstDecompBufCapacity = 0;
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

        // Source addresses for block metadata — read directly from mmap,
        // no copy into cursor-local arrays. Set by readDeltaBlockMetadata().
        private long srcValueCountsAddr; // N × 1B (byte → int widened on read)
        private long srcFirstValuesAddr; // N × 8B
        private long srcMinDeltasAddr;   // N × 8B
        private long srcBitWidthsAddr;   // N × 1B (byte → int widened on read)

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

            // Store pointers directly into the mmap'd file — no copy needed.
            // decodeNextBlock reads from these addresses one block at a time.
            srcValueCountsAddr = pos;
            pos += blockCount;

            srcFirstValuesAddr = pos;
            pos += (long) blockCount * Long.BYTES;

            srcMinDeltasAddr = pos;
            pos += (long) blockCount * Long.BYTES;

            srcBitWidthsAddr = pos;
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
                    if (Unsafe.getUnsafe().getLong(srcFirstValuesAddr + (long) mid * Long.BYTES) <= minValue) {
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
                int vc = Unsafe.getUnsafe().getByte(srcValueCountsAddr + b) & 0xFF;
                int numDeltas = vc - 1;
                packedDataStart += BitpackUtils.packedDataSize(numDeltas, Unsafe.getUnsafe().getByte(srcBitWidthsAddr + b) & 0xFF);
                skippedValueCount += vc;
            }
            this.sidecarStrideKeyStart += skippedValueCount;
            // For sparse gens, the raw sidecar readers use sidecarOrdinal directly
            // (not sidecarStrideKeyStart). Advance it past skipped blocks' values.
            if (!isCurrentGenDense && coverCount > 0) {
                this.sidecarOrdinal += skippedValueCount;
            }

            // Trim trailing blocks that are entirely above maxValue
            int endBlock = blockCount;
            if (maxValue < Long.MAX_VALUE && blockCount > 0) {
                // Find first block whose firstValue > maxValue — all subsequent blocks can be skipped
                for (int b = startBlock; b < blockCount; b++) {
                    if (Unsafe.getUnsafe().getLong(srcFirstValuesAddr + (long) b * Long.BYTES) > maxValue) {
                        endBlock = b;
                        break;
                    }
                }
            }

            // Full-scan fast path: decode all blocks into one large buffer.
            // Eliminates per-block decodeNextBlock() overhead (157 calls for S4).
            if (startBlock == 0 && endBlock == blockCount && minValue == 0 && maxValue == Long.MAX_VALUE) {
                decodeAllBlocksBulk(blockCount, packedDataStart);
                return;
            }

            this.encodedBlockCount = endBlock;
            this.packedDataAddr = packedDataStart;
            this.currentBlock = startBlock;
            this.blockBufferPos = 0;
            this.blockBufferEnd = 0;
        }

        /**
         * Decode all blocks for a full-scan query. When all blocks have constant
         * delta (bitWidth=0), uses streaming mode: values are computed on-the-fly
         * in hasNext() as firstValue + i * delta, eliminating buffer writes.
         * Falls back to bulk buffer decode for variable-delta blocks.
         */
        private void decodeAllBlocksBulk(int blockCount, long packedDataStart) {
            // Check if all blocks have constant delta (bitWidth=0).
            // This is the common case for sequential inserts (S4 scenario).
            boolean allConstantDelta = true;
            for (int b = 0; b < blockCount; b++) {
                if ((Unsafe.getUnsafe().getByte(srcBitWidthsAddr + b) & 0xFF) != 0) {
                    allConstantDelta = false;
                    break;
                }
            }

            if (allConstantDelta) {
                // Streaming mode: no buffer writes, compute values in hasNext().
                this.constantDeltaMode = true;
                this.constantDeltaBlockCount = blockCount;
                this.constantDeltaBlock = 0;
                this.encodedBlockCount = 0;
                this.currentBlock = blockCount;
                this.blockBufferPos = 0;
                this.blockBufferEnd = 0;
                // Load first block
                advanceConstantDeltaBlock();
                return;
            }

            // Variable-delta fallback: decode into buffer
            if (totalValueCount > blockBufferCapacity) {
                Unsafe.free(blockBufferAddr, (long) blockBufferCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                blockBufferCapacity = Math.max(totalValueCount, blockBufferCapacity * 2);
                blockBufferAddr = Unsafe.malloc((long) blockBufferCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }

            long packedPos = packedDataStart;
            int destIdx = 0;

            for (int b = 0; b < blockCount; b++) {
                int count = Unsafe.getUnsafe().getByte(srcValueCountsAddr + b) & 0xFF;
                int bitWidth = Unsafe.getUnsafe().getByte(srcBitWidthsAddr + b) & 0xFF;
                long firstValue = Unsafe.getUnsafe().getLong(srcFirstValuesAddr + (long) b * Long.BYTES);
                long minD = Unsafe.getUnsafe().getLong(srcMinDeltasAddr + (long) b * Long.BYTES);
                int numDeltas = count - 1;

                Unsafe.getUnsafe().putLong(blockBufferAddr + (long) destIdx * Long.BYTES, firstValue);
                long cumulative = firstValue;

                if (numDeltas > 0) {
                    if (bitWidth == 0) {
                        for (int i = 0; i < numDeltas; i++) {
                            cumulative += minD;
                            Unsafe.getUnsafe().putLong(blockBufferAddr + (long) (destIdx + 1 + i) * Long.BYTES, cumulative);
                        }
                    } else {
                        BitpackUtils.unpackAllValues(packedPos, numDeltas, bitWidth, minD, blockDeltasAddr);
                        for (int i = 0; i < numDeltas; i++) {
                            cumulative += Unsafe.getUnsafe().getLong(blockDeltasAddr + (long) i * Long.BYTES);
                            Unsafe.getUnsafe().putLong(blockBufferAddr + (long) (destIdx + 1 + i) * Long.BYTES, cumulative);
                        }
                        packedPos += BitpackUtils.packedDataSize(numDeltas, bitWidth);
                    }
                }
                destIdx += count;
            }

            this.blockBufferPos = 0;
            this.blockBufferEnd = totalValueCount;
            this.encodedBlockCount = 0;
            this.currentBlock = blockCount;
        }
    }
}
