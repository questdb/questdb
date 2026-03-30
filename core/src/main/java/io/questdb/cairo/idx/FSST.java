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

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

/**
 * Fast Static Symbol Table (FSST) codec for 8-byte integer sequences.
 * <p>
 * Maps frequently occurring 1–8 byte patterns to single-byte codes (0–254).
 * Code 255 is the escape byte, followed by a literal byte.
 * <p>
 * Key properties:
 * <ul>
 *   <li>Symbol table fits in ~2.3 KB (L1 cache)</li>
 *   <li>Stateless per-value decode: no inter-value dependencies</li>
 *   <li>O(1) random access to any encoded value via offset table</li>
 *   <li>Greedy longest-match encoding, table-lookup decoding</li>
 * </ul>
 * <p>
 * Training uses a 5-iteration counter-based algorithm:
 * compress sample, count code frequencies, generate candidates by
 * concatenating frequent pairs, select top 255 by compression gain.
 */
public final class FSST {

    public static final int ESCAPE = 255;
    public static final int FSST_BLOCK_FLAG = 0x80000000;
    public static final int MAX_SYMBOLS = 255;
    // Serialized size: 1 byte (count) + count * (1 byte len + 8 bytes symbol) = 1 + 255*9 = 2296
    public static final int SERIALIZED_MAX_SIZE = 1 + MAX_SYMBOLS * 9;
    private static final int HASH_SIZE = 1 << 14; // 16384 entries
    private static final int TRAIN_ITERATIONS = 5;
    private static final int TRAIN_SAMPLE_LIMIT = 16384; // max sample values for training

    private FSST() {
    }

    /**
     * Compress a sequence of 8-byte longs using a pre-trained symbol table.
     *
     * @param table   symbol table
     * @param srcAddr source address (array of longs in native memory)
     * @param count   number of longs
     * @param dstAddr destination address (must have capacity for worst case: count * 16)
     * @return number of bytes written to dst
     */
    public static int compress(SymbolTable table, long srcAddr, int count, long dstAddr) {
        int dstOff = 0;
        for (int i = 0; i < count; i++) {
            long value = Unsafe.getUnsafe().getLong(srcAddr + (long) i * Long.BYTES);
            dstOff = encodeValue(table, value, dstAddr, dstOff);
        }
        return dstOff;
    }

    /**
     * Compress an arbitrary-length byte sequence using a pre-trained symbol table.
     *
     * @param table   symbol table
     * @param srcAddr source byte address
     * @param srcLen  source byte length
     * @param dstAddr destination address (worst case: srcLen * 2)
     * @return number of bytes written to dst
     */
    public static int compressBytes(SymbolTable table, long srcAddr, int srcLen, long dstAddr) {
        int srcOff = 0;
        int dstOff = 0;
        while (srcOff < srcLen) {
            int remaining = srcLen - srcOff;
            int bestLen = 0;
            int bestCode = -1;

            if (remaining >= 2) {
                long window = readWindow(srcAddr + srcOff, Math.min(remaining, 8));
                // Probe the hash table for each candidate symbol length from longest
                // to shortest. Symbols are inserted at hash(symbol, symbolLen), so we
                // must hash the corresponding prefix of the window for each length.
                int maxCandLen = Math.min(remaining, 8);
                for (int candLen = maxCandLen; candLen >= 2 && candLen > bestLen; candLen--) {
                    long prefix = candLen == 8 ? window : (window & ((1L << (candLen * 8)) - 1));
                    int h = hash(prefix, candLen);
                    for (int probe = 0; probe < 4; probe++) {
                        int idx = (h + probe) & (HASH_SIZE - 1);
                        int code = table.getHashCode(idx);
                        if (code < 0) break;
                        if (table.getLen(code) != candLen) continue;
                        if (table.getSymbol(code) == prefix) {
                            bestLen = candLen;
                            bestCode = code;
                            break;
                        }
                    }
                }
            }

            if (bestLen <= 1) {
                int b = Unsafe.getUnsafe().getByte(srcAddr + srcOff) & 0xFF;
                int singleCode = table.getByteMap(b);
                if (singleCode >= 0) {
                    Unsafe.getUnsafe().putByte(dstAddr + dstOff, (byte) singleCode);
                    dstOff++;
                    srcOff++;
                    continue;
                }
            }

            if (bestLen >= 2) {
                Unsafe.getUnsafe().putByte(dstAddr + dstOff, (byte) bestCode);
                dstOff++;
                srcOff += bestLen;
            } else {
                int b = Unsafe.getUnsafe().getByte(srcAddr + srcOff) & 0xFF;
                Unsafe.getUnsafe().putByte(dstAddr + dstOff, (byte) ESCAPE);
                Unsafe.getUnsafe().putByte(dstAddr + dstOff + 1, (byte) b);
                dstOff += 2;
                srcOff++;
            }
        }
        return dstOff;
    }

    /**
     * Decompress an FSST-encoded byte sequence back to original bytes.
     *
     * @param table   symbol table
     * @param srcAddr compressed byte address
     * @param srcLen  compressed byte length
     * @param dstAddr destination address
     * @return number of decompressed bytes written
     */
    public static int decompressBytes(SymbolTable table, long srcAddr, int srcLen, long dstAddr) {
        int srcOff = 0;
        int dstOff = 0;
        while (srcOff < srcLen) {
            int code = Unsafe.getUnsafe().getByte(srcAddr + srcOff) & 0xFF;
            srcOff++;
            if (code == ESCAPE) {
                if (srcOff < srcLen) {
                    Unsafe.getUnsafe().putByte(dstAddr + dstOff, Unsafe.getUnsafe().getByte(srcAddr + srcOff));
                    srcOff++;
                    dstOff++;
                }
            } else {
                int len = table.getLen(code);
                long sym = table.getSymbol(code);
                for (int b = 0; b < len; b++) {
                    Unsafe.getUnsafe().putByte(dstAddr + dstOff + b, (byte) (sym >>> (b * 8)));
                }
                dstOff += len;
            }
        }
        return dstOff;
    }

    /**
     * Train a symbol table from raw byte data in native memory.
     * Internally chunks the data into 8-byte segments for the standard training algorithm.
     *
     * @param srcAddr byte data address
     * @param srcLen  byte data length
     * @return trained SymbolTable, or null if data is too small to benefit from compression
     */
    public static SymbolTable trainBytes(long srcAddr, int srcLen) {
        if (srcLen < 16) {
            return null;
        }
        // Chunk into 8-byte longs for the existing training algorithm
        int longCount = (srcLen + 7) / 8;
        long bufAddr = Unsafe.malloc(longCount * 8L, MemoryTag.NATIVE_DEFAULT);
        try {
            // Copy, padding last chunk with zeros
            Unsafe.getUnsafe().copyMemory(srcAddr, bufAddr, srcLen);
            if (srcLen < longCount * 8L) {
                Unsafe.getUnsafe().setMemory(bufAddr + srcLen, longCount * 8L - srcLen, (byte) 0);
            }
            return train(bufAddr, longCount);
        } finally {
            Unsafe.free(bufAddr, longCount * 8L, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Read up to 8 bytes from srcAddr into a long (little-endian), without reading past the boundary.
     */
    private static long readWindow(long addr, int available) {
        long result = 0;
        for (int i = 0; i < available && i < 8; i++) {
            result |= ((long) (Unsafe.getUnsafe().getByte(addr + i) & 0xFF)) << (i * 8);
        }
        return result;
    }

    /**
     * Decode multiple 8-byte longs from a contiguous FSST-encoded byte stream.
     *
     * @param table   symbol table
     * @param srcAddr start of encoded bytes
     * @param srcLen  total encoded byte length
     * @param count   number of longs to decode
     * @param dstAddr destination address (array of count longs)
     */
    public static void decodeBulk(SymbolTable table, long srcAddr, int srcLen, int count, long dstAddr) {
        int off = 0;
        for (int v = 0; v < count; v++) {
            long result = 0;
            int bytePos = 0;
            while (bytePos < 8 && off < srcLen) {
                int code = Unsafe.getUnsafe().getByte(srcAddr + off) & 0xFF;
                off++;
                if (code == ESCAPE) {
                    if (off < srcLen) {
                        int literal = Unsafe.getUnsafe().getByte(srcAddr + off) & 0xFF;
                        off++;
                        result |= ((long) literal) << (bytePos * 8);
                        bytePos++;
                    }
                } else {
                    int len = table.getLen(code);
                    long sym = table.getSymbol(code);
                    for (int b = 0; b < len && bytePos < 8; b++) {
                        result |= ((sym >>> (b * 8)) & 0xFFL) << (bytePos * 8);
                        bytePos++;
                    }
                }
            }
            Unsafe.getUnsafe().putLong(dstAddr + (long) v * Long.BYTES, result);
        }
    }

    /**
     * Decode a single 8-byte long from FSST-encoded bytes.
     *
     * @param table   symbol table
     * @param srcAddr start of encoded bytes for this value
     * @param srcLen  number of encoded bytes for this value
     * @return decoded long value
     */
    public static long decodeSingle(SymbolTable table, long srcAddr, int srcLen) {
        long result = 0;
        int bytePos = 0;
        int off = 0;
        while (off < srcLen && bytePos < 8) {
            int code = Unsafe.getUnsafe().getByte(srcAddr + off) & 0xFF;
            off++;
            if (code == ESCAPE) {
                if (off < srcLen) {
                    int literal = Unsafe.getUnsafe().getByte(srcAddr + off) & 0xFF;
                    off++;
                    result |= ((long) literal) << (bytePos * 8);
                    bytePos++;
                }
            } else {
                int len = table.getLen(code);
                long sym = table.getSymbol(code);
                for (int b = 0; b < len && bytePos < 8; b++) {
                    result |= ((sym >>> (b * 8)) & 0xFFL) << (bytePos * 8);
                    bytePos++;
                }
            }
        }
        return result;
    }

    /**
     * Returns the encoded size of a single long value (without writing anything).
     */
    public static int encodedSize(SymbolTable table, long value) {
        int size = 0;
        int remaining = 8;
        int bytePos = 0;
        while (remaining > 0) {
            int bestLen = 0;
            // Try to find longest matching symbol
            if (remaining >= 2) {
                long window = (value >>> (bytePos * 8));
                for (int candLen = Math.min(remaining, 8); candLen >= 2 && candLen > bestLen; candLen--) {
                    long prefix = candLen == 8 ? window : (window & ((1L << (candLen * 8)) - 1));
                    int h = hash(prefix, candLen);
                    for (int probe = 0; probe < 4; probe++) {
                        int idx = (h + probe) & (HASH_SIZE - 1);
                        int code = table.getHashCode(idx);
                        if (code < 0) break;
                        if (table.getLen(code) != candLen) continue;
                        if (table.getSymbol(code) == prefix) {
                            bestLen = candLen;
                            break;
                        }
                    }
                }
            }
            // Also try single-byte symbol (common case)
            if (bestLen <= 1) {
                int b = (int) ((value >>> (bytePos * 8)) & 0xFF);
                int singleCode = table.getByteMap(b);
                if (singleCode >= 0) {
                    size++;
                    bytePos++;
                    remaining--;
                    continue;
                }
            }
            if (bestLen >= 2) {
                size++;
                bytePos += bestLen;
                remaining -= bestLen;
            } else {
                // Escape + literal
                size += 2;
                bytePos++;
                remaining--;
            }
        }
        return size;
    }

    /**
     * Serialize symbol table to native memory.
     *
     * @return bytes written
     */
    public static int serialize(SymbolTable table, long dstAddr) {
        int off = 0;
        Unsafe.getUnsafe().putByte(dstAddr + off, (byte) table.symbolCount);
        off++;
        for (int i = 0; i < table.symbolCount; i++) {
            Unsafe.getUnsafe().putByte(dstAddr + off, (byte) table.getLen(i));
            off++;
            Unsafe.getUnsafe().putLong(dstAddr + off, table.getSymbol(i));
            off += Long.BYTES;
        }
        return off;
    }

    /**
     * Deserialize symbol table from native memory.
     *
     * @return new SymbolTable
     */
    public static SymbolTable deserialize(long srcAddr) {
        SymbolTable table = new SymbolTable();
        deserializeInto(srcAddr, table);
        return table;
    }

    /**
     * Train a symbol table from a sample of long values in native memory.
     *
     * @param srcAddr address of long array in native memory
     * @param count   number of longs
     * @return trained SymbolTable
     */
    public static SymbolTable train(long srcAddr, int count) {
        if (count == 0) {
            return new SymbolTable();
        }

        int sampleCount = Math.min(count, TRAIN_SAMPLE_LIMIT);

        // Start with single-byte symbols (all 256 possible byte values)
        long[] candidateSyms = new long[MAX_SYMBOLS];
        int[] candidateLens = new int[MAX_SYMBOLS];
        int candidateCount = 0;

        // Count byte frequencies in sample
        int[] byteFreq = new int[256];
        for (int i = 0; i < sampleCount; i++) {
            long v = Unsafe.getUnsafe().getLong(srcAddr + (long) i * Long.BYTES);
            for (int b = 0; b < 8; b++) {
                byteFreq[(int) ((v >>> (b * 8)) & 0xFF)]++;
            }
        }

        // Seed with the 255 most frequent single bytes
        int[] order = new int[256];
        for (int i = 0; i < 256; i++) order[i] = i;
        // Simple selection sort for top 255
        for (int i = 0; i < Math.min(MAX_SYMBOLS, 256); i++) {
            int best = i;
            for (int j = i + 1; j < 256; j++) {
                if (byteFreq[order[j]] > byteFreq[order[best]]) {
                    best = j;
                }
            }
            int tmp = order[i];
            order[i] = order[best];
            order[best] = tmp;
        }

        for (int i = 0; i < Math.min(MAX_SYMBOLS, 256); i++) {
            if (byteFreq[order[i]] == 0) break;
            candidateSyms[candidateCount] = order[i] & 0xFFL;
            candidateLens[candidateCount] = 1;
            candidateCount++;
        }

        if (candidateCount == 0) {
            return new SymbolTable();
        }

        SymbolTable table = new SymbolTable();
        try {
        table.populateFrom(candidateSyms, candidateLens, candidateCount);

        // Iterative refinement
        for (int iter = 0; iter < TRAIN_ITERATIONS; iter++) {
            // Compress sample with current table and count code pair frequencies
            int[] codeFreq = new int[MAX_SYMBOLS];
            // pairFreq[a * 256 + b] = frequency of code a followed by code b
            // Use a smaller hash map instead of 64K array
            int pairHashSize = 4096;
            int[] pairKeys = new int[pairHashSize];
            int[] pairVals = new int[pairHashSize];
            java.util.Arrays.fill(pairKeys, -1);

            int prevCode = -1;
            for (int i = 0; i < sampleCount; i++) {
                long value = Unsafe.getUnsafe().getLong(srcAddr + (long) i * Long.BYTES);
                int bytePos = 0;
                while (bytePos < 8) {
                    int code = findBestCode(table, value, bytePos);
                    int advance;
                    if (code >= 0) {
                        codeFreq[code]++;
                        advance = table.getLen(code);
                    } else {
                        // Escape
                        code = -1;
                        advance = 1;
                    }

                    if (prevCode >= 0 && code >= 0) {
                        int pairKey = prevCode * 256 + code;
                        int ph = (pairKey * 0x9E3779B1) >>> (32 - 12);
                        ph &= pairHashSize - 1;
                        for (int p = 0; p < 8; p++) {
                            int slot = (ph + p) & (pairHashSize - 1);
                            if (pairKeys[slot] == pairKey) {
                                pairVals[slot]++;
                                break;
                            }
                            if (pairKeys[slot] == -1) {
                                pairKeys[slot] = pairKey;
                                pairVals[slot] = 1;
                                break;
                            }
                        }
                    }
                    prevCode = code;
                    bytePos += advance;
                }
                prevCode = -1; // Reset across values
            }

            // Generate candidates: existing symbols + concatenations of frequent pairs
            long[] newSyms = new long[MAX_SYMBOLS * 2];
            int[] newLens = new int[MAX_SYMBOLS * 2];
            long[] newGains = new long[MAX_SYMBOLS * 2];
            int newCount = 0;

            // Keep existing symbols with their frequency as gain
            for (int i = 0; i < table.symbolCount; i++) {
                if (codeFreq[i] > 0 && newCount < newSyms.length) {
                    newSyms[newCount] = table.getSymbol(i);
                    newLens[newCount] = table.getLen(i);
                    // Gain = freq * (encodedWithout - encodedWith)
                    // Single-byte symbol saves (2-1)=1 byte per use if the byte would otherwise escape
                    // Multi-byte symbol saves (len*2 - 1) per use in the worst case
                    newGains[newCount] = (long) codeFreq[i] * (table.getLen(i) > 1 ? table.getLen(i) : 1);
                    newCount++;
                }
            }

            // Add pair concatenations
            for (int slot = 0; slot < pairHashSize; slot++) {
                if (pairKeys[slot] < 0 || pairVals[slot] < 2) continue;
                int a = pairKeys[slot] / 256;
                int b = pairKeys[slot] % 256;
                int combinedLen = table.getLen(a) + table.getLen(b);
                if (combinedLen > 8) continue;
                if (newCount >= newSyms.length) break;

                // Build concatenated symbol
                long sym = table.getSymbol(a) | (table.getSymbol(b) << (table.getLen(a) * 8));
                if (combinedLen < 8) {
                    sym &= (1L << (combinedLen * 8)) - 1;
                }
                newSyms[newCount] = sym;
                newLens[newCount] = combinedLen;
                // Gain: each occurrence saves 1 code byte (2 codes → 1 code)
                newGains[newCount] = pairVals[slot];
                newCount++;
            }

            // Select top 255 by gain
            // Partial sort: repeatedly find max
            int selected = Math.min(MAX_SYMBOLS, newCount);
            long[] selSyms = new long[MAX_SYMBOLS];
            int[] selLens = new int[MAX_SYMBOLS];
            int selCount = 0;
            boolean[] used = new boolean[newCount];

            for (int s = 0; s < selected; s++) {
                long bestGain = -1;
                int bestIdx = -1;
                for (int j = 0; j < newCount; j++) {
                    if (!used[j] && newGains[j] > bestGain) {
                        bestGain = newGains[j];
                        bestIdx = j;
                    }
                }
                if (bestIdx < 0 || bestGain <= 0) break;
                used[bestIdx] = true;
                selSyms[selCount] = newSyms[bestIdx];
                selLens[selCount] = newLens[bestIdx];
                selCount++;
            }

            if (selCount == 0) break;
            table.populateFrom(selSyms, selLens, selCount);
        }

        return table;
        } catch (Throwable t) {
            table.close();
            throw t;
        }
    }

    private static int encodeValue(SymbolTable table, long value, long dstAddr, int dstOff) {
        int bytePos = 0;
        while (bytePos < 8) {
            int remaining = 8 - bytePos;
            int bestLen = 0;
            int bestCode = -1;

            // Try hash lookup for multi-byte symbols
            if (remaining >= 2) {
                long window = (value >>> (bytePos * 8));
                for (int candLen = Math.min(remaining, 8); candLen >= 2 && candLen > bestLen; candLen--) {
                    long prefix = candLen == 8 ? window : (window & ((1L << (candLen * 8)) - 1));
                    int h = hash(prefix, candLen);
                    for (int probe = 0; probe < 4; probe++) {
                        int idx = (h + probe) & (HASH_SIZE - 1);
                        int code = table.getHashCode(idx);
                        if (code < 0) break;
                        if (table.getLen(code) != candLen) continue;
                        if (table.getSymbol(code) == prefix) {
                            bestLen = candLen;
                            bestCode = code;
                            break;
                        }
                    }
                }
            }

            // Try single-byte symbol
            if (bestLen <= 1) {
                int b = (int) ((value >>> (bytePos * 8)) & 0xFF);
                int singleCode = table.getByteMap(b);
                if (singleCode >= 0) {
                    Unsafe.getUnsafe().putByte(dstAddr + dstOff, (byte) singleCode);
                    dstOff++;
                    bytePos++;
                    continue;
                }
            }

            if (bestLen >= 2) {
                Unsafe.getUnsafe().putByte(dstAddr + dstOff, (byte) bestCode);
                dstOff++;
                bytePos += bestLen;
            } else {
                // Escape + literal byte
                int b = (int) ((value >>> (bytePos * 8)) & 0xFF);
                Unsafe.getUnsafe().putByte(dstAddr + dstOff, (byte) ESCAPE);
                Unsafe.getUnsafe().putByte(dstAddr + dstOff + 1, (byte) b);
                dstOff += 2;
                bytePos++;
            }
        }
        return dstOff;
    }

    private static int findBestCode(SymbolTable table, long value, int bytePos) {
        int remaining = 8 - bytePos;
        int bestLen = 0;
        int bestCode = -1;

        if (remaining >= 2) {
            long window = (value >>> (bytePos * 8));
            for (int candLen = Math.min(remaining, 8); candLen >= 2 && candLen > bestLen; candLen--) {
                long prefix = candLen == 8 ? window : (window & ((1L << (candLen * 8)) - 1));
                int h = hash(prefix, candLen);
                for (int probe = 0; probe < 4; probe++) {
                    int idx = (h + probe) & (HASH_SIZE - 1);
                    int code = table.getHashCode(idx);
                    if (code < 0) break;
                    if (table.getLen(code) != candLen) continue;
                    if (table.getSymbol(code) == prefix) {
                        bestLen = candLen;
                        bestCode = code;
                        break;
                    }
                }
            }
        }

        // Try single-byte
        if (bestLen <= 1) {
            int b = (int) ((value >>> (bytePos * 8)) & 0xFF);
            int singleCode = table.getByteMap(b);
            if (singleCode >= 0) {
                return singleCode;
            }
        }

        return bestCode;
    }

    private static int hash(long window, int remaining) {
        // Hash the first few bytes of the window
        long masked = remaining >= 8 ? window : (window & ((1L << (remaining * 8)) - 1));
        return (int) ((masked * 0x517CC1B727220A95L) >>> (64 - 14)) & (HASH_SIZE - 1);
    }

    /**
     * Native-memory symbol table. All arrays (symbols, lens, byteMap, hashCodes)
     * are stored in a single contiguous native allocation for cache locality and
     * zero GC pressure. Must be explicitly freed via {@link #close()}.
     * <p>
     * Layout of the native block:
     * <pre>
     *   [symbols: MAX_SYMBOLS * 8B][lens: MAX_SYMBOLS * 4B][byteMap: 256 * 4B][hashCodes: HASH_SIZE * 4B]
     * </pre>
     */
    public static final class SymbolTable implements io.questdb.std.QuietCloseable {
        private static final long SYMBOLS_SIZE = (long) MAX_SYMBOLS * Long.BYTES;     // 2040
        private static final long LENS_SIZE = (long) MAX_SYMBOLS * Integer.BYTES;      // 1020
        private static final long BYTEMAP_SIZE = 256L * Integer.BYTES;                 // 1024
        private static final long HASHCODES_SIZE = (long) HASH_SIZE * Integer.BYTES;   // 65536
        private static final long TOTAL_SIZE = SYMBOLS_SIZE + LENS_SIZE + BYTEMAP_SIZE + HASHCODES_SIZE;

        private static final long LENS_OFFSET = SYMBOLS_SIZE;
        private static final long BYTEMAP_OFFSET = LENS_OFFSET + LENS_SIZE;
        private static final long HASHCODES_OFFSET = BYTEMAP_OFFSET + BYTEMAP_SIZE;

        private long baseAddr;
        int symbolCount;

        SymbolTable() {
            this.baseAddr = Unsafe.malloc(TOTAL_SIZE, MemoryTag.NATIVE_INDEX_READER);
            this.symbolCount = 0;
        }

        long getSymbol(int idx) {
            return Unsafe.getUnsafe().getLong(baseAddr + (long) idx * Long.BYTES);
        }

        void putSymbol(int idx, long sym) {
            Unsafe.getUnsafe().putLong(baseAddr + (long) idx * Long.BYTES, sym);
        }

        int getLen(int idx) {
            return Unsafe.getUnsafe().getInt(baseAddr + LENS_OFFSET + (long) idx * Integer.BYTES);
        }

        void putLen(int idx, int len) {
            Unsafe.getUnsafe().putInt(baseAddr + LENS_OFFSET + (long) idx * Integer.BYTES, len);
        }

        int getByteMap(int b) {
            return Unsafe.getUnsafe().getInt(baseAddr + BYTEMAP_OFFSET + (long) b * Integer.BYTES);
        }

        void putByteMap(int b, int code) {
            Unsafe.getUnsafe().putInt(baseAddr + BYTEMAP_OFFSET + (long) b * Integer.BYTES, code);
        }

        int getHashCode(int idx) {
            return Unsafe.getUnsafe().getInt(baseAddr + HASHCODES_OFFSET + (long) idx * Integer.BYTES);
        }

        void putHashCode(int idx, int code) {
            Unsafe.getUnsafe().putInt(baseAddr + HASHCODES_OFFSET + (long) idx * Integer.BYTES, code);
        }

        /**
         * Rebuild lookup structures (byteMap, hashCodes) from the current symbols/lens.
         * Called after symbols and lens have been populated.
         */
        void rebuildLookups() {
            // Fill byteMap with -1
            Unsafe.getUnsafe().setMemory(baseAddr + BYTEMAP_OFFSET, BYTEMAP_SIZE, (byte) 0xFF);
            for (int i = 0; i < symbolCount; i++) {
                if (getLen(i) == 1) {
                    int b = (int) (getSymbol(i) & 0xFF);
                    if (getByteMap(b) == -1 && i != ESCAPE) {
                        putByteMap(b, i);
                    }
                }
            }

            // Fill hashCodes with -1
            Unsafe.getUnsafe().setMemory(baseAddr + HASHCODES_OFFSET, HASHCODES_SIZE, (byte) 0xFF);
            for (int pass = 8; pass >= 2; pass--) {
                for (int i = 0; i < symbolCount; i++) {
                    if (getLen(i) != pass) continue;
                    if (i == ESCAPE) continue;
                    int h = hash(getSymbol(i), getLen(i));
                    for (int probe = 0; probe < 4; probe++) {
                        int idx = (h + probe) & (HASH_SIZE - 1);
                        if (getHashCode(idx) < 0) {
                            putHashCode(idx, i);
                            break;
                        }
                    }
                }
            }
        }

        /**
         * Populate from Java arrays (used by train()). Zero-copy into native memory.
         */
        void populateFrom(long[] syms, int[] lens, int count) {
            this.symbolCount = count;
            for (int i = 0; i < count; i++) {
                putSymbol(i, syms[i]);
                putLen(i, lens[i]);
            }
            rebuildLookups();
        }

        @Override
        public void close() {
            if (baseAddr != 0) {
                Unsafe.free(baseAddr, TOTAL_SIZE, MemoryTag.NATIVE_INDEX_READER);
                baseAddr = 0;
            }
        }
    }

    /**
     * Deserialize symbol table from native memory into an existing table (zero allocation).
     */
    public static void deserializeInto(long srcAddr, SymbolTable table) {
        int count = Unsafe.getUnsafe().getByte(srcAddr) & 0xFF;
        int off = 1;
        for (int i = 0; i < count; i++) {
            int len = Unsafe.getUnsafe().getByte(srcAddr + off) & 0xFF;
            off++;
            long sym = Unsafe.getUnsafe().getLong(srcAddr + off);
            if (len < 8) {
                sym &= (1L << (len * 8)) - 1;
            }
            off += Long.BYTES;
            table.putSymbol(i, sym);
            table.putLen(i, len);
        }
        table.symbolCount = count;
        table.rebuildLookups();
    }
}
