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
                    int len = table.lens[code];
                    long sym = table.symbols[code];
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
                int len = table.lens[code];
                long sym = table.symbols[code];
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
                // Extract up to 8 bytes starting at bytePos for hash lookup
                long window = (value >>> (bytePos * 8));
                int h = hash(window, remaining);
                for (int probe = 0; probe < 4; probe++) {
                    int idx = (h + probe) & (HASH_SIZE - 1);
                    int code = table.hashCodes[idx];
                    if (code < 0) break;
                    int slen = table.lens[code];
                    if (slen > remaining) continue;
                    if (slen <= bestLen) continue;
                    long sym = table.symbols[code];
                    long mask = slen == 8 ? -1L : (1L << (slen * 8)) - 1;
                    if ((window & mask) == sym) {
                        bestLen = slen;
                    }
                }
            }
            // Also try single-byte symbol (common case)
            if (bestLen <= 1) {
                int b = (int) ((value >>> (bytePos * 8)) & 0xFF);
                int singleCode = table.byteMap[b];
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
            Unsafe.getUnsafe().putByte(dstAddr + off, (byte) table.lens[i]);
            off++;
            Unsafe.getUnsafe().putLong(dstAddr + off, table.symbols[i]);
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
        int count = Unsafe.getUnsafe().getByte(srcAddr) & 0xFF;
        int off = 1;
        int[] lens = new int[MAX_SYMBOLS];
        long[] symbols = new long[MAX_SYMBOLS];
        for (int i = 0; i < count; i++) {
            lens[i] = Unsafe.getUnsafe().getByte(srcAddr + off) & 0xFF;
            off++;
            symbols[i] = Unsafe.getUnsafe().getLong(srcAddr + off);
            // Mask the symbol to its actual length
            if (lens[i] < 8) {
                symbols[i] &= (1L << (lens[i] * 8)) - 1;
            }
            off += Long.BYTES;
        }
        return new SymbolTable(symbols, lens, count);
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
            return new SymbolTable(new long[0], new int[0], 0);
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
            return new SymbolTable(new long[0], new int[0], 0);
        }

        SymbolTable table = new SymbolTable(candidateSyms, candidateLens, candidateCount);

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
                        advance = table.lens[code];
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
                    newSyms[newCount] = table.symbols[i];
                    newLens[newCount] = table.lens[i];
                    // Gain = freq * (encodedWithout - encodedWith)
                    // Single-byte symbol saves (2-1)=1 byte per use if the byte would otherwise escape
                    // Multi-byte symbol saves (len*2 - 1) per use in the worst case
                    newGains[newCount] = (long) codeFreq[i] * (table.lens[i] > 1 ? table.lens[i] : 1);
                    newCount++;
                }
            }

            // Add pair concatenations
            for (int slot = 0; slot < pairHashSize; slot++) {
                if (pairKeys[slot] < 0 || pairVals[slot] < 2) continue;
                int a = pairKeys[slot] / 256;
                int b = pairKeys[slot] % 256;
                int combinedLen = table.lens[a] + table.lens[b];
                if (combinedLen > 8) continue;
                if (newCount >= newSyms.length) break;

                // Build concatenated symbol
                long sym = table.symbols[a] | (table.symbols[b] << (table.lens[a] * 8));
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
            table = new SymbolTable(selSyms, selLens, selCount);
        }

        return table;
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
                int h = hash(window, remaining);
                for (int probe = 0; probe < 4; probe++) {
                    int idx = (h + probe) & (HASH_SIZE - 1);
                    int code = table.hashCodes[idx];
                    if (code < 0) break;
                    int slen = table.lens[code];
                    if (slen > remaining) continue;
                    if (slen <= bestLen) continue;
                    long sym = table.symbols[code];
                    long mask = slen == 8 ? -1L : (1L << (slen * 8)) - 1;
                    if ((window & mask) == sym) {
                        bestLen = slen;
                        bestCode = code;
                    }
                }
            }

            // Try single-byte symbol
            if (bestLen <= 1) {
                int b = (int) ((value >>> (bytePos * 8)) & 0xFF);
                int singleCode = table.byteMap[b];
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
            int h = hash(window, remaining);
            for (int probe = 0; probe < 4; probe++) {
                int idx = (h + probe) & (HASH_SIZE - 1);
                int code = table.hashCodes[idx];
                if (code < 0) break;
                int slen = table.lens[code];
                if (slen > remaining) continue;
                if (slen <= bestLen) continue;
                long sym = table.symbols[code];
                long mask = slen == 8 ? -1L : (1L << (slen * 8)) - 1;
                if ((window & mask) == sym) {
                    bestLen = slen;
                    bestCode = code;
                }
            }
        }

        // Try single-byte
        if (bestLen <= 1) {
            int b = (int) ((value >>> (bytePos * 8)) & 0xFF);
            int singleCode = table.byteMap[b];
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

    public static final class SymbolTable {
        final int[] byteMap;   // byte value → code index for single-byte symbols, -1 if none
        final int[] hashCodes; // hash table: hash → code index for multi-byte symbols
        final int[] lens;      // symbol lengths (1-8 bytes)
        final int symbolCount;
        final long[] symbols;  // symbol values (little-endian packed bytes)

        SymbolTable(long[] symbols, int[] lens, int symbolCount) {
            this.symbols = new long[MAX_SYMBOLS];
            this.lens = new int[MAX_SYMBOLS];
            this.symbolCount = symbolCount;

            System.arraycopy(symbols, 0, this.symbols, 0, symbolCount);
            System.arraycopy(lens, 0, this.lens, 0, symbolCount);

            // Build byte map for single-byte symbols
            this.byteMap = new int[256];
            java.util.Arrays.fill(byteMap, -1);
            for (int i = 0; i < symbolCount; i++) {
                if (lens[i] == 1) {
                    int b = (int) (symbols[i] & 0xFF);
                    // Keep first (highest priority) mapping
                    if (byteMap[b] == -1 && i != ESCAPE) {
                        byteMap[b] = i;
                    }
                }
            }

            // Build hash table for multi-byte symbols (longest first for greedy matching)
            this.hashCodes = new int[HASH_SIZE];
            java.util.Arrays.fill(hashCodes, -1);

            // Insert longest symbols first so they get priority slots
            for (int pass = 8; pass >= 2; pass--) {
                for (int i = 0; i < symbolCount; i++) {
                    if (lens[i] != pass) continue;
                    if (i == ESCAPE) continue;
                    int h = hash(symbols[i], lens[i]);
                    for (int probe = 0; probe < 4; probe++) {
                        int idx = (h + probe) & (HASH_SIZE - 1);
                        if (hashCodes[idx] < 0) {
                            hashCodes[idx] = i;
                            break;
                        }
                    }
                }
            }
        }
    }
}
