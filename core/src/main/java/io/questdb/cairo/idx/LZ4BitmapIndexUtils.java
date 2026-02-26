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

import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Constants and LZ4 block codec for the page-grouped LZ4-compressed bitmap index.
 * <p>
 * Groups multiple keys into large LZ4-compressed pages (~64 KB uncompressed)
 * for much better compression than per-key blocks. Within each page, values
 * are interleaved by round for optimal LZ4 match distances.
 * <p>
 * Key File Layout (.lk):
 * <pre>
 * [Header 64B: sig, seq, valMemSize, blockValues, keyCount, seqCheck, maxVal, keysPerPage, pageCount]
 * [Page Directory: pageCount × 12B (offset(8), compressedSize(4))]
 * </pre>
 * <p>
 * Value File Layout (.lv):
 * <pre>
 * [Page 0 compressed data]
 * [Page 1 compressed data]
 * ...
 * </pre>
 * <p>
 * Page Format (uncompressed, before LZ4):
 * <pre>
 * [Per-key counts: keysInPage × int, 8-byte aligned]
 * [Round 0: val0 of key0, val0 of key1, ..., val0 of keyN-1]
 * [Round 1: val1 of key0, val1 of key1, ..., val1 of keyN-1]
 * ...
 * </pre>
 */
public final class LZ4BitmapIndexUtils {

    // === Key File Header (64 bytes) ===
    public static final int KEY_FILE_RESERVED = 64;
    public static final int KEY_RESERVED_OFFSET_SIGNATURE = 0;
    public static final int KEY_RESERVED_OFFSET_SEQUENCE = 8;
    public static final int KEY_RESERVED_OFFSET_VALUE_MEM_SIZE = 16;
    public static final int KEY_RESERVED_OFFSET_BLOCK_VALUES = 24;
    public static final int KEY_RESERVED_OFFSET_KEY_COUNT = 28;
    public static final int KEY_RESERVED_OFFSET_SEQUENCE_CHECK = 32;
    public static final int KEY_RESERVED_OFFSET_MAX_VALUE = 40;
    public static final int KEY_RESERVED_OFFSET_KEYS_PER_PAGE = 48;
    public static final int KEY_RESERVED_OFFSET_PAGE_COUNT = 52;
    public static final int KEY_RESERVED_OFFSET_PAGES_PER_GEN = 56;

    // === Page Directory Entry (12 bytes per page) ===
    public static final int PAGE_DIR_ENTRY_SIZE = 12;
    public static final int PAGE_DIR_OFFSET_FILE_OFFSET = 0;
    public static final int PAGE_DIR_OFFSET_COMPRESSED_SIZE = 8;

    public static final int DEFAULT_BLOCK_VALUES = 128;
    public static final int DEFAULT_TARGET_PAGE_SIZE = 65536; // 64 KB

    public static final byte SIGNATURE = (byte) 0xfe;

    // === LZ4 Block Codec Constants ===
    private static final int HASH_LOG = 12;
    private static final int HASH_TABLE_SIZE = 1 << HASH_LOG;
    private static final int MIN_MATCH = 4;
    private static final int MATCH_FIND_LIMIT = 5;
    private static final int MAX_DISTANCE = 65535;

    private LZ4BitmapIndexUtils() {
    }

    /**
     * LZ4 block compress from native memory.
     *
     * @param src         source address
     * @param srcLen      source length in bytes
     * @param dst         destination address (must have at least {@link #maxCompressedLength(int)} bytes)
     * @param dstCapacity destination capacity
     * @param hashTable   reusable hash table, must be int[4096]
     * @return compressed size in bytes, or srcLen if compression didn't help (caller should store raw)
     */
    public static int compress(long src, int srcLen, long dst, int dstCapacity, int[] hashTable) {
        if (srcLen == 0) {
            return 0;
        }

        if (srcLen < MIN_MATCH + MATCH_FIND_LIMIT) {
            return emitLiteralsOnly(src, srcLen, dst, dstCapacity);
        }

        // No hash table clear needed: stale entries from previous calls are
        // safe because (1) ref < sIdx ensures forward-only references,
        // (2) the 4-byte match check validates against current source data,
        // and (3) Java zero-initializes the array so fresh entries are 0.
        int sIdx = 0;
        int dIdx = 0;
        int anchor = 0;
        int searchLimit = srcLen - MATCH_FIND_LIMIT;

        hashTable[hash4(Unsafe.getUnsafe().getInt(src))] = 0;
        sIdx = 1;

        while (sIdx < searchLimit) {
            int h = hash4(Unsafe.getUnsafe().getInt(src + sIdx));
            int ref = hashTable[h];
            hashTable[h] = sIdx;

            if (ref >= 0
                    && ref < sIdx
                    && sIdx - ref <= MAX_DISTANCE
                    && Unsafe.getUnsafe().getInt(src + ref) == Unsafe.getUnsafe().getInt(src + sIdx)) {
                int litLen = sIdx - anchor;

                int matchLen = MIN_MATCH;
                int maxMatchLen = srcLen - sIdx;
                while (matchLen < maxMatchLen
                        && Unsafe.getUnsafe().getByte(src + ref + matchLen) == Unsafe.getUnsafe().getByte(src + sIdx + matchLen)) {
                    matchLen++;
                }

                int worstTokenBytes = 1 + (litLen >= 15 ? 1 + litLen / 255 : 0) + litLen + 2 + (matchLen - 4 >= 15 ? 1 + (matchLen - 4) / 255 : 0);
                if (dIdx + worstTokenBytes > dstCapacity) {
                    return srcLen;
                }

                int tokenLit = Math.min(litLen, 15);
                int tokenMatch = Math.min(matchLen - MIN_MATCH, 15);
                Unsafe.getUnsafe().putByte(dst + dIdx++, (byte) ((tokenLit << 4) | tokenMatch));

                if (litLen >= 15) {
                    int remaining = litLen - 15;
                    while (remaining >= 255) {
                        Unsafe.getUnsafe().putByte(dst + dIdx++, (byte) 0xFF);
                        remaining -= 255;
                    }
                    Unsafe.getUnsafe().putByte(dst + dIdx++, (byte) remaining);
                }

                Unsafe.getUnsafe().copyMemory(src + anchor, dst + dIdx, litLen);
                dIdx += litLen;

                int offset = sIdx - ref;
                Unsafe.getUnsafe().putByte(dst + dIdx++, (byte) (offset & 0xFF));
                Unsafe.getUnsafe().putByte(dst + dIdx++, (byte) ((offset >> 8) & 0xFF));

                if (matchLen - MIN_MATCH >= 15) {
                    int remaining = matchLen - MIN_MATCH - 15;
                    while (remaining >= 255) {
                        Unsafe.getUnsafe().putByte(dst + dIdx++, (byte) 0xFF);
                        remaining -= 255;
                    }
                    Unsafe.getUnsafe().putByte(dst + dIdx++, (byte) remaining);
                }

                sIdx += matchLen;
                anchor = sIdx;

                if (sIdx < searchLimit) {
                    hashTable[hash4(Unsafe.getUnsafe().getInt(src + sIdx))] = sIdx;
                }
            } else {
                sIdx++;
            }
        }

        int lastLitLen = srcLen - anchor;
        int lastTokenBytes = 1 + (lastLitLen >= 15 ? 1 + lastLitLen / 255 : 0) + lastLitLen;
        if (dIdx + lastTokenBytes > dstCapacity) {
            return srcLen;
        }

        int tokenLit = Math.min(lastLitLen, 15);
        Unsafe.getUnsafe().putByte(dst + dIdx++, (byte) (tokenLit << 4));
        if (lastLitLen >= 15) {
            int remaining = lastLitLen - 15;
            while (remaining >= 255) {
                Unsafe.getUnsafe().putByte(dst + dIdx++, (byte) 0xFF);
                remaining -= 255;
            }
            Unsafe.getUnsafe().putByte(dst + dIdx++, (byte) remaining);
        }
        Unsafe.getUnsafe().copyMemory(src + anchor, dst + dIdx, lastLitLen);
        dIdx += lastLitLen;

        return dIdx;
    }

    public static int computeKeysPerPage(int blockValues) {
        return computeKeysPerPage(blockValues, DEFAULT_TARGET_PAGE_SIZE);
    }

    public static int computeKeysPerPage(int blockValues, int targetPageSize) {
        int bytesPerKey = Integer.BYTES + blockValues * (int) Long.BYTES;
        return Math.max(1, targetPageSize / bytesPerKey);
    }

    /**
     * LZ4 block decompress from native memory.
     *
     * @param src    compressed source address
     * @param srcLen compressed length
     * @param dst    destination address
     * @param dstLen expected uncompressed length (must be exact)
     */
    public static void decompress(long src, int srcLen, long dst, int dstLen) {
        int sIdx = 0;
        int dIdx = 0;

        while (sIdx < srcLen) {
            int token = Unsafe.getUnsafe().getByte(src + sIdx++) & 0xFF;

            int litLen = token >>> 4;
            if (litLen == 15) {
                int b;
                do {
                    b = Unsafe.getUnsafe().getByte(src + sIdx++) & 0xFF;
                    litLen += b;
                } while (b == 255);
            }

            Unsafe.getUnsafe().copyMemory(src + sIdx, dst + dIdx, litLen);
            sIdx += litLen;
            dIdx += litLen;

            if (dIdx >= dstLen) {
                break;
            }

            int offset = (Unsafe.getUnsafe().getByte(src + sIdx) & 0xFF)
                    | ((Unsafe.getUnsafe().getByte(src + sIdx + 1) & 0xFF) << 8);
            sIdx += 2;

            int matchLen = (token & 0x0F) + MIN_MATCH;
            if ((token & 0x0F) == 15) {
                int b;
                do {
                    b = Unsafe.getUnsafe().getByte(src + sIdx++) & 0xFF;
                    matchLen += b;
                } while (b == 255);
            }

            long matchSrc = dst + dIdx - offset;
            if (offset >= 8 && matchLen <= offset) {
                Unsafe.getUnsafe().copyMemory(matchSrc, dst + dIdx, matchLen);
            } else {
                for (int i = 0; i < matchLen; i++) {
                    Unsafe.getUnsafe().putByte(dst + dIdx + i, Unsafe.getUnsafe().getByte(matchSrc + i));
                }
            }
            dIdx += matchLen;
        }
    }

    public static long getPageDirOffset(int pageIndex) {
        return KEY_FILE_RESERVED + (long) pageIndex * PAGE_DIR_ENTRY_SIZE;
    }

    public static int keysInPage(int pageIndex, int keysPerPage, int keyCount) {
        int firstKey = pageIndex * keysPerPage;
        return Math.min(keysPerPage, keyCount - firstKey);
    }

    public static LPSZ keyFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".lk");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }

    public static int maxCompressedLength(int srcLen) {
        return srcLen + srcLen / 255 + 16;
    }

    public static int pageCountsSize(int keysInPage) {
        return (keysInPage * Integer.BYTES + 7) & ~7;
    }

    public static int pageRawSize(int keysInPage, int blockValues) {
        return pageCountsSize(keysInPage) + keysInPage * blockValues * (int) Long.BYTES;
    }

    public static LPSZ valueFileName(Path path, CharSequence name, long columnNameTxn) {
        path.concat(name).put(".lv");
        if (columnNameTxn > COLUMN_NAME_TXN_NONE) {
            path.put('.').put(columnNameTxn);
        }
        return path.$();
    }

    private static int emitLiteralsOnly(long src, int srcLen, long dst, int dstCapacity) {
        int dIdx = 0;
        int tokenLit = Math.min(srcLen, 15);
        if (1 + (srcLen >= 15 ? 1 + srcLen / 255 : 0) + srcLen > dstCapacity) {
            return srcLen;
        }
        Unsafe.getUnsafe().putByte(dst + dIdx++, (byte) (tokenLit << 4));
        if (srcLen >= 15) {
            int remaining = srcLen - 15;
            while (remaining >= 255) {
                Unsafe.getUnsafe().putByte(dst + dIdx++, (byte) 0xFF);
                remaining -= 255;
            }
            Unsafe.getUnsafe().putByte(dst + dIdx++, (byte) remaining);
        }
        Unsafe.getUnsafe().copyMemory(src, dst + dIdx, srcLen);
        dIdx += srcLen;
        return dIdx;
    }

    private static int hash4(int v) {
        return (int) (((long) v * 2654435761L) >>> (32 - HASH_LOG)) & (HASH_TABLE_SIZE - 1);
    }
}
