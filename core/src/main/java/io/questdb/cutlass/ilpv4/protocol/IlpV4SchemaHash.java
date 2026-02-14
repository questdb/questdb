/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cutlass.ilpv4.protocol;


import io.questdb.std.Unsafe;

/**
 * XXHash64 implementation for schema hashing in ILP v4 protocol.
 * <p>
 * The schema hash is computed over column definitions (name + type) to enable
 * schema caching. When a client sends a schema reference (hash), the server
 * can look up the cached schema instead of re-parsing the full schema each time.
 * <p>
 * This is a pure Java implementation of XXHash64 based on the original algorithm
 * by Yann Collet. It's optimized for small inputs typical of schema hashing.
 *
 * @see <a href="https://github.com/Cyan4973/xxHash">xxHash</a>
 */
public final class IlpV4SchemaHash {

    // XXHash64 constants
    private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
    private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
    private static final long PRIME64_3 = 0x165667B19E3779F9L;
    private static final long PRIME64_4 = 0x85EBCA77C2B2AE63L;
    private static final long PRIME64_5 = 0x27D4EB2F165667C5L;

    // Default seed (0 for ILP v4)
    private static final long DEFAULT_SEED = 0L;

    // Thread-local Hasher to avoid allocation on every computeSchemaHash call
    private static final ThreadLocal<Hasher> HASHER_POOL = ThreadLocal.withInitial(Hasher::new);

    private IlpV4SchemaHash() {
        // utility class
    }

    /**
     * Computes XXHash64 of a byte array.
     *
     * @param data the data to hash
     * @return the 64-bit hash value
     */
    public static long hash(byte[] data) {
        return hash(data, 0, data.length, DEFAULT_SEED);
    }

    /**
     * Computes XXHash64 of a byte array region.
     *
     * @param data   the data to hash
     * @param offset starting offset
     * @param length number of bytes to hash
     * @return the 64-bit hash value
     */
    public static long hash(byte[] data, int offset, int length) {
        return hash(data, offset, length, DEFAULT_SEED);
    }

    /**
     * Computes XXHash64 of a byte array region with custom seed.
     *
     * @param data   the data to hash
     * @param offset starting offset
     * @param length number of bytes to hash
     * @param seed   the hash seed
     * @return the 64-bit hash value
     */
    public static long hash(byte[] data, int offset, int length, long seed) {
        long h64;
        int end = offset + length;
        int pos = offset;

        if (length >= 32) {
            int limit = end - 32;
            long v1 = seed + PRIME64_1 + PRIME64_2;
            long v2 = seed + PRIME64_2;
            long v3 = seed;
            long v4 = seed - PRIME64_1;

            do {
                v1 = round(v1, getLong(data, pos));
                pos += 8;
                v2 = round(v2, getLong(data, pos));
                pos += 8;
                v3 = round(v3, getLong(data, pos));
                pos += 8;
                v4 = round(v4, getLong(data, pos));
                pos += 8;
            } while (pos <= limit);

            h64 = Long.rotateLeft(v1, 1) + Long.rotateLeft(v2, 7) +
                    Long.rotateLeft(v3, 12) + Long.rotateLeft(v4, 18);
            h64 = mergeRound(h64, v1);
            h64 = mergeRound(h64, v2);
            h64 = mergeRound(h64, v3);
            h64 = mergeRound(h64, v4);
        } else {
            h64 = seed + PRIME64_5;
        }

        h64 += length;

        // Process remaining 8-byte blocks
        while (pos + 8 <= end) {
            long k1 = getLong(data, pos);
            k1 *= PRIME64_2;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= PRIME64_1;
            h64 ^= k1;
            h64 = Long.rotateLeft(h64, 27) * PRIME64_1 + PRIME64_4;
            pos += 8;
        }

        // Process remaining 4-byte block
        if (pos + 4 <= end) {
            h64 ^= (getInt(data, pos) & 0xFFFFFFFFL) * PRIME64_1;
            h64 = Long.rotateLeft(h64, 23) * PRIME64_2 + PRIME64_3;
            pos += 4;
        }

        // Process remaining bytes
        while (pos < end) {
            h64 ^= (data[pos] & 0xFFL) * PRIME64_5;
            h64 = Long.rotateLeft(h64, 11) * PRIME64_1;
            pos++;
        }

        return avalanche(h64);
    }

    /**
     * Computes XXHash64 of direct memory.
     *
     * @param address start address
     * @param length  number of bytes
     * @return the 64-bit hash value
     */
    public static long hash(long address, long length) {
        return hash(address, length, DEFAULT_SEED);
    }

    /**
     * Computes XXHash64 of direct memory with custom seed.
     *
     * @param address start address
     * @param length  number of bytes
     * @param seed    the hash seed
     * @return the 64-bit hash value
     */
    public static long hash(long address, long length, long seed) {
        long h64;
        long end = address + length;
        long pos = address;

        if (length >= 32) {
            long limit = end - 32;
            long v1 = seed + PRIME64_1 + PRIME64_2;
            long v2 = seed + PRIME64_2;
            long v3 = seed;
            long v4 = seed - PRIME64_1;

            do {
                v1 = round(v1, Unsafe.getUnsafe().getLong(pos));
                pos += 8;
                v2 = round(v2, Unsafe.getUnsafe().getLong(pos));
                pos += 8;
                v3 = round(v3, Unsafe.getUnsafe().getLong(pos));
                pos += 8;
                v4 = round(v4, Unsafe.getUnsafe().getLong(pos));
                pos += 8;
            } while (pos <= limit);

            h64 = Long.rotateLeft(v1, 1) + Long.rotateLeft(v2, 7) +
                    Long.rotateLeft(v3, 12) + Long.rotateLeft(v4, 18);
            h64 = mergeRound(h64, v1);
            h64 = mergeRound(h64, v2);
            h64 = mergeRound(h64, v3);
            h64 = mergeRound(h64, v4);
        } else {
            h64 = seed + PRIME64_5;
        }

        h64 += length;

        // Process remaining 8-byte blocks
        while (pos + 8 <= end) {
            long k1 = Unsafe.getUnsafe().getLong(pos);
            k1 *= PRIME64_2;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= PRIME64_1;
            h64 ^= k1;
            h64 = Long.rotateLeft(h64, 27) * PRIME64_1 + PRIME64_4;
            pos += 8;
        }

        // Process remaining 4-byte block
        if (pos + 4 <= end) {
            h64 ^= (Unsafe.getUnsafe().getInt(pos) & 0xFFFFFFFFL) * PRIME64_1;
            h64 = Long.rotateLeft(h64, 23) * PRIME64_2 + PRIME64_3;
            pos += 4;
        }

        // Process remaining bytes
        while (pos < end) {
            h64 ^= (Unsafe.getUnsafe().getByte(pos) & 0xFFL) * PRIME64_5;
            h64 = Long.rotateLeft(h64, 11) * PRIME64_1;
            pos++;
        }

        return avalanche(h64);
    }

    /**
     * Computes the schema hash for ILP v4 using String column names.
     * Note: Iterates over String chars and converts to UTF-8 bytes directly to avoid getBytes() allocation.
     *
     * @param columnNames array of column names
     * @param columnTypes array of type codes
     * @return the schema hash
     */
    public static long computeSchemaHash(String[] columnNames, byte[] columnTypes) {
        // Use pooled hasher to avoid allocation
        Hasher hasher = HASHER_POOL.get();
        hasher.reset(DEFAULT_SEED);

        for (int i = 0; i < columnNames.length; i++) {
            String name = columnNames[i];
            // Encode UTF-8 directly without allocating byte array
            for (int j = 0, len = name.length(); j < len; j++) {
                char c = name.charAt(j);
                if (c < 0x80) {
                    // Single byte (ASCII)
                    hasher.update((byte) c);
                } else if (c < 0x800) {
                    // Two bytes
                    hasher.update((byte) (0xC0 | (c >> 6)));
                    hasher.update((byte) (0x80 | (c & 0x3F)));
                } else if (c >= 0xD800 && c <= 0xDBFF && j + 1 < len) {
                    // Surrogate pair (4 bytes)
                    char c2 = name.charAt(++j);
                    int codePoint = 0x10000 + ((c - 0xD800) << 10) + (c2 - 0xDC00);
                    hasher.update((byte) (0xF0 | (codePoint >> 18)));
                    hasher.update((byte) (0x80 | ((codePoint >> 12) & 0x3F)));
                    hasher.update((byte) (0x80 | ((codePoint >> 6) & 0x3F)));
                    hasher.update((byte) (0x80 | (codePoint & 0x3F)));
                } else {
                    // Three bytes
                    hasher.update((byte) (0xE0 | (c >> 12)));
                    hasher.update((byte) (0x80 | ((c >> 6) & 0x3F)));
                    hasher.update((byte) (0x80 | (c & 0x3F)));
                }
            }
            hasher.update(columnTypes[i]);
        }

        return hasher.getValue();
    }

    /**
     * Computes the schema hash directly from column buffers without intermediate arrays.
     * This is the most efficient method when column data is already available.
     *
     * @param columns list of column buffers
     * @return the schema hash
     */
    public static long computeSchemaHashDirect(io.questdb.std.ObjList<IlpV4TableBuffer.ColumnBuffer> columns) {
        // Use pooled hasher to avoid allocation
        Hasher hasher = HASHER_POOL.get();
        hasher.reset(DEFAULT_SEED);

        for (int i = 0, n = columns.size(); i < n; i++) {
            IlpV4TableBuffer.ColumnBuffer col = columns.get(i);
            String name = col.getName();
            // Encode UTF-8 directly without allocating byte array
            for (int j = 0, len = name.length(); j < len; j++) {
                char c = name.charAt(j);
                if (c < 0x80) {
                    hasher.update((byte) c);
                } else if (c < 0x800) {
                    hasher.update((byte) (0xC0 | (c >> 6)));
                    hasher.update((byte) (0x80 | (c & 0x3F)));
                } else if (c >= 0xD800 && c <= 0xDBFF && j + 1 < len) {
                    char c2 = name.charAt(++j);
                    int codePoint = 0x10000 + ((c - 0xD800) << 10) + (c2 - 0xDC00);
                    hasher.update((byte) (0xF0 | (codePoint >> 18)));
                    hasher.update((byte) (0x80 | ((codePoint >> 12) & 0x3F)));
                    hasher.update((byte) (0x80 | ((codePoint >> 6) & 0x3F)));
                    hasher.update((byte) (0x80 | (codePoint & 0x3F)));
                } else {
                    hasher.update((byte) (0xE0 | (c >> 12)));
                    hasher.update((byte) (0x80 | ((c >> 6) & 0x3F)));
                    hasher.update((byte) (0x80 | (c & 0x3F)));
                }
            }
            // Wire type code: type | (nullable ? 0x80 : 0)
            byte wireType = (byte) (col.getType() | (col.nullable ? 0x80 : 0));
            hasher.update(wireType);
        }

        return hasher.getValue();
    }

    private static long round(long acc, long input) {
        acc += input * PRIME64_2;
        acc = Long.rotateLeft(acc, 31);
        acc *= PRIME64_1;
        return acc;
    }

    private static long mergeRound(long acc, long val) {
        val = round(0, val);
        acc ^= val;
        acc = acc * PRIME64_1 + PRIME64_4;
        return acc;
    }

    private static long avalanche(long h64) {
        h64 ^= h64 >>> 33;
        h64 *= PRIME64_2;
        h64 ^= h64 >>> 29;
        h64 *= PRIME64_3;
        h64 ^= h64 >>> 32;
        return h64;
    }

    private static long getLong(byte[] data, int pos) {
        return ((long) data[pos] & 0xFF) |
                (((long) data[pos + 1] & 0xFF) << 8) |
                (((long) data[pos + 2] & 0xFF) << 16) |
                (((long) data[pos + 3] & 0xFF) << 24) |
                (((long) data[pos + 4] & 0xFF) << 32) |
                (((long) data[pos + 5] & 0xFF) << 40) |
                (((long) data[pos + 6] & 0xFF) << 48) |
                (((long) data[pos + 7] & 0xFF) << 56);
    }

    private static int getInt(byte[] data, int pos) {
        return (data[pos] & 0xFF) |
                ((data[pos + 1] & 0xFF) << 8) |
                ((data[pos + 2] & 0xFF) << 16) |
                ((data[pos + 3] & 0xFF) << 24);
    }

    /**
     * Streaming hasher for incremental hash computation.
     * <p>
     * This is useful when building the schema hash incrementally
     * as columns are processed.
     */
    public static class Hasher {
        private long v1, v2, v3, v4;
        private long totalLen;
        private final byte[] buffer = new byte[32];
        private int bufferPos;
        private long seed;

        public Hasher() {
            reset(DEFAULT_SEED);
        }

        /**
         * Resets the hasher with the given seed.
         *
         * @param seed the hash seed
         */
        public void reset(long seed) {
            this.seed = seed;
            v1 = seed + PRIME64_1 + PRIME64_2;
            v2 = seed + PRIME64_2;
            v3 = seed;
            v4 = seed - PRIME64_1;
            totalLen = 0;
            bufferPos = 0;
        }

        /**
         * Updates the hash with a single byte.
         *
         * @param b the byte to add
         */
        public void update(byte b) {
            buffer[bufferPos++] = b;
            totalLen++;

            if (bufferPos == 32) {
                processBuffer();
            }
        }

        /**
         * Updates the hash with a byte array.
         *
         * @param data the bytes to add
         */
        public void update(byte[] data) {
            update(data, 0, data.length);
        }

        /**
         * Updates the hash with a byte array region.
         *
         * @param data   the bytes to add
         * @param offset starting offset
         * @param length number of bytes
         */
        public void update(byte[] data, int offset, int length) {
            totalLen += length;

            // Fill buffer first
            if (bufferPos > 0) {
                int toCopy = Math.min(32 - bufferPos, length);
                System.arraycopy(data, offset, buffer, bufferPos, toCopy);
                bufferPos += toCopy;
                offset += toCopy;
                length -= toCopy;

                if (bufferPos == 32) {
                    processBuffer();
                }
            }

            // Process 32-byte blocks directly
            while (length >= 32) {
                v1 = round(v1, getLong(data, offset));
                v2 = round(v2, getLong(data, offset + 8));
                v3 = round(v3, getLong(data, offset + 16));
                v4 = round(v4, getLong(data, offset + 24));
                offset += 32;
                length -= 32;
            }

            // Buffer remaining
            if (length > 0) {
                System.arraycopy(data, offset, buffer, 0, length);
                bufferPos = length;
            }
        }

        /**
         * Finalizes and returns the hash value.
         *
         * @return the 64-bit hash
         */
        public long getValue() {
            long h64;

            if (totalLen >= 32) {
                h64 = Long.rotateLeft(v1, 1) + Long.rotateLeft(v2, 7) +
                        Long.rotateLeft(v3, 12) + Long.rotateLeft(v4, 18);
                h64 = mergeRound(h64, v1);
                h64 = mergeRound(h64, v2);
                h64 = mergeRound(h64, v3);
                h64 = mergeRound(h64, v4);
            } else {
                h64 = seed + PRIME64_5;
            }

            h64 += totalLen;

            // Process buffered data
            int pos = 0;
            while (pos + 8 <= bufferPos) {
                long k1 = getLong(buffer, pos);
                k1 *= PRIME64_2;
                k1 = Long.rotateLeft(k1, 31);
                k1 *= PRIME64_1;
                h64 ^= k1;
                h64 = Long.rotateLeft(h64, 27) * PRIME64_1 + PRIME64_4;
                pos += 8;
            }

            if (pos + 4 <= bufferPos) {
                h64 ^= (getInt(buffer, pos) & 0xFFFFFFFFL) * PRIME64_1;
                h64 = Long.rotateLeft(h64, 23) * PRIME64_2 + PRIME64_3;
                pos += 4;
            }

            while (pos < bufferPos) {
                h64 ^= (buffer[pos] & 0xFFL) * PRIME64_5;
                h64 = Long.rotateLeft(h64, 11) * PRIME64_1;
                pos++;
            }

            return avalanche(h64);
        }

        private void processBuffer() {
            v1 = round(v1, getLong(buffer, 0));
            v2 = round(v2, getLong(buffer, 8));
            v3 = round(v3, getLong(buffer, 16));
            v4 = round(v4, getLong(buffer, 24));
            bufferPos = 0;
        }
    }
}
