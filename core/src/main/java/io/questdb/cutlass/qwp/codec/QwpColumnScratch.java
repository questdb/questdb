/*+*****************************************************************************
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

package io.questdb.cutlass.qwp.codec;

import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Utf8Sequence;

/**
 * Per-column native scratch used by {@link QwpResultBatchBuffer} during one
 * {@code RESULT_BATCH}. Replaces the per-cell heap-boxed {@code Object[]}
 * accumulator with type-specialised native buffers; after warmup, append
 * does no JVM-heap allocation.
 * <p>
 * Storage layout depends on the column wire type and is selected lazily:
 * <ul>
 *   <li>Fixed-width types (BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, CHAR, DATE,
 *       TIMESTAMP, TIMESTAMP_NANOS, UUID, LONG256, DECIMAL64/128/256, GEOHASH):
 *       packed dense values in valuesAddr, byte size determined per type.</li>
 *   <li>BOOLEAN: bit-packed into valuesAddr, 8 values per byte (LSB-first).</li>
 *   <li>STRING/VARCHAR: UTF-8 bytes appended to stringHeapAddr; per-value end offset
 *       stored at stringOffsetsAddr + 4 * (nonNullIdx + 1) (offset 0 is implicit zero).</li>
 *   <li>SYMBOL: dict (heap) + per-non-null-row dict id at symbolIdsAddr + 4 * idx.</li>
 *   <li>ARRAY: each row's wire-format bytes appended to arrayHeapAddr.</li>
 * </ul>
 * The null bitmap is stored at {@link #nullBitmapAddr} and is grown the first time any null
 * is seen in the batch.
 */
final class QwpColumnScratch implements QuietCloseable {

    private static final int INITIAL_BYTES = 4096;
    // Growth factor to shrink to, vs. peak observed in the last query. Realloc
    // target is max(INITIAL_BYTES, SHRINK_TARGET_FACTOR * peak). Combined with
    // SHRINK_TRIGGER_FACTOR, this produces a stable hysteresis band: peaks
    // trigger growth geometrically, idle queries trim back to ~2x the last
    // peak, so a workload that oscillates within 2x does not reallocate.
    private static final int SHRINK_TARGET_FACTOR = 2;
    // Shrink threshold: a buffer whose capacity exceeds SHRINK_TRIGGER_FACTOR *
    // peak-seen-in-the-query is eligible for a trim at query boundary.
    private static final int SHRINK_TRIGGER_FACTOR = 4;
    final Decimal128 decimal128Sink = new Decimal128();
    final Decimal256 decimal256Sink = new Decimal256();
    // Dedup by native symbol-table key -> connection-scoped dict id. Persists across
    // batches on this connection for the lifetime of a single query. Reset via
    // {@link #resetConnSymbolMap} when a new query begins so that native keys from
    // a different cursor/table don't reuse stale mappings.
    final IntIntHashMap connKeyToConnId = new IntIntHashMap();
    long arrayHeapAddr;
    int arrayHeapCapacity;
    int arrayHeapPos;
    QwpEgressColumnDef def;
    int nonNullCount;
    long nullBitmapAddr;
    int nullBitmapCapacity;
    int nullCount;
    // Peak bytes observed for each buffer within the current query. Updated in
    // {@link #beginBatch} (capturing the previous batch's footprint before
    // positions reset) and again in {@link #resetForNewQuery} (capturing the
    // final batch). Used to decide whether a buffer has outgrown its actual
    // needs and should shrink at query boundary -- prevents a one-off wide
    // batch from permanently retaining megabytes of native memory.
    int peakArrayHeapBytes;
    int peakNullBitmapBytes;
    int peakStringHeapBytes;
    int peakStringOffsetsBytes;
    int peakSymbolIdsBytes;
    int peakValuesBytes;
    int rowCount;
    long stringHeapAddr;
    int stringHeapCapacity;
    int stringHeapPos;
    long stringOffsetsAddr;      // (nonNullCount + 1) x i32
    int stringOffsetsCapacity;   // bytes
    // SYMBOL: per-non-null-row connection dict id. With the per-batch dict gone
    // these are already the ids the wire will carry, so emit is a straight varint
    // pass over this array (no localToConn translation).
    long symbolIdsAddr;
    int symbolIdsCapacity;       // bytes
    long valuesAddr;
    int valuesCapacity;          // bytes
    int valuesPos;               // bytes written

    @Override
    public void close() {
        if (valuesAddr != 0) {
            Unsafe.free(valuesAddr, valuesCapacity, MemoryTag.NATIVE_HTTP_CONN);
            valuesAddr = 0;
            valuesCapacity = 0;
        }
        if (nullBitmapAddr != 0) {
            Unsafe.free(nullBitmapAddr, nullBitmapCapacity, MemoryTag.NATIVE_HTTP_CONN);
            nullBitmapAddr = 0;
            nullBitmapCapacity = 0;
        }
        if (stringHeapAddr != 0) {
            Unsafe.free(stringHeapAddr, stringHeapCapacity, MemoryTag.NATIVE_HTTP_CONN);
            stringHeapAddr = 0;
            stringHeapCapacity = 0;
        }
        if (stringOffsetsAddr != 0) {
            Unsafe.free(stringOffsetsAddr, stringOffsetsCapacity, MemoryTag.NATIVE_HTTP_CONN);
            stringOffsetsAddr = 0;
            stringOffsetsCapacity = 0;
        }
        if (symbolIdsAddr != 0) {
            Unsafe.free(symbolIdsAddr, symbolIdsCapacity, MemoryTag.NATIVE_HTTP_CONN);
            symbolIdsAddr = 0;
            symbolIdsCapacity = 0;
        }
        if (arrayHeapAddr != 0) {
            Unsafe.free(arrayHeapAddr, arrayHeapCapacity, MemoryTag.NATIVE_HTTP_CONN);
            arrayHeapAddr = 0;
            arrayHeapCapacity = 0;
        }
    }

    /**
     * UTF-8 encode {@code cs} into {@code heapAddr} starting at {@code pos}; returns
     * the position one past the last written byte. Caller must pre-size the heap for
     * the worst case ({@code 4 * cs.length()} bytes).
     */
    private static int encodeUtf8(CharSequence cs, long heapAddr, int pos) {
        final int charLen = cs.length();
        for (int i = 0; i < charLen; i++) {
            char c = cs.charAt(i);
            if (c < 0x80) {
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) c);
            } else if (c < 0x800) {
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) (0xC0 | (c >> 6)));
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) (0x80 | (c & 0x3F)));
            } else if (Character.isHighSurrogate(c) && i + 1 < charLen
                    && Character.isLowSurrogate(cs.charAt(i + 1))) {
                int cp = Character.toCodePoint(c, cs.charAt(i + 1));
                i++;
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) (0xF0 | (cp >> 18)));
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) (0x80 | ((cp >> 12) & 0x3F)));
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) (0x80 | ((cp >> 6) & 0x3F)));
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) (0x80 | (cp & 0x3F)));
            } else {
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) (0xE0 | (c >> 12)));
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) (0x80 | ((c >> 6) & 0x3F)));
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) (0x80 | (c & 0x3F)));
            }
        }
        return pos;
    }

    private void ensureNullBitmapCapacity(int rowIdx) {
        int needed = (rowIdx >>> 3) + 1;
        if (nullBitmapCapacity >= needed) return;
        int oldCap = nullBitmapCapacity;
        int newCap = Math.max(oldCap * 2, Math.max(INITIAL_BYTES, needed));
        nullBitmapAddr = Unsafe.realloc(nullBitmapAddr, oldCap, newCap, MemoryTag.NATIVE_HTTP_CONN);
        nullBitmapCapacity = newCap;
        // Zero the newly added bytes so appendNull() can OR in bits without per-byte init.
        Unsafe.getUnsafe().setMemory(nullBitmapAddr + oldCap, newCap - oldCap, (byte) 0);
    }

    private void ensureStringHeapCapacity(int required) {
        if (stringHeapCapacity >= required) return;
        int newCap = Math.max(stringHeapCapacity * 2, Math.max(INITIAL_BYTES, required));
        stringHeapAddr = Unsafe.realloc(stringHeapAddr, stringHeapCapacity, newCap, MemoryTag.NATIVE_HTTP_CONN);
        stringHeapCapacity = newCap;
    }

    private void ensureSymbolIdsCapacity(int required) {
        if (symbolIdsCapacity >= required) return;
        int newCap = Math.max(symbolIdsCapacity * 2, Math.max(INITIAL_BYTES, required));
        symbolIdsAddr = Unsafe.realloc(symbolIdsAddr, symbolIdsCapacity, newCap, MemoryTag.NATIVE_HTTP_CONN);
        symbolIdsCapacity = newCap;
    }

    private void ensureValuesCapacity(int required) {
        if (valuesCapacity >= required) return;
        int newCap = Math.max(valuesCapacity * 2, Math.max(INITIAL_BYTES, required));
        valuesAddr = Unsafe.realloc(valuesAddr, valuesCapacity, newCap, MemoryTag.NATIVE_HTTP_CONN);
        valuesCapacity = newCap;
    }

    /**
     * Sets bit {@code rowIdx} in the null bitmap. Caller must have already
     * called {@link #ensureNullBitmapCapacity} to cover at least rowIdx.
     */
    private void setNullBit(int rowIdx) {
        long byteAddr = nullBitmapAddr + (rowIdx >>> 3);
        byte cur = Unsafe.getUnsafe().getByte(byteAddr);
        Unsafe.getUnsafe().putByte(byteAddr, (byte) (cur | (1 << (rowIdx & 7))));
    }

    /**
     * STRING/VARCHAR: record the end-offset of the value just written. Offsets are stored
     * at {@code 4 * (nonNullCount + 1)} in {@link #stringOffsetsAddr}; offset[0] is the
     * implicit zero (written at emit time to keep parsing simple).
     */
    private void recordStringOffset() {
        int slotIdx = nonNullCount + 1;             // we'll occupy this slot
        int needed = 4 * (slotIdx + 1);              // +1 for the implicit offset[0]
        if (stringOffsetsCapacity < needed) {
            int newCap = Math.max(stringOffsetsCapacity * 2, Math.max(INITIAL_BYTES, needed));
            stringOffsetsAddr = Unsafe.realloc(stringOffsetsAddr, stringOffsetsCapacity, newCap, MemoryTag.NATIVE_HTTP_CONN);
            stringOffsetsCapacity = newCap;
        }
        Unsafe.getUnsafe().putInt(stringOffsetsAddr + 4L * slotIdx, stringHeapPos);
    }

    /**
     * BINARY: copy opaque bytes from {@code BinarySequence} into the string heap via the
     * sequence's bulk {@link BinarySequence#copyTo(long, long, long)} method, which native
     * implementations override with a single memcpy/JNI bulk path. Caller has confirmed
     * {@code b.length()} fits in {@code int} (per-row BINARY is bounded by the wire u32
     * offset; values exceeding {@link Integer#MAX_VALUE} are rejected by the caller).
     */
    void appendBinary(BinarySequence b) {
        long len = b.length();
        // Treat as int below; the caller (QwpResultBatchBuffer) validates the bound.
        int n = (int) len;
        ensureStringHeapCapacity(stringHeapPos + n);
        b.copyTo(stringHeapAddr + stringHeapPos, 0, len);
        stringHeapPos += n;
        recordStringOffset();
        markNonNullAndAdvanceRow();
    }

    /**
     * BOOLEAN: bit-pack into {@link #valuesAddr}. Bit index = {@link #nonNullCount}
     * (so {@link #valuesPos} is unused for this wire type).
     */
    void appendBool(boolean v) {
        int bitIdx = nonNullCount;
        int byteIdx = bitIdx >>> 3;
        ensureValuesCapacity(byteIdx + 1);
        long byteAddr = valuesAddr + byteIdx;
        if ((bitIdx & 7) == 0) {
            // First bit in a new byte: zero it before OR-ing.
            Unsafe.getUnsafe().putByte(byteAddr, (byte) 0);
        }
        if (v) {
            byte cur = Unsafe.getUnsafe().getByte(byteAddr);
            Unsafe.getUnsafe().putByte(byteAddr, (byte) (cur | (1 << (bitIdx & 7))));
        }
        markNonNullAndAdvanceRow();
    }

    void appendByte(byte v) {
        ensureValuesCapacity(valuesPos + 1);
        Unsafe.getUnsafe().putByte(valuesAddr + valuesPos, v);
        valuesPos += 1;
        markNonNullAndAdvanceRow();
    }

    /**
     * BOOLEAN column bulk append: reads {@code n} raw bytes from
     * {@code srcAddr} (QuestDB native layout: 1 byte per row, 0 or 1) and
     * bit-packs them into {@code valuesAddr} starting at the current
     * {@code nonNullCount}. BOOLEAN has no null representation so rowCount /
     * nonNullCount advance by {@code n}.
     */
    void appendColumnBoolean(long srcAddr, int n) {
        int startBit = nonNullCount;
        int bytesNeeded = (startBit + n + 7) >>> 3;
        ensureValuesCapacity(bytesNeeded);
        for (int i = 0; i < n; i++) {
            int bitIdx = startBit + i;
            long byteAddr = valuesAddr + (bitIdx >>> 3);
            if ((bitIdx & 7) == 0) {
                // First bit in a new byte: zero it before OR-ing. Matches the
                // single-row appendBool convention.
                Unsafe.getUnsafe().putByte(byteAddr, (byte) 0);
            }
            if (Unsafe.getUnsafe().getByte(srcAddr + i) != 0) {
                byte cur = Unsafe.getUnsafe().getByte(byteAddr);
                Unsafe.getUnsafe().putByte(byteAddr, (byte) (cur | (1 << (bitIdx & 7))));
            }
        }
        nonNullCount += n;
        rowCount += n;
    }

    /**
     * DOUBLE / FLOAT-as-double column bulk append: reads {@code n} 8-byte
     * values from {@code srcAddr} (QuestDB stores DOUBLE NULL as NaN). Values
     * that are NaN go into the null bitmap; non-null values are packed dense
     * into {@code valuesAddr}. Uses {@code v != v} for the NaN test so every
     * NaN bit-pattern is treated as null (spec 11.5).
     */
    void appendColumnDouble8(long srcAddr, int n) {
        int startRow = rowCount;
        ensureNullBitmapCapacity(startRow + n);
        ensureValuesCapacity(valuesPos + n * 8);
        long dst = valuesAddr + valuesPos;
        int nonNullWritten = 0;
        for (int i = 0; i < n; i++) {
            double v = Unsafe.getUnsafe().getDouble(srcAddr + i * 8L);
            if (Double.isNaN(v)) {
                setNullBit(startRow + i);
                nullCount++;
            } else {
                Unsafe.getUnsafe().putDouble(dst, v);
                dst += 8;
                nonNullWritten++;
            }
        }
        valuesPos += nonNullWritten * 8;
        nonNullCount += nonNullWritten;
        rowCount += n;
    }

    /**
     * No-null fixed-width column bulk append: BYTE / SHORT / CHAR columns
     * have no sentinel and never contribute to the null bitmap, so we copy
     * the whole block into {@code valuesAddr} in one {@code memcpy}.
     */
    void appendColumnFixedNoNull(long srcAddr, int n, int typeSize) {
        int bytes = n * typeSize;
        ensureValuesCapacity(valuesPos + bytes);
        Vect.memcpy(valuesAddr + valuesPos, srcAddr, bytes);
        valuesPos += bytes;
        nonNullCount += n;
        rowCount += n;
    }

    /**
     * FLOAT column bulk append: reads {@code n} 4-byte floats from
     * {@code srcAddr}. QuestDB stores FLOAT NULL as NaN. NaN values go into
     * the null bitmap; non-null values pack dense into {@code valuesAddr}.
     */
    void appendColumnFloat4(long srcAddr, int n) {
        int startRow = rowCount;
        ensureNullBitmapCapacity(startRow + n);
        ensureValuesCapacity(valuesPos + n * 4);
        long dst = valuesAddr + valuesPos;
        int nonNullWritten = 0;
        for (int i = 0; i < n; i++) {
            float v = Unsafe.getUnsafe().getFloat(srcAddr + i * 4L);
            if (Float.isNaN(v)) {
                setNullBit(startRow + i);
                nullCount++;
            } else {
                Unsafe.getUnsafe().putFloat(dst, v);
                dst += 4;
                nonNullWritten++;
            }
        }
        valuesPos += nonNullWritten * 4;
        nonNullCount += nonNullWritten;
        rowCount += n;
    }

    /**
     * INT / IPv4 column bulk append: reads {@code n} 4-byte values from
     * {@code srcAddr}. Values equal to {@code sentinel} go into the null
     * bitmap (INT: {@link Numbers#INT_NULL}; IPv4: {@link Numbers#IPv4_NULL},
     * i.e. 0).
     */
    void appendColumnInt4WithSentinel(long srcAddr, int n, int sentinel) {
        int startRow = rowCount;
        ensureNullBitmapCapacity(startRow + n);
        ensureValuesCapacity(valuesPos + n * 4);
        long dst = valuesAddr + valuesPos;
        int nonNullWritten = 0;
        for (int i = 0; i < n; i++) {
            int v = Unsafe.getUnsafe().getInt(srcAddr + i * 4L);
            if (v == sentinel) {
                setNullBit(startRow + i);
                nullCount++;
            } else {
                Unsafe.getUnsafe().putInt(dst, v);
                dst += 4;
                nonNullWritten++;
            }
        }
        valuesPos += nonNullWritten * 4;
        nonNullCount += nonNullWritten;
        rowCount += n;
    }

    /**
     * LONG-family column bulk append: LONG, DATE, TIMESTAMP, TIMESTAMP_NANOS,
     * DECIMAL64. All use {@link Numbers#LONG_NULL} (= {@link Long#MIN_VALUE})
     * as the null sentinel. Dense non-null values land in {@code valuesAddr};
     * null positions are marked in the null bitmap.
     */
    void appendColumnLong8WithSentinel(long srcAddr, int n) {
        int startRow = rowCount;
        ensureNullBitmapCapacity(startRow + n);
        ensureValuesCapacity(valuesPos + n * 8);
        long dst = valuesAddr + valuesPos;
        int nonNullWritten = 0;
        for (int i = 0; i < n; i++) {
            long v = Unsafe.getUnsafe().getLong(srcAddr + i * 8L);
            if (v == Numbers.LONG_NULL) {
                setNullBit(startRow + i);
                nullCount++;
            } else {
                Unsafe.getUnsafe().putLong(dst, v);
                dst += 8;
                nonNullWritten++;
            }
        }
        valuesPos += nonNullWritten * 8;
        nonNullCount += nonNullWritten;
        rowCount += n;
    }

    /**
     * SYMBOL column bulk append: reads {@code n} 4-byte native symbol keys
     * from {@code srcAddr}. Each non-null key is translated into a
     * connection-scoped dict id via the per-column {@link #connKeyToConnId}
     * map; on first sight it is inserted into {@code dict} and the new id
     * cached. Null keys ({@link SymbolTable#VALUE_IS_NULL}) go into the null
     * bitmap.
     */
    void appendColumnSymbolKeys(long srcAddr, int n, SymbolTable st, QwpEgressConnSymbolDict dict) {
        int startRow = rowCount;
        ensureNullBitmapCapacity(startRow + n);
        // Pre-grow symbol-ids buffer for the worst case (every input row is
        // non-null) so the per-row hot path doesn't pay the capacity check on
        // every iteration. Mirrors the pattern other appendColumn* bulk
        // methods already use for valuesAddr.
        ensureSymbolIdsCapacity(4 * (nonNullCount + n));
        IntIntHashMap k2c = connKeyToConnId;
        int nonNullWritten = 0;
        for (int i = 0; i < n; i++) {
            int key = Unsafe.getUnsafe().getInt(srcAddr + i * 4L);
            if (key == SymbolTable.VALUE_IS_NULL) {
                setNullBit(startRow + i);
                nullCount++;
            } else {
                int mapIdx = k2c.keyIndex(key);
                int connId;
                if (mapIdx < 0) {
                    connId = k2c.valueAt(mapIdx);
                } else {
                    connId = dict.addEntry(st.valueOf(key));
                    k2c.putAt(mapIdx, key, connId);
                }
                int slot = nonNullCount + nonNullWritten;
                Unsafe.getUnsafe().putInt(symbolIdsAddr + 4L * slot, connId);
                nonNullWritten++;
            }
        }
        nonNullCount += nonNullWritten;
        rowCount += n;
    }

    void appendChar(char v) {
        ensureValuesCapacity(valuesPos + 2);
        Unsafe.getUnsafe().putShort(valuesAddr + valuesPos, (short) v);
        valuesPos += 2;
        markNonNullAndAdvanceRow();
    }

    void appendDecimal128(long lo, long hi) {
        ensureValuesCapacity(valuesPos + 16);
        Unsafe.getUnsafe().putLong(valuesAddr + valuesPos, lo);
        Unsafe.getUnsafe().putLong(valuesAddr + valuesPos + 8, hi);
        valuesPos += 16;
        markNonNullAndAdvanceRow();
    }

    void appendDecimal256(long ll, long lh, long hl, long hh) {
        ensureValuesCapacity(valuesPos + 32);
        Unsafe.getUnsafe().putLong(valuesAddr + valuesPos, ll);
        Unsafe.getUnsafe().putLong(valuesAddr + valuesPos + 8, lh);
        Unsafe.getUnsafe().putLong(valuesAddr + valuesPos + 16, hl);
        Unsafe.getUnsafe().putLong(valuesAddr + valuesPos + 24, hh);
        valuesPos += 32;
        markNonNullAndAdvanceRow();
    }

    void appendDouble(double v) {
        ensureValuesCapacity(valuesPos + 8);
        Unsafe.getUnsafe().putDouble(valuesAddr + valuesPos, v);
        valuesPos += 8;
        markNonNullAndAdvanceRow();
    }

    void appendDoubleOrNull(double v) {
        if (Double.isNaN(v)) {
            appendNull();
        } else {
            appendDouble(v);
        }
    }

    void appendFloat(float v) {
        ensureValuesCapacity(valuesPos + 4);
        Unsafe.getUnsafe().putFloat(valuesAddr + valuesPos, v);
        valuesPos += 4;
        markNonNullAndAdvanceRow();
    }

    void appendFloatOrNull(float v) {
        if (Float.isNaN(v)) {
            appendNull();
        } else {
            appendFloat(v);
        }
    }

    /**
     * GEOHASH: write {@code bytesPerValue} bytes from {@code bits}, LSB-first.
     */
    void appendGeohash(long bits, int bytesPerValue) {
        ensureValuesCapacity(valuesPos + bytesPerValue);
        for (int b = 0; b < bytesPerValue; b++) {
            Unsafe.getUnsafe().putByte(valuesAddr + valuesPos + b, (byte) (bits >>> (b * 8)));
        }
        valuesPos += bytesPerValue;
        markNonNullAndAdvanceRow();
    }

    void appendIPv4OrNull(int v) {
        // QuestDB stores IPv4 NULL as the bit pattern 0 (Numbers.IPv4_NULL). The wire
        // format cannot represent the literal address 0.0.0.0 as non-null - QuestDB-level
        // limitation inherited by the wire format.
        if (v == Numbers.IPv4_NULL) {
            appendNull();
        } else {
            appendInt(v);
        }
    }

    void appendInt(int v) {
        ensureValuesCapacity(valuesPos + 4);
        Unsafe.getUnsafe().putInt(valuesAddr + valuesPos, v);
        valuesPos += 4;
        markNonNullAndAdvanceRow();
    }

    void appendIntOrNull(int v) {
        if (v == Numbers.INT_NULL) {
            appendNull();
        } else {
            appendInt(v);
        }
    }

    void appendLong(long v) {
        ensureValuesCapacity(valuesPos + 8);
        Unsafe.getUnsafe().putLong(valuesAddr + valuesPos, v);
        valuesPos += 8;
        markNonNullAndAdvanceRow();
    }

    void appendLong256(long l0, long l1, long l2, long l3) {
        ensureValuesCapacity(valuesPos + 32);
        Unsafe.getUnsafe().putLong(valuesAddr + valuesPos, l0);
        Unsafe.getUnsafe().putLong(valuesAddr + valuesPos + 8, l1);
        Unsafe.getUnsafe().putLong(valuesAddr + valuesPos + 16, l2);
        Unsafe.getUnsafe().putLong(valuesAddr + valuesPos + 24, l3);
        valuesPos += 32;
        markNonNullAndAdvanceRow();
    }

    void appendLongOrNull(long v) {
        if (v == Numbers.LONG_NULL) {
            appendNull();
        } else {
            appendLong(v);
        }
    }

    void appendNull() {
        ensureNullBitmapCapacity(rowCount);
        // The bitmap is zeroed at beginBatch and at every grow, so we can OR in unconditionally.
        long byteAddr = nullBitmapAddr + (rowCount >>> 3);
        byte cur = Unsafe.getUnsafe().getByte(byteAddr);
        Unsafe.getUnsafe().putByte(byteAddr, (byte) (cur | (1 << (rowCount & 7))));
        nullCount++;
        rowCount++;
    }

    /**
     * Bulk equivalent of {@code n} consecutive {@link #appendNull()} calls.
     * Sets bits {@code [rowCount, rowCount + n)} in the null bitmap using one
     * partial-byte OR at each end and a single {@code setMemory(0xFF)} for the
     * full bytes in between, replacing the per-row read-modify-write loop. The
     * bitmap is zeroed at {@link #beginBatch} and at every grow, so OR is safe
     * against any state in the partial bytes.
     */
    void appendNullColumn(int n) {
        if (n <= 0) {
            return;
        }
        int start = rowCount;
        int lastBit = start + n - 1;
        ensureNullBitmapCapacity(lastBit);
        int firstByte = start >>> 3;
        int lastByte = lastBit >>> 3;
        int firstBitInFirstByte = start & 7;
        int lastBitInLastByte = lastBit & 7;
        if (firstByte == lastByte) {
            // All n bits (n <= 8) sit in a single byte at offset firstBitInFirstByte.
            int mask = ((1 << n) - 1) << firstBitInFirstByte;
            long addr = nullBitmapAddr + firstByte;
            byte cur = Unsafe.getUnsafe().getByte(addr);
            Unsafe.getUnsafe().putByte(addr, (byte) (cur | mask));
        } else {
            // First (possibly partial) byte: bits [firstBitInFirstByte, 8).
            int firstMask = (0xFF << firstBitInFirstByte) & 0xFF;
            long firstAddr = nullBitmapAddr + firstByte;
            byte firstCur = Unsafe.getUnsafe().getByte(firstAddr);
            Unsafe.getUnsafe().putByte(firstAddr, (byte) (firstCur | firstMask));
            // Middle bytes [firstByte + 1, lastByte) are entirely owned by this
            // column-top (rows haven't been written yet) and were left zero by
            // the most recent grow / beginBatch, so memset is correct.
            int middleStart = firstByte + 1;
            int middleLen = lastByte - middleStart;
            if (middleLen > 0) {
                Unsafe.getUnsafe().setMemory(nullBitmapAddr + middleStart, middleLen, (byte) 0xFF);
            }
            // Last (possibly partial) byte: bits [0, lastBitInLastByte + 1).
            int lastMask = (1 << (lastBitInLastByte + 1)) - 1;
            long lastAddr = nullBitmapAddr + lastByte;
            byte lastCur = Unsafe.getUnsafe().getByte(lastAddr);
            Unsafe.getUnsafe().putByte(lastAddr, (byte) (lastCur | lastMask));
        }
        nullCount += n;
        rowCount += n;
    }

    void appendShort(short v) {
        ensureValuesCapacity(valuesPos + 2);
        Unsafe.getUnsafe().putShort(valuesAddr + valuesPos, v);
        valuesPos += 2;
        markNonNullAndAdvanceRow();
    }

    /**
     * STRING: UTF-8 encode the {@link CharSequence} into the string heap, record the
     * end offset. Pre-sizes for the worst case (4 bytes/char) so the inner loop can't
     * trigger a realloc mid-write.
     */
    void appendString(CharSequence cs) {
        int charLen = cs.length();
        // Worst-case: 4 bytes per code point pair (one surrogate pair = 2 chars = 4 bytes).
        // For BMP-only it's 3 bytes/char; allocate the conservative bound once.
        ensureStringHeapCapacity(stringHeapPos + 4 * charLen);
        stringHeapPos = encodeUtf8(cs, stringHeapAddr, stringHeapPos);
        recordStringOffset();
        markNonNullAndAdvanceRow();
    }

    /**
     * SYMBOL: writes a connection-scoped dict id for this non-null row. Caller (the
     * batch buffer) has already resolved the id via the per-column native-key-to-
     * connId map and the shared connection dict; this method just stores it.
     */
    void appendSymbolConnId(int connId) {
        ensureSymbolIdsCapacity(symbolIdsCapacity == 0 ? INITIAL_BYTES : 4 * (nonNullCount + 1));
        Unsafe.getUnsafe().putInt(symbolIdsAddr + 4L * nonNullCount, connId);
        markNonNullAndAdvanceRow();
    }

    void appendUuid(long lo, long hi) {
        ensureValuesCapacity(valuesPos + 16);
        Unsafe.getUnsafe().putLong(valuesAddr + valuesPos, lo);
        Unsafe.getUnsafe().putLong(valuesAddr + valuesPos + 8, hi);
        valuesPos += 16;
        markNonNullAndAdvanceRow();
    }

    /**
     * VARCHAR: copy raw UTF-8 bytes from {@code Utf8Sequence} into the string heap.
     * For direct-backed sequences we issue a single {@code Unsafe.copyMemory} over the
     * bytes; on-heap sequences fall through to {@code Utf8Sequence.writeTo}, which
     * already does 8-byte chunked writes via {@code longAt} rather than per-byte loads.
     */
    void appendVarchar(Utf8Sequence us) {
        int n = us.size();
        ensureStringHeapCapacity(stringHeapPos + n);
        long dst = stringHeapAddr + stringHeapPos;
        long src = us.ptr();
        if (src >= 0) {
            Vect.memcpy(dst, src, n);
        } else {
            us.writeTo(dst, 0, n);
        }
        stringHeapPos += n;
        recordStringOffset();
        markNonNullAndAdvanceRow();
    }

    void beginBatch(QwpEgressColumnDef def) {
        this.def = def;
        // Capture this column's footprint from the batch that just finished
        // before the positions reset, so peak* tracks the largest batch seen
        // since the last query boundary. Does nothing on the first batch of
        // a query (all positions are already 0).
        updatePeakUsage();
        this.rowCount = 0;
        this.nonNullCount = 0;
        this.nullCount = 0;
        this.valuesPos = 0;
        this.stringHeapPos = 0;
        this.arrayHeapPos = 0;
        // Note: connKeyToConnId persists across batches within a single query --
        // resetting it here would force every batch to re-ship every symbol value
        // through the delta section. Only {@link #resetForNewQuery} clears it.
        // Zero the null bitmap so appendNull() can OR in bits without per-byte init.
        // This costs nullBitmapCapacity bytes per batch (e.g. 512 B for 4096 rows). Cheap.
        if (nullBitmapAddr != 0 && nullBitmapCapacity > 0) {
            Unsafe.getUnsafe().setMemory(nullBitmapAddr, nullBitmapCapacity, (byte) 0);
        }
    }

    /**
     * Clears the native-key -> connId map so native keys from a new query (which
     * may live in a different symbol-table space) don't reuse stale mappings,
     * and trims any native buffer whose capacity has outgrown this column's
     * actual use by more than {@link #SHRINK_TRIGGER_FACTOR}x. Called once per
     * query start by {@link QwpResultBatchBuffer#resetForNewQuery}.
     * <p>
     * Without the shrink pass, a single wide batch on a long-lived connection
     * would permanently retain its peak-sized scratch until the connection
     * drops -- a DoS surface for slow clients or bursty workloads.
     */
    void resetForNewQuery() {
        connKeyToConnId.clear();
        // Roll the final batch of the previous query into the peak counters
        // before we use them to decide whether to shrink.
        updatePeakUsage();
        // Trim each oversized buffer down to SHRINK_TARGET_FACTOR * peak. Manual
        // per-buffer sequence rather than a helper-plus-setter so we don't
        // allocate a capturing lambda on the shrink path (resetForNewQuery fires
        // on every query boundary).
        int target;
        if (valuesCapacity > 0 && (peakValuesBytes == 0
                || valuesCapacity >= (long) peakValuesBytes * SHRINK_TRIGGER_FACTOR)) {
            target = shrinkTarget(peakValuesBytes);
            if (target < valuesCapacity) {
                valuesAddr = Unsafe.realloc(valuesAddr, valuesCapacity, target, MemoryTag.NATIVE_HTTP_CONN);
                valuesCapacity = target;
            }
        }
        if (nullBitmapCapacity > 0 && (peakNullBitmapBytes == 0
                || nullBitmapCapacity >= (long) peakNullBitmapBytes * SHRINK_TRIGGER_FACTOR)) {
            target = shrinkTarget(peakNullBitmapBytes);
            if (target < nullBitmapCapacity) {
                nullBitmapAddr = Unsafe.realloc(nullBitmapAddr, nullBitmapCapacity, target, MemoryTag.NATIVE_HTTP_CONN);
                nullBitmapCapacity = target;
            }
        }
        if (stringHeapCapacity > 0 && (peakStringHeapBytes == 0
                || stringHeapCapacity >= (long) peakStringHeapBytes * SHRINK_TRIGGER_FACTOR)) {
            target = shrinkTarget(peakStringHeapBytes);
            if (target < stringHeapCapacity) {
                stringHeapAddr = Unsafe.realloc(stringHeapAddr, stringHeapCapacity, target, MemoryTag.NATIVE_HTTP_CONN);
                stringHeapCapacity = target;
            }
        }
        if (stringOffsetsCapacity > 0 && (peakStringOffsetsBytes == 0
                || stringOffsetsCapacity >= (long) peakStringOffsetsBytes * SHRINK_TRIGGER_FACTOR)) {
            target = shrinkTarget(peakStringOffsetsBytes);
            if (target < stringOffsetsCapacity) {
                stringOffsetsAddr = Unsafe.realloc(stringOffsetsAddr, stringOffsetsCapacity, target, MemoryTag.NATIVE_HTTP_CONN);
                stringOffsetsCapacity = target;
            }
        }
        if (symbolIdsCapacity > 0 && (peakSymbolIdsBytes == 0
                || symbolIdsCapacity >= (long) peakSymbolIdsBytes * SHRINK_TRIGGER_FACTOR)) {
            target = shrinkTarget(peakSymbolIdsBytes);
            if (target < symbolIdsCapacity) {
                symbolIdsAddr = Unsafe.realloc(symbolIdsAddr, symbolIdsCapacity, target, MemoryTag.NATIVE_HTTP_CONN);
                symbolIdsCapacity = target;
            }
        }
        if (arrayHeapCapacity > 0 && (peakArrayHeapBytes == 0
                || arrayHeapCapacity >= (long) peakArrayHeapBytes * SHRINK_TRIGGER_FACTOR)) {
            target = shrinkTarget(peakArrayHeapBytes);
            if (target < arrayHeapCapacity) {
                arrayHeapAddr = Unsafe.realloc(arrayHeapAddr, arrayHeapCapacity, target, MemoryTag.NATIVE_HTTP_CONN);
                arrayHeapCapacity = target;
            }
        }
        peakValuesBytes = 0;
        peakNullBitmapBytes = 0;
        peakStringHeapBytes = 0;
        peakStringOffsetsBytes = 0;
        peakSymbolIdsBytes = 0;
        peakArrayHeapBytes = 0;
    }

    /**
     * Computes the shrink target for a buffer given its per-query peak usage.
     * Never goes below {@link #INITIAL_BYTES}; rounds at
     * {@link #SHRINK_TARGET_FACTOR} x peak for non-zero peaks.
     */
    private static int shrinkTarget(int peak) {
        if (peak == 0) {
            return INITIAL_BYTES;
        }
        long target = (long) peak * SHRINK_TARGET_FACTOR;
        return (int) Math.max(INITIAL_BYTES, target);
    }

    /**
     * Rolls the current batch's footprint into the peak counters. Called from
     * {@link #beginBatch} (just before positions reset) and from
     * {@link #resetForNewQuery} (to catch the final batch of a query).
     */
    private void updatePeakUsage() {
        if (valuesPos > peakValuesBytes) peakValuesBytes = valuesPos;
        if (stringHeapPos > peakStringHeapBytes) peakStringHeapBytes = stringHeapPos;
        if (arrayHeapPos > peakArrayHeapBytes) peakArrayHeapBytes = arrayHeapPos;
        int offsetsBytes = nonNullCount > 0 ? 4 * (nonNullCount + 1) : 0;
        if (offsetsBytes > peakStringOffsetsBytes) peakStringOffsetsBytes = offsetsBytes;
        int symIdsBytes = 4 * nonNullCount;
        if (symIdsBytes > peakSymbolIdsBytes) peakSymbolIdsBytes = symIdsBytes;
        int bitmapBytes = (rowCount + 7) >>> 3;
        if (bitmapBytes > peakNullBitmapBytes) peakNullBitmapBytes = bitmapBytes;
    }

    void ensureArrayHeapCapacity(int required) {
        if (arrayHeapCapacity >= required) return;
        int newCap = Math.max(arrayHeapCapacity * 2, Math.max(INITIAL_BYTES, required));
        arrayHeapAddr = Unsafe.realloc(arrayHeapAddr, arrayHeapCapacity, newCap, MemoryTag.NATIVE_HTTP_CONN);
        arrayHeapCapacity = newCap;
    }

    void markNonNullAndAdvanceRow() {
        nonNullCount++;
        rowCount++;
    }
}
