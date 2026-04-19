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
     * the worst case ({@code 4 * cs.length()} bytes). Shared between {@code appendString}
     * so {@code appendString} doesn't duplicate the codec.
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
     * may live in a different symbol-table space) don't reuse stale mappings.
     * Called once per query start by {@link QwpResultBatchBuffer#resetForNewQuery}.
     */
    void resetForNewQuery() {
        connKeyToConnId.clear();
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
