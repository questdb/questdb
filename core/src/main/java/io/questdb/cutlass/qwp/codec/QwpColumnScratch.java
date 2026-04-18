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

import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;

import java.nio.charset.StandardCharsets;

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
    QwpEgressColumnDef def;

    int nonNullCount;
    int nullCount;
    int rowCount;

    int nullBitmapCapacity;
    long nullBitmapAddr;

    int valuesCapacity;          // bytes
    long valuesAddr;
    int valuesPos;               // bytes written

    int stringHeapCapacity;
    long stringHeapAddr;
    int stringHeapPos;
    int stringOffsetsCapacity;   // bytes
    long stringOffsetsAddr;      // (nonNullCount + 1) × i32

    int symbolIdsCapacity;       // bytes
    long symbolIdsAddr;
    final ObjList<String> symbolDictOrder = new ObjList<>();
    final CharSequenceIntHashMap symbolDict = new CharSequenceIntHashMap();

    int arrayHeapCapacity;
    long arrayHeapAddr;
    int arrayHeapPos;

    final Decimal128 decimal128Sink = new Decimal128();
    final Decimal256 decimal256Sink = new Decimal256();

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

    /**
     * Writes the per-row bytes (header + flat values) of a serialised array.
     */
    void appendArrayBytes(long srcAddr, int len) {
        ensureArrayHeapCapacity(arrayHeapPos + len);
        Unsafe.getUnsafe().copyMemory(srcAddr, arrayHeapAddr + arrayHeapPos, len);
        arrayHeapPos += len;
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

    void appendFloat(float v) {
        ensureValuesCapacity(valuesPos + 4);
        Unsafe.getUnsafe().putFloat(valuesAddr + valuesPos, v);
        valuesPos += 4;
        markNonNullAndAdvanceRow();
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

    void appendInt(int v) {
        ensureValuesCapacity(valuesPos + 4);
        Unsafe.getUnsafe().putInt(valuesAddr + valuesPos, v);
        valuesPos += 4;
        markNonNullAndAdvanceRow();
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
        int pos = stringHeapPos;
        for (int i = 0; i < charLen; i++) {
            char c = cs.charAt(i);
            if (c < 0x80) {
                Unsafe.getUnsafe().putByte(stringHeapAddr + pos++, (byte) c);
            } else if (c < 0x800) {
                Unsafe.getUnsafe().putByte(stringHeapAddr + pos++, (byte) (0xC0 | (c >> 6)));
                Unsafe.getUnsafe().putByte(stringHeapAddr + pos++, (byte) (0x80 | (c & 0x3F)));
            } else if (Character.isHighSurrogate(c) && i + 1 < charLen
                    && Character.isLowSurrogate(cs.charAt(i + 1))) {
                int cp = Character.toCodePoint(c, cs.charAt(i + 1));
                i++;
                Unsafe.getUnsafe().putByte(stringHeapAddr + pos++, (byte) (0xF0 | (cp >> 18)));
                Unsafe.getUnsafe().putByte(stringHeapAddr + pos++, (byte) (0x80 | ((cp >> 12) & 0x3F)));
                Unsafe.getUnsafe().putByte(stringHeapAddr + pos++, (byte) (0x80 | ((cp >> 6) & 0x3F)));
                Unsafe.getUnsafe().putByte(stringHeapAddr + pos++, (byte) (0x80 | (cp & 0x3F)));
            } else {
                Unsafe.getUnsafe().putByte(stringHeapAddr + pos++, (byte) (0xE0 | (c >> 12)));
                Unsafe.getUnsafe().putByte(stringHeapAddr + pos++, (byte) (0x80 | ((c >> 6) & 0x3F)));
                Unsafe.getUnsafe().putByte(stringHeapAddr + pos++, (byte) (0x80 | (c & 0x3F)));
            }
        }
        stringHeapPos = pos;
        recordStringOffset();
        markNonNullAndAdvanceRow();
    }

    /**
     * SYMBOL: dedup against the per-batch dict; store dict id (i32) in {@link #symbolIdsAddr}.
     * Dict keys remain on the heap (CharSequenceIntHashMap); per unique value we allocate a
     * String once per batch.
     */
    void appendSymbol(CharSequence cs) {
        int id = symbolDict.get(cs);
        if (id == -1) {
            String s = cs.toString();
            id = symbolDictOrder.size();
            symbolDict.put(s, id);
            symbolDictOrder.add(s);
        }
        ensureSymbolIdsCapacity(symbolIdsCapacity == 0 ? INITIAL_BYTES : 4 * (nonNullCount + 1));
        Unsafe.getUnsafe().putInt(symbolIdsAddr + 4L * nonNullCount, id);
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
     */
    void appendVarchar(Utf8Sequence us) {
        int n = us.size();
        ensureStringHeapCapacity(stringHeapPos + n);
        long p = stringHeapAddr + stringHeapPos;
        for (int i = 0; i < n; i++) {
            Unsafe.getUnsafe().putByte(p + i, us.byteAt(i));
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
        this.symbolDict.clear();
        this.symbolDictOrder.clear();
        // Zero the null bitmap so appendNull() can OR in bits without per-byte init.
        // This costs nullBitmapCapacity bytes per batch (e.g. 512 B for 4096 rows). Cheap.
        if (nullBitmapAddr != 0 && nullBitmapCapacity > 0) {
            Unsafe.getUnsafe().setMemory(nullBitmapAddr, nullBitmapCapacity, (byte) 0);
        }
    }

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

    void ensureArrayHeapCapacity(int required) {
        if (arrayHeapCapacity >= required) return;
        int newCap = Math.max(arrayHeapCapacity * 2, Math.max(INITIAL_BYTES, required));
        arrayHeapAddr = Unsafe.realloc(arrayHeapAddr, arrayHeapCapacity, newCap, MemoryTag.NATIVE_HTTP_CONN);
        arrayHeapCapacity = newCap;
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

    void markNonNullAndAdvanceRow() {
        nonNullCount++;
        rowCount++;
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
}
