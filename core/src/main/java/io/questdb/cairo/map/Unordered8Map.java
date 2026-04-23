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

package io.questdb.cairo.map;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Hash;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.bytes.Bytes;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.Numbers.MAX_SAFE_INT_POW_2;

/**
 * Unordered8Map is a general purpose off-heap hash table with 8 bytes keys (LONG, TIMESTAMP, DATE) used
 * to store intermediate data of group by, sample by queries. It provides {@link MapKey} and
 * {@link MapValue}, as well as {@link RecordCursor} interfaces for data access and modification.
 * The preferred way to create an Unordered8Map is {@link MapFactory}.
 * <p>
 * Map iteration provided by {@link RecordCursor} does not preserve the key insertion order, hence
 * the unordered map name.
 * <strong>Important!</strong>
 * Key and value structures must match the ones provided via lists of columns ({@link ColumnTypes})
 * to the map constructor. Later put* calls made on {@link MapKey} and {@link MapValue} must match
 * the declared column types to guarantee memory access safety.
 * <p>
 * Keys must be fixed-size and up to 8 bytes total. Only insertions and updates operations are
 * supported meaning that a key can't be removed from the map once it was inserted.
 * <p>
 * The hash table is organized into the following parts:
 * <ul>
 * <li>1. Off-heap memory for writing new keys</li>
 * <li>2. Off-heap memory for zero key and value</li>
 * <li>3. Off-heap memory for key-value pairs, i.e. the hash table with open addressing</li>
 * </ul>
 * The hash table uses linear probing.
 * <p>
 * Key-value pairs stored in the hash table may have the following layout:
 * <pre>
 * |   Key columns 0..K   | Optional padding | Value columns 0..V |
 * +----------------------+------------------+--------------------+
 * |    up to 8 bytes     |        -         |         -          |
 * +----------------------+------------------+--------------------+
 * </pre>
 */
public class Unordered8Map implements Map, Reopenable {
    static final long KEY_SIZE = Long.BYTES;
    private static final int MIN_KEY_CAPACITY = 16;

    private final Unordered8MapCursor cursor;
    private final long entrySize;
    private final Key key;
    private final double loadFactor;
    private final int maxResizes;
    private final int memoryTag;
    private final Unordered8MapRecord record;
    private final FlyweightPackedMapValue value;
    private final FlyweightPackedMapValue value2;
    private final FlyweightPackedMapValue value3;
    private final long valueSize;
    private long batchEmptyValueStart;
    private int free;
    private boolean hasZero;
    private int initialKeyCapacity;
    private int keyCapacity;
    private long mask;
    private long memLimit; // Hash table memory limit pointer.
    private long memStart; // Hash table memory start pointer.
    private int nResizes;
    private int size = 0;
    private long zeroMemStart; // Zero key-value pair memory start pointer.

    public Unordered8Map(
            int keyType,
            @Transient @Nullable ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes
    ) {
        this(keyType, valueTypes, keyCapacity, loadFactor, maxResizes, MemoryTag.NATIVE_UNORDERED_MAP);
    }

    Unordered8Map(
            int keyType,
            @Nullable @Transient ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes,
            int memoryTag
    ) {
        assert loadFactor > 0 && loadFactor < 1d;

        try {
            this.memoryTag = memoryTag;
            this.loadFactor = loadFactor;
            this.keyCapacity = (int) (keyCapacity / loadFactor);
            this.keyCapacity = this.initialKeyCapacity = Math.max(Numbers.ceilPow2(this.keyCapacity), MIN_KEY_CAPACITY);
            this.maxResizes = maxResizes;
            mask = this.keyCapacity - 1;
            free = (int) (this.keyCapacity * loadFactor);
            nResizes = 0;

            if (!isSupportedKeyType(keyType)) {
                throw CairoException.nonCritical().put("unexpected key type: ").put(keyType);
            }

            long valueOffset = 0;
            long[] valueOffsets = null;
            long valueSize = 0;
            if (valueTypes != null) {
                int valueColumnCount = valueTypes.getColumnCount();
                valueOffsets = new long[valueColumnCount];

                for (int i = 0; i < valueColumnCount; i++) {
                    valueOffsets[i] = valueOffset;
                    final int columnType = valueTypes.getColumnType(i);
                    final int size = ColumnType.sizeOf(columnType);
                    if (size <= 0) {
                        throw CairoException.nonCritical().put("value type is not supported: ").put(ColumnType.nameOf(columnType));
                    }
                    valueOffset += size;
                    valueSize += size;
                }
            }
            this.valueSize = valueSize;

            this.entrySize = Bytes.align8b(KEY_SIZE + valueSize);

            // Allocate one extra slot at the end for the zero key entry.
            final long sizeBytes = entrySize * (this.keyCapacity + 1);
            validateBatchAddressable(sizeBytes);
            memStart = Unsafe.malloc(sizeBytes, memoryTag);
            Vect.memset(memStart, sizeBytes, 0);
            memLimit = memStart + entrySize * this.keyCapacity;
            zeroMemStart = memLimit; // zero key lives right after the hash table

            value = new FlyweightPackedMapValue(valueSize, valueOffsets);
            value2 = new FlyweightPackedMapValue(valueSize, valueOffsets);
            value3 = new FlyweightPackedMapValue(valueSize, valueOffsets);

            record = new Unordered8MapRecord(valueSize, valueOffsets, value, valueTypes);
            cursor = new Unordered8MapCursor(record, this);
            key = new Key();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    public static boolean isSupportedKeyType(int columnType) {
        return columnType == ColumnType.LONG || columnType == ColumnType.TIMESTAMP || columnType == ColumnType.DATE;
    }

    @Override
    public void clear() {
        free = (int) (keyCapacity * loadFactor);
        size = 0;
        nResizes = 0;
        hasZero = false;
        Vect.memset(memStart, memLimit - memStart + entrySize, 0);
    }

    @Override
    public void close() {
        if (memStart != 0) {
            memLimit = memStart = Unsafe.free(memStart, memLimit - memStart + entrySize, memoryTag);
            zeroMemStart = 0;
            free = 0;
            size = 0;
            hasZero = false;
        }
        if (batchEmptyValueStart != 0) {
            batchEmptyValueStart = Unsafe.free(batchEmptyValueStart, valueSize, memoryTag);
        }
    }

    @Override
    public MapRecordCursor getCursor() {
        if (hasZero) {
            return cursor.init(memStart, memLimit, zeroMemStart, size + 1);
        }
        return cursor.init(memStart, memLimit, 0, size);
    }

    @Override
    public int getKeyCapacity() {
        return keyCapacity;
    }

    @Override
    public MapRecord getRecord() {
        return record;
    }

    @Override
    public void initCursor(MapRecordCursor cursor) {
        Unordered8MapCursor c = (Unordered8MapCursor) cursor;
        if (hasZero) {
            c.init(memStart, memLimit, zeroMemStart, size + 1);
        } else {
            c.init(memStart, memLimit, 0, size);
        }
    }

    @Override
    public boolean isOpen() {
        return memStart != 0;
    }

    @Override
    public void merge(Map srcMap, MapValueMergeFunction mergeFunc) {
        assert this != srcMap;
        long srcSize = srcMap.size();
        if (srcSize == 0) {
            return;
        }
        Unordered8Map src8Map = (Unordered8Map) srcMap;

        // First, we handle zero key.
        if (src8Map.hasZero) {
            if (hasZero) {
                mergeFunc.merge(
                        valueAt(zeroMemStart),
                        src8Map.valueAt(src8Map.zeroMemStart)
                );
            } else {
                Unsafe.getUnsafe().copyMemory(src8Map.zeroMemStart, zeroMemStart, entrySize);
                hasZero = true;
            }
            // Check if zero was the only element in the source map.
            if (srcSize == 1) {
                return;
            }
        }

        // Then we handle all non-zero keys.
        OUTER:
        for (long srcAddr = src8Map.memStart; srcAddr < src8Map.memLimit; srcAddr += entrySize) {
            long key = Unsafe.getUnsafe().getLong(srcAddr);
            if (key == 0) {
                continue;
            }

            long destAddr = getStartAddress(Hash.hashLong64(key) & mask);
            for (; ; ) {
                long k = Unsafe.getUnsafe().getLong(destAddr);
                if (k == 0) {
                    break;
                } else if (k == key) {
                    // Match found, merge values.
                    mergeFunc.merge(
                            valueAt(destAddr),
                            src8Map.valueAt(srcAddr)
                    );
                    continue OUTER;
                }
                destAddr = getNextAddress(destAddr);
            }

            Unsafe.getUnsafe().copyMemory(srcAddr, destAddr, entrySize);
            size++;
            if (--free == 0) {
                try {
                    rehash();
                } catch (CairoException e) {
                    free = 1;
                    throw e;
                }
            }
        }
    }

    @Override
    public MapRecordCursor newCursor() {
        Unordered8MapCursor c = new Unordered8MapCursor(record.clone(), this);
        if (hasZero) {
            return c.init(memStart, memLimit, zeroMemStart, size + 1);
        }
        return c.init(memStart, memLimit, 0, size);
    }

    @Override
    public long probeBatch(
            PageFrameMemoryRecord record,
            RecordSink mapSink,
            long batchStart,
            long batchEnd,
            long batchAddr
    ) {
        // Caller must have pre-reserved at least (batchEnd - batchStart) free slots via
        // reserveCapacity(), so the hot loop skips the per-insert rehash check — a mid-batch
        // rehash would invalidate offsets already packed into batchAddr.
        assert free > batchEnd - batchStart;

        final int directColumnIndex = mapSink.getDirectColumnIndex();
        if (directColumnIndex >= 0) {
            // Zero page address means a column top; fall through to the sink-based path.
            final long columnAddr = record.getPageAddress(directColumnIndex);
            if (columnAddr != 0) {
                return probeBatchUnsafe(columnAddr, batchStart, batchEnd, batchAddr);
            }
        }

        for (long r = batchStart; r < batchEnd; r++) {
            record.setRowIndex(r);
            mapSink.copy(record, key);
            final long k = key.key;

            long startAddress;
            boolean isNew;
            if (k != 0) {
                long hashCode = Hash.hashLong64(k);
                startAddress = getStartAddress(hashCode & mask);
                for (; ; ) {
                    long existing = Unsafe.getUnsafe().getLong(startAddress);
                    if (existing == 0) {
                        Unsafe.getUnsafe().putLong(startAddress, k);
                        free--;
                        size++;
                        if (batchEmptyValueStart != 0) {
                            Unsafe.getUnsafe().copyMemory(batchEmptyValueStart, startAddress + KEY_SIZE, valueSize);
                        }
                        isNew = true;
                        break;
                    } else if (existing == k) {
                        isNew = false;
                        break;
                    }
                    startAddress = getNextAddress(startAddress);
                }
            } else {
                // Zero key — stored in the dedicated slot at the end of the buffer.
                startAddress = zeroMemStart;
                isNew = !hasZero;
                if (isNew) {
                    hasZero = true;
                    if (batchEmptyValueStart != 0) {
                        Unsafe.getUnsafe().copyMemory(batchEmptyValueStart, startAddress + KEY_SIZE, valueSize);
                    }
                }
            }

            long encoded = Map.encodeBatchEntry(r, startAddress + KEY_SIZE - memStart, isNew);
            Unsafe.getUnsafe().putLong(batchAddr, encoded);
            batchAddr += Long.BYTES;
        }
        return memStart;
    }

    @Override
    public long probeBatchFiltered(
            PageFrameMemoryRecord record,
            RecordSink mapSink,
            long rowIdsAddr,
            long batchStart,
            long batchEnd,
            long batchAddr
    ) {
        assert free > batchEnd - batchStart;

        final int directColumnIndex = mapSink.getDirectColumnIndex();
        if (directColumnIndex >= 0) {
            // Zero page address means a column top; fall through to the sink-based path.
            final long columnAddr = record.getPageAddress(directColumnIndex);
            if (columnAddr != 0) {
                return probeBatchFilteredUnsafe(columnAddr, rowIdsAddr, batchStart, batchEnd, batchAddr);
            }
        }

        for (long p = batchStart; p < batchEnd; p++) {
            final long r = Unsafe.getUnsafe().getLong(rowIdsAddr + (p << 3));
            record.setRowIndex(r);
            mapSink.copy(record, key);
            final long k = key.key;

            long startAddress;
            boolean isNew;
            if (k != 0) {
                long hashCode = Hash.hashLong64(k);
                startAddress = getStartAddress(hashCode & mask);
                for (; ; ) {
                    long existing = Unsafe.getUnsafe().getLong(startAddress);
                    if (existing == 0) {
                        Unsafe.getUnsafe().putLong(startAddress, k);
                        free--;
                        size++;
                        if (batchEmptyValueStart != 0) {
                            Unsafe.getUnsafe().copyMemory(batchEmptyValueStart, startAddress + KEY_SIZE, valueSize);
                        }
                        isNew = true;
                        break;
                    } else if (existing == k) {
                        isNew = false;
                        break;
                    }
                    startAddress = getNextAddress(startAddress);
                }
            } else {
                startAddress = zeroMemStart;
                isNew = !hasZero;
                if (isNew) {
                    hasZero = true;
                    if (batchEmptyValueStart != 0) {
                        Unsafe.getUnsafe().copyMemory(batchEmptyValueStart, startAddress + KEY_SIZE, valueSize);
                    }
                }
            }

            long encoded = Map.encodeBatchEntry(r, startAddress + KEY_SIZE - memStart, isNew);
            Unsafe.getUnsafe().putLong(batchAddr, encoded);
            batchAddr += Long.BYTES;
        }
        return memStart;
    }

    @Override
    public void reopen(int keyCapacity, long heapSize) {
        if (memStart == 0) {
            keyCapacity = (int) (keyCapacity / loadFactor);
            initialKeyCapacity = Math.max(Numbers.ceilPow2(keyCapacity), MIN_KEY_CAPACITY);
            restoreInitialCapacity();
        }
    }

    public void reopen() {
        if (memStart == 0) {
            restoreInitialCapacity();
        }
    }

    @Override
    public void reserveCapacity(long additionalKeys) {
        // +1: guarantee free > additionalKeys on return so that asNew's --free == 0
        // rehash never fires on the last insertion within a probeBatch.
        if (free <= additionalKeys) {
            long required = keyCapacity + (long) Math.ceil((additionalKeys - free + 1) / loadFactor);
            rehash(Numbers.ceilPow2(required));
        }
    }

    @Override
    public void restoreInitialCapacity() {
        if (memStart == 0 || keyCapacity != initialKeyCapacity) {
            // Allocate one extra slot at the end for the zero key entry.
            final long sizeBytes = entrySize * (initialKeyCapacity + 1);
            long newMemStart;
            if (memStart == 0) {
                newMemStart = Unsafe.malloc(sizeBytes, memoryTag);
            } else {
                newMemStart = Unsafe.realloc(memStart, memLimit - memStart + entrySize, sizeBytes, memoryTag);
            }
            memStart = newMemStart;
            memLimit = memStart + entrySize * initialKeyCapacity;
            zeroMemStart = memLimit;
            keyCapacity = initialKeyCapacity;
            mask = keyCapacity - 1;
        } else if (zeroMemStart == 0) {
            zeroMemStart = memLimit;
        }

        clear();
    }

    @Override
    public void setBatchEmptyValue(GroupByFunctionsUpdater updater) {
        if (batchEmptyValueStart != 0) {
            batchEmptyValueStart = Unsafe.free(batchEmptyValueStart, valueSize, memoryTag);
        }
        if (updater == null || valueSize == 0) {
            return;
        }
        final long buf = Unsafe.malloc(valueSize, memoryTag);
        try {
            Vect.memset(buf, valueSize, 0);
            // Populate the empty value into the scratch buffer using value as a flyweight.
            // updateEmpty() only writes to value addresses (valueAddress + offset), so the
            // entry address is irrelevant here.
            value.of(buf);
            updater.updateEmpty(value);
            // If the resulting value region is all zeros, we don't need a per-entry memcpy
            // since fresh slots are already zeroed by clear().
            boolean allZero = true;
            for (long p = buf, end = buf + valueSize; p < end; p++) {
                if (Unsafe.getUnsafe().getByte(p) != 0) {
                    allZero = false;
                    break;
                }
            }
            if (allZero) {
                Unsafe.free(buf, valueSize, memoryTag);
            } else {
                batchEmptyValueStart = buf;
            }
        } catch (Throwable th) {
            if (batchEmptyValueStart != buf) {
                Unsafe.free(buf, valueSize, memoryTag);
            }
            throw th;
        }
    }

    @Override
    public void setKeyCapacity(int newKeyCapacity) {
        long requiredCapacity = (long) (newKeyCapacity / loadFactor);
        if (requiredCapacity > MAX_SAFE_INT_POW_2) {
            throw CairoException.nonCritical().put("map capacity overflow");
        }
        rehash(Numbers.ceilPow2((int) requiredCapacity));
    }

    @Override
    public long size() {
        return hasZero ? size + 1 : size;
    }

    @Override
    public MapValue valueAt(long startAddress) {
        return valueOf(startAddress, false, value);
    }

    @Override
    public MapKey withKey() {
        return key;
    }

    private static void validateBatchAddressable(long sizeBytes) {
        // A silent truncation here would feed corrupted offsets into every batched
        // probe; fail loudly instead of producing wrong aggregation results.
        if (sizeBytes > Map.BATCH_OFFSET_MASK) {
            throw CairoException.nonCritical()
                    .put("Unordered8Map heap size exceeds batched probe addressable range [heapBytes=").put(sizeBytes)
                    .put(", maxAddressable=").put(Map.BATCH_OFFSET_MASK)
                    .put(']');
        }
    }

    private FlyweightPackedMapValue asNew(long startAddress, long key, long hashCode, FlyweightPackedMapValue value) {
        Unsafe.getUnsafe().putLong(startAddress, key);
        if (--free == 0) {
            try {
                rehash();
            } catch (CairoException e) {
                free = 1;
                throw e;
            }
            // Index may have changed after rehash, so we need to find the key.
            startAddress = getStartAddress(hashCode & mask);
            for (; ; ) {
                long k = Unsafe.getUnsafe().getLong(startAddress);
                if (k == key) {
                    break;
                }
                startAddress = getNextAddress(startAddress);
            }
        }
        size++;
        return valueOf(startAddress, true, value);
    }

    // Advance through the map data structure sequentially,
    // avoiding multiplication and pseudo-random access.
    private long getNextAddress(long entryAddress) {
        entryAddress += entrySize;
        if (entryAddress < memLimit) {
            return entryAddress;
        }
        return memStart;
    }

    private long getStartAddress(long memStart, long index) {
        return memStart + entrySize * index;
    }

    private long getStartAddress(long index) {
        return memStart + entrySize * index;
    }

    private long probeBatchFilteredUnsafe(long columnAddr, long rowIdsAddr, long batchStart, long batchEnd, long batchAddr) {
        for (long p = batchStart; p < batchEnd; p++) {
            final long r = Unsafe.getUnsafe().getLong(rowIdsAddr + (p << 3));
            final long k = Unsafe.getUnsafe().getLong(columnAddr + r * Long.BYTES);

            long startAddress;
            boolean isNew;
            if (k != 0) {
                long hashCode = Hash.hashLong64(k);
                startAddress = getStartAddress(hashCode & mask);
                for (; ; ) {
                    long existing = Unsafe.getUnsafe().getLong(startAddress);
                    if (existing == 0) {
                        Unsafe.getUnsafe().putLong(startAddress, k);
                        free--;
                        size++;
                        if (batchEmptyValueStart != 0) {
                            Unsafe.getUnsafe().copyMemory(batchEmptyValueStart, startAddress + KEY_SIZE, valueSize);
                        }
                        isNew = true;
                        break;
                    } else if (existing == k) {
                        isNew = false;
                        break;
                    }
                    startAddress = getNextAddress(startAddress);
                }
            } else {
                startAddress = zeroMemStart;
                isNew = !hasZero;
                if (isNew) {
                    hasZero = true;
                    if (batchEmptyValueStart != 0) {
                        Unsafe.getUnsafe().copyMemory(batchEmptyValueStart, startAddress + KEY_SIZE, valueSize);
                    }
                }
            }

            long encoded = Map.encodeBatchEntry(r, startAddress + KEY_SIZE - memStart, isNew);
            Unsafe.getUnsafe().putLong(batchAddr, encoded);
            batchAddr += Long.BYTES;
        }
        return memStart;
    }

    private long probeBatchUnsafe(long columnAddr, long batchStart, long batchEnd, long batchAddr) {
        for (long r = batchStart; r < batchEnd; r++) {
            final long k = Unsafe.getUnsafe().getLong(columnAddr + r * Long.BYTES);

            long startAddress;
            boolean isNew;
            if (k != 0) {
                long hashCode = Hash.hashLong64(k);
                startAddress = getStartAddress(hashCode & mask);
                for (; ; ) {
                    long existing = Unsafe.getUnsafe().getLong(startAddress);
                    if (existing == 0) {
                        Unsafe.getUnsafe().putLong(startAddress, k);
                        free--;
                        size++;
                        if (batchEmptyValueStart != 0) {
                            Unsafe.getUnsafe().copyMemory(batchEmptyValueStart, startAddress + KEY_SIZE, valueSize);
                        }
                        isNew = true;
                        break;
                    } else if (existing == k) {
                        isNew = false;
                        break;
                    }
                    startAddress = getNextAddress(startAddress);
                }
            } else {
                startAddress = zeroMemStart;
                isNew = !hasZero;
                if (isNew) {
                    hasZero = true;
                    if (batchEmptyValueStart != 0) {
                        Unsafe.getUnsafe().copyMemory(batchEmptyValueStart, startAddress + KEY_SIZE, valueSize);
                    }
                }
            }

            long encoded = Map.encodeBatchEntry(r, startAddress + KEY_SIZE - memStart, isNew);
            Unsafe.getUnsafe().putLong(batchAddr, encoded);
            batchAddr += Long.BYTES;
        }
        return memStart;
    }

    private void rehash() {
        rehash((long) keyCapacity << 1);
    }

    private void rehash(long newKeyCapacity) {
        if (nResizes == maxResizes) {
            throw LimitOverflowException.instance().put("limit of ").put(maxResizes).put(" resizes exceeded in unordered map");
        }
        if (newKeyCapacity > MAX_SAFE_INT_POW_2) {
            throw CairoException.nonCritical().put("map capacity overflow");
        }
        if (newKeyCapacity <= keyCapacity) {
            return;
        }

        // Allocate one extra slot at the end for the zero key entry.
        final long newSizeBytes = entrySize * (newKeyCapacity + 1);
        validateBatchAddressable(newSizeBytes);
        final long newMemStart = Unsafe.malloc(newSizeBytes, memoryTag);
        final long newMemLimit = newMemStart + entrySize * newKeyCapacity;
        Vect.memset(newMemStart, newSizeBytes, 0);
        final int newMask = (int) newKeyCapacity - 1;

        for (long addr = memStart; addr < memLimit; addr += entrySize) {
            long key = Unsafe.getUnsafe().getLong(addr);
            if (key == 0) {
                continue;
            }

            long newAddr = getStartAddress(newMemStart, Hash.hashLong64(key) & newMask);
            while (Unsafe.getUnsafe().getLong(newAddr) != 0) {
                newAddr += entrySize;
                if (newAddr >= newMemLimit) {
                    newAddr = newMemStart;
                }
            }
            Unsafe.getUnsafe().copyMemory(addr, newAddr, entrySize);
        }

        // Copy the zero key entry to the new end-of-buffer slot.
        if (hasZero) {
            Unsafe.getUnsafe().copyMemory(zeroMemStart, newMemLimit, entrySize);
        }

        Unsafe.free(memStart, memLimit - memStart + entrySize, memoryTag);

        memStart = newMemStart;
        memLimit = newMemLimit;
        zeroMemStart = newMemLimit;
        mask = newMask;
        free += (int) ((newKeyCapacity - keyCapacity) * loadFactor);
        keyCapacity = (int) newKeyCapacity;
        nResizes++;
    }

    private FlyweightPackedMapValue valueOf(long startAddress, boolean newValue, FlyweightPackedMapValue value) {
        return value.of(startAddress, startAddress + KEY_SIZE, newValue);
    }

    long entrySize() {
        return entrySize;
    }

    boolean isZeroKey(long startAddress) {
        return Unsafe.getUnsafe().getLong(startAddress) == 0;
    }

    class Key implements MapKey {
        private long key;

        @Override
        public long commit() {
            return KEY_SIZE; // we don't need to track the actual key size
        }

        @Override
        public void copyFrom(MapKey srcKey) {
            Key src8Key = (Key) srcKey;
            copyFromRawKey(src8Key.key);
        }

        @Override
        public MapValue createValue() {
            if (key != 0) {
                return createNonZeroKeyValue(key, Hash.hashLong64(key));
            }
            return createZeroKeyValue();
        }

        @Override
        public MapValue createValue(long hashCode) {
            if (key != 0) {
                return createNonZeroKeyValue(key, hashCode);
            }
            return createZeroKeyValue();
        }

        @Override
        public MapValue findValue() {
            return findValue(value);
        }

        @Override
        public MapValue findValue2() {
            return findValue(value2);
        }

        @Override
        public MapValue findValue3() {
            return findValue(value3);
        }

        @Override
        public long hash() {
            return Hash.hashLong64(key);
        }

        @Override
        public void put(Record record, RecordSink sink) {
            sink.copy(record, this);
        }

        @Override
        public void putArray(ArrayView view) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putBin(BinarySequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putBool(boolean value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putByte(byte value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putChar(char value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putDate(long value) {
            putLong(value);
        }

        @Override
        public void putDecimal128(Decimal128 decimal128) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putDecimal256(Decimal256 decimal256) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putDouble(double value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putFloat(float value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putIPv4(int value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putInt(int value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putInterval(Interval interval) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putLong(long value) {
            this.key = value;
        }

        @Override
        public void putLong128(long lo, long hi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putLong256(Long256 value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putLong256(long l0, long l1, long l2, long l3) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putRecord(Record value) {
            // no-op
        }

        @Override
        public void putShort(short value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putStr(CharSequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putStr(CharSequence value, int lo, int hi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putTimestamp(long value) {
            putLong(value);
        }

        @Override
        public void putVarchar(Utf8Sequence value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void skip(int bytes) {
            throw new UnsupportedOperationException();
        }

        private MapValue createNonZeroKeyValue(long key, long hashCode) {
            long startAddress = getStartAddress(hashCode & mask);
            for (; ; ) {
                long k = Unsafe.getUnsafe().getLong(startAddress);
                if (k == 0) {
                    return asNew(startAddress, key, hashCode, value);
                } else if (k == key) {
                    return valueOf(startAddress, false, value);
                }
                startAddress = getNextAddress(startAddress);
            }
        }

        private MapValue createZeroKeyValue() {
            if (hasZero) {
                return valueOf(zeroMemStart, false, value);
            }
            hasZero = true;
            return valueOf(zeroMemStart, true, value);
        }

        private MapValue findValue(FlyweightPackedMapValue value) {
            if (key == 0) {
                return hasZero ? valueOf(zeroMemStart, false, value) : null;
            }

            long startAddress = getStartAddress(Hash.hashLong64(key) & mask);
            for (; ; ) {
                long k = Unsafe.getUnsafe().getLong(startAddress);
                if (k == 0) {
                    return null;
                } else if (k == key) {
                    return valueOf(startAddress, false, value);
                }
                startAddress = getNextAddress(startAddress);
            }
        }

        void copyFromRawKey(long key) {
            this.key = key;
        }
    }
}
