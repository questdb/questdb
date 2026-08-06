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
import io.questdb.std.MemoryTracker;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.bytes.Bytes;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.Numbers.MAX_SAFE_INT_POW_2;

/**
 * Unordered16Map is a specialized off-heap hash table with a 4-byte key column followed by an
 * 8-byte key column. The supported 4-byte types are INT/IPv4/SYMBOL and the supported 8-byte
 * types are LONG/TIMESTAMP/DATE.
 * It stores intermediate data for group-by and sample-by queries. It provides {@link MapKey} and
 * {@link MapValue}, as well as {@link RecordCursor} interfaces for data access and modification.
 * The preferred way to create an Unordered16Map is {@link MapFactory}.
 * <p>
 * Map iteration provided by {@link RecordCursor} does not preserve the key insertion order, hence
 * the unordered map name.
 * <strong>Important!</strong>
 * Key and value structures must match the ones provided via lists of columns ({@link ColumnTypes})
 * to the map constructor. Later put* calls made on {@link MapKey} and {@link MapValue} must match
 * the declared column types to guarantee memory access safety.
 * <p>
 * Keys use a 16-byte inline representation. Only insertions and updates operations are
 * supported meaning that a key can't be removed from the map once it was inserted.
 * <p>
 * The hash table is organized into the following parts:
 * <ul>
 * <li>1. One control/fingerprint byte per hash-table slot</li>
 * <li>2. Off-heap memory for key-value pairs, i.e. the hash table with open addressing</li>
 * <li>3. One extra key-value entry for the all-zero key</li>
 * </ul>
 * The hash table uses linear probing. Empty slots are identified through the control array,
 * so clearing or allocating a map only zeroes one byte per slot. A non-zero control byte also
 * carries a hash fingerprint, avoiding most full 16-byte key comparisons on collisions.
 * <p>
 * Key-value pairs stored in the hash table may have the following layout:
 * <pre>
 * | 4-byte key | Padding | 8-byte key | Value columns 0..V |
 * +------------+---------+------------+--------------------+
 * |  4 bytes   | 4 bytes |  8 bytes   |         -          |
 * +------------+---------+------------+--------------------+
 * </pre>
 */
public class Unordered16Map implements Map, Reopenable {
    static final long KEY_SIZE = 2L * Long.BYTES;
    private static final int MERGE_NEW_BATCH_SIZE = 8;
    private static final int MIN_KEY_CAPACITY = 16;

    private final Unordered16MapCursor cursor;
    private final long entrySize;
    private final Key key;
    private final double loadFactor;
    private final int maxResizes;
    private final int memoryTag;
    private final Unordered16MapRecord record;
    private final FlyweightPackedMapValue value;
    private final FlyweightPackedMapValue value2;
    private final FlyweightPackedMapValue value3;
    private final long valueSize;
    private long batchEmptyValueStart;
    private long controlLimit;
    private long controlStart;
    private int free;
    private boolean hasZero;
    private int initialKeyCapacity;
    private int keyCapacity;
    private long mask;
    private long memLimit; // Hash table memory limit pointer.
    private long memStart; // Hash table memory start pointer.
    @Nullable
    private long[] mergeNewEntryRowIds;
    private long rawMemStart; // Allocation start; control bytes precede the entry array.
    // Per-query native memory tracker bound by the owning factory at cursor start.
    // Null when no per-query limit applies; all Unsafe.{malloc,realloc,free} calls
    // degrade to the global-only overloads in that case.
    @Nullable
    private MemoryTracker memoryTracker;
    private int nResizes;
    private int size = 0;
    private long zeroMemStart; // Zero key-value pair memory start pointer.

    public Unordered16Map(
            @Transient ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes
    ) {
        this(keyTypes, valueTypes, keyCapacity, loadFactor, maxResizes, MemoryTag.NATIVE_UNORDERED_MAP, true);
    }

    public Unordered16Map(
            @Transient ColumnTypes keyTypes,
            @Transient @Nullable ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes,
            boolean openOnInit
    ) {
        this(keyTypes, valueTypes, keyCapacity, loadFactor, maxResizes, MemoryTag.NATIVE_UNORDERED_MAP, openOnInit);
    }

    Unordered16Map(
            @Transient ColumnTypes keyTypes,
            @Nullable @Transient ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes,
            int memoryTag,
            boolean openOnInit
    ) {
        assert loadFactor > 0 && loadFactor < 1d;

        try {
            this.memoryTag = memoryTag;
            this.loadFactor = loadFactor;
            this.initialKeyCapacity = Math.max(Numbers.ceilPow2((int) (keyCapacity / loadFactor)), MIN_KEY_CAPACITY);
            this.maxResizes = maxResizes;
            nResizes = 0;

            if (!isSupportedKeyTypes(keyTypes)) {
                throw CairoException.nonCritical().put("unexpected key types");
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
            // Validate against initialKeyCapacity so both eager and lazy modes catch the
            // overflow up front, before any cursor opens.
            validateBatchAddressable(entrySize * (this.initialKeyCapacity + 1));

            if (openOnInit) {
                this.keyCapacity = this.initialKeyCapacity;
                mask = this.keyCapacity - 1;
                free = (int) (this.keyCapacity * loadFactor);
                allocate(this.keyCapacity);
            }
            // else: memStart / memLimit / zeroMemStart stay 0, keyCapacity stays 0;
            // first reopen() allocates initial backing under whatever MemoryTracker
            // is bound at that time.

            value = new FlyweightPackedMapValue(valueSize, valueOffsets);
            value2 = new FlyweightPackedMapValue(valueSize, valueOffsets);
            value3 = new FlyweightPackedMapValue(valueSize, valueOffsets);

            record = new Unordered16MapRecord(valueSize, valueOffsets, value, keyTypes, valueTypes);
            cursor = new Unordered16MapCursor(record, this);
            key = new Key();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    public static boolean isSupportedKeyTypes(ColumnTypes keyTypes) {
        final int keyCount = keyTypes.getColumnCount();
        return keyCount == 2
                && Unordered4Map.isSupportedKeyType(keyTypes.getColumnType(0))
                && Unordered8Map.isSupportedKeyType(keyTypes.getColumnType(1));
    }

    @Override
    public void clear() {
        free = (int) (keyCapacity * loadFactor);
        size = 0;
        nResizes = 0;
        hasZero = false;
        if (controlStart != 0) {
            Vect.memset(controlStart, keyCapacity, 0);
        }
    }

    @Override
    public void close() {
        if (rawMemStart != 0) {
            rawMemStart = Unsafe.free(rawMemStart, allocationSize(keyCapacity), memoryTag, memoryTracker);
            controlLimit = controlStart = 0;
            memLimit = memStart = 0;
            zeroMemStart = 0;
            free = 0;
            size = 0;
            hasZero = false;
        }
        if (batchEmptyValueStart != 0) {
            batchEmptyValueStart = Unsafe.free(batchEmptyValueStart, valueSize, memoryTag, memoryTracker);
        }
    }

    @Override
    public MapRecordCursor getCursor() {
        if (hasZero) {
            return cursor.init(controlStart, controlLimit, memStart, memLimit, zeroMemStart, size + 1);
        }
        return cursor.init(controlStart, controlLimit, memStart, memLimit, 0, size);
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
        Unordered16MapCursor c = (Unordered16MapCursor) cursor;
        if (hasZero) {
            c.init(controlStart, controlLimit, memStart, memLimit, zeroMemStart, size + 1);
        } else {
            c.init(controlStart, controlLimit, memStart, memLimit, 0, size);
        }
    }

    @Override
    public boolean isOpen() {
        return rawMemStart != 0;
    }

    @Override
    public void merge(Map srcMap, MapValueMergeFunction mergeFunc) {
        merge(srcMap, mergeFunc, null);
    }

    @Override
    public void merge(
            Map srcMap,
            MapValueMergeFunction mergeFunc,
            @Nullable MapRecordMergeFunction newEntryFunc
    ) {
        assert this != srcMap;
        long srcSize = srcMap.size();
        if (srcSize == 0) {
            return;
        }
        Unordered16Map src16Map = (Unordered16Map) srcMap;
        final long[] newEntryRowIds;
        if (newEntryFunc != null) {
            if (mergeNewEntryRowIds == null) {
                mergeNewEntryRowIds = new long[MERGE_NEW_BATCH_SIZE];
            }
            newEntryRowIds = mergeNewEntryRowIds;
        } else {
            newEntryRowIds = null;
        }
        int newEntryCount = 0;

        // First, we handle zero key.
        if (src16Map.hasZero) {
            if (hasZero) {
                mergeFunc.merge(
                        valueAt(zeroMemStart),
                        src16Map.valueAt(src16Map.zeroMemStart)
                );
            } else {
                Unsafe.copyMemory(src16Map.zeroMemStart, zeroMemStart, entrySize);
                hasZero = true;
                if (newEntryRowIds != null) {
                    newEntryRowIds[newEntryCount++] = src16Map.zeroMemStart;
                }
            }
            // Check if zero was the only element in the source map.
            if (srcSize == 1) {
                mergeNewEntries(src16Map, newEntryFunc, newEntryRowIds, newEntryCount);
                return;
            }
        }

        // Then we handle all non-zero keys.
        OUTER:
        for (
                long srcAddr = src16Map.memStart, srcControl = src16Map.controlStart;
                srcAddr < src16Map.memLimit;
                srcAddr += entrySize, srcControl++
        ) {
            if (Unsafe.getByte(srcControl) == 0) {
                continue;
            }
            final long keyLo = Unsafe.getLong(srcAddr);
            final long keyHi = Unsafe.getLong(srcAddr + Long.BYTES);

            final long hashCode = hashKey(keyLo, keyHi);
            final byte fingerprint = hashFingerprint(hashCode);
            final long index = hashCode & mask;
            long destAddr = getStartAddress(index);
            long destControl = controlStart + index;
            for (; ; ) {
                final byte control = Unsafe.getByte(destControl);
                if (control == 0) {
                    break;
                } else if (control == fingerprint && keyEquals(destAddr, keyLo, keyHi)) {
                    // Match found, merge values.
                    mergeFunc.merge(
                            valueAt(destAddr),
                            src16Map.valueAt(srcAddr)
                    );
                    continue OUTER;
                }
                destAddr = getNextAddress(destAddr);
                if (++destControl == controlLimit) {
                    destControl = controlStart;
                }
            }

            Unsafe.copyMemory(srcAddr, destAddr, entrySize);
            Unsafe.putByte(destControl, fingerprint);
            size++;
            if (--free == 0) {
                try {
                    rehash();
                } catch (CairoException e) {
                    free = 1;
                    throw e;
                }
            }
            if (newEntryRowIds != null) {
                newEntryRowIds[newEntryCount++] = srcAddr;
                if (newEntryCount == MERGE_NEW_BATCH_SIZE) {
                    newEntryFunc.mergeNewBatch(src16Map.record, newEntryRowIds, newEntryCount);
                    newEntryCount = 0;
                }
            }
        }
        mergeNewEntries(src16Map, newEntryFunc, newEntryRowIds, newEntryCount);
    }

    @Override
    public MapRecordCursor newCursor() {
        Unordered16MapCursor c = new Unordered16MapCursor(record.clone(), this);
        if (hasZero) {
            return c.init(controlStart, controlLimit, memStart, memLimit, zeroMemStart, size + 1);
        }
        return c.init(controlStart, controlLimit, memStart, memLimit, 0, size);
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

        for (long r = batchStart; r < batchEnd; r++) {
            record.setRowIndex(r);
            mapSink.copy(record, key);
            final long keyLo = key.keyLo;
            final long keyHi = key.keyHi;

            long startAddress;
            boolean isNew;
            if (!isZeroKey(keyLo, keyHi)) {
                final long hashCode = hashKey(keyLo, keyHi);
                final byte fingerprint = hashFingerprint(hashCode);
                final long index = hashCode & mask;
                startAddress = getStartAddress(index);
                long controlAddress = controlStart + index;
                for (; ; ) {
                    final byte control = Unsafe.getByte(controlAddress);
                    if (control == 0) {
                        putKey(startAddress, keyLo, keyHi);
                        Unsafe.putByte(controlAddress, fingerprint);
                        free--;
                        size++;
                        initializeNewValue(startAddress);
                        isNew = true;
                        break;
                    } else if (control == fingerprint && keyEquals(startAddress, keyLo, keyHi)) {
                        isNew = false;
                        break;
                    }
                    startAddress = getNextAddress(startAddress);
                    if (++controlAddress == controlLimit) {
                        controlAddress = controlStart;
                    }
                }
            } else {
                // Zero key — stored in the dedicated slot at the end of the buffer.
                startAddress = zeroMemStart;
                isNew = !hasZero;
                if (isNew) {
                    hasZero = true;
                    putKey(startAddress, 0, 0);
                    initializeNewValue(startAddress);
                }
            }

            long encoded = Map.encodeBatchEntry(r, startAddress + KEY_SIZE - memStart, isNew);
            Unsafe.putLong(batchAddr, encoded);
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

        for (long p = batchStart; p < batchEnd; p++) {
            final long r = Unsafe.getLong(rowIdsAddr + (p << 3));
            record.setRowIndex(r);
            mapSink.copy(record, key);
            final long keyLo = key.keyLo;
            final long keyHi = key.keyHi;

            long startAddress;
            boolean isNew;
            if (!isZeroKey(keyLo, keyHi)) {
                final long hashCode = hashKey(keyLo, keyHi);
                final byte fingerprint = hashFingerprint(hashCode);
                final long index = hashCode & mask;
                startAddress = getStartAddress(index);
                long controlAddress = controlStart + index;
                for (; ; ) {
                    final byte control = Unsafe.getByte(controlAddress);
                    if (control == 0) {
                        putKey(startAddress, keyLo, keyHi);
                        Unsafe.putByte(controlAddress, fingerprint);
                        free--;
                        size++;
                        initializeNewValue(startAddress);
                        isNew = true;
                        break;
                    } else if (control == fingerprint && keyEquals(startAddress, keyLo, keyHi)) {
                        isNew = false;
                        break;
                    }
                    startAddress = getNextAddress(startAddress);
                    if (++controlAddress == controlLimit) {
                        controlAddress = controlStart;
                    }
                }
            } else {
                startAddress = zeroMemStart;
                isNew = !hasZero;
                if (isNew) {
                    hasZero = true;
                    putKey(startAddress, 0, 0);
                    initializeNewValue(startAddress);
                }
            }

            long encoded = Map.encodeBatchEntry(r, startAddress + KEY_SIZE - memStart, isNew);
            Unsafe.putLong(batchAddr, encoded);
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
        if (rawMemStart == 0 || keyCapacity != initialKeyCapacity) {
            final long newSizeBytes = allocationSize(initialKeyCapacity);
            if (rawMemStart == 0) {
                rawMemStart = Unsafe.malloc(newSizeBytes, memoryTag, memoryTracker);
            } else {
                rawMemStart = Unsafe.realloc(
                        rawMemStart,
                        allocationSize(keyCapacity),
                        newSizeBytes,
                        memoryTag,
                        memoryTracker
                );
            }
            keyCapacity = initialKeyCapacity;
            setMemoryLayout();
            mask = keyCapacity - 1;
        }

        clear();
    }

    @Override
    public void setBatchEmptyValue(GroupByFunctionsUpdater updater) {
        if (batchEmptyValueStart != 0) {
            batchEmptyValueStart = Unsafe.free(batchEmptyValueStart, valueSize, memoryTag, memoryTracker);
        }
        if (updater == null || valueSize == 0) {
            return;
        }
        final long buf = Unsafe.malloc(valueSize, memoryTag, memoryTracker);
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
                if (Unsafe.getByte(p) != 0) {
                    allZero = false;
                    break;
                }
            }
            if (allZero) {
                Unsafe.free(buf, valueSize, memoryTag, memoryTracker);
            } else {
                batchEmptyValueStart = buf;
            }
        } catch (Throwable th) {
            if (batchEmptyValueStart != buf) {
                Unsafe.free(buf, valueSize, memoryTag, memoryTracker);
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
    public void setMemoryTracker(@Nullable MemoryTracker tracker) {
        this.memoryTracker = tracker;
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
                    .put("Unordered16Map heap size exceeds batched probe addressable range [heapBytes=").put(sizeBytes)
                    .put(", maxAddressable=").put(Map.BATCH_OFFSET_MASK)
                    .put(']');
        }
    }

    private long allocationSize(long capacity) {
        return Bytes.align8b(capacity) + entrySize * (capacity + 1);
    }

    private void allocate(int capacity) {
        rawMemStart = Unsafe.malloc(allocationSize(capacity), memoryTag, memoryTracker);
        setMemoryLayout();
        Vect.memset(controlStart, capacity, 0);
    }

    private static byte hashFingerprint(long hashCode) {
        final byte fingerprint = (byte) (hashCode >>> 48);
        return fingerprint != 0 ? fingerprint : 1;
    }

    private static long hashKey(long keyLo, long keyHi) {
        return Hash.hashLong128_64(keyLo, keyHi);
    }

    private static boolean isZeroKey(long keyLo, long keyHi) {
        return keyLo == 0 && keyHi == 0;
    }

    private static boolean keyEquals(long startAddress, long keyLo, long keyHi) {
        return Unsafe.getLong(startAddress) == keyLo
                && Unsafe.getLong(startAddress + Long.BYTES) == keyHi;
    }

    private static void putKey(long startAddress, long keyLo, long keyHi) {
        Unsafe.putLong(startAddress, keyLo);
        Unsafe.putLong(startAddress + Long.BYTES, keyHi);
    }

    private void initializeNewValue(long startAddress) {
        if (valueSize == 0) {
            return;
        }
        if (batchEmptyValueStart != 0) {
            Unsafe.copyMemory(batchEmptyValueStart, startAddress + KEY_SIZE, valueSize);
        } else {
            Vect.memset(startAddress + KEY_SIZE, valueSize, 0);
        }
    }

    private void setMemoryLayout() {
        controlStart = rawMemStart;
        controlLimit = controlStart + keyCapacity;
        memStart = rawMemStart + Bytes.align8b(keyCapacity);
        memLimit = memStart + entrySize * keyCapacity;
        zeroMemStart = memLimit;
    }

    private FlyweightPackedMapValue asNew(
            long startAddress,
            long controlAddress,
            long keyLo,
            long keyHi,
            long hashCode,
            FlyweightPackedMapValue value
    ) {
        putKey(startAddress, keyLo, keyHi);
        Unsafe.putByte(controlAddress, hashFingerprint(hashCode));
        initializeNewValue(startAddress);
        if (--free == 0) {
            try {
                rehash();
            } catch (CairoException e) {
                free = 1;
                throw e;
            }
            // Index may have changed after rehash, so we need to find the key.
            final byte fingerprint = hashFingerprint(hashCode);
            final long index = hashCode & mask;
            startAddress = getStartAddress(index);
            controlAddress = controlStart + index;
            for (; ; ) {
                if (Unsafe.getByte(controlAddress) == fingerprint && keyEquals(startAddress, keyLo, keyHi)) {
                    break;
                }
                startAddress = getNextAddress(startAddress);
                if (++controlAddress == controlLimit) {
                    controlAddress = controlStart;
                }
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

    private void rehash() {
        rehash((long) keyCapacity << 1);
    }

    private static void mergeNewEntries(
            Unordered16Map srcMap,
            @Nullable MapRecordMergeFunction newEntryFunc,
            @Nullable long[] newEntryRowIds,
            int newEntryCount
    ) {
        if (newEntryCount > 0) {
            assert newEntryFunc != null && newEntryRowIds != null;
            newEntryFunc.mergeNewBatch(srcMap.record, newEntryRowIds, newEntryCount);
        }
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

        // Allocate one extra entry at the end for the zero key. The compact control
        // array is the only region that must be cleared; stale entry bytes are ignored.
        final long newEntrySizeBytes = entrySize * (newKeyCapacity + 1);
        validateBatchAddressable(newEntrySizeBytes);
        final long newSizeBytes = allocationSize(newKeyCapacity);
        final long newRawMemStart = Unsafe.malloc(newSizeBytes, memoryTag, memoryTracker);
        final long newControlStart = newRawMemStart;
        final long newControlLimit = newControlStart + newKeyCapacity;
        final long newMemStart = newRawMemStart + Bytes.align8b(newKeyCapacity);
        final long newMemLimit = newMemStart + entrySize * newKeyCapacity;
        Vect.memset(newControlStart, newKeyCapacity, 0);
        final int newMask = (int) newKeyCapacity - 1;

        for (
                long addr = memStart, controlAddress = controlStart;
                addr < memLimit;
                addr += entrySize, controlAddress++
        ) {
            if (Unsafe.getByte(controlAddress) == 0) {
                continue;
            }
            final long keyLo = Unsafe.getLong(addr);
            final long keyHi = Unsafe.getLong(addr + Long.BYTES);
            final long hashCode = hashKey(keyLo, keyHi);
            final byte fingerprint = hashFingerprint(hashCode);
            final long index = hashCode & newMask;
            long newAddr = getStartAddress(newMemStart, index);
            long newControlAddress = newControlStart + index;
            while (Unsafe.getByte(newControlAddress) != 0) {
                newAddr += entrySize;
                newControlAddress++;
                if (newAddr >= newMemLimit) {
                    newAddr = newMemStart;
                    newControlAddress = newControlStart;
                }
            }
            Unsafe.copyMemory(addr, newAddr, entrySize);
            Unsafe.putByte(newControlAddress, fingerprint);
        }

        // Copy the zero key entry to the new end-of-buffer slot.
        if (hasZero) {
            Unsafe.copyMemory(zeroMemStart, newMemLimit, entrySize);
        }

        Unsafe.free(rawMemStart, allocationSize(keyCapacity), memoryTag, memoryTracker);

        rawMemStart = newRawMemStart;
        controlStart = newControlStart;
        controlLimit = newControlLimit;
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

    class Key implements MapKey {
        private long keyHi;
        protected long keyLo;

        @Override
        public long commit() {
            return KEY_SIZE; // we don't need to track the actual key size
        }

        @Override
        public void copyFrom(MapKey srcKey) {
            Key src16Key = (Key) srcKey;
            copyFromRawKey(src16Key.keyLo, src16Key.keyHi);
        }

        @Override
        public MapValue createValue() {
            if (!isZeroKey(keyLo, keyHi)) {
                return createNonZeroKeyValue(keyLo, keyHi, hashKey(keyLo, keyHi));
            }
            return createZeroKeyValue();
        }

        @Override
        public MapValue createValue(long hashCode) {
            if (!isZeroKey(keyLo, keyHi)) {
                return createNonZeroKeyValue(keyLo, keyHi, hashCode);
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
            return hashKey(keyLo, keyHi);
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
            putInt(value);
        }

        @Override
        public void putInt(int value) {
            keyLo = Integer.toUnsignedLong(value);
        }

        @Override
        public void putInterval(Interval interval) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putLong(long value) {
            keyHi = value;
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

        private MapValue createNonZeroKeyValue(long keyLo, long keyHi, long hashCode) {
            final byte fingerprint = hashFingerprint(hashCode);
            final long index = hashCode & mask;
            long startAddress = getStartAddress(index);
            long controlAddress = controlStart + index;
            for (; ; ) {
                final byte control = Unsafe.getByte(controlAddress);
                if (control == 0) {
                    return asNew(startAddress, controlAddress, keyLo, keyHi, hashCode, value);
                } else if (control == fingerprint && keyEquals(startAddress, keyLo, keyHi)) {
                    return valueOf(startAddress, false, value);
                }
                startAddress = getNextAddress(startAddress);
                if (++controlAddress == controlLimit) {
                    controlAddress = controlStart;
                }
            }
        }

        private MapValue createZeroKeyValue() {
            if (hasZero) {
                return valueOf(zeroMemStart, false, value);
            }
            hasZero = true;
            putKey(zeroMemStart, 0, 0);
            initializeNewValue(zeroMemStart);
            return valueOf(zeroMemStart, true, value);
        }

        private MapValue findValue(FlyweightPackedMapValue value) {
            if (isZeroKey(keyLo, keyHi)) {
                return hasZero ? valueOf(zeroMemStart, false, value) : null;
            }

            final long hashCode = hashKey(keyLo, keyHi);
            final byte fingerprint = hashFingerprint(hashCode);
            final long index = hashCode & mask;
            long startAddress = getStartAddress(index);
            long controlAddress = controlStart + index;
            for (; ; ) {
                final byte control = Unsafe.getByte(controlAddress);
                if (control == 0) {
                    return null;
                } else if (control == fingerprint && keyEquals(startAddress, keyLo, keyHi)) {
                    return valueOf(startAddress, false, value);
                }
                startAddress = getNextAddress(startAddress);
                if (++controlAddress == controlLimit) {
                    controlAddress = controlStart;
                }
            }
        }

        void copyFromRawKey(long keyLo, long keyHi) {
            this.keyLo = keyLo;
            this.keyHi = keyHi;
        }
    }

}
