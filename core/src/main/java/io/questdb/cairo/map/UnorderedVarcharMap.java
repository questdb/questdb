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

package io.questdb.cairo.map;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Hash;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.bytes.Bytes;
import io.questdb.std.bytes.DirectByteSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

/**
 * UnorderedVarcharMap is an off-heap hash table with single varchar key used
 * to store intermediate data of group by, sample by queries. It provides {@link MapKey} and
 * {@link MapValue}, as well as {@link RecordCursor} interfaces for data access and modification.
 * The preferred way to create an UnorderedVarcharMap is {@link MapFactory}.
 * <p>
 * Map iteration provided by {@link RecordCursor} does not preserve the key insertion order, hence
 * the unordered map name.
 * <strong>Important!</strong>
 * Key and value structures must match the ones provided via lists of columns ({@link ColumnTypes})
 * to the map constructor. Later put* calls made on {@link MapKey} and {@link MapValue} must match
 * the declared column types to guarantee memory access safety.
 * <p>
 * Key must be single varchar. Only insertions and updates operations are
 * supported meaning that a key can't be removed from the map once it was inserted.
 * <p>
 * The hash table is organized in off-heap memory for key-value pairs. It's a hash table with
 * open addressing. The hash table uses linear probing.
 * <p>
 * Key-value pairs stored in the hash table may have the following layout:
 * <pre>
 * | Hash code 32 LSBs |    Size    | Varchar pointer | Value columns 0..V |
 * +-------------------+------------+-----------------+--------------------+
 * |       4 bytes     |  4 bytes   |     8 bytes     |         -          |
 * +-------------------+------------+-----------------+--------------------+
 * </pre>
 * Length is a 30-bit integer, the top 2 bits are reserved for flags. The flags are:
 * <ul>
 *     <li>Bit 31 (the highest bit, indexing from zero): ascii flag, this is also set for empty and null sequences</li>
 *     <li>Bit 30: null bit</li>
 * </ul>
 * The highest bit of the varchar pointer is set when the pointer is unstable, i.e. it points to our own arena
 * memory. The lower 63 bits are the actual pointer value.
 */
public class UnorderedVarcharMap implements Map, Reopenable {
    static final byte FLAG_IS_ASCII = (byte) (1 << 7);
    static final byte FLAG_IS_NULL = (byte) (1 << 6);
    static final long KEY_SIZE = 2 * Long.BYTES;
    static final int MASK_FLAGS_FROM_SIZE = 0x3FFFFFFF; // clear top 2 bits: ascii and null
    static final long PTR_UNSTABLE_MASK = 0x8000000000000000L; // 63 bits
    static final long PTR_MASK = ~PTR_UNSTABLE_MASK;
    static final int SIZE_IS_NULL = 1 << 30;
    private static final int KEY_SINK_INITIAL_CAPACITY = 16;
    private static final long MAX_SAFE_INT_POW_2 = 1L << 31;
    private static final int MIN_KEY_CAPACITY = 16;
    private final GroupByAllocator allocator;
    private final UnorderedVarcharMapCursor cursor;
    private final long entrySize;
    private final Key key;
    private final DirectByteSink keySink; // Used to copy keys with unstable pointers.
    private final double loadFactor;
    private final int maxResizes;
    private final int memoryTag;
    private final UnorderedVarcharMapRecord record;
    private final UnorderedVarcharMapValue value;
    private final UnorderedVarcharMapValue value2;
    private final UnorderedVarcharMapValue value3;
    private int free;
    private int initialKeyCapacity;
    private int keyCapacity;
    private long mask;
    private long memLimit; // Hash table memory limit pointer.
    private long memStart; // Hash table memory start pointer.
    private int nResizes;
    private int size = 0;

    public UnorderedVarcharMap(
            @Transient @Nullable ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes,
            long allocatorDefaultChunkSize,
            long allocatorMaxChunkSize
    ) {
        this(valueTypes, keyCapacity, loadFactor, maxResizes, MemoryTag.NATIVE_UNORDERED_MAP, allocatorDefaultChunkSize, allocatorMaxChunkSize);
    }

    UnorderedVarcharMap(
            @Nullable @Transient ColumnTypes valueTypes,
            int keyCapacity,
            double loadFactor,
            int maxResizes,
            int memoryTag,
            long allocatorDefaultChunkSize,
            long allocatorMaxChunkSize
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

            this.entrySize = Bytes.align8b(KEY_SIZE + valueSize);
            final long sizeBytes = entrySize * this.keyCapacity;
            memStart = Unsafe.malloc(sizeBytes, memoryTag);
            Vect.memset(memStart, sizeBytes, 0);
            memLimit = memStart + sizeBytes;
            keySink = new DirectByteSink(KEY_SINK_INITIAL_CAPACITY, memoryTag);

            value = new UnorderedVarcharMapValue(valueSize, valueOffsets);
            value2 = new UnorderedVarcharMapValue(valueSize, valueOffsets);
            value3 = new UnorderedVarcharMapValue(valueSize, valueOffsets);

            record = new UnorderedVarcharMapRecord(valueSize, valueOffsets, value, valueTypes);
            cursor = new UnorderedVarcharMapCursor(record, this);
            key = new Key();
            allocator = new FastGroupByAllocator(allocatorDefaultChunkSize, allocatorMaxChunkSize, false);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    // public for testing
    public static long packHashSizeFlags(long hash, int size, byte flags) {
        // size must be within 30 bits
        assert size < (1 << 30);
        // flags must have lower 6 bits clear
        assert (flags & 0x3f) == 0;

        int sizeAndFlags = size | (flags << 24);
        return ((long) sizeAndFlags << 32) | (hash & 0xffffffffL);
    }

    // public for testing
    public static long unpackHash(long hashSizeFlags) {
        return hashSizeFlags & 0xffffffffL;
    }

    @Override
    public void clear() {
        free = (int) (keyCapacity * loadFactor);
        size = 0;
        nResizes = 0;
        Vect.memset(memStart, memLimit - memStart, 0);
        Misc.free(allocator); // free all memory, but allocator remains usable for further allocations
    }

    @Override
    public void close() {
        if (memStart != 0) {
            memLimit = memStart = Unsafe.free(memStart, memLimit - memStart, memoryTag);
            free = 0;
            size = 0;
        }
        Misc.free(keySink);
        Misc.free(allocator);
    }

    @Override
    public MapRecordCursor getCursor() {
        return cursor.init(memStart, memLimit, size);
    }

    @Override
    public long getHeapSize() {
        return memLimit - memStart + allocator.allocated();
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
    public boolean isOpen() {
        return memStart != 0;
    }

    @Override
    public void merge(Map srcMap, MapValueMergeFunction mergeFunc) {
        assert this != srcMap;
        long srcMapSize = srcMap.size();
        if (srcMapSize == 0) {
            return;
        }
        UnorderedVarcharMap srcVarcharMap = (UnorderedVarcharMap) srcMap;

        OUTER:
        for (long srcAddr = srcVarcharMap.memStart; srcAddr < srcVarcharMap.memLimit; srcAddr += entrySize) {
            long srcHashSizeFlags = Unsafe.getUnsafe().getLong(srcAddr);
            if (srcHashSizeFlags == 0) {
                continue;
            }

            long srcPtrWithUnstableFlags = Unsafe.getUnsafe().getLong(srcAddr + 8);
            int srcSize = unpackSize(srcHashSizeFlags);

            // mask is guaranteed to have 32 or fewer bits sets -> we can use the stored 32 LSBs of the
            // hash since the higher bits will be masked out and won't contribute to destAddr calculation
            long srcHash = unpackHash(srcHashSizeFlags);
            long destAddr = getStartAddress(srcHash & mask);

            for (; ; ) {
                long dstHashSizeFlags = Unsafe.getUnsafe().getLong(destAddr);
                if (dstHashSizeFlags == 0) {
                    break;
                } else if (dstHashSizeFlags == srcHashSizeFlags) {
                    // lower 32 bits of hash, size, and flags match, let's compare keys.
                    long dstPtrWithUnstableFlags = Unsafe.getUnsafe().getLong(destAddr + 8);

                    if (Vect.memeq(srcPtrWithUnstableFlags & PTR_MASK, dstPtrWithUnstableFlags & PTR_MASK, srcSize)) {
                        // Match found, merge values.
                        mergeFunc.merge(
                                valueAt(destAddr),
                                srcVarcharMap.valueAt(srcAddr)
                        );
                        continue OUTER;
                    }
                }
                destAddr = getNextAddress(destAddr);
            }
            // dst key not found, insert src key-value pair
            if ((srcPtrWithUnstableFlags & PTR_UNSTABLE_MASK) == 0) {
                // stable pointer
                Vect.memcpy(destAddr, srcAddr, entrySize);
            } else {
                // unstable pointer, copy key to our memory
                long arenaPtr = allocator.malloc(srcSize);
                Vect.memcpy(arenaPtr, srcPtrWithUnstableFlags & PTR_MASK, srcSize);
                long arenaPtrWithUnstableFlags = arenaPtr | PTR_UNSTABLE_MASK;
                Unsafe.getUnsafe().putLong(destAddr, srcHashSizeFlags);
                Unsafe.getUnsafe().putLong(destAddr + 8, arenaPtrWithUnstableFlags);

                // copy value
                Vect.memcpy(destAddr + KEY_SIZE, srcAddr + KEY_SIZE, entrySize - KEY_SIZE);
            }
            size++;
            if (--free == 0) {
                rehash();
            }
        }
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
            // handles both mem and offsets
            restoreInitialCapacity();
        }
    }

    @Override
    public void restoreInitialCapacity() {
        if (memStart == 0 || keyCapacity != initialKeyCapacity) {
            keyCapacity = initialKeyCapacity;
            mask = keyCapacity - 1;
            final long sizeBytes = entrySize * keyCapacity;
            if (memStart == 0) {
                memStart = Unsafe.malloc(sizeBytes, memoryTag);
            } else {
                memStart = Unsafe.realloc(memStart, memLimit - memStart, sizeBytes, memoryTag);
            }
            memLimit = memStart + sizeBytes;

            keySink.reopen();
        }

        clear();
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
        return size;
    }

    @Override
    public MapValue valueAt(long startAddress) {
        return valueOf(startAddress, false, value);
    }

    @Override
    public MapKey withKey() {
        return key.init();
    }

    private UnorderedVarcharMapValue asNew(
            long startAddress,
            long hash,
            long keyPtrWithUnstableFlag,
            int keySize,
            long keyHashSizeFlags,
            UnorderedVarcharMapValue value
    ) {
        Unsafe.getUnsafe().putLong(startAddress, keyHashSizeFlags);
        if ((keyPtrWithUnstableFlag & PTR_UNSTABLE_MASK) == 0) {
            // stable pointer
            Unsafe.getUnsafe().putLong(startAddress + 8L, keyPtrWithUnstableFlag);
        } else {
            // unstable pointer, copy key to our memory
            long arenaPtr = allocator.malloc(keySize);
            Vect.memcpy(arenaPtr, keyPtrWithUnstableFlag & PTR_MASK, keySize);
            long arenaPtrWithUnstableFlags = arenaPtr | PTR_UNSTABLE_MASK;
            Unsafe.getUnsafe().putLong(startAddress + 8, arenaPtrWithUnstableFlags);
        }
        if (--free == 0) {
            rehash();
            // Index may have changed after rehash, so we need to find the key.
            startAddress = getStartAddress(hash & mask);
            long ptr = keyPtrWithUnstableFlag & PTR_MASK;
            for (; ; ) {
                long loadedHashSizeFlags = Unsafe.getUnsafe().getLong(startAddress);
                if (loadedHashSizeFlags == keyHashSizeFlags) {
                    long entryPtrWithUnstableFlag = Unsafe.getUnsafe().getLong(startAddress + 8);
                    if (Vect.memeq(entryPtrWithUnstableFlag & PTR_MASK, ptr, keySize)) {
                        break;
                    }
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
        return getStartAddress(memStart, index);
    }

    private UnorderedVarcharMapValue probe0(
            long startAddress,
            long hash,
            long ptrWithUnstableFlag,
            int size,
            long packedHashSizeFlagsToFind,
            UnorderedVarcharMapValue value
    ) {
        long ptr = ptrWithUnstableFlag & PTR_MASK;
        for (; ; ) {
            startAddress = getNextAddress(startAddress);
            long loadedHashSizeFlags = Unsafe.getUnsafe().getLong(startAddress);
            if (loadedHashSizeFlags == 0) {
                return asNew(startAddress, hash, ptrWithUnstableFlag, size, packedHashSizeFlagsToFind, value);
            }
            if (loadedHashSizeFlags == packedHashSizeFlagsToFind) {
                long currentEntryPtr = Unsafe.getUnsafe().getLong(startAddress + 8) & PTR_MASK;
                if (Vect.memeq(currentEntryPtr, ptr, size)) {
                    return valueOf(startAddress, false, value);
                }
            }
        }
    }

    private UnorderedVarcharMapValue probeReadOnly(long startAddress, long ptr, long size, long packedHashSizeFlagsToFind, UnorderedVarcharMapValue value) {
        for (; ; ) {
            startAddress = getNextAddress(startAddress);
            long loadedHashSizeFlags = Unsafe.getUnsafe().getLong(startAddress);
            if (loadedHashSizeFlags == 0) {
                return null;
            }
            if (loadedHashSizeFlags == packedHashSizeFlagsToFind) {
                long currentEntryPtr = Unsafe.getUnsafe().getLong(startAddress + 8) & PTR_MASK;
                if (Vect.memeq(currentEntryPtr, ptr, size)) {
                    return valueOf(startAddress, false, value);
                }
            }
        }
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

        final long newSizeBytes = entrySize * newKeyCapacity;
        final long newMemStart = Unsafe.malloc(newSizeBytes, memoryTag);
        final long newMemLimit = newMemStart + newSizeBytes;
        Vect.memset(newMemStart, newSizeBytes, 0);
        final int newMask = (int) newKeyCapacity - 1;

        for (long addr = memStart; addr < memLimit; addr += entrySize) {
            long packedHashSizeFlags = Unsafe.getUnsafe().getLong(addr);
            if (packedHashSizeFlags == 0) {
                continue;
            }

            int hash = Unsafe.getUnsafe().getInt(addr);
            long newAddr = getStartAddress(newMemStart, hash & newMask);
            while (Unsafe.getUnsafe().getLong(newAddr) != 0) {
                newAddr += entrySize;
                if (newAddr >= newMemLimit) {
                    newAddr = newMemStart;
                }
            }
            Vect.memcpy(newAddr, addr, entrySize);
        }

        Unsafe.free(memStart, memLimit - memStart, memoryTag);

        memStart = newMemStart;
        memLimit = newMemStart + newSizeBytes;
        mask = newMask;
        free += (int) ((newKeyCapacity - keyCapacity) * loadFactor);
        keyCapacity = (int) newKeyCapacity;
        nResizes++;
    }

    private UnorderedVarcharMapValue valueOf(long startAddress, boolean newValue, UnorderedVarcharMapValue value) {
        return value.of(startAddress, memLimit, newValue);
    }

    static boolean isAscii(byte flags) {
        return (flags & FLAG_IS_ASCII) != 0;
    }

    static boolean isNull(byte flags) {
        return (flags & FLAG_IS_NULL) != 0;
    }

    static boolean isSizeNull(int sizeAndFlags) {
        return (sizeAndFlags & SIZE_IS_NULL) != 0;
    }

    static byte unpackFlags(long packedHashSizeFlags) {
        return (byte) (packedHashSizeFlags >>> 56);
    }

    static int unpackSize(long packedHashSizeFlags) {
        int sizeAndFlags = (int) (packedHashSizeFlags >>> 32);

        // clear top 2 bits
        return sizeAndFlags & MASK_FLAGS_FROM_SIZE;
    }

    long entrySize() {
        return entrySize;
    }

    boolean isZeroKey(long startAddress) {
        long packedHashAndSize = Unsafe.getUnsafe().getLong(startAddress);
        return packedHashAndSize == 0;
    }

    class Key implements MapKey {
        private byte flags;
        private long ptrWithUnstableFlag;
        private int size;

        @Override
        public long commit() {
            return KEY_SIZE; // we don't need to track the actual key size
        }

        @Override
        public void copyFrom(MapKey srcKey) {
            Key srcVarcharKey = (Key) srcKey;
            size = srcVarcharKey.size;
            flags = srcVarcharKey.flags;
            if ((srcVarcharKey.ptrWithUnstableFlag & PTR_UNSTABLE_MASK) == 0) {
                // stable pointer
                ptrWithUnstableFlag = srcVarcharKey.ptrWithUnstableFlag;
            } else {
                // unstable pointer, copy key to key memory
                keySink.clear();
                long srcPtr = srcVarcharKey.ptrWithUnstableFlag & PTR_MASK;
                keySink.put(srcPtr, srcPtr + size);
                long ptr = keySink.ptr();
                ptrWithUnstableFlag = ptr | PTR_UNSTABLE_MASK;
            }
        }

        @Override
        public MapValue createValue() {
            long hash = Hash.hashMem64(ptrWithUnstableFlag & PTR_MASK, size);
            return createValue(hash);
        }

        @Override
        public MapValue createValue(long hashCode) {
            long index = hashCode & mask;
            long startAddress = getStartAddress(index);

            long loadedHashSizeFlags = Unsafe.getUnsafe().getLong(startAddress);
            long currentHashSizeFlags = packHashSizeFlags(hashCode, size, flags);
            if (loadedHashSizeFlags == 0) {
                return asNew(startAddress, hashCode, ptrWithUnstableFlag, size, currentHashSizeFlags, value);
            }
            if (loadedHashSizeFlags == currentHashSizeFlags) {
                long currentPtr = Unsafe.getUnsafe().getLong(startAddress + 8) & PTR_MASK;
                if (Vect.memeq(currentPtr, ptrWithUnstableFlag & PTR_MASK, size)) {
                    return valueOf(startAddress, false, value);
                }
            }
            return probe0(startAddress, hashCode, ptrWithUnstableFlag, size, currentHashSizeFlags, value);
        }

        @Override
        public MapValue findValue() {
            return findValue(ptrWithUnstableFlag, size, flags, value);
        }

        @Override
        public MapValue findValue2() {
            return findValue(ptrWithUnstableFlag, size, flags, value2);
        }

        @Override
        public MapValue findValue3() {
            return findValue(ptrWithUnstableFlag, size, flags, value3);
        }

        @Override
        public long hash() {
            return Hash.hashMem64(ptrWithUnstableFlag & PTR_MASK, size);
        }

        public Key init() {
            // no-op since all fields are set in putVarchar().
            // Q: What if a user does not call putVarchar()?
            // A: It's illegal to use an uninitialized key and will result in undefined behavior.
            return this;
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
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
        }

        @Override
        public void putVarchar(Utf8Sequence value) {
            if (value == null) {
                size = 0;
                ptrWithUnstableFlag = 0;
                // set the 2 top bits to indicate null value (+ascii flag)
                flags = FLAG_IS_NULL | FLAG_IS_ASCII;
            } else {
                size = value.size();
                if (value.isStable()) {
                    ptrWithUnstableFlag = value.ptr();
                } else {
                    // unstable pointer, copy key to key memory
                    keySink.clear();
                    // Try to use Vect#memcpy() if possible.
                    if (value.ptr() != -1) {
                        keySink.put(value.ptr(), value.ptr() + size);
                    } else {
                        keySink.put(value);
                    }
                    long ptr = keySink.ptr();
                    ptrWithUnstableFlag = ptr | PTR_UNSTABLE_MASK;
                }
                flags = value.isAscii() ? FLAG_IS_ASCII : 0;
            }

            // Empty string must have the ascii flag set to true, otherwise we won't be able to differentiate
            // between a missing entry and an empty string.
            // The flags are encoded into the high bits in the size field so an empty string will still have a non-zero size stored.
            assert size > 0 || (flags & FLAG_IS_ASCII) != 0;
        }

        @Override
        public void skip(int bytes) {
            // no-op
        }

        private MapValue findValue(long ptrWithUnstableFlag, int size, byte flags, UnorderedVarcharMapValue value) {
            long ptr = ptrWithUnstableFlag & PTR_MASK;
            long hash = Hash.hashMem64(ptr, size);
            long index = hash & mask;
            long startAddress = getStartAddress(index);

            long loadedHashSizeFlags = Unsafe.getUnsafe().getLong(startAddress);
            if (loadedHashSizeFlags == 0) {
                return null;
            }
            long packedHashSizeFlags = packHashSizeFlags(hash, size, flags);
            if (loadedHashSizeFlags == packedHashSizeFlags) {
                long currentPtr = Unsafe.getUnsafe().getLong(startAddress + 8) & PTR_MASK;
                if (Vect.memeq(currentPtr, ptr, size)) {
                    return valueOf(startAddress, false, value);
                }
            }
            return probeReadOnly(startAddress, ptr, size, packedHashSizeFlags, value);
        }

        void copyFromStartAddress(long address) {
            long srcPackedHashAndSize = Unsafe.getUnsafe().getLong(address);
            byte srcFlags = UnorderedVarcharMap.unpackFlags(srcPackedHashAndSize);
            int srcSize = UnorderedVarcharMap.unpackSize(srcPackedHashAndSize);
            long srcPtrWithUnstableFlag = Unsafe.getUnsafe().getLong(address + Long.BYTES);

            if ((srcPtrWithUnstableFlag & PTR_UNSTABLE_MASK) == 0) {
                // stable pointer
                ptrWithUnstableFlag = srcPtrWithUnstableFlag;
            } else {
                // unstable pointer, copy key to key memory
                keySink.clear();
                long srcPtr = srcPtrWithUnstableFlag & PTR_MASK;
                keySink.put(srcPtr, srcPtr + srcSize);
                long ptr = keySink.ptr();
                ptrWithUnstableFlag = ptr | PTR_UNSTABLE_MASK;
            }

            size = srcSize;
            flags = srcFlags;
        }
    }
}
