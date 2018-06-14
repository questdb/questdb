/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo.map;

import com.questdb.cairo.CairoException;
import com.questdb.cairo.ColumnTypes;
import com.questdb.cairo.TableUtils;
import com.questdb.cairo.VirtualMemory;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.common.ColumnType;
import com.questdb.std.*;

import java.io.Closeable;

/**
 * Storage structure to support queries such as "select distinct ...",
 * group by queries and analytic functions. It can be thought of as a
 * hash map with composite keys and values. Composite key is allowed
 * to contain any number of fields of any type. In practice key will
 * be a record of columns, including both variable-length (string and binary)
 * and fixed-length types. Composite values can be any combination of
 * fixed-length types only.
 * <p>
 * To construct QMap exact types of keys and values are not
 * required. Only counts of each, e.g. 'x' number of keys and 'y' number
 * of values. Each map entry will consists of a number of 8-byte slots,
 * which is equal to sum of these key and value counts. That is value
 * followed by key slots. Fixed-length values will be written directly
 * to slots. Variable-length values will be appended at end of parent
 * entry and value offset, relative to entry, will be written to
 * corresponding slot.
 * <p>
 * QMap uses open addressing to keep track of entry offsets. Key hash
 * code determines bucket. Entries in the same bucket are stored
 * as mono-directional linked list. In this list the reference part
 * is a one-byte distance from parent to the next list entry. The
 * value of this byte is an index in fixed jump distance table. QMap
 * also provides and maintains guarantee that each hash code root
 * entry will be stored in bucket, which can be computed directly from
 * this hash code.
 */
public class QMap implements Closeable {
    public static final byte BITS_DIRECT_HIT = (byte) 0b10000000;
    public static final byte BITS_DISTANCE = 0b01111111;
    public static final int jumpDistancesLen = 126;
    static final int ENTRY_HEADER_SIZE = 9;
    private static final long[] jumpDistances =
            {
                    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,

                    21, 28, 36, 45, 55, 66, 78, 91, 105, 120, 136, 153, 171, 190, 210, 231,
                    253, 276, 300, 325, 351, 378, 406, 435, 465, 496, 528, 561, 595, 630,
                    666, 703, 741, 780, 820, 861, 903, 946, 990, 1035, 1081, 1128, 1176,
                    1225, 1275, 1326, 1378, 1431, 1485, 1540, 1596, 1653, 1711, 1770, 1830,
                    1891, 1953, 2016, 2080, 2145, 2211, 2278, 2346, 2415, 2485, 2556,

                    3741, 8385, 18915, 42486, 95703, 215496, 485605, 1091503, 2456436,
                    5529475, 12437578, 27986421, 62972253, 141700195, 318819126, 717314626,
                    1614000520, 3631437253L, 8170829695L, 18384318876L, 41364501751L, 93070021080L, 209407709220L,
                    471167588430L, 1060127437995L, 2385287281530L, 5366895564381L, 12075513791265L, 27169907873235L,
                    61132301007778L, 137547673121001L, 309482258302503L, 696335090510256L, 1566753939653640L,
                    3525196427195653L, 7931691866727775L, 17846306747368716L, 40154190394120111L, 90346928493040500L,
                    203280588949935750L, 457381324898247375L, 1029107980662394500L, 2315492957028380766L,
                    5209859150892887590L
            };
    private static final HashFunction DEFAULT_HASH = (mem, offset, size) -> {
        final long n = size - size % 2;

        long h = 0;
        for (long i = 0; i < n; i += 2) {
            h = (h << 5) - h + mem.getShort(offset + i);
        }

        if (n > 0) {
            h = (h << 5) - h + mem.getByte(offset + size - 1);
        }
        return h;
    };

    private final VirtualMemory entries;
    private final VirtualMemory entrySlots;
    private final Key key = new Key();
    private final Value value = new ValueImpl();
    private final double loadFactor;
    private final long entryFixedSize;
    private final HashFunction hashFunction;
    private final QMapCursor cursor;
    private final long columnOffsets[];
    private final long entryKeyOffset;
    private RecordWriter recordWriter = null;
    private long currentEntryOffset;
    private long currentEntrySize = 0;
    private long keyCapacity;
    private long mask;
    private long size;
    private long currentValueOffset;
    private int valueColumnCount;

    public QMap(int pageSize, ColumnTypes keyTypes, ColumnTypes valueTypes, long keyCapacity, double loadFactor) {
        this(pageSize, keyTypes, valueTypes, keyCapacity, loadFactor, DEFAULT_HASH);
    }

    QMap(int pageSize, ColumnTypes keyTypes, ColumnTypes valueTypes, long keyCapacity, double loadFactor, HashFunction hashFunction) {
        this.entries = new VirtualMemory(pageSize);
        this.entrySlots = new VirtualMemory(pageSize);
        this.loadFactor = loadFactor;
        this.columnOffsets = new long[keyTypes.getColumnCount() + valueTypes.getColumnCount()];

        // todo: reduce storage if there are no binary columns
        this.valueColumnCount = valueTypes.getColumnCount();
        this.entryFixedSize = calcColumnOffsets(keyTypes, calcColumnOffsets(valueTypes, ENTRY_HEADER_SIZE, 0), this.valueColumnCount);
        this.entryKeyOffset = columnOffsets[valueColumnCount];
        this.keyCapacity = Math.max(keyCapacity, 16);
        this.hashFunction = hashFunction;
        configureCapacity();
        this.cursor = new QMapCursor(entries, columnOffsets);
    }

    private long calcColumnOffsets(ColumnTypes valueTypes, long startOffset, int startPosition) {
        long o = startOffset;
        for (int i = 0, n = valueTypes.getColumnCount(); i < n; i++) {
            int sz;
            switch (valueTypes.getColumnType(i)) {
                case ColumnType.BOOLEAN:
                case ColumnType.BYTE:
                    sz = 1;
                    break;
                case ColumnType.DOUBLE:
                case ColumnType.LONG:
                case ColumnType.DATE:
                case ColumnType.TIMESTAMP:
                    sz = 8;
                    break;
                case ColumnType.FLOAT:
                case ColumnType.INT:
                    sz = 4;
                    break;
                case ColumnType.SHORT:
                    sz = 2;
                    break;
                case ColumnType.STRING:
                case ColumnType.BINARY:
                    sz = 8;
                    break;
                default:
                    throw CairoException.instance(0).put("Unsupported column type: ").put(ColumnType.nameOf(valueTypes.getColumnType(i)));
            }
            columnOffsets[startPosition + i] = o;
            o += sz;
        }
        return o;
    }

    public QMapCursor getCursor() {
        cursor.of(currentEntryOffset + currentEntrySize);
        return cursor;
    }

    public void configureKeyAdaptor(
            @Transient BytecodeAssembler asm,
            @Transient RecordMetadata meta,
            @Transient IntList columns,
            boolean symbolAsString) {

        asm.init(RecordWriter.class);
        asm.setupPool();
        int thisClassIndex = asm.poolClass(asm.poolUtf8("questdbasm"));
        int interfaceClassIndex = asm.poolClass(RecordWriter.class);

        final int rGetInt = asm.poolInterfaceMethod(Record.class, "getInt", "(I)I");
        final int rGetLong = asm.poolInterfaceMethod(Record.class, "getLong", "(I)J");
        final int rGetDate = asm.poolInterfaceMethod(Record.class, "getDate", "(I)J");
        final int rGetTimestamp = asm.poolInterfaceMethod(Record.class, "getTimestamp", "(I)J");
        final int rGetByte = asm.poolInterfaceMethod(Record.class, "getByte", "(I)B");
        final int rGetShort = asm.poolInterfaceMethod(Record.class, "getShort", "(I)S");
        final int rGetBool = asm.poolInterfaceMethod(Record.class, "getBool", "(I)Z");
        final int rGetFloat = asm.poolInterfaceMethod(Record.class, "getFloat", "(I)F");
        final int rGetDouble = asm.poolInterfaceMethod(Record.class, "getDouble", "(I)D");
        final int rGetStr = asm.poolInterfaceMethod(Record.class, "getStr", "(I)Ljava/lang/CharSequence;");
        final int rGetSym = asm.poolInterfaceMethod(Record.class, "getSym", "(I)Ljava/lang/CharSequence;");
        final int rGetBin = asm.poolInterfaceMethod(Record.class, "getBin", "(I)Lcom/questdb/std/BinarySequence;");
        //
        final int wPutByte = asm.poolMethod(Key.class, "putByte", "(B)V");
        final int wPutShort = asm.poolMethod(Key.class, "putShort", "(S)V");
        final int wPutInt = asm.poolMethod(Key.class, "putInt", "(I)V");
        final int wPutLong = asm.poolMethod(Key.class, "putLong", "(J)V");
        final int wPutBool = asm.poolMethod(Key.class, "putBool", "(Z)V");
        final int wPutFloat = asm.poolMethod(Key.class, "putFloat", "(F)V");
        final int wPutDouble = asm.poolMethod(Key.class, "putDouble", "(D)V");
        final int wPutStr = asm.poolMethod(Key.class, "putStr", "(Ljava/lang/CharSequence;)V");
        final int wPutBin = asm.poolMethod(Key.class, "putBin", "(Lcom/questdb/std/BinarySequence;)V");
        //
        final int copyNameIndex = asm.poolUtf8("copy");
        final int copySigIndex = asm.poolUtf8("(Lcom/questdb/cairo/sql/Record;Lcom/questdb/cairo/map/QMap$Key;)V");

        asm.finishPool();
        asm.defineClass(thisClassIndex);
        asm.interfaceCount(1);
        asm.putShort(interfaceClassIndex);
        asm.fieldCount(0);
        asm.methodCount(2);
        asm.defineDefaultConstructor();

        asm.startMethod(copyNameIndex, copySigIndex, 4, 3);

        final int n = columns.size();
        for (int i = 0; i < n; i++) {

            int index = columns.getQuick(i);
            asm.aload(2);
            asm.aload(1);
            asm.iconst(index);

            switch (meta.getColumnType(index)) {
                case ColumnType.INT:
                    asm.invokeInterface(rGetInt, 1);
                    asm.invokeVirtual(wPutInt);
                    break;
                case ColumnType.SYMBOL:
                    if (symbolAsString) {
                        asm.invokeInterface(rGetSym, 1);
                        asm.invokeVirtual(wPutStr);
                    } else {
                        asm.invokeInterface(rGetInt, 1);
                        asm.i2l();
                        asm.invokeVirtual(wPutLong);
                    }
                    break;
                case ColumnType.LONG:
                    asm.invokeInterface(rGetLong, 1);
                    asm.invokeVirtual(wPutLong);
                    break;
                case ColumnType.DATE:
                    asm.invokeInterface(rGetDate, 1);
                    asm.invokeVirtual(wPutLong);
                    break;
                case ColumnType.TIMESTAMP:
                    asm.invokeInterface(rGetTimestamp, 1);
                    asm.invokeVirtual(wPutLong);
                    break;
                case ColumnType.BYTE:
                    asm.invokeInterface(rGetByte, 1);
                    asm.invokeVirtual(wPutByte);
                    break;
                case ColumnType.SHORT:
                    asm.invokeInterface(rGetShort, 1);
                    asm.invokeVirtual(wPutShort);
                    break;
                case ColumnType.BOOLEAN:
                    asm.invokeInterface(rGetBool, 1);
                    asm.invokeVirtual(wPutBool);
                    break;
                case ColumnType.FLOAT:
                    asm.invokeInterface(rGetFloat, 1);
                    asm.invokeVirtual(wPutFloat);
                    break;
                case ColumnType.DOUBLE:
                    asm.invokeInterface(rGetDouble, 1);
                    asm.invokeVirtual(wPutDouble);
                    break;
                case ColumnType.STRING:
                    asm.invokeInterface(rGetStr, 1);
                    asm.invokeVirtual(wPutStr);
                    break;
                default:
                    // binary
                    asm.invokeInterface(rGetBin, 1);
                    asm.invokeVirtual(wPutBin);
                    break;
            }
        }

        asm.return_();
        asm.endMethodCode();

        // exceptions
        asm.putShort(0);

        // we have to add stack map table as branch target
        // jvm requires it

        // attributes: 0 (void, no stack verification)
        asm.putShort(0);

        asm.endMethod();

        // class attribute count
        asm.putShort(0);

        recordWriter = asm.newInstance();
    }

    @Override
    public void close() {
        entries.close();
        entrySlots.close();
    }

    public Key withKey() {
        currentEntryOffset = currentEntryOffset + currentEntrySize;
        entries.jumpTo(currentEntryOffset + columnOffsets[valueColumnCount]);

        // each entry cell is 8-byte value, which either holds cell value
        // or reference, relative to entry start, where value is kept in size-prefixed format
        // Variable size key cells are stored right behind entry.
        // Value (as in key-Value pair) is stored first. It is always fixed length and when
        // it is out of the way we can calculate key hash on contiguous memory.

        // entry actual size always starts with sum of fixed size columns we have
        // and may grow when we add variable key values.
        currentEntrySize = entryFixedSize;

        // make sure values are not read or written before we finish with key
        currentValueOffset = -1;
        return key;
    }

    public long size() {
        return size;
    }

    private void configureCapacity() {
        this.mask = Numbers.ceilPow2((long) (keyCapacity / loadFactor)) - 1;
        entrySlots.jumpTo((mask + 1) * 8);
        entrySlots.zero();
    }

    long getActualCapacity() {
        return mask + 1;
    }

    long getAppendOffset() {
        return currentEntryOffset + currentEntrySize;
    }

    long getKeyCapacity() {
        return keyCapacity;
    }


    public interface Value {
        byte getByte(int column);

        double getDouble(int column);

        int getInt(int column);

        long getLong(int column);

        short getShort(int column);

        boolean isNew();

        Value putDouble(double value);

        Value putLong(long value);
    }

    @FunctionalInterface
    public interface HashFunction {
        long hash(VirtualMemory mem, long offset, long size);
    }

    @FunctionalInterface
    private interface RecordWriter {
        void copy(Record record, Key key);
    }

    private class ValueImpl implements Value {

        private long getValueColumnOffset(int columnIndex) {
            return currentValueOffset + Unsafe.arrayGet(columnOffsets, columnIndex);
        }

        @Override
        public byte getByte(int columnIndex) {
            assert currentValueOffset != -1;
            return entries.getByte(getValueColumnOffset(columnIndex));
        }

        @Override
        public double getDouble(int columnIndex) {
            assert currentValueOffset != -1;
            return entries.getDouble(getValueColumnOffset(columnIndex));
        }

        @Override
        public int getInt(int columnIndex) {
            assert currentValueOffset != -1;
            return entries.getInt(getValueColumnOffset(columnIndex));
        }

        @Override
        public long getLong(int columnIndex) {
            assert currentValueOffset != -1;
            return entries.getLong(getValueColumnOffset(columnIndex));
        }

        @Override
        public short getShort(int columnIndex) {
            assert currentValueOffset != -1;
            return entries.getShort(getValueColumnOffset(columnIndex));
        }

        @Override
        public boolean isNew() {
            return currentEntrySize != 0;
        }

        @Override
        public Value putDouble(double value) {
            assert currentValueOffset != -1;
            entries.putDouble(value);
            return this;
        }

        @Override
        public Value putLong(long value) {
            assert currentValueOffset != -1;
            entries.putLong(value);
            return this;
        }
    }

    public class Key {

        public void putRecord(Record record) {
            recordWriter.copy(record, key);
        }

        public Value createValue() {
            long slot = calculateEntrySlot(currentEntryOffset, currentEntrySize);
            long offset = getOffsetAt(slot);

            if (offset == -1) {
                // great, slot is empty, create new entry as direct hit
                return putNewEntryAt(currentEntryOffset, currentEntrySize, slot, BITS_DIRECT_HIT);
            }

            // check if this was a direct hit
            final byte flag = entries.getByte(offset);
            if ((flag & BITS_DIRECT_HIT) == 0) {
                // this is not a direct hit slot, reshuffle entries to free this slot up
                // then create new entry here with direct hit flag
                // we don't have to compare keys, because this isn't our hash code

                // steps to take
                // 1. find parent of this rogue entry: compute hash code on key, find direct hit entry and
                //    descend until we find the sucker just above this one. We need this in order to change
                //    distance byte to keep structure consistent
                // 2. Find empty slot from parent
                // 3. Move current entry there
                // 4. For next entry - current will be parent
                // 5. Find empty slot from new parent
                // 6. Move entry there
                // 7. etc
                // as we shuffle these things we have to be careful not to use
                // entry we originally set out to free

                if (moveForeignEntries(slot, offset)) {
                    return putNewEntryAt(currentEntryOffset, currentEntrySize, slot, BITS_DIRECT_HIT);
                }

                grow();
                return createValue();
            }

            // this is direct hit, scroll down all keys with same hashcode
            // and exit this loop as soon as equality operator scores
            // in simple terms check key equality on this key
            if (cmp(offset)) {
                return found(offset);
            }

            return appendEntry(offset, slot, flag);
        }

        public Value findValue() {
            if (currentValueOffset == -1) {
                long slot = calculateEntrySlot(currentEntryOffset, currentEntrySize);
                long offset = getOffsetAt(slot);

                if (offset == -1) {
                    return null;
                } else {
                    // check if this was a direct hit
                    byte flag = entries.getByte(offset);
                    if ((flag & BITS_DIRECT_HIT) == 0) {
                        // not a direct hit? not our value
                        return null;
                    } else {
                        // this is direct hit, scroll down all keys with same hashcode
                        // and exit this loop as soon as equality operator scores

                        // in simple terms check key equality on this key
                        if (cmp(offset)) {
                            return found(offset);
                        } else {

                            // then go down the list until either list ends or we find value
                            int distance = flag & BITS_DISTANCE;
                            while (distance > 0) {
                                slot = nextSlot(slot, distance);
                                offset = getOffsetAt(slot);

                                // this offset cannot be 0 when data structure is consistent
                                assert offset != 0;

                                if (cmp(offset)) {
                                    return found(offset);
                                }
                                distance = entries.getByte(offset) & BITS_DISTANCE;
                            }
                            // reached the end of the list, nothing found
                            return null;
                        }
                    }
                }
            }
            return value;
        }

        public void putDouble(double value) {
            entries.putDouble(value);
        }

        public void putBool(boolean value) {
            entries.putBool(value);
        }

        public void putLong(long value) {
            entries.putLong(value);
        }

        public void putShort(short value) {
            entries.putShort(value);
        }

        public void putByte(byte value) {
            entries.putByte(value);
        }

        public void putInt(int value) {
            entries.putInt(value);
        }

        public void putFloat(float value) {
            entries.putFloat(value);
        }

        public void putStr(CharSequence value) {
            if (value == null) {
                entries.putLong(TableUtils.NULL_LEN);
            } else {
                // offset of string value relative to record start
                entries.putLong(currentEntrySize);
                long o = entries.getAppendOffset();
                entries.jumpTo(currentEntryOffset + currentEntrySize);
                entries.putStr(value);
                currentEntrySize += VirtualMemory.getStorageLength(value);
                entries.jumpTo(o);
            }
        }

        public void putBin(BinarySequence value) {
            if (value == null) {
                entries.putLong(TableUtils.NULL_LEN);
            } else {
                entries.putLong(currentEntrySize);
                long o = entries.getAppendOffset();
                entries.jumpTo(currentEntryOffset + currentEntrySize);
                entries.putBin(value);
                currentEntrySize += 8 + value.length();
                entries.jumpTo(o);
            }
        }


        private Value appendEntry(long offset, long slot, byte flag) {
            int distance = flag & BITS_DISTANCE;
            long original = offset;

            while (distance > 0) {
                slot = nextSlot(slot, distance);
                offset = getOffsetAt(slot);

                // this offset cannot be 0 when data structure is consistent
                assert offset != 0;

                if (cmp(offset)) {
                    return found(offset);
                }
                distance = entries.getByte(offset) & BITS_DISTANCE;
            }

            // create entry at "nextOffset"
            distance = findFreeSlot(slot);

            // we must have space here because to get to this place
            // we must have checked available capacity
            // anyway there is a test that ensures that going
            // down the chain will not hit problems.
            assert distance != 0;

            slot = nextSlot(slot, distance);

            // update distance on last entry in linked list
            entries.jumpTo(offset);
            if (original == offset) {
                distance = distance | BITS_DIRECT_HIT;
            }
            entries.putByte((byte) distance);

            // add new entry
            return putNewEntryAt(currentEntryOffset, currentEntrySize, slot, (byte) 0);
        }

        private long calculateEntrySlot(long offset, long size) {
            return hashFunction.hash(entries, offset + entryKeyOffset, size - entryKeyOffset) & mask;
        }

        private boolean cmp(long offset) {
            return cmp(currentEntryOffset + entryKeyOffset, offset + entryKeyOffset, currentEntrySize - entryKeyOffset);
        }

        private boolean cmp(long offset1, long offset2, long size) {
            final long lim = size - size % 8L;

            for (long i = 0; i < lim; i += 8L) {
                if (entries.getLong(offset1 + i) != entries.getLong(offset2 + i)) {
                    return false;
                }
            }

            for (long i = lim; i < size; i++) {
                if (entries.getByte(offset1 + i) != entries.getByte(offset2 + i)) {
                    return false;
                }
            }

            return true;
        }

        // technically we should always have free slots when load factory is less than 1 (which we enforce)
        // however, sometimes these free slots cannot be reached via jump table. This is purely because
        // jump table is limited. When this occurs the caller has to handle 0 distance by re-hashing
        // of all entries and retrying new entry creation.
        private int findFreeSlot(long slot) {
            for (int i = 1; i < jumpDistancesLen; i++) {
                if (entrySlots.getLong(nextSlot(slot, i) * 8) == 0) {
                    return i;
                }
            }
            return 0;
        }

        /**
         * Finds parent of given entry. Entry in question is represented by two attributes: offset and targetSlot
         *
         * @param offset     offset of entry data
         * @param targetSlot slot of entry
         * @return parent slot - offset in entrySlots
         */
        private long findParentSlot(long offset, long targetSlot) {
            long parentSlot = calculateEntrySlot(offset, getEntrySize(offset));

            do {
                final int distance = entries.getByte(getOffsetAt(parentSlot)) & BITS_DISTANCE;
                assert distance != 0;
                final long nextSlot = nextSlot(parentSlot, distance);
                if (nextSlot == targetSlot) {
                    return parentSlot;
                }
                parentSlot = nextSlot;
            } while (true);
        }

        private Value found(long offset) {
            // found key
            // values offset will be
            currentValueOffset = offset;
            entries.jumpTo(currentEntryOffset);
            // undo this key append
            currentEntrySize = 0;
            return value;
        }

        private long getEntrySize(long offset) {
            return entries.getLong(offset + 1);
        }

        private long getOffsetAt(long slot) {
            return entrySlots.getLong(slot * 8) - 1;
        }

        private void grow() {
            // resize offsets virtual memory
            long appendPosition = entries.getAppendOffset();
            try {
                keyCapacity = keyCapacity * 2;
                configureCapacity();
                long target = size;
                long offset = 0L;
                while (target > 0) {
                    final long entrySize = getEntrySize(offset);
                    rehashEntry(offset, entrySize);
                    offset += entrySize;
                    target--;
                }
            } finally {
                entries.jumpTo(appendPosition);
            }
        }

        private boolean moveForeignEntries(final long slot, final long offset) {
            // find parent slot for our direct hit
            long parentSlot = findParentSlot(offset, slot);
            // find entry for the parent slot, we will be updating distance here
            long parentOffset = getOffsetAt(parentSlot);
            long currentSlot = slot;
            long currentOffset = offset;

            while (true) {
                // find where "current" slot is going to
                int dist = findFreeSlot(parentSlot);

                if (dist == 0) {
                    // we are out of space; let parent method know that we have to grow slots and retry
                    return false;
                }

                // update parent entry with its new location
                entries.jumpTo(parentOffset);

                if ((entries.getByte(parentOffset) & BITS_DIRECT_HIT) == 0) {
                    entries.putByte((byte) dist);
                } else {
                    entries.putByte((byte) (dist | BITS_DIRECT_HIT));
                }

                // update slot with current offset
                long nextSlot = nextSlot(parentSlot, dist);
                setOffsetAt(nextSlot, currentOffset);

                // check if the current entry has child
                dist = entries.getByte(currentOffset) & BITS_DISTANCE;

                if (currentSlot != slot) {
                    setOffsetAt(currentSlot, -1);
                }

                if (dist == 0) {
                    // done
                    break;
                }

                // parent of next entry will be current entry
                parentSlot = nextSlot;
                parentOffset = currentOffset;
                currentSlot = nextSlot(currentSlot, dist);
                currentOffset = getOffsetAt(currentSlot);
            }

            return true;
        }

        private long nextSlot(long slot, int distance) {
            return (slot + Unsafe.arrayGet(jumpDistances, distance)) & mask;
        }

        private void putEntryAt(long entryOffset, long slot, byte flag) {
            setOffsetAt(slot, entryOffset);
            entries.jumpTo(entryOffset);
            entries.putByte(flag);
        }

        private Value putNewEntryAt(long entryOffset, long entrySize, long slot, byte flag) {
            // entry size is now known
            // values are always fixed size and already accounted for
            // so go ahead and finalize
            entries.jumpTo(entryOffset);
            entries.putByte(flag);
            entries.putLong(entrySize); // size

            if (++size == keyCapacity) {
                // reached capacity?
                // no need to populate slot, grow() will do the job for us
                grow();
            } else {
                setOffsetAt(slot, entryOffset);
            }
            // this would be offset of entry values
            currentValueOffset = entries.getAppendOffset();
            return value;
        }

        private void rehashEntry(long entryOffset, long currentEntrySize) {
            long slot = calculateEntrySlot(entryOffset, currentEntrySize);
            long offset = getOffsetAt(slot);

            if (offset == -1) {
                // great, slot is empty, create new entry as direct hit
                putEntryAt(entryOffset, slot, BITS_DIRECT_HIT);
            } else {
                // check if this was a direct hit
                final byte flag = entries.getByte(offset);
                if ((flag & BITS_DIRECT_HIT) == 0) {
                    moveForeignEntries(slot, offset);
                    putEntryAt(entryOffset, slot, BITS_DIRECT_HIT);
                } else {
                    // Our entries are now guaranteed to be unique. In case of direct hit we simply append
                    // entry to end of list.
                    int distance = flag & BITS_DISTANCE;
                    long original = offset;

                    while (distance > 0) {
                        slot = nextSlot(slot, distance);
                        distance = entries.getByte(offset = getOffsetAt(slot)) & BITS_DISTANCE;
                    }

                    // create entry at "nextOffset"
                    distance = findFreeSlot(slot);
                    assert distance != 0;
                    slot = nextSlot(slot, distance);

                    // update distance on last entry in linked list
                    entries.jumpTo(offset);
                    if (original == offset) {
                        entries.putByte((byte) (distance | BITS_DIRECT_HIT));
                    } else {
                        entries.putByte((byte) distance);
                    }
                    // add new entry
                    putEntryAt(entryOffset, slot, (byte) 0);
                }
            }
        }

        private void setOffsetAt(long slot, long offset) {
            entrySlots.jumpTo(slot * 8);
            entrySlots.putLong(offset + 1);
        }
    }
}
