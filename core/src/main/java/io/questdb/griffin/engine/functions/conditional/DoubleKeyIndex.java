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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.std.DoubleList;
import io.questdb.std.Hash;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.TestOnly;

import java.util.Arrays;

/**
 * Finds the first of a list of keys that {@link Numbers#equals(double, double)} finds equal to a
 * value, as '=' compares DOUBLE values, in constant time. switch() uses it for a FLOAT or DOUBLE
 * operand.
 * <p>
 * Numbers.equals() takes values within {@link Numbers#DOUBLE_TOLERANCE} of each other as equal, so
 * keys cannot be hashed by their bits. Below 2^30 the index hashes cells of the number line, twice
 * the tolerance wide, and files each key under its own cell and the cells on both sides of it, so
 * the cell of a value holds every key that can equal it. Rounding cannot put two equal values two
 * cells apart: below 2^19 it moves a cell number by at most a quarter, and from 2^19 up adjacent
 * doubles lie more than the tolerance apart, so equal values are identical. From 2^30 up, where
 * cell numbers would overflow a long, the index hashes the bits.
 * <p>
 * Linear probing keeps the slots of one cell in key order, so the first slot that equals a value
 * holds the first key that equals it: the branch the searched form of CASE takes.
 */
public final class DoubleKeyIndex {
    private static final double CELLS_PER_UNIT = 1 / (2 * Numbers.DOUBLE_TOLERANCE);
    // Never a cell: cell numbers stay below 2^30 * CELLS_PER_UNIT < 2^63 in magnitude, and the
    // only double with these bits is -0.0, which is filed under a cell number.
    private static final long EMPTY = Long.MIN_VALUE;
    // NaN and +-Infinity, which Numbers.isNull() reads as NULL, have every exponent bit set
    private static final long EXPONENT_BITS = 0x7FF0000000000000L;
    // the bits of non-negative doubles order as the doubles do
    private static final long GRID_LIMIT_BITS = Double.doubleToRawLongBits(1L << 30);
    // Up to this many keys, comparing with each one costs about as much as a hash lookup when no
    // key matches, and less when one does; SwitchDoubleKeyBenchmark measures both.
    private static final int LINEAR_SCAN_LIMIT = 4;
    private final DoubleList keys = new DoubleList();
    private long[] cells;
    private int[] keyIndexes;
    private double[] keyValues;
    private int mask;
    private int nullKeyIndex = -1;
    private int usedSlots;

    public DoubleKeyIndex() {
        allocate(16);
    }

    /**
     * Appends a key, unless an earlier key equals it.
     *
     * @return the index of the first earlier key that equals this one, or -1 when there is
     * none and the key was added
     */
    public int add(double key) {
        final int equal = indexOf(key);
        if (equal > -1) {
            return equal;
        }
        keys.add(key);
        final int keyIndex = keys.size() - 1;
        if (isNull(Double.doubleToRawLongBits(key))) {
            nullKeyIndex = keyIndex;
        } else if ((usedSlots + 3) * 2 > cells.length) {
            // keep at least half of the slots empty; filing the keys again in key order keeps
            // the slots of each cell in key order
            allocate(cells.length * 2);
            for (int i = 0; i <= keyIndex; i++) {
                file(i);
            }
        } else {
            file(keyIndex);
        }
        return -1;
    }

    /**
     * Returns the index of the first key that equals the value, or -1 when none does.
     */
    public int indexOf(double value) {
        final long bits = Double.doubleToRawLongBits(value);
        if (isNull(bits)) {
            return nullKeyIndex;
        }
        // the value is not NULL, and the distance to a NULL key is NaN or infinite
        final int keyCount = keys.size();
        if (isScanned(keyCount)) {
            for (int i = 0; i < keyCount; i++) {
                if (Math.abs(keys.getQuick(i) - value) <= Numbers.DOUBLE_TOLERANCE) {
                    return i;
                }
            }
            return -1;
        }
        final long cell = lookupCell(value, bits);
        int slot = homeSlot(cell);
        long slotCell;
        while ((slotCell = cells[slot]) != EMPTY) {
            if (slotCell == cell && Math.abs(keyValues[slot] - value) <= Numbers.DOUBLE_TOLERANCE) {
                return keyIndexes[slot];
            }
            slot = (slot + 1) & mask;
        }
        return -1;
    }

    /**
     * Returns how many entries {@link #indexOf(double)} may compare the value with: every key when it
     * scans them, otherwise the full slots from the value's home slot to the next empty one. Tests
     * check that it stays small however many keys there are.
     */
    @TestOnly
    public int probeLength(double value) {
        final long bits = Double.doubleToRawLongBits(value);
        if (isNull(bits)) {
            return 0;
        }
        final int keyCount = keys.size();
        if (isScanned(keyCount)) {
            return keyCount;
        }
        int slot = homeSlot(lookupCell(value, bits));
        int length = 0;
        while (cells[slot] != EMPTY) {
            length++;
            slot = (slot + 1) & mask;
        }
        return length;
    }

    // Truncation toward zero makes cell 0 two cells wide, and still keeps the cell numbers of two
    // values at most one apart when the values are at most one cell apart.
    private static long cellOf(double value) {
        return (long) (value * CELLS_PER_UNIT);
    }

    private static boolean isNull(long bits) {
        return (bits & EXPONENT_BITS) == EXPONENT_BITS;
    }

    private static boolean isOnGrid(long bits) {
        return (bits & Long.MAX_VALUE) < GRID_LIMIT_BITS;
    }

    private static boolean isScanned(int keyCount) {
        return keyCount <= LINEAR_SCAN_LIMIT;
    }

    // the cell a value is looked up under: its grid cell, or its bits from 2^30 up
    private static long lookupCell(double value, long bits) {
        return isOnGrid(bits) ? cellOf(value) : bits;
    }

    private void allocate(int capacity) {
        cells = new long[capacity];
        Arrays.fill(cells, EMPTY);
        keyIndexes = new int[capacity];
        keyValues = new double[capacity];
        mask = capacity - 1;
        usedSlots = 0;
    }

    private void file(int keyIndex) {
        final double key = keys.getQuick(keyIndex);
        final long bits = Double.doubleToRawLongBits(key);
        if (isNull(bits)) {
            return;
        }
        if (isOnGrid(bits)) {
            final long cell = cellOf(key);
            put(cell - 1, keyIndex, key);
            put(cell, keyIndex, key);
            put(cell + 1, keyIndex, key);
        } else {
            put(bits, keyIndex, key);
        }
    }

    private int homeSlot(long cell) {
        return Hash.hashLong32(cell) & mask;
    }

    private void put(long cell, int keyIndex, double key) {
        int slot = homeSlot(cell);
        while (cells[slot] != EMPTY) {
            slot = (slot + 1) & mask;
        }
        cells[slot] = cell;
        keyIndexes[slot] = keyIndex;
        keyValues[slot] = key;
        usedSlots++;
    }
}
