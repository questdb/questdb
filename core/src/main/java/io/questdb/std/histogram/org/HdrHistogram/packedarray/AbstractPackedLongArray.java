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

package io.questdb.std.histogram.org.HdrHistogram.packedarray;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A Packed array of signed 64 bit values, and supports {@link #get get()}, {@link #set set()},
 * {@link #add add()} and {@link #increment increment()} operations on the logical contents of the array.
 */
abstract class AbstractPackedLongArray implements Iterable<Long>, Serializable {
    static final int NUMBER_OF_NON_ZEROS_TO_HASH = 8;
    /**
     * An {@link AbstractPackedLongArray} Uses {@link AbstractPackedArrayContext} to track
     * the array's logical contents. Contexts may be switched when a context requires resizing
     * to complete logical array operations (get, set, add, increment). Contexts are
     * established and used within critical sections in order to facilitate concurrent
     * implementors.
     */

    private static final int NUMBER_OF_SETS = 8;
    private AbstractPackedArrayContext arrayContext;
    private long endTimeStampMsec = 0;
    private long startTimeStampMsec = Long.MAX_VALUE;

    /**
     * Add to a value at a virtual index in the array
     *
     * @param index the virtual index of the value to be added to
     * @param value the value to add
     */
    public void add(final int index, final long value) {
        if (value == 0) {
            return;
        }
        long remainingValueToAdd = value;

        do {
            try {
                long byteMask = 0xff;
                for (int byteNum = 0, byteShift = 0;
                     byteNum < NUMBER_OF_SETS;
                     byteNum++, byteShift += 8, byteMask <<= 8) {
                    final long criticalValue = criticalSectionEnter();
                    try {
                        // Establish context within: critical section
                        AbstractPackedArrayContext arrayContext = getArrayContext();
                        // Deal with unpacked context:
                        if (!arrayContext.isPacked()) {
                            arrayContext.addAndGetAtUnpackedIndex(index, remainingValueToAdd);
                            return;
                        }
                        // Context is packed:
                        int packedIndex = arrayContext.getPackedIndex(byteNum, index, true);

                        long amountToAddAtSet = remainingValueToAdd & byteMask;
                        byte byteToAdd = (byte) (amountToAddAtSet >> byteShift);
                        long afterAddByteValue = arrayContext.addAtByteIndex(packedIndex, byteToAdd);

                        // Reduce remaining value to add by amount just added:
                        remainingValueToAdd -= amountToAddAtSet;

                        // Account for carry:
                        long carryAmount = afterAddByteValue & 0x100;
                        remainingValueToAdd += carryAmount << byteShift;

                        if (remainingValueToAdd == 0) {
                            return; // nothing to add to higher magnitudes
                        }
                    } finally {
                        criticalSectionExit(criticalValue);

                    }
                }
                return;
            } catch (ResizeException ex) {
                resizeStorageArray(ex.getNewSize()); // Resize outside of critical section
            }
        } while (true);
    }

    /**
     * Add the contents of the other array to this one
     *
     * @param other The to add to this array
     */
    public void add(final AbstractPackedLongArray other) {
        for (IterationValue v : other.nonZeroValues()) {
            add(v.getIndex(), v.getValue());
        }
    }

    /**
     * Clear the array contents
     */
    public void clear() {
        clearContents();
    }

    /**
     * Create a copy of this array, complete with data and everything.
     *
     * @return A distinct copy of this array.
     */
    abstract public AbstractPackedLongArray copy();

    /**
     * Determine if this array is equivalent to another.
     *
     * @param other the other array to compare to
     * @return True if this array are equivalent with the other.
     */
    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof AbstractPackedLongArray)) {
            return false;
        }
        AbstractPackedLongArray that = (AbstractPackedLongArray) other;
        if (length() != that.length()) {
            return false;
        }
        if (this.arrayContext.isPacked() || that.arrayContext.isPacked()) {
            // If at least one of the arrays is packed, comparing only the
            // non-zero values that exist in both arrays, using two passes,
            // will likely be more efficient than a single all-index pass:
            // - If both are packed, it will obvioulsy be much faster.
            // - If one is packed and the other is not, we would be visiting
            //   every index in the non-packed array, in one of the passes,
            //   but would still only visit the non-zero elements in the
            //   packed one.
            for (IterationValue v : this.nonZeroValues()) {
                if (that.get(v.getIndex()) != v.getValue()) {
                    return false;
                }
            }
            for (IterationValue v : that.nonZeroValues()) {
                if (this.get(v.getIndex()) != v.getValue()) {
                    return false;
                }
            }
        } else {
            for (int i = 0; i < this.length(); i++) {
                if (this.get(i) != that.get(i)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Get value at virtual index in the array
     *
     * @param index the virtual array index
     * @return the array value at the virtual index given
     */
    public long get(final int index) {
        long value = 0;
        for (int byteNum = 0; byteNum < NUMBER_OF_SETS; byteNum++) {
            int packedIndex = 0;
            long byteValueAtPackedIndex = 0;
            do {
                int newArraySize = 0;
                long criticalValue = criticalSectionEnter();
                try {
                    // Establish context within: critical section
                    AbstractPackedArrayContext arrayContext = getArrayContext();
                    // Deal with unpacked context:
                    if (!arrayContext.isPacked()) {
                        return arrayContext.getAtUnpackedIndex(index);
                    }
                    // Context is packed:
                    packedIndex = arrayContext.getPackedIndex(byteNum, index, false);
                    if (packedIndex < 0) {
                        return value;
                    }
                    byteValueAtPackedIndex =
                            (((long) arrayContext.getAtByteIndex(packedIndex)) & 0xff) << (byteNum << 3);
                } catch (ResizeException ex) {
                    newArraySize = ex.getNewSize(); // Resize outside of critical section
                } finally {
                    criticalSectionExit(criticalValue);
                    if (newArraySize != 0) {
                        resizeStorageArray(newArraySize);
                    }
                }
            } while (packedIndex == 0);

            value += byteValueAtPackedIndex;
        }
        return value;
    }

    /**
     * get the end time stamp [optionally] stored with this array
     *
     * @return the end time stamp [optionally] stored with this array
     */
    public long getEndTimeStamp() {
        return endTimeStampMsec;
    }

    /**
     * Get the current physical length (in longs) of the array's backing storage
     *
     * @return the current physical length (in longs) of the array's current backing storage
     */
    public int getPhysicalLength() {
        return getArrayContext().length();
    }

    /**
     * get the start time stamp [optionally] stored with this array
     *
     * @return the start time stamp [optionally] stored with this array
     */
    public long getStartTimeStamp() {
        return startTimeStampMsec;
    }

    @Override
    public int hashCode() {
        int h = 0;
        h = oneAtATimeHashStep(h, length());
        int count = 0;
        // Include the first NUMBER_OF_NON_ZEROS_TO_HASH non-zeros in the hash:
        for (IterationValue v : nonZeroValues()) {
            if (++count > NUMBER_OF_NON_ZEROS_TO_HASH) {
                break;
            }
            h = oneAtATimeHashStep(h, (int) v.getIndex());
            h = oneAtATimeHashStep(h, (int) v.getValue());
        }
        h += (h << 3);
        h ^= (h >> 11);
        h += (h << 15);
        return h;
    }

    /**
     * Increment value at a virrual index in the array
     *
     * @param index virtual index of value to increment
     */
    public void increment(final int index) {
        add(index, 1);
    }

    /**
     * An Iterator over all values in the array
     *
     * @return an Iterator over all values in the array
     */
    public Iterator<Long> iterator() {
        return new AllValuesIterator();
    }

    /**
     * Get the (virtual) length of the array
     *
     * @return the (virtual) length of the array
     */
    public int length() {
        return getArrayContext().getVirtualLength();
    }

    /**
     * An Iterator over all non-Zero values in the array
     *
     * @return an Iterator over all non-Zero values in the array
     */
    public Iterable<IterationValue> nonZeroValues() {
        return getArrayContext().nonZeroValues();
    }

    /**
     * Set the value at a virtual index in the array
     *
     * @param index the virtual index of the value to set
     * @param value the value to set
     */
    public void set(final int index, final long value) {
        int bytesAlreadySet = 0;
        do {
            long valueForNextLevels = value;
            try {
                for (int byteNum = 0; byteNum < NUMBER_OF_SETS; byteNum++) {
                    long criticalValue = criticalSectionEnter();
                    try {
                        // Establish context within: critical section
                        AbstractPackedArrayContext arrayContext = getArrayContext();
                        // Deal with unpacked context:
                        if (!arrayContext.isPacked()) {
                            arrayContext.setAtUnpackedIndex(index, value);
                            return;
                        }
                        // Context is packed:
                        if (valueForNextLevels == 0) {
                            // Special-case zeros to avoid inflating packed array for no reason
                            int packedIndex = arrayContext.getPackedIndex(byteNum, index, false);
                            if (packedIndex < 0) {
                                return; // no need to create entries for zero values if they don't already exist
                            }
                        }
                        // Make sure byte is populated:
                        int packedIndex = arrayContext.getPackedIndex(byteNum, index, true);

                        // Determine value to write, and prepare for next levels
                        byte byteToWrite = (byte) (valueForNextLevels & 0xff);
                        valueForNextLevels >>= 8;

                        if (byteNum < bytesAlreadySet) {
                            // We want to avoid writing to the same byte twice when not doing so for the
                            // entire 64 bit value atomically, as doing so opens a race with e.g. concurrent
                            // adders. So dobn't actually write the byte if has been written before.
                            continue;
                        }
                        arrayContext.setAtByteIndex(packedIndex, byteToWrite);
                        bytesAlreadySet++;
                    } finally {
                        criticalSectionExit(criticalValue);
                    }
                }
                return;
            } catch (ResizeException ex) {
                resizeStorageArray(ex.getNewSize()); // Resize outside of critical section
            }
        } while (true);
    }

    /**
     * Set the end time stamp value associated with this array to a given value.
     *
     * @param timeStampMsec the value to set the time stamp to, [by convention] in msec since the epoch.
     */
    public void setEndTimeStamp(final long timeStampMsec) {
        this.endTimeStampMsec = timeStampMsec;
    }

    /**
     * Set the start time stamp value associated with this array to a given value.
     *
     * @param timeStampMsec the value to set the time stamp to, [by convention] in msec since the epoch.
     */
    public void setStartTimeStamp(final long timeStampMsec) {
        this.startTimeStampMsec = timeStampMsec;
    }

    /**
     * Set a new virtual length for the array.
     *
     * @param newVirtualArrayLength the
     */
    abstract public void setVirtualLength(final int newVirtualArrayLength);

    @Override
    public String toString() {
        String output = "PackedArray:\n";
        AbstractPackedArrayContext arrayContext = getArrayContext();
        output += arrayContext.toString();
        return output;
    }

    private int oneAtATimeHashStep(final int incomingHash, final int v) {
        int h = incomingHash;
        h += v;
        h += (h << 10);
        h ^= (h >> 6);
        return h;
    }

    // Regular array iteration (iterates over all virtrual indexes, zero-value or not:

    abstract void clearContents();

    abstract long criticalSectionEnter();

    abstract void criticalSectionExit(long criticalValueAtEnter);

    AbstractPackedArrayContext getArrayContext() {
        return arrayContext;
    }

    abstract void resizeStorageArray(int newPhysicalLengthInLongs);

    void setArrayContext(AbstractPackedArrayContext newArrayContext) {
        arrayContext = newArrayContext;
    }

    class AllValuesIterator implements Iterator<Long> {

        int nextVirtrualIndex = 0;

        @Override
        public boolean hasNext() {
            return ((nextVirtrualIndex >= 0) &&
                    (nextVirtrualIndex < length()));
        }

        @Override
        public Long next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return get(nextVirtrualIndex++);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
