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

/**
 * A Packed array of signed 64 bit values, and supports {@link #get get()}, {@link #set set()},
 * {@link #add add()} and {@link #increment increment()} operations on the logical contents of the array.
 */
public class PackedLongArray extends AbstractPackedLongArray {

    public PackedLongArray(final int virtualLength) {
        this(virtualLength, AbstractPackedArrayContext.MINIMUM_INITIAL_PACKED_ARRAY_CAPACITY);
    }

    public PackedLongArray(final int virtualLength, final int initialPhysicalLength) {
        setArrayContext(new PackedArrayContext(virtualLength, initialPhysicalLength));
    }

    @Override
    public PackedLongArray copy() {
        PackedLongArray copy = new PackedLongArray(this.length(), this.getPhysicalLength());
        copy.add(this);
        return copy;
    }

    @Override
    public void setVirtualLength(final int newVirtualArrayLength) {
        if (newVirtualArrayLength < length()) {
            throw new IllegalArgumentException(
                    "Cannot set virtual length, as requested length " + newVirtualArrayLength +
                            " is smaller than the current virtual length " + length());
        }
        AbstractPackedArrayContext currentArrayContext = getArrayContext();
        if (currentArrayContext.isPacked() &&
                (currentArrayContext.determineTopLevelShiftForVirtualLength(newVirtualArrayLength) ==
                        currentArrayContext.getTopLevelShift())) {
            // No changes to the array context contents is needed. Just change the virtual length.
            currentArrayContext.setVirtualLength(newVirtualArrayLength);
            return;
        }
        setArrayContext(new PackedArrayContext(newVirtualArrayLength, currentArrayContext, currentArrayContext.length()));
        for (IterationValue v : currentArrayContext.nonZeroValues()) {
            set(v.getIndex(), v.getValue());
        }
    }

    @Override
    void clearContents() {
        getArrayContext().clearContents();
    }

    @Override
    long criticalSectionEnter() {
        return 0;
    }

    @Override
    void criticalSectionExit(final long criticalValueAtEnter) {
    }

    @Override
    void resizeStorageArray(final int newPhysicalLengthInLongs) {
        AbstractPackedArrayContext oldArrayContext = getArrayContext();
        PackedArrayContext newArrayContext =
                new PackedArrayContext(oldArrayContext.getVirtualLength(), oldArrayContext, newPhysicalLengthInLongs);
        setArrayContext(newArrayContext);
        for (IterationValue v : oldArrayContext.nonZeroValues()) {
            set(v.getIndex(), v.getValue());
        }
    }
}

