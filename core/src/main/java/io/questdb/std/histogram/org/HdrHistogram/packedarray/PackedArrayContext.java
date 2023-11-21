package io.questdb.std.histogram.org.HdrHistogram.packedarray;

import java.util.Arrays;

/**
 * A non-concurrent array context. No atomics used.
 */
class PackedArrayContext extends AbstractPackedArrayContext {

    private long[] array;
    private int populatedShortLength = 0;

    PackedArrayContext(final int virtualLength,
                       final int initialPhysicalLength,
                       final boolean allocateArray) {
        super(virtualLength, initialPhysicalLength);
        if (allocateArray) {
            array = new long[getPhysicalLength()];
            init(virtualLength);
        }
    }

    PackedArrayContext(final int virtualLength,
                       final int initialPhysicalLength) {
        this(virtualLength, initialPhysicalLength, true);
    }
    PackedArrayContext(final int virtualLength,
                       final AbstractPackedArrayContext from,
                       final int newPhysicalArrayLength) {
        this(virtualLength, newPhysicalArrayLength);
        if (isPacked()) {
            populateEquivalentEntriesWithZerosFromOther(from);
        }
    }

    @Override
    long addAndGetAtUnpackedIndex(final int index, final long valueToAdd) {
        array[index] += valueToAdd;
        return array[index];
    }

    @Override
    boolean casAtLongIndex(final int longIndex, final long expectedValue, final long newValue) {
        if (array[longIndex] != expectedValue) return false;
        array[longIndex] = newValue;
        return true;
    }

    @Override
    boolean casPopulatedLongLength(final int expectedPopulatedLongLength, final int newPopulatedLongLength) {
        if (getPopulatedLongLength() != expectedPopulatedLongLength) return false;
        return casPopulatedShortLength(populatedShortLength, newPopulatedLongLength << 2);
    }

    @Override
    boolean casPopulatedShortLength(final int expectedPopulatedShortLength, final int newPopulatedShortLength) {
        if (this.populatedShortLength != expectedPopulatedShortLength) return false;
        this.populatedShortLength = newPopulatedShortLength;
        return true;
    }

    @Override
    void clearContents() {
        java.util.Arrays.fill(array, 0);
        init(getVirtualLength());
    }

    @Override
    long getAtLongIndex(final int longIndex) {
        return array[longIndex];
    }

    @Override
    long getAtUnpackedIndex(final int index) {
        return array[index];
    }

    @Override
    int getPopulatedShortLength() {
        return populatedShortLength;
    }

    @Override
    long incrementAndGetAtUnpackedIndex(final int index) {
        array[index]++;
        return array[index];
    }

    @Override
    void lazySetAtLongIndex(final int longIndex, final long newValue) {
        array[longIndex] = newValue;
    }

    @Override
    void lazysetAtUnpackedIndex(final int index, final long newValue) {
        array[index] = newValue;
    }

    @Override
    int length() {
        return array.length;
    }

    @Override
    void resizeArray(final int newLength) {
        array = Arrays.copyOf(array, newLength);
    }

    @Override
    void setAtUnpackedIndex(final int index, final long newValue) {
        array[index] = newValue;
    }

    @Override
    String unpackedToString() {
        return Arrays.toString(array);
    }
}
