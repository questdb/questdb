package io.questdb.std.histogram.org.HdrHistogram.packedarray;

/**
 * A Packed array of signed 64 bit values, and supports {@link #get get()}, {@link #set set()},
 * {@link #add add()} and {@link #increment increment()} operations on the logical contents of the array.
 */
public class PackedLongArray extends AbstractPackedLongArray {

    PackedLongArray() {
    }

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
        AbstractPackedArrayContext oldArrayContext = currentArrayContext;
        setArrayContext(new PackedArrayContext(newVirtualArrayLength, oldArrayContext, oldArrayContext.length()));
        for (IterationValue v : oldArrayContext.nonZeroValues()) {
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

