/**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 *
 * @author Gil Tene
 */

package io.questdb.std.histogram.org.HdrHistogram;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.zip.DataFormatException;

/**
 * An integer values High Dynamic Range (HDR) Histogram that supports safe concurrent recording operations.
 * A ConcurrentHistogram guarantees lossless recording of values into the histogram even when the
 * histogram is updated by multiple threads, and supports auto-resize and shift operations that may
 * result from or occur concurrently with other recording operations.
 * <p>
 * It is important to note that concurrent recording, auto-sizing, and value shifting are the only thread-safe
 * behaviors provided by {@link ConcurrentHistogram}, and that it is not otherwise synchronized. Specifically, {@link
 * ConcurrentHistogram} provides no implicit synchronization that would prevent the contents of the histogram
 * from changing during queries, iterations, copies, or addition operations on the histogram. Callers wishing to make
 * potentially concurrent, multi-threaded updates that would safely work in the presence of queries, copies, or
 * additions of histogram objects should either take care to externally synchronize and/or order their access,
 * use the SynchronizedHistogram variant, or (recommended) use Recorder or
 * SingleWriterRecorder which are intended for this purpose.
 * <p>
 * Auto-resizing: When constructed with no specified value range range (or when auto-resize is turned on with {@link
 * Histogram#setAutoResize}) a {@link Histogram} will auto-resize its dynamic range to include recorded values as
 * they are encountered. Note that recording calls that cause auto-resizing may take longer to execute, as resizing
 * incurs allocation and copying of internal data structures.
 * <p>
 * See package description org.HdrHistogram for details.
 */

@SuppressWarnings("unused")
public class ConcurrentHistogram extends Histogram {

    static final AtomicLongFieldUpdater<ConcurrentHistogram> totalCountUpdater =
            AtomicLongFieldUpdater.newUpdater(ConcurrentHistogram.class, "totalCount");
    volatile long totalCount;

    volatile ConcurrentArrayWithNormalizingOffset activeCounts;
    volatile ConcurrentArrayWithNormalizingOffset inactiveCounts;
    transient WriterReaderPhaser wrp = new WriterReaderPhaser();

    @Override
    void setIntegerToDoubleValueConversionRatio(final double integerToDoubleValueConversionRatio) {
        try {
            wrp.readerLock();

            inactiveCounts.setDoubleToIntegerValueConversionRatio(1.0 / integerToDoubleValueConversionRatio);

            // switch active and inactive:
            ConcurrentArrayWithNormalizingOffset tmp = activeCounts;
            activeCounts = inactiveCounts;
            inactiveCounts = tmp;

            wrp.flipPhase();

            inactiveCounts.setDoubleToIntegerValueConversionRatio(1.0 / integerToDoubleValueConversionRatio);

            // switch active and inactive again:
            tmp = activeCounts;
            activeCounts = inactiveCounts;
            inactiveCounts = tmp;

            wrp.flipPhase();

            // At this point, both active and inactive have normalizingIndexOffset safely set,
            // and the switch in each was done without any writers using the wrong value in flight.

        } finally {
            wrp.readerUnlock();
        }
        super.setIntegerToDoubleValueConversionRatio(integerToDoubleValueConversionRatio);
    }

    @Override
    public long getCountAtIndex(final int index) {
        try {
            wrp.readerLock();
            assert (countsArrayLength == activeCounts.length());
            assert (countsArrayLength == inactiveCounts.length());
            long activeCount = activeCounts.get(
                    normalizeIndex(index, activeCounts.getNormalizingIndexOffset(), activeCounts.length()));
            long inactiveCount = inactiveCounts.get(
                    normalizeIndex(index, inactiveCounts.getNormalizingIndexOffset(), inactiveCounts.length()));
            return activeCount + inactiveCount;
        } finally {
            wrp.readerUnlock();
        }
    }

    @Override
    long getCountAtNormalizedIndex(final int index) {
        try {
            wrp.readerLock();
            assert (countsArrayLength == activeCounts.length());
            assert (countsArrayLength == inactiveCounts.length());
            long activeCount = activeCounts.get(index);
            long inactiveCount = inactiveCounts.get(index);
            return activeCount + inactiveCount;
        } finally {
            wrp.readerUnlock();
        }
    }

    @Override
    void incrementCountAtIndex(final int index) {
        long criticalValue = wrp.writerCriticalSectionEnter();
        try {
            activeCounts.atomicIncrement(
                    normalizeIndex(index, activeCounts.getNormalizingIndexOffset(), activeCounts.length()));
        } finally {
            wrp.writerCriticalSectionExit(criticalValue);
        }
    }

    @Override
    void addToCountAtIndex(final int index, final long value) {
        long criticalValue = wrp.writerCriticalSectionEnter();
        try {
            activeCounts.atomicAdd(
                    normalizeIndex(index, activeCounts.getNormalizingIndexOffset(), activeCounts.length()), value);
        } finally {
            wrp.writerCriticalSectionExit(criticalValue);
        }
    }

    @Override
    public void setCountAtIndex(final int index, final long value) {
        try {
            wrp.readerLock();
            assert (countsArrayLength == activeCounts.length());
            assert (countsArrayLength == inactiveCounts.length());
            activeCounts.lazySet(
                    normalizeIndex(index, activeCounts.getNormalizingIndexOffset(), activeCounts.length()), value);
            inactiveCounts.lazySet(
                    normalizeIndex(index, inactiveCounts.getNormalizingIndexOffset(),
                            inactiveCounts.length()), 0);
        } finally {
            wrp.readerUnlock();
        }
    }

    @Override
    void setCountAtNormalizedIndex(final int index, final long value) {
        try {
            wrp.readerLock();
            assert (countsArrayLength == activeCounts.length());
            assert (countsArrayLength == inactiveCounts.length());
            inactiveCounts.lazySet(index, value);
            activeCounts.lazySet(index, 0);
        } finally {
            wrp.readerUnlock();
        }
    }

    @Override
    void recordConvertedDoubleValue(final double value) {
        long criticalValue = wrp.writerCriticalSectionEnter();
        try {
            long integerValue = (long) (value * activeCounts.getDoubleToIntegerValueConversionRatio());
            int index = countsArrayIndex(integerValue);
            activeCounts.atomicIncrement(
                    normalizeIndex(index, activeCounts.getNormalizingIndexOffset(), activeCounts.length()));
            updateMinAndMax(integerValue);
            incrementTotalCount();
        } finally {
            wrp.writerCriticalSectionExit(criticalValue);
        }
    }

    @Override
    public void recordConvertedDoubleValueWithCount(final double value, final long count)
            throws ArrayIndexOutOfBoundsException {
        long criticalValue = wrp.writerCriticalSectionEnter();
        try {
            long integerValue = (long) (value * activeCounts.getDoubleToIntegerValueConversionRatio());
            int index = countsArrayIndex(integerValue);
            activeCounts.atomicAdd(
                    normalizeIndex(index, activeCounts.getNormalizingIndexOffset(), activeCounts.length()), count);
            updateMinAndMax(integerValue);
            addToTotalCount(count);
        } finally {
            wrp.writerCriticalSectionExit(criticalValue);
        }
    }

    @Override
    int getNormalizingIndexOffset() {
        return activeCounts.getNormalizingIndexOffset();
    }

    @Override
    void setNormalizingIndexOffset(final int normalizingIndexOffset) {
        setNormalizingIndexOffset(normalizingIndexOffset, 0,
                false, getIntegerToDoubleValueConversionRatio());
    }

    private void setNormalizingIndexOffset(
            final int newNormalizingIndexOffset,
            final int shiftedAmount,
            final boolean lowestHalfBucketPopulated,
            final double newIntegerToDoubleValueConversionRatio) {
        try {
            wrp.readerLock();

            assert (countsArrayLength == activeCounts.length());
            assert (countsArrayLength == inactiveCounts.length());

            assert (activeCounts.getNormalizingIndexOffset() == inactiveCounts.getNormalizingIndexOffset());

            if (newNormalizingIndexOffset == activeCounts.getNormalizingIndexOffset()) {
                return; // Nothing to do.
            }

            setNormalizingIndexOffsetForInactive(newNormalizingIndexOffset, shiftedAmount,
                    lowestHalfBucketPopulated, newIntegerToDoubleValueConversionRatio);

            // switch active and inactive:
            ConcurrentArrayWithNormalizingOffset tmp = activeCounts;
            activeCounts = inactiveCounts;
            inactiveCounts = tmp;

            wrp.flipPhase();

            setNormalizingIndexOffsetForInactive(newNormalizingIndexOffset, shiftedAmount,
                    lowestHalfBucketPopulated, newIntegerToDoubleValueConversionRatio);

            // switch active and inactive again:
            tmp = activeCounts;
            activeCounts = inactiveCounts;
            inactiveCounts = tmp;

            wrp.flipPhase();

            // At this point, both active and inactive have normalizingIndexOffset safely set,
            // and the switch in each was done without any writers using the wrong value in flight.

        } finally {
            wrp.readerUnlock();
        }
    }

    private void setNormalizingIndexOffsetForInactive(final int newNormalizingIndexOffset,
                                                      final int shiftedAmount,
                                                      final boolean lowestHalfBucketPopulated,
                                                      final double newIntegerToDoubleValueConversionRatio) {
        int zeroIndex;
        long inactiveZeroValueCount;

        // Save and clear the inactive 0 value count:
        zeroIndex = normalizeIndex(0, inactiveCounts.getNormalizingIndexOffset(),
                inactiveCounts.length());
        inactiveZeroValueCount = inactiveCounts.get(zeroIndex);
        inactiveCounts.lazySet(zeroIndex, 0);

        // Change the normalizingIndexOffset on the current inactiveCounts:
        inactiveCounts.setNormalizingIndexOffset(newNormalizingIndexOffset);

        // Handle the inactive lowest half bucket:
        if ((shiftedAmount > 0) && lowestHalfBucketPopulated) {
            shiftLowestInactiveHalfBucketContentsLeft(shiftedAmount, zeroIndex);
        }

        // Restore the inactive 0 value count:
        zeroIndex = normalizeIndex(0, inactiveCounts.getNormalizingIndexOffset(), inactiveCounts.length());
        inactiveCounts.lazySet(zeroIndex, inactiveZeroValueCount);

        inactiveCounts.setDoubleToIntegerValueConversionRatio(1.0 / newIntegerToDoubleValueConversionRatio);
    }

    private void shiftLowestInactiveHalfBucketContentsLeft(final int shiftAmount, final int preShiftZeroIndex) {
        final int numberOfBinaryOrdersOfMagnitude = shiftAmount >> subBucketHalfCountMagnitude;

        // The lowest inactive half-bucket (not including the 0 value) is special: unlike all other half
        // buckets, the lowest half bucket values cannot be scaled by simply changing the
        // normalizing offset. Instead, they must be individually re-recorded at the new
        // scale, and cleared from the current one.
        //
        // We know that all half buckets "below" the current lowest one are full of 0s, because
        // we would have overflowed otherwise. So we need to shift the values in the current
        // lowest half bucket into that range (including the current lowest half bucket itself).
        // Iterating up from the lowermost non-zero "from slot" and copying values to the newly
        // scaled "to slot" (and then zeroing the "from slot"), will work in a single pass,
        // because the scale "to slot" index will always be a lower index than its or any
        // preceding non-scaled "from slot" index:
        //
        // (Note that we specifically avoid slot 0, as it is directly handled in the outer case)

        for (int fromIndex = 1; fromIndex < subBucketHalfCount; fromIndex++) {
            long toValue = valueFromIndex(fromIndex) << numberOfBinaryOrdersOfMagnitude;
            int toIndex = countsArrayIndex(toValue);
            int normalizedToIndex =
                    normalizeIndex(toIndex, inactiveCounts.getNormalizingIndexOffset(), inactiveCounts.length());
            long countAtFromIndex = inactiveCounts.get(fromIndex + preShiftZeroIndex);
            inactiveCounts.lazySet(normalizedToIndex, countAtFromIndex);
            inactiveCounts.lazySet(fromIndex + preShiftZeroIndex, 0);
        }

        // Note that the above loop only creates O(N) work for histograms that have values in
        // the lowest half-bucket (excluding the 0 value). Histograms that never have values
        // there (e.g. all integer value histograms used as internal storage in DoubleHistograms)
        // will never loop, and their shifts will remain O(1).
    }

    @Override
    void shiftNormalizingIndexByOffset(final int offsetToAdd,
                                       final boolean lowestHalfBucketPopulated,
                                       final double newIntegerToDoubleValueConversionRatio) {
        try {
            wrp.readerLock();
            assert (countsArrayLength == activeCounts.length());
            assert (countsArrayLength == inactiveCounts.length());
            int newNormalizingIndexOffset = getNormalizingIndexOffset() + offsetToAdd;
            setNormalizingIndexOffset(newNormalizingIndexOffset,
                    offsetToAdd,
                    lowestHalfBucketPopulated,
                    newIntegerToDoubleValueConversionRatio
            );
        } finally {
            wrp.readerUnlock();
        }
    }

    ConcurrentArrayWithNormalizingOffset allocateArray(int length, int normalizingIndexOffset) {
        return new AtomicLongArrayWithNormalizingOffset(length, normalizingIndexOffset);
    }

    @Override
    public void resize(final long newHighestTrackableValue) {
        try {
            wrp.readerLock();

            assert (countsArrayLength == activeCounts.length());
            assert (countsArrayLength == inactiveCounts.length());

            int newArrayLength = determineArrayLengthNeeded(newHighestTrackableValue);
            int countsDelta = newArrayLength - countsArrayLength;

            if (countsDelta <= 0) {
                // This resize need was already covered by a concurrent resize op.
                return;
            }

            // Allocate both counts arrays here, so if one allocation fails, neither will "take":
            ConcurrentArrayWithNormalizingOffset newInactiveCounts1 =
                    allocateArray(newArrayLength, inactiveCounts.getNormalizingIndexOffset());
            ConcurrentArrayWithNormalizingOffset newInactiveCounts2 =
                    allocateArray(newArrayLength, activeCounts.getNormalizingIndexOffset());


            // Resize the current inactiveCounts:
            ConcurrentArrayWithNormalizingOffset oldInactiveCounts = inactiveCounts;
            inactiveCounts = newInactiveCounts1;

            // Copy inactive contents to newly sized inactiveCounts:
            copyInactiveCountsContentsOnResize(oldInactiveCounts, countsDelta);

            // switch active and inactive:
            ConcurrentArrayWithNormalizingOffset tmp = activeCounts;
            activeCounts = inactiveCounts;
            inactiveCounts = tmp;

            wrp.flipPhase();

            // Resize the newly inactiveCounts:
            oldInactiveCounts = inactiveCounts;
            inactiveCounts = newInactiveCounts2;

            // Copy inactive contents to newly sized inactiveCounts:
            copyInactiveCountsContentsOnResize(oldInactiveCounts, countsDelta);

            // switch active and inactive again:
            tmp = activeCounts;
            activeCounts = inactiveCounts;
            inactiveCounts = tmp;

            wrp.flipPhase();

            // At this point, both active and inactive have been safely resized,
            // and the switch in each was done without any writers modifying it in flight.

            // We resized things. We can now make the histogram establish size accordingly for future recordings:
            establishSize(newHighestTrackableValue);

            assert (countsArrayLength == activeCounts.length());
            assert (countsArrayLength == inactiveCounts.length());

        } finally {
            wrp.readerUnlock();
        }
    }

    void copyInactiveCountsContentsOnResize(
            ConcurrentArrayWithNormalizingOffset oldInactiveCounts, int countsDelta) {
        int oldNormalizedZeroIndex =
                normalizeIndex(0,
                        oldInactiveCounts.getNormalizingIndexOffset(),
                        oldInactiveCounts.length());

        if (oldNormalizedZeroIndex == 0) {
            // Copy old inactive contents to (current) newly sized inactiveCounts, in place:
            for (int i = 0; i < oldInactiveCounts.length(); i++) {
                inactiveCounts.lazySet(i, oldInactiveCounts.get(i));
            }
        } else {
            // We need to shift the stuff from the zero index and up to the end of the array:

            // Copy everything up to the oldNormalizedZeroIndex in place:
            for (int fromIndex = 0; fromIndex < oldNormalizedZeroIndex; fromIndex++) {
                inactiveCounts.lazySet(fromIndex, oldInactiveCounts.get(fromIndex));
            }

            // Copy everything from the oldNormalizedZeroIndex to the end with an index delta shift:
            for (int fromIndex = oldNormalizedZeroIndex; fromIndex < oldInactiveCounts.length(); fromIndex++) {
                int toIndex = fromIndex + countsDelta;
                inactiveCounts.lazySet(toIndex, oldInactiveCounts.get(fromIndex));
            }
        }
    }

    @Override
    public void setAutoResize(final boolean autoResize) {
        this.autoResize = true;
    }

    @Override
    void clearCounts() {
        try {
            wrp.readerLock();
            assert (countsArrayLength == activeCounts.length());
            assert (countsArrayLength == inactiveCounts.length());
            for (int i = 0; i < activeCounts.length(); i++) {
                activeCounts.lazySet(i, 0);
                inactiveCounts.lazySet(i, 0);
            }
            totalCountUpdater.set(this, 0);
        } finally {
            wrp.readerUnlock();
        }
    }

    @Override
    public ConcurrentHistogram copy() {
        ConcurrentHistogram copy = new ConcurrentHistogram(this);
        copy.add(this);
        return copy;
    }

    @Override
    public ConcurrentHistogram copyCorrectedForCoordinatedOmission(final long expectedIntervalBetweenValueSamples) {
        ConcurrentHistogram toHistogram = new ConcurrentHistogram(this);
        toHistogram.addWhileCorrectingForCoordinatedOmission(this, expectedIntervalBetweenValueSamples);
        return toHistogram;
    }

    @Override
    public long getTotalCount() {
        return totalCountUpdater.get(this);
    }

    @Override
    void setTotalCount(final long totalCount) {
        totalCountUpdater.set(this, totalCount);
    }

    @Override
    void incrementTotalCount() {
        totalCountUpdater.incrementAndGet(this);
    }

    @Override
    void addToTotalCount(final long value) {
        totalCountUpdater.addAndGet(this, value);
    }

    @Override
    int _getEstimatedFootprintInBytes() {
        return (512 + (2 * 8 * activeCounts.length()));
    }

    /**
     * Construct an auto-resizing ConcurrentHistogram with a lowest discernible value of 1 and an auto-adjusting
     * highestTrackableValue. Can auto-resize up to track values up to (Long.MAX_VALUE / 2).
     *
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     */
    public ConcurrentHistogram(final int numberOfSignificantValueDigits) {
        this(1, 2, numberOfSignificantValueDigits);
        setAutoResize(true);
    }

    /**
     * Construct a ConcurrentHistogram given the Highest value to be tracked and a number of significant decimal
     * digits. The histogram will be constructed to implicitly track (distinguish from 0) values as low as 1.
     *
     * @param highestTrackableValue The highest value to be tracked by the histogram. Must be a positive
     *                              integer that is {@literal >=} 2.
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     */
    public ConcurrentHistogram(final long highestTrackableValue, final int numberOfSignificantValueDigits) {
        this(1, highestTrackableValue, numberOfSignificantValueDigits);
    }

    /**
     * Construct a ConcurrentHistogram given the Lowest and Highest values to be tracked and a number of significant
     * decimal digits. Providing a lowestDiscernibleValue is useful is situations where the units used
     * for the histogram's values are much smaller that the minimal accuracy required. E.g. when tracking
     * time values stated in nanosecond units, where the minimal accuracy required is a microsecond, the
     * proper value for lowestDiscernibleValue would be 1000.
     *
     * @param lowestDiscernibleValue The lowest value that can be tracked (distinguished from 0) by the histogram.
     *                               Must be a positive integer that is {@literal >=} 1. May be internally rounded
     *                               down to nearest power of 2.
     * @param highestTrackableValue The highest value to be tracked by the histogram. Must be a positive
     *                              integer that is {@literal >=} (2 * lowestDiscernibleValue).
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     */
    public ConcurrentHistogram(final long lowestDiscernibleValue, final long highestTrackableValue,
                               final int numberOfSignificantValueDigits) {
        this(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits,
                true);
    }

    /**
     * Construct a histogram with the same range settings as a given source histogram,
     * duplicating the source's start/end timestamps (but NOT it's contents)
     * @param source The source histogram to duplicate
     */
    public ConcurrentHistogram(final AbstractHistogram source) {
        this(source, true);
    }

    ConcurrentHistogram(final AbstractHistogram source, boolean allocateCountsArray) {
        super(source,false);
        if (allocateCountsArray) {
            activeCounts = new AtomicLongArrayWithNormalizingOffset(countsArrayLength, 0);
            inactiveCounts = new AtomicLongArrayWithNormalizingOffset(countsArrayLength, 0);
        }
        wordSizeInBytes = 8;
    }

    ConcurrentHistogram(final long lowestDiscernibleValue, final long highestTrackableValue,
                        final int numberOfSignificantValueDigits, boolean allocateCountsArray) {
        super(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits,
                false);
        if (allocateCountsArray) {
            activeCounts = new AtomicLongArrayWithNormalizingOffset(countsArrayLength, 0);
            inactiveCounts = new AtomicLongArrayWithNormalizingOffset(countsArrayLength, 0);
        }
        wordSizeInBytes = 8;
    }

    /**
     * Construct a new histogram by decoding it from a ByteBuffer.
     * @param buffer The buffer to decode from
     * @param minBarForHighestTrackableValue Force highestTrackableValue to be set at least this high
     * @return The newly constructed histogram
     */
    public static ConcurrentHistogram decodeFromByteBuffer(final ByteBuffer buffer,
                                                           final long minBarForHighestTrackableValue) {
        return decodeFromByteBuffer(buffer, ConcurrentHistogram.class, minBarForHighestTrackableValue);
    }

    /**
     * Construct a new histogram by decoding it from a compressed form in a ByteBuffer.
     * @param buffer The buffer to decode from
     * @param minBarForHighestTrackableValue Force highestTrackableValue to be set at least this high
     * @return The newly constructed histogram
     * @throws java.util.zip.DataFormatException on error parsing/decompressing the buffer
     */
    public static ConcurrentHistogram decodeFromCompressedByteBuffer(final ByteBuffer buffer,
                                                                     final long minBarForHighestTrackableValue)
            throws DataFormatException {
        return decodeFromCompressedByteBuffer(buffer, ConcurrentHistogram.class, minBarForHighestTrackableValue);
    }

    /**
     * Construct a new ConcurrentHistogram by decoding it from a String containing a base64 encoded
     * compressed histogram representation.
     *
     * @param base64CompressedHistogramString A string containing a base64 encoding of a compressed histogram
     * @return A ConcurrentHistogram decoded from the string
     * @throws DataFormatException on error parsing/decompressing the input
     */
    public static ConcurrentHistogram fromString(final String base64CompressedHistogramString)
            throws DataFormatException {
        return decodeFromCompressedByteBuffer(
                ByteBuffer.wrap(Base64Helper.parseBase64Binary(base64CompressedHistogramString)),
                0);
    }

    private void readObject(final ObjectInputStream o)
            throws IOException, ClassNotFoundException {
        o.defaultReadObject();
        wrp = new WriterReaderPhaser();
    }

    @Override
    synchronized void fillBufferFromCountsArray(final ByteBuffer buffer) {
        try {
            wrp.readerLock();
            super.fillBufferFromCountsArray(buffer);
        } finally {
            wrp.readerUnlock();
        }
    }

    interface ConcurrentArrayWithNormalizingOffset {

        int getNormalizingIndexOffset();

        void setNormalizingIndexOffset(int normalizingIndexOffset);

        double getDoubleToIntegerValueConversionRatio();

        void setDoubleToIntegerValueConversionRatio(double doubleToIntegerValueConversionRatio);

        int getEstimatedFootprintInBytes();

        long get(int index);

        void atomicIncrement(int index);

        void atomicAdd(int index, long valueToAdd);

        void lazySet(int index, long newValue);

        int length();
    }

    static class AtomicLongArrayWithNormalizingOffset extends AtomicLongArray
            implements ConcurrentArrayWithNormalizingOffset {
        private int normalizingIndexOffset;
        private double doubleToIntegerValueConversionRatio;

        AtomicLongArrayWithNormalizingOffset(int length, int normalizingIndexOffset) {
            super(length);
            this.normalizingIndexOffset = normalizingIndexOffset;
        }

        @Override
        public int getNormalizingIndexOffset() {
            return normalizingIndexOffset;
        }

        @Override
        public void setNormalizingIndexOffset(int normalizingIndexOffset) {
            this.normalizingIndexOffset = normalizingIndexOffset;
        }

        @Override
        public double getDoubleToIntegerValueConversionRatio() {
            return doubleToIntegerValueConversionRatio;
        }

        @Override
        public void setDoubleToIntegerValueConversionRatio(double doubleToIntegerValueConversionRatio) {
            this.doubleToIntegerValueConversionRatio = doubleToIntegerValueConversionRatio;
        }

        @Override
        public int getEstimatedFootprintInBytes() {
            return 256 + (8 * this.length());
        }

        @Override
        public void atomicIncrement(int index) {
            incrementAndGet(index);
        }

        @Override
        public void atomicAdd(int index, long valueToAdd) {
            addAndGet(index, valueToAdd);
        }

    }
}