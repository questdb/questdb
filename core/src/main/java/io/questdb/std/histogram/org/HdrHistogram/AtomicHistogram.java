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
import java.util.concurrent.atomic.*;
import java.util.zip.DataFormatException;

/**
 * <h3>A High Dynamic Range (HDR) Histogram using atomic <b><code>long</code></b> count type </h3>
 * An AtomicHistogram guarantees lossless recording of values into the histogram even when the
 * histogram is updated by multiple threads. It is important to note though that this lossless
 * recording capability is the only thread-safe behavior provided by AtomicHistogram, and that it
 * is not otherwise synchronized. Specifically, AtomicHistogram does not support auto-resizing,
 * does not support value shift operations, and provides no implicit synchronization
 * that would prevent the contents of the histogram from changing during iterations, copies, or
 * addition operations on the histogram. Callers wishing to make potentially concurrent,
 * multi-threaded updates that would safely work in the presence of queries, copies, or additions
 * of histogram objects should either take care to externally synchronize and/or order their access,
 * use the { org.HdrHistogram.SynchronizedHistogram} variant, or (recommended) use the
 * { Recorder} class, which is intended for this purpose.
 * <p>
 * See package description for { org.HdrHistogram} for details.
 */

public class AtomicHistogram extends Histogram {

    static final AtomicLongFieldUpdater<AtomicHistogram> totalCountUpdater =
            AtomicLongFieldUpdater.newUpdater(AtomicHistogram.class, "totalCount");
    volatile long totalCount;
    volatile AtomicLongArray counts;

    @Override
    public long getCountAtIndex(final int index) {
        return counts.get(index);
    }

    @Override
    long getCountAtNormalizedIndex(final int index) {
        return counts.get(index);
    }

    @Override
    void incrementCountAtIndex(final int index) {
        counts.getAndIncrement(index);
    }

    @Override
    void addToCountAtIndex(final int index, final long value) {
        counts.getAndAdd(index, value);
    }

    @Override
    public void setCountAtIndex(int index, long value) {
        counts.lazySet(index, value);
    }

    @Override
    void setCountAtNormalizedIndex(int index, long value) {
        counts.lazySet(index, value);
    }

    @Override
    int getNormalizingIndexOffset() {
        return 0;
    }

    @Override
    void setNormalizingIndexOffset(int normalizingIndexOffset) {
        if (normalizingIndexOffset != 0) {
            throw new IllegalStateException(
                    "AtomicHistogram does not support non-zero normalizing index settings." +
                            " Use ConcurrentHistogram Instead.");
        }
    }

    @Override
    void shiftNormalizingIndexByOffset(int offsetToAdd,
                                       boolean lowestHalfBucketPopulated,
                                       double newIntegerToDoubleValueConversionRatio) {
        throw new IllegalStateException(
                "AtomicHistogram does not support Shifting operations." +
                        " Use ConcurrentHistogram Instead.");
    }

    @Override
    void resize(long newHighestTrackableValue) {
        throw new IllegalStateException(
                "AtomicHistogram does not support resizing operations." +
                        " Use ConcurrentHistogram Instead.");
    }

    @Override
    public void setAutoResize(boolean autoResize) {
        throw new IllegalStateException(
                "AtomicHistogram does not support AutoResize operation." +
                        " Use ConcurrentHistogram Instead.");
    }

    @Override
    public boolean supportsAutoResize() { return false; }

    @Override
    void clearCounts() {
        for (int i = 0; i < counts.length(); i++) {
            counts.lazySet(i, 0);
        }
        totalCountUpdater.set(this, 0);
    }

    @Override
    public AtomicHistogram copy() {
        AtomicHistogram copy = new AtomicHistogram(this);
        copy.add(this);
        return copy;
    }

    @Override
    public AtomicHistogram copyCorrectedForCoordinatedOmission(final long expectedIntervalBetweenValueSamples) {
        AtomicHistogram toHistogram = new AtomicHistogram(this);
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
        return (512 + (8 * counts.length()));
    }

    /**
     * Construct a AtomicHistogram given the Highest value to be tracked and a number of significant decimal digits.
     * The histogram will be constructed to implicitly track (distinguish from 0) values as low as 1.
     *
     * @param highestTrackableValue The highest value to be tracked by the histogram. Must be a positive
     *                              integer that is {@literal >=} 2.
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     */
    public AtomicHistogram(final long highestTrackableValue, final int numberOfSignificantValueDigits) {
        this(1, highestTrackableValue, numberOfSignificantValueDigits);
    }

    /**
     * Construct a AtomicHistogram given the Lowest and Highest values to be tracked and a number of significant
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
    public AtomicHistogram(final long lowestDiscernibleValue, final long highestTrackableValue,
                           final int numberOfSignificantValueDigits) {
        super(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits, false);
        counts = new AtomicLongArray(countsArrayLength);
        wordSizeInBytes = 8;
    }

    /**
     * Construct a histogram with the same range settings as a given source histogram,
     * duplicating the source's start/end timestamps (but NOT it's contents)
     * @param source The source histogram to duplicate
     */
    public AtomicHistogram(final AbstractHistogram source) {
        super(source, false);
        counts = new AtomicLongArray(countsArrayLength);
        wordSizeInBytes = 8;
    }

    /**
     * Construct a new histogram by decoding it from a ByteBuffer.
     * @param buffer The buffer to decode from
     * @param minBarForHighestTrackableValue Force highestTrackableValue to be set at least this high
     * @return The newly constructed histogram
     */
    public static AtomicHistogram decodeFromByteBuffer(final ByteBuffer buffer,
                                                       final long minBarForHighestTrackableValue) {
        return decodeFromByteBuffer(buffer, AtomicHistogram.class, minBarForHighestTrackableValue);
    }

    /**
     * Construct a new histogram by decoding it from a compressed form in a ByteBuffer.
     * @param buffer The buffer to decode from
     * @param minBarForHighestTrackableValue Force highestTrackableValue to be set at least this high
     * @return The newly constructed histogram
     * @throws DataFormatException on error parsing/decompressing the buffer
     */
    public static AtomicHistogram decodeFromCompressedByteBuffer(final ByteBuffer buffer,
                                                                 final long minBarForHighestTrackableValue)
            throws DataFormatException {
        return decodeFromCompressedByteBuffer(buffer, AtomicHistogram.class, minBarForHighestTrackableValue);
    }

    /**
     * Construct a new AtomicHistogram by decoding it from a String containing a base64 encoded
     * compressed histogram representation.
     *
     * @param base64CompressedHistogramString A string containing a base64 encoding of a compressed histogram
     * @return A AtomicHistogram decoded from the string
     * @throws DataFormatException on error parsing/decompressing the input
     */
    public static AtomicHistogram fromString(final String base64CompressedHistogramString)
            throws DataFormatException {
        return decodeFromCompressedByteBuffer(
                ByteBuffer.wrap(Base64Helper.parseBase64Binary(base64CompressedHistogramString)),
                0);
    }

    private void readObject(final ObjectInputStream o)
            throws IOException, ClassNotFoundException {
        o.defaultReadObject();
    }
}