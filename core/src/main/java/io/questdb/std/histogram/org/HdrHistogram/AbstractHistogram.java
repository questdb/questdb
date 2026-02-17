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

// Written by Gil Tene of Azul Systems, and released to the public domain,
// as explained at http://creativecommons.org/publicdomain/zero/1.0/
//
// @author Gil Tene

package io.questdb.std.histogram.org.HdrHistogram;

import io.questdb.cairo.CairoException;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static java.nio.ByteOrder.BIG_ENDIAN;

/**
 * <h2>An abstract base class for integer values High Dynamic Range (HDR) Histograms</h2>
 * <p>
 * AbstractHistogram supports the recording and analyzing sampled data value counts across a configurable integer value
 * range with configurable value precision within the range. Value precision is expressed as the number of significant
 * digits in the value recording, and provides control over value quantization behavior across the value range and the
 * subsequent value resolution at any given level.
 * <p>
 * For example, a Histogram could be configured to track the counts of observed integer values between 0 and
 * 3,600,000,000 while maintaining a value precision of 3 significant digits across that range. Value quantization
 * within the range will thus be no larger than 1/1,000th (or 0.1%) of any value. This example Histogram could
 * be used to track and analyze the counts of observed response times ranging between 1 microsecond and 1 hour
 * in magnitude, while maintaining a value resolution of 1 microsecond up to 1 millisecond, a resolution of
 * 1 millisecond (or better) up to one second, and a resolution of 1 second (or better) up to 1,000 seconds. At it's
 * maximum tracked value (1 hour), it would still maintain a resolution of 3.6 seconds (or better).
 * <p>
 * See package description for {@link io.questdb.std.histogram.org.HdrHistogram} for details.
 */
public abstract class AbstractHistogram extends AbstractHistogramBase implements ValueRecorder, Serializable {

    // "Hot" accessed fields (used in the value recording code path) are bunched here, such
    // that they will have a good chance of ending up in the same cache line as the totalCounts and
    // counts array reference fields that subclass implementations will typically add.

    private static final int ENCODING_HEADER_SIZE = 40;
    private static final int V0CompressedEncodingCookieBase = 0x1c849309;
    private static final int V0EncodingCookieBase = 0x1c849308;
    private static final int V0_ENCODING_HEADER_SIZE = 32;
    private static final int V1CompressedEncodingCookieBase = 0x1c849302;
    private static final int V1EncodingCookieBase = 0x1c849301;
    private static final int V2CompressedEncodingCookieBase = 0x1c849304;
    private static final int V2EncodingCookieBase = 0x1c849303;
    private static final int V2maxWordSizeInBytes = 9; // LEB128-64b9B + ZigZag require up to 9 bytes per word
    private static final int compressedEncodingCookieBase = V2CompressedEncodingCookieBase;

    // Sub-classes will typically add a totalCount field and a counts array field, which will likely be laid out
    // right around here due to the subclass layout rules in most practical JVM implementations.

    //   ########     ###     ######  ##    ##    ###     ######   ########
    //   ##     ##   ## ##   ##    ## ##   ##    ## ##   ##    ##  ##
    //   ##     ##  ##   ##  ##       ##  ##    ##   ##  ##        ##
    //   ########  ##     ## ##       #####    ##     ## ##   #### ######
    //   ##        ######### ##       ##  ##   ######### ##    ##  ##
    //   ##        ##     ## ##    ## ##   ##  ##     ## ##    ##  ##
    //   ##        ##     ##  ######  ##    ## ##     ##  ######   ########
    //
    //      ###    ########   ######  ######## ########     ###     ######  ########
    //     ## ##   ##     ## ##    ##    ##    ##     ##   ## ##   ##    ##    ##
    //    ##   ##  ##     ## ##          ##    ##     ##  ##   ##  ##          ##
    //   ##     ## ########   ######     ##    ########  ##     ## ##          ##
    //   ######### ##     ##       ##    ##    ##   ##   ######### ##          ##
    //   ##     ## ##     ## ##    ##    ##    ##    ##  ##     ## ##    ##    ##
    //   ##     ## ########   ######     ##    ##     ## ##     ##  ######     ##
    //
    // Abstract, counts-type dependent methods to be provided by subclass implementations:
    //
    private static final Class[] constructorArgsTypes = {Long.TYPE, Long.TYPE, Integer.TYPE};
    private static final int encodingCookieBase = V2EncodingCookieBase;
    private static final long serialVersionUID = 0x1c849302;
    /**
     * Number of leading zeros in the largest value that can fit in bucket 0.
     */
    int leadingZeroCountBase;
    long maxValue = 0;
    long minNonZeroValue = Long.MAX_VALUE;
    int subBucketHalfCount;
    int subBucketHalfCountMagnitude;
    /**
     * Biggest value that can fit in bucket 0
     */
    long subBucketMask;
    /**
     * Largest k such that 2^k &lt;= lowestDiscernibleValue
     */
    int unitMagnitude;
    /**
     * Lowest unitMagnitude bits are set
     */
    long unitMagnitudeMask;

    /**
     * Construct an auto-resizing histogram with a lowest discernible value of 1 and an auto-adjusting
     * highestTrackableValue. Can auto-resize up to track values up to (Long.MAX_VALUE / 2).
     *
     * @param numberOfSignificantValueDigits The number of significant decimal digits to which the histogram will
     *                                       maintain value resolution and separation. Must be a non-negative
     *                                       integer between 0 and 5.
     */
    protected AbstractHistogram(final int numberOfSignificantValueDigits) {
        this(1, 2, numberOfSignificantValueDigits);
        autoResize = true;
    }

    /**
     * Construct a histogram given the Lowest and Highest values to be tracked and a number of significant
     * decimal digits. Providing a lowestDiscernibleValue is useful is situations where the units used
     * for the histogram's values are much smaller that the minimal accuracy required. E.g. when tracking
     * time values stated in nanosecond units, where the minimal accuracy required is a microsecond, the
     * proper value for lowestDiscernibleValue would be 1000.
     *
     * @param lowestDiscernibleValue         The lowest value that can be discerned (distinguished from 0) by the histogram.
     *                                       Must be a positive integer that is {@literal >=} 1. May be internally rounded
     *                                       down to nearest power of 2.
     * @param highestTrackableValue          The highest value to be tracked by the histogram. Must be a positive
     *                                       integer that is {@literal >=} (2 * lowestDiscernibleValue).
     * @param numberOfSignificantValueDigits The number of significant decimal digits to which the histogram will
     *                                       maintain value resolution and separation. Must be a non-negative
     *                                       integer between 0 and 5.
     */
    protected AbstractHistogram(final long lowestDiscernibleValue, final long highestTrackableValue,
                                final int numberOfSignificantValueDigits) {
        // Verify argument validity
        if (lowestDiscernibleValue < 1) {
            throw new IllegalArgumentException("lowestDiscernibleValue must be >= 1");
        }
        if (lowestDiscernibleValue > Long.MAX_VALUE / 2) {
            // prevent subsequent multiplication by 2 for highestTrackableValue check from overflowing
            throw new IllegalArgumentException("lowestDiscernibleValue must be <= Long.MAX_VALUE / 2");
        }
        if (highestTrackableValue < 2L * lowestDiscernibleValue) {
            throw new IllegalArgumentException("highestTrackableValue must be >= 2 * lowestDiscernibleValue");
        }
        if ((numberOfSignificantValueDigits < 0) || (numberOfSignificantValueDigits > 5)) {
            throw new IllegalArgumentException("numberOfSignificantValueDigits must be between 0 and 5");
        }
        identity = constructionIdentityCount.getAndIncrement();

        init(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits, 1.0, 0);
    }

    /**
     * Construct a histogram with the same range settings as a given source histogram,
     * duplicating the source's start/end timestamps (but NOT it's contents)
     *
     * @param source The source histogram to duplicate
     */
    protected AbstractHistogram(final AbstractHistogram source) {
        this(source.getLowestDiscernibleValue(), source.getHighestTrackableValue(),
                source.getNumberOfSignificantValueDigits());
        this.setStartTimeStamp(source.getStartTimeStamp());
        this.setEndTimeStamp(source.getEndTimeStamp());
        this.autoResize = source.autoResize;
    }

    /**
     * Add the contents of another histogram to this one.
     * <p>
     * As part of adding the contents, the start/end timestamp range of this histogram will be
     * extended to include the start/end timestamp range of the other histogram.
     *
     * @param otherHistogram The other histogram.
     * @throws CairoException (may throw) if values in fromHistogram's are
     *                        higher than highestTrackableValue.
     */
    public void add(final AbstractHistogram otherHistogram) throws CairoException {
        long highestRecordableValue = highestEquivalentValue(valueFromIndex(countsArrayLength - 1));
        if (highestRecordableValue < otherHistogram.getMaxValue()) {
            if (!isAutoResize()) {
                throw CairoException.nonCritical().put(
                        "The other histogram includes values that do not fit in this histogram's range.");
            }
            resize(otherHistogram.getMaxValue());
        }
        if ((bucketCount == otherHistogram.bucketCount) &&
                (subBucketCount == otherHistogram.subBucketCount) &&
                (unitMagnitude == otherHistogram.unitMagnitude) &&
                (getNormalizingIndexOffset() == otherHistogram.getNormalizingIndexOffset())) {
            // Counts arrays are of the same length and meaning, so we can just iterate and add directly:
            long observedOtherTotalCount = 0;
            for (int i = 0; i < otherHistogram.countsArrayLength; i++) {
                long otherCount = otherHistogram.getCountAtIndex(i);
                if (otherCount > 0) {
                    addToCountAtIndex(i, otherCount);
                    observedOtherTotalCount += otherCount;
                }
            }
            setTotalCount(getTotalCount() + observedOtherTotalCount);
            updateMaxValue(Math.max(getMaxValue(), otherHistogram.getMaxValue()));
            updateMinNonZeroValue(Math.min(getMinNonZeroValue(), otherHistogram.getMinNonZeroValue()));
        } else {
            // Arrays are not a direct match (or the other could change on the fly in some valid way),
            // so we can't just stream through and add them. Instead, go through the array and add each
            // non-zero value found at it's proper value:

            // Do max value first, to avoid max value updates on each iteration:
            int otherMaxIndex = otherHistogram.countsArrayIndex(otherHistogram.getMaxValue());
            long otherCount = otherHistogram.getCountAtIndex(otherMaxIndex);
            recordValueWithCount(otherHistogram.valueFromIndex(otherMaxIndex), otherCount);

            // Record the remaining values, up to but not including the max value:
            for (int i = 0; i < otherMaxIndex; i++) {
                otherCount = otherHistogram.getCountAtIndex(i);
                if (otherCount > 0) {
                    recordValueWithCount(otherHistogram.valueFromIndex(i), otherCount);
                }
            }
        }
        setStartTimeStamp(Math.min(startTimeStampMsec, otherHistogram.startTimeStampMsec));
        setEndTimeStamp(Math.max(endTimeStampMsec, otherHistogram.endTimeStampMsec));
    }

    /**
     * Add the contents of another histogram to this one, while correcting the incoming data for coordinated omission.
     * <p>
     * To compensate for the loss of sampled values when a recorded value is larger than the expected
     * interval between value samples, the values added will include an auto-generated additional series of
     * decreasingly-smaller (down to the expectedIntervalBetweenValueSamples) value records for each count found
     * in the current histogram that is larger than the expectedIntervalBetweenValueSamples.
     * <p>
     * Note: This is a post-recording correction method, as opposed to the at-recording correction method provided
     * by {@link #recordValueWithExpectedInterval(long, long) recordValueWithExpectedInterval}. The two
     * methods are mutually exclusive, and only one of the two should be be used on a given data set to correct
     * for the same coordinated omission issue.
     * by
     * <p>
     * See notes in the description of the Histogram calls for an illustration of why this corrective behavior is
     * important.
     *
     * @param otherHistogram                      The other histogram. highestTrackableValue and largestValueWithSingleUnitResolution must match.
     * @param expectedIntervalBetweenValueSamples If expectedIntervalBetweenValueSamples is larger than 0, add
     *                                            auto-generated value records as appropriate if value is larger
     *                                            than expectedIntervalBetweenValueSamples
     * @throws CairoException (may throw) if values exceed highestTrackableValue
     */
    public void addWhileCorrectingForCoordinatedOmission(final AbstractHistogram otherHistogram,
                                                         final long expectedIntervalBetweenValueSamples) {
        final AbstractHistogram toHistogram = this;

        for (HistogramIterationValue v : otherHistogram.recordedValues()) {
            toHistogram.recordValueWithCountAndExpectedInterval(v.getValueIteratedTo(),
                    v.getCountAtValueIteratedTo(), expectedIntervalBetweenValueSamples);
        }
    }

    /**
     * Provide a means of iterating through all histogram values using the finest granularity steps supported by
     * the underlying representation. The iteration steps through all possible unit value levels, regardless of
     * whether or not there were recorded values for that value level, and terminates when all recorded histogram
     * values are exhausted.
     *
     * @return An {@link java.lang.Iterable}{@literal <}{@link HistogramIterationValue}{@literal >}
     * through the histogram using
     * a {@link AllValuesIterator}
     */
    public AllValues allValues() {
        return new AllValues(this);
    }

    /**
     * Create a copy of this histogram, complete with data and everything.
     *
     * @return A distinct copy of this histogram.
     */
    abstract public AbstractHistogram copy();

    /**
     * Get a copy of this histogram, corrected for coordinated omission.
     * <p>
     * To compensate for the loss of sampled values when a recorded value is larger than the expected
     * interval between value samples, the new histogram will include an auto-generated additional series of
     * decreasingly-smaller (down to the expectedIntervalBetweenValueSamples) value records for each count found
     * in the current histogram that is larger than the expectedIntervalBetweenValueSamples.
     * <p>
     * Note: This is a post-correction method, as opposed to the at-recording correction method provided
     * by {@link #recordValueWithExpectedInterval(long, long) recordValueWithExpectedInterval}. The two
     * methods are mutually exclusive, and only one of the two should be be used on a given data set to correct
     * for the same coordinated omission issue.
     * by
     * <p>
     * See notes in the description of the Histogram calls for an illustration of why this corrective behavior is
     * important.
     *
     * @param expectedIntervalBetweenValueSamples If expectedIntervalBetweenValueSamples is larger than 0, add
     *                                            auto-generated value records as appropriate if value is larger
     *                                            than expectedIntervalBetweenValueSamples
     * @return a copy of this histogram, corrected for coordinated omission.
     */
    abstract public AbstractHistogram copyCorrectedForCoordinatedOmission(long expectedIntervalBetweenValueSamples);

    /**
     * Copy this histogram into the target histogram, overwriting it's contents.
     *
     * @param targetHistogram the histogram to copy into
     */
    public void copyInto(final AbstractHistogram targetHistogram) {
        targetHistogram.reset();
        targetHistogram.add(this);
        targetHistogram.setStartTimeStamp(this.startTimeStampMsec);
        targetHistogram.setEndTimeStamp(this.endTimeStampMsec);
    }

    //    ######   #######  ##    ##  ######  ######## ########  ##     ##  ######  ######## ####  #######  ##    ##
    //   ##    ## ##     ## ###   ## ##    ##    ##    ##     ## ##     ## ##    ##    ##     ##  ##     ## ###   ##
    //   ##       ##     ## ####  ## ##          ##    ##     ## ##     ## ##          ##     ##  ##     ## ####  ##
    //   ##       ##     ## ## ## ##  ######     ##    ########  ##     ## ##          ##     ##  ##     ## ## ## ##
    //   ##       ##     ## ##  ####       ##    ##    ##   ##   ##     ## ##          ##     ##  ##     ## ##  ####
    //   ##    ## ##     ## ##   ### ##    ##    ##    ##    ##  ##     ## ##    ##    ##     ##  ##     ## ##   ###
    //    ######   #######  ##    ##  ######     ##    ##     ##  #######   ######     ##    ####  #######  ##    ##
    //
    // Construction:
    //

    public int countsArrayIndex(final long value) {
        if (value < 0) {
            throw CairoException.nonCritical().put("Histogram recorded value cannot be negative.");
        }
        final int bucketIndex = getBucketIndex(value);
        final int subBucketIndex = getSubBucketIndex(value, bucketIndex);
        return countsArrayIndex(bucketIndex, subBucketIndex);
    }

    /**
     * Encode this histogram into a ByteBuffer
     *
     * @param buffer The buffer to encode into
     * @return The number of bytes written to the buffer
     */
    synchronized public int encodeIntoByteBuffer(final ByteBuffer buffer) {
        final long maxValue = getMaxValue();
        final int relevantLength = countsArrayIndex(maxValue) + 1;
        if (buffer.capacity() < getNeededByteBufferCapacity(relevantLength)) {
            throw CairoException.nonCritical().put("buffer does not have capacity for " +
                    getNeededByteBufferCapacity(relevantLength) + " bytes");
        }
        int initialPosition = buffer.position();
        buffer.putInt(getEncodingCookie());
        buffer.putInt(0); // Placeholder for payload length in bytes.
        buffer.putInt(getNormalizingIndexOffset());
        buffer.putInt(numberOfSignificantValueDigits);
        buffer.putLong(lowestDiscernibleValue);
        buffer.putLong(highestTrackableValue);
        buffer.putDouble(getIntegerToDoubleValueConversionRatio());

        int payloadStartPosition = buffer.position();
        fillBufferFromCountsArray(buffer);
        buffer.putInt(initialPosition + 4, buffer.position() - payloadStartPosition); // Record the payload length


        return buffer.position() - initialPosition;
    }

    /**
     * Encode this histogram in compressed form into a byte array
     *
     * @param targetBuffer     The buffer to encode into
     * @param compressionLevel Compression level (for java.util.zip.Deflater).
     * @return The number of bytes written to the buffer
     */
    @Override
    synchronized public int encodeIntoCompressedByteBuffer(
            final ByteBuffer targetBuffer,
            final int compressionLevel) {
        int neededCapacity = getNeededByteBufferCapacity(countsArrayLength);
        if (intermediateUncompressedByteBuffer == null || intermediateUncompressedByteBuffer.capacity() < neededCapacity) {
            intermediateUncompressedByteBuffer = ByteBuffer.allocate(neededCapacity).order(BIG_ENDIAN);
        }
        intermediateUncompressedByteBuffer.clear();
        int initialTargetPosition = targetBuffer.position();

        final int uncompressedLength = encodeIntoByteBuffer(intermediateUncompressedByteBuffer);
        targetBuffer.putInt(getCompressedEncodingCookie());

        targetBuffer.putInt(0); // Placeholder for compressed contents length

        Deflater compressor = new Deflater(compressionLevel);
        compressor.setInput(intermediateUncompressedByteBuffer.array(), 0, uncompressedLength);
        compressor.finish();

        byte[] targetArray;

        if (targetBuffer.hasArray()) {
            targetArray = targetBuffer.array();
        } else {
            if (intermediateUncompressedByteArray == null ||
                    intermediateUncompressedByteArray.length < targetBuffer.capacity()) {
                intermediateUncompressedByteArray = new byte[targetBuffer.capacity()];
            }
            targetArray = intermediateUncompressedByteArray;
        }

        int compressedTargetOffset = initialTargetPosition + 8;
        int compressedDataLength =
                compressor.deflate(
                        targetArray,
                        compressedTargetOffset,
                        targetArray.length - compressedTargetOffset
                );
        compressor.end();

        if (!targetBuffer.hasArray()) {
            targetBuffer.put(targetArray, compressedTargetOffset, compressedDataLength);
        }

        targetBuffer.putInt(initialTargetPosition + 4, compressedDataLength); // Record the compressed length
        int bytesWritten = compressedDataLength + 8;
        targetBuffer.position(initialTargetPosition + bytesWritten);
        return bytesWritten;
    }

    /**
     * Encode this histogram in compressed form into a byte array
     *
     * @param targetBuffer The buffer to encode into
     * @return The number of bytes written to the array
     */
    public int encodeIntoCompressedByteBuffer(final ByteBuffer targetBuffer) {
        return encodeIntoCompressedByteBuffer(targetBuffer, Deflater.DEFAULT_COMPRESSION);
    }

    /**
     * Determine if this histogram is equivalent to another.
     *
     * @param other the other histogram to compare to
     * @return True if this histogram are equivalent with the other.
     */
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof AbstractHistogram)) {
            return false;
        }
        AbstractHistogram that = (AbstractHistogram) other;
        if ((lowestDiscernibleValue != that.lowestDiscernibleValue) ||
                (numberOfSignificantValueDigits != that.numberOfSignificantValueDigits) ||
                (integerToDoubleValueConversionRatio != that.integerToDoubleValueConversionRatio)) {
            return false;
        }
        if (getTotalCount() != that.getTotalCount()) {
            return false;
        }
        if (getMaxValue() != that.getMaxValue()) {
            return false;
        }
        if (getMinNonZeroValue() != that.getMinNonZeroValue()) {
            return false;
        }
        // 2 histograms may be equal but have different underlying array sizes. This can happen for instance due to
        // resizing.
        if (countsArrayLength == that.countsArrayLength) {
            for (int i = 0; i < countsArrayLength; i++) {
                if (getCountAtIndex(i) != that.getCountAtIndex(i)) {
                    return false;
                }
            }
        } else {
            // Comparing the values is valid here because we have already confirmed the histograms have the same total
            // count. It would not be correct otherwise.
            for (HistogramIterationValue value : this.recordedValues()) {
                long countAtValueIteratedTo = value.getCountAtValueIteratedTo();
                long valueIteratedTo = value.getValueIteratedTo();
                if (that.getCountAtValue(valueIteratedTo) != countAtValueIteratedTo) {
                    return false;
                }
            }
        }
        return true;
    }

    //      ###    ##     ## ########  #######
    //     ## ##   ##     ##    ##    ##     ##
    //    ##   ##  ##     ##    ##    ##     ##
    //   ##     ## ##     ##    ##    ##     ##
    //   ######### ##     ##    ##    ##     ##
    //   ##     ## ##     ##    ##    ##     ##
    //   ##     ##  #######     ##     #######
    //
    //   ########  ########  ######  #### ######## #### ##    ##  ######
    //   ##     ## ##       ##    ##  ##       ##   ##  ###   ## ##    ##
    //   ##     ## ##       ##        ##      ##    ##  ####  ## ##
    //   ########  ######    ######   ##     ##     ##  ## ## ## ##   ####
    //   ##   ##   ##             ##  ##    ##      ##  ##  #### ##    ##
    //   ##    ##  ##       ##    ##  ##   ##       ##  ##   ### ##    ##
    //   ##     ## ########  ######  #### ######## #### ##    ##  ######
    //
    // Auto-resizing control:
    //

    /**
     * @return the lowest (and therefore highest precision) bucket index that can represent the value
     */
    public int getBucketIndex(final long value) {
        // Calculates the number of powers of two by which the value is greater than the biggest value that fits in
        // bucket 0. This is the bucket index since each successive bucket can hold a value 2x greater.
        // The mask maps small values to bucket 0.
        return leadingZeroCountBase - Long.numberOfLeadingZeros(value | subBucketMask);
    }

    public abstract long getCountAtIndex(int index);

    /**
     * Get the count of recorded values at a specific value (to within the histogram resolution at the value level).
     *
     * @param value The value for which to provide the recorded count
     * @return The total count of values recorded in the histogram within the value range that is
     * {@literal >=} lowestEquivalentValue(<i>value</i>) and {@literal <=} highestEquivalentValue(<i>value</i>)
     */
    public long getCountAtValue(final long value) throws CairoException {
        final int index = Math.min(Math.max(0, countsArrayIndex(value)), (countsArrayLength - 1));
        return getCountAtIndex(index);
    }

    //   ##     ##    ###    ##       ##     ## ########
    //   ##     ##   ## ##   ##       ##     ## ##
    //   ##     ##  ##   ##  ##       ##     ## ##
    //   ##     ## ##     ## ##       ##     ## ######
    //    ##   ##  ######### ##       ##     ## ##
    //     ## ##   ##     ## ##       ##     ## ##
    //      ###    ##     ## ########  #######  ########
    //
    //   ########  ########  ######   #######  ########  ########  #### ##    ##  ######
    //   ##     ## ##       ##    ## ##     ## ##     ## ##     ##  ##  ###   ## ##    ##
    //   ##     ## ##       ##       ##     ## ##     ## ##     ##  ##  ####  ## ##
    //   ########  ######   ##       ##     ## ########  ##     ##  ##  ## ## ## ##   ####
    //   ##   ##   ##       ##       ##     ## ##   ##   ##     ##  ##  ##  #### ##    ##
    //   ##    ##  ##       ##    ## ##     ## ##    ##  ##     ##  ##  ##   ### ##    ##
    //   ##     ## ########  ######   #######  ##     ## ########  #### ##    ##  ######
    //
    // Value recording support:
    //

    /**
     * Get the count of recorded values within a range of value levels (inclusive to within the histogram's resolution).
     *
     * @param lowValue  The lower value bound on the range for which
     *                  to provide the recorded count. Will be rounded down with
     *                  {@link Histogram#lowestEquivalentValue lowestEquivalentValue}.
     * @param highValue The higher value bound on the range for which to provide the recorded count.
     *                  Will be rounded up with {@link Histogram#highestEquivalentValue highestEquivalentValue}.
     * @return the total count of values recorded in the histogram within the value range that is
     * {@literal >=} lowestEquivalentValue(<i>lowValue</i>) and {@literal <=} highestEquivalentValue(<i>highValue</i>)
     */
    public long getCountBetweenValues(final long lowValue, final long highValue) throws CairoException {
        final int lowIndex = Math.max(0, countsArrayIndex(lowValue));
        final int highIndex = Math.min(countsArrayIndex(highValue), (countsArrayLength - 1));
        long count = 0;
        for (int i = lowIndex; i <= highIndex; i++) {
            count += getCountAtIndex(i);
        }
        return count;
    }

    /**
     * get the end time stamp [optionally] stored with this histogram
     *
     * @return the end time stamp [optionally] stored with this histogram
     */
    @Override
    public long getEndTimeStamp() {
        return endTimeStampMsec;
    }

    /**
     * Provide a (conservatively high) estimate of the Histogram's total footprint in bytes
     *
     * @return a (conservatively high) estimate of the Histogram's total footprint in bytes
     */
    public int getEstimatedFootprintInBytes() {
        return _getEstimatedFootprintInBytes();
    }

    /**
     * get the configured highestTrackableValue
     *
     * @return highestTrackableValue
     */
    public long getHighestTrackableValue() {
        return highestTrackableValue;
    }

    /**
     * get the configured lowestDiscernibleValue
     *
     * @return lowestDiscernibleValue
     */
    public long getLowestDiscernibleValue() {
        return lowestDiscernibleValue;
    }

    /**
     * Get the highest recorded value level in the histogram. If the histogram has no recorded values,
     * the value returned is undefined.
     *
     * @return the Max value recorded in the histogram
     */
    public long getMaxValue() {
        return (maxValue == 0) ? 0 : highestEquivalentValue(maxValue);
    }

    /**
     * Get the highest recorded value level in the histogram as a double
     *
     * @return the Max value recorded in the histogram
     */
    @Override
    public double getMaxValueAsDouble() {
        return getMaxValue();
    }

    /**
     * Get the computed mean value of all recorded values in the histogram
     *
     * @return the mean value (in value units) of the histogram data
     */
    public double getMean() {
        if (getTotalCount() == 0) {
            return 0.0;
        }
        recordedValuesIterator.reset();
        double totalValue = 0;
        while (recordedValuesIterator.hasNext()) {
            HistogramIterationValue iterationValue = recordedValuesIterator.next();
            totalValue += medianEquivalentValue(iterationValue.getValueIteratedTo())
                    * (double) iterationValue.getCountAtValueIteratedTo();
        }
        return (totalValue) / getTotalCount();
    }

    /**
     * Get the lowest recorded non-zero value level in the histogram. If the histogram has no recorded values,
     * the value returned is undefined.
     *
     * @return the lowest recorded non-zero value level in the histogram
     */
    public long getMinNonZeroValue() {
        return (minNonZeroValue == Long.MAX_VALUE) ?
                Long.MAX_VALUE : lowestEquivalentValue(minNonZeroValue);
    }

    /**
     * Get the lowest recorded value level in the histogram. If the histogram has no recorded values,
     * the value returned is undefined.
     *
     * @return the Min value recorded in the histogram
     */
    public long getMinValue() {
        if ((getCountAtIndex(0) > 0) || (getTotalCount() == 0)) {
            return 0;
        }
        return getMinNonZeroValue();
    }

    /**
     * Get the capacity needed to encode this histogram into a ByteBuffer
     *
     * @return the capacity needed to encode this histogram into a ByteBuffer
     */
    @Override
    public int getNeededByteBufferCapacity() {
        return getNeededByteBufferCapacity(countsArrayLength);
    }

    /**
     * get the configured numberOfSignificantValueDigits
     *
     * @return numberOfSignificantValueDigits
     */
    public int getNumberOfSignificantValueDigits() {
        return numberOfSignificantValueDigits;
    }

    //    ######  ##       ########    ###    ########  #### ##    ##  ######
    //   ##    ## ##       ##         ## ##   ##     ##  ##  ###   ## ##    ##
    //   ##       ##       ##        ##   ##  ##     ##  ##  ####  ## ##
    //   ##       ##       ######   ##     ## ########   ##  ## ## ## ##   ####
    //   ##       ##       ##       ######### ##   ##    ##  ##  #### ##    ##
    //   ##    ## ##       ##       ##     ## ##    ##   ##  ##   ### ##    ##
    //    ######  ######## ######## ##     ## ##     ## #### ##    ##  ######
    //
    // Clearing support:
    //

    /**
     * Get the percentile at a given value.
     * The percentile returned is the percentile of values recorded in the histogram that are smaller
     * than or equivalent to the given value.
     * <p>
     * Note that two values are "equivalent" in this statement if
     * {@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram#valuesAreEquivalent} would return true.
     *
     * @param value The value for which to return the associated percentile
     * @return The percentile of values recorded in the histogram that are smaller than or equivalent
     * to the given value.
     */
    public double getPercentileAtOrBelowValue(final long value) {
        if (getTotalCount() == 0) {
            return 100.0;
        }
        final int targetIndex = Math.min(countsArrayIndex(value), (countsArrayLength - 1));
        long totalToCurrentIndex = 0;
        for (int i = 0; i <= targetIndex; i++) {
            totalToCurrentIndex += getCountAtIndex(i);
        }
        return (100.0 * totalToCurrentIndex) / getTotalCount();
    }

    //     ######   #######  ########  ##    ##
    //   ##    ## ##     ## ##     ##  ##  ##
    //   ##       ##     ## ##     ##   ####
    //   ##       ##     ## ########     ##
    //   ##       ##     ## ##           ##
    //   ##    ## ##     ## ##           ##
    //    ######   #######  ##           ##
    //
    // Copy support:
    //

    /**
     * get the start time stamp [optionally] stored with this histogram
     *
     * @return the start time stamp [optionally] stored with this histogram
     */
    @Override
    public long getStartTimeStamp() {
        return startTimeStampMsec;
    }

    /**
     * Get the computed standard deviation of all recorded values in the histogram
     *
     * @return the standard deviation (in value units) of the histogram data
     */
    public double getStdDeviation() {
        if (getTotalCount() == 0) {
            return 0.0;
        }
        final double mean = getMean();
        double geometric_deviation_total = 0.0;
        recordedValuesIterator.reset();
        while (recordedValuesIterator.hasNext()) {
            HistogramIterationValue iterationValue = recordedValuesIterator.next();
            double deviation = (medianEquivalentValue(iterationValue.getValueIteratedTo()) * 1.0) - mean;
            geometric_deviation_total += (deviation * deviation) * iterationValue.getCountAddedInThisIterationStep();
        }
        return Math.sqrt(geometric_deviation_total / getTotalCount());
    }

    public int getSubBucketIndex(final long value, final int bucketIndex) {
        // For bucketIndex 0, this is just value, so it may be anywhere in 0 to subBucketCount.
        // For other bucketIndex, this will always end up in the top half of subBucketCount: assume that for some bucket
        // k > 0, this calculation will yield a value in the bottom half of 0 to subBucketCount. Then, because of how
        // buckets overlap, it would have also been in the top half of bucket k-1, and therefore would have
        // returned k-1 in getBucketIndex(). Since we would then shift it one fewer bits here, it would be twice as big,
        // and therefore in the top half of subBucketCount.
        return (int) (value >>> (bucketIndex + unitMagnitude));
    }

    /**
     * get the tag string [optionally] associated with this histogram
     *
     * @return tag string [optionally] associated with this histogram
     */
    public String getTag() {
        return tag;
    }

    //      ###    ########  ########
    //     ## ##   ##     ## ##     ##
    //    ##   ##  ##     ## ##     ##
    //   ##     ## ##     ## ##     ##
    //   ######### ##     ## ##     ##
    //   ##     ## ##     ## ##     ##
    //   ##     ## ########  ########
    //
    // Add support:
    //

    /**
     * Get the total count of all recorded values in the histogram
     *
     * @return the total count of all recorded values in the histogram
     */
    abstract public long getTotalCount();

    /**
     * Get the value at a given percentile.
     * Returns the largest value that (100% - percentile) [+/- 1 ulp] of the overall recorded value entries
     * in the histogram are either larger than or equivalent to. Returns 0 if no recorded values exist.
     * <p>
     * Note that two values are "equivalent" in this statement if
     * {@link io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram#valuesAreEquivalent} would return true.
     *
     * @param percentile The percentile for which to return the associated value
     * @return The largest value that (100% - percentile) [+/- 1 ulp] of the overall recorded value entries
     * in the histogram are either larger than or equivalent to. Returns 0 if no recorded values exist.
     */
    public long getValueAtPercentile(final double percentile) {
        // Truncate to 0..100%, and remove 1 ulp to avoid roundoff overruns into next bucket when we
        // subsequently round up to the nearest integer:
        double requestedPercentile =
                Math.min(Math.max(Math.nextAfter(percentile, Double.NEGATIVE_INFINITY), 0.0D), 100.0D);
        // derive the count at the requested percentile. We round up to nearest integer to ensure that the
        // largest value that the requested percentile of overall recorded values is <= is actually included.
        double fpCountAtPercentile = (requestedPercentile * getTotalCount()) / 100.0D;
        long countAtPercentile = (long) (Math.ceil(fpCountAtPercentile)); // round up

        countAtPercentile = Math.max(countAtPercentile, 1); // Make sure we at least reach the first recorded entry
        long totalToCurrentIndex = 0;
        for (int i = 0; i < countsArrayLength; i++) {
            totalToCurrentIndex += getCountAtIndex(i);
            if (totalToCurrentIndex >= countAtPercentile) {
                long valueAtIndex = valueFromIndex(i);
                return (percentile == 0.0) ?
                        lowestEquivalentValue(valueAtIndex) :
                        highestEquivalentValue(valueAtIndex);
            }
        }
        return 0;
    }

    @Override
    public int hashCode() {
        int h = 0;
        h = oneAtATimeHashStep(h, unitMagnitude);
        h = oneAtATimeHashStep(h, numberOfSignificantValueDigits);
        h = oneAtATimeHashStep(h, (int) getTotalCount());
        h = oneAtATimeHashStep(h, (int) getMaxValue());
        h = oneAtATimeHashStep(h, (int) getMinNonZeroValue());
        h += (h << 3);
        h ^= (h >> 11);
        h += (h << 15);
        return h;
    }

    //    ######  ##     ## #### ######## ######## #### ##    ##  ######
    //   ##    ## ##     ##  ##  ##          ##     ##  ###   ## ##    ##
    //   ##       ##     ##  ##  ##          ##     ##  ####  ## ##
    //    ######  #########  ##  ######      ##     ##  ## ## ## ##   ####
    //         ## ##     ##  ##  ##          ##     ##  ##  #### ##    ##
    //   ##    ## ##     ##  ##  ##          ##     ##  ##   ### ##    ##
    //    ######  ##     ## #### ##          ##    #### ##    ##  ######
    //
    //
    // Shifting support:
    //

    /**
     * Get the highest value that is equivalent to the given value within the histogram's resolution.
     * Where "equivalent" means that value samples recorded for any two
     * equivalent values are counted in a common total count.
     *
     * @param value The given value
     * @return The highest value that is equivalent to the given value within the histogram's resolution.
     */
    public long highestEquivalentValue(final long value) {
        return nextNonEquivalentValue(value) - 1;
    }

    /**
     * Indicate whether or not the histogram is set to auto-resize and auto-adjust it's
     * highestTrackableValue
     *
     * @return autoResize setting
     */
    public boolean isAutoResize() {
        return autoResize;
    }

    public int leadingZeroCountBase() {
        return leadingZeroCountBase;
    }

    /**
     * Provide a means of iterating through histogram values using linear steps. The iteration is
     * performed in steps of <i>valueUnitsPerBucket</i> in size, terminating when all recorded histogram
     * values are exhausted.
     *
     * @param valueUnitsPerBucket The size (in value units) of the linear buckets to use
     * @return An {@link java.lang.Iterable}{@literal <}{@link HistogramIterationValue}{@literal >}
     * through the histogram using a
     * {@link LinearIterator}
     */
    public LinearBucketValues linearBucketValues(final long valueUnitsPerBucket) {
        return new LinearBucketValues(this, valueUnitsPerBucket);
    }

    /**
     * Provide a means of iterating through histogram values at logarithmically increasing levels. The iteration is
     * performed in steps that start at <i>valueUnitsInFirstBucket</i> and increase exponentially according to
     * <i>logBase</i>, terminating when all recorded histogram values are exhausted.
     *
     * @param valueUnitsInFirstBucket The size (in value units) of the first bucket in the iteration
     * @param logBase                 The multiplier by which bucket sizes will grow in each iteration step
     * @return An {@link java.lang.Iterable}{@literal <}{@link HistogramIterationValue}{@literal >}
     * through the histogram using
     * a {@link LogarithmicIterator}
     */
    public LogarithmicBucketValues logarithmicBucketValues(final long valueUnitsInFirstBucket, final double logBase) {
        return new LogarithmicBucketValues(this, valueUnitsInFirstBucket, logBase);
    }

    /**
     * Get the lowest value that is equivalent to the given value within the histogram's resolution.
     * Where "equivalent" means that value samples recorded for any two
     * equivalent values are counted in a common total count.
     *
     * @param value The given value
     * @return The lowest value that is equivalent to the given value within the histogram's resolution.
     */
    public long lowestEquivalentValue(final long value) {
        final int bucketIndex = getBucketIndex(value);
        final int subBucketIndex = getSubBucketIndex(value, bucketIndex);
        return valueFromIndex(bucketIndex, subBucketIndex);
    }

    //    ######   #######  ##     ## ########     ###    ########  ####  ######   #######  ##    ##
    //   ##    ## ##     ## ###   ### ##     ##   ## ##   ##     ##  ##  ##    ## ##     ## ###   ##
    //   ##       ##     ## #### #### ##     ##  ##   ##  ##     ##  ##  ##       ##     ## ####  ##
    //   ##       ##     ## ## ### ## ########  ##     ## ########   ##   ######  ##     ## ## ## ##
    //   ##       ##     ## ##     ## ##        ######### ##   ##    ##        ## ##     ## ##  ####
    //   ##    ## ##     ## ##     ## ##        ##     ## ##    ##   ##  ##    ## ##     ## ##   ###
    //    ######   #######  ##     ## ##        ##     ## ##     ## ####  ######   #######  ##    ##
    //
    // Comparison support:
    //

    /**
     * Get a value that lies in the middle (rounded up) of the range of values equivalent the given value.
     * Where "equivalent" means that value samples recorded for any two
     * equivalent values are counted in a common total count.
     *
     * @param value The given value
     * @return The value lies in the middle (rounded up) of the range of values equivalent the given value.
     */
    public long medianEquivalentValue(final long value) {
        return (lowestEquivalentValue(value) + (sizeOfEquivalentValueRange(value) >> 1));
    }

    /**
     * Get the next value that is not equivalent to the given value within the histogram's resolution.
     * Where "equivalent" means that value samples recorded for any two
     * equivalent values are counted in a common total count.
     *
     * @param value The given value
     * @return The next value that is not equivalent to the given value within the histogram's resolution.
     */
    public long nextNonEquivalentValue(final long value) {
        return lowestEquivalentValue(value) + sizeOfEquivalentValueRange(value);
    }

    /**
     * Produce textual representation of the value distribution of histogram data by percentile. The distribution is
     * output with exponentially increasing resolution, with each exponentially decreasing half-distance containing
     * five (5) percentile reporting tick points.
     *
     * @param printStream                 Stream into which the distribution will be output
     *                                    <p>
     * @param outputValueUnitScalingRatio The scaling factor by which to divide histogram recorded values units in
     *                                    output
     */
    public void outputPercentileDistribution(final PrintStream printStream,
                                             final Double outputValueUnitScalingRatio) {
        outputPercentileDistribution(printStream, 5, outputValueUnitScalingRatio);
    }

    //    ######  ######## ########  ##     ##  ######  ######## ##     ## ########  ########
    //   ##    ##    ##    ##     ## ##     ## ##    ##    ##    ##     ## ##     ## ##
    //   ##          ##    ##     ## ##     ## ##          ##    ##     ## ##     ## ##
    //    ######     ##    ########  ##     ## ##          ##    ##     ## ########  ######
    //         ##    ##    ##   ##   ##     ## ##          ##    ##     ## ##   ##   ##
    //   ##    ##    ##    ##    ##  ##     ## ##    ##    ##    ##     ## ##    ##  ##
    //    ######     ##    ##     ##  #######   ######     ##     #######  ##     ## ########
    //
    //    #######  ##     ## ######## ########  ##    ## #### ##    ##  ######
    //   ##     ## ##     ## ##       ##     ##  ##  ##   ##  ###   ## ##    ##
    //   ##     ## ##     ## ##       ##     ##   ####    ##  ####  ## ##
    //   ##     ## ##     ## ######   ########     ##     ##  ## ## ## ##   ####
    //   ##  ## ## ##     ## ##       ##   ##      ##     ##  ##  #### ##    ##
    //   ##    ##  ##     ## ##       ##    ##     ##     ##  ##   ### ##    ##
    //    ##### ##  #######  ######## ##     ##    ##    #### ##    ##  ######
    //
    // Histogram structure querying support:
    //

    /**
     * Produce textual representation of the value distribution of histogram data by percentile. The distribution is
     * output with exponentially increasing resolution, with each exponentially decreasing half-distance containing
     * <i>dumpTicksPerHalf</i> percentile reporting tick points.
     *
     * @param printStream                    Stream into which the distribution will be output
     *                                       <p>
     * @param percentileTicksPerHalfDistance The number of reporting points per exponentially decreasing half-distance
     *                                       <p>
     * @param outputValueUnitScalingRatio    The scaling factor by which to divide histogram recorded values units in
     *                                       output
     */
    public void outputPercentileDistribution(final PrintStream printStream,
                                             final int percentileTicksPerHalfDistance,
                                             final Double outputValueUnitScalingRatio) {
        outputPercentileDistribution(printStream, percentileTicksPerHalfDistance, outputValueUnitScalingRatio, false);
    }

    /**
     * Produce textual representation of the value distribution of histogram data by percentile. The distribution is
     * output with exponentially increasing resolution, with each exponentially decreasing half-distance containing
     * <i>dumpTicksPerHalf</i> percentile reporting tick points.
     *
     * @param printStream                    Stream into which the distribution will be output
     *                                       <p>
     * @param percentileTicksPerHalfDistance The number of reporting points per exponentially decreasing half-distance
     *                                       <p>
     * @param outputValueUnitScalingRatio    The scaling factor by which to divide histogram recorded values units in
     *                                       output
     * @param useCsvFormat                   Output in CSV format if true. Otherwise use plain text form.
     */
    public void outputPercentileDistribution(final PrintStream printStream,
                                             final int percentileTicksPerHalfDistance,
                                             final Double outputValueUnitScalingRatio,
                                             final boolean useCsvFormat) {

        if (useCsvFormat) {
            printStream.format("\"Value\",\"Percentile\",\"TotalCount\",\"1/(1-Percentile)\"\n");
        } else {
            printStream.format("%12s %14s %10s %14s\n\n", "Value", "Percentile", "TotalCount", "1/(1-Percentile)");
        }

        PercentileIterator iterator = percentileIterator;
        iterator.reset(percentileTicksPerHalfDistance);

        String percentileFormatString;
        String lastLinePercentileFormatString;
        if (useCsvFormat) {
            percentileFormatString = "%." + numberOfSignificantValueDigits + "f,%.12f,%d,%.2f\n";
            lastLinePercentileFormatString = "%." + numberOfSignificantValueDigits + "f,%.12f,%d,Infinity\n";
        } else {
            percentileFormatString = "%12." + numberOfSignificantValueDigits + "f %2.12f %10d %14.2f\n";
            lastLinePercentileFormatString = "%12." + numberOfSignificantValueDigits + "f %2.12f %10d\n";
        }

        while (iterator.hasNext()) {
            HistogramIterationValue iterationValue = iterator.next();
            if (iterationValue.getPercentileLevelIteratedTo() != 100.0D) {
                printStream.format(Locale.US, percentileFormatString,
                        iterationValue.getValueIteratedTo() / outputValueUnitScalingRatio,
                        iterationValue.getPercentileLevelIteratedTo() / 100.0D,
                        iterationValue.getTotalCountToThisValue(),
                        1 / (1.0D - (iterationValue.getPercentileLevelIteratedTo() / 100.0D)));
            } else {
                printStream.format(Locale.US, lastLinePercentileFormatString,
                        iterationValue.getValueIteratedTo() / outputValueUnitScalingRatio,
                        iterationValue.getPercentileLevelIteratedTo() / 100.0D,
                        iterationValue.getTotalCountToThisValue());
            }
        }

        if (!useCsvFormat) {
            // Calculate and output mean and std. deviation.
            // Note: mean/std. deviation numbers are very often completely irrelevant when
            // data is extremely non-normal in distribution (e.g. in cases of strong multi-modal
            // response time distribution associated with GC pauses). However, reporting these numbers
            // can be very useful for contrasting with the detailed percentile distribution
            // reported by outputPercentileDistribution(). It is not at all surprising to find
            // percentile distributions where results fall many tens or even hundreds of standard
            // deviations away from the mean - such results simply indicate that the data sampled
            // exhibits a very non-normal distribution, highlighting situations for which the std.
            // deviation metric is a useless indicator.
            //

            double mean = getMean() / outputValueUnitScalingRatio;
            double std_deviation = getStdDeviation() / outputValueUnitScalingRatio;
            printStream.format(Locale.US,
                    "#[Mean    = %12." + numberOfSignificantValueDigits + "f, StdDeviation   = %12." +
                            numberOfSignificantValueDigits + "f]\n",
                    mean, std_deviation);
            printStream.format(Locale.US,
                    "#[Max     = %12." + numberOfSignificantValueDigits + "f, Total count    = %12d]\n",
                    getMaxValue() / outputValueUnitScalingRatio, getTotalCount());
            printStream.format(Locale.US, "#[Buckets = %12d, SubBuckets     = %12d]\n",
                    bucketCount, subBucketCount);
        }
    }

    /**
     * Provide a means of iterating through histogram values according to percentile levels. The iteration is
     * performed in steps that start at 0% and reduce their distance to 100% according to the
     * <i>percentileTicksPerHalfDistance</i> parameter, ultimately reaching 100% when all recorded histogram
     * values are exhausted.
     * <p>
     *
     * @param percentileTicksPerHalfDistance The number of iteration steps per half-distance to 100%.
     * @return An {@link java.lang.Iterable}{@literal <}{@link HistogramIterationValue}{@literal >}
     * through the histogram using a
     * {@link PercentileIterator}
     */
    public Percentiles percentiles(final int percentileTicksPerHalfDistance) {
        return new Percentiles(this, percentileTicksPerHalfDistance);
    }

    public void recordConvertedDoubleValueWithCount(final double value, final long count) throws CairoException {
        long integerValue = (long) (value * doubleToIntegerValueConversionRatio);
        recordCountAtValue(count, integerValue);
    }

    /**
     * Record a value in the histogram
     *
     * @param value The value to be recorded
     * @throws CairoException (may throw) if value exceeds highestTrackableValue
     */
    @Override
    public void recordValue(final long value) throws CairoException {
        recordSingleValue(value);
    }

    /**
     * Record a value in the histogram (adding to the value's current count)
     *
     * @param value The value to be recorded
     * @param count The number of occurrences of this value to record
     * @throws CairoException (may throw) if value exceeds highestTrackableValue
     */
    @Override
    public void recordValueWithCount(final long value, final long count) throws CairoException {
        recordCountAtValue(count, value);
    }

    /**
     * Record a value in the histogram.
     * <p>
     * To compensate for the loss of sampled values when a recorded value is larger than the expected
     * interval between value samples, Histogram will auto-generate an additional series of decreasingly-smaller
     * (down to the expectedIntervalBetweenValueSamples) value records.
     * <p>
     * Note: This is a at-recording correction method, as opposed to the post-recording correction method provided
     * by {@link #copyCorrectedForCoordinatedOmission(long)}.
     * The two methods are mutually exclusive, and only one of the two should be be used on a given data set to correct
     * for the same coordinated omission issue.
     * <p>
     * See notes in the description of the Histogram calls for an illustration of why this corrective behavior is
     * important.
     *
     * @param value                               The value to record
     * @param expectedIntervalBetweenValueSamples If expectedIntervalBetweenValueSamples is larger than 0, add
     *                                            auto-generated value records as appropriate if value is larger
     *                                            than expectedIntervalBetweenValueSamples
     * @throws CairoException (may throw) if value exceeds highestTrackableValue
     */
    @Override
    public void recordValueWithExpectedInterval(final long value, final long expectedIntervalBetweenValueSamples)
            throws CairoException {
        recordSingleValueWithExpectedInterval(value, expectedIntervalBetweenValueSamples);
    }

    /**
     * Provide a means of iterating through all recorded histogram values using the finest granularity steps
     * supported by the underlying representation. The iteration steps through all non-zero recorded value counts,
     * and terminates when all recorded histogram values are exhausted.
     *
     * @return An {@link java.lang.Iterable}{@literal <}{@link HistogramIterationValue}{@literal >}
     * through the histogram using
     * a {@link RecordedValuesIterator}
     */
    public RecordedValues recordedValues() {
        return new RecordedValues(this);
    }

    /**
     * Reset the contents and stats of this histogram
     */
    @Override
    public void reset() {
        clearCounts();
        resetMaxValue(0);
        resetMinNonZeroValue(Long.MAX_VALUE);
        setNormalizingIndexOffset(0);
        startTimeStampMsec = Long.MAX_VALUE;
        endTimeStampMsec = 0;
        tag = null;
    }

    //   ######## #### ##     ## ########  ######  ########    ###    ##     ## ########
    //      ##     ##  ###   ### ##       ##    ##    ##      ## ##   ###   ### ##     ##
    //      ##     ##  #### #### ##       ##          ##     ##   ##  #### #### ##     ##
    //      ##     ##  ## ### ## ######    ######     ##    ##     ## ## ### ## ########
    //      ##     ##  ##     ## ##             ##    ##    ######### ##     ## ##
    //      ##     ##  ##     ## ##       ##    ##    ##    ##     ## ##     ## ##
    //      ##    #### ##     ## ########  ######     ##    ##     ## ##     ## ##
    //
    //     ####       ########    ###     ######
    //    ##  ##         ##      ## ##   ##    ##
    //     ####          ##     ##   ##  ##
    //    ####           ##    ##     ## ##   ####
    //   ##  ## ##       ##    ######### ##    ##
    //   ##   ##         ##    ##     ## ##    ##
    //    ####  ##       ##    ##     ##  ######
    //
    // Timestamp and tag support:
    //

    /**
     * Control whether or not the histogram can auto-resize and auto-adjust it's
     * highestTrackableValue
     *
     * @param autoResize autoResize setting
     */
    public void setAutoResize(boolean autoResize) {
        this.autoResize = autoResize;
    }

    /**
     * Set the end time stamp value associated with this histogram to a given value.
     *
     * @param timeStampMsec the value to set the time stamp to, [by convention] in msec since the epoch.
     */
    @Override
    public void setEndTimeStamp(final long timeStampMsec) {
        this.endTimeStampMsec = timeStampMsec;
    }

    /**
     * Set the start time stamp value associated with this histogram to a given value.
     *
     * @param timeStampMsec the value to set the time stamp to, [by convention] in msec since the epoch.
     */
    @Override
    public void setStartTimeStamp(final long timeStampMsec) {
        this.startTimeStampMsec = timeStampMsec;
    }

    /**
     * Set the tag string associated with this histogram
     *
     * @param tag the tag string to assciate with this histogram
     */
    public void setTag(String tag) {
        this.tag = tag;
    }

    /**
     * Shift recorded values to the left (the equivalent of a &lt;&lt; shift operation on all recorded values). The
     * configured integer value range limits and value precision setting will remain unchanged.
     * <p>
     * An {@link CairoException} will be thrown if any recorded values may be lost
     * as a result of the attempted operation, reflecting an "overflow" conditions. Expect such an overflow
     * exception if the operation would cause the current maxValue to be scaled to a value that is outside
     * of the covered value range.
     *
     * @param numberOfBinaryOrdersOfMagnitude The number of binary orders of magnitude to shift by
     */
    public void shiftValuesLeft(final int numberOfBinaryOrdersOfMagnitude) {
        shiftValuesLeft(numberOfBinaryOrdersOfMagnitude, integerToDoubleValueConversionRatio);
    }

    /**
     * Shift recorded values to the right (the equivalent of a &gt;&gt; shift operation on all recorded values). The
     * configured integer value range limits and value precision setting will remain unchanged.
     * <p>
     * Shift right operations that do not underflow are reversible with a shift left operation with no loss of
     * information. An {@link CairoException} reflecting an "underflow" conditions will be thrown
     * if any recorded values may lose representation accuracy as a result of the attempted shift operation.
     * <p>
     * For a shift of a single order of magnitude, expect such an underflow exception if any recorded non-zero
     * values up to [numberOfSignificantValueDigits (rounded up to nearest power of 2) multiplied by
     * (2 ^ numberOfBinaryOrdersOfMagnitude) currently exist in the histogram.
     *
     * @param numberOfBinaryOrdersOfMagnitude The number of binary orders of magnitude to shift by
     */
    public void shiftValuesRight(final int numberOfBinaryOrdersOfMagnitude) {
        shiftValuesRight(numberOfBinaryOrdersOfMagnitude, integerToDoubleValueConversionRatio);
    }

    //   ########     ###    ########    ###          ###     ######   ######  ########  ######   ######
    //   ##     ##   ## ##      ##      ## ##        ## ##   ##    ## ##    ## ##       ##    ## ##    ##
    //   ##     ##  ##   ##     ##     ##   ##      ##   ##  ##       ##       ##       ##       ##
    //   ##     ## ##     ##    ##    ##     ##    ##     ## ##       ##       ######    ######   ######
    //   ##     ## #########    ##    #########    ######### ##       ##       ##             ##       ##
    //   ##     ## ##     ##    ##    ##     ##    ##     ## ##    ## ##    ## ##       ##    ## ##    ##
    //   ########  ##     ##    ##    ##     ##    ##     ##  ######   ######  ########  ######   ######
    //
    // Histogram Data access support:
    //

    /**
     * Get the size (in value units) of the range of values that are equivalent to the given value within the
     * histogram's resolution. Where "equivalent" means that value samples recorded for any two
     * equivalent values are counted in a common total count.
     *
     * @param value The given value
     * @return The size of the range of values equivalent to the given value.
     */
    public long sizeOfEquivalentValueRange(final long value) {
        final int bucketIndex = getBucketIndex(value);
        return 1L << (unitMagnitude + bucketIndex);
    }

    /**
     * Subtract the contents of another histogram from this one.
     * <p>
     * The start/end timestamps of this histogram will remain unchanged.
     *
     * @param otherHistogram The other histogram.
     * @throws CairoException (may throw) if values in otherHistogram's are higher than highestTrackableValue.
     */
    public void subtract(final AbstractHistogram otherHistogram)
            throws CairoException, IllegalArgumentException {
        if (highestEquivalentValue(otherHistogram.getMaxValue()) >
                highestEquivalentValue(valueFromIndex(this.countsArrayLength - 1))) {
            throw new IllegalArgumentException(
                    "The other histogram includes values that do not fit in this histogram's range.");
        }
        for (int i = 0; i < otherHistogram.countsArrayLength; i++) {
            long otherCount = otherHistogram.getCountAtIndex(i);
            if (otherCount > 0) {
                long otherValue = otherHistogram.valueFromIndex(i);
                if (getCountAtValue(otherValue) < otherCount) {
                    throw new IllegalArgumentException("otherHistogram count (" + otherCount + ") at value " +
                            otherValue + " is larger than this one's (" + getCountAtValue(otherValue) + ")");
                }
                recordValueWithCount(otherValue, -otherCount);
            }
        }
        // With subtraction, the max and minNonZero values could have changed:
        if ((getCountAtValue(getMaxValue()) <= 0) || getCountAtValue(getMinNonZeroValue()) <= 0) {
            establishInternalTackingValues();
        }
    }

    /**
     * Indicate whether or not the histogram is capable of supporting auto-resize functionality.
     * Note that this is an indication that enabling auto-resize by calling setAutoResize() is allowed,
     * and NOT that the histogram will actually auto-resize. Use isAutoResize() to determine if
     * the histogram is in auto-resize mode.
     *
     * @return autoResize setting
     */
    public boolean supportsAutoResize() {
        return true;
    }

    @Override
    public String toString() {
        String output = "AbstractHistogram:\n";
        output += super.toString();
        output += recordedValuesToString();
        return output;
    }

    public int unitMagnitude() {
        return unitMagnitude;
    }

    public final long valueFromIndex(final int index) {
        int bucketIndex = (index >> subBucketHalfCountMagnitude) - 1;
        int subBucketIndex = (index & (subBucketHalfCount - 1)) + subBucketHalfCount;
        if (bucketIndex < 0) {
            subBucketIndex -= subBucketHalfCount;
            bucketIndex = 0;
        }
        return valueFromIndex(bucketIndex, subBucketIndex);
    }

    /**
     * Determine if two values are equivalent with the histogram's resolution.
     * Where "equivalent" means that value samples recorded for any two
     * equivalent values are counted in a common total count.
     *
     * @param value1 first value to compare
     * @param value2 second value to compare
     * @return True if values are equivalent with the histogram's resolution.
     */
    public boolean valuesAreEquivalent(final long value1, final long value2) {
        return (lowestEquivalentValue(value1) == lowestEquivalentValue(value2));
    }

    private static <T extends AbstractHistogram> T decodeFromByteBuffer(
            final ByteBuffer buffer,
            final Class<T> histogramClass,
            final long minBarForHighestTrackableValue,
            final Inflater decompressor) throws DataFormatException {

        final int cookie = buffer.getInt();
        final int payloadLengthInBytes;
        final int normalizingIndexOffset;
        final int numberOfSignificantValueDigits;
        final long lowestTrackableUnitValue;
        long highestTrackableValue;
        final double integerToDoubleValueConversionRatio;

        if ((getCookieBase(cookie) == encodingCookieBase) ||
                (getCookieBase(cookie) == V1EncodingCookieBase)) {
            if (getCookieBase(cookie) == V2EncodingCookieBase) {
                if (getWordSizeInBytesFromCookie(cookie) != V2maxWordSizeInBytes) {
                    throw new IllegalArgumentException(
                            "The buffer does not contain a Histogram (no valid cookie found)");
                }
            }
            payloadLengthInBytes = buffer.getInt();
            normalizingIndexOffset = buffer.getInt();
            numberOfSignificantValueDigits = buffer.getInt();
            lowestTrackableUnitValue = buffer.getLong();
            highestTrackableValue = buffer.getLong();
            integerToDoubleValueConversionRatio = buffer.getDouble();
        } else if (getCookieBase(cookie) == V0EncodingCookieBase) {
            numberOfSignificantValueDigits = buffer.getInt();
            lowestTrackableUnitValue = buffer.getLong();
            highestTrackableValue = buffer.getLong();
            buffer.getLong(); // Discard totalCount field in V0 header.
            payloadLengthInBytes = Integer.MAX_VALUE;
            integerToDoubleValueConversionRatio = 1.0;
            normalizingIndexOffset = 0;
        } else {
            throw new IllegalArgumentException("The buffer does not contain a Histogram (no valid cookie found)");
        }
        highestTrackableValue = Math.max(highestTrackableValue, minBarForHighestTrackableValue);

        T histogram;

        // Construct histogram:
        try {
            Constructor<T> constructor = histogramClass.getConstructor(constructorArgsTypes);
            histogram = constructor.newInstance(lowestTrackableUnitValue, highestTrackableValue,
                    numberOfSignificantValueDigits);
            histogram.setIntegerToDoubleValueConversionRatio(integerToDoubleValueConversionRatio);
            histogram.setNormalizingIndexOffset(normalizingIndexOffset);
            try {
                histogram.setAutoResize(true);
            } catch (IllegalStateException ex) {
                // Allow histogram to refuse auto-sizing setting
            }
        } catch (IllegalAccessException | NoSuchMethodException |
                 InstantiationException | InvocationTargetException ex) {
            throw new IllegalArgumentException(ex);
        }

        ByteBuffer payLoadSourceBuffer;

        final int expectedCapacity =
                Math.min(
                        histogram.getNeededV0PayloadByteBufferCapacity(histogram.countsArrayLength),
                        payloadLengthInBytes
                );

        if (decompressor == null) {
            // No compressed source buffer. Payload is in buffer, after header.
            if (expectedCapacity > buffer.remaining()) {
                throw new IllegalArgumentException("The buffer does not contain the full Histogram payload");
            }
            payLoadSourceBuffer = buffer;
        } else {
            // Compressed source buffer. Payload needs to be decoded from there.
            payLoadSourceBuffer = ByteBuffer.allocate(expectedCapacity).order(BIG_ENDIAN);
            int decompressedByteCount = decompressor.inflate(payLoadSourceBuffer.array());
            if ((payloadLengthInBytes != Integer.MAX_VALUE) && (decompressedByteCount < payloadLengthInBytes)) {
                throw new IllegalArgumentException("The buffer does not contain the indicated payload amount");
            }
        }

        int filledLength = ((AbstractHistogram) histogram).fillCountsArrayFromSourceBuffer(
                payLoadSourceBuffer,
                expectedCapacity,
                getWordSizeInBytesFromCookie(cookie));


        histogram.establishInternalTackingValues(filledLength);

        return histogram;
    }

    private static int getCookieBase(final int cookie) {
        return (cookie & ~0xf0);
    }

    private static int getWordSizeInBytesFromCookie(final int cookie) {
        if ((getCookieBase(cookie) == V2EncodingCookieBase) ||
                (getCookieBase(cookie) == V2CompressedEncodingCookieBase)) {
            return V2maxWordSizeInBytes;
        }
        int sizeByte = (cookie & 0xf0) >> 4;
        return sizeByte & 0xe;
    }

    //   #### ######## ######## ########     ###    ######## ####  #######  ##    ##
    //    ##     ##    ##       ##     ##   ## ##      ##     ##  ##     ## ###   ##
    //    ##     ##    ##       ##     ##  ##   ##     ##     ##  ##     ## ####  ##
    //    ##     ##    ######   ########  ##     ##    ##     ##  ##     ## ## ## ##
    //    ##     ##    ##       ##   ##   #########    ##     ##  ##     ## ##  ####
    //    ##     ##    ##       ##    ##  ##     ##    ##     ##  ##     ## ##   ###
    //   ####    ##    ######## ##     ## ##     ##    ##    ####  #######  ##    ##
    //
    // Iteration Support:
    //

    private int countsArrayIndex(final int bucketIndex, final int subBucketIndex) {
        assert (subBucketIndex < subBucketCount);
        assert (bucketIndex == 0 || (subBucketIndex >= subBucketHalfCount));
        // Calculate the index for the first entry that will be used in the bucket (halfway through subBucketCount).
        // For bucketIndex 0, all subBucketCount entries may be used, but bucketBaseIndex is still set in the middle.
        final int bucketBaseIndex = (bucketIndex + 1) << subBucketHalfCountMagnitude;
        // Calculate the offset in the bucket. This subtraction will result in a positive value in all buckets except
        // the 0th bucket (since a value in that bucket may be less than half the bucket's 0 to subBucketCount range).
        // However, this works out since we give bucket 0 twice as much space.
        final int offsetInBucket = subBucketIndex - subBucketHalfCount;
        // The following is the equivalent of ((subBucketIndex  - subBucketHalfCount) + bucketBaseIndex;
        return bucketBaseIndex + offsetInBucket;
    }

    private int fillCountsArrayFromSourceBuffer(ByteBuffer sourceBuffer, int lengthInBytes, int wordSizeInBytes) {
        if ((wordSizeInBytes != 2) && (wordSizeInBytes != 4) &&
                (wordSizeInBytes != 8) && (wordSizeInBytes != V2maxWordSizeInBytes)) {
            throw new IllegalArgumentException("word size must be 2, 4, 8, or V2maxWordSizeInBytes (" +
                    V2maxWordSizeInBytes + ") bytes");
        }
        final long maxAllowableCountInHistigram =
                ((this.wordSizeInBytes == 2) ? Short.MAX_VALUE :
                        ((this.wordSizeInBytes == 4) ? Integer.MAX_VALUE : Long.MAX_VALUE)
                );

        int dstIndex = 0;
        int endPosition = sourceBuffer.position() + lengthInBytes;
        while (sourceBuffer.position() < endPosition) {
            long count;
            int zerosCount = 0;
            if (wordSizeInBytes == V2maxWordSizeInBytes) {
                // V2 encoding format uses a long encoded in a ZigZag LEB128 format (up to V2maxWordSizeInBytes):
                count = ZigZagEncoding.getLong(sourceBuffer);
                if (count < 0) {
                    long zc = -count;
                    if (zc > Integer.MAX_VALUE) {
                        throw new IllegalArgumentException(
                                "An encoded zero count of > Integer.MAX_VALUE was encountered in the source");
                    }
                    zerosCount = (int) zc;
                }
            } else {
                // decoding V1 and V0 encoding formats depends on indicated word size:
                count =
                        ((wordSizeInBytes == 2) ? sourceBuffer.getShort() :
                                ((wordSizeInBytes == 4) ? sourceBuffer.getInt() :
                                        sourceBuffer.getLong()
                                )
                        );
            }
            if (count > maxAllowableCountInHistigram) {
                throw new IllegalArgumentException(
                        "An encoded count (" + count +
                                ") does not fit in the Histogram's (" +
                                this.wordSizeInBytes + " bytes) was encountered in the source");
            }
            if (zerosCount > 0) {
                dstIndex += zerosCount; // No need to set zeros in array. Just skip them.
            } else {
                setCountAtIndex(dstIndex++, count);
            }
        }
        return dstIndex; // this is the destination length
    }

    private int getCompressedEncodingCookie() {
        return compressedEncodingCookieBase | 0x10; // LSBit of wordsize byte indicates TLZE Encoding
    }

    private int getEncodingCookie() {
        return encodingCookieBase | 0x10; // LSBit of wordsize byte indicates TLZE Encoding
    }

    private void handleRecordException(final long count, final long value, Exception ex) {
        if (!autoResize) {
            throw CairoException.nonCritical().put("value ").put(value)
                    .put(" outside of histogram covered range. Caused by: ").put(ex.getMessage());
        }
        resize(value);
        int countsIndex = countsArrayIndex(value);
        addToCountAtIndex(countsIndex, count);
        this.highestTrackableValue = highestEquivalentValue(valueFromIndex(countsArrayLength - 1));
    }

    // Percentile iterator support:

    private void init(final long lowestDiscernibleValue,
                      final long highestTrackableValue,
                      final int numberOfSignificantValueDigits,
                      final double integerToDoubleValueConversionRatio,
                      final int normalizingIndexOffset) {
        this.lowestDiscernibleValue = lowestDiscernibleValue;
        this.highestTrackableValue = highestTrackableValue;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
        this.integerToDoubleValueConversionRatio = integerToDoubleValueConversionRatio;
        if (normalizingIndexOffset != 0) {
            setNormalizingIndexOffset(normalizingIndexOffset);
        }

        /*
         * Given a 3 decimal point accuracy, the expectation is obviously for "+/- 1 unit at 1000". It also means that
         * it's "ok to be +/- 2 units at 2000". The "tricky" thing is that it is NOT ok to be +/- 2 units at 1999. Only
         * starting at 2000. So internally, we need to maintain single unit resolution to 2x 10^decimalPoints.
         */
        final long largestValueWithSingleUnitResolution = 2 * (long) Math.pow(10, numberOfSignificantValueDigits);

        unitMagnitude = (int) (Math.log(lowestDiscernibleValue) / Math.log(2));
        unitMagnitudeMask = (1L << unitMagnitude) - 1;

        // We need to maintain power-of-two subBucketCount (for clean direct indexing) that is large enough to
        // provide unit resolution to at least largestValueWithSingleUnitResolution. So figure out
        // largestValueWithSingleUnitResolution's nearest power-of-two (rounded up), and use that:
        int subBucketCountMagnitude = (int) Math.ceil(Math.log(largestValueWithSingleUnitResolution) / Math.log(2));
        subBucketHalfCountMagnitude = subBucketCountMagnitude - 1;
        subBucketCount = 1 << subBucketCountMagnitude;
        subBucketHalfCount = subBucketCount / 2;
        subBucketMask = ((long) subBucketCount - 1) << unitMagnitude;

        if (subBucketCountMagnitude + unitMagnitude > 62) {
            // subBucketCount entries can't be represented, with unitMagnitude applied, in a positive long.
            // Technically it still sort of works if their sum is 63: you can represent all but the last number
            // in the shifted subBucketCount. However, the utility of such a histogram vs ones whose magnitude here
            // fits in 62 bits is debatable, and it makes it harder to work through the logic.
            // Sums larger than 64 are totally broken as leadingZeroCountBase would go negative.
            throw new IllegalArgumentException("Cannot represent numberOfSignificantValueDigits worth of values " +
                    "beyond lowestDiscernibleValue");
        }

        // determine exponent range needed to support the trackable value with no overflow:
        establishSize(highestTrackableValue);

        // Establish leadingZeroCountBase, used in getBucketIndex() fast path:
        // subtract the bits that would be used by the largest value in bucket 0.
        leadingZeroCountBase = 64 - unitMagnitude - subBucketCountMagnitude;

        percentileIterator = new PercentileIterator(this, 1);
        recordedValuesIterator = new RecordedValuesIterator(this);
    }

    // Linear iterator support:

    private int oneAtATimeHashStep(int h, final int v) {
        h += v;
        h += (h << 10);
        h ^= (h >> 6);
        return h;
    }

    // Logarithmic iterator support:

    private void readObject(final ObjectInputStream o)
            throws IOException, ClassNotFoundException {
        final long lowestDiscernibleValue = o.readLong();
        final long highestTrackableValue = o.readLong();
        final int numberOfSignificantValueDigits = o.readInt();
        final int normalizingIndexOffset = o.readInt();
        final double integerToDoubleValueConversionRatio = o.readDouble();
        final long indicatedTotalCount = o.readLong();
        final long indicatedMaxValue = o.readLong();
        final long indicatedMinNonZeroValue = o.readLong();
        final long indicatedStartTimeStampMsec = o.readLong();
        final long indicatedEndTimeStampMsec = o.readLong();
        final boolean indicatedAutoResize = o.readBoolean();
        final int indicatedwordSizeInBytes = o.readInt();

        init(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits,
                integerToDoubleValueConversionRatio, normalizingIndexOffset);
        // Set internalTrackingValues (can't establish them from array yet, because it's not yet read...)
        setTotalCount(indicatedTotalCount);
        maxValue = indicatedMaxValue;
        minNonZeroValue = indicatedMinNonZeroValue;
        startTimeStampMsec = indicatedStartTimeStampMsec;
        endTimeStampMsec = indicatedEndTimeStampMsec;
        autoResize = indicatedAutoResize;
        wordSizeInBytes = indicatedwordSizeInBytes;
    }

    // Recorded value iterator support:

    private void recordCountAtValue(final long count, final long value) throws CairoException {
        int countsIndex = countsArrayIndex(value);
        try {
            addToCountAtIndex(countsIndex, count);
        } catch (CairoException ex) {
            handleRecordException(count, value, ex);
        }
        updateMinAndMax(value);
        addToTotalCount(count);
    }

    // AllValues iterator support:

    private void recordSingleValue(final long value) throws CairoException {
        int countsIndex = countsArrayIndex(value);
        try {
            incrementCountAtIndex(countsIndex);
        } catch (CairoException | ArrayIndexOutOfBoundsException ex) {
            handleRecordException(1, value, ex);
        }
        updateMinAndMax(value);
        incrementTotalCount();
    }

    //   ########  ######## ########   ######  ######## ##    ## ######## #### ##       ########
    //   ##     ## ##       ##     ## ##    ## ##       ###   ##    ##     ##  ##       ##
    //   ##     ## ##       ##     ## ##       ##       ####  ##    ##     ##  ##       ##
    //   ########  ######   ########  ##       ######   ## ## ##    ##     ##  ##       ######
    //   ##        ##       ##   ##   ##       ##       ##  ####    ##     ##  ##       ##
    //   ##        ##       ##    ##  ##    ## ##       ##   ###    ##     ##  ##       ##
    //   ##        ######## ##     ##  ######  ######## ##    ##    ##    #### ######## ########
    //
    //    #######  ##     ## ######## ########  ##     ## ########
    //   ##     ## ##     ##    ##    ##     ## ##     ##    ##
    //   ##     ## ##     ##    ##    ##     ## ##     ##    ##
    //   ##     ## ##     ##    ##    ########  ##     ##    ##
    //   ##     ## ##     ##    ##    ##        ##     ##    ##
    //   ##     ## ##     ##    ##    ##        ##     ##    ##
    //    #######   #######     ##    ##         #######     ##
    //
    // Textual percentile output support:
    //

    private void recordSingleValueWithExpectedInterval(final long value, final long expectedIntervalBetweenValueSamples)
            throws CairoException {
        recordSingleValue(value);
        if (expectedIntervalBetweenValueSamples <= 0)
            return;
        for (long missingValue = value - expectedIntervalBetweenValueSamples;
             missingValue >= expectedIntervalBetweenValueSamples;
             missingValue -= expectedIntervalBetweenValueSamples) {
            recordSingleValue(missingValue);
        }
    }

    private void recordValueWithCountAndExpectedInterval(
            final long value,
            final long count,
            final long expectedIntervalBetweenValueSamples
    ) throws CairoException {
        recordCountAtValue(count, value);
        if (expectedIntervalBetweenValueSamples <= 0)
            return;
        for (long missingValue = value - expectedIntervalBetweenValueSamples;
             missingValue >= expectedIntervalBetweenValueSamples;
             missingValue -= expectedIntervalBetweenValueSamples) {
            recordCountAtValue(count, missingValue);
        }
    }

    private String recordedValuesToString() {
        String output = "";
        try {
            for (int i = 0; i < countsArrayLength; i++) {
                if (getCountAtIndex(i) != 0) {
                    output += String.format("[%d] : %d\n", i, getCountAtIndex(i));
                }
            }
            return output;
        } catch (Exception ex) {
            output += "!!! Exception thown in value iteration...\n";
        }
        return output;
    }

    //    ######  ######## ########  ####    ###    ##       #### ########    ###    ######## ####  #######  ##    ##
    //   ##    ## ##       ##     ##  ##    ## ##   ##        ##       ##    ## ##      ##     ##  ##     ## ###   ##
    //   ##       ##       ##     ##  ##   ##   ##  ##        ##      ##    ##   ##     ##     ##  ##     ## ####  ##
    //    ######  ######   ########   ##  ##     ## ##        ##     ##    ##     ##    ##     ##  ##     ## ## ## ##
    //         ## ##       ##   ##    ##  ######### ##        ##    ##     #########    ##     ##  ##     ## ##  ####
    //   ##    ## ##       ##    ##   ##  ##     ## ##        ##   ##      ##     ##    ##     ##  ##     ## ##   ###
    //    ######  ######## ##     ## #### ##     ## ######## #### ######## ##     ##    ##    ####  #######  ##    ##
    //
    // Serialization support:
    //

    private void resetMaxValue(final long maxValue) {
        this.maxValue = maxValue | unitMagnitudeMask; // Max unit-equivalent value
    }

    private void resetMinNonZeroValue(final long minNonZeroValue) {
        final long internalValue = minNonZeroValue & ~unitMagnitudeMask; // Min unit-equivalent value
        this.minNonZeroValue = (minNonZeroValue == Long.MAX_VALUE) ?
                minNonZeroValue : internalValue;
    }

    private void shiftLowestHalfBucketContentsLeft(int shiftAmount, int preShiftZeroIndex) {
        final int numberOfBinaryOrdersOfMagnitude = shiftAmount >> subBucketHalfCountMagnitude;

        // The lowest half-bucket (not including the 0 value) is special: unlike all other half
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
            long countAtFromIndex = getCountAtNormalizedIndex(fromIndex + preShiftZeroIndex);
            setCountAtIndex(toIndex, countAtFromIndex);
            setCountAtNormalizedIndex(fromIndex + preShiftZeroIndex, 0);
        }

        // Note that the above loop only creates O(N) work for histograms that have values in
        // the lowest half-bucket (excluding the 0 value). Histograms that never have values
        // there (e.g. all integer value histograms used as internal storage in DoubleHistograms)
        // will never loop, and their shifts will remain O(1).
    }

    //   ######## ##    ##  ######   #######  ########  #### ##    ##  ######
    //   ##       ###   ## ##    ## ##     ## ##     ##  ##  ###   ## ##    ##
    //   ##       ####  ## ##       ##     ## ##     ##  ##  ####  ## ##
    //   ######   ## ## ## ##       ##     ## ##     ##  ##  ## ## ## ##   ####
    //   ##       ##  #### ##       ##     ## ##     ##  ##  ##  #### ##    ##
    //   ##       ##   ### ##    ## ##     ## ##     ##  ##  ##   ### ##    ##
    //   ######## ##    ##  ######   #######  ########  #### ##    ##  ######
    //
    //     ####       ########  ########  ######   #######  ########  #### ##    ##  ######
    //    ##  ##      ##     ## ##       ##    ## ##     ## ##     ##  ##  ###   ## ##    ##
    //     ####       ##     ## ##       ##       ##     ## ##     ##  ##  ####  ## ##
    //    ####        ##     ## ######   ##       ##     ## ##     ##  ##  ## ## ## ##   ####
    //   ##  ## ##    ##     ## ##       ##       ##     ## ##     ##  ##  ##  #### ##    ##
    //   ##   ##      ##     ## ##       ##    ## ##     ## ##     ##  ##  ##   ### ##    ##
    //    ####  ##    ########  ########  ######   #######  ########  #### ##    ##  ######
    //
    // Encoding/Decoding support:
    //

    private long valueFromIndex(final int bucketIndex, final int subBucketIndex) {
        return ((long) subBucketIndex) << (bucketIndex + unitMagnitude);
    }

    private void writeObject(final ObjectOutputStream o)
            throws IOException {
        o.writeLong(lowestDiscernibleValue);
        o.writeLong(highestTrackableValue);
        o.writeInt(numberOfSignificantValueDigits);
        o.writeInt(getNormalizingIndexOffset());
        o.writeDouble(integerToDoubleValueConversionRatio);
        o.writeLong(getTotalCount());
        // Max Value is added to the serialized form because establishing max via scanning is "harder" during
        // deserialization, as the counts array is not available at the subclass deserializing level, and we don't
        // really want to have each subclass establish max on it's own...
        o.writeLong(maxValue);
        o.writeLong(minNonZeroValue);
        o.writeLong(startTimeStampMsec);
        o.writeLong(endTimeStampMsec);
        o.writeBoolean(autoResize);
        o.writeInt(wordSizeInBytes);
    }

    static <T extends AbstractHistogram> T decodeFromByteBuffer(
            final ByteBuffer buffer,
            final Class<T> histogramClass,
            final long minBarForHighestTrackableValue) {
        try {
            return decodeFromByteBuffer(buffer, histogramClass, minBarForHighestTrackableValue, null);
        } catch (DataFormatException ex) {
            throw new RuntimeException(ex);
        }
    }

    static <T extends AbstractHistogram> T decodeFromCompressedByteBuffer(
            final ByteBuffer buffer,
            final Class<T> histogramClass,
            final long minBarForHighestTrackableValue)
            throws DataFormatException {
        int initialTargetPosition = buffer.position();
        final int cookie = buffer.getInt();
        final int headerSize;
        if ((getCookieBase(cookie) == compressedEncodingCookieBase) ||
                (getCookieBase(cookie) == V1CompressedEncodingCookieBase)) {
            headerSize = ENCODING_HEADER_SIZE;
        } else if (getCookieBase(cookie) == V0CompressedEncodingCookieBase) {
            headerSize = V0_ENCODING_HEADER_SIZE;
        } else {
            throw new IllegalArgumentException("The buffer does not contain a compressed Histogram");
        }

        final int lengthOfCompressedContents = buffer.getInt();
        final Inflater decompressor = new Inflater();

        if (buffer.hasArray()) {
            decompressor.setInput(buffer.array(), initialTargetPosition + 8, lengthOfCompressedContents);
        } else {
            byte[] compressedContents = new byte[lengthOfCompressedContents];
            buffer.get(compressedContents);
            decompressor.setInput(compressedContents);
        }

        final ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize).order(BIG_ENDIAN);
        decompressor.inflate(headerBuffer.array());
        T histogram = decodeFromByteBuffer(
                headerBuffer, histogramClass, minBarForHighestTrackableValue, decompressor);

        decompressor.end();

        return histogram;
    }

    static int numberOfSubbuckets(final int numberOfSignificantValueDigits) {
        final long largestValueWithSingleUnitResolution = 2 * (long) Math.pow(10, numberOfSignificantValueDigits);

        // We need to maintain power-of-two subBucketCount (for clean direct indexing) that is large enough to
        // provide unit resolution to at least largestValueWithSingleUnitResolution. So figure out
        // largestValueWithSingleUnitResolution's nearest power-of-two (rounded up), and use that:
        int subBucketCountMagnitude = (int) Math.ceil(Math.log(largestValueWithSingleUnitResolution) / Math.log(2));
        return (int) Math.pow(2, subBucketCountMagnitude);
    }

    abstract int _getEstimatedFootprintInBytes();

    abstract void addToCountAtIndex(int index, long value);

    abstract void addToTotalCount(long value);

    abstract void clearCounts();

    final int determineArrayLengthNeeded(long highestTrackableValue) {
        if (highestTrackableValue < 2L * lowestDiscernibleValue) {
            throw new IllegalArgumentException("highestTrackableValue (" + highestTrackableValue +
                    ") cannot be < (2 * lowestDiscernibleValue)");
        }
        //determine counts array length needed:
        return getLengthForNumberOfBuckets(getBucketsNeededToCoverValue(highestTrackableValue));
    }

    void establishInternalTackingValues() {
        establishInternalTackingValues(countsArrayLength);
    }

    void establishInternalTackingValues(final int lengthToCover) {
        resetMaxValue(0);
        resetMinNonZeroValue(Long.MAX_VALUE);
        int maxIndex = -1;
        int minNonZeroIndex = -1;
        long observedTotalCount = 0;
        for (int index = 0; index < lengthToCover; index++) {
            long countAtIndex;
            if ((countAtIndex = getCountAtIndex(index)) > 0) {
                observedTotalCount += countAtIndex;
                maxIndex = index;
                if ((minNonZeroIndex == -1) && (index != 0)) {
                    minNonZeroIndex = index;
                }
            }
        }
        if (maxIndex >= 0) {
            updateMaxValue(highestEquivalentValue(valueFromIndex(maxIndex)));
        }
        if (minNonZeroIndex >= 0) {
            updateMinNonZeroValue(valueFromIndex(minNonZeroIndex));
        }
        setTotalCount(observedTotalCount);
    }

    /**
     * The buckets (each of which has subBucketCount sub-buckets, here assumed to be 2048 as an example) overlap:
     *
     * <pre>
     * The 0'th bucket covers from 0...2047 in multiples of 1, using all 2048 sub-buckets
     * The 1'th bucket covers from 2048..4097 in multiples of 2, using only the top 1024 sub-buckets
     * The 2'th bucket covers from 4096..8191 in multiple of 4, using only the top 1024 sub-buckets
     * ...
     * </pre>
     * <p>
     * Bucket 0 is "special" here. It is the only one that has 2048 entries. All the rest have 1024 entries (because
     * their bottom half overlaps with and is already covered by the all of the previous buckets put together). In other
     * words, the k'th bucket could represent 0 * 2^k to 2048 * 2^k in 2048 buckets with 2^k precision, but the midpoint
     * of 1024 * 2^k = 2048 * 2^(k-1) = the k-1'th bucket's end, so we would use the previous bucket for those lower
     * values as it has better precision.
     */
    final void establishSize(long newHighestTrackableValue) {
        // establish counts array length:
        countsArrayLength = determineArrayLengthNeeded(newHighestTrackableValue);
        // establish exponent range needed to support the trackable value with no overflow:
        bucketCount = getBucketsNeededToCoverValue(newHighestTrackableValue);
        // establish the new highest trackable value:
        highestTrackableValue = newHighestTrackableValue;
    }

    synchronized void fillBufferFromCountsArray(ByteBuffer buffer) {
        final int countsLimit = countsArrayIndex(maxValue) + 1;
        int srcIndex = 0;

        while (srcIndex < countsLimit) {
            // V2 encoding format uses a ZigZag LEB128-64b9B encoded long. Positive values are counts,
            // while negative values indicate a repeat zero counts.
            long count = getCountAtIndex(srcIndex++);
            if (count < 0) {
                throw new RuntimeException("Cannot encode histogram containing negative counts (" +
                        count + ") at index " + srcIndex + ", corresponding the value range [" +
                        lowestEquivalentValue(valueFromIndex(srcIndex)) + "," +
                        nextNonEquivalentValue(valueFromIndex(srcIndex)) + ")");
            }
            // Count trailing 0s (which follow this count):
            long zerosCount = 0;
            if (count == 0) {
                zerosCount = 1;
                while ((srcIndex < countsLimit) && (getCountAtIndex(srcIndex) == 0)) {
                    zerosCount++;
                    srcIndex++;
                }
            }
            if (zerosCount > 1) {
                ZigZagEncoding.putLong(buffer, -zerosCount);
            } else {
                ZigZagEncoding.putLong(buffer, count);
            }
        }
    }

    int getBucketsNeededToCoverValue(final long value) {
        // Shift won't overflow because subBucketMagnitude + unitMagnitude <= 62.
        // the k'th bucket can express from 0 * 2^k to subBucketCount * 2^k in units of 2^k
        long smallestUntrackableValue = ((long) subBucketCount) << unitMagnitude;

        // always have at least 1 bucket
        int bucketsNeeded = 1;
        while (smallestUntrackableValue <= value) {
            if (smallestUntrackableValue > (Long.MAX_VALUE / 2)) {
                // next shift will overflow, meaning that bucket could represent values up to ones greater than
                // Long.MAX_VALUE, so it's the last bucket
                return bucketsNeeded + 1;
            }
            smallestUntrackableValue <<= 1;
            bucketsNeeded++;
        }
        return bucketsNeeded;
    }

    abstract long getCountAtNormalizedIndex(int index);

    /**
     * If we have N such that subBucketCount * 2^N > max value, we need storage for N+1 buckets, each with enough
     * slots to hold the top half of the subBucketCount (the lower half is covered by previous buckets), and the +1
     * being used for the lower half of the 0'th bucket. Or, equivalently, we need 1 more bucket to capture the max
     * value if we consider the sub-bucket length to be halved.
     */
    int getLengthForNumberOfBuckets(final int numberOfBuckets) {
        return (numberOfBuckets + 1) * (subBucketHalfCount);
    }

    int getNeededByteBufferCapacity(final int relevantLength) {
        return getNeededPayloadByteBufferCapacity(relevantLength) + ENCODING_HEADER_SIZE;
    }

    int getNeededPayloadByteBufferCapacity(final int relevantLength) {
        return (relevantLength * V2maxWordSizeInBytes);
    }

    int getNeededV0PayloadByteBufferCapacity(final int relevantLength) {
        return (relevantLength * wordSizeInBytes);
    }

    abstract int getNormalizingIndexOffset();

    abstract void incrementCountAtIndex(int index);

    abstract void incrementTotalCount();

    void nonConcurrentNormalizingIndexShift(int shiftAmount, boolean lowestHalfBucketPopulated) {

        // Save and clear the 0 value count:
        long zeroValueCount = getCountAtIndex(0);
        setCountAtIndex(0, 0);
        int preShiftZeroIndex = normalizeIndex(0, getNormalizingIndexOffset(), countsArrayLength);

        setNormalizingIndexOffset(getNormalizingIndexOffset() + shiftAmount);

        // Deal with lower half bucket if needed:
        if (lowestHalfBucketPopulated) {
            if (shiftAmount <= 0) {
                // Shifts with lowest half bucket populated can only be to the left.
                // Any right shift logic calling this should have already verified that
                // the lowest half bucket is not populated.
                throw CairoException.nonCritical().put(
                        "Attempt to right-shift with already-recorded value counts that would underflow and lose precision");
            }
            shiftLowestHalfBucketContentsLeft(shiftAmount, preShiftZeroIndex);
        }

        // Restore the 0 value count:
        setCountAtIndex(0, zeroValueCount);
    }

    /**
     * @return The value `index - normalizingIndexOffset` modulo arrayLength (always nonnegative)
     */
    int normalizeIndex(int index, int normalizingIndexOffset, int arrayLength) {
        if (normalizingIndexOffset == 0) {
            // Fastpath out of normalization. Keeps integer value histograms fast while allowing
            // others (like DoubleHistogram) to use normalization at a cost...
            return index;
        }
        if ((index > arrayLength) || (index < 0)) {
            throw CairoException.nonCritical().put("index out of covered value range");
        }
        int normalizedIndex = index - normalizingIndexOffset;
        // The following is the same as an unsigned remainder operation, as long as no double wrapping happens
        // (which shouldn't happen, as normalization is never supposed to wrap, since it would have overflowed
        // or underflowed before it did). This (the + and - tests) seems to be faster than a % op with a
        // correcting if < 0...:
        if (normalizedIndex < 0) {
            normalizedIndex += arrayLength;
        } else if (normalizedIndex >= arrayLength) {
            normalizedIndex -= arrayLength;
        }
        return normalizedIndex;
    }

    // Package-internal support for converting and recording double values into integer histograms:
    void recordConvertedDoubleValue(final double value) {
        long integerValue = (long) (value * doubleToIntegerValueConversionRatio);
        recordValue(integerValue);
    }

    abstract void resize(long newHighestTrackableValue);

    abstract void setCountAtIndex(int index, long value);

    //   #### ##    ## ######## ######## ########  ##    ##    ###    ##
    //    ##  ###   ##    ##    ##       ##     ## ###   ##   ## ##   ##
    //    ##  ####  ##    ##    ##       ##     ## ####  ##  ##   ##  ##
    //    ##  ## ## ##    ##    ######   ########  ## ## ## ##     ## ##
    //    ##  ##  ####    ##    ##       ##   ##   ##  #### ######### ##
    //    ##  ##   ###    ##    ##       ##    ##  ##   ### ##     ## ##
    //   #### ##    ##    ##    ######## ##     ## ##    ## ##     ## ########
    //
    //   ##     ## ######## ##       ########  ######## ########   ######
    //   ##     ## ##       ##       ##     ## ##       ##     ## ##    ##
    //   ##     ## ##       ##       ##     ## ##       ##     ## ##
    //   ######### ######   ##       ########  ######   ########   ######
    //   ##     ## ##       ##       ##        ##       ##   ##         ##
    //   ##     ## ##       ##       ##        ##       ##    ##  ##    ##
    //   ##     ## ######## ######## ##        ######## ##     ##  ######
    //
    // Internal helper methods:
    //

    abstract void setCountAtNormalizedIndex(int index, long value);

    abstract void setNormalizingIndexOffset(int normalizingIndexOffset);

    abstract void setTotalCount(long totalCount);

    abstract void shiftNormalizingIndexByOffset(int offsetToAdd, boolean lowestHalfBucketPopulated,
                                                double newIntegerToDoubleValueConversionRatio);

    void shiftValuesLeft(final int numberOfBinaryOrdersOfMagnitude, final double newIntegerToDoubleValueConversionRatio) {
        if (numberOfBinaryOrdersOfMagnitude < 0) {
            throw new IllegalArgumentException("Cannot shift by a negative number of magnitudes");
        }
        if (numberOfBinaryOrdersOfMagnitude == 0) {
            return;
        }
        if (getTotalCount() == getCountAtIndex(0)) {
            // (no need to shift any values if all recorded values are at the 0 value level:)
            return;
        }

        final int shiftAmount = numberOfBinaryOrdersOfMagnitude << subBucketHalfCountMagnitude;
        int maxValueIndex = countsArrayIndex(getMaxValue());
        // indicate overflow if maxValue is in the range being wrapped:
        if (maxValueIndex >= (countsArrayLength - shiftAmount)) {
            throw CairoException.nonCritical().put(
                    "Operation would overflow, would discard recorded value counts");
        }

        long maxValueBeforeShift = maxValue;
        maxValue = 0;
        long minNonZeroValueBeforeShift = minNonZeroValue;
        minNonZeroValue = Long.MAX_VALUE;

        boolean lowestHalfBucketPopulated = (minNonZeroValueBeforeShift < ((long) subBucketHalfCount << unitMagnitude));

        // Perform the shift:
        shiftNormalizingIndexByOffset(shiftAmount, lowestHalfBucketPopulated, newIntegerToDoubleValueConversionRatio);

        // adjust min, max:
        updateMinAndMax(maxValueBeforeShift << numberOfBinaryOrdersOfMagnitude);
        if (minNonZeroValueBeforeShift < Long.MAX_VALUE) {
            updateMinAndMax(minNonZeroValueBeforeShift << numberOfBinaryOrdersOfMagnitude);
        }
    }

    void shiftValuesRight(final int numberOfBinaryOrdersOfMagnitude, final double newIntegerToDoubleValueConversionRatio) {
        if (numberOfBinaryOrdersOfMagnitude < 0) {
            throw new IllegalArgumentException("Cannot shift by a negative number of magnitudes");
        }
        if (numberOfBinaryOrdersOfMagnitude == 0) {
            return;
        }
        if (getTotalCount() == getCountAtIndex(0)) {
            // (no need to shift any values if all recorded values are at the 0 value level:)
            return;
        }

        final int shiftAmount = subBucketHalfCount * numberOfBinaryOrdersOfMagnitude;

        // indicate underflow if minValue is in the range being shifted from:
        int minNonZeroValueIndex = countsArrayIndex(getMinNonZeroValue());

        // Any shifting into the bottom-most half bucket would represents a loss of accuracy,
        // and a non-reversible operation. Therefore any non-0 value that falls in an
        // index below (shiftAmount + subBucketHalfCount) would represent an underflow:
        // <DetailedExplanation:>
        //     The fact that the top and bottom halves of the first bucket use the same scale
        //     means any shift into the bottom half is invalid. The shift requires that each
        //     successive subBucketCount be encoded with a scale 2x the previous one, as that
        //     is how the powers of 2 are applied.
        //     In particular, if the shift amount is such that it would shift something from
        //     the top half of the first bucket to the bottom half, that's all stored with the
        //     same unit, so half of a larger odd value couldn't be restored to its proper
        //     value by a subsequent left shift because we would need the bottom half to be
        //     encoded in half-units.
        //     Furthermore, shifts from anywhere (in the top half of the first bucket or
        //     beyond) will be incorrectly encoded if they end up in the bottom half. If
        //     subBucketHalfCount is, say, 1024, and the shift is by 1, the value for 1600
        //     would become 576, which is certainly not 1600/2. With a shift of 2 and a
        //     value of 2112 (index 2048 + 32), the resulting value is 32, not 525. For
        //     comparison, with shift 2 and value 4096 (index 2048 + 1024 = 3072), 3072 - 2048 = 1024.
        //     That's the first entry in the top half of bucket 0, which encodes simply
        //     1024 = 4096 / 4. Thus, any non-0 value that falls in an index below
        //     (shiftAmount + subBucketHalfCount) would represent an underflow.
        // </DetailedExplanation:>

        if (minNonZeroValueIndex < shiftAmount + subBucketHalfCount) {
            throw CairoException.nonCritical().put(
                    "Operation would underflow and lose precision of already recorded value counts");
        }

        // perform shift:

        long maxValueBeforeShift = maxValue;
        maxValue = 0;
        long minNonZeroValueBeforeShift = minNonZeroValue;
        minNonZeroValue = Long.MAX_VALUE;

        // move normalizingIndexOffset
        shiftNormalizingIndexByOffset(-shiftAmount, false, newIntegerToDoubleValueConversionRatio);

        // adjust min, max:
        updateMinAndMax(maxValueBeforeShift >> numberOfBinaryOrdersOfMagnitude);
        if (minNonZeroValueBeforeShift < Long.MAX_VALUE) {
            updateMinAndMax(minNonZeroValueBeforeShift >> numberOfBinaryOrdersOfMagnitude);
        }
    }

    /**
     * Set internally tracked maxValue to new value if new value is greater than current one.
     * May be overridden by subclasses for synchronization or atomicity purposes.
     *
     * @param value new maxValue to set
     */
    protected void updateMaxValue(final long value) {
        maxValue = Math.max(maxValue, value | unitMagnitudeMask); // Max unit-equivalent value
    }

    void updateMinAndMax(final long value) {
        if (value > maxValue) {
            maxValue = value | unitMagnitudeMask; // Max unit-equivalent value
        }

        if ((value < minNonZeroValue) && (value != 0)) {
            if (value <= unitMagnitudeMask) {
                return; // Unit-equivalent to 0.
            }
            minNonZeroValue = value & ~unitMagnitudeMask; // Min unit-equivalent value
        }
    }

    /**
     * Set internally tracked minNonZeroValue to new value if new value is smaller than current one.
     * May be overridden by subclasses for synchronization or atomicity purposes.
     *
     * @param value new minNonZeroValue to set
     */
    protected void updateMinNonZeroValue(final long value) {
        if (value <= unitMagnitudeMask) {
            return; // Unit-equivalent to 0.
        }
        minNonZeroValue = Math.min(minNonZeroValue, value & ~unitMagnitudeMask); // Min unit-equivalent value
    }

    /**
     * An {@link java.lang.Iterable}{@literal <}{@link HistogramIterationValue}{@literal >} through
     * the histogram using a {@link AllValuesIterator}
     */
    public static class AllValues implements Iterable<HistogramIterationValue> {
        final AbstractHistogram histogram;

        private AllValues(final AbstractHistogram histogram) {
            this.histogram = histogram;
        }

        /**
         * @return A {@link AllValuesIterator}{@literal <}{@link HistogramIterationValue}{@literal >}
         */
        public @NotNull Iterator<HistogramIterationValue> iterator() {
            return new AllValuesIterator(histogram);
        }
    }

    /**
     * An {@link java.lang.Iterable}{@literal <}{@link HistogramIterationValue}{@literal >} through
     * the histogram using a {@link LinearIterator}
     */
    public static class LinearBucketValues implements Iterable<HistogramIterationValue> {
        final AbstractHistogram histogram;
        final long valueUnitsPerBucket;

        private LinearBucketValues(final AbstractHistogram histogram, final long valueUnitsPerBucket) {
            this.histogram = histogram;
            this.valueUnitsPerBucket = valueUnitsPerBucket;
        }

        /**
         * @return A {@link LinearIterator}{@literal <}{@link HistogramIterationValue}{@literal >}
         */
        public @NotNull Iterator<HistogramIterationValue> iterator() {
            return new LinearIterator(histogram, valueUnitsPerBucket);
        }
    }

    /**
     * An {@link java.lang.Iterable}{@literal <}{@link HistogramIterationValue}{@literal >} through
     * the histogram using a {@link LogarithmicIterator}
     */
    public static class LogarithmicBucketValues implements Iterable<HistogramIterationValue> {
        final AbstractHistogram histogram;
        final double logBase;
        final long valueUnitsInFirstBucket;

        private LogarithmicBucketValues(final AbstractHistogram histogram,
                                        final long valueUnitsInFirstBucket, final double logBase) {
            this.histogram = histogram;
            this.valueUnitsInFirstBucket = valueUnitsInFirstBucket;
            this.logBase = logBase;
        }

        /**
         * @return A {@link LogarithmicIterator}{@literal <}{@link HistogramIterationValue}{@literal >}
         */
        public @NotNull Iterator<HistogramIterationValue> iterator() {
            return new LogarithmicIterator(histogram, valueUnitsInFirstBucket, logBase);
        }
    }

    /**
     * An {@link java.lang.Iterable}{@literal <}{@link HistogramIterationValue}{@literal >} through
     * the histogram using a {@link PercentileIterator}
     */
    public static class Percentiles implements Iterable<HistogramIterationValue> {
        final AbstractHistogram histogram;
        final int percentileTicksPerHalfDistance;

        private Percentiles(final AbstractHistogram histogram, final int percentileTicksPerHalfDistance) {
            this.histogram = histogram;
            this.percentileTicksPerHalfDistance = percentileTicksPerHalfDistance;
        }

        /**
         * @return A {@link PercentileIterator}{@literal <}{@link HistogramIterationValue}{@literal >}
         */
        @Override
        public @NotNull Iterator<HistogramIterationValue> iterator() {
            return new PercentileIterator(histogram, percentileTicksPerHalfDistance);
        }
    }

    /**
     * An {@link java.lang.Iterable}{@literal <}{@link HistogramIterationValue}{@literal >} through
     * the histogram using a {@link RecordedValuesIterator}
     */
    public static class RecordedValues implements Iterable<HistogramIterationValue> {
        final AbstractHistogram histogram;

        private RecordedValues(final AbstractHistogram histogram) {
            this.histogram = histogram;
        }

        /**
         * @return A {@link RecordedValuesIterator}{@literal <}{@link HistogramIterationValue}{@literal >}
         */
        public @NotNull Iterator<HistogramIterationValue> iterator() {
            return new RecordedValuesIterator(histogram);
        }
    }
}

/**
 * This non-public AbstractHistogramBase super-class separation is meant to bunch "cold" fields
 * separately from "hot" fields, in an attempt to force the JVM to place the (hot) fields
 * commonly used in the value recording code paths close together.
 * Subclass boundaries tend to be strongly control memory layout decisions in most practical
 * JVM implementations, making this an effective method for control filed grouping layout.
 */

abstract class AbstractHistogramBase extends EncodableHistogram {
    static final AtomicLong constructionIdentityCount = new AtomicLong(0);
    volatile boolean autoResize = false;
    int bucketCount;
    int countsArrayLength;
    double doubleToIntegerValueConversionRatio = 1.0;
    long endTimeStampMsec = 0;
    long highestTrackableValue;
    // "Cold" accessed fields. Not used in the recording code path:
    long identity;
    double integerToDoubleValueConversionRatio = 1.0;
    byte[] intermediateUncompressedByteArray = null;
    ByteBuffer intermediateUncompressedByteBuffer = null;
    long lowestDiscernibleValue;
    int numberOfSignificantValueDigits;
    PercentileIterator percentileIterator;
    RecordedValuesIterator recordedValuesIterator;
    long startTimeStampMsec = Long.MAX_VALUE;
    /**
     * Power-of-two length of linearly scaled array slots in the counts array. Long enough to hold the first sequence of
     * entries that must be distinguished by a single unit (determined by configured precision).
     */
    int subBucketCount;
    String tag = null;
    int wordSizeInBytes;

    public int bucketCount() {
        return bucketCount;
    }

    public int countsArrayLength() {
        return countsArrayLength;
    }

    public int subBucketCount() {
        return subBucketCount;
    }

    double getDoubleToIntegerValueConversionRatio() {
        return doubleToIntegerValueConversionRatio;
    }

    double getIntegerToDoubleValueConversionRatio() {
        return integerToDoubleValueConversionRatio;
    }

    void nonConcurrentSetIntegerToDoubleValueConversionRatio(double integerToDoubleValueConversionRatio) {
        this.integerToDoubleValueConversionRatio = integerToDoubleValueConversionRatio;
        this.doubleToIntegerValueConversionRatio = 1.0 / integerToDoubleValueConversionRatio;
    }

    abstract void setIntegerToDoubleValueConversionRatio(double integerToDoubleValueConversionRatio);
}
