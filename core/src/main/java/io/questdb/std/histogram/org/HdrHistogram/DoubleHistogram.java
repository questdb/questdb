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
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;

/**
 * <h2>A floating point values High Dynamic Range (HDR) Histogram</h2>
 * <p>
 * It is important to note that {@link DoubleHistogram} is not thread-safe, and does not support safe concurrent
 * recording by multiple threads.
 * <p>
 * {@link DoubleHistogram} supports the recording and analyzing sampled data value counts across a
 * configurable dynamic range of floating point (double) values, with configurable value precision within the range.
 * Dynamic range is expressed as a ratio between the highest and lowest non-zero values trackable within the histogram
 * at any given time. Value precision is expressed as the number of significant [decimal] digits in the value recording,
 * and provides control over value quantization behavior across the value range and the subsequent value resolution at
 * any given level.
 * <p>
 * Auto-ranging: Unlike integer value based histograms, the specific value range tracked by a {@link
 * DoubleHistogram} is not specified upfront. Only the dynamic range of values that the histogram can cover is
 * (optionally) specified. E.g. When a {@link DoubleHistogram} is created to track a dynamic range of
 * 3600000000000 (enough to track values from a nanosecond to an hour), values could be recorded into into it in any
 * consistent unit of time as long as the ratio between the highest and lowest non-zero values stays within the
 * specified dynamic range, so recording in units of nanoseconds (1.0 thru 3600000000000.0), milliseconds (0.000001
 * thru 3600000.0) seconds (0.000000001 thru 3600.0), hours (1/3.6E12 thru 1.0) will all work just as well.
 * <p>
 * Auto-resizing: When constructed with no specified dynamic range (or when auto-resize is turned on with {@link
 * DoubleHistogram#setAutoResize}) a {@link DoubleHistogram} will auto-resize its dynamic range to
 * include recorded values as they are encountered. Note that recording calls that cause auto-resizing may take
 * longer to execute, as resizing incurs allocation and copying of internal data structures.
 * <p>
 * Attempts to record non-zero values that range outside of the specified dynamic range (or exceed the limits of
 * of dynamic range when auto-resizing) may results in {@link CairoException} exceptions, either
 * due to overflow or underflow conditions. These exceptions will only be thrown if recording the value would have
 * resulted in discarding or losing the required value precision of values already recorded in the histogram.
 * <p>
 * See package description for {@link io.questdb.std.histogram.org.HdrHistogram} for details.
 */
public class DoubleHistogram extends EncodableHistogram implements DoubleValueRecorder, Serializable {
    private static final int DHIST_compressedEncodingCookie = 0x0c72124f;
    private static final int DHIST_encodingCookie = 0x0c72124e;
    private static final Class<?>[] constructorArgTypes = {long.class, int.class, Class.class, AbstractHistogram.class};
    private static final double highestAllowedValueEver; // A value that will keep us from multiplying into infinity.
    private static final long serialVersionUID = 42L;

    AbstractHistogram integerValuesHistogram;
    private boolean autoResize = false;
    private long configuredHighestToLowestValueRatio;
    private volatile double currentHighestValueLimitInAutoRange;
    private volatile double currentLowestValueInAutoRange;

    /**
     * Construct a new auto-resizing DoubleHistogram using a precision stated as a number
     * of significant decimal digits.
     *
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     */
    public DoubleHistogram(final int numberOfSignificantValueDigits) {
        this(2, numberOfSignificantValueDigits, Histogram.class, null);
        setAutoResize(true);
    }

    /**
     * Construct a new auto-resizing DoubleHistogram using a precision stated as a number
     * of significant decimal digits.
     * <p>
     * The {@link io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram} will use the specified AbstractHistogram subclass
     * for tracking internal counts (e.g. {@link io.questdb.std.histogram.org.HdrHistogram.Histogram},
     * {@link io.questdb.std.histogram.org.HdrHistogram.IntCountsHistogram}, {@link io.questdb.std.histogram.org.HdrHistogram.ShortCountsHistogram}).
     *
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     * @param internalCountsHistogramClass   The class to use for internal counts tracking
     */
    public DoubleHistogram(final int numberOfSignificantValueDigits,
                           final Class<? extends AbstractHistogram> internalCountsHistogramClass) {
        this(2, numberOfSignificantValueDigits, internalCountsHistogramClass, null);
        setAutoResize(true);
    }

    /**
     * Construct a new DoubleHistogram with the specified dynamic range (provided in
     * {@code highestToLowestValueRatio}) and using a precision stated as a number of significant
     * decimal digits.
     *
     * @param highestToLowestValueRatio      specifies the dynamic range to use
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     */
    public DoubleHistogram(final long highestToLowestValueRatio, final int numberOfSignificantValueDigits) {
        this(highestToLowestValueRatio, numberOfSignificantValueDigits, Histogram.class);
    }

    /**
     * Construct a new DoubleHistogram with the specified dynamic range (provided in
     * {@code highestToLowestValueRatio}) and using a precision stated as a number of significant
     * decimal digits.
     * <p>
     * The {@link io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram} will use the specified AbstractHistogram subclass
     * for tracking internal counts (e.g. {@link io.questdb.std.histogram.org.HdrHistogram.Histogram},
     * {@link io.questdb.std.histogram.org.HdrHistogram.IntCountsHistogram}, {@link io.questdb.std.histogram.org.HdrHistogram.ShortCountsHistogram}).
     *
     * @param highestToLowestValueRatio      specifies the dynamic range to use.
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     * @param internalCountsHistogramClass   The class to use for internal counts tracking
     */
    public DoubleHistogram(final long highestToLowestValueRatio,
                           final int numberOfSignificantValueDigits,
                           final Class<? extends AbstractHistogram> internalCountsHistogramClass) {
        this(highestToLowestValueRatio, numberOfSignificantValueDigits, internalCountsHistogramClass, null);
    }

    DoubleHistogram(final long highestToLowestValueRatio,
                    final int numberOfSignificantValueDigits,
                    final Class<? extends AbstractHistogram> internalCountsHistogramClass,
                    AbstractHistogram internalCountsHistogram) {
        this(
                highestToLowestValueRatio,
                numberOfSignificantValueDigits,
                internalCountsHistogramClass,
                internalCountsHistogram,
                false
        );
    }

    private DoubleHistogram(final long highestToLowestValueRatio,
                            final int numberOfSignificantValueDigits,
                            final Class<? extends AbstractHistogram> internalCountsHistogramClass,
                            AbstractHistogram internalCountsHistogram,
                            boolean mimicInternalModel) {
        try {
            if (highestToLowestValueRatio < 2) {
                throw new IllegalArgumentException("highestToLowestValueRatio must be >= 2");
            }

            if ((highestToLowestValueRatio * Math.pow(10.0, numberOfSignificantValueDigits)) >= (1L << 61)) {
                throw new IllegalArgumentException(
                        "highestToLowestValueRatio * (10^numberOfSignificantValueDigits) must be < (1L << 61)");
            }

            long integerValueRange = deriveIntegerValueRange(highestToLowestValueRatio, numberOfSignificantValueDigits);

            final AbstractHistogram valuesHistogram;
            double initialLowestValueInAutoRange;

            if (internalCountsHistogram == null) {
                // Create the internal counts histogram:
                Constructor<? extends AbstractHistogram> histogramConstructor =
                        internalCountsHistogramClass.getConstructor(long.class, long.class, int.class);

                valuesHistogram =
                        histogramConstructor.newInstance(
                                1L,
                                (integerValueRange - 1),
                                numberOfSignificantValueDigits
                        );

                // We want the auto-ranging to tend towards using a value range that will result in using the
                // lower tracked value ranges and leave the higher end empty unless the range is actually used.
                // This is most easily done by making early recordings force-shift the lower value limit to
                // accommodate them (forcing a force-shift for the higher values would achieve the opposite).
                // We will therefore start with a very high value range, and let the recordings autoAdjust
                // downwards from there:
                initialLowestValueInAutoRange = Math.pow(2.0, 800);
            } else if (mimicInternalModel) {
                Constructor<? extends AbstractHistogram> histogramConstructor =
                        internalCountsHistogramClass.getConstructor(AbstractHistogram.class);

                valuesHistogram = histogramConstructor.newInstance(internalCountsHistogram);

                initialLowestValueInAutoRange = Math.pow(2.0, 800);
            } else {
                // Verify that the histogram we got matches:
                if ((internalCountsHistogram.getLowestDiscernibleValue() != 1) ||
                        (internalCountsHistogram.getHighestTrackableValue() != integerValueRange - 1) ||
                        internalCountsHistogram.getNumberOfSignificantValueDigits() != numberOfSignificantValueDigits) {
                    throw new IllegalStateException("integer values histogram does not match stated parameters.");
                }
                valuesHistogram = internalCountsHistogram;
                // Derive initialLowestValueInAutoRange from valuesHistogram's integerToDoubleValueConversionRatio:
                initialLowestValueInAutoRange =
                        internalCountsHistogram.getIntegerToDoubleValueConversionRatio() *
                                internalCountsHistogram.subBucketHalfCount;
            }

            // Set our double tracking range and internal histogram:
            init(highestToLowestValueRatio, initialLowestValueInAutoRange, valuesHistogram);

        } catch (NoSuchMethodException | IllegalAccessException |
                 InstantiationException | InvocationTargetException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    /**
     * Construct a {@link io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram} with the same range settings as a given source,
     * duplicating the source's start/end timestamps (but NOT it's contents)
     *
     * @param source The source histogram to duplicate
     */
    public DoubleHistogram(final DoubleHistogram source) {
        this(source.configuredHighestToLowestValueRatio,
                source.getNumberOfSignificantValueDigits(),
                source.integerValuesHistogram.getClass(),
                source.integerValuesHistogram,
                true);
        this.autoResize = source.autoResize;
        setTrackableValueRange(source.currentLowestValueInAutoRange, source.currentHighestValueLimitInAutoRange);
    }

    //
    //
    // Auto-resizing control:
    //
    //

    /**
     * Construct a new DoubleHistogram by decoding it from a ByteBuffer.
     *
     * @param buffer                             The buffer to decode from
     * @param minBarForHighestToLowestValueRatio Force highestTrackableValue to be set at least this high
     * @return The newly constructed DoubleHistogram
     */
    public static DoubleHistogram decodeFromByteBuffer(
            final ByteBuffer buffer,
            final long minBarForHighestToLowestValueRatio) {
        return decodeFromByteBuffer(buffer, Histogram.class, minBarForHighestToLowestValueRatio);
    }

    /**
     * Construct a new DoubleHistogram by decoding it from a ByteBuffer, using a
     * specified AbstractHistogram subclass for tracking internal counts (e.g. {@link io.questdb.std.histogram.org.HdrHistogram.Histogram},
     * {@link io.questdb.std.histogram.org.HdrHistogram.IntCountsHistogram}, {@link io.questdb.std.histogram.org.HdrHistogram.ShortCountsHistogram}).
     *
     * @param buffer                             The buffer to decode from
     * @param internalCountsHistogramClass       The class to use for internal counts tracking
     * @param minBarForHighestToLowestValueRatio Force highestTrackableValue to be set at least this high
     * @return The newly constructed DoubleHistogram
     */
    public static DoubleHistogram decodeFromByteBuffer(
            final ByteBuffer buffer,
            final Class<? extends AbstractHistogram> internalCountsHistogramClass,
            long minBarForHighestToLowestValueRatio) {
        try {
            int cookie = buffer.getInt();
            if (!isNonCompressedDoubleHistogramCookie(cookie)) {
                throw new IllegalArgumentException("The buffer does not contain a DoubleHistogram");
            }
            return constructHistogramFromBuffer(cookie, buffer,
                    DoubleHistogram.class, internalCountsHistogramClass,
                    minBarForHighestToLowestValueRatio);
        } catch (DataFormatException ex) {
            throw new RuntimeException(ex);
        }
    }

    //
    //
    //
    // Value recording support:
    //
    //
    //

    /**
     * Construct a new DoubleHistogram by decoding it from a compressed form in a ByteBuffer.
     *
     * @param buffer                             The buffer to decode from
     * @param minBarForHighestToLowestValueRatio Force highestTrackableValue to be set at least this high
     * @return The newly constructed DoubleHistogram
     * @throws DataFormatException on error parsing/decompressing the buffer
     */
    public static DoubleHistogram decodeFromCompressedByteBuffer(
            final ByteBuffer buffer,
            final long minBarForHighestToLowestValueRatio) throws DataFormatException {
        return decodeFromCompressedByteBuffer(buffer, Histogram.class, minBarForHighestToLowestValueRatio);
    }

    /**
     * Construct a new DoubleHistogram by decoding it from a compressed form in a ByteBuffer, using a
     * specified AbstractHistogram subclass for tracking internal counts (e.g. {@link io.questdb.std.histogram.org.HdrHistogram.Histogram},
     * {@link io.questdb.std.histogram.org.HdrHistogram.IntCountsHistogram}, {@link io.questdb.std.histogram.org.HdrHistogram.ShortCountsHistogram}).
     *
     * @param buffer                             The buffer to decode from
     * @param internalCountsHistogramClass       The class to use for internal counts tracking
     * @param minBarForHighestToLowestValueRatio Force highestTrackableValue to be set at least this high
     * @return The newly constructed DoubleHistogram
     * @throws DataFormatException on error parsing/decompressing the buffer
     */
    public static DoubleHistogram decodeFromCompressedByteBuffer(
            final ByteBuffer buffer,
            Class<? extends AbstractHistogram> internalCountsHistogramClass,
            long minBarForHighestToLowestValueRatio) throws DataFormatException {
        int cookie = buffer.getInt();
        if (!isCompressedDoubleHistogramCookie(cookie)) {
            throw new IllegalArgumentException("The buffer does not contain a compressed DoubleHistogram");
        }
        return constructHistogramFromBuffer(cookie, buffer,
                DoubleHistogram.class, internalCountsHistogramClass,
                minBarForHighestToLowestValueRatio);
    }

    /**
     * Construct a new DoubleHistogram by decoding it from a String containing a base64 encoded
     * compressed histogram representation.
     *
     * @param base64CompressedHistogramString A string containing a base64 encoding of a compressed histogram
     * @return A DoubleHistogram decoded from the string
     * @throws DataFormatException on error parsing/decompressing the input
     */
    public static DoubleHistogram fromString(final String base64CompressedHistogramString)
            throws DataFormatException {
        return decodeFromCompressedByteBuffer(
                ByteBuffer.wrap(Base64Helper.parseBase64Binary(base64CompressedHistogramString)),
                0);
    }

    /**
     * Add the contents of another histogram to this one.
     *
     * @param fromHistogram The other histogram.
     * @throws CairoException (may throw) if values in fromHistogram's cannot be
     *                        covered by this histogram's range
     */
    public void add(final DoubleHistogram fromHistogram) throws CairoException {
        int arrayLength = fromHistogram.integerValuesHistogram.countsArrayLength;
        AbstractHistogram fromIntegerHistogram = fromHistogram.integerValuesHistogram;
        for (int i = 0; i < arrayLength; i++) {
            long count = fromIntegerHistogram.getCountAtIndex(i);
            if (count > 0) {
                recordValueWithCount(
                        fromIntegerHistogram.valueFromIndex(i) *
                                fromHistogram.getIntegerToDoubleValueConversionRatio(),
                        count);
            }
        }
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
     * by {@link #recordValueWithExpectedInterval(double, double) recordValueWithExpectedInterval}. The two
     * methods are mutually exclusive, and only one of the two should be be used on a given data set to correct
     * for the same coordinated omission issue.
     * by
     * <p>
     * See notes in the description of the Histogram calls for an illustration of why this corrective behavior is
     * important.
     *
     * @param fromHistogram                       Other histogram. highestToLowestValueRatio and numberOfSignificantValueDigits must match.
     * @param expectedIntervalBetweenValueSamples If expectedIntervalBetweenValueSamples is larger than 0, add
     *                                            auto-generated value records as appropriate if value is larger
     *                                            than expectedIntervalBetweenValueSamples
     * @throws CairoException (may throw) if values exceed highestTrackableValue
     */
    public void addWhileCorrectingForCoordinatedOmission(final DoubleHistogram fromHistogram,
                                                         final double expectedIntervalBetweenValueSamples) {
        final DoubleHistogram toHistogram = this;

        for (HistogramIterationValue v : fromHistogram.integerValuesHistogram.recordedValues()) {
            toHistogram.recordValueWithCountAndExpectedInterval(
                    v.getValueIteratedTo() * getIntegerToDoubleValueConversionRatio(),
                    v.getCountAtValueIteratedTo(), expectedIntervalBetweenValueSamples);
        }
    }

    /**
     * Provide a means of iterating through all histogram values using the finest granularity steps supported by
     * the underlying representation. The iteration steps through all possible unit value levels, regardless of
     * whether or not there were recorded values for that value level, and terminates when all recorded histogram
     * values are exhausted.
     *
     * @return An {@link java.lang.Iterable}{@literal <}{@link DoubleHistogramIterationValue}{@literal >}
     * through the histogram using a {@link DoubleAllValuesIterator}
     */
    public AllValues allValues() {
        return new AllValues(this);
    }

    //
    //
    //
    // Shift and auto-ranging support:
    //
    //
    //

    /**
     * Create a copy of this histogram, complete with data and everything.
     *
     * @return A distinct copy of this histogram.
     */
    public DoubleHistogram copy() {
        final DoubleHistogram targetHistogram =
                new DoubleHistogram(configuredHighestToLowestValueRatio, getNumberOfSignificantValueDigits());
        targetHistogram.setTrackableValueRange(currentLowestValueInAutoRange, currentHighestValueLimitInAutoRange);
        integerValuesHistogram.copyInto(targetHistogram.integerValuesHistogram);
        return targetHistogram;
    }

    /**
     * Get a copy of this histogram, corrected for coordinated omission.
     * <p>
     * To compensate for the loss of sampled values when a recorded value is larger than the expected
     * interval between value samples, the new histogram will include an auto-generated additional series of
     * decreasingly-smaller (down to the expectedIntervalBetweenValueSamples) value records for each count found
     * in the current histogram that is larger than the expectedIntervalBetweenValueSamples.
     * <p>
     * Note: This is a post-correction method, as opposed to the at-recording correction method provided
     * by {@link #recordValueWithExpectedInterval(double, double) recordValueWithExpectedInterval}. The two
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
    public DoubleHistogram copyCorrectedForCoordinatedOmission(final double expectedIntervalBetweenValueSamples) {
        final DoubleHistogram targetHistogram =
                new DoubleHistogram(configuredHighestToLowestValueRatio, getNumberOfSignificantValueDigits());
        targetHistogram.setTrackableValueRange(currentLowestValueInAutoRange, currentHighestValueLimitInAutoRange);
        targetHistogram.addWhileCorrectingForCoordinatedOmission(this, expectedIntervalBetweenValueSamples);
        return targetHistogram;
    }

    /**
     * Copy this histogram into the target histogram, overwriting it's contents.
     *
     * @param targetHistogram the histogram to copy into
     */
    public void copyInto(final DoubleHistogram targetHistogram) {
        targetHistogram.reset();
        targetHistogram.add(this);
        targetHistogram.setStartTimeStamp(integerValuesHistogram.startTimeStampMsec);
        targetHistogram.setEndTimeStamp(integerValuesHistogram.endTimeStampMsec);
    }

    /**
     * Copy this histogram, corrected for coordinated omission, into the target histogram, overwriting it's contents.
     * (see {@link #copyCorrectedForCoordinatedOmission} for more detailed explanation about how correction is applied)
     *
     * @param targetHistogram                     the histogram to copy into
     * @param expectedIntervalBetweenValueSamples If expectedIntervalBetweenValueSamples is larger than 0, add
     *                                            auto-generated value records as appropriate if value is larger
     *                                            than expectedIntervalBetweenValueSamples
     */
    public void copyIntoCorrectedForCoordinatedOmission(final DoubleHistogram targetHistogram,
                                                        final double expectedIntervalBetweenValueSamples) {
        targetHistogram.reset();
        targetHistogram.addWhileCorrectingForCoordinatedOmission(this, expectedIntervalBetweenValueSamples);
        targetHistogram.setStartTimeStamp(integerValuesHistogram.startTimeStampMsec);
        targetHistogram.setEndTimeStamp(integerValuesHistogram.endTimeStampMsec);
    }

    /**
     * Encode this histogram into a ByteBuffer
     *
     * @param buffer The buffer to encode into
     * @return The number of bytes written to the buffer
     */
    synchronized public int encodeIntoByteBuffer(final ByteBuffer buffer) {
        long maxValue = integerValuesHistogram.getMaxValue();
        int relevantLength = integerValuesHistogram.getLengthForNumberOfBuckets(
                integerValuesHistogram.getBucketsNeededToCoverValue(maxValue));
        if (buffer.capacity() < getNeededByteBufferCapacity(relevantLength)) {
            throw CairoException.nonCritical().put("buffer does not have capacity for ")
                    .put(getNeededByteBufferCapacity(relevantLength)).put(" bytes");
        }
        buffer.putInt(DHIST_encodingCookie);
        buffer.putInt(getNumberOfSignificantValueDigits());
        buffer.putLong(configuredHighestToLowestValueRatio);
        return integerValuesHistogram.encodeIntoByteBuffer(buffer) + 16;
    }

    //
    //
    //
    // Clearing support:
    //
    //
    //

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
        targetBuffer.putInt(DHIST_compressedEncodingCookie);
        targetBuffer.putInt(getNumberOfSignificantValueDigits());
        targetBuffer.putLong(configuredHighestToLowestValueRatio);
        return integerValuesHistogram.encodeIntoCompressedByteBuffer(targetBuffer, compressionLevel) + 16;
    }

    //
    //
    //
    // Copy support:
    //
    //
    //

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
        if (!(other instanceof DoubleHistogram)) {
            return false;
        }
        DoubleHistogram that = (DoubleHistogram) other;
        return integerValuesHistogram.equals(that.integerValuesHistogram);
    }

    /**
     * Get the count of recorded values at a specific value (to within the histogram resolution at the value level).
     *
     * @param value The value for which to provide the recorded count
     * @return The total count of values recorded in the histogram within the value range that is
     * {@literal >=} lowestEquivalentValue(<i>value</i>) and {@literal <=} highestEquivalentValue(<i>value</i>)
     */
    public long getCountAtValue(final double value) throws CairoException {
        return integerValuesHistogram.getCountAtValue((long) (value * getDoubleToIntegerValueConversionRatio()));
    }

    /**
     * Get the count of recorded values within a range of value levels (inclusive to within the histogram's resolution).
     *
     * @param lowValue  The lower value bound on the range for which
     *                  to provide the recorded count. Will be rounded down with
     *                  {@link DoubleHistogram#lowestEquivalentValue lowestEquivalentValue}.
     * @param highValue The higher value bound on the range for which to provide the recorded count.
     *                  Will be rounded up with {@link DoubleHistogram#highestEquivalentValue highestEquivalentValue}.
     * @return the total count of values recorded in the histogram within the value range that is
     * {@literal >=} lowestEquivalentValue(<i>lowValue</i>) and {@literal <=} highestEquivalentValue(<i>highValue</i>)
     */
    public double getCountBetweenValues(final double lowValue, final double highValue) throws CairoException {
        return integerValuesHistogram.getCountBetweenValues(
                (long) (lowValue * getDoubleToIntegerValueConversionRatio()),
                (long) (highValue * getDoubleToIntegerValueConversionRatio())
        );
    }

    //
    //
    //
    // Add support:
    //
    //
    //

    /**
     * get the current highest trackable value in the automatically determined range
     * (keep in mind that this can change because it is auto ranging)
     *
     * @return current highest trackable value in the automatically determined range
     */
    public double getCurrentHighestTrackableValue() {
        return currentHighestValueLimitInAutoRange;
    }

    /**
     * get the current lowest (non zero) trackable value the automatically determined range
     * (keep in mind that this can change because it is auto ranging)
     *
     * @return current lowest trackable value the automatically determined range
     */
    public double getCurrentLowestTrackableNonZeroValue() {
        return currentLowestValueInAutoRange;
    }

    public double getDoubleToIntegerValueConversionRatio() {
        return integerValuesHistogram.getDoubleToIntegerValueConversionRatio();
    }

    //
    //
    //
    // Comparison support:
    //
    //
    //

    /**
     * get the end time stamp [optionally] stored with this histogram
     *
     * @return the end time stamp [optionally] stored with this histogram
     */
    public long getEndTimeStamp() {
        return integerValuesHistogram.getEndTimeStamp();
    }

    /**
     * Provide a (conservatively high) estimate of the Histogram's total footprint in bytes
     *
     * @return a (conservatively high) estimate of the Histogram's total footprint in bytes
     */
    public int getEstimatedFootprintInBytes() {
        return integerValuesHistogram._getEstimatedFootprintInBytes();
    }

    //
    //
    //
    // Histogram structure querying support:
    //
    //
    //

    /**
     * get the Dynamic range of the histogram: the configured ratio between the highest trackable value and the
     * lowest trackable non zero value at any given time.
     *
     * @return the dynamic range of the histogram, expressed as the ratio between the highest trackable value
     * and the lowest trackable non zero value at any given time.
     */
    public long getHighestToLowestValueRatio() {
        return configuredHighestToLowestValueRatio;
    }

    /**
     * Get the current conversion ratio from interval integer value representation to double units.
     * (keep in mind that this can change because it is auto ranging). This ratio can be useful
     * for converting integer values found in iteration, although the preferred form for accessing
     * iteration values would be to use the
     * {@link io.questdb.std.histogram.org.HdrHistogram.HistogramIterationValue#getDoubleValueIteratedTo() getDoubleValueIteratedTo()}
     * and
     * {@link io.questdb.std.histogram.org.HdrHistogram.HistogramIterationValue#getDoubleValueIteratedFrom() getDoubleValueIteratedFrom()}
     * accessors to {@link io.questdb.std.histogram.org.HdrHistogram.HistogramIterationValue} iterated values.
     *
     * @return the current conversion ratio from interval integer value representation to double units.
     */
    public double getIntegerToDoubleValueConversionRatio() {
        return integerValuesHistogram.integerToDoubleValueConversionRatio;
    }

    /**
     * Get the highest recorded value level in the histogram
     *
     * @return the Max value recorded in the histogram
     */
    public double getMaxValue() {
        return integerValuesHistogram.getMaxValue() * getIntegerToDoubleValueConversionRatio();
    }

    /**
     * Get the highest recorded value level in the histogram as a double
     *
     * @return the highest recorded value level in the histogram as a double
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
        return integerValuesHistogram.getMean() * getIntegerToDoubleValueConversionRatio();
    }

    /**
     * Get the lowest recorded non-zero value level in the histogram
     *
     * @return the lowest recorded non-zero value level in the histogram
     */
    public double getMinNonZeroValue() {
        return integerValuesHistogram.getMinNonZeroValue() * getIntegerToDoubleValueConversionRatio();
    }

    /**
     * Get the lowest recorded value level in the histogram
     *
     * @return the Min value recorded in the histogram
     */
    public double getMinValue() {
        return integerValuesHistogram.getMinValue() * getIntegerToDoubleValueConversionRatio();
    }

    /**
     * Get the capacity needed to encode this histogram into a ByteBuffer
     *
     * @return the capacity needed to encode this histogram into a ByteBuffer
     */
    @Override
    public int getNeededByteBufferCapacity() {
        return integerValuesHistogram.getNeededByteBufferCapacity();
    }

    /**
     * get the configured numberOfSignificantValueDigits
     *
     * @return numberOfSignificantValueDigits
     */
    public int getNumberOfSignificantValueDigits() {
        return integerValuesHistogram.numberOfSignificantValueDigits;
    }

    /**
     * Get the percentile at a given value.
     * The percentile returned is the percentile of values recorded in the histogram that are smaller
     * than or equivalent to the given value.
     * <p>
     * Note that two values are "equivalent" in this statement if
     * {@link io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram#valuesAreEquivalent} would return true.
     *
     * @param value The value for which to return the associated percentile
     * @return The percentile of values recorded in the histogram that are smaller than or equivalent
     * to the given value.
     */
    public double getPercentileAtOrBelowValue(final double value) {
        return integerValuesHistogram.getPercentileAtOrBelowValue((long) (value * getDoubleToIntegerValueConversionRatio()));
    }

    /**
     * get the start time stamp [optionally] stored with this histogram
     *
     * @return the start time stamp [optionally] stored with this histogram
     */
    public long getStartTimeStamp() {
        return integerValuesHistogram.getStartTimeStamp();
    }

    /**
     * Get the computed standard deviation of all recorded values in the histogram
     *
     * @return the standard deviation (in value units) of the histogram data
     */
    public double getStdDeviation() {
        return integerValuesHistogram.getStdDeviation() * getIntegerToDoubleValueConversionRatio();
    }

    /**
     * get the tag string [optionally] associated with this histogram
     *
     * @return tag string [optionally] associated with this histogram
     */
    public String getTag() {
        return integerValuesHistogram.getTag();
    }

    //
    //
    //
    // Timestamp and tag support:
    //
    //
    //

    /**
     * Get the total count of all recorded values in the histogram
     *
     * @return the total count of all recorded values in the histogram
     */
    public long getTotalCount() {
        return integerValuesHistogram.getTotalCount();
    }

    /**
     * Get the value at a given percentile.
     * When the percentile is &gt; 0.0, the value returned is the value that the given the given
     * percentage of the overall recorded value entries in the histogram are either smaller than
     * or equivalent to. When the percentile is 0.0, the value returned is the value that all value
     * entries in the histogram are either larger than or equivalent to.
     * <p>
     * Note that two values are "equivalent" in this statement if
     * {@link io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram#valuesAreEquivalent} would return true.
     *
     * @param percentile The percentile for which to return the associated value
     * @return The value that the given percentage of the overall recorded value entries in the
     * histogram are either smaller than or equivalent to. When the percentile is 0.0, returns the
     * value that all value entries in the histogram are either larger than or equivalent to.
     */
    public double getValueAtPercentile(final double percentile) {
        return integerValuesHistogram.getValueAtPercentile(percentile) * getIntegerToDoubleValueConversionRatio();
    }

    @Override
    public int hashCode() {
        return integerValuesHistogram.hashCode();
    }

    /**
     * Get the highest value that is equivalent to the given value within the histogram's resolution.
     * Where "equivalent" means that value samples recorded for any two
     * equivalent values are counted in a common total count.
     *
     * @param value The given value
     * @return The highest value that is equivalent to the given value within the histogram's resolution.
     */
    public double highestEquivalentValue(final double value) {
        double nextNonEquivalentValue = nextNonEquivalentValue(value);
        // Theoretically, nextNonEquivalentValue - ulp(nextNonEquivalentValue) == nextNonEquivalentValue
        // is possible (if the ulp size switches right at nextNonEquivalentValue), so drop by 2 ulps and
        // increment back up to closest within-ulp value.
        double highestEquivalentValue = nextNonEquivalentValue - (2 * Math.ulp(nextNonEquivalentValue));
        while (highestEquivalentValue + Math.ulp(highestEquivalentValue) < nextNonEquivalentValue) {
            highestEquivalentValue += Math.ulp(highestEquivalentValue);
        }

        return highestEquivalentValue;
    }

    public AbstractHistogram integerValuesHistogram() {
        return integerValuesHistogram;
    }

    public boolean isAutoResize() {
        return autoResize;
    }

    //
    //
    //
    // Histogram Data access support:
    //
    //
    //

    /**
     * Provide a means of iterating through histogram values using linear steps. The iteration is
     * performed in steps of <i>valueUnitsPerBucket</i> in size, terminating when all recorded histogram
     * values are exhausted.
     *
     * @param valueUnitsPerBucket The size (in value units) of the linear buckets to use
     * @return An {@link java.lang.Iterable}{@literal <}{@link DoubleHistogramIterationValue}{@literal >}
     * through the histogram using a
     * {@link DoubleLinearIterator}
     */
    public LinearBucketValues linearBucketValues(final double valueUnitsPerBucket) {
        return new LinearBucketValues(this, valueUnitsPerBucket);
    }

    /**
     * Provide a means of iterating through histogram values at logarithmically increasing levels. The iteration is
     * performed in steps that start at <i>valueUnitsInFirstBucket</i> and increase exponentially according to
     * <i>logBase</i>, terminating when all recorded histogram values are exhausted.
     *
     * @param valueUnitsInFirstBucket The size (in value units) of the first bucket in the iteration
     * @param logBase                 The multiplier by which bucket sizes will grow in each iteration step
     * @return An {@link java.lang.Iterable}{@literal <}{@link DoubleHistogramIterationValue}{@literal >}
     * through the histogram using
     * a {@link DoubleLogarithmicIterator}
     */
    public LogarithmicBucketValues logarithmicBucketValues(final double valueUnitsInFirstBucket,
                                                           final double logBase) {
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
    public double lowestEquivalentValue(final double value) {
        return integerValuesHistogram.lowestEquivalentValue((long) (value * getDoubleToIntegerValueConversionRatio())) *
                getIntegerToDoubleValueConversionRatio();
    }

    /**
     * Get a value that lies in the middle (rounded up) of the range of values equivalent the given value.
     * Where "equivalent" means that value samples recorded for any two
     * equivalent values are counted in a common total count.
     *
     * @param value The given value
     * @return The value lies in the middle (rounded up) of the range of values equivalent the given value.
     */
    public double medianEquivalentValue(final double value) {
        return integerValuesHistogram.medianEquivalentValue((long) (value * getDoubleToIntegerValueConversionRatio())) *
                getIntegerToDoubleValueConversionRatio();
    }

    /**
     * Get the next value that is not equivalent to the given value within the histogram's resolution.
     * Where "equivalent" means that value samples recorded for any two
     * equivalent values are counted in a common total count.
     *
     * @param value The given value
     * @return The next value that is not equivalent to the given value within the histogram's resolution.
     */
    public double nextNonEquivalentValue(final double value) {
        return integerValuesHistogram.nextNonEquivalentValue((long) (value * getDoubleToIntegerValueConversionRatio())) *
                getIntegerToDoubleValueConversionRatio();
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
        integerValuesHistogram.outputPercentileDistribution(printStream,
                percentileTicksPerHalfDistance,
                outputValueUnitScalingRatio / getIntegerToDoubleValueConversionRatio(),
                useCsvFormat);
    }

    /**
     * Provide a means of iterating through histogram values according to percentile levels. The iteration is
     * performed in steps that start at 0% and reduce their distance to 100% according to the
     * <i>percentileTicksPerHalfDistance</i> parameter, ultimately reaching 100% when all recorded histogram
     * values are exhausted.
     * <p>
     *
     * @param percentileTicksPerHalfDistance The number of iteration steps per half-distance to 100%.
     * @return An {@link java.lang.Iterable}{@literal <}{@link DoubleHistogramIterationValue}{@literal >}
     * through the histogram using a
     * {@link DoublePercentileIterator}
     */
    public Percentiles percentiles(final int percentileTicksPerHalfDistance) {
        return new Percentiles(this, percentileTicksPerHalfDistance);
    }

    /**
     * Record a value in the histogram
     *
     * @param value The value to be recorded
     * @throws CairoException (may throw) if value cannot be covered by the histogram's range
     */
    @Override
    public void recordValue(final double value) throws CairoException {
        recordSingleValue(value);
    }

    /**
     * Record a value in the histogram (adding to the value's current count)
     *
     * @param value The value to be recorded
     * @param count The number of occurrences of this value to record
     * @throws CairoException (may throw) if value cannot be covered by the histogram's range
     */
    @Override
    public void recordValueWithCount(final double value, final long count) throws CairoException {
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
     * by {@link #copyCorrectedForCoordinatedOmission(double)}.
     * The use cases for these two methods are mutually exclusive, and only one of the two should be be used on
     * a given data set to correct for the same coordinated omission issue.
     * <p>
     * See notes in the description of the Histogram calls for an illustration of why this corrective behavior is
     * important.
     *
     * @param value                               The value to record
     * @param expectedIntervalBetweenValueSamples If expectedIntervalBetweenValueSamples is larger than 0, add
     *                                            auto-generated value records as appropriate if value is larger
     *                                            than expectedIntervalBetweenValueSamples
     * @throws CairoException (may throw) if value cannot be covered by the histogram's range
     */
    @Override
    public void recordValueWithExpectedInterval(final double value, final double expectedIntervalBetweenValueSamples)
            throws CairoException {
        recordValueWithCountAndExpectedInterval(value, 1, expectedIntervalBetweenValueSamples);
    }

    /**
     * Provide a means of iterating through all recorded histogram values using the finest granularity steps
     * supported by the underlying representation. The iteration steps through all non-zero recorded value counts,
     * and terminates when all recorded histogram values are exhausted.
     *
     * @return An {@link java.lang.Iterable}{@literal <}{@link DoubleHistogramIterationValue}{@literal >}
     * through the histogram using
     * a {@link DoubleRecordedValuesIterator}
     */
    public RecordedValues recordedValues() {
        return new RecordedValues(this);
    }

    /**
     * Reset the contents and stats of this histogram
     */
    @Override
    public void reset() {
        integerValuesHistogram.reset();
        double initialLowestValueInAutoRange = Math.pow(2.0, 800);
        init(configuredHighestToLowestValueRatio, initialLowestValueInAutoRange, integerValuesHistogram);
    }

    public void setAutoResize(boolean autoResize) {
        this.autoResize = autoResize;
    }


    // Percentile iterator support:

    /**
     * Set the end time stamp value associated with this histogram to a given value.
     *
     * @param timeStampMsec the value to set the time stamp to, [by convention] in msec since the epoch.
     */
    public void setEndTimeStamp(final long timeStampMsec) {
        integerValuesHistogram.setEndTimeStamp(timeStampMsec);
    }

    // Linear iterator support:

    /**
     * Set the start time stamp value associated with this histogram to a given value.
     *
     * @param timeStampMsec the value to set the time stamp to, [by convention] in msec since the epoch.
     */
    public void setStartTimeStamp(final long timeStampMsec) {
        integerValuesHistogram.setStartTimeStamp(timeStampMsec);
    }

    // Logarithmic iterator support:

    /**
     * Set the tag string associated with this histogram
     *
     * @param tag the tag string to assciate with this histogram
     */
    public void setTag(String tag) {
        integerValuesHistogram.setTag(tag);
    }

    // Recorded value iterator support:

    /**
     * Get the size (in value units) of the range of values that are equivalent to the given value within the
     * histogram's resolution. Where "equivalent" means that value samples recorded for any two
     * equivalent values are counted in a common total count.
     *
     * @param value The given value
     * @return The lowest value that is equivalent to the given value within the histogram's resolution.
     */
    public double sizeOfEquivalentValueRange(final double value) {
        return integerValuesHistogram.sizeOfEquivalentValueRange((long) (value * getDoubleToIntegerValueConversionRatio())) *
                getIntegerToDoubleValueConversionRatio();
    }

    // AllValues iterator support:

    /**
     * Subtract the contents of another histogram from this one.
     *
     * @param otherHistogram The other histogram.
     * @throws CairoException (may throw) if values in fromHistogram's cannot be
     *                        covered by this histogram's range
     */
    public void subtract(final DoubleHistogram otherHistogram) {
        int arrayLength = otherHistogram.integerValuesHistogram.countsArrayLength;
        AbstractHistogram otherIntegerHistogram = otherHistogram.integerValuesHistogram;
        for (int i = 0; i < arrayLength; i++) {
            long otherCount = otherIntegerHistogram.getCountAtIndex(i);
            if (otherCount > 0) {
                double otherValue = otherIntegerHistogram.valueFromIndex(i) *
                        otherHistogram.getIntegerToDoubleValueConversionRatio();
                if (getCountAtValue(otherValue) < otherCount) {
                    throw new IllegalArgumentException("otherHistogram count (" + otherCount + ") at value " +
                            otherValue + " is larger than this one's (" + getCountAtValue(otherValue) + ")");
                }
                recordValueWithCount(otherValue, -otherCount);
            }
        }
    }

    /**
     * Determine if two values are equivalent with the histogram's resolution.
     * Where "equivalent" means that value samples recorded for any two
     * equivalent values are counted in a common total count.
     *
     * @param value1 first value to compare
     * @param value2 second value to compare
     * @return True if values are equivalent to within the histogram's resolution.
     */
    public boolean valuesAreEquivalent(final double value1, final double value2) {
        return (lowestEquivalentValue(value1) == lowestEquivalentValue(value2));
    }

    //
    //
    //
    // Textual percentile output support:
    //
    //
    //

    private static int findContainingBinaryOrderOfMagnitude(final long longNumber) {
        // smallest power of 2 containing value
        return 64 - Long.numberOfLeadingZeros(longNumber);
    }

    private static int findContainingBinaryOrderOfMagnitude(final double doubleNumber) {
        long longNumber = (long) Math.ceil(doubleNumber);
        return findContainingBinaryOrderOfMagnitude(longNumber);
    }

    //
    //
    //
    // Serialization support:
    //
    //
    //

    private void autoAdjustRangeForValue(final double value) {
        // Zero is always valid, and doesn't need auto-range adjustment:
        if (value == 0.0) {
            return;
        }
        autoAdjustRangeForValueSlowPath(value);
    }

    private synchronized void autoAdjustRangeForValueSlowPath(final double value) {
        try {
            if (value < currentLowestValueInAutoRange) {
                if (value < 0.0) {
                    throw CairoException.nonCritical().put("Negative values cannot be recorded");
                }
                do {
                    int shiftAmount =
                            findCappedContainingBinaryOrderOfMagnitude(
                                    Math.ceil(currentLowestValueInAutoRange / value) - 1.0);
                    shiftCoveredRangeToTheRight(shiftAmount);
                }
                while (value < currentLowestValueInAutoRange);
            } else if (value >= currentHighestValueLimitInAutoRange) {
                if (value > highestAllowedValueEver) {
                    throw CairoException.nonCritical().put("Values above ").put(highestAllowedValueEver).put(" cannot be recorded");
                }
                do {
                    // If value is an exact whole multiple of currentHighestValueLimitInAutoRange, it "belongs" with
                    // the next level up, as it crosses the limit. With floating point values, the simplest way to
                    // make this shift on exact multiple values happen (but not for any just-smaller-than-exact-multiple
                    // values) is to use a value that is 1 ulp bigger in computing the ratio for the shift amount:
                    int shiftAmount =
                            findCappedContainingBinaryOrderOfMagnitude(
                                    Math.ceil((value + Math.ulp(value)) / currentHighestValueLimitInAutoRange) - 1.0);
                    shiftCoveredRangeToTheLeft(shiftAmount);
                }
                while (value >= currentHighestValueLimitInAutoRange);
            }
        } catch (CairoException ex) {
            // Build the base error message first
            CairoException err = CairoException.nonCritical().put("The value ").put(value)
                    .put(" is out of bounds for histogram, current covered range [")
                    .put(currentLowestValueInAutoRange).put(", ").put(currentHighestValueLimitInAutoRange)
                    .put(") cannot be extended any further.\nCaused by: ").put(ex.getMessage());


            err.put("\nHint: Try normalizing your values to fit into the covered range");
            if (getNumberOfSignificantValueDigits() > 0) {
                err.put("\nOr lower the histogram precision (for example, use approx_median(x, 0))");
            }


            throw err;
        }
    }

    private long deriveIntegerValueRange(final long externalHighestToLowestValueRatio,
                                         final int numberOfSignificantValueDigits) {
        long internalHighestToLowestValueRatio =
                deriveInternalHighestToLowestValueRatio(externalHighestToLowestValueRatio);

        // We cannot use the bottom half of bucket 0 in an integer values histogram to represent double
        // values, because the required precision does not exist there. We therefore need the integer
        // range to be bigger, such that the entire double value range can fit in the upper halves of
        // all buckets. Compute the integer value range that will achieve this:

        long lowestTackingIntegerValue = AbstractHistogram.numberOfSubbuckets(numberOfSignificantValueDigits) / 2;
        return lowestTackingIntegerValue * internalHighestToLowestValueRatio;
    }

    //
    //
    //
    // Encoding/Decoding support:
    //
    //
    //

    private long deriveInternalHighestToLowestValueRatio(final long externalHighestToLowestValueRatio) {
        // Internal dynamic range needs to be 1 order of magnitude larger than the containing order of magnitude.
        // e.g. the dynamic range that covers [0.9, 2.1) is 2.33x, which on it's own would require 4x range to
        // cover the contained order of magnitude. But (if 1.0 was a bucket boundary, for example, the range
        // will actually need to cover [0.5..1.0) [1.0..2.0) [2.0..4.0), mapping to an 8x internal dynamic range.
        return 1L << (findContainingBinaryOrderOfMagnitude(externalHighestToLowestValueRatio) + 1);
    }

    private int findCappedContainingBinaryOrderOfMagnitude(final double doubleNumber) {
        if (doubleNumber > configuredHighestToLowestValueRatio) {
            return (int) (Math.log(configuredHighestToLowestValueRatio) / Math.log(2));
        }
        if (doubleNumber > Math.pow(2.0, 50)) {
            return 50;
        }
        return findContainingBinaryOrderOfMagnitude(doubleNumber);
    }

    private long getLowestTrackingIntegerValue() {
        return integerValuesHistogram.subBucketHalfCount;
    }

    private int getNeededByteBufferCapacity(final int relevantLength) {
        return integerValuesHistogram.getNeededByteBufferCapacity(relevantLength);
    }

    private void handleShiftValuesException(final int numberOfBinaryOrdersOfMagnitude, Exception ex) {
        if (!autoResize) {
            throw CairoException.nonCritical().put("Value outside of histogram covered range.\nCaused by: ").put(ex.getMessage());
        }

        long highestTrackableValue = integerValuesHistogram.getHighestTrackableValue();
        int currentContainingOrderOfMagnitude = findContainingBinaryOrderOfMagnitude(highestTrackableValue);
        int newContainingOrderOfMagnitude = numberOfBinaryOrdersOfMagnitude + currentContainingOrderOfMagnitude;
        if (newContainingOrderOfMagnitude > 63) {
            throw CairoException.nonCritical().put("Cannot resize histogram covered range beyond (1L << 63) / (1L << ")
                    .put(integerValuesHistogram.subBucketHalfCountMagnitude)
                    .put(") - 1.\nCaused by: ").put(ex.getMessage());
        }
        long newHighestTrackableValue = (1L << newContainingOrderOfMagnitude) - 1;
        integerValuesHistogram.resize(newHighestTrackableValue);
        integerValuesHistogram.highestTrackableValue = newHighestTrackableValue;
        configuredHighestToLowestValueRatio <<= numberOfBinaryOrdersOfMagnitude;
    }

    private void init(final long configuredHighestToLowestValueRatio, final double lowestTrackableUnitValue,
                      final AbstractHistogram integerValuesHistogram) {
        this.configuredHighestToLowestValueRatio = configuredHighestToLowestValueRatio;
        this.integerValuesHistogram = integerValuesHistogram;
        long internalHighestToLowestValueRatio =
                deriveInternalHighestToLowestValueRatio(configuredHighestToLowestValueRatio);
        setTrackableValueRange(lowestTrackableUnitValue, lowestTrackableUnitValue * internalHighestToLowestValueRatio);
    }

    private void readObject(final ObjectInputStream o)
            throws IOException, ClassNotFoundException {
        final long configuredHighestToLowestValueRatio = o.readLong();
        final double lowestValueInAutoRange = o.readDouble();
        AbstractHistogram integerValuesHistogram = (AbstractHistogram) o.readObject();
        init(configuredHighestToLowestValueRatio, lowestValueInAutoRange, integerValuesHistogram);
    }

    private void recordCountAtValue(final long count, final double value) throws CairoException {
        int throwCount = 0;
        while (true) {
            if ((value < currentLowestValueInAutoRange) || (value >= currentHighestValueLimitInAutoRange)) {
                // Zero is valid and needs no auto-ranging, but also rare enough that we should deal
                // with it on the slow path...
                autoAdjustRangeForValue(value);
            }
            try {
                integerValuesHistogram.recordConvertedDoubleValueWithCount(value, count);
                return;
            } catch (CairoException ex) {
                // A race that would pass the auto-range check above and would still take an AIOOB
                // can only occur due to a value that would have been valid becoming invalid due
                // to a concurrent adjustment operation. Such adjustment operations can happen no
                // more than 64 times in the entire lifetime of the Histogram, which makes it safe
                // to retry with no fear of live-locking.
                if (++throwCount > 64) {
                    // For the retry check to not detect an out of range attempt after 64 retries
                    // should be  theoretically impossible, and would indicate a bug.
                    throw CairoException.nonCritical().put("BUG: Unexpected non-transient AIOOB Exception caused by:\n")
                            .put(ex.getMessage());
                }
            }
        }
    }

    private void recordSingleValue(final double value) throws CairoException {
        int throwCount = 0;
        while (true) {
            if ((value < currentLowestValueInAutoRange) || (value >= currentHighestValueLimitInAutoRange)) {
                // Zero is valid and needs no auto-ranging, but also rare enough that we should deal
                // with it on the slow path...
                autoAdjustRangeForValue(value);
            }
            try {
                integerValuesHistogram.recordConvertedDoubleValue(value);
                return;
            } catch (CairoException ex) {
                // A race that would pass the auto-range check above and would still take an AIOOB
                // can only occur due to a value that would have been valid becoming invalid due
                // to a concurrent adjustment operation. Such adjustment operations can happen no
                // more than 64 times in the entire lifetime of the Histogram, which makes it safe
                // to retry with no fear of live-locking.
                if (++throwCount > 64) {
                    // For the retry check to not detect an out of range attempt after 64 retries
                    // should be  theoretically impossible, and would indicate a bug.
                    throw CairoException.nonCritical().put("BUG: Unexpected non-transient AIOOB Exception caused by:\n")
                            .put(ex.getMessage());
                }
            }
        }
    }

    private void recordValueWithCountAndExpectedInterval(
            final double value,
            final long count,
            final double expectedIntervalBetweenValueSamples
    ) throws CairoException {
        recordCountAtValue(count, value);
        if (expectedIntervalBetweenValueSamples <= 0)
            return;
        for (double missingValue = value - expectedIntervalBetweenValueSamples;
             missingValue >= expectedIntervalBetweenValueSamples;
             missingValue -= expectedIntervalBetweenValueSamples) {
            recordCountAtValue(count, missingValue);
        }
    }

    private void setTrackableValueRange(final double lowestValueInAutoRange, final double highestValueInAutoRange) {
        this.currentLowestValueInAutoRange = lowestValueInAutoRange;
        this.currentHighestValueLimitInAutoRange = highestValueInAutoRange;
        double integerToDoubleValueConversionRatio = lowestValueInAutoRange / getLowestTrackingIntegerValue();
        integerValuesHistogram.setIntegerToDoubleValueConversionRatio(integerToDoubleValueConversionRatio);
    }

    private void shiftCoveredRangeToTheLeft(final int numberOfBinaryOrdersOfMagnitude) {
        // We are going to adjust the tracked range by effectively shifting it to the right
        // (in the integer shift sense).
        //
        // To counter the left shift of the value multipliers, we need to right shift the internal
        // representation such that the newly shifted integer values will continue to return the
        // same double values.

        // Initially, new range is the same as current range, to make sure we correctly recover
        // from a shift failure if one happens:
        double newLowestValueInAutoRange = currentLowestValueInAutoRange;
        double newHighestValueLimitInAutoRange = currentHighestValueLimitInAutoRange;

        try {
            double shiftMultiplier = 1.0 * (1L << numberOfBinaryOrdersOfMagnitude);

            double newIntegerToDoubleValueConversionRatio =
                    getIntegerToDoubleValueConversionRatio() * shiftMultiplier;

            // First, temporarily change the lowest value in auto-range without changing conversion ratios.
            // This is done to force new values lower than the new expected lowest value to attempt an
            // adjustment (which is synchronized and will wait behind this one). This ensures that we will
            // not end up with any concurrently recorded values that would need to be discarded if the shift
            // fails. If this shift succeeds, the pending adjustment attempt will end up doing nothing.
            currentLowestValueInAutoRange *= shiftMultiplier;

            // First shift the values, to give the shift a chance to fail:

            // Shift integer histogram right, decreasing the recorded integer values for current recordings
            // by a factor of (1 << numberOfBinaryOrdersOfMagnitude):

            // (no need to shift any values if all recorded values are at the 0 value level:)
            if (getTotalCount() > integerValuesHistogram.getCountAtIndex(0)) {
                // Apply the shift:
                try {
                    integerValuesHistogram.shiftValuesRight(numberOfBinaryOrdersOfMagnitude,
                            newIntegerToDoubleValueConversionRatio);
                    // Shift was successful. Adjust new range to reflect:
                    newLowestValueInAutoRange *= shiftMultiplier;
                    newHighestValueLimitInAutoRange *= shiftMultiplier;
                } catch (CairoException ex) {
                    // Failed to shift, try to expand size instead:
                    handleShiftValuesException(numberOfBinaryOrdersOfMagnitude, ex);
                    // Successfully expanded histogram range by numberOfBinaryOrdersOfMagnitude, but not
                    // by shifting (shifting failed because there was not room to shift right into). Instead,
                    // we grew the max value without changing the value mapping. Since we were trying to
                    // shift values right to begin with to make room for a larger value than we had had
                    // been able to fit before, no shift is needed, as the value should now fit. So rather
                    // than shifting and adjusting both lowest and highest limits, we'll end up just
                    // expanding newHighestValueLimitInAutoRange to indicate the newly expanded range.
                    // We therefore reverse-scale the newLowestValueInAutoRange before lating the later
                    // code scale both up:
                    newLowestValueInAutoRange /= shiftMultiplier;
                }
            }
            // Shift (or resize) was successful. Adjust new range to reflect:
            newLowestValueInAutoRange *= shiftMultiplier;
            newHighestValueLimitInAutoRange *= shiftMultiplier;
        } finally {
            // Set the new range to either the successfully changed one, or the original one:
            setTrackableValueRange(newLowestValueInAutoRange, newHighestValueLimitInAutoRange);
        }
    }

    private void shiftCoveredRangeToTheRight(final int numberOfBinaryOrdersOfMagnitude) {
        // We are going to adjust the tracked range by effectively shifting it to the right
        // (in the integer shift sense).
        //
        // To counter the right shift of the value multipliers, we need to left shift the internal
        // representation such that the newly shifted integer values will continue to return the
        // same double values.

        // Initially, new range is the same as current range, to make sure we correctly recover
        // from a shift failure if one happens:
        double newLowestValueInAutoRange = currentLowestValueInAutoRange;
        double newHighestValueLimitInAutoRange = currentHighestValueLimitInAutoRange;

        try {
            double shiftMultiplier = 1.0 / (1L << numberOfBinaryOrdersOfMagnitude);

            // First, temporarily change the highest value in auto-range without changing conversion ratios.
            // This is done to force new values higher than the new expected highest value to attempt an
            // adjustment (which is synchronized and will wait behind this one). This ensures that we will
            // not end up with any concurrently recorded values that would need to be discarded if the shift
            // fails. If this shift succeeds, the pending adjustment attempt will end up doing nothing.
            currentHighestValueLimitInAutoRange *= shiftMultiplier;

            double newIntegerToDoubleValueConversionRatio =
                    getIntegerToDoubleValueConversionRatio() * shiftMultiplier;

            // First shift the values, to give the shift a chance to fail:

            // Shift integer histogram left, increasing the recorded integer values for current recordings
            // by a factor of (1 << numberOfBinaryOrdersOfMagnitude):

            // (no need to shift any values if all recorded values are at the 0 value level:)
            if (getTotalCount() > integerValuesHistogram.getCountAtIndex(0)) {
                // Apply the shift:
                try {
                    integerValuesHistogram.shiftValuesLeft(numberOfBinaryOrdersOfMagnitude,
                            newIntegerToDoubleValueConversionRatio);
                } catch (CairoException ex) {
                    // Failed to shift, try to expand size instead:
                    handleShiftValuesException(numberOfBinaryOrdersOfMagnitude, ex);
                    // First expand the highest limit to reflect successful size expansion:
                    newHighestValueLimitInAutoRange /= shiftMultiplier;
                    // Successfully expanded histogram range by numberOfBinaryOrdersOfMagnitude, but not
                    // by shifting (shifting failed because there was not room to shift left into). Instead,
                    // we grew the max value without changing the value mapping. Since we were trying to
                    // shift values left to begin with, trying to shift the left again will work (we now
                    // have room to shift into):
                    integerValuesHistogram.shiftValuesLeft(numberOfBinaryOrdersOfMagnitude,
                            newIntegerToDoubleValueConversionRatio);
                }
            }
            // Shift (or resize) was successful. Adjust new range to reflect:
            newLowestValueInAutoRange *= shiftMultiplier;
            newHighestValueLimitInAutoRange *= shiftMultiplier;
        } finally {
            // Set the new range to either the successfully changed one, or the original one:
            setTrackableValueRange(newLowestValueInAutoRange, newHighestValueLimitInAutoRange);
        }
    }

    private void writeObject(final ObjectOutputStream o)
            throws IOException {
        o.writeLong(configuredHighestToLowestValueRatio);
        o.writeDouble(currentLowestValueInAutoRange);
        o.writeObject(integerValuesHistogram);
    }

    static <T extends DoubleHistogram> T constructHistogramFromBuffer(
            int cookie,
            final ByteBuffer buffer,
            final Class<T> doubleHistogramClass,
            final Class<? extends AbstractHistogram> histogramClass,
            final long minBarForHighestToLowestValueRatio) throws DataFormatException {
        int numberOfSignificantValueDigits = buffer.getInt();
        long configuredHighestToLowestValueRatio = buffer.getLong();
        final AbstractHistogram valuesHistogram;
        if (isNonCompressedDoubleHistogramCookie(cookie)) {
            valuesHistogram =
                    AbstractHistogram.decodeFromByteBuffer(buffer, histogramClass, minBarForHighestToLowestValueRatio);
        } else if (isCompressedDoubleHistogramCookie(cookie)) {
            valuesHistogram =
                    AbstractHistogram.decodeFromCompressedByteBuffer(buffer, histogramClass, minBarForHighestToLowestValueRatio);
        } else {
            throw new IllegalArgumentException("The buffer does not contain a DoubleHistogram");
        }

        try {
            Constructor<T> doubleHistogramConstructor =
                    doubleHistogramClass.getDeclaredConstructor(constructorArgTypes);

            T histogram =
                    doubleHistogramConstructor.newInstance(
                            configuredHighestToLowestValueRatio,
                            numberOfSignificantValueDigits,
                            histogramClass,
                            valuesHistogram
                    );
            histogram.setAutoResize(true);
            return histogram;
        } catch (NoSuchMethodException | InstantiationException |
                 IllegalAccessException | InvocationTargetException ex) {
            throw new IllegalStateException("Unable to construct DoubleHistogram of type " + doubleHistogramClass);
        }
    }

    static boolean isCompressedDoubleHistogramCookie(int cookie) {
        return (cookie == DHIST_compressedEncodingCookie);
    }

    static boolean isDoubleHistogramCookie(int cookie) {
        return isCompressedDoubleHistogramCookie(cookie) || isNonCompressedDoubleHistogramCookie(cookie);
    }

    //
    //
    //
    // Internal helper methods:
    //
    //
    //

    static boolean isNonCompressedDoubleHistogramCookie(int cookie) {
        return (cookie == DHIST_encodingCookie);
    }

    /**
     * An {@link java.lang.Iterable}{@literal <}{@link DoubleHistogramIterationValue}{@literal >} through
     * the histogram using a {@link DoubleAllValuesIterator}
     */
    public static class AllValues implements Iterable<DoubleHistogramIterationValue> {
        final DoubleHistogram histogram;

        private AllValues(final DoubleHistogram histogram) {
            this.histogram = histogram;
        }

        /**
         * @return A {@link DoubleAllValuesIterator}{@literal <}{@link HistogramIterationValue}{@literal >}
         */
        public Iterator<DoubleHistogramIterationValue> iterator() {
            return new DoubleAllValuesIterator(histogram);
        }
    }

    /**
     * An {@link java.lang.Iterable}{@literal <}{@link DoubleHistogramIterationValue}{@literal >} through
     * the histogram using a {@link DoubleLinearIterator}
     */
    public static class LinearBucketValues implements Iterable<DoubleHistogramIterationValue> {
        final DoubleHistogram histogram;
        final double valueUnitsPerBucket;

        private LinearBucketValues(final DoubleHistogram histogram, final double valueUnitsPerBucket) {
            this.histogram = histogram;
            this.valueUnitsPerBucket = valueUnitsPerBucket;
        }

        /**
         * @return A {@link DoubleLinearIterator}{@literal <}{@link DoubleHistogramIterationValue}{@literal >}
         */
        public Iterator<DoubleHistogramIterationValue> iterator() {
            return new DoubleLinearIterator(histogram, valueUnitsPerBucket);
        }
    }

    /**
     * An {@link java.lang.Iterable}{@literal <}{@link DoubleHistogramIterationValue}{@literal >} through
     * the histogram using a {@link DoubleLogarithmicIterator}
     */
    public static class LogarithmicBucketValues implements Iterable<DoubleHistogramIterationValue> {
        final DoubleHistogram histogram;
        final double logBase;
        final double valueUnitsInFirstBucket;

        private LogarithmicBucketValues(final DoubleHistogram histogram,
                                        final double valueUnitsInFirstBucket, final double logBase) {
            this.histogram = histogram;
            this.valueUnitsInFirstBucket = valueUnitsInFirstBucket;
            this.logBase = logBase;
        }

        /**
         * @return A {@link DoubleLogarithmicIterator}{@literal <}{@link DoubleHistogramIterationValue}{@literal >}
         */
        public @NotNull Iterator<DoubleHistogramIterationValue> iterator() {
            return new DoubleLogarithmicIterator(histogram, valueUnitsInFirstBucket, logBase);
        }
    }

    /**
     * An {@link java.lang.Iterable}{@literal <}{@link DoubleHistogramIterationValue}{@literal >} through
     * the histogram using a {@link DoublePercentileIterator}
     */
    public static class Percentiles implements Iterable<DoubleHistogramIterationValue> {
        final DoubleHistogram histogram;
        final int percentileTicksPerHalfDistance;

        private Percentiles(final DoubleHistogram histogram, final int percentileTicksPerHalfDistance) {
            this.histogram = histogram;
            this.percentileTicksPerHalfDistance = percentileTicksPerHalfDistance;
        }

        /**
         * @return A {@link DoublePercentileIterator}{@literal <}{@link DoubleHistogramIterationValue}{@literal >}
         */
        public @NotNull Iterator<DoubleHistogramIterationValue> iterator() {
            return new DoublePercentileIterator(histogram, percentileTicksPerHalfDistance);
        }
    }

    /**
     * An {@link java.lang.Iterable}{@literal <}{@link DoubleHistogramIterationValue}{@literal >} through
     * the histogram using a {@link DoubleRecordedValuesIterator}
     */
    public static class RecordedValues implements Iterable<DoubleHistogramIterationValue> {
        final DoubleHistogram histogram;

        private RecordedValues(final DoubleHistogram histogram) {
            this.histogram = histogram;
        }

        /**
         * @return A {@link DoubleRecordedValuesIterator}{@literal <}{@link HistogramIterationValue}{@literal >}
         */
        public @NotNull Iterator<DoubleHistogramIterationValue> iterator() {
            return new DoubleRecordedValuesIterator(histogram);
        }
    }

    static {
        // We don't want to allow the histogram to shift and expand into value ranges that could equate
        // to infinity (e.g. 1024.0 * (Double.MAX_VALUE / 1024.0) == Infinity). So lets makes sure the
        // highestAllowedValueEver cap is a couple of bindary orders of magnitude away from MAX_VALUE:

        // Choose a highestAllowedValueEver that is a nice power of 2 multiple of 1.0 :
        double value = 1.0;
        while (value < Double.MAX_VALUE / 4.0) {
            value *= 2;
        }
        highestAllowedValueEver = value;
    }
}
