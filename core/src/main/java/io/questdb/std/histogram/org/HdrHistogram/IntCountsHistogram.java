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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.DataFormatException;

/**
 * <h2>A High Dynamic Range (HDR) Histogram using an <b><code>int</code></b> count type </h2>
 * <p>
 * See package description for {@link io.questdb.std.histogram.org.HdrHistogram} for details.
 */

public class IntCountsHistogram extends AbstractHistogram {
    int[] counts;
    int normalizingIndexOffset;
    long totalCount;

    /**
     * Construct an auto-resizing IntCountsHistogram with a lowest discernible value of 1 and an auto-adjusting
     * highestTrackableValue. Can auto-resize up to track values up to (Long.MAX_VALUE / 2).
     *
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     */
    public IntCountsHistogram(final int numberOfSignificantValueDigits) {
        this(1, 2, numberOfSignificantValueDigits);
        setAutoResize(true);
    }

    /**
     * Construct a IntCountsHistogram given the Highest value to be tracked and a number of significant decimal digits. The
     * histogram will be constructed to implicitly track (distinguish from 0) values as low as 1.
     *
     * @param highestTrackableValue          The highest value to be tracked by the histogram. Must be a positive
     *                                       integer that is {@literal >=} 2.
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     */
    public IntCountsHistogram(final long highestTrackableValue, final int numberOfSignificantValueDigits) {
        this(1, highestTrackableValue, numberOfSignificantValueDigits);
    }

    /**
     * Construct a IntCountsHistogram given the Lowest and Highest values to be tracked and a number of significant
     * decimal digits. Providing a lowestDiscernibleValue is useful is situations where the units used
     * for the histogram's values are much smaller that the minimal accuracy required. E.g. when tracking
     * time values stated in nanosecond units, where the minimal accuracy required is a microsecond, the
     * proper value for lowestDiscernibleValue would be 1000.
     *
     * @param lowestDiscernibleValue         The lowest value that can be tracked (distinguished from 0) by the histogram.
     *                                       Must be a positive integer that is {@literal >=} 1. May be internally rounded
     *                                       down to nearest power of 2.
     * @param highestTrackableValue          The highest value to be tracked by the histogram. Must be a positive
     *                                       integer that is {@literal >=} (2 * lowestDiscernibleValue).
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     */
    public IntCountsHistogram(final long lowestDiscernibleValue, final long highestTrackableValue,
                              final int numberOfSignificantValueDigits) {
        super(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
        counts = new int[countsArrayLength];
        wordSizeInBytes = 4;
    }

    /**
     * Construct a histogram with the same range settings as a given source histogram,
     * duplicating the source's start/end timestamps (but NOT it's contents)
     *
     * @param source The source histogram to duplicate
     */
    public IntCountsHistogram(final AbstractHistogram source) {
        super(source);
        counts = new int[countsArrayLength];
        wordSizeInBytes = 4;
    }

    /**
     * Construct a new histogram by decoding it from a ByteBuffer.
     *
     * @param buffer                         The buffer to decode from
     * @param minBarForHighestTrackableValue Force highestTrackableValue to be set at least this high
     * @return The newly constructed histogram
     */
    public static IntCountsHistogram decodeFromByteBuffer(final ByteBuffer buffer,
                                                          final long minBarForHighestTrackableValue) {
        return decodeFromByteBuffer(buffer, IntCountsHistogram.class, minBarForHighestTrackableValue);
    }

    /**
     * Construct a new histogram by decoding it from a compressed form in a ByteBuffer.
     *
     * @param buffer                         The buffer to decode from
     * @param minBarForHighestTrackableValue Force highestTrackableValue to be set at least this high
     * @return The newly constructed histogram
     * @throws DataFormatException on error parsing/decompressing the buffer
     */
    public static IntCountsHistogram decodeFromCompressedByteBuffer(final ByteBuffer buffer,
                                                                    final long minBarForHighestTrackableValue)
            throws DataFormatException {
        return decodeFromCompressedByteBuffer(buffer, IntCountsHistogram.class, minBarForHighestTrackableValue);
    }

    /**
     * Construct a new IntCountsHistogram by decoding it from a String containing a base64 encoded
     * compressed histogram representation.
     *
     * @param base64CompressedHistogramString A string containing a base64 encoding of a compressed histogram
     * @return A IntCountsHistogram decoded from the string
     * @throws DataFormatException on error parsing/decompressing the input
     */
    public static IntCountsHistogram fromString(final String base64CompressedHistogramString)
            throws DataFormatException {
        return decodeFromCompressedByteBuffer(
                ByteBuffer.wrap(Base64Helper.parseBase64Binary(base64CompressedHistogramString)),
                0);
    }

    @Override
    public IntCountsHistogram copy() {
        IntCountsHistogram copy = new IntCountsHistogram(this);
        copy.add(this);
        return copy;
    }

    @Override
    public IntCountsHistogram copyCorrectedForCoordinatedOmission(final long expectedIntervalBetweenValueSamples) {
        IntCountsHistogram toHistogram = new IntCountsHistogram(this);
        toHistogram.addWhileCorrectingForCoordinatedOmission(this, expectedIntervalBetweenValueSamples);
        return toHistogram;
    }

    @Override
    public long getCountAtIndex(final int index) {
        return counts[normalizeIndex(index, normalizingIndexOffset, countsArrayLength)];
    }

    @Override
    public long getTotalCount() {
        return totalCount;
    }

    private void readObject(final ObjectInputStream o)
            throws IOException, ClassNotFoundException {
        o.defaultReadObject();
    }

    @Override
    int _getEstimatedFootprintInBytes() {
        return (512 + (4 * counts.length));
    }

    @Override
    void addToCountAtIndex(final int index, final long value) {
        int normalizedIndex = normalizeIndex(index, normalizingIndexOffset, countsArrayLength);

        long currentCount = counts[normalizedIndex];
        long newCount = (currentCount + value);
        if ((newCount < Integer.MIN_VALUE) || (newCount > Integer.MAX_VALUE)) {
            throw new IllegalStateException("would overflow integer count");
        }
        counts[normalizedIndex] = (int) newCount;
    }

    @Override
    void addToTotalCount(long value) {
        totalCount += value;
    }

    @Override
    void clearCounts() {
        java.util.Arrays.fill(counts, 0);
        totalCount = 0;
    }

    @Override
    long getCountAtNormalizedIndex(final int index) {
        return counts[index];
    }

    @Override
    int getNormalizingIndexOffset() {
        return normalizingIndexOffset;
    }

    @Override
    void incrementCountAtIndex(final int index) {
        int normalizedIndex = normalizeIndex(index, normalizingIndexOffset, countsArrayLength);
        int currentCount = counts[normalizedIndex];
        int newCount = currentCount + 1;
        if (newCount < 0) {
            throw new IllegalStateException("would overflow integer count");
        }
        counts[normalizedIndex] = newCount;
    }

    @Override
    void incrementTotalCount() {
        totalCount++;
    }

    @Override
    void resize(long newHighestTrackableValue) {
        int oldNormalizedZeroIndex = normalizeIndex(0, normalizingIndexOffset, countsArrayLength);

        establishSize(newHighestTrackableValue);

        int countsDelta = countsArrayLength - counts.length;

        counts = Arrays.copyOf(counts, countsArrayLength);

        if (oldNormalizedZeroIndex != 0) {
            // We need to shift the stuff from the zero index and up to the end of the array:
            int newNormalizedZeroIndex = oldNormalizedZeroIndex + countsDelta;
            int lengthToCopy = (countsArrayLength - countsDelta) - oldNormalizedZeroIndex;
            System.arraycopy(counts, oldNormalizedZeroIndex, counts, newNormalizedZeroIndex, lengthToCopy);
            Arrays.fill(counts, oldNormalizedZeroIndex, newNormalizedZeroIndex, 0);
        }
    }

    @Override
    void setCountAtIndex(int index, long value) {
        setCountAtNormalizedIndex(normalizeIndex(index, normalizingIndexOffset, countsArrayLength), value);
    }

    @Override
    void setCountAtNormalizedIndex(int index, long value) {
        if ((value < 0) || (value > Integer.MAX_VALUE)) {
            throw new IllegalStateException("would overflow integer count");
        }
        counts[index] = (int) value;
    }

    @Override
    void setIntegerToDoubleValueConversionRatio(double integerToDoubleValueConversionRatio) {
        nonConcurrentSetIntegerToDoubleValueConversionRatio(integerToDoubleValueConversionRatio);
    }

    @Override
    void setNormalizingIndexOffset(int normalizingIndexOffset) {
        this.normalizingIndexOffset = normalizingIndexOffset;
    }

    @Override
    void setTotalCount(final long totalCount) {
        this.totalCount = totalCount;
    }

    @Override
    void shiftNormalizingIndexByOffset(int offsetToAdd,
                                       boolean lowestHalfBucketPopulated,
                                       double newIntegerToDoubleValueConversionRatio) {
        nonConcurrentNormalizingIndexShift(offsetToAdd, lowestHalfBucketPopulated);
    }
}