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

import io.questdb.std.histogram.org.HdrHistogram.packedarray.PackedLongArray;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;

/**
 * <h2>A High Dynamic Range (HDR) Histogram that uses a packed internal representation</h2>
 * <p>
 * {@link PackedHistogram} supports the recording and analyzing sampled data value counts across a configurable
 * integer value range with configurable value precision within the range. Value precision is expressed as the
 * number of significant digits in the value recording, and provides control over value quantization behavior
 * across the value range and the subsequent value resolution at any given level.
 * <p>
 * {@link PackedHistogram} tracks value counts in a packed internal representation optimized
 * for typical histogram recoded values are sparse in the value range and tend to be incremented in small unit counts.
 * This packed representation tends to require significantly smaller amounts of storage when compared to unpacked
 * representations, but can incur additional recording cost due to resizing and repacking operations that may
 * occur as previously unrecorded values are encountered.
 * <p>
 * For example, a {@link PackedHistogram} could be configured to track the counts of observed integer values between 0 and
 * 3,600,000,000,000 while maintaining a value precision of 3 significant digits across that range. Value quantization
 * within the range will thus be no larger than 1/1,000th (or 0.1%) of any value. This example Histogram could
 * be used to track and analyze the counts of observed response times ranging between 1 nanosecond and 1 hour
 * in magnitude, while maintaining a value resolution of 1 microsecond up to 1 millisecond, a resolution of
 * 1 millisecond (or better) up to one second, and a resolution of 1 second (or better) up to 1,000 seconds. At its
 * maximum tracked value (1 hour), it would still maintain a resolution of 3.6 seconds (or better).
 * <p>
 * Auto-resizing: When constructed with no specified value range range (or when auto-resize is turned on with {@link
 * Histogram#setAutoResize}) a {@link PackedHistogram} will auto-resize its dynamic range to include recorded values as
 * they are encountered. Note that recording calls that cause auto-resizing may take longer to execute, as resizing
 * incurs allocation and copying of internal data structures.
 * <p>
 * See package description for {@link io.questdb.std.histogram.org.HdrHistogram} for details.
 */

public class PackedHistogram extends Histogram {

    private PackedLongArray packedCounts;

    /**
     * Construct an auto-resizing PackedHistogram with a lowest discernible value of 1 and an auto-adjusting
     * highestTrackableValue. Can auto-resize up to track values up to (Long.MAX_VALUE / 2).
     *
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     */
    public PackedHistogram(final int numberOfSignificantValueDigits) {
        this(1, 2, numberOfSignificantValueDigits);
        setAutoResize(true);
    }

    /**
     * Construct a PackedHistogram given the Highest value to be tracked and a number of significant decimal digits. The
     * histogram will be constructed to implicitly track (distinguish from 0) values as low as 1.
     *
     * @param highestTrackableValue          The highest value to be tracked by the histogram. Must be a positive
     *                                       integer that is {@literal >=} 2.
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     */
    public PackedHistogram(final long highestTrackableValue, final int numberOfSignificantValueDigits) {
        this(1, highestTrackableValue, numberOfSignificantValueDigits);
    }

    /**
     * Construct a PackedHistogram given the Lowest and Highest values to be tracked and a number of significant
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
    public PackedHistogram(final long lowestDiscernibleValue, final long highestTrackableValue, final int numberOfSignificantValueDigits) {
        super(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits, false);
        packedCounts = new PackedLongArray(countsArrayLength);
        wordSizeInBytes = 8;
    }

    /**
     * Construct a PackedHistogram with the same range settings as a given source histogram,
     * duplicating the source's start/end timestamps (but NOT it's contents)
     *
     * @param source The source histogram to duplicate
     */
    public PackedHistogram(final AbstractHistogram source) {
        super(source, false);
        packedCounts = new PackedLongArray(countsArrayLength);
        wordSizeInBytes = 8;
    }

    /**
     * Construct a new histogram by decoding it from a ByteBuffer.
     *
     * @param buffer                         The buffer to decode from
     * @param minBarForHighestTrackableValue Force highestTrackableValue to be set at least this high
     * @return The newly constructed histogram
     */
    public static PackedHistogram decodeFromByteBuffer(final ByteBuffer buffer,
                                                       final long minBarForHighestTrackableValue) {
        return decodeFromByteBuffer(buffer, PackedHistogram.class,
                minBarForHighestTrackableValue);
    }

    /**
     * Construct a new histogram by decoding it from a compressed form in a ByteBuffer.
     *
     * @param buffer                         The buffer to decode from
     * @param minBarForHighestTrackableValue Force highestTrackableValue to be set at least this high
     * @return The newly constructed histogram
     * @throws DataFormatException on error parsing/decompressing the buffer
     */
    public static PackedHistogram decodeFromCompressedByteBuffer(final ByteBuffer buffer,
                                                                 final long minBarForHighestTrackableValue) throws DataFormatException {
        return decodeFromCompressedByteBuffer(buffer, PackedHistogram.class, minBarForHighestTrackableValue);
    }

    @Override
    public void addToCountAtIndex(final int index, final long value) {
        packedCounts.add(normalizeIndex(index, normalizingIndexOffset, countsArrayLength), value);
    }

    @Override
    public PackedHistogram copy() {
        PackedHistogram copy = new PackedHistogram(this);
        copy.add(this);
        return copy;
    }

    @Override
    public PackedHistogram copyCorrectedForCoordinatedOmission(final long expectedIntervalBetweenValueSamples) {
        PackedHistogram toHistogram = new PackedHistogram(this);
        toHistogram.addWhileCorrectingForCoordinatedOmission(this, expectedIntervalBetweenValueSamples);
        return toHistogram;
    }

    @Override
    public long getCountAtIndex(final int index) {
        return getCountAtNormalizedIndex(normalizeIndex(index, normalizingIndexOffset, countsArrayLength));
    }

    private void readObject(final ObjectInputStream o)
            throws IOException, ClassNotFoundException {
        o.defaultReadObject();
    }

    @Override
    int _getEstimatedFootprintInBytes() {
        return 192 + (8 * packedCounts.getPhysicalLength());
    }

    @Override
    void clearCounts() {
        packedCounts.clear();
        packedCounts.setVirtualLength(countsArrayLength);
        totalCount = 0;
    }

    @Override
    long getCountAtNormalizedIndex(final int index) {
        return packedCounts.get(index);
    }

    @Override
    void incrementCountAtIndex(final int index) {
        packedCounts.increment(normalizeIndex(index, normalizingIndexOffset, countsArrayLength));
    }

    @Override
    void resize(long newHighestTrackableValue) {
        int oldNormalizedZeroIndex = normalizeIndex(0, normalizingIndexOffset, countsArrayLength);
        int oldCountsArrayLength = countsArrayLength;

        establishSize(newHighestTrackableValue);

        if (oldNormalizedZeroIndex != 0) {
            // We need to shift the stuff from the zero index and up to the end of the array:

            // When things are shifted in a packed array its not simple to identify the region shifed,
            // so re-record everything from the old normalized indexes to the new normalized indexes:

            PackedLongArray newPackedCounts = new PackedLongArray(countsArrayLength, packedCounts.getPhysicalLength());
            // Copy everything up to the oldNormalizedZeroIndex in place:
            for (int fromIndex = 0; fromIndex < oldNormalizedZeroIndex; fromIndex++) {
                long value = packedCounts.get(fromIndex);
                if (value != 0) {
                    newPackedCounts.set(fromIndex, value);
                }
            }
            // Copy everything from the oldNormalizedZeroIndex to the end with an index delta shift:
            int countsDelta = countsArrayLength - oldCountsArrayLength;

            for (int fromIndex = oldNormalizedZeroIndex; fromIndex < oldCountsArrayLength; fromIndex++) {
                long value = packedCounts.get(fromIndex);
                if (value != 0) {
                    int toIndex = fromIndex + countsDelta;
                    newPackedCounts.set(toIndex, value);
                }
            }
            // All unrecorded values are implicitly zero in the packed array

            packedCounts = newPackedCounts;
        } else {
            packedCounts.setVirtualLength(countsArrayLength);
        }
    }

    @Override
    void setCountAtIndex(int index, long value) {
        setCountAtNormalizedIndex(normalizeIndex(index, normalizingIndexOffset, countsArrayLength), value);
    }

    @Override
    void setCountAtNormalizedIndex(int index, long value) {
        packedCounts.set(index, value);
    }
}
