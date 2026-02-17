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

import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;

/**
 * <h2>A floating point values High Dynamic Range (HDR) Histogram that uses a packed internal representation</h2>
 * <p>
 * It is important to note that {@link PackedDoubleHistogram} is not thread-safe, and does not support safe concurrent
 * recording by multiple threads.
 * <p>
 * {@link PackedDoubleHistogram} tracks value counts in a packed internal representation optimized
 * for typical histogram recoded values are sparse in the value range and tend to be incremented in small unit counts.
 * This packed representation tends to require significantly smaller amounts of stoarge when compared to unpacked
 * representations, but can incur additional recording cost due to resizing and repacking operations that may
 * occur as previously unrecorded values are encountered.
 * <p>
 * {@link PackedDoubleHistogram} supports the recording and analyzing sampled data value counts across a
 * configurable dynamic range of floating point (double) values, with configurable value precision within the range.
 * Dynamic range is expressed as a ratio between the highest and lowest non-zero values trackable within the histogram
 * at any given time. Value precision is expressed as the number of significant [decimal] digits in the value recording,
 * and provides control over value quantization behavior across the value range and the subsequent value resolution at
 * any given level.
 * <p>
 * Auto-ranging: Unlike integer value based histograms, the specific value range tracked by a {@link
 * PackedDoubleHistogram} is not specified upfront. Only the dynamic range of values that the histogram can cover is
 * (optionally) specified. E.g. When a {@link PackedDoubleHistogram} is created to track a dynamic range of
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
public class PackedDoubleHistogram extends DoubleHistogram {

    /**
     * Construct a new auto-resizing DoubleHistogram using a precision stated as a number of significant decimal
     * digits.
     *
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant decimal
     *                                       digits to which the histogram will maintain value resolution and
     *                                       separation. Must be a non-negative integer between 0 and 5.
     */
    public PackedDoubleHistogram(final int numberOfSignificantValueDigits) {
        this(2, numberOfSignificantValueDigits);
        setAutoResize(true);
    }

    /**
     * Construct a new DoubleHistogram with the specified dynamic range (provided in {@code highestToLowestValueRatio})
     * and using a precision stated as a number of significant decimal digits.
     *
     * @param highestToLowestValueRatio      specifies the dynamic range to use
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant decimal
     *                                       digits to which the histogram will maintain value resolution and
     *                                       separation. Must be a non-negative integer between 0 and 5.
     */
    public PackedDoubleHistogram(final long highestToLowestValueRatio, final int numberOfSignificantValueDigits) {
        this(highestToLowestValueRatio, numberOfSignificantValueDigits, PackedHistogram.class);
    }

    /**
     * Construct a {@link PackedDoubleHistogram} with the same range settings as a given source,
     * duplicating the source's start/end timestamps (but NOT it's contents)
     *
     * @param source The source histogram to duplicate
     */
    public PackedDoubleHistogram(final DoubleHistogram source) {
        super(source);
    }

    PackedDoubleHistogram(final long highestToLowestValueRatio,
                          final int numberOfSignificantValueDigits,
                          final Class<? extends AbstractHistogram> internalCountsHistogramClass) {
        super(highestToLowestValueRatio, numberOfSignificantValueDigits, internalCountsHistogramClass);
    }

    PackedDoubleHistogram(final long highestToLowestValueRatio,
                          final int numberOfSignificantValueDigits,
                          final Class<? extends AbstractHistogram> internalCountsHistogramClass,
                          AbstractHistogram internalCountsHistogram) {
        super(
                highestToLowestValueRatio,
                numberOfSignificantValueDigits,
                internalCountsHistogramClass,
                internalCountsHistogram
        );
    }

    /**
     * Construct a new PackedDoubleHistogram by decoding it from a ByteBuffer.
     *
     * @param buffer                             The buffer to decode from
     * @param minBarForHighestToLowestValueRatio Force highestTrackableValue to be set at least this high
     * @return The newly constructed PackedDoubleHistogram
     */
    public static PackedDoubleHistogram decodeFromByteBuffer(
            final ByteBuffer buffer,
            final long minBarForHighestToLowestValueRatio) {
        try {
            int cookie = buffer.getInt();
            if (!isNonCompressedDoubleHistogramCookie(cookie)) {
                throw new IllegalArgumentException("The buffer does not contain a DoubleHistogram");
            }
            return constructHistogramFromBuffer(cookie, buffer,
                    PackedDoubleHistogram.class, PackedHistogram.class,
                    minBarForHighestToLowestValueRatio);
        } catch (DataFormatException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Construct a new PackedDoubleHistogram by decoding it from a compressed form in a ByteBuffer.
     *
     * @param buffer                             The buffer to decode from
     * @param minBarForHighestToLowestValueRatio Force highestTrackableValue to be set at least this high
     * @return The newly constructed PackedDoubleHistogram
     * @throws DataFormatException on error parsing/decompressing the buffer
     */
    public static PackedDoubleHistogram decodeFromCompressedByteBuffer(
            final ByteBuffer buffer,
            final long minBarForHighestToLowestValueRatio) throws DataFormatException {
        int cookie = buffer.getInt();
        if (!isCompressedDoubleHistogramCookie(cookie)) {
            throw new IllegalArgumentException("The buffer does not contain a compressed DoubleHistogram");
        }
        return constructHistogramFromBuffer(cookie, buffer,
                PackedDoubleHistogram.class, PackedHistogram.class,
                minBarForHighestToLowestValueRatio);
    }
}
