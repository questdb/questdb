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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Records integer values, and provides stable interval {@link Histogram} samples from
 * live recorded data without interrupting or stalling active recording of values. Each interval
 * histogram provided contains all value counts accumulated since the previous interval histogram
 * was taken.
 * <p>
 * This pattern is commonly used in logging interval histogram information while recording is ongoing.
 * <p>
 * {@link SingleWriterRecorder} expects only a single thread (the "single writer") to
 * call {@link SingleWriterRecorder#recordValue} or
 * {@link SingleWriterRecorder#recordValueWithExpectedInterval} at any point in time.
 * It DOES NOT safely support concurrent recording calls.
 * Recording calls are wait-free on architectures that support atomic increment operations, and
 * re lock-free on architectures that do not.
 * * <p>
 * A common pattern for using a {@link SingleWriterRecorder} looks like this:
 * <br><pre><code>
 * SingleWriterRecorder recorder = new SingleWriterRecorder(2); // Two decimal point accuracy
 * Histogram intervalHistogram = null;
 * ...
 * [start of some loop construct that periodically wants to grab an interval histogram]
 *   ...
 *   // Get interval histogram, recycling previous interval histogram:
 *   intervalHistogram = recorder.getIntervalHistogram(intervalHistogram);
 *   histogramLogWriter.outputIntervalHistogram(intervalHistogram);
 *   ...
 * [end of loop construct]
 * </code></pre>
 */

public class SingleWriterRecorder implements ValueRecorder {
    private static final AtomicLong instanceIdSequencer = new AtomicLong(1);
    private final long instanceId = instanceIdSequencer.getAndIncrement();

    private final WriterReaderPhaser recordingPhaser = new WriterReaderPhaser();

    private volatile Histogram activeHistogram;
    private Histogram inactiveHistogram;

    /**
     * Construct an auto-resizing {@link SingleWriterRecorder} with a lowest discernible value of
     * 1 and an auto-adjusting highestTrackableValue. Can auto-resize up to track values up to (Long.MAX_VALUE / 2).
     * <p>
     * Depending on the valuer of the <b><code>packed</code></b> parameter {@link SingleWriterRecorder} can be configuired to
     * track value counts in a packed internal representation optimized for typical histogram recoded values are
     * sparse in the value range and tend to be incremented in small unit counts. This packed representation tends
     * to require significantly smaller amounts of stoarge when compared to unpacked representations, but can incur
     * additional recording cost due to resizing and repacking operations that may
     * occur as previously unrecorded values are encountered.
     *
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     * @param packed                         Specifies whether the recorder will uses a packed internal representation or not.
     */
    public SingleWriterRecorder(final int numberOfSignificantValueDigits, final boolean packed) {
        activeHistogram = packed ?
                new PackedInternalHistogram(instanceId, numberOfSignificantValueDigits) :
                new InternalHistogram(instanceId, numberOfSignificantValueDigits);
        inactiveHistogram = null;
        activeHistogram.setStartTimeStamp(System.currentTimeMillis());
    }

    /**
     * Construct an auto-resizing {@link SingleWriterRecorder} with a lowest discernible value of
     * 1 and an auto-adjusting highestTrackableValue. Can auto-resize up to track values up to (Long.MAX_VALUE / 2).
     *
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     */
    public SingleWriterRecorder(final int numberOfSignificantValueDigits) {
        this(numberOfSignificantValueDigits, false);
    }

    /**
     * Construct a {@link SingleWriterRecorder} given the highest value to be tracked and a number
     * of significant decimal digits. The histogram will be constructed to implicitly track (distinguish from 0)
     * values as low as 1.
     *
     * @param highestTrackableValue          The highest value to be tracked by the histogram. Must be a positive
     *                                       integer that is {@literal >=} 2.
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     */
    public SingleWriterRecorder(final long highestTrackableValue,
                                final int numberOfSignificantValueDigits) {
        this(1, highestTrackableValue, numberOfSignificantValueDigits);
    }

    /**
     * Construct a {@link SingleWriterRecorder} given the Lowest and highest values to be tracked
     * and a number of significant decimal digits. Providing a lowestDiscernibleValue is useful is situations where
     * the units used for the histogram's values are much smaller that the minimal accuracy required. E.g. when
     * tracking time values stated in nanosecond units, where the minimal accuracy required is a microsecond, the
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
    public SingleWriterRecorder(final long lowestDiscernibleValue,
                                final long highestTrackableValue,
                                final int numberOfSignificantValueDigits) {
        activeHistogram = new InternalHistogram(
                instanceId, lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
        inactiveHistogram = null;
        activeHistogram.setStartTimeStamp(System.currentTimeMillis());
    }

    /**
     * Get a new instance of an interval histogram, which will include a stable, consistent view of all value
     * counts accumulated since the last interval histogram was taken.
     * <p>
     * Calling {@code getIntervalHistogram()} will reset
     * the value counts, and start accumulating value counts for the next interval.
     *
     * @return a histogram containing the value counts accumulated since the last interval histogram was taken.
     */
    public synchronized Histogram getIntervalHistogram() {
        return getIntervalHistogram(null);
    }

    /**
     * Get an interval histogram, which will include a stable, consistent view of all value counts
     * accumulated since the last interval histogram was taken.
     * <p>
     * {@code getIntervalHistogram(histogramToRecycle)}
     * accepts a previously returned interval histogram that can be recycled internally to avoid allocation
     * and content copying operations, and is therefore significantly more efficient for repeated use than
     * {@link SingleWriterRecorder#getIntervalHistogram()} and
     * {@link SingleWriterRecorder#getIntervalHistogramInto getIntervalHistogramInto()}. The provided
     * {@code histogramToRecycle} must
     * be either be null or an interval histogram returned by a previous call to
     * {@code getIntervalHistogram(histogramToRecycle)} or
     * {@link SingleWriterRecorder#getIntervalHistogram()}.
     * <p>
     * NOTE: The caller is responsible for not recycling the same returned interval histogram more than once. If
     * the same interval histogram instance is recycled more than once, behavior is undefined.
     * <p>
     * Calling {@code getIntervalHistogram(histogramToRecycle)} will reset the value counts, and start
     * accumulating value counts for the next interval
     *
     * @param histogramToRecycle a previously returned interval histogram (from this instance of
     *                           {@link SingleWriterRecorder}) that may be recycled to avoid allocation and
     *                           copy operations.
     * @return a histogram containing the value counts accumulated since the last interval histogram was taken.
     */
    public synchronized Histogram getIntervalHistogram(Histogram histogramToRecycle) {
        return getIntervalHistogram(histogramToRecycle, true);
    }

    /**
     * Get an interval histogram, which will include a stable, consistent view of all value counts
     * accumulated since the last interval histogram was taken.
     * <p>
     * {@link SingleWriterRecorder#getIntervalHistogram(Histogram histogramToRecycle)
     * getIntervalHistogram(histogramToRecycle)}
     * accepts a previously returned interval histogram that can be recycled internally to avoid allocation
     * and content copying operations, and is therefore significantly more efficient for repeated use than
     * {@link SingleWriterRecorder#getIntervalHistogram()} and
     * {@link SingleWriterRecorder#getIntervalHistogramInto getIntervalHistogramInto()}. The provided
     * {@code histogramToRecycle} must
     * be either be null or an interval histogram returned by a previous call to
     * {@link SingleWriterRecorder#getIntervalHistogram(Histogram histogramToRecycle)
     * getIntervalHistogram(histogramToRecycle)} or
     * {@link SingleWriterRecorder#getIntervalHistogram()}.
     * <p>
     * NOTE: The caller is responsible for not recycling the same returned interval histogram more than once. If
     * the same interval histogram instance is recycled more than once, behavior is undefined.
     * <p>
     * Calling {@link SingleWriterRecorder#getIntervalHistogram(Histogram histogramToRecycle)
     * getIntervalHistogram(histogramToRecycle)} will reset the value counts, and start accumulating value
     * counts for the next interval
     *
     * @param histogramToRecycle       a previously returned interval histogram that may be recycled to avoid allocation and
     *                                 copy operations.
     * @param enforeContainingInstance if true, will only allow recycling of histograms previously returned from this
     *                                 instance of {@link SingleWriterRecorder}. If false, will allow recycling histograms
     *                                 previously returned by other instances of {@link SingleWriterRecorder}.
     * @return a histogram containing the value counts accumulated since the last interval histogram was taken.
     */
    public synchronized Histogram getIntervalHistogram(Histogram histogramToRecycle,
                                                       boolean enforeContainingInstance) {
        // Verify that replacement histogram can validly be used as an inactive histogram replacement:
        validateFitAsReplacementHistogram(histogramToRecycle, enforeContainingInstance);
        inactiveHistogram = histogramToRecycle;
        performIntervalSample();
        Histogram sampledHistogram = inactiveHistogram;
        inactiveHistogram = null; // Once we expose the sample, we can't reuse it internally until it is recycled
        return sampledHistogram;
    }

    /**
     * Place a copy of the value counts accumulated since accumulated (since the last interval histogram
     * was taken) into {@code targetHistogram}.
     * <p>
     * Calling {@code getIntervalHistogramInto(targetHistogram)} will reset
     * the value counts, and start accumulating value counts for the next interval.
     *
     * @param targetHistogram the histogram into which the interval histogram's data should be copied
     */
    public synchronized void getIntervalHistogramInto(Histogram targetHistogram) {
        performIntervalSample();
        inactiveHistogram.copyInto(targetHistogram);
    }

    /**
     * Record a value
     *
     * @param value the value to record
     * @throws CairoException (may throw) if value exceeds highestTrackableValue
     */
    @Override
    public void recordValue(final long value) {
        long criticalValueAtEnter = recordingPhaser.writerCriticalSectionEnter();
        try {
            activeHistogram.recordValue(value);
        } finally {
            recordingPhaser.writerCriticalSectionExit(criticalValueAtEnter);
        }
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
        long criticalValueAtEnter = recordingPhaser.writerCriticalSectionEnter();
        try {
            activeHistogram.recordValueWithCount(value, count);
        } finally {
            recordingPhaser.writerCriticalSectionExit(criticalValueAtEnter);
        }
    }

    /**
     * Record a value
     * <p>
     * To compensate for the loss of sampled values when a recorded value is larger than the expected
     * interval between value samples, Histogram will auto-generate an additional series of decreasingly-smaller
     * (down to the expectedIntervalBetweenValueSamples) value records.
     * <p>
     * See related notes {@link AbstractHistogram#recordValueWithExpectedInterval(long, long)}
     * for more explanations about coordinated omission and expected interval correction.
     * *
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
        long criticalValueAtEnter = recordingPhaser.writerCriticalSectionEnter();
        try {
            activeHistogram.recordValueWithExpectedInterval(value, expectedIntervalBetweenValueSamples);
        } finally {
            recordingPhaser.writerCriticalSectionExit(criticalValueAtEnter);
        }
    }

    /**
     * Reset any value counts accumulated thus far.
     */
    @Override
    public synchronized void reset() {
        // the currently inactive histogram is reset each time we flip. So flipping twice resets both:
        performIntervalSample();
        performIntervalSample();
    }

    private void performIntervalSample() {
        try {
            recordingPhaser.readerLock();

            // Make sure we have an inactive version to flip in:
            if (inactiveHistogram == null) {
                if (activeHistogram instanceof InternalHistogram) {
                    inactiveHistogram = new InternalHistogram((InternalHistogram) activeHistogram);
                } else if (activeHistogram instanceof PackedInternalHistogram) {
                    inactiveHistogram = new PackedInternalHistogram(
                            instanceId, activeHistogram.getNumberOfSignificantValueDigits());
                } else {
                    throw new IllegalStateException("Unexpected internal histogram type for activeHistogram");
                }
            }

            inactiveHistogram.reset();

            // Swap active and inactive histograms:
            final Histogram tempHistogram = inactiveHistogram;
            inactiveHistogram = activeHistogram;
            activeHistogram = tempHistogram;

            // Mark end time of previous interval and start time of new one:
            long now = System.currentTimeMillis();
            activeHistogram.setStartTimeStamp(now);
            inactiveHistogram.setEndTimeStamp(now);

            // Make sure we are not in the middle of recording a value on the previously active histogram:

            // Flip phase to make sure no recordings that were in flight pre-flip are still active:
            recordingPhaser.flipPhase(500000L /* yield in 0.5 msec units if needed */);
        } finally {
            recordingPhaser.readerUnlock();
        }
    }

    private void validateFitAsReplacementHistogram(Histogram replacementHistogram,
                                                   boolean enforeContainingInstance) {
        boolean bad = true;
        if (replacementHistogram == null) {
            bad = false;
        } else if ((replacementHistogram instanceof InternalHistogram)
                &&
                ((!enforeContainingInstance) ||
                        (((InternalHistogram) replacementHistogram).containingInstanceId ==
                                ((InternalHistogram) activeHistogram).containingInstanceId)
                )) {
            bad = false;
        } else if ((replacementHistogram instanceof PackedInternalHistogram)
                &&
                ((!enforeContainingInstance) ||
                        (((PackedInternalHistogram) replacementHistogram).containingInstanceId ==
                                ((PackedInternalHistogram) activeHistogram).containingInstanceId)
                )) {
            bad = false;
        }

        if (bad) {
            throw new IllegalArgumentException("replacement histogram must have been obtained via a previous " +
                    "getIntervalHistogram() call from this " + this.getClass().getName() +
                    (enforeContainingInstance ? " insatnce" : " class"));
        }
    }

    private static class InternalHistogram extends Histogram {
        private final long containingInstanceId;

        private InternalHistogram(long id, int numberOfSignificantValueDigits) {
            super(numberOfSignificantValueDigits);
            this.containingInstanceId = id;
        }

        private InternalHistogram(long id,
                                  long lowestDiscernibleValue,
                                  long highestTrackableValue,
                                  int numberOfSignificantValueDigits) {
            super(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
            this.containingInstanceId = id;
        }

        private InternalHistogram(InternalHistogram source) {
            super(source);
            this.containingInstanceId = source.containingInstanceId;
        }
    }

    private static class PackedInternalHistogram extends PackedHistogram {
        private final long containingInstanceId;

        private PackedInternalHistogram(long id, int numberOfSignificantValueDigits) {
            super(numberOfSignificantValueDigits);
            this.containingInstanceId = id;
        }
    }
}
