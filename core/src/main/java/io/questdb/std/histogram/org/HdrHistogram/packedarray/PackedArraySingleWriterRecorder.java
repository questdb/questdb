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

package io.questdb.std.histogram.org.HdrHistogram.packedarray;

import io.questdb.cairo.CairoException;
import io.questdb.std.histogram.org.HdrHistogram.WriterReaderPhaser;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Records increments and adds of integer values at indexes of a logical array of 64 bit signed integer values, and
 * provides stable interval {@link PackedLongArray} samples from live recorded data without interrupting or stalling
 * active recording of values. Each interval array provided contains all values accumulated since the previous
 * interval array was taken.
 * <p>
 * This pattern is commonly used in logging interval accumulator information while recording is ongoing.
 * <p>
 * {@link PackedArraySingleWriterRecorder} expects only a single thread (the "single writer") to call
 * {@link PackedArraySingleWriterRecorder#increment(int)} or
 * {@link PackedArraySingleWriterRecorder#add(int, long)} at any point in time.
 * It DOES NOT safely support concurrent increment or add calls.
 * While the {@link #increment increment()} and {@link #add add()} methods are not quite wait-free, they
 * come "close" to that behvaior in the sense that a given thread will incur a total of no more than a capped
 * fixed number (e.g. 74 in a current implementation) of non-wait-free add or increment operations during
 * the lifetime of an interval array (including across recycling of that array across intervals within the
 * same recorder), regradless of the number of operations done.
 * <p>
 * A common pattern for using a {@link PackedArraySingleWriterRecorder} looks like this:
 * <br><pre><code>
 * PackedArraySingleWriterRecorder recorder = new PackedArraySingleWriterRecorder(); //
 * PackedLongArray intervalArray = null;
 * ...
 * [start of some loop construct that periodically wants to grab an interval array]
 *   ...
 *   // Get interval array, recycling previous interval array:
 *   intervalArray = recorder.getIntervalArray(intervalArray);
 *   // Process the interval array, which is nice and stable here:
 *   myLogWriter.logArrayContents(intervalArray);
 *   ...
 * [end of loop construct]
 * </code></pre>
 */

public class PackedArraySingleWriterRecorder {
    private static final AtomicLong instanceIdSequencer = new AtomicLong(1);
    private final long instanceId = instanceIdSequencer.getAndIncrement();

    private final WriterReaderPhaser recordingPhaser = new WriterReaderPhaser();

    private volatile PackedLongArray activeArray;

    /**
     * Construct a {@link PackedArraySingleWriterRecorder} with a given (virtual) array length.
     *
     * @param virtualLength The (virtual) array length
     */
    public PackedArraySingleWriterRecorder(final int virtualLength) {
        activeArray = new InternalPackedLongArray(instanceId, virtualLength);
        activeArray.setStartTimeStamp(System.currentTimeMillis());
    }

    /**
     * Construct a {@link PackedArraySingleWriterRecorder} with a given (virtual) array length, starting with a given
     * initial physical backing store length
     *
     * @param virtualLength         The (virtual) array length
     * @param initialPhysicalLength The initial physical backing store length
     */
    public PackedArraySingleWriterRecorder(final int virtualLength, final int initialPhysicalLength) {
        activeArray = new InternalPackedLongArray(instanceId, virtualLength, initialPhysicalLength);
        activeArray.setStartTimeStamp(System.currentTimeMillis());
    }

    /**
     * Add to a value at a given index in the array
     *
     * @param index      The index of value to add to
     * @param valueToAdd The amount to add to the value at the given index
     * @throws CairoException (may throw) if value exceeds length()
     */
    public void add(final int index, final long valueToAdd) throws CairoException {
        long criticalValueAtEnter = recordingPhaser.writerCriticalSectionEnter();
        try {
            activeArray.add(index, valueToAdd);
        } finally {
            recordingPhaser.writerCriticalSectionExit(criticalValueAtEnter);
        }
    }

    /**
     * Get an interval array, which will include a stable, consistent view of all values
     * accumulated since the last interval array was taken.
     * <p>
     * Calling this method is equivalent to calling {@code getIntervalArray(null)}. It is generally recommended
     * that the {@link PackedArraySingleWriterRecorder#getIntervalArray(PackedLongArray arrayToRecycle)
     * getIntervalHistogram(arrayToRecycle)} orm be used for
     * regular interval array sampling, as that form accepts a previously returned interval array that can be
     * recycled internally to avoid allocation and content copying operations, and is therefore significantly
     * more efficient for repeated use than {@link PackedArraySingleWriterRecorder#getIntervalArray()}.
     * <p>
     * Calling {@link PackedArraySingleWriterRecorder#getIntervalArray()} will reset the values at
     * all indexes of the array tracked by the recorder, and start accumulating values for the next interval.
     *
     * @return an array containing the values accumulated since the last interval array was taken.
     */
    public synchronized PackedLongArray getIntervalArray() {
        return getIntervalArray(null);
    }

    /**
     * Get an interval array, which will include a stable, consistent view of all values
     * accumulated since the last interval array was taken.
     * <p>
     * {@link PackedArraySingleWriterRecorder#getIntervalArray(PackedLongArray arrayToRecycle)
     * getIntervalArray(arrayToRecycle)}
     * accepts a previously returned interval array that can be recycled internally to avoid allocation
     * and content copying operations, and is therefore significantly more efficient for repeated use than
     * {@link PackedArraySingleWriterRecorder#getIntervalArray()}. The provided {@code arrayToRecycle} must
     * be either be null or an interval array returned by a previous call to
     * {@link PackedArraySingleWriterRecorder#getIntervalArray(PackedLongArray arrayToRecycle)
     * getIntervalArray(arrayToRecycle)} or
     * {@link PackedArraySingleWriterRecorder#getIntervalArray()}.
     * <p>
     * NOTE: The caller is responsible for not recycling the same returned interval array more than once. If
     * the same interval array instance is recycled more than once, behavior is undefined.
     * <p>
     * Calling {@link PackedArraySingleWriterRecorder#getIntervalArray(PackedLongArray arrayToRecycle)
     * getIntervalArray(arrayToRecycle)} will reset the values at all indexes of the array
     * tracked by the recorder, and start accumulating values for the next interval.
     *
     * @param arrayToRecycle a previously returned interval array (from this instance of
     *                       {@link PackedArraySingleWriterRecorder}) that may be recycled to avoid allocation and
     *                       copy operations.
     * @return an array containing the values accumulated since the last interval array was taken.
     */
    public synchronized PackedLongArray getIntervalArray(final PackedLongArray arrayToRecycle) {
        return getIntervalArray(arrayToRecycle, true);
    }

    /**
     * Get an interval array, which will include a stable, consistent view of all values
     * accumulated since the last interval array was taken.
     * <p>
     * {@link PackedArraySingleWriterRecorder#getIntervalArray(PackedLongArray arrayToRecycle)
     * getIntervalArray(arrayToRecycle)}
     * accepts a previously returned interval array that can be recycled internally to avoid allocation
     * and content copying operations, and is therefore significantly more efficient for repeated use than
     * {@link PackedArraySingleWriterRecorder#getIntervalArray()}. The provided {@code arrayToRecycle} must
     * be either be null or an interval array returned by a previous call to
     * {@link PackedArraySingleWriterRecorder#getIntervalArray(PackedLongArray arrayToRecycle)
     * getIntervalArray(arrayToRecycle)} or
     * {@link PackedArraySingleWriterRecorder#getIntervalArray()}.
     * <p>
     * NOTE: The caller is responsible for not recycling the same returned interval array more than once. If
     * the same interval array instance is recycled more than once, behavior is undefined.
     * <p>
     * Calling {@link PackedArraySingleWriterRecorder#getIntervalArray(PackedLongArray arrayToRecycle)
     * getIntervalArray(arrayToRecycle, enforeContainingInstance)} will reset the values at all indexes
     * of the array tracked by the recorder, and start accumulating values for the next interval.
     *
     * @param arrayToRecycle           a previously returned interval array that may be recycled to avoid allocation and
     *                                 copy operations.
     * @param enforeContainingInstance if true, will only allow recycling of arrays previously returned from this
     *                                 instance of {@link PackedArraySingleWriterRecorder}. If false, will allow recycling arrays
     *                                 previously returned by other instances of {@link PackedArraySingleWriterRecorder}.
     * @return an array containing the values accumulated since the last interval array was taken.
     */
    public synchronized PackedLongArray getIntervalArray(final PackedLongArray arrayToRecycle,
                                                         final boolean enforeContainingInstance) {
        // Verify that replacement array can validly be used as an inactive array replacement:
        validateFitAsReplacementArray(arrayToRecycle, enforeContainingInstance);
        return performIntervalSample(arrayToRecycle);
    }

    /**
     * Increment a value at a given index in the array
     *
     * @param index the index of trhe value to be incremented
     * @throws CairoException (may throw) if value exceeds length()
     */
    public void increment(final int index) throws CairoException {
        long criticalValueAtEnter = recordingPhaser.writerCriticalSectionEnter();
        try {
            activeArray.increment(index);
        } finally {
            recordingPhaser.writerCriticalSectionExit(criticalValueAtEnter);
        }
    }

    /**
     * Returns the virtual length of the array represented by this recorder
     *
     * @return The virtual length of the array represented by this recorder
     */
    public int length() {
        return activeArray.length();
    }

    /**
     * Reset the array contents to all zeros.
     */
    public synchronized void reset() {
        // the currently active array is reset each time we flip:
        performIntervalSample(null);
    }

    /**
     * Change the (virtual) length of the array represented by the this recorder
     *
     * @param newVirtualLength the new (virtual) length to use
     */
    public void setVirtualLength(int newVirtualLength) {
        try {
            recordingPhaser.readerLock();
            // We don't care about concurrent modifications to the array, as setVirtualLength() in the
            // ConcurrentPackedLongArray takes care of those. However, we must perform the change of virtual
            // length under the recorder's readerLock proptection to prevent mid-change observations:
            activeArray.setVirtualLength(newVirtualLength);
        } finally {
            recordingPhaser.readerUnlock();
        }
    }

    private PackedLongArray performIntervalSample(final PackedLongArray arrayToRecycle) {
        PackedLongArray inactiveArray = arrayToRecycle;
        try {
            recordingPhaser.readerLock();

            // Make sure we have an inactive version to flip in:
            if (inactiveArray == null) {
                if (activeArray instanceof InternalPackedLongArray) {
                    inactiveArray = new InternalPackedLongArray(instanceId, activeArray.length());
                } else {
                    throw new IllegalStateException("Unexpected internal array type for activeArray");
                }
            } else {
                inactiveArray.clear();
            }

            // Swap active and inactive arrays:
            final PackedLongArray tempArray = inactiveArray;
            inactiveArray = activeArray;
            activeArray = tempArray;

            // Mark end time of previous interval and start time of new one:
            long now = System.currentTimeMillis();
            activeArray.setStartTimeStamp(now);
            inactiveArray.setEndTimeStamp(now);

            // Make sure we are not in the middle of recording a value on the previously active array:

            // Flip phase to make sure no recordings that were in flight pre-flip are still active:
            recordingPhaser.flipPhase(500000L /* yield in 0.5 msec units if needed */);
        } finally {
            recordingPhaser.readerUnlock();
        }
        return inactiveArray;
    }

    private void validateFitAsReplacementArray(final PackedLongArray replacementArray,
                                               final boolean enforeContainingInstance) {
        boolean bad = true;
        if (replacementArray == null) {
            bad = false;
        } else if (replacementArray instanceof InternalPackedLongArray) {
            if ((activeArray instanceof InternalPackedLongArray)
                    &&
                    ((!enforeContainingInstance) ||
                            (((InternalPackedLongArray) replacementArray).containingInstanceId ==
                                    ((InternalPackedLongArray) activeArray).containingInstanceId)
                    )) {
                bad = false;
            }
        }
        if (bad) {
            throw new IllegalArgumentException("replacement array must have been obtained via a previous" +
                    " getIntervalArray() call from this " + this.getClass().getName() +
                    (enforeContainingInstance ? " insatnce" : " class"));
        }
    }

    private static class InternalPackedLongArray extends PackedLongArray {
        private final long containingInstanceId;

        private InternalPackedLongArray(final long id, int virtualLength, final int initialPhysicalLength) {
            super(virtualLength, initialPhysicalLength);
            this.containingInstanceId = id;
        }

        private InternalPackedLongArray(final long id, final int virtualLength) {
            super(virtualLength);
            this.containingInstanceId = id;
        }
    }
}
