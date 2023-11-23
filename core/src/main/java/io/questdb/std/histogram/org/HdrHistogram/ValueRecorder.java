package io.questdb.std.histogram.org.HdrHistogram;

import io.questdb.cairo.CairoException;

public interface ValueRecorder {

    /**
     * Record a value
     *
     * @param value The value to be recorded
     * @throws CairoException (may throw) if value cannot be covered by the histogram's range
     */
    void recordValue(long value) throws CairoException;

    /**
     * Record a value (adding to the value's current count)
     *
     * @param value The value to be recorded
     * @param count The number of occurrences of this value to record
     * @throws CairoException (may throw) if value cannot be covered by the histogram's range
     */
    void recordValueWithCount(long value, long count) throws CairoException;

    /**
     * Record a value.
     * <p>
     * To compensate for the loss of sampled values when a recorded value is larger than the expected
     * interval between value samples, will auto-generate an additional series of decreasingly-smaller
     * (down to the expectedIntervalBetweenValueSamples) value records.
     * <p>
     * Note: This is a at-recording correction method, as opposed to the post-recording correction method provided
     * by {@link AbstractHistogram#copyCorrectedForCoordinatedOmission(long)}.
     * The two methods are mutually exclusive, and only one of the two should be be used on a given data set to correct
     * for the same coordinated omission issue.
     *
     * @param value                               The value to record
     * @param expectedIntervalBetweenValueSamples If expectedIntervalBetweenValueSamples is larger than 0, add
     *                                            auto-generated value records as appropriate if value is larger
     *                                            than expectedIntervalBetweenValueSamples
     * @throws CairoException (may throw) if value cannot be covered by the histogram's range
     */
    void recordValueWithExpectedInterval(long value, long expectedIntervalBetweenValueSamples)
            throws CairoException;

    /**
     * Reset the contents and collected stats
     */
    void reset();
}
