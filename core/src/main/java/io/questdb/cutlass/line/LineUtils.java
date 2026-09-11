package io.questdb.cutlass.line;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cutlass.line.tcp.LineProtocolException;

public final class LineUtils {
    private LineUtils() {
    }

    /**
     * Converts a line protocol timestamp to the precision of the target timestamp column. Every ILP
     * timestamp conversion must funnel through this method: {@link TimestampDriver#from(long, byte)}
     * signals a value the column cannot hold with an {@link ArithmeticException} and a unit it does
     * not support with an {@link UnsupportedOperationException}, and neither classifies as a
     * per-message rejection. The ILP callers cannot tell either apart from an infrastructure
     * failure, so they escalate to closing the table writer, wedging every other producer writing
     * to the same table. A {@link LineProtocolException} instead rejects just the offending message
     * and leaves the writer alone.
     *
     * @param driver timestamp driver of the target timestamp column
     * @param ts     timestamp, in the units the producer sent
     * @param unit   units of {@code ts}
     * @return the timestamp converted to the column's precision
     */
    public static long from(TimestampDriver driver, long ts, byte unit) {
        try {
            return driver.from(ts, unit);
        } catch (ArithmeticException e) {
            throw LineProtocolException.timestampValueOverflow(ts);
        } catch (UnsupportedOperationException e) {
            // TIMESTAMP_UNIT_UNSET lands here: a producer that sends the value as a plain integer
            // field leaves the unit unset, and so does a binary entity carrying an unknown unit byte
            throw LineProtocolException.unsupportedTimestampUnit(unit);
        }
    }

    /**
     * Converts a line protocol designated timestamp to the precision of the designated timestamp
     * column and checks it against the bounds the storage layer enforces. Every ILP entry point
     * must funnel the designated timestamp through this method: {@code TableWriter}/{@code WalWriter}
     * reject an out-of-range value with a plain {@link CairoException}, which the ILP callers
     * cannot tell apart from an infrastructure failure and therefore escalate to closing the table
     * writer. A {@link LineProtocolException} instead classifies as a per-message rejection and
     * leaves the writer alone.
     * <p>
     * The ceiling is the column's own: {@link TimestampDriver#validateBounds(long)} caps a
     * micros designated timestamp at 9999-12-31 and a nanos one at 2261-12-31, and the writer
     * applies the same check, so nothing this method accepts is refused downstream.
     *
     * @param driver         timestamp driver of the designated timestamp column
     * @param ts             designated timestamp, in the units the producer sent
     * @param unit           units of {@code ts}
     * @param tableNameUtf16 table name, for the error message
     * @return the timestamp converted to the column's precision
     */
    public static long fromDesignatedTimestamp(TimestampDriver driver, long ts, byte unit, String tableNameUtf16) {
        if (ts < 0) {
            // Numbers.LONG_NULL is negative, so this rejects a NULL designated timestamp too
            throw LineProtocolException.designatedTimestampMustBePositive(tableNameUtf16, ts);
        }
        final long timestamp = from(driver, ts, unit);
        try {
            driver.validateBounds(timestamp);
        } catch (CairoException e) {
            throw LineProtocolException.designatedTimestampOutOfBounds(tableNameUtf16, timestamp, e.getFlyweightMessage());
        }
        return timestamp;
    }
}
