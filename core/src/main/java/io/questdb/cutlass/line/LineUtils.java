package io.questdb.cutlass.line;

import io.questdb.cairo.TimestampDriver;
import io.questdb.cutlass.line.tcp.LineProtocolException;

public final class LineUtils {
    private LineUtils() {
    }

    public static long from(TimestampDriver driver, long ts, byte unit) {
        try {
            return driver.from(ts, unit);
        } catch (ArithmeticException e) {
            throw LineProtocolException.timestampValueOverflow(ts);
        }
    }
}
