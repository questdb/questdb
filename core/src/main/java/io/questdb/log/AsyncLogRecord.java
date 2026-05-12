/*+*****************************************************************************
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

package io.questdb.log;

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TimestampDriver;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.network.Net;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjHashSet;
import io.questdb.std.datetime.Clock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.Set;

import static io.questdb.ParanoiaState.*;

/**
 * Per-carrier {@link LogRecord} builder. One instance lives in
 * {@link AbstractLogRecord#tl} per carrier; {@code AbstractLogRecord.prepareLogRecord}
 * performs a single {@link io.questdb.std.CarrierLocal#get()} (one FFI downcall via
 * {@link io.questdb.mp.CarrierIdentity#current()}) at the start of a chain, then all
 * {@code $(...)}/{@code $()} calls read this record's plain fields. The previous
 * design routed every chain segment through {@code tl.get().sink}, paying the FFI
 * cost ~6 times per chain.
 * <p>
 * Safety vs. the C2 hoist hazard documented in
 * {@code mp/continuation/CARRIER_LOCAL.md}: a log chain has no continuation
 * suspend point - {@code Sequence.next()} spins, sink writes are plain memory,
 * {@code seq.done()} publishes - so the carrier captured at the start of the
 * chain is the carrier executing the whole chain. One {@code tl.get()} at the
 * start is sufficient; subsequent reads of this record are field accesses.
 */
final class AsyncLogRecord implements LogRecord {
    final LogError abandonedLogRecordError;
    private final Clock clock;
    private final ObjHashSet<Throwable> dejaVu = new ObjHashSet<>();
    long cursor;
    boolean isLogRecordInProgress;
    RingQueue<LogRecordUtf8Sink> ring;
    Sequence seq;
    LogRecordUtf8Sink sink;

    AsyncLogRecord(Clock clock, CharSequence name) {
        this.clock = clock;
        this.abandonedLogRecordError = createAbandonedLogError();
    }

    @Override
    public LogRecord $(int x) {
        sink.put(x);
        return this;
    }

    @Override
    public LogRecord $(double x) {
        sink.put(x);
        return this;
    }

    @Override
    public LogRecord $(@Nullable Utf8Sequence sequence) {
        if (sequence == null) {
            sink.putAscii("null");
        } else {
            sink.put(sequence);
        }
        return this;
    }

    @Override
    public LogRecord $(@Nullable DirectUtf8Sequence sequence) {
        if (sequence == null) {
            sink.putAscii("null");
        } else {
            sink.put(sequence);
        }
        return this;
    }

    @Override
    public LogRecord $(@Nullable File x) {
        sink.put(x == null ? "null" : x.getAbsolutePath());
        return this;
    }

    @Override
    public LogRecord $(@Nullable CharSequence sequence) {
        if (sequence == null) {
            sink.putAscii("null");
        } else {
            sink.putAscii(sequence);
        }
        return this;
    }

    @Override
    public LogRecord $(@Nullable Object x) {
        if (x == null) {
            sink.putAscii("null");
        } else {
            try {
                sink.put(x.toString());
            } catch (Throwable t) {
                // Complex toString() method could throw e.g. NullPointerException.
                // If that happens, release the cursor to prevent blocking log queue.
                $();
                throw t;
            }
        }
        return this;
    }

    @Override
    public LogRecord $(@Nullable Sinkable x) {
        if (x == null) {
            sink.putAscii("null");
        } else {
            try {
                x.toSink(sink);
            } catch (Throwable t) {
                // Complex toSink() method could throw e.g. NullPointerException.
                // If that happens, release the cursor to prevent blocking log queue.
                $();
                throw t;
            }
        }
        return this;
    }

    @Override
    public LogRecord $(long l) {
        sink.put(l);
        return this;
    }

    @Override
    public LogRecord $(boolean x) {
        sink.put(x);
        return this;
    }

    @Override
    public LogRecord $(char c) {
        sink.put(c);
        return this;
    }

    @Override
    public LogRecord $(@Nullable Throwable e) {
        if (e == null) {
            return this;
        }

        final LogRecordUtf8Sink s = sink;
        dejaVu.add(e);
        // Do not log EOL before exception type and message for log alerting to have more context.
        put0(s, e);
        s.putEOL();

        StackTraceElement[] trace = e.getStackTrace();
        for (int i = 0, n = trace.length; i < n; i++) {
            put(s, trace[i]);
        }

        // Print suppressed exceptions, if any
        Throwable[] suppressed = e.getSuppressed();
        for (int i = 0, n = suppressed.length; i < n; i++) {
            put(s, suppressed[i], trace, "Suppressed: ", "\t", dejaVu);
        }

        // Print cause, if any
        Throwable ourCause = e.getCause();
        if (ourCause != null) {
            put(s, ourCause, trace, "Caused by: ", "", dejaVu);
        }

        return this;
    }

    @Override
    public void $() {
        sink.putEOL();
        try {
            if (LOG_PARANOIA_MODE != LOG_PARANOIA_MODE_NONE) {
                validateUtf8(sink);
            }
        } finally {
            isLogRecordInProgress = false;
            seq.done(cursor);
        }
    }

    @Override
    public LogRecord $256(long a, long b, long c, long d) {
        Numbers.appendLong256(a, b, c, d, sink);
        return this;
    }

    @Override
    public LogRecord $hex(long value) {
        Numbers.appendHex(sink, value, false);
        return this;
    }

    @Override
    public LogRecord $hexPadded(long value) {
        Numbers.appendHex(sink, value, true);
        return this;
    }

    @Override
    public LogRecord $ip(long ip) {
        Net.appendIP4(sink, ip);
        return this;
    }

    @Override
    public LogRecord $safe(@Nullable DirectUtf8Sequence sequence) {
        if (sequence == null) {
            sink.putAscii("null");
        } else {
            Utf8s.putSafe(sequence.lo(), sequence.hi(), sink);
        }
        return this;
    }

    @Override
    public LogRecord $safe(@NotNull CharSequence sequence, int lo, int hi) {
        sink.put(sequence, lo, hi);
        return this;
    }

    @Override
    public LogRecord $safe(@Nullable Utf8Sequence sequence) {
        if (sequence == null) {
            sink.putAscii("null");
        } else {
            Utf8s.putSafe(sequence, sink);
        }
        return this;
    }

    @Override
    public LogRecord $safe(long lo, long hi) {
        Utf8s.putSafe(lo, hi, sink);
        return this;
    }

    @Override
    public LogRecord $safe(@Nullable CharSequence sequence) {
        if (sequence == null) {
            sink.putAscii("null");
        } else {
            sink.put(sequence);
        }
        return this;
    }

    @Override
    public LogRecord $size(long memoryBytes) {
        sink.putSize(memoryBytes);
        return this;
    }

    @Override
    public LogRecord $substr(int from, @Nullable DirectUtf8Sequence sequence) {
        if (sequence == null) {
            sink.putAscii("null");
        } else {
            if (from > -1 && sequence.size() > from) {
                sink.putNonAscii(sequence.lo() + from, sequence.hi());
            } else {
                sink
                        .put("WTF? substr? [from:").put(from)
                        .put(", sequence=").put(sequence)
                        .put(", size=").put(sequence.size())
                        .put(']');
            }
        }
        return this;
    }

    @Override
    public LogRecord $ts(long x) {
        sink.putISODate(x);
        return this;
    }

    @Override
    public LogRecord $ts(TimestampDriver driver, long x) {
        sink.putISODate(driver, x);
        return this;
    }

    @Override
    public LogRecord $uuid(long lo, long hi) {
        Numbers.appendUuid(lo, hi, this);
        return this;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public LogRecord microTime(long x) {
        MicrosTimestampDriver.INSTANCE.append(sink, x);
        return this;
    }

    @Override
    public LogRecord put(char c) {
        sink.put(c);
        return this;
    }

    @Override
    public LogRecord put(byte b) {
        sink.put(b);
        return this;
    }

    @Override
    public Utf8Sink put(@Nullable Utf8Sequence us) {
        sink.put(us);
        return this;
    }

    @Override
    public Utf8Sink putNonAscii(long lo, long hi) {
        sink.putNonAscii(lo, hi);
        return this;
    }

    @Override
    public LogRecord ts() {
        final long us = clock.getTicks();
        if (LogLevel.TIMESTAMP_TIMEZONE_RULES != null) {
            LogLevel.TIMESTAMP_FORMAT.format(
                    LogLevel.TIMESTAMP_TIMEZONE_RULES.getOffset(us) + us,
                    LogLevel.TIMESTAMP_TIMEZONE_LOCALE,
                    LogLevel.TIMESTAMP_TIMEZONE,
                    sink
            );
        } else {
            sink.putISODate(us);
        }

        return this;
    }

    /**
     * Returns the previously-installed abandoned log record if the prior chain
     * never reached {@code $()}, or {@code null} if the record is in a clean
     * state. Mirrors {@link AbstractLogRecord}'s former private helper but
     * operates on this carrier's record fields, avoiding a second {@code tl.get()}.
     */
    @Nullable LogError detectAbandonedLogRecord() throws LogError {
        if (!isLogRecordInProgress) {
            isLogRecordInProgress = true;
            abandonedLogRecordError.fillInStackTrace();
            return null;
        }
        $(" #$#$ ABANDONED LOG RECORD #$#$").$();
        return abandonedLogRecordError;
    }

    private static @NotNull LogError createAbandonedLogError() {
        if (LOG_PARANOIA_MODE == LOG_PARANOIA_MODE_AGGRESSIVE) {
            return new LogError("Abandoned log record");
        } else {
            return new LogError("Abandoned log record detected. Use LOG_PARANOIA_MODE_AGGRESSIVE to diagnose.",
                    false);
        }
    }

    private static void put(
            Utf8Sink sink,
            Throwable throwable,
            StackTraceElement[] enclosingTrace,
            String caption,
            String prefix,
            Set<Throwable> dejaVu
    ) {
        if (dejaVu.contains(throwable)) {
            sink.putAscii("\t[CIRCULAR REFERENCE:");
            put0(sink, throwable);
            sink.putAscii(']');
        } else {
            dejaVu.add(throwable);

            // Compute number of frames in common between this and enclosing trace
            StackTraceElement[] trace = throwable.getStackTrace();
            int m = trace.length - 1;
            int n = enclosingTrace.length - 1;
            while (m >= 0 && n >= 0 && trace[m].equals(enclosingTrace[n])) {
                m--;
                n--;
            }
            int framesInCommon = trace.length - 1 - m;

            sink.put(prefix).put(caption);
            put0(sink, throwable);
            sink.putEOL();

            for (int i = 0; i <= m; i++) {
                sink.put(prefix);
                put(sink, trace[i]);
            }
            if (framesInCommon != 0) {
                sink.put(prefix).putAscii("\t...").put(framesInCommon).putAscii(" more");
            }

            // Print suppressed exceptions, if any
            Throwable[] suppressed = throwable.getSuppressed();
            for (int i = 0, k = suppressed.length; i < k; i++) {
                put(sink, suppressed[i], trace, "Suppressed: ", prefix + '\t', dejaVu);
            }

            // Print cause, if any
            Throwable cause = throwable.getCause();
            if (cause != null) {
                put(sink, cause, trace, "Caused by: ", prefix, dejaVu);
            }
        }
    }

    private static void put(Utf8Sink sink, StackTraceElement e) {
        sink.putAscii("\tat ");
        sink.putAscii(e.getClassName());
        sink.putAscii('.');
        sink.putAscii(e.getMethodName());
        if (e.isNativeMethod()) {
            sink.putAscii("(Native Method)");
        } else {
            if (e.getFileName() != null && e.getLineNumber() > -1) {
                sink.putAscii('(').put(e.getFileName()).putAscii(':').put(e.getLineNumber()).putAscii(')');
            } else if (e.getFileName() != null) {
                sink.putAscii('(').put(e.getFileName()).putAscii(')');
            } else {
                sink.putAscii("(Unknown Source)");
            }
        }
        sink.put(Misc.EOL);
    }

    private static void put0(Utf8Sink sink, Throwable e) {
        sink.putAscii(e.getClass().getName());
        if (e.getMessage() != null) {
            sink.putAscii(": ").put(e.getMessage());
        }
    }

    private static void validateUtf8(LogRecordUtf8Sink sink) {
        if (Utf8s.validateUtf8(sink) < 0) {
            LogError e = new LogError("Invalid UTF-8, partial message: \n"
                    + Utf8s.stringFromUtf8BytesSafe(sink) + "\nEND partial message");
            sink.clear();
            e.printStackTrace(System.out);
            throw e;
        }
    }

    /**
     * Returns this record's {@code dejaVu} set. Used by {@code $(Throwable)} to
     * accumulate already-printed exception identities across nested cause/suppressed
     * traversal. The set is cleared by the writer thread when it consumes the entry,
     * NOT here - callers reuse it across chains and rely on identity inequality
     * across messages.
     */
    @SuppressWarnings("unused")
    ObjHashSet<Throwable> getDejaVu() {
        return dejaVu;
    }
}
