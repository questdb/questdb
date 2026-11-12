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
import io.questdb.mp.CarrierIdentity;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.network.Net;
import io.questdb.std.CarrierLocal;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Os;
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
 * Per-carrier {@link LogRecord} builder, shared by all loggers. A chain starts in
 * {@link #begin}, which {@code AbstractLogRecord} reaches through a single
 * {@link CarrierLocal#get()} (one FFI downcall via {@link CarrierIdentity#current()});
 * all subsequent {@code $(...)}/{@code $()} calls read this record's plain fields.
 * <p>
 * The chain formats its message into the carrier's on-heap staging
 * {@link #sink} and does not touch the log ring until {@code $()}. There, it
 * claims a ring slot, copies the staged bytes into it and publishes it right
 * away. Nothing between claiming and publishing can throw, so a chain that
 * fails or gets abandoned half-way loses at most its own message and can never
 * leave a claimed-but-unpublished slot behind to wedge the log queue.
 * <p>
 * Safety vs. the C2 hoist hazard documented in
 * {@code mp/continuation/CARRIER_LOCAL.md}: a log chain has no continuation
 * suspend point - sink writes are plain memory, {@code Sequence.next()} spins,
 * {@code seq.done()} publishes - so the carrier captured at the start of the
 * chain is the carrier executing the whole chain.
 */
final class AsyncLogRecord implements LogRecord {
    // no initial value: forCarrier() sizes the record for the first destination ring
    private static final CarrierLocal<AsyncLogRecord> CARRIER_RECORD = new CarrierLocal<>();
    final LogError abandonedLogRecordError;
    private final ObjHashSet<Throwable> dejaVu = new ObjHashSet<>();
    boolean isLogRecordInProgress;
    // non-final so that tests can inject a failing sink
    HeapLogRecordUtf8Sink sink;
    private Clock clock;
    private boolean isGuaranteed;
    private int level;
    private RingQueue<LogRecordUtf8Sink> ring;
    private int[] ryuE10;
    private Sequence seq;

    private AsyncLogRecord(int capacity) {
        this.abandonedLogRecordError = createAbandonedLogError();
        this.sink = new HeapLogRecordUtf8Sink(capacity);
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
            try {
                sink.put(sequence);
            } catch (Throwable t) {
                releaseOnFailure(t);
                throw t;
            }
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
        try {
            // getAbsolutePath() allocates
            sink.put(x == null ? "null" : x.getAbsolutePath());
        } catch (Throwable t) {
            releaseOnFailure(t);
            throw t;
        }
        return this;
    }

    @Override
    public LogRecord $(@Nullable CharSequence sequence) {
        if (sequence == null) {
            sink.putAscii("null");
        } else {
            try {
                sink.putAscii(sequence);
            } catch (Throwable t) {
                // Arbitrary CharSequence implementation could throw.
                // If that happens, publish the partial message.
                releaseOnFailure(t);
                throw t;
            }
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
                // If that happens, publish the partial message.
                releaseOnFailure(t);
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
                // If that happens, publish the partial message.
                releaseOnFailure(t);
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

        try {
            final HeapLogRecordUtf8Sink s = sink;
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
        } catch (Throwable t) {
            releaseOnFailure(t);
            throw t;
        }

        return this;
    }

    @Override
    public void $() {
        if (!isLogRecordInProgress) {
            return;
        }
        try {
            sink.putEOL();
            if (LOG_PARANOIA_MODE != LOG_PARANOIA_MODE_NONE) {
                validateUtf8(sink);
            }
            publish(isGuaranteed);
        } finally {
            reset();
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
        try {
            sink.put(sequence, lo, hi);
        } catch (Throwable t) {
            releaseOnFailure(t);
            throw t;
        }
        return this;
    }

    @Override
    public LogRecord $safe(@Nullable Utf8Sequence sequence) {
        if (sequence == null) {
            sink.putAscii("null");
        } else {
            try {
                Utf8s.putSafe(sequence, sink);
            } catch (Throwable t) {
                releaseOnFailure(t);
                throw t;
            }
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
            try {
                sink.put(sequence);
            } catch (Throwable t) {
                releaseOnFailure(t);
                throw t;
            }
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
            try {
                if (from > -1 && sequence.size() > from) {
                    sink.putNonAscii(sequence.lo() + from, sequence.hi());
                } else {
                    sink
                            .put("WTF? substr? [from:").put(from)
                            .put(", sequence=").put(sequence)
                            .put(", size=").put(sequence.size())
                            .put(']');
                }
            } catch (Throwable t) {
                releaseOnFailure(t);
                throw t;
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
        try {
            sink.putISODate(driver, x);
        } catch (Throwable t) {
            releaseOnFailure(t);
            throw t;
        }
        return this;
    }

    @Override
    public LogRecord $uuid(long lo, long hi) {
        Numbers.appendUuid(lo, hi, this);
        return this;
    }

    @Override
    public void I$() {
        if (isLogRecordInProgress) {
            $(']').$();
        }
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
        try {
            sink.put(us);
        } catch (Throwable t) {
            releaseOnFailure(t);
            throw t;
        }
        return this;
    }

    @Override
    public Utf8Sink putNonAscii(long lo, long hi) {
        sink.putNonAscii(lo, hi);
        return this;
    }

    @Override
    public int[] ryuScratch() {
        if (ryuE10 == null) {
            ryuE10 = new int[1];
        }
        return ryuE10;
    }

    @Override
    public LogRecord ts() {
        try {
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
        } catch (Throwable t) {
            releaseOnFailure(t);
            throw t;
        }
        return this;
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

    // Handles a chain that started while the previous one on this carrier never
    // reached $(). The previous chain was either abandoned (e.g. an exception
    // skipped its $()) or the new chain is nested inside it (e.g. a toString()
    // that logs). Either way the previous message only exists in the staging
    // sink, so dropping it cannot block the log queue.
    private void onAbandoned(RingQueue<LogRecordUtf8Sink> newRing) {
        try {
            // Publish the partial message with a marker, so the log shows the
            // abandoned call site. Only when the abandoned chain targeted the
            // same ring as the new chain: the new chain proves that ring is
            // alive, while another ring may belong to a closed LogFactory.
            if (ring == newRing) {
                sink.putAscii(" #$#$ ABANDONED LOG RECORD #$#$");
                sink.putEOL();
                publish(false);
            }
        } finally {
            reset();
        }
        abandonedLogRecordError.printStackTrace(System.out);
        if (LOG_PARANOIA_MODE != LOG_PARANOIA_MODE_NONE) {
            throw abandonedLogRecordError;
        }
    }

    // Claims a ring slot, copies the staged record into it and publishes it.
    // Nothing between next()/nextBully() and done() can throw. A non-guaranteed
    // record gets dropped when the ring is full.
    private void publish(boolean isWaiting) {
        final Sequence seq = this.seq;
        long cursor;
        if (isWaiting) {
            cursor = seq.nextBully();
        } else {
            // -2 means a lost CAS race with another producer; retry
            while ((cursor = seq.next()) == -2) {
                Os.pause();
            }
        }
        if (cursor > -1) {
            final LogRecordUtf8Sink slot = ring.get(cursor);
            slot.copyFrom(sink);
            slot.setLevel(level);
            seq.done(cursor);
        }
    }

    /**
     * Returns the carrier's record, creating it on the carrier's first chain with
     * a staging buffer sized to match the destination ring's slots. All rings of
     * a LogFactory share one slot size, so the buffer never needs to grow.
     */
    static AsyncLogRecord forCarrier(RingQueue<LogRecordUtf8Sink> ring) {
        AsyncLogRecord rec = CARRIER_RECORD.getIfPresent();
        if (rec == null) {
            rec = new AsyncLogRecord(ring.get(0).capacity());
            CARRIER_RECORD.set(rec);
        }
        return rec;
    }

    private static @NotNull LogError createAbandonedLogError() {
        if (LOG_PARANOIA_MODE == LOG_PARANOIA_MODE_AGGRESSIVE) {
            return new LogError("Abandoned log record");
        } else {
            return new LogError("Abandoned log record detected. Use LOG_PARANOIA_MODE_AGGRESSIVE to diagnose.",
                    false);
        }
    }

    private static void put0(Utf8Sink sink, Throwable e) {
        sink.putAscii(e.getClass().getName());
        if (e.getMessage() != null) {
            sink.putAscii(": ").put(e.getMessage());
        }
    }

    private static void validateUtf8(HeapLogRecordUtf8Sink sink) {
        if (Utf8s.validateUtf8(sink) < 0) {
            LogError e = new LogError("Invalid UTF-8, partial message: \n"
                    + Utf8s.stringFromUtf8BytesSafe(sink) + "\nEND partial message");
            sink.clear();
            e.printStackTrace(System.out);
            throw e;
        }
    }

    // Publishes the partial message up to the failure point, so that the log
    // still shows which chain failed. The staged record never holds a ring slot,
    // so failing to publish it cannot block the log queue either.
    private void releaseOnFailure(Throwable failure) {
        try {
            $();
        } catch (Throwable releaseFailure) {
            if (releaseFailure != failure) {
                failure.addSuppressed(releaseFailure);
            }
        }
    }

    private void reset() {
        isLogRecordInProgress = false;
        if (dejaVu.size() > 0) {
            dejaVu.clear();
        }
        // do not retain the rings of a LogFactory that may get closed
        seq = null;
        ring = null;
        clock = null;
    }

    /**
     * Starts a new chain that formats into the staging sink and publishes to the
     * given ring on {@code $()}. Claims nothing, so a throw from here or from any
     * appender leaves the log queue untouched.
     */
    LogRecord begin(Clock clock, Sequence seq, RingQueue<LogRecordUtf8Sink> ring, int level, boolean isGuaranteed) {
        if (isLogRecordInProgress) {
            onAbandoned(ring);
        }
        // Captures the stack trace of the chain start, reported if this chain
        // gets abandoned. Production (paranoia mode NONE) never reports it.
        if (LOG_PARANOIA_MODE != LOG_PARANOIA_MODE_NONE) {
            abandonedLogRecordError.fillInStackTrace();
        }
        sink.of(ring.get(0).capacity());
        this.clock = clock;
        this.seq = seq;
        this.ring = ring;
        this.level = level;
        this.isGuaranteed = isGuaranteed;
        isLogRecordInProgress = true;
        return this;
    }
}
