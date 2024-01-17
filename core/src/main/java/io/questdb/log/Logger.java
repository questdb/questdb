/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.network.Net;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;

/**
 * Builds and sends log messages to writer thread. Log messages are constructed using "builder" pattern,
 * which usually begins with "level method" {@link #debug()}, {@link #info()} or {@link #error()} followed by
 * $(x) to append log message content and must terminate with {@link #$()}. There are $(x) methods for all types
 * of "x" parameter to help avoid string concatenation and string creation in general.
 * <p>
 * <code>
 * private static final Log LOG = LogFactory.getLog(MyClass.class);
 * ...
 * LOG.info().$("Hello world: ").$(123).$();
 * </code>
 * <p>
 * Logger appends messages to native memory buffer and dispatches buffer to writer thread queue with {@link #$()} call.
 * When writer queue is full all logger method calls between level and $() become no-ops. In this case queue size
 * have to be increased or choice of log storage has to be reviewed. Depending on complexity of log message
 * structure it should be possible to log between 1,000,000 and 10,000,000 messages per second to SSD device.
 * </p>
 */
public final class Logger extends AbstractLogRecord implements Log {
    private final RingQueue<LogRecordSink> advisoryRing;
    private final Sequence advisorySeq;
    private final MicrosecondClock clock;
    private final RingQueue<LogRecordSink> criticalRing;
    private final Sequence criticalSeq;
    private final RingQueue<LogRecordSink> debugRing;
    private final Sequence debugSeq;
    private final RingQueue<LogRecordSink> errorRing;
    private final Sequence errorSeq;
    private final RingQueue<LogRecordSink> infoRing;
    private final Sequence infoSeq;
    private final CharSequence name;
    private final ThreadLocalCursor tl = new ThreadLocalCursor();

    Logger(
            MicrosecondClock clock,
            CharSequence name,
            RingQueue<LogRecordSink> debugRing,
            Sequence debugSeq,
            RingQueue<LogRecordSink> infoRing,
            Sequence infoSeq,
            RingQueue<LogRecordSink> errorRing,
            Sequence errorSeq,
            RingQueue<LogRecordSink> criticalRing,
            Sequence criticalSeq,
            RingQueue<LogRecordSink> advisoryRing,
            Sequence advisorySeq
    ) {
        this.clock = clock;
        this.name = name;
        this.debugRing = debugRing;
        this.debugSeq = debugSeq;
        this.infoRing = infoRing;
        this.infoSeq = infoSeq;
        this.errorRing = errorRing;
        this.errorSeq = errorSeq;
        this.criticalRing = criticalRing;
        this.criticalSeq = criticalSeq;
        this.advisoryRing = advisoryRing;
        this.advisorySeq = advisorySeq;
    }

    @Override
    public LogRecord $(@Nullable Utf8Sequence sequence) {
        if (sequence == null) {
            sink().putAscii("null");
        } else {
            sink().put(sequence);
        }
        return this;
    }

    @Override
    public LogRecord $(@Nullable DirectUtf8Sequence sequence) {
        if (sequence == null) {
            sink().putAscii("null");
        } else {
            sink().put(sequence);
        }
        return this;
    }

    @Override
    public void $() {
        sink().putEOL();
        Holder h = tl.get();
        h.seq.done(h.cursor);
    }

    @Override
    public LogRecord $(@Nullable CharSequence sequence) {
        if (sequence == null) {
            sink().putAscii("null");
        } else {
            sink().putAscii(sequence);
        }
        return this;
    }

    @Override
    public LogRecord $(@NotNull CharSequence sequence, int lo, int hi) {
        sink().putAscii(sequence, lo, hi);
        return this;
    }

    @Override
    public LogRecord $(int x) {
        sink().put(x);
        return this;
    }

    @Override
    public LogRecord $(double x) {
        sink().put(x);
        return this;
    }

    @Override
    public LogRecord $(long l) {
        sink().put(l);
        return this;
    }

    @Override
    public LogRecord $(boolean x) {
        sink().put(x);
        return this;
    }

    @Override
    public LogRecord $(char c) {
        sink().put(c);
        return this;
    }

    @Override
    public LogRecord $(@Nullable File x) {
        sink().put(x == null ? "null" : x.getAbsolutePath());
        return this;
    }

    @Override
    public LogRecord $(@Nullable Object x) {
        if (x == null) {
            sink().putAscii("null");
        } else {
            try {
                sink().put(x.toString());
            } catch (Throwable t) {
                // Complex toString() method could throw e.g. NullPointerException.
                // If that happens, we've to release cursor to prevent blocking log queue.
                $();
                throw t;
            }
        }
        return this;
    }

    @Override
    public LogRecord $(@Nullable Sinkable x) {
        if (x == null) {
            sink().putAscii("null");
        } else {
            try {
                x.toSink(sink());
            } catch (Throwable t) {
                // Complex toSink() method could throw e.g. NullPointerException.
                // If that happens, we've to release cursor to prevent blocking log queue.
                $();
                throw t;
            }
        }
        return this;
    }

    @Override
    public LogRecord $256(long a, long b, long c, long d) {
        Numbers.appendLong256(a, b, c, d, sink());
        return this;
    }

    @Override
    public LogRecord $hex(long value) {
        Numbers.appendHex(sink(), value, false);
        return this;
    }

    @Override
    public LogRecord $hexPadded(long value) {
        Numbers.appendHex(sink(), value, true);
        return this;
    }

    @Override
    public LogRecord $ip(long ip) {
        Net.appendIP4(sink(), ip);
        return this;
    }

    @Override
    public LogRecord $ts(long x) {
        sink().putISODate(x);
        return this;
    }

    @Override
    public LogRecord $utf8(long lo, long hi) {
        sink().put(lo, hi);
        return this;
    }

    @Override
    public LogRecord advisory() {
        // Same as advisoryW()
        return addTimestamp(xAdvisoryW(), LogLevel.ADVISORY_HEADER);
    }

    @Override
    public LogRecord advisoryW() {
        return addTimestamp(xAdvisoryW(), LogLevel.ADVISORY_HEADER);
    }

    @Override
    public LogRecord critical() {
        // same as criticalW()
        return addTimestamp(xCriticalW(), LogLevel.CRITICAL_HEADER);
    }

    @Override
    public LogRecord debug() {
        return addTimestamp(xdebug(), LogLevel.DEBUG_HEADER);
    }

    @Override
    public LogRecord debugW() {
        return addTimestamp(xDebugW(), LogLevel.DEBUG_HEADER);
    }

    @Override
    public LogRecord error() {
        return addTimestamp(xerror(), LogLevel.ERROR_HEADER);
    }

    @Override
    public LogRecord errorW() {
        return addTimestamp(xErrorW(), LogLevel.ERROR_HEADER);
    }

    public Sequence getCriticalSequence() {
        return criticalSeq;
    }

    @Override
    public LogRecord info() {
        return addTimestamp(xinfo(), LogLevel.INFO_HEADER);
    }

    @Override
    public LogRecord infoW() {
        return addTimestamp(xInfoW(), LogLevel.INFO_HEADER);
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public LogRecord microTime(long x) {
        TimestampFormatUtils.appendDateTimeUSec(sink(), x);
        return this;
    }

    @Override
    public LogRecord put(char c) {
        sink().put(c);
        return this;
    }

    @Override
    public LogRecord ts() {
        sink().putISODate(clock.getTicks());
        return this;
    }

    @Override
    public LogRecord utf8(@Nullable CharSequence sequence) {
        if (sequence == null) {
            sink().putAscii("null");
        } else {
            sink().put(sequence);
        }
        return this;
    }

    public LogRecord xAdvisoryW() {
        return nextWaiting(advisorySeq, advisoryRing, LogLevel.ADVISORY);
    }

    public LogRecord xCriticalW() {
        return nextWaiting(criticalSeq, criticalRing, LogLevel.CRITICAL);
    }

    @Override
    public LogRecord xDebugW() {
        return nextWaiting(debugSeq, debugRing, LogLevel.DEBUG);
    }

    public LogRecord xErrorW() {
        return nextWaiting(errorSeq, errorRing, LogLevel.ERROR);
    }

    /**
     * Guaranteed log delivery at INFO level. The calling thread will wait for async logger
     * to become available instead of discarding log message.
     *
     * @return log record API
     */
    @Override
    public LogRecord xInfoW() {
        return nextWaiting(infoSeq, infoRing, LogLevel.INFO);
    }

    @Override
    public LogRecord xadvisory() {
        return next(advisorySeq, advisoryRing, LogLevel.ADVISORY);
    }

    @Override
    public LogRecord xcritical() {
        return next(criticalSeq, criticalRing, LogLevel.CRITICAL);
    }

    @Override
    public LogRecord xdebug() {
        return next(debugSeq, debugRing, LogLevel.DEBUG);
    }

    @Override
    public LogRecord xerror() {
        return next(errorSeq, errorRing, LogLevel.ERROR);
    }

    @Override
    public LogRecord xinfo() {
        return next(infoSeq, infoRing, LogLevel.INFO);
    }

    private LogRecord addTimestamp(LogRecord rec, String level) {
        return rec.ts().$(level).$(name);
    }

    private LogRecord next(Sequence seq, RingQueue<LogRecordSink> ring, int level) {
        if (seq == null) {
            return NullLogRecord.INSTANCE;
        }

        final long cursor = seq.next();
        if (cursor < 0) {
            return NullLogRecord.INSTANCE;
        }
        return prepareLogRecord(seq, ring, level, cursor);
    }

    private LogRecord nextWaiting(Sequence seq, RingQueue<LogRecordSink> ring, int level) {
        if (seq == null) {
            return NullLogRecord.INSTANCE;
        }
        return prepareLogRecord(seq, ring, level, seq.nextBully());
    }

    @NotNull
    private LogRecord prepareLogRecord(Sequence seq, RingQueue<LogRecordSink> ring, int level, long cursor) {
        Holder h = tl.get();
        h.cursor = cursor;
        h.seq = seq;
        h.ring = ring;
        LogRecordSink r = ring.get(cursor);
        r.setLevel(level);
        r.clear();
        return this;
    }

    @Override
    protected LogRecordSink sink() {
        Holder h = tl.get();
        return h.ring.get(h.cursor);
    }

    private static class Holder {
        private long cursor;
        private RingQueue<LogRecordSink> ring;
        private Sequence seq;
    }

    private static class ThreadLocalCursor extends ThreadLocal<Holder> {
        @Override
        protected Holder initialValue() {
            return new Holder();
        }
    }
}
