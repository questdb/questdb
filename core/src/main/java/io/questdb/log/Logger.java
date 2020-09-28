/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Sinkable;
import io.questdb.std.microtime.MicrosecondClock;
import io.questdb.std.microtime.TimestampFormatUtils;
import io.questdb.std.str.CharSink;

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
 * <p>
 * Logger appends messages to native memory buffer and dispatches buffer to writer thread queue with {@link #$()} call.
 * When writer queue is full all logger method calls between level and $() become no-ops. In this case queue size
 * have to be increased or choice of log storage has to be reviewed. Depending on complexity of log message
 * structure it should be possible to log between 1,000,000 and 10,000,000 messages per second to SSD device.
 * </p>
 */
class Logger implements LogRecord, Log {
    private final CharSequence name;
    private final RingQueue<LogRecordSink> debugRing;
    private final Sequence debugSeq;
    private final RingQueue<LogRecordSink> infoRing;
    private final Sequence infoSeq;
    private final RingQueue<LogRecordSink> errorRing;
    private final Sequence errorSeq;
    private final ThreadLocalCursor tl = new ThreadLocalCursor();
    private final MicrosecondClock clock;

    Logger(
            MicrosecondClock clock,
            CharSequence name,
            RingQueue<LogRecordSink> debugRing,
            Sequence debugSeq,
            RingQueue<LogRecordSink> infoRing,
            Sequence infoSeq,
            RingQueue<LogRecordSink> errorRing,
            Sequence errorSeq
    ) {
        this.clock = clock;
        this.name = name;
        this.debugRing = debugRing;
        this.debugSeq = debugSeq;
        this.infoRing = infoRing;
        this.infoSeq = infoSeq;
        this.errorRing = errorRing;
        this.errorSeq = errorSeq;
    }

    @Override
    public void $() {
        $(Misc.EOL);
        Holder h = tl.get();
        h.seq.done(h.cursor);
    }

    @Override
    public LogRecord $(CharSequence sequence) {
        if (sequence == null) {
            sink().put("null");
        } else {
            sink().put(sequence);
        }
        return this;
    }

    @Override
    public LogRecord $(CharSequence sequence, int lo, int hi) {
        sink().put(sequence, lo, hi);
        return this;
    }

    @Override
    public LogRecord utf8(CharSequence sequence) {
        if (sequence == null) {
            sink().put("null");
        } else {
            sink().encodeUtf8(sequence);
        }
        return this;
    }

    @Override
    public LogRecord $(int x) {
        sink().put(x);
        return this;
    }

    @Override
    public LogRecord $(double x) {
        sink().put(x, Numbers.MAX_SCALE);
        return this;
    }

    @Override
    public LogRecord $(long x) {
        sink().put(x);
        return this;
    }

    @Override
    public LogRecord $(char c) {
        sink().put(c);
        return this;
    }

    @Override
    public LogRecord $(boolean x) {
        sink().put(x);
        return this;
    }

    @Override
    public LogRecord $(Throwable e) {
        if (e != null) {
            sink().put(Misc.EOL).put(e);
        }
        return this;
    }

    @Override
    public LogRecord $(File x) {
        sink().put(x == null ? "null" : x.getAbsolutePath());
        return this;
    }

    @Override
    public LogRecord $(Object x) {
        sink().put(x == null ? "null" : x.toString());
        return this;
    }

    @Override
    public LogRecord $(Sinkable x) {
        if (x == null) {
            sink().put("null");
        } else {
            x.toSink(sink());
        }
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
    public boolean isEnabled() {
        return true;
    }

    @Override
    public LogRecord ts() {
        sink().putISODate(clock.getTicks());
        return this;
    }

    @Override
    public LogRecord microTime(long x) {
        TimestampFormatUtils.appendDateTimeUSec(sink(), x);
        return this;
    }

    @Override
    public LogRecord debug() {
        return xdebug().ts().$(" D ").$(name);
    }

    @Override
    public LogRecord error() {
        return xerror().ts().$(" E ").$(name);
    }

    @Override
    public LogRecord info() {
        return xinfo().ts().$(" I ").$(name);
    }

    @Override
    public boolean isDebugEnabled() {
        return debugSeq != null;
    }

    public LogRecord xerror() {
        return next(errorSeq, errorRing, LogLevel.LOG_LEVEL_ERROR);
    }

    public LogRecord xinfo() {
        return next(infoSeq, infoRing, LogLevel.LOG_LEVEL_INFO);
    }

    private LogRecord next(Sequence seq, RingQueue<LogRecordSink> ring, int level) {

        if (seq == null) {
            return NullLogRecord.INSTANCE;
        }

        long cursor = seq.next();
        if (cursor < 0) {
            return NullLogRecord.INSTANCE;
        }
        Holder h = tl.get();
        h.cursor = cursor;
        h.seq = seq;
        h.ring = ring;
        LogRecordSink r = ring.get(cursor);
        r.setLevel(level);
        r.clear(0);
        return this;
    }

    private CharSink sink() {
        Holder h = tl.get();
        return h.ring.get(h.cursor);
    }

    private LogRecord xdebug() {
        return next(debugSeq, debugRing, LogLevel.LOG_LEVEL_DEBUG);
    }

    private static class Holder {
        private long cursor;
        private Sequence seq;
        private RingQueue<LogRecordSink> ring;
    }

    private static class ThreadLocalCursor extends ThreadLocal<Holder> {
        @Override
        protected Holder initialValue() {
            return new Holder();
        }
    }
}
