/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.log;

import com.questdb.misc.Misc;
import com.questdb.misc.Net;
import com.questdb.mp.RingQueue;
import com.questdb.mp.Sequence;
import com.questdb.std.Sinkable;
import com.questdb.std.str.CharSink;

import java.io.File;

class Logger implements LogRecord, Log {
    private final CharSequence name;
    private final RingQueue<LogRecordSink> debugRing;
    private final Sequence debugSeq;
    private final RingQueue<LogRecordSink> infoRing;
    private final Sequence infoSeq;
    private final RingQueue<LogRecordSink> errorRing;
    private final Sequence errorSeq;
    private final ThreadLocalCursor tl = new ThreadLocalCursor();

    Logger(
            CharSequence name,
            RingQueue<LogRecordSink> debugRing,
            Sequence debugSeq,
            RingQueue<LogRecordSink> infoRing,
            Sequence infoSeq,
            RingQueue<LogRecordSink> errorRing,
            Sequence errorSeq
    ) {
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
    public LogRecord $(int x) {
        sink().put(x);
        return this;
    }

    @Override
    public LogRecord $(double x) {
        sink().put(x, 2);
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
    public LogRecord $(Enum e) {
        sink().put(e != null ? e.name() : "null");
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
        sink().putISODate(System.currentTimeMillis());
        return this;
    }

    @Override
    public LogRecord debug() {
        return xdebug().ts().$(" DEBUG ").$(name);
    }

    @Override
    public LogRecord error() {
        return xerror().ts().$(" ERROR ").$(name);
    }

    @Override
    public LogRecord info() {
        return xinfo().ts().$(" INFO ").$(name);
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
