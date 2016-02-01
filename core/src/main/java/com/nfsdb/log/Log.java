/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.log;

import com.nfsdb.io.sink.CharSink;
import com.nfsdb.misc.Misc;
import com.nfsdb.mp.RingQueue;
import com.nfsdb.mp.Sequence;

import java.io.File;

public class Log implements LogRecord {
    private final CharSequence name;
    private final RingQueue<LogRecordSink> debugRing;
    private final Sequence debugSeq;
    private final RingQueue<LogRecordSink> infoRing;
    private final Sequence infoSeq;
    private final RingQueue<LogRecordSink> errorRing;
    private final Sequence errorSeq;
    private final ThreadLocalCursor tl = new ThreadLocalCursor();

    public Log(
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
        sink().put(sequence);
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

    public LogRecord debug() {
        return xdebug().ts().$(" DEBUG ").$(name);
    }

    public LogRecord error() {
        return xerror().ts().$(" ERROR ").$(name);
    }

    public LogRecord info() {
        return xinfo().ts().$(" INFO ").$(name);
    }

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
