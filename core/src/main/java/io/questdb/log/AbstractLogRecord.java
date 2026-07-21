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

import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.CarrierLocal;
import io.questdb.std.datetime.Clock;
import org.jetbrains.annotations.NotNull;

import static io.questdb.ParanoiaState.*;

abstract class AbstractLogRecord implements Log {
    protected final RingQueue<LogRecordUtf8Sink> advisoryRing;
    protected final Sequence advisorySeq;
    protected final RingQueue<LogRecordUtf8Sink> criticalRing;
    protected final Sequence criticalSeq;
    protected final RingQueue<LogRecordUtf8Sink> debugRing;
    protected final Sequence debugSeq;
    protected final RingQueue<LogRecordUtf8Sink> errorRing;
    protected final Sequence errorSeq;
    protected final RingQueue<LogRecordUtf8Sink> infoRing;
    protected final Sequence infoSeq;
    // One AsyncLogRecord per carrier per Logger. A LOG chain pays exactly one
    // CarrierLocal.get() (one FFI downcall via CarrierIdentity.current()) at the
    // top of the chain via prepareLogRecord; subsequent $(...) and $() calls
    // are direct field reads on the returned AsyncLogRecord.
    protected final CarrierLocal<AsyncLogRecord> tl;
    private final CharSequence name;

    AbstractLogRecord(
            Clock clock,
            CharSequence name,
            RingQueue<LogRecordUtf8Sink> debugRing,
            Sequence debugSeq,
            RingQueue<LogRecordUtf8Sink> infoRing,
            Sequence infoSeq,
            RingQueue<LogRecordUtf8Sink> errorRing,
            Sequence errorSeq,
            RingQueue<LogRecordUtf8Sink> criticalRing,
            Sequence criticalSeq,
            RingQueue<LogRecordUtf8Sink> advisoryRing,
            Sequence advisorySeq
    ) {
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
        // The supplier captures clock and name so the per-carrier AsyncLogRecord
        // can format timestamps and emit the logger name without a back-reference
        // to this AbstractLogRecord.
        this.tl = CarrierLocal.withInitial(() -> new AsyncLogRecord(clock, name));
    }

    public LogRecord advisory() {
        // Same as advisoryW()
        return addTimestamp(xAdvisoryW(), LogLevel.ADVISORY_HEADER);
    }

    public LogRecord advisoryW() {
        return addTimestamp(xAdvisoryW(), LogLevel.ADVISORY_HEADER);
    }

    public LogRecord critical() {
        // same as criticalW()
        return addTimestamp(xCriticalW(), LogLevel.CRITICAL_HEADER);
    }

    public LogRecord debug() {
        return addTimestamp(xdebug(), LogLevel.DEBUG_HEADER);
    }

    public LogRecord debugW() {
        return addTimestamp(xDebugW(), LogLevel.DEBUG_HEADER);
    }

    public LogRecord error() {
        return addTimestamp(xerror(), LogLevel.ERROR_HEADER);
    }

    public LogRecord errorW() {
        return addTimestamp(xErrorW(), LogLevel.ERROR_HEADER);
    }

    public Sequence getCriticalSequence() {
        return criticalSeq;
    }

    public LogRecord info() {
        return addTimestamp(xinfo(), LogLevel.INFO_HEADER);
    }

    public LogRecord infoW() {
        return addTimestamp(xInfoW(), LogLevel.INFO_HEADER);
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
    public LogRecord xInfoW() {
        return nextWaiting(infoSeq, infoRing, LogLevel.INFO);
    }

    protected LogRecord addTimestamp(LogRecord rec, String level) {
        return rec.ts().$(level).$(name);
    }

    protected LogRecord nextWaiting(Sequence seq, RingQueue<LogRecordUtf8Sink> ring, int level) {
        if (seq == null) {
            return NullLogRecord.INSTANCE;
        }
        return prepareLogRecord(seq, ring, level, seq.nextBully());
    }

    @NotNull
    protected LogRecord prepareLogRecord(Sequence seq, RingQueue<LogRecordUtf8Sink> ring, int level, long cursor) {
        AsyncLogRecord rec = tl.get();
        // It's important to detect abandoned-record state BEFORE assigning the
        // new cursor/seq/ring/sink: the recovery path needs the previous chain's
        // sink and cursor to write the ABANDONED marker and release the stuck slot.
        LogError logError = rec.detectAbandonedLogRecord();
        rec.cursor = cursor;
        rec.seq = seq;
        rec.ring = ring;
        LogRecordUtf8Sink sink = ring.get(cursor);
        rec.sink = sink;
        sink.setLevel(level);
        sink.clear();
        if (logError == null) {
            return rec;
        }
        logError.printStackTrace(System.out);
        if (LOG_PARANOIA_MODE != LOG_PARANOIA_MODE_NONE) {
            seq.done(cursor);
            throw logError;
        }
        return rec;
    }

    protected LogRecord xAdvisoryW() {
        return nextWaiting(advisorySeq, advisoryRing, LogLevel.ADVISORY);
    }

    protected LogRecord xCriticalW() {
        return nextWaiting(criticalSeq, criticalRing, LogLevel.CRITICAL);
    }
}
