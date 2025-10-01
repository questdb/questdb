/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.std.datetime.Clock;

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
    Logger(
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
        super(
                clock,
                name,
                debugRing,
                debugSeq,
                infoRing,
                infoSeq,
                errorRing,
                errorSeq,
                criticalRing,
                criticalSeq,
                advisoryRing,
                advisorySeq
        );
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

    private LogRecord next(Sequence seq, RingQueue<LogRecordUtf8Sink> ring, int level) {
        if (seq == null) {
            return NullLogRecord.INSTANCE;
        }

        final long cursor = seq.next();
        if (cursor < 0) {
            return NullLogRecord.INSTANCE;
        }
        return prepareLogRecord(seq, ring, level, cursor);
    }
}
