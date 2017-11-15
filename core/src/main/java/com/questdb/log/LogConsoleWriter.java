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

import com.questdb.mp.RingQueue;
import com.questdb.mp.Sequence;
import com.questdb.mp.SynchronizedJob;
import com.questdb.std.Files;
import com.questdb.std.Os;

import java.io.Closeable;

public class LogConsoleWriter extends SynchronizedJob implements Closeable, LogWriter {
    private final long fd = Files.getStdOutFd();
    private final RingQueue<LogRecordSink> ring;
    private final Sequence subSeq;
    private final int level;

    public LogConsoleWriter(RingQueue<LogRecordSink> ring, Sequence subSeq, int level) {
        this.ring = ring;
        this.subSeq = subSeq;
        this.level = level;
    }

    @Override
    public void bindProperties() {
    }

    @Override
    public void close() {
    }

    @Override
    public boolean runSerially() {
        long cursor = subSeq.next();
        if (cursor < 0) {
            return false;
        }

        final LogRecordSink sink = ring.get(cursor);
        if ((sink.getLevel() & this.level) != 0) {
            Files.append(fd, sink.getAddress(), sink.length());
        }
        subSeq.done(cursor);
        return true;
    }

    static {
        Os.init();
    }
}
