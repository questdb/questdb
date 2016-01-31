/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.logging;

import com.nfsdb.concurrent.RingQueue;
import com.nfsdb.concurrent.Sequence;
import com.nfsdb.concurrent.SynchronizedJob;
import com.nfsdb.misc.Files;
import com.nfsdb.misc.Os;

import java.io.Closeable;

public class LogConsoleWriter extends SynchronizedJob implements Closeable, LogWriter {
    private static final long fd = 1;
    private final RingQueue<LogRecordSink> ring;
    private final Sequence subSeq;
    private final int level;

    public LogConsoleWriter(RingQueue<LogRecordSink> ring, Sequence subSeq, int level) {
        this.ring = ring;
        this.subSeq = subSeq;
        this.level = level;
    }

    @Override
    public boolean _run() {
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

    @Override
    public void bindProperties() {
    }

    @Override
    public void close() {
    }

    static {
        Os.init();
    }
}
