/*******************************************************************************
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

import io.questdb.mp.QueueConsumer;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

public class LogFileWriter extends SynchronizedJob implements Closeable, LogWriter {

    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    private final int level;
    private final RingQueue<LogRecordUtf8Sink> ring;
    private final SCSequence subSeq;
    private long _wptr;
    private long buf;
    private int bufSize;
    private String bufferSize;
    private long fd = -1;
    private long lim;
    private String location;
    private QueueConsumer<LogRecordUtf8Sink> myConsumer = this::copyToBuffer;
    // can be set via reflection
    @SuppressWarnings("unused")
    private String truncate;

    public LogFileWriter(RingQueue<LogRecordUtf8Sink> ring, SCSequence subSeq, int level) {
        this.ring = ring;
        this.subSeq = subSeq;
        this.level = level;
    }

    @Override
    public void bindProperties(LogFactory factory) {
        if (this.bufferSize != null) {
            try {
                bufSize = Numbers.parseIntSize(this.bufferSize);
            } catch (NumericException e) {
                throw new LogError("Invalid value for bufferSize");
            }
        } else {
            bufSize = DEFAULT_BUFFER_SIZE;
        }
        this.buf = _wptr = Unsafe.malloc(bufSize, MemoryTag.NATIVE_LOGGER);
        this.lim = buf + bufSize;
        try (Path path = new Path()) {
            path.of(location);
            if (truncate != null && Chars.equalsLowerCaseAscii(truncate, "true")) {
                this.fd = Files.openRW(path.$());
                Files.truncate(fd, 0);
            } else {
                this.fd = Files.openAppend(path.$());
            }
        }
        if (this.fd == -1) {
            throw new LogError("Cannot open file for append: " + location + " [errno=" + Os.errno() + ']');
        }
    }

    @Override
    public void close() {
        if (buf != 0) {
            if (_wptr > buf) {
                flush();
            }
            Unsafe.free(buf, bufSize, MemoryTag.NATIVE_LOGGER);
            buf = 0;
        }
        if (this.fd != -1) {
            Files.close(this.fd);
            this.fd = -1;
        }
    }

    public int getBufSize() {
        return bufSize;
    }

    @TestOnly
    public QueueConsumer<LogRecordUtf8Sink> getMyConsumer() {
        return myConsumer;
    }

    @Override
    public boolean runSerially() {
        if (subSeq.consumeAll(ring, myConsumer)) {
            return true;
        }
        if (_wptr > buf) {
            flush();
            return true;
        }

        return false;
    }

    @SuppressWarnings("unused")
    public void setBufferSize(String bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @TestOnly
    public void setMyConsumer(QueueConsumer<LogRecordUtf8Sink> myConsumer) {
        this.myConsumer = myConsumer;
    }

    private void copyToBuffer(LogRecordUtf8Sink sink) {
        final int size = sink.size();
        if ((sink.getLevel() & this.level) != 0 && size > 0) {
            if (_wptr + size >= lim) {
                flush();
            }

            Vect.memcpy(_wptr, sink.ptr(), size);
            _wptr += size;
        }
    }

    private void flush() {
        Files.append(fd, buf, (int) (_wptr - buf));
        _wptr = buf;
    }
}
