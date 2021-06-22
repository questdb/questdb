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

import io.questdb.mp.QueueConsumer;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class LogFileWriter extends SynchronizedJob implements Closeable, LogWriter {

    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    private final RingQueue<LogRecordSink> ring;
    private final SCSequence subSeq;
    private final int level;
    private long fd = -1;
    private long lim;
    private long buf;
    private long _wptr;
    private final QueueConsumer<LogRecordSink> myConsumer = this::copyToBuffer;
    private String location;
    // can be set via reflection
    @SuppressWarnings("unused")
    private String truncate;
    private String bufferSize;
    private int bufSize;

    public LogFileWriter(RingQueue<LogRecordSink> ring, SCSequence subSeq, int level) {
        this.ring = ring;
        this.subSeq = subSeq;
        this.level = level;
    }

    @Override
    public void bindProperties() {
        if (this.bufferSize != null) {
            try {
                bufSize = Numbers.parseIntSize(this.bufferSize);
            } catch (NumericException e) {
                throw new LogError("Invalid value for bufferSize");
            }
        } else {
            bufSize = DEFAULT_BUFFER_SIZE;
        }
        this.buf = _wptr = Unsafe.malloc(bufSize);
        this.lim = buf + bufSize;
        try (Path path = new Path().of(location).$()) {
            if (truncate != null && Chars.equalsLowerCaseAscii(truncate, "true")) {
                this.fd = Files.openRW(path);
                Files.truncate(fd, 0);
            } else {
                this.fd = Files.openAppend(path);
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
            Unsafe.free(buf, bufSize);
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

    private void copyToBuffer(LogRecordSink sink) {
        final int l = sink.length();
        if ((sink.getLevel() & this.level) != 0 && l > 0) {
            if (_wptr + l >= lim) {
                flush();
            }

            Vect.memcpy(sink.getAddress(), _wptr, l);
            _wptr += l;
        }
    }

    private void flush() {
        Files.append(fd, buf, (int) (_wptr - buf));
        _wptr = buf;
    }
}
