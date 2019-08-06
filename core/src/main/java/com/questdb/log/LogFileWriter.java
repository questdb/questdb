/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

import com.questdb.mp.QueueConsumer;
import com.questdb.mp.RingQueue;
import com.questdb.mp.SCSequence;
import com.questdb.mp.SynchronizedJob;
import com.questdb.std.*;
import com.questdb.std.str.Path;

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
            throw new LogError("Cannot open file for append: " + location);
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
        if ((sink.getLevel() & this.level) != 0) {
            int l = sink.length();

            if (_wptr + l >= lim) {
                flush();
            }

            Unsafe.getUnsafe().copyMemory(sink.getAddress(), _wptr, l);
            _wptr += l;
        }
    }

    private void flush() {
        Files.append(fd, buf, (int) (_wptr - buf));
        _wptr = buf;
    }
}
