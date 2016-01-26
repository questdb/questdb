/*
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
 */

package com.nfsdb.logging;

import com.nfsdb.collections.Path;
import com.nfsdb.concurrent.RingQueue;
import com.nfsdb.concurrent.Sequence;
import com.nfsdb.concurrent.SynchronizedJob;
import com.nfsdb.exceptions.NumericException;
import com.nfsdb.misc.Files;
import com.nfsdb.misc.Numbers;
import com.nfsdb.misc.Unsafe;

import java.io.Closeable;

public class LogFileWriter extends SynchronizedJob implements Closeable, LogWriter {
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    private final RingQueue<LogRecordSink> ring;
    private final Sequence subSeq;
    private long fd = 0;
    private long lim;
    private long buf;
    private long _wptr;
    private String location;
    private String bufferSizeStr;

    public LogFileWriter(RingQueue<LogRecordSink> ring, Sequence subSeq) {
        this.ring = ring;
        this.subSeq = subSeq;
    }

    @Override
    public boolean _run() {
        long cursor = subSeq.next();
        if (cursor < 0) {

            if (_wptr > buf) {
                flush();
                return true;
            }

            return false;
        }

        final LogRecordSink sink = ring.get(cursor);
        int l = sink.length();

        if (_wptr + l >= lim) {
            flush();
        }

        Unsafe.getUnsafe().copyMemory(sink.getAddress(), _wptr, l);
        _wptr += l;
        subSeq.done(cursor);
        return true;
    }

    @Override
    public void bindProperties() {
        int bufferSize;
        if (bufferSizeStr != null) {
            try {
                bufferSize = Numbers.parseIntSize(bufferSizeStr);
            } catch (NumericException e) {
                bufferSize = DEFAULT_BUFFER_SIZE;
            }
        } else {
            bufferSize = DEFAULT_BUFFER_SIZE;
        }
        this.buf = _wptr = Unsafe.getUnsafe().allocateMemory(bufferSize);
        this.lim = buf + bufferSize;
        this.fd = Files.openAppend(new Path(location));
        if (this.fd < 0) {
            throw new LoggerError("Cannot open file for append: " + location);
        }
    }

    @Override
    public void close() {
        if (buf != 0) {
            if (_wptr > buf) {
                flush();
            }
            Unsafe.getUnsafe().freeMemory(buf);
            buf = 0;
        }
        if (this.fd != 0) {
            Files.close(this.fd);
            this.fd = 0;
        }
    }

    public void setBufferSizeStr(String bufferSizeStr) {
        this.bufferSizeStr = bufferSizeStr;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    private void flush() {
        Files.append(fd, buf, (int) (_wptr - buf));
        _wptr = buf;
    }
}
