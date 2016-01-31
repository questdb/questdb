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

package com.nfsdb.log;

import com.nfsdb.ex.NumericException;
import com.nfsdb.misc.Files;
import com.nfsdb.misc.Numbers;
import com.nfsdb.misc.Os;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.mp.RingQueue;
import com.nfsdb.mp.Sequence;
import com.nfsdb.mp.SynchronizedJob;
import com.nfsdb.std.Path;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;

public class LogFileWriter extends SynchronizedJob implements Closeable, LogWriter {
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    private final RingQueue<LogRecordSink> ring;
    private final Sequence subSeq;
    private final int level;
    private long fd = 0;
    private long lim;
    private long buf;
    private long _wptr;
    private String location;
    // can be set via reflection
    private String bufferSize;
    private int bufSize;

    public LogFileWriter(RingQueue<LogRecordSink> ring, Sequence subSeq, int level) {
        this.ring = ring;
        this.subSeq = subSeq;
        this.level = level;
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
        if ((sink.getLevel() & this.level) != 0) {
            int l = sink.length();

            if (_wptr + l >= lim) {
                flush();
            }

            Unsafe.getUnsafe().copyMemory(sink.getAddress(), _wptr, l);
            _wptr += l;
        }
        subSeq.done(cursor);
        return true;
    }

    @SuppressFBWarnings("LEST_LOST_EXCEPTION_STACK_TRACE")
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
        this.buf = _wptr = Unsafe.getUnsafe().allocateMemory(bufSize);
        this.lim = buf + bufSize;
        this.fd = Files.openAppend(new Path(location));
        if (this.fd < 0) {
            throw new LogError("Cannot open file for append: " + location);
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

    public int getBufSize() {
        return bufSize;
    }

    public void setBufferSize(String bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    private void flush() {
        Files.append(fd, buf, (int) (_wptr - buf));
        _wptr = buf;
    }

    static {
        Os.init();
    }
}
