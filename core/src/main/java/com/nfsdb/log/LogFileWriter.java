/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.log;

import com.nfsdb.ex.LogError;
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

    @Override
    public boolean runSerially() {
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
