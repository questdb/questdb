/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
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
 ******************************************************************************/

package com.questdb.net.http;

import com.questdb.ex.ResponseHeaderBufferTooSmallException;
import com.questdb.io.sink.AbstractCharSink;
import com.questdb.io.sink.CharSink;
import com.questdb.iter.clock.Clock;
import com.questdb.misc.*;
import com.questdb.std.IntObjHashMap;
import com.questdb.std.Mutable;

import java.io.Closeable;
import java.nio.ByteBuffer;

public class ResponseHeaderBuffer extends AbstractCharSink implements Closeable, Mutable {
    private static final IntObjHashMap<CharSequence> statusMap = new IntObjHashMap<>();
    private static final IntObjHashMap<String> httpStatusMap = new IntObjHashMap<>();
    private final long headerPtr;
    private final long limit;
    private final ByteBuffer headers;
    private final Clock clock;
    private long _wptr;
    private boolean chunky;
    private int code;

    public ResponseHeaderBuffer(int size, Clock clock) {
        this.clock = clock;
        int sz = Numbers.ceilPow2(size);
        this.headers = ByteBuffer.allocateDirect(sz);
        this.headerPtr = _wptr = ByteBuffers.getAddress(headers);
        this.limit = headerPtr + sz;
    }

    @Override
    public void clear() {
        headers.clear();
        _wptr = headerPtr;
        chunky = false;
    }

    @Override
    public void close() {
        ByteBuffers.release(headers);
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSink put(CharSequence cs) {
        int len = cs.length();
        long p = _wptr;
        if (p + len < limit) {
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(p++, (byte) cs.charAt(i));
            }
            _wptr = p;
        } else {
            throw ResponseHeaderBufferTooSmallException.INSTANCE;
        }
        return this;
    }

    @Override
    public CharSink put(char c) {
        if (_wptr < limit) {
            Unsafe.getUnsafe().putByte(_wptr++, (byte) c);
            return this;
        }
        throw ResponseHeaderBufferTooSmallException.INSTANCE;
    }

    public int getCode() {
        return code;
    }

    public boolean isChunky() {
        return chunky;
    }

    public String status(int code, CharSequence contentType, long contentLength) {
        this.code = code;
        String status = httpStatusMap.get(code);
        if (status == null) {
            throw new IllegalArgumentException("Illegal status code: " + code);
        }
        put("HTTP/1.1 ").put(code).put(' ').put(status).put(Misc.EOL);
        put("Server: ").put("questDB/1.0").put(Misc.EOL);
        put("Date: ");
        Dates.formatHTTP(this, clock.getTicks());
        put(Misc.EOL);
        if (contentLength > -2) {
            if (this.chunky = (contentLength == -1)) {
                put("Transfer-Encoding: ").put("chunked").put(Misc.EOL);
            } else {
                put("Content-Length: ").put(contentLength).put(Misc.EOL);
            }
        }
        if (contentType != null) {
            put("Content-Type: ").put(contentType).put(Misc.EOL);
        }

        return status;
    }

    ByteBuffer prepareBuffer() {
        if (!chunky) {
            put(Misc.EOL);
        }
        headers.limit((int) (_wptr - headerPtr));
        return headers;
    }


    static {
        httpStatusMap.put(200, "OK");
        httpStatusMap.put(206, "Partial content");
        httpStatusMap.put(304, "Not Modified");
        httpStatusMap.put(400, "Bad request");
        httpStatusMap.put(404, "Not Found");
        httpStatusMap.put(416, "Request range not satisfiable");
        httpStatusMap.put(431, "Headers too large");
        httpStatusMap.put(500, "Internal server error");
    }

    static {
        statusMap.put(200, "OK");
    }
}
