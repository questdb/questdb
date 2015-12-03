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

package com.nfsdb.net.http;

import com.nfsdb.collections.IntObjHashMap;
import com.nfsdb.collections.Mutable;
import com.nfsdb.exceptions.ResponseHeaderBufferTooSmallException;
import com.nfsdb.io.sink.AbstractCharSink;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Misc;
import com.nfsdb.misc.Numbers;
import com.nfsdb.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public class ResponseHeaderBuffer extends AbstractCharSink implements Closeable, Mutable {
    private static final IntObjHashMap<CharSequence> statusMap = new IntObjHashMap<>();
    private static final IntObjHashMap<String> httpStatusMap = new IntObjHashMap<>();
    private final long headerPtr;
    private final long limit;
    private final ByteBuffer headers;
    private long _wptr;

    public ResponseHeaderBuffer(int size) {
        int sz = Numbers.ceilPow2(size);
        this.headers = ByteBuffer.allocateDirect(sz);
        this.headerPtr = _wptr = ((DirectBuffer) headers).address();
        this.limit = headerPtr + sz;
    }

    public void append(CharSequence name, CharSequence value) {
        put(name).put(": ").put(value).put(Misc.EOL);
    }

    @Override
    public void clear() {
        headers.clear();
        _wptr = headerPtr;
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

    public String status(int code, CharSequence contentType) {
        String status = httpStatusMap.get(code);
        if (status == null) {
            throw new IllegalArgumentException("Illegal status code: " + code);
        }
        put("HTTP/1.1 ").put(code).put(' ').put(status).put(Misc.EOL);
        append("Server", "nfsdb/1.0");
        append("Date", getServerTime());
        append("Transfer-Encoding", "chunked");
        append("Content-Type", contentType);

        return status;
    }

    // todo: implement this format
    private static String getServerTime() {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
        return format.format(calendar.getTime());
    }

    void flush(WritableByteChannel channel) throws IOException {
        headers.limit((int) (_wptr - headerPtr));
//        MultipartParser.dump(headers);
        channel.write(headers);
        headers.clear();
    }

    static {
        httpStatusMap.put(200, "OK");
        httpStatusMap.put(400, "Bad request");
        httpStatusMap.put(404, "Not Found");
        httpStatusMap.put(431, "Headers too large");
        httpStatusMap.put(500, "Internal server error");
    }

    static {
        statusMap.put(200, "OK");
    }
}
