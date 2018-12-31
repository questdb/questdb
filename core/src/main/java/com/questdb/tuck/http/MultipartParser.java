/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.tuck.http;

import com.questdb.ex.HeadersTooLargeException;
import com.questdb.ex.MalformedHeaderException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.Mutable;
import com.questdb.std.ObjectPool;
import com.questdb.std.Unsafe;
import com.questdb.std.str.DirectByteCharSequence;

import java.io.Closeable;
import java.io.IOException;

public class MultipartParser implements Closeable, Mutable {

    private static final int START_PARSING = 1;
    private static final int START_BOUNDARY = 2;
    private static final int PARTIAL_START_BOUNDARY = 3;
    private static final int HEADERS = 4;
    private static final int PARTIAL_HEADERS = 5;
    private static final int BODY = 6;
    private static final int BODY_CONTINUED = 7;
    private static final int BODY_BROKEN = 8;
    private static final int POTENTIAL_BOUNDARY = 9;
    private static final int PRE_HEADERS = 10;
    private static final int BOUNDARY_MATCH = 1;
    private static final int BOUNDARY_NO_MATCH = 2;
    private static final int BOUNDARY_INCOMPLETE = 3;
    private final Log LOG = LogFactory.getLog(MultipartParser.class);
    private final RequestHeaderBuffer hb;
    private final DirectByteCharSequence bytes = new DirectByteCharSequence();
    private DirectByteCharSequence boundary;
    private int boundaryLen;
    private int boundaryPtr;
    private int consumedBoundaryLen;
    private int state;

    public MultipartParser(int headerBufSize, ObjectPool<DirectByteCharSequence> pool) {
        this.hb = new RequestHeaderBuffer(headerBufSize, pool);
        clear();
    }

    @Override
    public final void clear() {
        this.state = START_PARSING;
        this.boundaryPtr = 0;
        this.consumedBoundaryLen = 0;
        hb.clear();
    }

    @Override
    public void close() {
        hb.close();
    }

    public MultipartParser of(DirectByteCharSequence boundary) {
        this.boundary = boundary;
        this.boundaryLen = boundary.length();
        return this;
    }

    public boolean parse(IOContext context, long ptr, int len, MultipartListener listener) throws HeadersTooLargeException, MalformedHeaderException, IOException {
        long hi = ptr + len;
        long _lo = Long.MAX_VALUE;
        char b;

        while (ptr < hi) {
            switch (state) {
                case BODY_BROKEN:
                    _lo = ptr;
                    state = BODY_CONTINUED;
                    break;
                case START_PARSING:
                    listener.setup(context);
                    state = START_BOUNDARY;
                    // fall thru
                case START_BOUNDARY:
                    boundaryPtr = 2;
                    // fall thru
                case PARTIAL_START_BOUNDARY:
                    switch (matchBoundary(ptr, hi)) {
                        case BOUNDARY_INCOMPLETE:
                            state = PARTIAL_START_BOUNDARY;
                            return false;
                        case BOUNDARY_MATCH:
                            state = PRE_HEADERS;
                            ptr += consumedBoundaryLen;
                            break;
                        default:
                            LOG.error().$("Malformed start boundary").$();
                            throw MalformedHeaderException.INSTANCE;
                    }
                    break;
                case PRE_HEADERS:
                    switch (Unsafe.getUnsafe().getByte(ptr)) {
                        case '\n':
                            state = HEADERS;
                            // fall thru
                        case '\r':
                            ptr++;
                            break;
                        case '-':
                            return true;
                        default:
                            _lo = ptr;
                            state = BODY_CONTINUED;
                            break;
                    }
                    break;
                case HEADERS:
                    hb.clear();
                    // fall through
                case PARTIAL_HEADERS:
                    ptr = hb.write(ptr, (int) (hi - ptr), false);
                    if (hb.isIncomplete()) {
                        state = PARTIAL_HEADERS;
                        return false;
                    }
                    _lo = ptr;
                    state = BODY;
                    break;
                case BODY_CONTINUED:
                case BODY:
                    b = (char) Unsafe.getUnsafe().getByte(ptr++);
                    if (b == boundary.charAt(0)) {
                        boundaryPtr = 1;
                        switch (matchBoundary(ptr, hi)) {
                            case BOUNDARY_INCOMPLETE:
                                listener.onChunk(context, hb, bytes.of(_lo, ptr - 1), state == BODY_CONTINUED);
                                state = POTENTIAL_BOUNDARY;
                                return false;
                            case BOUNDARY_MATCH:
                                listener.onChunk(context, hb, bytes.of(_lo, ptr - 1), state == BODY_CONTINUED);
                                state = PRE_HEADERS;
                                ptr += consumedBoundaryLen;
                                break;
                            default:
                                break;
                        }
                    }
                    break;
                case POTENTIAL_BOUNDARY:
                    int p = boundaryPtr;
                    switch (matchBoundary(ptr, hi)) {
                        case BOUNDARY_INCOMPLETE:
                            return false;
                        case BOUNDARY_MATCH:
                            ptr += consumedBoundaryLen;
                            state = PRE_HEADERS;
                            break;
                        case BOUNDARY_NO_MATCH:
                            listener.onChunk(context, hb, bytes.of(boundary.getLo(), boundary.getLo() + p), true);
                            state = BODY_BROKEN;
                            break;
                        default:
                            break;
                    }
                    break;
                default:
                    break;
            }
        }

        if (state == BODY || state == BODY_CONTINUED) {
            listener.onChunk(context, hb, bytes.of(_lo, ptr), state == BODY_CONTINUED);
            state = BODY_BROKEN;
        }

        return false;
    }

    private int matchBoundary(long lo, long hi) {
        long start = lo;
        int ptr = boundaryPtr;

        while (lo < hi && ptr < boundaryLen) {
            if (Unsafe.getUnsafe().getByte(lo++) != boundary.byteAt(ptr++)) {
                return BOUNDARY_NO_MATCH;
            }
        }

        this.boundaryPtr = ptr;

        if (boundaryPtr < boundaryLen) {
            return BOUNDARY_INCOMPLETE;
        }

        this.consumedBoundaryLen = (int) (lo - start);
        return BOUNDARY_MATCH;
    }
}
