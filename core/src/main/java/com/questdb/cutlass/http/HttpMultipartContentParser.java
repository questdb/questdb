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

package com.questdb.cutlass.http;

import com.questdb.network.PeerDisconnectedException;
import com.questdb.network.PeerIsSlowToReadException;
import com.questdb.std.Mutable;
import com.questdb.std.Unsafe;
import com.questdb.std.str.DirectByteCharSequence;

import java.io.Closeable;

public class HttpMultipartContentParser implements Closeable, Mutable {

    private static final int START_PARSING = 1;
    private static final int START_BOUNDARY = 2;
    private static final int PARTIAL_START_BOUNDARY = 3;
    private static final int HEADERS = 4;
    private static final int PARTIAL_HEADERS = 5;
    private static final int BODY = 6;
    private static final int BODY_BROKEN = 8;
    private static final int POTENTIAL_BOUNDARY = 9;
    private static final int PRE_HEADERS = 10;
    private static final int START_PRE_HEADERS = 11;
    private static final int START_HEADERS = 12;
    private static final int DONE = 13;
    private static final int BOUNDARY_MATCH = 1;
    private static final int BOUNDARY_NO_MATCH = 2;
    private static final int BOUNDARY_INCOMPLETE = 3;
    private final HttpHeaderParser headerParser;
    private DirectByteCharSequence boundary;
    private byte boundaryByte;
    private int boundaryLen;
    private int boundaryPtr;
    private int consumedBoundaryLen;
    private int state;

    public HttpMultipartContentParser(HttpHeaderParser headerParser) {
        this.headerParser = headerParser;
        clear();
    }

    @Override
    public final void clear() {
        this.state = START_PARSING;
        this.boundaryPtr = 0;
        this.consumedBoundaryLen = 0;
        headerParser.clear();
    }

    @Override
    public void close() {
        headerParser.close();
    }

    /**
     * Setup multi-part parser with boundary. Boundary value retrieved from HTTP header must be
     * prefixed with '\r\n--'.
     *
     * @param boundary boundary value
     * @return parser instance ready to stream
     */
    public HttpMultipartContentParser of(DirectByteCharSequence boundary) {
        this.boundary = boundary;
        this.boundaryLen = boundary.length();
        this.boundaryByte = (byte) boundary.charAt(0);
        return this;
    }

    public boolean parse(long lo, long hi, HttpMultipartContentListener listener) throws PeerDisconnectedException, PeerIsSlowToReadException {
        long _lo = Long.MAX_VALUE;
        long ptr = lo;
        while (ptr < hi) {
            switch (state) {
                case BODY_BROKEN:
                    _lo = ptr;
                    state = BODY;
                    break;
                case START_PARSING:
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
                            state = START_PRE_HEADERS;
                            ptr += consumedBoundaryLen;
                            break;
                        default:
                            throw HttpException.instance("Malformed start boundary");
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
                            listener.onPartEnd(headerParser);
                            state = DONE;
                            return true;
                        default:
                            listener.onChunk(headerParser, boundary.getLo(), boundary.getHi());
                            _lo = ptr;
                            state = BODY;
                            break;
                    }
                    break;
                case START_PRE_HEADERS:
                    switch (Unsafe.getUnsafe().getByte(ptr)) {
                        case '\n':
                            state = START_HEADERS;
                            // fall thru
                        case '\r':
                            ptr++;
                            break;
                        case '-':
                            return true;
                        default:
                            throw HttpException.instance("Malformed start boundary");
                    }
                    break;
                case HEADERS:
                    listener.onPartEnd(headerParser);
                    // fall thru
                case START_HEADERS:
                    headerParser.clear();
                    // fall through
                case PARTIAL_HEADERS:
                    ptr = headerParser.parse(ptr, hi, false);
                    if (headerParser.isIncomplete()) {
                        state = PARTIAL_HEADERS;
                        return false;
                    }
                    _lo = ptr;
                    state = BODY;
                    listener.onPartBegin(headerParser);
                    break;
                case BODY:
                    byte b = Unsafe.getUnsafe().getByte(ptr++);
                    if (b == boundaryByte) {
                        boundaryPtr = 1;
                        switch (matchBoundary(ptr, hi)) {
                            case BOUNDARY_INCOMPLETE:
                                listener.onChunk(headerParser, _lo, ptr - 1);
                                state = POTENTIAL_BOUNDARY;
                                return false;
                            case BOUNDARY_MATCH:
                                listener.onChunk(headerParser, _lo, ptr - 1);
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
                        default:
                            // can only be BOUNDARY_NO_MATCH:
                            listener.onChunk(headerParser, boundary.getLo(), boundary.getLo() + p);
                            state = BODY_BROKEN;
                            break;
                    }
                    break;
                default:
                    // DONE
                    return true;
            }
        }

        if (state == BODY) {
            listener.onChunk(headerParser, _lo, ptr);
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
