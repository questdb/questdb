/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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

package com.nfsdb.net.http;

import com.nfsdb.ex.HeadersTooLargeException;
import com.nfsdb.ex.MalformedHeaderException;
import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.std.DirectByteCharSequence;
import com.nfsdb.std.Mutable;
import com.nfsdb.std.ObjectPool;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;
import java.io.IOException;

@SuppressFBWarnings("SF_SWITCH_FALLTHROUGH")
public class MultipartParser implements Closeable, Mutable {

    private final Log LOG = LogFactory.getLog(MultipartParser.class);

    private final RequestHeaderBuffer hb;
    private final DirectByteCharSequence bytes = new DirectByteCharSequence();
    private DirectByteCharSequence boundary;
    private int boundaryLen;
    private int boundaryPtr;
    private int consumedBoundaryLen;
    private State state;

    public MultipartParser(int headerBufSize, ObjectPool<DirectByteCharSequence> pool) {
        this.hb = new RequestHeaderBuffer(headerBufSize, pool);
        clear();
    }

    @Override
    public final void clear() {
        this.state = State.START_PARSING;
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
                    state = State.BODY_CONTINUED;
                    break;
                case START_PARSING:
                    listener.setup(context);
                    state = State.START_BOUNDARY;
                    // fall thru
                case START_BOUNDARY:
                    boundaryPtr = 2;
                    // fall thru
                case PARTIAL_START_BOUNDARY:
                    switch (matchBoundary(ptr, hi)) {
                        case INCOMPLETE:
                            state = State.PARTIAL_START_BOUNDARY;
                            return false;
                        case MATCH:
                            state = State.PRE_HEADERS;
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
                            state = State.HEADERS;
                            // fall thru
                        case '\r':
                            ptr++;
                            break;
                        case '-':
                            return true;
                        default:
                            _lo = ptr;
                            state = State.BODY_CONTINUED;
                            break;
                    }
                    break;
                case HEADERS:
                    hb.clear();
                    // fall through
                case PARTIAL_HEADERS:
                    ptr = hb.write(ptr, (int) (hi - ptr), false);
                    if (hb.isIncomplete()) {
                        state = State.PARTIAL_HEADERS;
                        return false;
                    }
                    _lo = ptr;
                    state = State.BODY;
                    break;
                case BODY_CONTINUED:
                case BODY:
                    b = (char) Unsafe.getUnsafe().getByte(ptr++);
                    if (b == boundary.charAt(0)) {
                        boundaryPtr = 1;
                        switch (matchBoundary(ptr, hi)) {
                            case INCOMPLETE:
                                listener.onChunk(context, hb, bytes.of(_lo, ptr - 1), state == State.BODY_CONTINUED);
                                state = State.POTENTIAL_BOUNDARY;
                                return false;
                            case MATCH:
                                listener.onChunk(context, hb, bytes.of(_lo, ptr - 1), state == State.BODY_CONTINUED);
                                state = State.PRE_HEADERS;
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
                        case INCOMPLETE:
                            return false;
                        case MATCH:
                            ptr += consumedBoundaryLen;
                            state = State.PRE_HEADERS;
                            break;
                        case NO_MATCH:
                            listener.onChunk(context, hb, bytes.of(boundary.getLo(), boundary.getLo() + p), true);
                            state = State.BODY_BROKEN;
                            break;
                        default:
                            break;
                    }
                    break;
                default:
                    break;
            }
        }

        if (state == State.BODY || state == State.BODY_CONTINUED) {
            listener.onChunk(context, hb, bytes.of(_lo, ptr), state == State.BODY_CONTINUED);
            state = State.BODY_BROKEN;
        }

        return false;
    }

    private BoundaryStatus matchBoundary(long lo, long hi) {
        long start = lo;
        int ptr = boundaryPtr;

        while (lo < hi && ptr < boundaryLen) {
            if (Unsafe.getUnsafe().getByte(lo++) != boundary.byteAt(ptr++)) {
                return BoundaryStatus.NO_MATCH;
            }
        }

        this.boundaryPtr = ptr;

        if (boundaryPtr < boundaryLen) {
            return BoundaryStatus.INCOMPLETE;
        }

        this.consumedBoundaryLen = (int) (lo - start);
        return BoundaryStatus.MATCH;
    }

    private enum State {
        START_PARSING,
        START_BOUNDARY,
        PARTIAL_START_BOUNDARY,
        HEADERS,
        PARTIAL_HEADERS,
        BODY,
        BODY_CONTINUED,
        BODY_BROKEN,
        POTENTIAL_BOUNDARY,
        PRE_HEADERS
    }

    private enum BoundaryStatus {
        MATCH, NO_MATCH, INCOMPLETE
    }
}
