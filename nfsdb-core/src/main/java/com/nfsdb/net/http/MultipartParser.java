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

import com.nfsdb.collections.ByteSequenceImpl;
import com.nfsdb.collections.DirectByteCharSequence;
import com.nfsdb.collections.Mutable;
import com.nfsdb.collections.ObjectPool;
import com.nfsdb.exceptions.HeadersTooLargeException;
import com.nfsdb.exceptions.InvalidMultipartHeader;
import com.nfsdb.utils.Unsafe;

import java.io.Closeable;
import java.nio.ByteBuffer;

public class MultipartParser implements Closeable, Mutable {

    private final RequestHeaderBuffer hb;
    private final DirectByteCharSequence bytes = new DirectByteCharSequence();
    private final ByteSequenceImpl chars = new ByteSequenceImpl();
    private CharSequence boundary;
    private int boundaryLen;
    private int boundaryPtr;
    private int consumedBoundaryLen;
    private State state;

    public MultipartParser(int headerBufSize, ObjectPool<DirectByteCharSequence> pool) {
        this.hb = new RequestHeaderBuffer(headerBufSize, pool);
        clear();
    }

    public static void dump(ByteBuffer b) {
        int p = b.position();
        while (b.hasRemaining()) {
            System.out.print((char) b.get());
        }
        b.position(p);
    }

    @Override
    public void clear() {
        this.state = State.START_BOUNDARY;
        this.boundaryPtr = 0;
        this.consumedBoundaryLen = 0;
        hb.clear();
    }

    @Override
    public void close() {
        hb.close();
    }

    public MultipartParser of(CharSequence boundary) {
        this.boundary = boundary;
        this.boundaryLen = boundary.length();
        return this;
    }

    public boolean parse(long ptr, int len, MultipartListener listener) throws InvalidMultipartHeader, HeadersTooLargeException {
        long hi = ptr + len;
        long _lo = Long.MAX_VALUE;
        char b;

        while (ptr < hi) {
            switch (state) {
                case BODY_BROKEN:
                    _lo = ptr;
                    state = State.BODY_CONTINUED;
                    break;
                case START_BOUNDARY:
                    boundaryPtr = 1;
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
                            throw InvalidMultipartHeader.INSTANCE;
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
                                listener.onChunk(hb, bytes.of(_lo, ptr - 1), state == State.BODY_CONTINUED);
                                state = State.POTENTIAL_BOUNDARY;
                                return false;
                            case MATCH:
                                listener.onChunk(hb, bytes.of(_lo, ptr - 1), state == State.BODY_CONTINUED);
                                state = State.PRE_HEADERS;
                                ptr += consumedBoundaryLen;
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
                            listener.onChunk(hb, chars.of(boundary, 0, p), true);
                            state = State.BODY_BROKEN;
                            break;
                    }
                    break;
            }
        }

        if (state == State.BODY || state == State.BODY_CONTINUED) {
            listener.onChunk(hb, bytes.of(_lo, ptr), state == State.BODY_CONTINUED);
            state = State.BODY_BROKEN;
        }

        return false;
    }

    private BoundaryStatus matchBoundary(long lo, long hi) {
        long start = lo;
        int ptr = boundaryPtr;

        while (lo < hi && ptr < boundaryLen) {
            if (Unsafe.getUnsafe().getByte(lo++) != boundary.charAt(ptr++)) {
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
