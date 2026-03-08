/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cutlass.http;

import io.questdb.cutlass.http.ex.NotEnoughLinesException;
import io.questdb.cutlass.http.ex.RetryOperationException;
import io.questdb.cutlass.http.ex.TooFewBytesReceivedException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sequence;

import java.io.Closeable;

public class HttpMultipartContentParser implements Closeable, Mutable {

    private static final int BODY = 6;
    private static final int BODY_BROKEN = 8;
    private static final int BOUNDARY_INCOMPLETE = 3;
    private static final int BOUNDARY_MATCH = 1;
    private static final int BOUNDARY_NO_MATCH = 2;
    private static final int DONE = 13;
    private static final int HEADERS = 4;
    private static final int PARTIAL_HEADERS = 5;
    private static final int PARTIAL_START_BOUNDARY = 3;
    private static final int POTENTIAL_BOUNDARY = 9;
    private static final int PRE_HEADERS = 10;
    private static final int START_BOUNDARY = 2;
    private static final int START_HEADERS = 12;
    private static final int START_PARSING = 1;
    private static final int START_PRE_HEADERS = 11;
    private final HttpHeaderParser headerParser;
    private DirectUtf8Sequence boundary;
    private byte boundaryByte;
    private int boundaryLen;
    private int boundaryPtr;
    private int consumedBoundaryLen;
    private boolean firstDashRead;
    private long resumePtr;
    private int state;

    public HttpMultipartContentParser(HttpHeaderParser headerParser) {
        this.headerParser = headerParser;
        clear();
    }

    @Override
    public final void clear() {
        this.state = START_PARSING;
        this.boundaryPtr = 0;
        this.boundaryByte = 0;
        this.boundary = null;
        this.consumedBoundaryLen = 0;
        this.firstDashRead = false;
        this.headerParser.clear();
    }

    @Override
    public void close() {
        headerParser.close();
    }

    public long getResumePtr() {
        return resumePtr;
    }

    /**
     * Setup multipart parser with boundary. Boundary value retrieved from HTTP header must be
     * prefixed with '\r\n--'.
     *
     * @param boundary boundary value
     */
    public void of(DirectUtf8Sequence boundary) {
        this.boundary = boundary;
        this.boundaryLen = boundary.size();
        this.boundaryByte = boundary.byteAt(0);
    }

    public boolean parse(
            long lo,
            long hi,
            HttpMultipartContentProcessor processor
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        long _lo = lo;
        long ptr = lo;
        while (ptr < hi) {
            switch (state) {
                case BODY_BROKEN:
                    _lo = ptr;
                    state = BODY;
                    break;
                case START_PARSING:
                    state = START_BOUNDARY;
                    // fall through
                case START_BOUNDARY:
                    boundaryPtr = 2;
                    // fall through
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
                            // fall through
                        case '\r':
                            ptr++;
                            break;
                        case '-':
                            // make sure that we set the status to DONE only after we read the second '-'
                            if (!firstDashRead) {
                                firstDashRead = true;
                                // on the first '-' we just need to read the next byte
                                ptr++;
                                break;
                            }
                            processor.onPartEnd();
                            state = DONE;
                            return true;
                        default:
                            processor.onChunk(boundary.lo(), boundary.hi());
                            _lo = ptr;
                            state = BODY;
                            break;
                    }
                    break;
                case START_PRE_HEADERS:
                    switch (Unsafe.getUnsafe().getByte(ptr)) {
                        case '\n':
                            state = START_HEADERS;
                            // fall through
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
                    processor.onPartEnd();
                    state = HEADERS;
                    // fall through
                case START_HEADERS:
                    headerParser.clear();
                    state = START_HEADERS;
                    // fall through
                case PARTIAL_HEADERS:
                    ptr = headerParser.parse(ptr, hi, false, false);
                    if (headerParser.isIncomplete()) {
                        state = PARTIAL_HEADERS;
                        return false;
                    }
                    _lo = ptr;
                    processor.onPartBegin(headerParser);
                    state = BODY;
                    break;
                case BODY:
                    byte b = Unsafe.getUnsafe().getByte(ptr++);
                    if (b == boundaryByte) {
                        boundaryPtr = 1;
                        switch (matchBoundary(ptr, hi)) {
                            case BOUNDARY_INCOMPLETE:
                                onChunkWithRetryHandle(processor, _lo, ptr - 1, POTENTIAL_BOUNDARY, hi, true);
                                return false;
                            case BOUNDARY_MATCH:
                                ptr = onChunkWithRetryHandle(processor, _lo, ptr - 1, PRE_HEADERS, ptr + consumedBoundaryLen, false);
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
                            onChunkWithRetryHandle(processor, boundary.lo(), boundary.lo() + p, BODY_BROKEN, ptr, true);
                            break;
                    }
                    break;
                default:
                    // DONE
                    return true;
            }
        }

        if (state == BODY) {
            onChunkWithRetryHandle(processor, _lo, ptr, BODY_BROKEN, ptr, true);
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

    private long onChunkWithRetryHandle(
            HttpMultipartContentProcessor processor,
            long lo,
            long hi,
            int state,
            long resumePtr,
            boolean handleIncomplete
    ) throws PeerIsSlowToReadException, PeerDisconnectedException, ServerDisconnectException {
        RetryOperationException needsRetry = null;
        try {
            processor.onChunk(lo, hi);
        } catch (RetryOperationException e) {
            // Request re-try.
            needsRetry = e;
        } catch (NotEnoughLinesException e) {
            if (handleIncomplete) {
                this.resumePtr = lo;
                throw TooFewBytesReceivedException.INSTANCE;
            } else {
                throw e;
            }
        }

        // Roll to the next state.
        this.state = state;

        // And save point to continue even if RetryOperationException happened.
        this.resumePtr = resumePtr;

        // If retry exception happened, rethrow after setting state and resume point.
        if (needsRetry != null) throw needsRetry;

        return resumePtr;
    }
}
