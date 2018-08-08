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

package com.questdb.cutlass.json;

import com.questdb.std.*;
import com.questdb.std.str.StringSink;

import java.io.Closeable;

import static com.questdb.std.Chars.utf8DecodeMultiByte;

public class JsonLexer implements Mutable, Closeable {
    public static final int EVT_OBJ_START = 1;
    public static final int EVT_OBJ_END = 2;
    public static final int EVT_ARRAY_START = 3;
    public static final int EVT_ARRAY_END = 4;
    public static final int EVT_NAME = 5;
    public static final int EVT_VALUE = 6;
    public static final int EVT_ARRAY_VALUE = 7;

    private static final int S_START = 0;
    private static final int S_EXPECT_NAME = 1;
    private static final int S_EXPECT_FIRST_NAME = 5;
    private static final int S_EXPECT_VALUE = 2;
    private static final int S_EXPECT_COMMA = 3;
    private static final int S_EXPECT_COLON = 4;
    private static final IntHashSet unquotedTerminators = new IntHashSet(256);
    private final IntStack objDepthStack = new IntStack();
    private final IntStack arrayDepthStack = new IntStack();
    private final StringSink sink = new StringSink();
    private final int cacheSizeLimit;
    private int state = S_START;
    private int objDepth = 0;
    private int arrayDepth = 0;
    private boolean ignoreNext = false;
    private boolean quoted = false;
    private long cache;
    private int cacheCapacity;
    private int cacheSize = 0;
    private boolean useCache = false;
    private int position = 0;

    public JsonLexer(int cacheCapacity, int cacheSizeLimit) {
        this.cacheCapacity = cacheCapacity;
        this.cache = Unsafe.malloc(cacheCapacity);
        this.cacheSizeLimit = cacheSizeLimit;
    }

    @Override
    public void clear() {
        objDepthStack.clear();
        arrayDepthStack.clear();
        state = S_START;
        objDepth = 0;
        arrayDepth = 0;
        ignoreNext = false;
        quoted = false;
        cacheSize = 0;
        useCache = false;
        position = 0;
    }

    @Override
    public void close() {
        if (cacheCapacity > 0 && cache != 0) {
            Unsafe.free(cache, cacheCapacity);
        }
    }

    public void parse(long lo, long len, JsonParser listener) throws JsonException {

        if (len <= 0) {
            return;
        }

        long p = lo;
        long hi = lo + len;
        long valueStart = useCache ? lo : 0;
        int posAtStart = position;
        int state = this.state;
        boolean quoted = this.quoted;
        boolean ignoreNext = this.ignoreNext;
        boolean useCache = this.useCache;
        int objDepth = this.objDepth;
        int arrayDepth = this.arrayDepth;

        while (p < hi) {
            char c = (char) Unsafe.getUnsafe().getByte(p++);

            if (ignoreNext) {
                ignoreNext = false;
                continue;
            }

            if (valueStart > 0) {
                if (quoted) {
                    if (c == '\\') {
                        ignoreNext = true;
                        continue;
                    }

                    if (c != '"') {
                        continue;
                    }
                } else if (isNotATerminator(c)) {
                    continue;
                }

                int vp = (int) (posAtStart + valueStart - lo + 1 - cacheSize);
                if (state == S_EXPECT_NAME || state == S_EXPECT_FIRST_NAME) {
                    listener.onEvent(EVT_NAME, getCharSequence(valueStart, p, vp), vp);
                    state = S_EXPECT_COLON;
                } else {
                    listener.onEvent(arrayDepth > 0 ? EVT_ARRAY_VALUE : EVT_VALUE, getCharSequence(valueStart, p, vp), vp);
                    state = S_EXPECT_COMMA;
                }

                valueStart = 0;
                cacheSize = 0;
                useCache = false;

                if (quoted) {
                    // skip the quote mark
                    continue;
                }
            }

            switch (c) {
                case '{':
                    if (state != S_START && state != S_EXPECT_VALUE) {
                        throw JsonException.with("{ is not expected here", (int) (posAtStart + p - lo));
                    }
                    arrayDepthStack.push(arrayDepth);
                    arrayDepth = 0;

                    listener.onEvent(EVT_OBJ_START, null, (int) (posAtStart + p - lo));
                    objDepth++;
                    state = S_EXPECT_FIRST_NAME;
                    break;
                case '}':
                    if (arrayDepth > 0) {
                        throw JsonException.with("} is not expected here. You have non-terminated array", (int) (posAtStart + p - lo));
                    }

                    if (objDepth > 0) {
                        switch (state) {
                            case S_EXPECT_VALUE:
                                throw JsonException.with("Attribute value expected", (int) (posAtStart + p - lo - 1));
                            case S_EXPECT_NAME:
                                throw JsonException.with("Attribute name expected", (int) (posAtStart + p - lo - 1));
                            default:
                                break;
                        }
                        listener.onEvent(EVT_OBJ_END, null, (int) (posAtStart + p - lo));
                        objDepth--;
                        arrayDepth = arrayDepthStack.pop();
                        state = S_EXPECT_COMMA;
                    } else {
                        throw JsonException.with("Dangling }", (int) (posAtStart + p - lo));
                    }
                    break;
                case '[':
                    if (state != S_START && state != S_EXPECT_VALUE) {
                        throw JsonException.with("[ is not expected here", (int) (posAtStart + p - lo));
                    }

                    listener.onEvent(EVT_ARRAY_START, null, (int) (posAtStart + p - lo));
                    objDepthStack.push(objDepth);
                    objDepth = 0;
                    arrayDepth++;
                    state = S_EXPECT_VALUE;
                    break;
                case ']':
                    if (objDepth > 0) {
                        throw JsonException.with("] is not expected here. You have non-terminated object", (int) (posAtStart + p - lo));
                    }

                    if (arrayDepth == 0) {
                        throw JsonException.with("Dangling ]", (int) (posAtStart + p - lo));
                    }

                    listener.onEvent(EVT_ARRAY_END, null, (int) (posAtStart + p - lo));
                    arrayDepth--;
                    objDepth = objDepthStack.pop();
                    state = S_EXPECT_COMMA;
                    break;
                case ' ':
                case '\t':
                case '\n':
                case '\r':
                    break;
                case ',':
                    if (state != S_EXPECT_COMMA) {
                        throw JsonException.with("Unexpected comma", (int) (posAtStart + p - lo));
                    }
                    if (arrayDepth > 0) {
                        state = S_EXPECT_VALUE;
                    } else {
                        state = S_EXPECT_NAME;
                    }
                    break;
                case ':':
                    if (state != S_EXPECT_COLON) {
                        throw JsonException.with("Misplaced ':'", (int) (posAtStart + p - lo));
                    }
                    state = S_EXPECT_VALUE;
                    break;
                case '"':
                    if (state != S_EXPECT_NAME && state != S_EXPECT_FIRST_NAME && state != S_EXPECT_VALUE) {
                        throw JsonException.with("Unexpected quote '\"'", (int) (posAtStart + p - lo));
                    }
                    valueStart = p;
                    quoted = true;
                    break;
                default:
                    if (state != S_EXPECT_VALUE) {
                        throw JsonException.with("Unexpected symbol", (int) (posAtStart + p - lo));
                    }
                    // this isn't a quote, include this character
                    valueStart = p - 1;
                    quoted = false;
            }
        }

        this.position = (int) (posAtStart + p - lo);

        this.state = state;
        this.quoted = quoted;
        this.ignoreNext = ignoreNext;
        this.objDepth = objDepth;
        this.arrayDepth = arrayDepth;

        if (valueStart > 0) {
            // stash
            addToStash(valueStart, hi);
            useCache = true;
        }
        this.useCache = useCache;
    }

    public void parseLast() throws JsonException {
        if (cacheSize > 0) {
            throw JsonException.with("Unterminated string", position);
        }

        if (arrayDepth > 0) {
            throw JsonException.with("Unterminated array", position);
        }

        if (objDepth > 0 || arrayDepthStack.size() > 0 || objDepthStack.size() > 0) {
            throw JsonException.with("Unterminated object", position);
        }
    }

    private static boolean isNotATerminator(char c) {
        return unquotedTerminators.excludes(c);
    }

    private static JsonException unsupportedEncoding(int position) {
        return JsonException.with("Unsupported encoding", position);
    }

    private void addToStash(long lo, long hi) throws JsonException {
        final int len = (int) (hi - lo);
        int n = len + cacheSize;
        if (n > cacheCapacity) {
            extendCache(Numbers.ceilPow2(n));
        }

        if (len > 0) {
            Unsafe.getUnsafe().copyMemory(lo, cache + cacheSize, len);
            cacheSize += len;
        }
    }

    private void extendCache(int n) throws JsonException {
        if (n > cacheSizeLimit) {
            throw JsonException.with("String is too long", position);
        }
        long ptr = Unsafe.malloc(n);
        if (cacheCapacity > 0) {
            Unsafe.getUnsafe().copyMemory(cache, ptr, cacheSize);
            Unsafe.free(cache, cacheCapacity);
        }
        cacheCapacity = n;
        cache = ptr;
    }

    private CharSequence getCharSequence(long lo, long hi, int position) throws JsonException {
        sink.clear();
        if (cacheSize == 0) {
            if (!Chars.utf8Decode(lo, hi - 1, sink)) {
                throw unsupportedEncoding(position);
            }
        } else {
            utf8DecodeCacheAndBuffer(lo, hi - 1, position);
        }
        return sink;
    }

    private void utf8DecodeCacheAndBuffer(long lo, long hi, int position) throws JsonException {
        long p = cache;
        long lim = cache + cacheSize;
        int loOffset = 0;
        while (p < lim) {
            byte b = Unsafe.getUnsafe().getByte(p);
            if (b < 0) {
                int len = utf8DecodeMultiByte(p, lim, b, sink);
                if (len != -1) {
                    p += len;
                } else {
                    // UTF8 error, check if we can switch to main buffer
                    final int cacheRemaining = (int) (lim - p);
                    if (cacheRemaining > 4) {
                        throw unsupportedEncoding(position);
                    } else {
                        // add up to four bytes to stash and try again
                        int n = (int) Math.max(4, hi - lo);

                        // keep offset of 'p' in case stash re-sizes and updates pointers
                        long offset = p - cache;
                        addToStash(lo, lo + n);
                        assert offset < cacheSize;
                        assert cacheSize <= cacheCapacity;
                        len = utf8DecodeMultiByte(cache + offset, cache + cacheSize, b, sink);
                        if (len == -1) {
                            // definitely UTF8 error
                            throw unsupportedEncoding(position);
                        }
                        // right, decoding was a success, we must continue with decoding main buffer from
                        // non-zero offset, because we used some of the bytes to decode cache
                        loOffset = len - cacheRemaining;
                        p += cacheRemaining;
                    }
                }
            } else {
                sink.put((char) b);
                ++p;
            }
        }

        p = lo + loOffset;
        while (p < hi) {
            byte b = Unsafe.getUnsafe().getByte(p);
            if (b < 0) {
                int len = utf8DecodeMultiByte(p, hi, b, sink);
                if (len == -1) {
                    throw unsupportedEncoding(position);
                }
                p += len;
            } else {
                sink.put((char) b);
                ++p;
            }
        }
    }

    static {
        unquotedTerminators.add(' ');
        unquotedTerminators.add('\t');
        unquotedTerminators.add('\n');
        unquotedTerminators.add('\r');
        unquotedTerminators.add(',');
        unquotedTerminators.add('}');
        unquotedTerminators.add(']');
        unquotedTerminators.add('{');
        unquotedTerminators.add('[');
    }
}
