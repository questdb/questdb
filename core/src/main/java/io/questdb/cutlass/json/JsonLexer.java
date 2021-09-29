/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cutlass.json;

import io.questdb.std.*;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

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

    private final IntStack objDepthStack = new IntStack(64);
    private final IntStack arrayDepthStack = new IntStack(64);
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

    public JsonLexer(int cacheSize, int cacheSizeLimit) {
        this.cacheCapacity = cacheSize;
        this.cache = Unsafe.malloc(cacheSize, MemoryTag.NATIVE_DEFAULT);
        this.cacheSizeLimit = cacheSizeLimit;
    }

    private static boolean isNotATerminator(char c) {
        return unquotedTerminators.excludes(c);
    }

    private static JsonException unsupportedEncoding(int position) {
        return JsonException.$(position, "Unsupported encoding");
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
            Unsafe.free(cache, cacheCapacity, MemoryTag.NATIVE_DEFAULT);
        }
    }

    public void parse(long lo, long hi, JsonParser listener) throws JsonException {

        if (lo >= hi) {
            return;
        }

        long p = lo;
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
                        throw JsonException.$((int) (posAtStart + p - lo), "{ is not expected here");
                    }
                    arrayDepthStack.push(arrayDepth);
                    arrayDepth = 0;

                    listener.onEvent(EVT_OBJ_START, null, (int) (posAtStart + p - lo));
                    objDepth++;
                    state = S_EXPECT_FIRST_NAME;
                    break;
                case '}':
                    if (arrayDepth > 0) {
                        throw JsonException.$((int) (posAtStart + p - lo), "} is not expected here. You have non-terminated array");
                    }

                    if (objDepth > 0) {
                        switch (state) {
                            case S_EXPECT_VALUE:
                                throw JsonException.$((int) (posAtStart + p - lo - 1), "Attribute value expected");
                            case S_EXPECT_NAME:
                                throw JsonException.$((int) (posAtStart + p - lo - 1), "Attribute name expected");
                            default:
                                break;
                        }
                        listener.onEvent(EVT_OBJ_END, null, (int) (posAtStart + p - lo));
                        objDepth--;
                        arrayDepth = arrayDepthStack.pop();
                        state = S_EXPECT_COMMA;
                    } else {
                        throw JsonException.$((int) (posAtStart + p - lo), "Dangling }");
                    }
                    break;
                case '[':
                    if (state != S_START && state != S_EXPECT_VALUE) {
                        throw JsonException.$((int) (posAtStart + p - lo), "[ is not expected here");
                    }

                    listener.onEvent(EVT_ARRAY_START, null, (int) (posAtStart + p - lo));
                    objDepthStack.push(objDepth);
                    objDepth = 0;
                    arrayDepth++;
                    state = S_EXPECT_VALUE;
                    break;
                case ']':
                    if (objDepth > 0) {
                        throw JsonException.$((int) (posAtStart + p - lo), "] is not expected here. You have non-terminated object");
                    }

                    if (arrayDepth == 0) {
                        throw JsonException.$((int) (posAtStart + p - lo), "Dangling ]");
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
                        throw JsonException.$((int) (posAtStart + p - lo), "Unexpected comma");
                    }
                    if (arrayDepth > 0) {
                        state = S_EXPECT_VALUE;
                    } else {
                        state = S_EXPECT_NAME;
                    }
                    break;
                case ':':
                    if (state != S_EXPECT_COLON) {
                        throw JsonException.$((int) (posAtStart + p - lo), "Misplaced ':'");
                    }
                    state = S_EXPECT_VALUE;
                    break;
                case '"':
                    if (state != S_EXPECT_NAME && state != S_EXPECT_FIRST_NAME && state != S_EXPECT_VALUE) {
                        throw JsonException.$((int) (posAtStart + p - lo), "Unexpected quote '\"'");
                    }
                    valueStart = p;
                    quoted = true;
                    break;
                default:
                    if (state != S_EXPECT_VALUE) {
                        throw JsonException.$((int) (posAtStart + p - lo), "Unexpected symbol");
                    }
                    // this isn't a quote, include this character
                    valueStart = p - 1;
                    quoted = false;
                    break;
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
            throw JsonException.$(position, "Unterminated string");
        }

        if (arrayDepth > 0) {
            throw JsonException.$(position, "Unterminated array");
        }

        if (objDepth > 0 || arrayDepthStack.size() > 0 || objDepthStack.size() > 0) {
            throw JsonException.$(position, "Unterminated object");
        }
    }

    private void addToStash(long lo, long hi) throws JsonException {
        final int len = (int) (hi - lo);
        int n = len + cacheSize;
        if (n > cacheCapacity) {
            extendCache(Numbers.ceilPow2(n));
        }

        if (len > 0) {
            Vect.memcpy(lo, cache + cacheSize, len);
            cacheSize += len;
        }
    }

    private void extendCache(int n) throws JsonException {
        if (n > cacheSizeLimit) {
            throw JsonException.$(position, "String is too long");
        }
        long ptr = Unsafe.malloc(n, MemoryTag.NATIVE_DEFAULT);
        if (cacheCapacity > 0) {
            Vect.memcpy(cache, ptr, cacheSize);
            Unsafe.free(cache, cacheCapacity, MemoryTag.NATIVE_DEFAULT);
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
                int len = Chars.utf8DecodeMultiByte(p, lim, b, sink);
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
                        len = Chars.utf8DecodeMultiByte(cache + offset, cache + cacheSize, b, sink);
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
                int len = Chars.utf8DecodeMultiByte(p, hi, b, sink);
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
}
