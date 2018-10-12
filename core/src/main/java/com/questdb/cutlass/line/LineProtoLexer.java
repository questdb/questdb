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

package com.questdb.cutlass.line;

import com.questdb.std.Chars;
import com.questdb.std.Mutable;
import com.questdb.std.Unsafe;
import com.questdb.std.str.AbstractCharSequence;
import com.questdb.std.str.AbstractCharSink;
import com.questdb.std.str.CharSink;

import java.io.Closeable;

public class LineProtoLexer implements Mutable, Closeable {

    private final ArrayBackedCharSink sink = new ArrayBackedCharSink();
    private final ArrayBackedCharSequence cs = new ArrayBackedCharSequence();
    private final FloatingCharSequence floatingCharSequence = new FloatingCharSequence();

    private int state = LineProtoParser.EVT_MEASUREMENT;
    private boolean escape = false;
    private long buffer;

    private final CharSequenceCache charSequenceCache = address -> {
        floatingCharSequence.lo = buffer + (int) (address >> 32);
        floatingCharSequence.hi = buffer + ((int) address) - 2;
        return floatingCharSequence;
    };

    private long bufferHi;
    private long dstPos = 0;
    private long dstTop = 0;
    private boolean skipLine = false;
    private LineProtoParser parser;
    private long utf8ErrorTop;
    private long utf8ErrorPos;
    private int errorCode = 0;

    public LineProtoLexer(int bufferSize) {
        buffer = Unsafe.malloc(bufferSize);
        bufferHi = buffer + bufferSize;
        clear();
    }

    @Override
    public final void clear() {
        escape = false;
        dstTop = dstPos = buffer;
        state = LineProtoParser.EVT_MEASUREMENT;
        utf8ErrorTop = utf8ErrorPos = -1;
        skipLine = false;
        errorCode = 0;
    }

    @Override
    public void close() {
        Unsafe.free(buffer, bufferHi - buffer);
    }

    /**
     * Parses line-protocol as UTF8-encoded sequence of bytes.
     *
     * @param bytesPtr byte array address
     * @param hi       high watermark for byte array address
     */
    public void parse(long bytesPtr, long hi) {
        long p = bytesPtr;

        while (p < hi) {

            byte b = Unsafe.getUnsafe().getByte(p);

            if (skipLine) {
                doSkipLine(b);
                p++;
                continue;
            }

            if (escape) {
                dstPos -= 2;
            }

            try {
                char c;
                if (b < 0) {
                    try {
                        p = utf8Decode(p, hi, b);
                        c = Unsafe.getUnsafe().getChar(dstPos);
                    } catch (Utf8RepairContinue e) {
                        break;
                    }
                } else {
                    sink.put(c = (char) b);
                    p++;
                }

                dstPos += 2;

                if (escape) {
                    escape = false;
                    continue;
                }

                switch (c) {
                    case ',':
                        fireEventTransition(LineProtoParser.EVT_TAG_NAME, LineProtoParser.EVT_FIELD_NAME);
                        break;
                    case '=':
                        fireEventTransition2();
                        break;
                    case '\\':
                        escape = true;
                        continue;
                    case ' ':
                        fireEventTransition(LineProtoParser.EVT_FIELD_NAME, LineProtoParser.EVT_TIMESTAMP);
                        break;
                    case '\n':
                    case '\r':
                        consumeLineEnd();
                        break;
                    default:
                        // normal byte
                        continue;
                }
                dstTop = dstPos;
            } catch (LineProtoException ex) {
                skipLine = true;
                parser.onError((int) (dstPos - 2 - buffer) / 2, state, errorCode);
            }
        }
    }

    public void parseLast() {
        if (!skipLine) {
            dstPos += 2;
            try {
                consumeLineEnd();
            } catch (LineProtoException e) {
                parser.onError((int) (dstPos - 2 - buffer) / 2, state, errorCode);
            }
        }
        clear();
    }

    public void withParser(LineProtoParser parser) {
        this.parser = parser;
    }

    private void consumeLineEnd() throws LineProtoException {
        switch (state) {
            case LineProtoParser.EVT_MEASUREMENT:
                break;
            case LineProtoParser.EVT_TAG_VALUE:
            case LineProtoParser.EVT_FIELD_VALUE:
            case LineProtoParser.EVT_TIMESTAMP:
                fireEvent();
                parser.onLineEnd(charSequenceCache);
                clear();
                break;
            default:
                errorCode = LineProtoParser.ERROR_EXPECTED;
                throw LineProtoException.INSTANCE;
        }
    }

    private void doSkipLine(byte b) {
        switch (b) {
            case '\n':
            case '\r':
                clear();
                break;
            default:
                break;
        }
    }

    private void fireEvent() throws LineProtoException {
        // two bytes less between these and one more byte so we don't have to use >=
        if (dstTop > dstPos - 3) {
            errorCode = LineProtoParser.ERROR_EMPTY;
            throw LineProtoException.INSTANCE;
        }
        parser.onEvent(cs, state, charSequenceCache);
    }

    private void fireEventTransition(int evtTagName, int evtFieldName) {
        switch (state) {
            case LineProtoParser.EVT_MEASUREMENT:
                fireEvent();
                state = evtTagName;
                break;
            case LineProtoParser.EVT_TAG_VALUE:
                fireEvent();
                state = evtTagName;
                break;
            case LineProtoParser.EVT_FIELD_VALUE:
                fireEvent();
                state = evtFieldName;
                break;
            default:
                errorCode = LineProtoParser.ERROR_EXPECTED;
                throw LineProtoException.INSTANCE;
        }
    }

    private void fireEventTransition2() {
        switch (state) {
            case LineProtoParser.EVT_TAG_NAME:
                fireEvent();
                state = LineProtoParser.EVT_TAG_VALUE;
                break;
            case LineProtoParser.EVT_FIELD_NAME:
                fireEvent();
                state = LineProtoParser.EVT_FIELD_VALUE;
                break;
            default:
                errorCode = LineProtoParser.ERROR_EXPECTED;
                throw LineProtoException.INSTANCE;
        }
    }

    private long repairMultiByteChar(long lo, long hi, byte b) throws LineProtoException {
        int n = -1;
        do {
            // UTF8 error
            if (utf8ErrorTop == -1) {
                utf8ErrorTop = utf8ErrorPos = dstPos + 1;
            }
            // store partial byte
            dstPos = utf8ErrorPos;
            utf8ErrorPos += 1;
            sink.put((char) b);

            // try to decode partial bytes
            long errorLen = utf8ErrorPos - utf8ErrorTop;
            if (errorLen > 1) {
                dstPos = utf8ErrorTop - 1;
                n = Chars.utf8DecodeMultiByte(utf8ErrorTop, utf8ErrorPos, Unsafe.getUnsafe().getByte(utf8ErrorTop), sink);
            }

            if (n == -1 && errorLen > 3) {
                errorCode = LineProtoParser.ERROR_ENCODING;
                throw LineProtoException.INSTANCE;
            }

            if (n == -1 && ++lo < hi) {
                b = Unsafe.getUnsafe().getByte(lo);
            } else {
                break;
            }
        } while (true);

        // we can only be in error when we ran out of bytes to read
        // in which case we return array pointer to original position and exit method
        dstPos = utf8ErrorTop - 1;

        if (n > 0) {
            // if we are successful, reset error pointers
            utf8ErrorTop = utf8ErrorPos = -1;
            // bump pos by one more byte in addition to what we may have incremented in the loop
            return lo + 1;
        }
        throw Utf8RepairContinue.INSTANCE;
    }

    private long utf8Decode(long lo, long hi, byte b) throws LineProtoException {
        if (utf8ErrorPos > -1) {
            return repairMultiByteChar(lo, hi, b);
        }

        int n = Chars.utf8DecodeMultiByte(lo, hi, b, sink);
        if (n == -1) {
            return repairMultiByteChar(lo, hi, b);
        } else {
            return lo + n;
        }
    }

    private class ArrayBackedCharSink extends AbstractCharSink {

        @Override
        public CharSink put(char c) {
            if (dstPos == bufferHi) {
                extend();
            }

            Unsafe.getUnsafe().putChar(dstPos, c);
            return this;
        }

        private void extend() {
            int capacity = ((int) (bufferHi - buffer) * 2);
            if (capacity < 0) {
                // can't realistically reach this in test :(
                throw LineProtoException.INSTANCE;
            }
            long buf = Unsafe.malloc(capacity);
            Unsafe.getUnsafe().copyMemory(buffer, buf, (dstPos - buffer));
            Unsafe.free(buffer, bufferHi - buffer);

            long offset = dstTop - buffer;
            bufferHi = buf + capacity;
            buffer = buf;
            dstPos = buf + offset + (dstPos - dstTop);
            dstTop = buf + offset;
        }
    }

    private class ArrayBackedCharSequence extends AbstractCharSequence implements CachedCharSequence {

        @Override
        public long getCacheAddress() {
            long lo = dstTop - buffer;
            long hi = dstPos - buffer;
            return (lo << 32) | hi;
        }

        @Override
        public int length() {
            return (int) ((dstPos - dstTop) / 2 - 1);
        }

        @Override
        public char charAt(int index) {
            return Unsafe.getUnsafe().getChar(dstTop + index * 2L);
        }
    }

    private class FloatingCharSequence extends AbstractCharSequence {
        long lo, hi;

        @Override
        public int length() {
            return (int) (hi - lo) / 2;
        }

        @Override
        public char charAt(int index) {
            return Unsafe.getUnsafe().getChar(lo + index * 2L);
        }
    }
}
