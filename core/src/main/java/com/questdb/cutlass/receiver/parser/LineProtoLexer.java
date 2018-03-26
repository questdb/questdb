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

package com.questdb.cutlass.receiver.parser;

import com.questdb.std.Chars;
import com.questdb.std.Mutable;
import com.questdb.std.Unsafe;
import com.questdb.std.str.AbstractCharSequence;
import com.questdb.std.str.AbstractCharSink;
import com.questdb.std.str.ByteSequence;
import com.questdb.std.str.CharSink;

public class LineProtoLexer implements Mutable {

    private final ArrayBackedCharSink sink = new ArrayBackedCharSink();
    private final ArrayBackedCharSequence cs = new ArrayBackedCharSequence();
    private final ArrayBackedByteSequence utf8ErrorSeq = new ArrayBackedByteSequence();
    private final FloatingCharSequence floatingCharSequence = new FloatingCharSequence();
    private final CharSequenceCache charSequenceCache = address -> {
        floatingCharSequence.lo = (int) (address >> 32);
        floatingCharSequence.hi = ((int) address) - 1;
        return floatingCharSequence;
    };

    private int state = LineProtoParser.EVT_MEASUREMENT;
    private boolean escape = false;
    private char buffer[];
    private int dstPos = 0;
    private int dstTop = 0;
    private boolean skipLine = false;
    private LineProtoParser parser;
    private int utf8ErrorTop;
    private int utf8ErrorPos;
    private int errorCode = 0;

    public LineProtoLexer(int bufferSize) {
        buffer = new char[bufferSize];
    }

    @Override
    public final void clear() {
        escape = false;
        state = LineProtoParser.EVT_MEASUREMENT;
        dstTop = dstPos = 0;
        utf8ErrorTop = utf8ErrorPos = -1;
        skipLine = false;
        errorCode = 0;
    }

    /**
     * Parses line-protocol as UTF8-encoded sequence of bytes.
     *
     * @param bytes UTF8-encoded bytes
     */
    public void parse(ByteSequence bytes) {
        int srcPos = 0;
        int len = bytes.length();

        while (srcPos < len) {

            byte b = bytes.byteAt(srcPos);

            if (skipLine) {
                switch (b) {
                    case '\n':
                    case '\r':
                        clear();
                        break;
                    default:
                        break;
                }
                srcPos++;
                continue;
            }

            if (escape) {
                dstPos--;
            }

            try {
                if (b < 0) {
                    try {
                        srcPos = utf8Decode(bytes, srcPos, len, b);
                    } catch (Utf8RepairContinue e) {
                        break;
                    }
                } else {
                    sink.put((char) b);
                    srcPos++;
                }

                if (escape) {
                    escape = false;
                    dstPos++;
                    continue;
                }

                char c = Unsafe.arrayGet(buffer, dstPos++);

                switch (c) {
                    case ',':
                        switch (state) {
                            case LineProtoParser.EVT_MEASUREMENT:
                                fireEvent();
                                state = LineProtoParser.EVT_TAG_NAME;
                                break;
                            case LineProtoParser.EVT_TAG_VALUE:
                                fireEvent();
                                state = LineProtoParser.EVT_TAG_NAME;
                                break;
                            case LineProtoParser.EVT_FIELD_VALUE:
                                fireEvent();
                                state = LineProtoParser.EVT_FIELD_NAME;
                                break;
                            default:
                                errorCode = LineProtoParser.ERROR_EXPECTED;
                                throw LineProtoException.INSTANCE;
                        }
                        break;
                    case '=':
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
                        break;
                    case '\\':
                        escape = true;
                        continue;
                    case ' ':
                        switch (state) {
                            case LineProtoParser.EVT_MEASUREMENT:
                                fireEvent();
                                state = LineProtoParser.EVT_FIELD_NAME;
                                break;
                            case LineProtoParser.EVT_TAG_VALUE:
                                fireEvent();
                                state = LineProtoParser.EVT_FIELD_NAME;
                                break;
                            case LineProtoParser.EVT_FIELD_VALUE:
                                fireEvent();
                                state = LineProtoParser.EVT_TIMESTAMP;
                                break;
                            default:
                                errorCode = LineProtoParser.ERROR_EXPECTED;
                                throw LineProtoException.INSTANCE;
                        }
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
                parser.onError(dstPos - 1, state, errorCode);
            }
        }
    }

    public void parseLast() {
        if (!skipLine) {
            dstPos++;
            try {
                consumeLineEnd();
            } catch (LineProtoException e) {
                parser.onError(dstPos - 1, state, errorCode);
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

    private void fireEvent() throws LineProtoException {
        if (dstTop >= dstPos - 1) {
            errorCode = LineProtoParser.ERROR_EMPTY;
            throw LineProtoException.INSTANCE;
        }
        parser.onEvent(cs, state, charSequenceCache);
    }

    private int repairMultiByteChar(byte b, ByteSequence bytes, int pos, int len) throws LineProtoException {
        int n = -1;
        do {
            // UTF8 error
            if (utf8ErrorTop == -1) {
                utf8ErrorTop = utf8ErrorPos = dstPos + 1;
            }
            // store partial byte
            dstPos = utf8ErrorPos++;
            sink.put((char) b);

            // try to decode partial bytes
            int errorLen = utf8ErrorSeq.length();
            if (errorLen > 1) {
                dstPos = utf8ErrorTop - 1;
                n = Chars.utf8DecodeMultiByte(utf8ErrorSeq, utf8ErrorSeq.byteAt(0), 0, errorLen, sink);
            }

            if (n == -1 && errorLen > 3) {
                errorCode = LineProtoParser.ERROR_ENCODING;
                throw LineProtoException.INSTANCE;
            }

            if (n == -1 && ++pos < len) {
                b = bytes.byteAt(pos);
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
            return pos + 1;
        }
        throw Utf8RepairContinue.INSTANCE;
    }

    private int utf8Decode(ByteSequence bytes, int srcPos, int len, byte b) throws LineProtoException {
        int n = Chars.utf8DecodeMultiByte(bytes, b, srcPos, len, sink);
        if (n == -1) {
            srcPos = repairMultiByteChar(b, bytes, srcPos, len);
        } else {
            srcPos += n;
        }
        return srcPos;
    }

    private class ArrayBackedCharSink extends AbstractCharSink {

        @Override
        public CharSink put(char c) {
            if (dstPos == buffer.length) {
                extend();
            }
            Unsafe.arrayPut(buffer, dstPos, c);
            return this;
        }

        private void extend() {
            int capacity = dstPos * 2;
            if (capacity < 0) {
                // can't realistically reach this in test :(
                throw LineProtoException.INSTANCE;
            }
            char buf[] = new char[capacity];
            System.arraycopy(buffer, 0, buf, 0, dstPos);
            buffer = buf;
        }
    }

    private class ArrayBackedCharSequence extends AbstractCharSequence implements CachedCharSequence {

        @Override
        public long getCacheAddress() {
            return ((long) dstTop << 32) | dstPos;
        }

        @Override
        public int length() {
            return dstPos - dstTop - 1;
        }

        @Override
        public char charAt(int index) {
            return Unsafe.arrayGet(buffer, dstTop + index);
        }
    }

    private class FloatingCharSequence extends AbstractCharSequence {
        int lo, hi;

        @Override
        public int length() {
            return hi - lo;
        }

        @Override
        public char charAt(int index) {
            return Unsafe.arrayGet(buffer, lo + index);
        }
    }

    private class ArrayBackedByteSequence implements ByteSequence {
        @Override
        public byte byteAt(int index) {
            return (byte) Unsafe.arrayGet(buffer, utf8ErrorTop + index);
        }

        @Override
        public int length() {
            return utf8ErrorPos - utf8ErrorTop;
        }
    }
}
