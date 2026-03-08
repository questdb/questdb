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

package io.questdb.cutlass.line.udp;

import io.questdb.cutlass.line.LineException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.AbstractCharSequence;
import io.questdb.std.str.Utf16Sink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

public class LineUdpLexer implements Mutable, Closeable {

    protected final CharSequenceCache charSequenceCache;
    private final ArrayBackedCharSequence cs = new ArrayBackedCharSequence();
    private final FloatingCharSequence floatingCharSequence = new FloatingCharSequence();
    private final ArrayBackedUtf16Sink sink = new ArrayBackedUtf16Sink();
    private long buffer;
    private long bufferHi;
    private long dstPos = 0;
    private long dstTop = 0;
    private int errorCode = 0;
    private boolean escape = false;
    private boolean escapeQuote = false; // flag to signify we saw a '\' but while parsing a string
    private LineUdpParser parser;
    private boolean skipLine = false;
    private int state = LineUdpParser.EVT_MEASUREMENT;
    private boolean unquoted = true;
    private long utf8ErrorPos;
    private long utf8ErrorTop;

    public LineUdpLexer(int bufferSize) {
        buffer = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_ILP_RSS);
        bufferHi = buffer + bufferSize;
        charSequenceCache = address -> {
            floatingCharSequence.lo = buffer + Numbers.decodeHighInt(address);
            floatingCharSequence.hi = buffer + Numbers.decodeLowInt(address) - 2;
            assert floatingCharSequence.hi < bufferHi;
            assert floatingCharSequence.lo >= buffer;
            assert floatingCharSequence.lo <= floatingCharSequence.hi;
            return floatingCharSequence;
        };
        clear();
    }

    @Override
    public final void clear() {
        escape = false;
        escapeQuote = false;
        dstTop = dstPos = buffer;
        state = LineUdpParser.EVT_MEASUREMENT;
        utf8ErrorTop = utf8ErrorPos = -1;
        skipLine = false;
        unquoted = true;
        errorCode = 0;
    }

    @Override
    public void close() {
        Unsafe.free(buffer, bufferHi - buffer, MemoryTag.NATIVE_ILP_RSS);
    }

    /**
     * Parses line-protocol as UTF8-encoded sequence of bytes.
     *
     * @param bytesPtr byte array address
     * @param hi       high watermark for byte array address
     */
    public void parse(long bytesPtr, long hi) {
        parsePartial(bytesPtr, hi);
    }

    public void parseLast() {
        if (!skipLine) {
            dstPos += 2;
            try {
                onEol();
            } catch (LineException e) {
                parser.onError((int) (dstPos - 2 - buffer) / 2, state, errorCode);
            }
        }
        clear();
    }

    public void withParser(LineUdpParser parser) {
        this.parser = parser;
    }

    private void chop() {
        dstTop = dstPos;
    }

    private void doSkipLine(byte b) {
        if (b == '\n' || b == '\r') {
            clear();
            doSkipLineComplete();
        }
    }

    private void fireEvent() throws LineException {
        // two bytes less between these and one more byte, so we don't have to use >=
        if (dstTop > dstPos - 3 && state != LineUdpParser.EVT_FIELD_VALUE) { // fields do take empty values, same as null
            errorCode = LineUdpParser.ERROR_EMPTY;
            throw LineException.INSTANCE;
        }
        parser.onEvent(cs, state, charSequenceCache);
        chop();
    }

    private void fireEventTransition(int evtTagName, int evtFieldName) {
        switch (state) {
            case LineUdpParser.EVT_MEASUREMENT:
            case LineUdpParser.EVT_TAG_VALUE:
                fireEvent();
                state = evtTagName;
                break;
            case LineUdpParser.EVT_FIELD_VALUE:
                fireEvent();
                state = evtFieldName;
                break;
            default:
                errorCode = LineUdpParser.ERROR_EXPECTED;
                throw LineException.INSTANCE;
        }
    }

    private void fireEventTransition2() {
        switch (state) {
            case LineUdpParser.EVT_TAG_NAME:
                fireEvent();
                state = LineUdpParser.EVT_TAG_VALUE;
                break;
            case LineUdpParser.EVT_FIELD_NAME:
                fireEvent();
                state = LineUdpParser.EVT_FIELD_VALUE;
                break;
            default:
                errorCode = LineUdpParser.ERROR_EXPECTED;
                throw LineException.INSTANCE;
        }
    }

    private void onComma() {
        if (!escapeQuote && unquoted) {
            fireEventTransition(LineUdpParser.EVT_TAG_NAME, LineUdpParser.EVT_FIELD_NAME);
        }
        escapeQuote = false;
    }

    private void onEquals() {
        if (!escapeQuote && unquoted) {
            fireEventTransition2();
        }
        escapeQuote = false;
    }

    private void onEsc() { // '\' backslash
        if (!unquoted) {
            escapeQuote = true; // found in string
        } else {
            escape = true;
        }
    }

    private void onQuote(byte lastByte) {
        if (lastByte == (byte) '=' && !escapeQuote && unquoted) {
            unquoted = false; // open quote
        } else if (!unquoted && !escapeQuote) {
            unquoted = true; // close quote
        }
        escapeQuote = false;
    }

    private void onSpace() {
        if (!escapeQuote && unquoted) {
            fireEventTransition(LineUdpParser.EVT_FIELD_NAME, LineUdpParser.EVT_TIMESTAMP);
        }
        escapeQuote = false;
    }

    private long repairMultiByteChar(long lo, long hi, byte b) throws LineException {
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
                n = Utf8s.utf8DecodeMultiByte(utf8ErrorTop, utf8ErrorPos, Unsafe.getUnsafe().getByte(utf8ErrorTop), sink);
            }

            if (n == -1 && errorLen > 3) {
                errorCode = LineUdpParser.ERROR_ENCODING;
                throw LineException.INSTANCE;
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

    private long utf8Decode(long lo, long hi, byte b) throws LineException {
        if (utf8ErrorPos > -1) {
            return repairMultiByteChar(lo, hi, b);
        }

        int n = Utf8s.utf8DecodeMultiByte(lo, hi, b, sink);
        if (n == -1) {
            return repairMultiByteChar(lo, hi, b);
        } else {
            return lo + n;
        }
    }

    protected void doSkipLineComplete() {
        // for extension
    }

    protected void onEol() throws LineException {
        if (!escapeQuote) {
            switch (state) {
                case LineUdpParser.EVT_MEASUREMENT:
                    chop();
                    break;
                case LineUdpParser.EVT_TAG_VALUE:
                case LineUdpParser.EVT_FIELD_VALUE:
                case LineUdpParser.EVT_TIMESTAMP:
                    fireEvent();
                    parser.onLineEnd(charSequenceCache);
                    clear();
                    break;
                default:
                    errorCode = LineUdpParser.ERROR_EXPECTED;
                    throw LineException.INSTANCE;
            }
        }
    }

    protected void parsePartial(final long bytesPtr, final long hi) {
        long p = bytesPtr;

        byte lastByte = (byte) 0;
        while (p < hi && !partialComplete()) {
            final byte b = Unsafe.getUnsafe().getByte(p);
            if (skipLine) {
                doSkipLine(b);
                p++;
                lastByte = (byte) 0;
                continue;
            }

            if (escape) {
                dstPos -= 2;
            }

            try {
                if (b > -1) {
                    sink.put((char) b);
                    p++;
                } else {
                    try {
                        p = utf8Decode(p, hi, b);
                    } catch (Utf8RepairContinue e) {
                        break;
                    }
                }

                dstPos += 2;

                if (escape) {
                    escape = false;
                    lastByte = b;
                    continue;
                }

                switch (b) {
                    case '"':
                        onQuote(lastByte);
                        break;
                    case '\\':
                        onEsc();
                        break;
                    case '\n':
                    case '\r':
                        onEol();
                        break;
                    case ' ':
                        onSpace();
                        break;
                    case ',':
                        onComma();
                        break;
                    case '=':
                        onEquals();
                        break;
                    default:
                        escapeQuote = false;
                        break;
                }
                lastByte = b;
            } catch (LineException ex) {
                skipLine = true;
                parser.onError((int) (dstPos - 2 - buffer) / 2, state, errorCode);
            }
        }

    }

    protected boolean partialComplete() {
        // For extension
        return false;
    }

    private static class FloatingCharSequence extends AbstractCharSequence {
        long lo, hi;

        @Override
        public char charAt(int index) {
            return Unsafe.getUnsafe().getChar(lo + index * 2L);
        }

        @Override
        public int length() {
            return (int) (hi - lo) / 2;
        }

        @Override
        protected @NotNull CharSequence _subSequence(int start, int end) {
            FloatingCharSequence fcs = new FloatingCharSequence();
            fcs.lo = this.lo + start * 2L;
            fcs.hi = this.lo + end * 2L;
            return fcs;
        }
    }

    private class ArrayBackedCharSequence extends AbstractCharSequence implements CachedCharSequence {

        @Override
        public char charAt(int index) {
            return Unsafe.getUnsafe().getChar(dstTop + index * 2L);
        }

        @Override
        public long getCacheAddress() {
            return Numbers.encodeLowHighInts((int) (dstPos - buffer), (int) (dstTop - buffer));
        }

        @Override
        public int length() {
            return (int) ((dstPos - dstTop) / 2 - 1);
        }
    }

    private class ArrayBackedUtf16Sink implements Utf16Sink {

        @Override
        public Utf16Sink put(char c) {
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
                throw LineException.INSTANCE;
            }
            long buf = Unsafe.realloc(buffer, bufferHi - buffer, capacity, MemoryTag.NATIVE_ILP_RSS);
            long offset = dstTop - buffer;
            bufferHi = buf + capacity;
            buffer = buf;
            dstPos = buf + offset + (dstPos - dstTop);
            dstTop = buf + offset;
        }
    }
}
