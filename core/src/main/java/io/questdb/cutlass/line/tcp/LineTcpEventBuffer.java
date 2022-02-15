/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.CairoException;
import io.questdb.std.Chars;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.FloatingDirectCharSink;

import static io.questdb.cutlass.line.tcp.LineTcpUtils.utf8ToUtf16;

class LineTcpEventBuffer {
    private final long bufLo;
    private final long bufMax;

    private long bufPos;

    private DirectByteCharSequence value;
    private FloatingDirectCharSink sink;
    private boolean hasNonAsciiChars;

    LineTcpEventBuffer(long bufLo, long bufSize) {
        this.bufLo = bufLo;
        this.bufPos = bufLo;
        this.bufMax = bufLo + bufSize;
    }

    public long mark() {
        return bufPos;
    }

    public void reset(long markedPos) {
        assert markedPos >= bufLo && markedPos < bufMax;
        bufPos = markedPos;
    }

    public void reset() {
        bufPos = bufLo;
    }

    public CharSequence prepareUtf8CharSequence(DirectByteCharSequence value, FloatingDirectCharSink sink, boolean hasNonAsciiChars) {
        int length = value.length();
        long len = 2L * length;
        checkCapacity(Byte.BYTES + Integer.BYTES + len);

        this.value = value;
        this.sink = sink;
        this.hasNonAsciiChars = hasNonAsciiChars;

        long markedPos = mark();
        skipByte(); // for field type
        skipInt(); // for string length
        sink.of(bufPos, bufPos + len);
        final CharSequence columnValue = utf8ToUtf16(value, sink, hasNonAsciiChars);
        reset(markedPos);
        return columnValue;
    }

    public void closePreparedUtf8CharSequence(byte type) {
        putByte(type);
        if (!hasNonAsciiChars) {
            sink.put(value);
        }
        int length = sink.length();
        putInt(length);
        bufPos += length * 2L;
    }

    public void putByte(byte value) {
        checkCapacity(Byte.BYTES);
        Unsafe.getUnsafe().putByte(bufPos, value);
        bufPos += Byte.BYTES;
    }

    public void putChar(char value) {
        checkCapacity(Character.BYTES);
        Unsafe.getUnsafe().putChar(bufPos, value);
        bufPos += Character.BYTES;
    }

    public void putDouble(double value) {
        checkCapacity(Double.BYTES);
        Unsafe.getUnsafe().putDouble(bufPos, value);
        bufPos += Double.BYTES;
    }

    public void putFloat(float value) {
        checkCapacity(Float.BYTES);
        Unsafe.getUnsafe().putFloat(bufPos, value);
        bufPos += Float.BYTES;
    }

    public void putInt(int value) {
        checkCapacity(Integer.BYTES);
        Unsafe.getUnsafe().putInt(bufPos, value);
        bufPos += Integer.BYTES;
    }

    public void putLong(long value) {
        checkCapacity(Long.BYTES);
        Unsafe.getUnsafe().putLong(bufPos, value);
        bufPos += Long.BYTES;
    }

    public void putShort(short value) {
        checkCapacity(Short.BYTES);
        Unsafe.getUnsafe().putShort(bufPos, value);
        bufPos += Short.BYTES;
    }

    public void putUtf16CharSequence(CharSequence charSequence) {
        int length = charSequence.length();

        // Negative length indicates to the writer thread that column is passed by
        // name rather than by index. When value is positive (on the else branch)
        // the value is treated as column index.
        putInt(-1 * length);

        long len = 2L * length;
        checkCapacity(len);
        Chars.copyStrChars(charSequence, 0, length, bufPos);
        bufPos += len;
    }

    public byte readByte() {
        byte value = Unsafe.getUnsafe().getByte(bufPos);
        bufPos += Byte.BYTES;
        return value;
    }

    public char readChar() {
        char value = Unsafe.getUnsafe().getChar(bufPos);
        bufPos += Character.BYTES;
        return value;
    }

    public double readDouble() {
        double value = Unsafe.getUnsafe().getDouble(bufPos);
        bufPos += Double.BYTES;
        return value;
    }

    public float readFloat() {
        float value = Unsafe.getUnsafe().getFloat(bufPos);
        bufPos += Float.BYTES;
        return value;
    }

    public int readInt() {
        int value = Unsafe.getUnsafe().getInt(bufPos);
        bufPos += Integer.BYTES;
        return value;
    }

    public long readLong() {
        long value = Unsafe.getUnsafe().getLong(bufPos);
        bufPos += Long.BYTES;
        return value;
    }

    public short readShort() {
        short value = Unsafe.getUnsafe().getShort(bufPos);
        bufPos += Short.BYTES;
        return value;
    }

    public void readUtf16Chars(FloatingDirectCharSink sink) {
        readUtf16Chars(sink, readInt());
    }

    public void readUtf16Chars(FloatingDirectCharSink sink, int length) {
        long nameLo = bufPos;
        bufPos += 2L * length;
        sink.asCharSequence(nameLo, bufPos);
    }

    public void skipByte() {
        bufPos += Byte.BYTES;
    }

    public void skipInt() {
        bufPos += Integer.BYTES;
    }

    public void skipLong() {
        bufPos += Long.BYTES;
    }

    private void checkCapacity(long length) {
        if (bufPos + length >= bufMax) {
            throw CairoException.instance(0).put("queue buffer overflow");
        }
    }
}
