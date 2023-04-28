/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.cutlass.pgwire.test;

import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

public final class PgWireRecorder implements Closeable {
    private static final char[] HEX_DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    private long ptr;
    private long size;
    private long offset;

    private static final int IN_CONTROL = 0;
    private static final int OUT_CONTROL = 1;

    private final LongList ctrlOffsets = new LongList();
    private final IntList ctrls = new IntList();

    public PgWireRecorder(long size) {
        ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        this.size = size;
    }

    public void controlOut() {
        ctrls.add(OUT_CONTROL);
        ctrlOffsets.add(offset);
    }

    public void controlIn() {
        ctrls.add(IN_CONTROL);
        ctrlOffsets.add(offset);
    }

    @Override
    public void close() {
        ptr = Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
    }

    public void clear() {
        offset = 0;
        ctrlOffsets.clear();
        ctrls.clear();
    }

    public long offsetAndAdvanceInt() {
        long localOffset = offset;
        assert offset + Integer.BYTES <= size;
        offset += Integer.BYTES;
        return localOffset;
    }

    public long offsetAndAdvanceShort() {
        long localOffset = offset;
        assert offset + Short.BYTES <= size;
        offset += Short.BYTES;
        return localOffset;
    }

    public int appendByte(byte b) {
        // todo: check for overflow
        assert offset + Byte.BYTES <= size;
        setByte(b, offset);
        offset++;
        return Byte.BYTES;
    }

    public int appendBytes(byte[] bytes) {
        int len = bytes.length;
        for (byte b : bytes) {
            appendByte(b);
        }
        return len;
    }

    public int appendBytesZ(byte[] bytes) {
        int len = appendBytes(bytes);
        appendByte((byte) 0);
        return len + 1;
    }

    public void setByte(byte b, long offset) {
        assert offset + Byte.BYTES <= size;
        Unsafe.getUnsafe().putByte(ptr + offset, b);
    }

    public void setMsgSize(long initialOffset) {
        long msgSize = offset - initialOffset;
        setIntBE((int) msgSize, initialOffset);
    }

    public void setIntBE(int i, long offset) {
        assert offset + Integer.BYTES <= size;
        setByte((byte) (i >> 24), offset);
        setByte((byte) (i >> 16), offset + 1);
        setByte((byte) (i >> 8), offset + 2);
        setByte((byte) (i), offset + 3);
    }

    public void setShortBE(short s, long offset) {
        assert offset + Short.BYTES <= size;
        setByte((byte) (s >> 8), offset);
        setByte((byte) (s), offset + 1);
    }

    public int appendShortBE(short s) {
        assert offset + Short.BYTES <= size;
        appendByte((byte) (s >> 8));
        appendByte((byte) (s));
        return Short.BYTES;
    }

    public int appendIntBE(int i) {
        appendByte((byte) (i >> 24));
        appendByte((byte) (i >> 16));
        appendByte((byte) (i >> 8));
        appendByte((byte) (i));
        return Integer.BYTES;
    }

    public int appendLongBE(long l) {
        appendByte((byte) (l >> 56));
        appendByte((byte) (l >> 48));
        appendByte((byte) (l >> 40));
        appendByte((byte) (l >> 32));
        appendByte((byte) (l >> 24));
        appendByte((byte) (l >> 16));
        appendByte((byte) (l >> 8));
        appendByte((byte) (l));
        return Long.BYTES;
    }

    public int appendAsciiIntSizePrefix(CharSequence cs) {
        long startOffset = offsetAndAdvanceInt();
        int len = appendAscii(cs);
        setIntBE(len, startOffset);
        return len + Integer.BYTES;
    }

    public int appendAscii(CharSequence cs) {
        int len = cs.length();
        for (int i = 0; i < len; i++) {
            char c = cs.charAt(i);
            assert c < 128; // todo: support UTF-8 and rename related methods from "Ascii" to "Utf8"
            appendByte((byte) c);
        }
        return len;
    }

    public int appendAsciiZ(CharSequence cs) {
        int len = appendAscii(cs);
        appendByte((byte) 0);
        return len + 1;
    }

    public void appendHexAndClear(StringSink sb) {
        assert ctrlOffsets.size() > 0;
        assert ctrlOffsets.size() == ctrls.size();

        int controlOffsetIndex = 0;
        long nextControlOffset = ctrlOffsets.get(0);
        for (long i = 0; i < offset; i++) {
            if (i == nextControlOffset) {
                if (controlOffsetIndex != 0) {
                    // we are not at the start of the buffer
                    // so we need to add a newline before the control
                    sb.put('\n');
                }
                int control = ctrls.get(controlOffsetIndex);
                if (control == OUT_CONTROL) {
                    sb.put('>');
                } else if (control == IN_CONTROL) {
                    sb.put('<');
                } else {
                    throw new IllegalStateException("unknown control value at offset " + i + ": " + control);
                }
                if (++controlOffsetIndex < ctrlOffsets.size()) {
                    nextControlOffset = ctrlOffsets.get(controlOffsetIndex);
                }
            }
            byte b = Unsafe.getUnsafe().getByte(ptr + i);
            sb.put(HEX_DIGITS[(b >> 4) & 0x0F]);
            sb.put(HEX_DIGITS[b & 0x0F]);
        }
        clear();
    }
}
