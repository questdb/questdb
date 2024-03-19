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

package io.questdb.std.str;

import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static io.questdb.cairo.VarcharTypeDriver.VARCHAR_INLINED_PREFIX_MASK;

/**
 * An immutable on-heap sequence of UTF-8 bytes.
 */
public class Utf8String implements Utf8Sequence {
    public static final Utf8String EMPTY = new Utf8String("");
    private static final int ZERO_PADDING_LEN = Long.BYTES - 1;
    private final boolean ascii;
    private final AsciiCharSequence asciiCharSequence = new AsciiCharSequence();
    private final byte[] bytes;
    private final int length;

    public static Utf8String newInstance(@NotNull Utf8Sequence src) {
        byte[] bytes = new byte[src.size() + ZERO_PADDING_LEN];
        for (int i = 0, n = src.size(); i < n; i++) {
            bytes[i] = src.byteAt(i);
        }
        return new Utf8String(bytes, src.size(), src.isAscii());
    }

    public Utf8String(byte @NotNull [] bytes, boolean ascii) {
        this.length = bytes.length;
        this.ascii = ascii;
        this.bytes = zeroPad(bytes);
    }

    public Utf8String(@NotNull String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        this.length = bytes.length;
        this.ascii = (str.length() == bytes.length);
        this.bytes = zeroPad(bytes);
    }

    public Utf8String(char ch) {
        byte[] bytes = String.valueOf(ch).getBytes(StandardCharsets.UTF_8);
        this.length = bytes.length;
        this.ascii = (bytes.length == 1);
        this.bytes = zeroPad(bytes);
    }

    public Utf8String(@NotNull CharSequence seq) {
        byte[] bytes = seq.toString().getBytes(StandardCharsets.UTF_8);
        this.length = bytes.length;
        this.ascii = (seq.length() == bytes.length);
        this.bytes = zeroPad(bytes);
    }

    private Utf8String(byte @NotNull [] bytes, int length, boolean ascii) {
        this.length = length;
        this.ascii = ascii;
        this.bytes = bytes;
    }

    private static byte[] zeroPad(byte[] bytes) {
        return Arrays.copyOf(bytes, bytes.length + ZERO_PADDING_LEN);
    }

    @Override
    public @NotNull CharSequence asAsciiCharSequence() {
        return asciiCharSequence.of(this);
    }

    @Override
    public byte byteAt(int index) {
        return bytes[index];
    }

    public int intAt(int index) {
        return Unsafe.byteArrayGetInt(bytes, index);
    }

    @Override
    public boolean isAscii() {
        return ascii;
    }

    public long longAt(int index) {
        return Unsafe.byteArrayGetLong(bytes, index);
    }

    @Override
    public long zeroPaddedLongAt(int index) {
        return longAt(index);
    }

    @Override
    public long zeroPaddedSixPrefix() {
        return longAt(0) & VARCHAR_INLINED_PREFIX_MASK;
    }

    @Override
    public int size() {
        return length;
    }

    @Override
    public @NotNull String toString() {
        return Utf8s.stringFromUtf8Bytes(this);
    }
}
