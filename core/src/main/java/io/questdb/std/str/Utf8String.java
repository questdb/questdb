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

/**
 * An immutable on-heap sequence of UTF-8 bytes.
 */
public class Utf8String implements Utf8Sequence {
    public static final Utf8String EMPTY = new Utf8String("");
    private final boolean ascii;
    private final AsciiCharSequence asciiCharSequence = new AsciiCharSequence();
    private final byte[] bytes;

    public Utf8String(byte @NotNull [] bytes, boolean ascii) {
        this.bytes = bytes;
        this.ascii = ascii;
    }

    public Utf8String(@NotNull String str) {
        this.bytes = str.getBytes(StandardCharsets.UTF_8);
        this.ascii = str.length() == bytes.length;
    }

    public Utf8String(@NotNull CharSequence seq) {
        this.bytes = seq.toString().getBytes(StandardCharsets.UTF_8);
        this.ascii = seq.length() == bytes.length;
    }

    public static Utf8String newInstance(@NotNull Utf8Sequence src) {
        byte[] bytes = new byte[src.size()];
        for (int i = 0, n = src.size(); i < n; i++) {
            bytes[i] = src.byteAt(i);
        }
        return new Utf8String(bytes, src.isAscii());
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
    public int size() {
        return bytes.length;
    }

    @Override
    public @NotNull String toString() {
        return Utf8s.stringFromUtf8Bytes(this);
    }
}
