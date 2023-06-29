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

import io.questdb.std.Chars;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * UTF8-encoded on-heap char sequence.
 */
public class ByteCharSequence extends AbstractCharSequence implements ByteSequence {
    private final byte[] bytes;

    public ByteCharSequence(byte[] bytes) {
        this.bytes = bytes;
    }

    public static ByteCharSequence newInstance(DirectByteCharSequence src) {
        byte[] bytes = new byte[src.length()];
        for (int i = 0, n = src.length(); i < n; i++) {
            bytes[i] = src.byteAt(i);
        }
        return new ByteCharSequence(bytes);
    }

    @Override
    public byte byteAt(int index) {
        return bytes[index];
    }

    @Override
    public char charAt(int index) {
        return (char) byteAt(index);
    }

    public int intAt(int index) {
        return Unsafe.byteArrayGetInt(bytes, index);
    }

    @Override
    public int length() {
        return bytes.length;
    }

    public long longAt(int index) {
        return Unsafe.byteArrayGetLong(bytes, index);
    }

    @NotNull
    @Override
    public String toString() {
        return Chars.stringFromUtf8Bytes(this);
    }

    @Override
    protected CharSequence _subSequence(int start, int end) {
        return new ByteCharSequence(Arrays.copyOfRange(bytes, start, end));
    }
}
