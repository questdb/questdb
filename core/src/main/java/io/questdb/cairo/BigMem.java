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

package io.questdb.cairo;

import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

public interface BigMem extends Closeable {
    @Override
    void close();

    long putBin(BinarySequence value);

    long putBin(long from, long len);

    void putBool(boolean value);

    void putBool(long offset, boolean value);

    void putByte(long offset, byte value);

    void putByte(byte b);

    void putChar(long offset, char value);

    void putChar(char value);

    void putDouble(long offset, double value);

    void putDouble(double value);

    void putFloat(long offset, float value);

    void putFloat(float value);

    void putInt(long offset, int value);

    void putInt(int value);

    void putLong(long offset, long value);

    void putLong(long value);

    void putLong128(long l1, long l2);

    void putLong256(long offset, Long256 value);

    void putLong256(long offset, long l0, long l1, long l2, long l3);

    void putLong256(long l0, long l1, long l2, long l3);

    void putLong256(Long256 value);

    void putLong256(CharSequence hexString);

    void putLong256(@NotNull CharSequence hexString, int start, int end);

    long putNullBin();

    long putNullStr();

    void putNullStr(long offset);

    void putShort(long offset, short value);

    void putShort(short value);

    long putStr(CharSequence value);

    long putStr(char value);

    long putStr(CharSequence value, int pos, int len);

    void putStr(long offset, CharSequence value);

    void putStr(long offset, CharSequence value, int pos, int len);
}
