/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.meta;

import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface AppendableBlock {
    boolean commit(short type, byte version, byte flags);

    int length();

    long putBin(BinarySequence value);

    long putBin(long from, long len);

    void putBool(boolean value);

    void putByte(byte value);

    void putChar(char value);

    void putDouble(double value);

    void putFloat(float value);

    void putInt(int value);

    void putLong(long value);

    void putLong128(long lo, long hi);

    void putLong256(long l0, long l1, long l2, long l3);

    void putLong256(Long256 value);

    void putLong256(@Nullable CharSequence hexString);

    void putLong256(@NotNull CharSequence hexString, int start, int end);

    void putLong256Utf8(@Nullable Utf8Sequence hexString);

    void putRawBytes(long from, long len);

    void putShort(short value);

    long putStr(CharSequence value);

    long putStr(char value);

    long putStr(CharSequence value, int pos, int len);

    long putVarchar(@Nullable Utf8Sequence value);

    void skip(long bytes);
}
