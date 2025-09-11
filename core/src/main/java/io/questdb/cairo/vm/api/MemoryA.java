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

package io.questdb.cairo.vm.api;

import io.questdb.cairo.arr.ArrayView;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

// appendable
public interface MemoryA extends Closeable {

    @Override
    void close();

    long getAppendOffset();

    long getExtendSegmentSize();

    void jumpTo(long offset);

    void putArray(ArrayView array);

    long putBin(BinarySequence value);

    long putBin(long from, long len);

    void putBlockOfBytes(long from, long len);

    void putBool(boolean value);

    void putByte(byte value);

    void putChar(char value);

    void putDecimal128(long hi, long lo);

    void putDecimal256(long hh, long hl, long lh, long ll);

    void putDouble(double value);

    void putFloat(float value);

    void putInt(int value);

    void putLong(long value);

    // two longs are written back to back: little endian
    void putLong128(long lo, long hi);

    void putLong256(long l0, long l1, long l2, long l3);

    void putLong256(Long256 value);

    void putLong256(@Nullable CharSequence hexString);

    void putLong256(@NotNull CharSequence hexString, int start, int end);

    void putLong256Utf8(@Nullable Utf8Sequence hexString);

    long putNullBin();

    long putNullStr();

    void putShort(short value);

    long putStr(CharSequence value);

    long putStr(char value);

    long putStr(CharSequence value, int pos, int len);

    long putStrUtf8(DirectUtf8Sequence value);

    /**
     * Appends UTF8 sequence bytes to the memory. The binary format is bytes
     * only. Length to be encoded elsewhere.
     *
     * @param value any utf8 sequence
     * @return offset at the start of the written sequence.
     */
    default long putVarchar(@Nullable Utf8Sequence value) {
        if (value != null) {
            return putVarchar(value, 0, value.size());
        }
        return getAppendOffset();
    }

    long putVarchar(@NotNull Utf8Sequence value, int lo, int hi);

    void skip(long bytes);

    void truncate();

    void zeroMem(int length);
}
