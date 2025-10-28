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

package io.questdb.cairo;

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.Misc;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;

public interface RecordSinkSPI {
    void putArray(ArrayView view);

    void putBin(BinarySequence value);

    void putBool(boolean value);

    void putByte(byte value);

    void putChar(char value);

    void putDate(long value);

    void putDecimal128(Decimal128 value);

    void putDecimal256(Decimal256 value);

    void putDouble(double value);

    void putFloat(float value);

    void putIPv4(int value);

    void putInt(int value);

    // Used in RecordSinkFactory
    @SuppressWarnings("unused")
    void putInterval(Interval interval);

    void putLong(long value);

    void putLong128(long lo, long hi);

    void putLong256(Long256 value);

    void putLong256(long l0, long l1, long l2, long l3);

    // Used in RecordSinkFactory
    @SuppressWarnings("unused")
    void putRecord(Record value);

    void putShort(short value);

    void putStr(CharSequence value);

    void putStr(CharSequence value, int lo, int hi);

    default void putStrLowerCase(CharSequence value) {
        throw new UnsupportedOperationException();
    }

    default void putStrLowerCase(CharSequence value, int lo, int hi) {
        throw new UnsupportedOperationException();
    }

    void putTimestamp(long value);

    void putVarchar(Utf8Sequence value);

    default void putVarchar(CharSequence value) {
        if (value == null) {
            putVarchar((Utf8Sequence) null);
        } else {
            Utf8StringSink sink = Misc.getThreadLocalUtf8Sink();
            sink.put(value);
            putVarchar(sink);
        }
    }

    void skip(int bytes);
}
