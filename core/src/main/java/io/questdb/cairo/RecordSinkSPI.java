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

package io.questdb.cairo;

import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;

public interface RecordSinkSPI {
    void putBin(BinarySequence value);

    void putBool(boolean value);

    void putByte(byte value);

    void putDate(long value);

    void putDouble(double value);

    void putFloat(float value);

    void putInt(int value);

    void putLong(long value);

    void putLong256(Long256 value);

    void putLong128BigEndian(long hi, long lo);

    void putShort(short value);

    void putChar(char value);

    void putStr(CharSequence value);

    void putStr(CharSequence value, int lo, int hi);

    default void putStrLowerCase(CharSequence value) {
        throw new UnsupportedOperationException();
    }

    default void putStrLowerCase(CharSequence value, int lo, int hi) {
        throw new UnsupportedOperationException();
    }

    void putRecord(Record value);

    void putTimestamp(long value);

    void skip(int bytes);
}
