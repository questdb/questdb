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
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Long256;
import io.questdb.std.Long256Acceptor;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8SplitString;

import java.io.Closeable;

// readable
public interface MemoryR extends Closeable {

    long addressOf(long offset);

    @Override
    void close();

    void extend(long size);

    ArrayView getArray(long offset);

    BinarySequence getBin(long offset);

    long getBinLen(long offset);

    boolean getBool(long offset);

    byte getByte(long offset);

    char getChar(long offset);

    void getDecimal128(long offset, Decimal128 sink);

    short getDecimal16(long offset);

    void getDecimal256(long offset, Decimal256 sink);

    int getDecimal32(long offset);

    long getDecimal64(long offset);

    byte getDecimal8(long offset);

    default DirectUtf8Sequence getDirectVarchar(long offset, int size, boolean ascii) {
        throw new UnsupportedOperationException();
    }

    double getDouble(long offset);

    float getFloat(long offset);

    int getIPv4(long offset);

    int getInt(long offset);

    long getLong(long offset);

    void getLong256(long offset, CharSink<?> sink);

    default void getLong256(long offset, Long256Acceptor sink) {
        sink.fromAddress(addressOf(offset + Long.BYTES * 4) - Long.BYTES * 4);
    }

    Long256 getLong256A(long offset);

    Long256 getLong256B(long offset);

    long getPageAddress(int pageIndex);

    int getPageCount();

    long getPageSize();

    short getShort(long offset);

    default Utf8SplitString getSplitVarcharA(long auxLo, long dataLo, long dataLim, int size, boolean ascii) {
        throw new UnsupportedOperationException();
    }

    default Utf8SplitString getSplitVarcharB(long auxLo, long dataLo, long dataLim, int size, boolean ascii) {
        throw new UnsupportedOperationException();
    }

    CharSequence getStrA(long offset);

    CharSequence getStrB(long offset);

    int getStrLen(long offset);

    long offsetInPage(long offset);

    int pageIndex(long offset);

    long size();
}
