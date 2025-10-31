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

package io.questdb.cairo.vm;

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.FilesFacade;
import io.questdb.std.Long256;
import io.questdb.std.Long256Acceptor;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8SplitString;

public class NullMemoryCMR implements MemoryCMR {

    public static final NullMemoryCMR INSTANCE = new NullMemoryCMR();

    @Override
    public long addressHi() {
        return 0;
    }

    @Override
    public long addressOf(long offset) {
        return 0;
    }

    @Override
    public void changeSize(long dataSize) {
    }

    @Override
    public void close() {
    }

    @Override
    public long detachFdClose() {
        return -1;
    }

    @Override
    public void extend(long size) {
    }

    @Override
    public ArrayView getArray(long offset) {
        return null;
    }

    @Override
    public BinarySequence getBin(long offset) {
        return null;
    }

    @Override
    public long getBinLen(long offset) {
        return TableUtils.NULL_LEN;
    }

    @Override
    public boolean getBool(long offset) {
        return false;
    }

    @Override
    public byte getByte(long offset) {
        return 0;
    }

    @Override
    public char getChar(long offset) {
        return 0;
    }

    @Override
    public void getDecimal128(long offset, Decimal128 sink) {
        sink.ofRawNull();
    }

    @Override
    public short getDecimal16(long offset) {
        return Decimals.DECIMAL16_NULL;
    }

    @Override
    public void getDecimal256(long offset, Decimal256 sink) {
        sink.ofRawNull();
    }

    @Override
    public int getDecimal32(long offset) {
        return Decimals.DECIMAL32_NULL;
    }

    @Override
    public long getDecimal64(long offset) {
        return Decimals.DECIMAL64_NULL;
    }

    @Override
    public byte getDecimal8(long offset) {
        return Decimals.DECIMAL8_NULL;
    }

    @Override
    public DirectUtf8Sequence getDirectVarchar(long offset, int size, boolean ascii) {
        return null;
    }

    @Override
    public double getDouble(long offset) {
        return Double.NaN;
    }

    @Override
    public long getFd() {
        return -1;
    }

    @Override
    public FilesFacade getFilesFacade() {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(long offset) {
        return Float.NaN;
    }

    @Override
    public int getIPv4(long offset) {
        return Numbers.IPv4_NULL;
    }

    @Override
    public int getInt(long offset) {
        return Numbers.INT_NULL;
    }

    @Override
    public long getLong(long offset) {
        return Numbers.LONG_NULL;
    }

    public long getLong128Hi() {
        return Numbers.LONG_NULL;
    }

    public long getLong128Lo() {
        return Numbers.LONG_NULL;
    }

    @Override
    public void getLong256(long offset, Long256Acceptor sink) {
        sink.setAll(
                Long256Impl.NULL_LONG256.getLong0(),
                Long256Impl.NULL_LONG256.getLong1(),
                Long256Impl.NULL_LONG256.getLong2(),
                Long256Impl.NULL_LONG256.getLong3()
        );
    }

    @Override
    public void getLong256(long offset, CharSink<?> sink) {
    }

    @Override
    public Long256 getLong256A(long offset) {
        return Long256Impl.NULL_LONG256;
    }

    @Override
    public Long256 getLong256B(long offset) {
        return Long256Impl.NULL_LONG256;
    }

    @Override
    public long getPageAddress(int pageIndex) {
        return 0;
    }

    @Override
    public int getPageCount() {
        return 0;
    }

    @Override
    public long getPageSize() {
        return 0;
    }

    @Override
    public short getShort(long offset) {
        return 0;
    }

    @Override
    public Utf8SplitString getSplitVarcharA(long auxLo, long dataLo, long dataLim, int size, boolean ascii) {
        return null;
    }

    @Override
    public Utf8SplitString getSplitVarcharB(long auxLo, long dataLo, long dataLim, int size, boolean ascii) {
        return null;
    }

    @Override
    public CharSequence getStrA(long offset) {
        return null;
    }

    @Override
    public CharSequence getStrB(long offset) {
        return null;
    }

    @Override
    public int getStrLen(long offset) {
        return TableUtils.NULL_LEN;
    }

    @Override
    public boolean isDeleted() {
        return true;
    }

    @Override
    public boolean isMapped(long offset, long len) {
        return false;
    }

    @Override
    public void of(FilesFacade ff, LPSZ name, long extendSegmentSize, long size, int memoryTag, int opts, int madviseOpts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long offsetInPage(long offset) {
        return offset;
    }

    @Override
    public int pageIndex(long offset) {
        return 0;
    }

    @Override
    public long resize(long size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long size() {
        return 0;
    }
}
