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

package io.questdb.test.griffin;

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal256;
import io.questdb.std.Long256;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

public class RowAsserter implements TableWriter.Row {
    @Override
    public void append() {
        Assert.fail("Unexpected call to append");
    }

    @Override
    public void cancel() {
        Assert.fail("Unexpected call to cancel");
    }

    @Override
    public void putArray(int columnIndex, @NotNull ArrayView array) {
        Assert.fail("Unexpected call to putArray");
    }

    @Override
    public void putBin(int columnIndex, long address, long len) {
        Assert.fail("Unexpected call to putBin");
    }

    @Override
    public void putBin(int columnIndex, BinarySequence sequence) {
        Assert.fail("Unexpected call to putBin");
    }

    @Override
    public void putBool(int columnIndex, boolean value) {
        Assert.fail("Unexpected call to putBool");
    }

    @Override
    public void putByte(int columnIndex, byte value) {
        Assert.fail("Unexpected call to putByte");
    }

    @Override
    public void putChar(int columnIndex, char value) {
        Assert.fail("Unexpected call to putChar");
    }

    @Override
    public void putDate(int columnIndex, long value) {
        Assert.fail("Unexpected call to putDate");
    }

    @Override
    public void putDecimal(int columnIndex, Decimal256 value) {
        Assert.fail("Unexpected call to putDecimal");
    }

    @Override
    public void putDecimal128(int columnIndex, long high, long low) {
        Assert.fail("Unexpected call to putDecimal128");
    }

    @Override
    public void putDecimal256(int columnIndex, long hh, long hl, long lh, long ll) {
        Assert.fail("Unexpected call to putDecimal256");
    }

    @Override
    public void putDecimalStr(int columnIndex, CharSequence decimalValue) {
        Assert.fail("Unexpected call to putDecimalStr");
    }

    @Override
    public void putDouble(int columnIndex, double value) {
        Assert.fail("Unexpected call to putDouble");
    }

    @Override
    public void putFloat(int columnIndex, float value) {
        Assert.fail("Unexpected call to putFloat");
    }

    @Override
    public void putGeoHash(int columnIndex, long value) {
        Assert.fail("Unexpected call to putGeoHash");
    }

    @Override
    public void putGeoHashDeg(int columnIndex, double lat, double lon) {
        Assert.fail("Unexpected call to putGeoHashDeg");
    }

    @Override
    public void putGeoStr(int columnIndex, CharSequence value) {
        Assert.fail("Unexpected call to putGeoStr");
    }

    @Override
    public void putGeoVarchar(int columnIndex, Utf8Sequence value) {
        Assert.fail("Unexpected call to putGeoVarchar");
    }

    @Override
    public void putIPv4(int columnIndex, int value) {
        Assert.fail("Unexpected call to putIPv4");
    }

    @Override
    public void putInt(int columnIndex, int value) {
        Assert.fail("Unexpected call to putInt");
    }

    @Override
    public void putLong(int columnIndex, long value) {
        Assert.fail("Unexpected call to putLong");
    }

    @Override
    public void putLong128(int columnIndex, long lo, long hi) {
        Assert.fail("Unexpected call to putLong128");
    }

    @Override
    public void putLong256(int columnIndex, long l0, long l1, long l2, long l3) {
        Assert.fail("Unexpected call to putLong256");
    }

    @Override
    public void putLong256(int columnIndex, Long256 value) {
        Assert.fail("Unexpected call to putLong256");
    }

    @Override
    public void putLong256(int columnIndex, CharSequence hexString) {
        Assert.fail("Unexpected call to putLong256");
    }

    @Override
    public void putLong256(int columnIndex, @NotNull CharSequence hexString, int start, int end) {
        Assert.fail("Unexpected call to putLong256");
    }

    @Override
    public void putLong256Utf8(int columnIndex, DirectUtf8Sequence hexString) {
        Assert.fail("Unexpected call to putLong256Utf8");
    }

    @Override
    public void putShort(int columnIndex, short value) {
        Assert.fail("Unexpected call to putShort");
    }

    @Override
    public void putStr(int columnIndex, CharSequence value) {
        Assert.fail("Unexpected call to putStr");
    }

    @Override
    public void putStr(int columnIndex, char value) {
        Assert.fail("Unexpected call to putStr");
    }

    @Override
    public void putStr(int columnIndex, CharSequence value, int pos, int len) {
        Assert.fail("Unexpected call to putStr");
    }

    @Override
    public void putStrUtf8(int columnIndex, DirectUtf8Sequence value) {
        Assert.fail("Unexpected call to putStrUtf8");
    }

    @Override
    public void putSym(int columnIndex, CharSequence value) {
        Assert.fail("Unexpected call to putSym");
    }

    @Override
    public void putSym(int columnIndex, char value) {
        Assert.fail("Unexpected call to putSym");
    }

    @Override
    public void putSymIndex(int columnIndex, int key) {
        Assert.fail("Unexpected call to putSymIndex");
    }

    @Override
    public void putSymUtf8(int columnIndex, DirectUtf8Sequence value) {
        Assert.fail("Unexpected call to putSymUtf8");
    }

    @Override
    public void putTimestamp(int columnIndex, long value) {
        Assert.fail("Unexpected call to putTimestamp");
    }

    @Override
    public void putUuid(int columnIndex, CharSequence uuid) {
        Assert.fail("Unexpected call to putUuid");
    }

    @Override
    public void putUuidUtf8(int columnIndex, Utf8Sequence uuid) {
        Assert.fail("Unexpected call to putUuidUtf8");
    }

    @Override
    public void putVarchar(int columnIndex, char value) {
        Assert.fail("Unexpected call to putVarchar");
    }

    @Override
    public void putVarchar(int columnIndex, Utf8Sequence value) {
        Assert.fail("Unexpected call to putVarchar");
    }
}
