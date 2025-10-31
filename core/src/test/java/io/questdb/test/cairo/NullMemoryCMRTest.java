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

package io.questdb.test.cairo;

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class NullMemoryCMRTest {

    @Test
    public void close() {
        NullMemoryCMR.INSTANCE.close();
    }

    @Test
    public void getBin() {
        Assert.assertNull(NullMemoryCMR.INSTANCE.getBin(1234));
    }

    @Test
    public void getBinLen() {
        Assert.assertEquals(TableUtils.NULL_LEN, NullMemoryCMR.INSTANCE.getBinLen(1234));
    }

    @Test
    public void getBool() {
        Assert.assertFalse(NullMemoryCMR.INSTANCE.getBool(1234));
    }

    @Test
    public void getByte() {
        Assert.assertEquals(0, NullMemoryCMR.INSTANCE.getByte(1234));
    }

    @Test
    public void getChar() {
        Assert.assertEquals(0, NullMemoryCMR.INSTANCE.getChar(1234));
    }

    @Test
    public void getDecimal128() {
        var decimal128 = new Decimal128();
        NullMemoryCMR.INSTANCE.getDecimal128(1234, decimal128);
        Assert.assertTrue(decimal128.isNull());
    }

    @Test
    public void getDecimal16() {
        Assert.assertEquals(Decimals.DECIMAL16_NULL, NullMemoryCMR.INSTANCE.getDecimal16(1234));
    }

    @Test
    public void getDecimal256() {
        var decimal256 = new Decimal256();
        NullMemoryCMR.INSTANCE.getDecimal256(1234, decimal256);
        Assert.assertTrue(decimal256.isNull());
    }

    @Test
    public void getDecimal32() {
        Assert.assertEquals(Decimals.DECIMAL32_NULL, NullMemoryCMR.INSTANCE.getDecimal32(1234));
    }

    @Test
    public void getDecimal64() {
        Assert.assertEquals(Decimals.DECIMAL64_NULL, NullMemoryCMR.INSTANCE.getDecimal64(1234));
    }

    @Test
    public void getDecimal8() {
        Assert.assertEquals(Decimals.DECIMAL8_NULL, NullMemoryCMR.INSTANCE.getDecimal8(1234));
    }

    @Test
    public void getDouble() {
        Assert.assertTrue(Numbers.isNull(NullMemoryCMR.INSTANCE.getDouble(1234)));
    }

    @Test
    public void getFd() {
        Assert.assertEquals(-1, NullMemoryCMR.INSTANCE.getFd());
    }

    @Test
    public void getFloat() {
        Assert.assertTrue(Numbers.isNull(NullMemoryCMR.INSTANCE.getFloat(123)));
    }

    @Test
    public void getInt() {
        Assert.assertEquals(Numbers.INT_NULL, NullMemoryCMR.INSTANCE.getInt(1234));
    }

    @Test
    public void getLong() {
        Assert.assertEquals(Numbers.LONG_NULL, NullMemoryCMR.INSTANCE.getLong(1234));
    }

    @Test
    public void getLong256A() {
        Assert.assertEquals(Long256Impl.NULL_LONG256, NullMemoryCMR.INSTANCE.getLong256A(1234));
    }

    @Test
    public void getLong256B() {
        Assert.assertEquals(Long256Impl.NULL_LONG256, NullMemoryCMR.INSTANCE.getLong256B(1234));
    }

    @Test
    public void getShort() {
        Assert.assertEquals(0, NullMemoryCMR.INSTANCE.getShort(1234));
    }

    @Test
    public void getStr() {
        Assert.assertNull(NullMemoryCMR.INSTANCE.getStrA(1234));
    }

    @Test
    public void getStrB() {
        Assert.assertNull(NullMemoryCMR.INSTANCE.getStrB(1234));
    }

    @Test
    public void getStrLen() {
        Assert.assertEquals(TableUtils.NULL_LEN, NullMemoryCMR.INSTANCE.getStrLen(1234));
    }

    @Test
    public void isDeleted() {
        Assert.assertTrue(NullMemoryCMR.INSTANCE.isDeleted());
    }

    @Test
    public void size() {
        Assert.assertEquals(0, NullMemoryCMR.INSTANCE.size());
    }

    @Test
    public void testDeleted() {
        Assert.assertTrue(NullMemoryCMR.INSTANCE.isDeleted());
    }

    @Test
    public void testGrow() {
        // this method does nothing. Make sure it doesn't corrupt state of singleton and
        // doesn't throw exception
        NullMemoryCMR.INSTANCE.extend(100000);
    }
}