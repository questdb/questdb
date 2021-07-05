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

import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class NullColumnTest {

    @Test
    public void close() {
        NullColumn.INSTANCE.close();
    }

    @Test
    public void isDeleted() {
        Assert.assertTrue(NullColumn.INSTANCE.isDeleted());
    }

    @Test
    public void getBin() {
        Assert.assertNull(NullColumn.INSTANCE.getBin(1234));
    }

    @Test
    public void getBinLen() {
        Assert.assertEquals(TableUtils.NULL_LEN, NullColumn.INSTANCE.getBinLen(1234));
    }

    @Test
    public void getBool() {
        Assert.assertFalse(NullColumn.INSTANCE.getBool(1234));
    }

    @Test
    public void getByte() {
        Assert.assertEquals(0, NullColumn.INSTANCE.getByte(1234));
    }

    @Test
    public void getDouble() {
        Assert.assertTrue(Double.isNaN(NullColumn.INSTANCE.getDouble(1234)));
    }

    @Test
    public void getFd() {
        Assert.assertEquals(-1, NullColumn.INSTANCE.getFd());
    }

    @Test
    public void getFloat() {
        Assert.assertTrue(Float.isNaN(NullColumn.INSTANCE.getFloat(123)));
    }

    @Test
    public void getInt() {
        Assert.assertEquals(Numbers.INT_NaN, NullColumn.INSTANCE.getInt(1234));
    }

    @Test
    public void getChar() {
        Assert.assertEquals(0, NullColumn.INSTANCE.getChar(1234));
    }

    @Test
    public void getLong() {
        Assert.assertEquals(Numbers.LONG_NaN, NullColumn.INSTANCE.getLong(1234));
    }

    @Test
    public void getShort() {
        Assert.assertEquals(0, NullColumn.INSTANCE.getShort(1234));
    }

    @Test
    public void getStr() {
        Assert.assertNull(NullColumn.INSTANCE.getStr(1234));
    }

    @Test
    public void getStrB() {
        Assert.assertNull(NullColumn.INSTANCE.getStr2(1234));
    }

    @Test
    public void getLong256A() {
        Assert.assertEquals(Long256Impl.NULL_LONG256, NullColumn.INSTANCE.getLong256A(1234));
    }

    @Test
    public void getLong256B() {
        Assert.assertEquals(Long256Impl.NULL_LONG256, NullColumn.INSTANCE.getLong256B(1234));
    }

    @Test
    public void getStrLen() {
        Assert.assertEquals(TableUtils.NULL_LEN, NullColumn.INSTANCE.getStrLen(1234));
    }

    @Test
    public void testGrow() {
        // this method does nothing. Make sure it doesn corrupt state of singleton and
        // doesn't throw exception
        NullColumn.INSTANCE.grow(100000);
    }

    @Test
    public void size() {
        Assert.assertEquals(0, NullColumn.INSTANCE.size());
    }

    @Test
    public void testDeleted() {
        Assert.assertTrue(NullColumn.INSTANCE.isDeleted());
    }
}