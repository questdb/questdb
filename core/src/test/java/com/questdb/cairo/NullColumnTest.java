/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class NullColumnTest {

    @Test
    public void close() {
        NullColumn.INSTANCE.close();
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
    public void getStr2() {
        Assert.assertNull(NullColumn.INSTANCE.getStr2(1234));
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
    public void testDeleted() {
        Assert.assertTrue(NullColumn.INSTANCE.isDeleted());
    }
}