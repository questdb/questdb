/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.cutlass.text.types;

import com.questdb.cairo.ColumnType;
import org.junit.Assert;
import org.junit.Test;

public class UnsupportedProbeTest {
    @Test(expected = UnsupportedOperationException.class)
    public void testBadDate() {
        BadDateAdapter.INSTANCE.probe("xyz");
    }

    @Test
    public void testBadDateType() {
        Assert.assertEquals(ColumnType.DATE, BadDateAdapter.INSTANCE.getType());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBadTimestamp() {
        BadTimestampAdapter.INSTANCE.probe("xyz");
    }

    @Test
    public void testBadTimestampType() {
        Assert.assertEquals(ColumnType.TIMESTAMP, BadTimestampAdapter.INSTANCE.getType());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testByte() {
        ByteAdapter.INSTANCE.probe("xyz");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testFloat() {
        FloatAdapter.INSTANCE.probe("xyz");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testShort() {
        ShortAdapter.INSTANCE.probe("xyz");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testString() {
        new StringAdapter(null).probe("xyz");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSymbol() {
        new SymbolAdapter(null).probe("xyz");
    }
}