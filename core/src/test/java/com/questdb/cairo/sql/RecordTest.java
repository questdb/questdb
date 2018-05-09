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

package com.questdb.cairo.sql;

import org.junit.Test;

public class RecordTest {
    private final static MyRecord RECORD = new MyRecord();

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBin() {
        RECORD.getBin(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBinLen() {
        RECORD.getBinLen(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBool() {
        RECORD.getBool(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetByte() {
        RECORD.getByte(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDate() {
        RECORD.getDate(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDouble() {
        RECORD.getDouble(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetFloat() {
        RECORD.getFloat(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetInt() {
        RECORD.getInt(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong() {
        RECORD.getLong(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetRowId() {
        RECORD.getRowId();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetShort() {
        RECORD.getShort(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetStr() {
        RECORD.getStr(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetStrB() {
        RECORD.getStrB(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetStrLen() {
        RECORD.getStrLen(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetSym() {
        RECORD.getSym(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetTimestamp() {
        RECORD.getTimestamp(0);
    }

    private static class MyRecord implements Record {
    }
}
