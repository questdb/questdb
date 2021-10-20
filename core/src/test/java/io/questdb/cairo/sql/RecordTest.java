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

package io.questdb.cairo.sql;

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
