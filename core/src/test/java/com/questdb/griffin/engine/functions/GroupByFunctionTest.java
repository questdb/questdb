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

package com.questdb.griffin.engine.functions;

import com.questdb.cairo.ArrayColumnTypes;
import com.questdb.cairo.map.MapValue;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.std.BinarySequence;
import com.questdb.std.str.CharSink;
import org.junit.Test;

public class GroupByFunctionTest {
    private static final GroupByFunction function = new GroupByFunction() {
        @Override
        public void computeFirst(MapValue mapValue, Record record) {
        }

        @Override
        public void computeNext(MapValue mapValue, Record record) {
        }

        @Override
        public void pushValueTypes(ArrayColumnTypes columnTypes) {
        }

        @Override
        public void setNull(MapValue mapValue) {
        }

        @Override
        public BinarySequence getBin(Record rec) {
            return null;
        }

        @Override
        public long getBinLen(Record rec) {
            return 0;
        }

        @Override
        public boolean getBool(Record rec) {
            return false;
        }

        @Override
        public byte getByte(Record rec) {
            return 0;
        }

        @Override
        public long getDate(Record rec) {
            return 0;
        }

        @Override
        public double getDouble(Record rec) {
            return 0;
        }

        @Override
        public float getFloat(Record rec) {
            return 0;
        }

        @Override
        public int getInt(Record rec) {
            return 0;
        }

        @Override
        public long getLong(Record rec) {
            return 0;
        }

        @Override
        public RecordMetadata getMetadata() {
            return null;
        }

        @Override
        public int getPosition() {
            return 0;
        }

        @Override
        public RecordCursorFactory getRecordCursorFactory() {
            return null;
        }

        @Override
        public short getShort(Record rec) {
            return 0;
        }

        @Override
        public CharSequence getStr(Record rec) {
            return null;
        }

        @Override
        public void getStr(Record rec, CharSink sink) {
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return null;
        }

        @Override
        public int getStrLen(Record rec) {
            return 0;
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            return null;
        }

        @Override
        public long getTimestamp(Record rec) {
            return 0;
        }

        @Override
        public int getType() {
            return 0;
        }
    };

    @Test(expected = UnsupportedOperationException.class)
    public void testSetByte() {
        function.setByte(null, (byte) 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetDate() {
        function.setDate(null, 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetDouble() {
        function.setDouble(null, 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetFloat() {
        function.setFloat(null, 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetInt() {
        function.setInt(null, 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetLong() {
        function.setLong(null, 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetShort() {
        function.setShort(null, (short) 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetTimestamp() {
        function.setTimestamp(null, 0);
    }
}