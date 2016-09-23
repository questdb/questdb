/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.ql.impl.join;

import com.questdb.misc.Numbers;
import com.questdb.ql.AbstractRecord;
import com.questdb.ql.Record;
import com.questdb.std.CharSink;
import com.questdb.std.DirectInputStream;
import com.questdb.std.ObjList;

import java.io.OutputStream;

class SplitRecord extends AbstractRecord {
    private final int countA;
    private final int countB;
    private final ObjList<Record> records = new ObjList<>();
    private Record a;
    private Record b;

    SplitRecord(int countA, int countB, Record recordA, Record recordB) {
        this.countA = countA;
        this.countB = countB;
    }

    @Override
    public byte get(int col) {
        if (col < countA) {
            return a.get(col);
        } else {
            return b == null ? 0 : b.get(col - countA);
        }
    }

    @Override
    public void getBin(int col, OutputStream s) {
        if (col < countA) {
            a.getBin(col, s);
        } else if (b != null) {
            b.getBin(col - countA, s);
        }
    }

    @Override
    public DirectInputStream getBin(int col) {
        if (col < countA) {
            return a.getBin(col);
        } else {
            return b == null ? null : b.getBin(col - countA);
        }
    }

    @Override
    public long getBinLen(int col) {
        if (col < countA) {
            return a.getBinLen(col);
        } else if (b != null) {
            return b.getBinLen(col - countA);
        } else {
            return -1;
        }
    }

    @Override
    public boolean getBool(int col) {
        if (col < countA) {
            return a.getBool(col);
        } else {
            return b != null && b.getBool(col - countA);
        }
    }

    @Override
    public long getDate(int col) {
        if (col < countA) {
            return a.getDate(col);
        } else {
            return b == null ? Numbers.LONG_NaN : b.getDate(col - countA);
        }
    }

    @Override
    public double getDouble(int col) {
        if (col < countA) {
            return a.getDouble(col);
        } else {
            return b == null ? Double.NaN : b.getDouble(col - countA);
        }
    }

    @Override
    public float getFloat(int col) {
        if (col < countA) {
            return a.getFloat(col);
        } else {
            return b == null ? Float.NaN : b.getFloat(col - countA);
        }
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        if (col < countA) {
            return a.getFlyweightStr(col);
        } else {
            return b == null ? null : b.getFlyweightStr(col - countA);
        }
    }

    @Override
    public CharSequence getFlyweightStrB(int col) {
        if (col < countA) {
            return a.getFlyweightStrB(col);
        } else {
            return b == null ? null : b.getFlyweightStrB(col - countA);
        }
    }

    @Override
    public int getInt(int col) {
        if (col < countA) {
            return a.getInt(col);
        } else {
            return b == null ? Numbers.INT_NaN : b.getInt(col - countA);
        }
    }

    @Override
    public long getLong(int col) {
        if (col < countA) {
            return a.getLong(col);
        } else {
            return b == null ? Numbers.LONG_NaN : b.getLong(col - countA);
        }
    }

    @Override
    public long getRowId() {
        return -1;
    }

    @Override
    public short getShort(int col) {
        if (col < countA) {
            return a.getShort(col);
        } else {
            return b == null ? 0 : b.getShort(col - countA);
        }
    }

    @Override
    public CharSequence getStr(int col) {
        if (col < countA) {
            return a.getStr(col);
        } else {
            return b == null ? null : b.getStr(col - countA);
        }
    }

    @Override
    public void getStr(int col, CharSink sink) {
        if (col < countA) {
            a.getStr(col, sink);
        } else if (b != null) {
            b.getStr(col - countA, sink);
        }
    }

    @Override
    public int getStrLen(int col) {
        if (col < countA) {
            return a.getStrLen(col);
        } else {
            return b == null ? -1 : b.getStrLen(col - countA);
        }
    }

    @Override
    public String getSym(int col) {
        if (col < countA) {
            return a.getSym(col);
        } else {
            return b == null ? null : b.getSym(col - countA);
        }
    }

    public void setA(Record a) {
        this.a = a;
    }

    public void setB(Record b) {
        this.b = b;
    }
}
