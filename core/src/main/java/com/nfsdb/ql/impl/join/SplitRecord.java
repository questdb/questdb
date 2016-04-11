/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.impl.join;

import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.misc.Numbers;
import com.nfsdb.ql.AbstractRecord;
import com.nfsdb.ql.Record;
import com.nfsdb.std.DirectInputStream;

import java.io.OutputStream;

class SplitRecord extends AbstractRecord {
    private final int split;
    private Record a;
    private Record b;

    SplitRecord(RecordMetadata metadata, int split) {
        super(metadata);
        this.split = split;
    }

    @Override
    public byte get(int col) {
        if (col < split) {
            return a.get(col);
        } else {
            return b == null ? 0 : b.get(col - split);
        }
    }

    @Override
    public void getBin(int col, OutputStream s) {
        if (col < split) {
            a.getBin(col, s);
        } else if (b != null) {
            b.getBin(col - split, s);
        }
    }

    @Override
    public DirectInputStream getBin(int col) {
        if (col < split) {
            return a.getBin(col);
        } else {
            return b == null ? null : b.getBin(col - split);
        }
    }

    @Override
    public long getBinLen(int col) {
        if (col < split) {
            return a.getBinLen(col);
        } else if (b != null) {
            return b.getBinLen(col - split);
        } else {
            return -1;
        }
    }

    @Override
    public boolean getBool(int col) {
        if (col < split) {
            return a.getBool(col);
        } else {
            return b != null && b.getBool(col - split);
        }
    }

    @Override
    public long getDate(int col) {
        if (col < split) {
            return a.getDate(col);
        } else {
            return b == null ? Numbers.LONG_NaN : b.getDate(col - split);
        }
    }

    @Override
    public double getDouble(int col) {
        if (col < split) {
            return a.getDouble(col);
        } else {
            return b == null ? Double.NaN : b.getDouble(col - split);
        }
    }

    @Override
    public float getFloat(int col) {
        if (col < split) {
            return a.getFloat(col);
        } else {
            return b == null ? Float.NaN : b.getFloat(col - split);
        }
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        if (col < split) {
            return a.getFlyweightStr(col);
        } else {
            return b == null ? null : b.getFlyweightStr(col - split);
        }
    }

    @Override
    public int getInt(int col) {
        if (col < split) {
            return a.getInt(col);
        } else {
            return b == null ? Numbers.INT_NaN : b.getInt(col - split);
        }
    }

    @Override
    public long getLong(int col) {
        if (col < split) {
            return a.getLong(col);
        } else {
            return b == null ? Numbers.LONG_NaN : b.getLong(col - split);
        }
    }

    @Override
    public long getRowId() {
        return -1;
    }

    @Override
    public short getShort(int col) {
        if (col < split) {
            return a.getShort(col);
        } else {
            return b == null ? 0 : b.getShort(col - split);
        }
    }

    @Override
    public CharSequence getStr(int col) {
        if (col < split) {
            return a.getStr(col);
        } else {
            return b == null ? null : b.getStr(col - split);
        }
    }

    @Override
    public void getStr(int col, CharSink sink) {
        if (col < split) {
            a.getStr(col, sink);
        } else if (b != null) {
            b.getStr(col - split, sink);
        }
    }

    @Override
    public int getStrLen(int col) {
        if (col < split) {
            return a.getStrLen(col);
        } else {
            return b == null ? -1 : b.getStrLen(col - split);
        }
    }

    @Override
    public String getSym(int col) {
        if (col < split) {
            return a.getSym(col);
        } else {
            return b == null ? null : b.getSym(col - split);
        }
    }

    public void setA(Record a) {
        this.a = a;
    }

    public void setB(Record b) {
        this.b = b;
    }
}
