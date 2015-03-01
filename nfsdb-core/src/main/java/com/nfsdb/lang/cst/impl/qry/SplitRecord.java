/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.lang.cst.impl.qry;

import com.nfsdb.io.sink.CharSink;

import java.io.InputStream;
import java.io.OutputStream;

public class SplitRecord extends AbstractRecord {
    private final int split;
    private Record a;
    private Record b;

    public SplitRecord(RecordMetadata metadata, int split) {
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
    public InputStream getBin(int col) {
        if (col < split) {
            return a.getBin(col);
        } else {
            return b == null ? null : b.getBin(col - split);
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
            return b == null ? 0L : b.getDate(col - split);
        }
    }

    @Override
    public double getDouble(int col) {
        if (col < split) {
            return a.getDouble(col);
        } else {
            return b == null ? 0d : b.getDouble(col - split);
        }
    }

    @Override
    public float getFloat(int col) {
        if (col < split) {
            return a.getFloat(col);
        } else {
            return b == null ? 0f : b.getFloat(col - split);
        }
    }

    @Override
    public int getInt(int col) {
        if (col < split) {
            return a.getInt(col);
        } else {
            return b == null ? 0 : b.getInt(col - split);
        }
    }

    @Override
    public long getLong(int col) {
        if (col < split) {
            return a.getLong(col);
        } else {
            return b == null ? 0L : b.getLong(col - split);
        }
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
    public String getSym(int col) {
        if (col < split) {
            return a.getSym(col);
        } else {
            return b == null ? null : b.getSym(col - split);
        }
    }

    public boolean hasB() {
        return b != null;
    }

    public void setA(Record a) {
        this.a = a;
    }

    public void setB(Record b) {
        this.b = b;
    }
}
