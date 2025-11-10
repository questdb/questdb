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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;

public class JoinRecord implements Record {
    protected final int split;
    protected Record master;
    protected Record slave;

    public JoinRecord(int split) {
        this.split = split;
    }

    @Override
    public ArrayView getArray(int col, int columnType) {
        if (col < split) {
            return master.getArray(col, columnType);
        }
        return slave.getArray(col - split, columnType);
    }

    @Override
    public BinarySequence getBin(int col) {
        if (col < split) {
            return master.getBin(col);
        }
        return slave.getBin(col - split);
    }

    @Override
    public long getBinLen(int col) {
        if (col < split) {
            return master.getBinLen(col);
        }
        return slave.getBinLen(col - split);
    }

    @Override
    public boolean getBool(int col) {
        if (col < split) {
            return master.getBool(col);
        }
        return slave.getBool(col - split);
    }

    @Override
    public byte getByte(int col) {
        if (col < split) {
            return master.getByte(col);
        }
        return slave.getByte(col - split);
    }

    @Override
    public char getChar(int col) {
        if (col < split) {
            return master.getChar(col);
        }
        return slave.getChar(col - split);
    }

    @Override
    public long getDate(int col) {
        if (col < split) {
            return master.getDate(col);
        }
        return slave.getDate(col - split);
    }

    @Override
    public void getDecimal128(int col, Decimal128 sink) {
        if (col < split) {
            master.getDecimal128(col, sink);
        } else {
            slave.getDecimal128(col - split, sink);
        }
    }

    @Override
    public short getDecimal16(int col) {
        if (col < split) {
            return master.getDecimal16(col);
        }
        return slave.getDecimal16(col - split);
    }

    @Override
    public void getDecimal256(int col, Decimal256 sink) {
        if (col < split) {
            master.getDecimal256(col, sink);
        } else {
            slave.getDecimal256(col - split, sink);
        }
    }

    @Override
    public int getDecimal32(int col) {
        if (col < split) {
            return master.getDecimal32(col);
        }
        return slave.getDecimal32(col - split);
    }

    @Override
    public long getDecimal64(int col) {
        if (col < split) {
            return master.getDecimal64(col);
        }
        return slave.getDecimal64(col - split);
    }

    @Override
    public byte getDecimal8(int col) {
        if (col < split) {
            return master.getDecimal8(col);
        }
        return slave.getDecimal8(col - split);
    }

    @Override
    public double getDouble(int col) {
        if (col < split) {
            return master.getDouble(col);
        }
        return slave.getDouble(col - split);
    }

    @Override
    public float getFloat(int col) {
        if (col < split) {
            return master.getFloat(col);
        }
        return slave.getFloat(col - split);
    }

    @Override
    public byte getGeoByte(int col) {
        if (col < split) {
            return master.getGeoByte(col);
        }
        return slave.getGeoByte(col - split);
    }

    @Override
    public int getGeoInt(int col) {
        if (col < split) {
            return master.getGeoInt(col);
        }
        return slave.getGeoInt(col - split);
    }

    @Override
    public long getGeoLong(int col) {
        if (col < split) {
            return master.getGeoLong(col);
        }
        return slave.getGeoLong(col - split);
    }

    @Override
    public short getGeoShort(int col) {
        if (col < split) {
            return master.getGeoShort(col);
        }
        return slave.getGeoShort(col - split);
    }

    @Override
    public int getIPv4(int col) {
        if (col < split) {
            return master.getIPv4(col);
        }
        return slave.getIPv4(col - split);
    }

    @Override
    public int getInt(int col) {
        if (col < split) {
            return master.getInt(col);
        }
        return slave.getInt(col - split);
    }

    @Override
    public Interval getInterval(int col) {
        if (col < split) {
            return master.getInterval(col);
        }
        return slave.getInterval(col - split);
    }

    @Override
    public long getLong(int col) {
        if (col < split) {
            return master.getLong(col);
        }
        return slave.getLong(col - split);
    }

    @Override
    public long getLong128Hi(int col) {
        if (col < split) {
            return master.getLong128Hi(col);
        }
        return slave.getLong128Hi(col - split);
    }

    @Override
    public long getLong128Lo(int col) {
        if (col < split) {
            return master.getLong128Lo(col);
        }
        return slave.getLong128Lo(col - split);
    }

    @Override
    public void getLong256(int col, CharSink<?> sink) {
        if (col < split) {
            master.getLong256(col, sink);
        } else {
            slave.getLong256(col - split, sink);
        }
    }

    @Override
    public Long256 getLong256A(int col) {
        if (col < split) {
            return master.getLong256A(col);
        }
        return slave.getLong256A(col - split);
    }

    @Override
    public Long256 getLong256B(int col) {
        if (col < split) {
            return master.getLong256B(col);
        }
        return slave.getLong256B(col - split);
    }

    @Override
    public Record getRecord(int col) {
        if (col < split) {
            return master.getRecord(col);
        }
        return slave.getRecord(col - split);
    }

    @Override
    public long getRowId() {
        return master.getRowId();
    }

    @Override
    public short getShort(int col) {
        if (col < split) {
            return master.getShort(col);
        }
        return slave.getShort(col - split);
    }

    @Override
    public CharSequence getStrA(int col) {
        if (col < split) {
            return master.getStrA(col);
        }
        return slave.getStrA(col - split);
    }

    @Override
    public CharSequence getStrB(int col) {
        if (col < split) {
            return master.getStrB(col);
        }
        return slave.getStrB(col - split);
    }

    @Override
    public int getStrLen(int col) {
        if (col < split) {
            return master.getStrLen(col);
        }
        return slave.getStrLen(col - split);
    }

    @Override
    public CharSequence getSymA(int col) {
        if (col < split) {
            return master.getSymA(col);
        }
        return slave.getSymA(col - split);
    }

    @Override
    public CharSequence getSymB(int col) {
        if (col < split) {
            return master.getSymB(col);
        }
        return slave.getSymB(col - split);
    }

    @Override
    public long getTimestamp(int col) {
        if (col < split) {
            return master.getTimestamp(col);
        }
        return slave.getTimestamp(col - split);
    }

    @Override
    public long getUpdateRowId() {
        return master.getUpdateRowId();
    }

    @Override
    public Utf8Sequence getVarcharA(int col) {
        if (col < split) {
            return master.getVarcharA(col);
        }
        return slave.getVarcharA(col - split);
    }

    @Override
    public Utf8Sequence getVarcharB(int col) {
        if (col < split) {
            return master.getVarcharB(col);
        }
        return slave.getVarcharB(col - split);
    }

    @Override
    public int getVarcharSize(int col) {
        if (col < split) {
            return master.getVarcharSize(col);
        }
        return slave.getVarcharSize(col - split);
    }

    public void of(Record master, Record slave) {
        this.master = master;
        this.slave = slave;
    }
}
