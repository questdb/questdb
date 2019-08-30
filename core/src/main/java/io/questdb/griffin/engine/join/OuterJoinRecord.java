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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;

public class OuterJoinRecord implements Record {
    private final int split;
    private final Record nullRecord;
    private Record master;
    private Record slave;
    private Record activeSlave;

    public OuterJoinRecord(int split, Record nullRecord) {
        this.split = split;
        this.nullRecord = nullRecord;
    }

    @Override
    public BinarySequence getBin(int col) {
        if (col < split) {
            return master.getBin(col);
        }
        return activeSlave.getBin(col - split);
    }

    @Override
    public long getBinLen(int col) {
        if (col < split) {
            return master.getBinLen(col);
        }
        return activeSlave.getBinLen(col - split);
    }

    @Override
    public boolean getBool(int col) {
        if (col < split) {
            return master.getBool(col);
        }
        return activeSlave.getBool(col - split);
    }

    @Override
    public byte getByte(int col) {
        if (col < split) {
            return master.getByte(col);
        }
        return activeSlave.getByte(col - split);
    }

    @Override
    public long getDate(int col) {
        if (col < split) {
            return master.getDate(col);
        }
        return activeSlave.getDate(col - split);
    }

    @Override
    public double getDouble(int col) {
        if (col < split) {
            return master.getDouble(col);
        }
        return activeSlave.getDouble(col - split);
    }

    @Override
    public float getFloat(int col) {
        if (col < split) {
            return master.getFloat(col);
        }
        return activeSlave.getFloat(col - split);
    }

    @Override
    public int getInt(int col) {
        if (col < split) {
            return master.getInt(col);
        }
        return activeSlave.getInt(col - split);
    }

    @Override
    public char getChar(int col) {
        if (col < split) {
            return master.getChar(col);
        }
        return activeSlave.getChar(col - split);
    }

    @Override
    public void getLong256(int col, CharSink sink) {
        if (col < split) {
            master.getLong256(col, sink);
        } else {
            activeSlave.getLong256(col - split, sink);
        }
    }

    @Override
    public Long256 getLong256A(int col) {
        if (col < split) {
            return master.getLong256A(col);
        }
        return activeSlave.getLong256A(col - split);
    }

    @Override
    public Long256 getLong256B(int col) {
        if (col < split) {
            return master.getLong256B(col);
        }
        return activeSlave.getLong256B(col - split);
    }

    @Override
    public long getLong(int col) {
        if (col < split) {
            return master.getLong(col);
        }
        return activeSlave.getLong(col - split);
    }

    @Override
    public long getRowId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(int col) {
        if (col < split) {
            return master.getShort(col);
        }
        return activeSlave.getShort(col - split);
    }

    @Override
    public CharSequence getStr(int col) {
        if (col < split) {
            return master.getStr(col);
        }
        return activeSlave.getStr(col - split);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        if (col < split) {
            master.getStr(col, sink);
        } else {
            activeSlave.getStr(col - split, sink);
        }
    }

    @Override
    public CharSequence getStrB(int col) {
        if (col < split) {
            return master.getStrB(col);
        }
        return activeSlave.getStrB(col - split);
    }

    @Override
    public int getStrLen(int col) {
        if (col < split) {
            return master.getStrLen(col);
        }
        return activeSlave.getStrLen(col - split);
    }

    @Override
    public CharSequence getSym(int col) {
        if (col < split) {
            return master.getSym(col);
        }
        return activeSlave.getSym(col - split);
    }

    @Override
    public long getTimestamp(int col) {
        if (col < split) {
            return master.getTimestamp(col);
        }
        return activeSlave.getTimestamp(col - split);
    }

    void hasSlave(boolean value) {
        if (value) {
            if (activeSlave != slave) {
                activeSlave = slave;
            }
        } else {
            if (activeSlave != nullRecord) {
                activeSlave = nullRecord;
            }
        }
    }

    void of(Record master, Record slave) {
        this.master = master;
        this.slave = slave;
        this.activeSlave = null;
    }
}
