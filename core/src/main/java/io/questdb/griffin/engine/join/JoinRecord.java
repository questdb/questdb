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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;

public class JoinRecord implements Record {
    private final int split;
    private Record master;
    protected Record slave;

    public JoinRecord(int split) {
        this.split = split;
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
    public int getInt(int col) {
        if (col < split) {
            return master.getInt(col);
        }
        return slave.getInt(col - split);
    }

    @Override
    public long getLong(int col) {
        if (col < split) {
            return master.getLong(col);
        }
        return slave.getLong(col - split);
    }

    @Override
    public void getLong256(int col, CharSink sink) {
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
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(int col) {
        if (col < split) {
            return master.getShort(col);
        }
        return slave.getShort(col - split);
    }

    @Override
    public CharSequence getStr(int col) {
        if (col < split) {
            return master.getStr(col);
        }
        return slave.getStr(col - split);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        if (col < split) {
            master.getStr(col, sink);
        } else {
            slave.getStr(col - split, sink);
        }
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
    public CharSequence getSym(int col) {
        if (col < split) {
            return master.getSym(col);
        }
        return slave.getSym(col - split);
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

    void of(Record master, Record slave) {
        this.master = master;
        this.slave = slave;
    }
}
