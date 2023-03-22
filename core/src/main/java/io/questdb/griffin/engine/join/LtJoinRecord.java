/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.ColumnFilter;
import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;

public class LtJoinRecord implements Record {
    private final int masterSlaveSplit;
    private final Record nullRecord;
    private final int slaveWrappedOverMaster;
    protected Record slave;
    private Record flappingSlave;
    private Record master;
    private ColumnFilter masterTableKeyColumns;

    public LtJoinRecord(int masterSlaveSplit, Record nullRecord, ColumnFilter masterTableKeyColumns, int slaveWrappedOverMaster) {
        this.masterSlaveSplit = masterSlaveSplit;
        this.nullRecord = nullRecord;
        this.masterTableKeyColumns = masterTableKeyColumns;
        this.slaveWrappedOverMaster = slaveWrappedOverMaster;
    }

    @Override
    public BinarySequence getBin(int col) {
        if (col < masterSlaveSplit) {
            return master.getBin(col);
        }
        return slave.getBin(col - masterSlaveSplit);
    }

    @Override
    public long getBinLen(int col) {
        if (col < masterSlaveSplit) {
            return master.getBinLen(col);
        }
        return slave.getBinLen(col - masterSlaveSplit);
    }

    @Override
    public boolean getBool(int col) {
        if (col < masterSlaveSplit) {
            return master.getBool(col);
        }
        return slave.getBool(col - masterSlaveSplit);
    }

    @Override
    public byte getByte(int col) {
        if (col < masterSlaveSplit) {
            return master.getByte(col);
        }
        return slave.getByte(col - masterSlaveSplit);
    }

    @Override
    public char getChar(int col) {
        if (col < masterSlaveSplit) {
            return master.getChar(col);
        }
        return slave.getChar(col - masterSlaveSplit);
    }

    @Override
    public long getDate(int col) {
        if (col < masterSlaveSplit) {
            return master.getDate(col);
        }
        return slave.getDate(col - masterSlaveSplit);
    }

    @Override
    public double getDouble(int col) {
        if (col < masterSlaveSplit) {
            return master.getDouble(col);
        }
        return slave.getDouble(col - masterSlaveSplit);
    }

    @Override
    public float getFloat(int col) {
        if (col < masterSlaveSplit) {
            return master.getFloat(col);
        }
        return slave.getFloat(col - masterSlaveSplit);
    }

    @Override
    public byte getGeoByte(int col) {
        if (col < masterSlaveSplit) {
            return master.getGeoByte(col);
        }
        return slave.getGeoByte(col - masterSlaveSplit);
    }

    @Override
    public int getGeoInt(int col) {
        if (col < masterSlaveSplit) {
            return master.getGeoInt(col);
        }
        return slave.getGeoInt(col - masterSlaveSplit);
    }

    @Override
    public long getGeoLong(int col) {
        if (col < masterSlaveSplit) {
            return master.getGeoLong(col);
        }
        return slave.getGeoLong(col - masterSlaveSplit);
    }

    @Override
    public short getGeoShort(int col) {
        if (col < masterSlaveSplit) {
            return master.getGeoShort(col);
        }
        return slave.getGeoShort(col - masterSlaveSplit);
    }

    @Override
    public int getInt(int col) {
        if (col < masterSlaveSplit) {
            return master.getInt(col);
        }
        int slaveCol = col - masterSlaveSplit;
        if (slaveCol >= slaveWrappedOverMaster && slave != nullRecord) {
            slaveCol -= slaveWrappedOverMaster;
            int masterCol = masterTableKeyColumns.getColumnIndexFactored(slaveCol);
            return master.getInt(masterCol);
        }
        return slave.getInt(slaveCol);
    }

    @Override
    public long getLong(int col) {
        if (col < masterSlaveSplit) {
            return master.getLong(col);
        }
        return slave.getLong(col - masterSlaveSplit);
    }

    @Override
    public long getLong128Hi(int col) {
        if (col < masterSlaveSplit) {
            return master.getLong128Hi(col);
        }
        return slave.getLong128Hi(col - masterSlaveSplit);
    }

    @Override
    public long getLong128Lo(int col) {
        if (col < masterSlaveSplit) {
            return master.getLong128Lo(col);
        }
        return slave.getLong128Lo(col - masterSlaveSplit);
    }

    @Override
    public void getLong256(int col, CharSink sink) {
        if (col < masterSlaveSplit) {
            master.getLong256(col, sink);
        } else {
            slave.getLong256(col - masterSlaveSplit, sink);
        }
    }

    @Override
    public Long256 getLong256A(int col) {
        if (col < masterSlaveSplit) {
            return master.getLong256A(col);
        }
        return slave.getLong256A(col - masterSlaveSplit);
    }

    @Override
    public Long256 getLong256B(int col) {
        if (col < masterSlaveSplit) {
            return master.getLong256B(col);
        }
        return slave.getLong256B(col - masterSlaveSplit);
    }

    @Override
    public Record getRecord(int col) {
        if (col < masterSlaveSplit) {
            return master.getRecord(col);
        }
        return slave.getRecord(col - masterSlaveSplit);
    }

    @Override
    public long getRowId() {
        return master.getRowId();
    }

    @Override
    public short getShort(int col) {
        if (col < masterSlaveSplit) {
            return master.getShort(col);
        }
        return slave.getShort(col - masterSlaveSplit);
    }

    @Override
    public CharSequence getStr(int col) {
        if (col < masterSlaveSplit) {
            return master.getStr(col);
        }
        return slave.getStr(col - masterSlaveSplit);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        if (col < masterSlaveSplit) {
            master.getStr(col, sink);
        } else {
            slave.getStr(col - masterSlaveSplit, sink);
        }
    }

    @Override
    public CharSequence getStrB(int col) {
        if (col < masterSlaveSplit) {
            return master.getStrB(col);
        }
        return slave.getStrB(col - masterSlaveSplit);
    }

    @Override
    public int getStrLen(int col) {
        if (col < masterSlaveSplit) {
            return master.getStrLen(col);
        }
        return slave.getStrLen(col - masterSlaveSplit);
    }

    @Override
    public CharSequence getSym(int col) {
        if (col < masterSlaveSplit) {
            return master.getSym(col);
        }
        int slaveCol = col - masterSlaveSplit;
        if (slaveCol >= slaveWrappedOverMaster && slave != nullRecord) {
            slaveCol -= slaveWrappedOverMaster;
            int masterCol = masterTableKeyColumns.getColumnIndexFactored(slaveCol);
            return master.getSym(masterCol);
        }
        return slave.getSym(slaveCol);
    }

    @Override
    public CharSequence getSymB(int col) {
        if (col < masterSlaveSplit) {
            return master.getSymB(col);
        }
        return slave.getSymB(col - masterSlaveSplit);
    }

    @Override
    public long getTimestamp(int col) {
        if (col < masterSlaveSplit) {
            return master.getTimestamp(col);
        }
        return slave.getTimestamp(col - masterSlaveSplit);
    }

    @Override
    public long getUpdateRowId() {
        return master.getUpdateRowId();
    }

    void hasSlave(boolean value) {
        slave = value ? flappingSlave : nullRecord;
    }

    void of(Record master, Record slave) {
        this.master = master;
        this.slave = slave;
        this.flappingSlave = slave;
    }
}
