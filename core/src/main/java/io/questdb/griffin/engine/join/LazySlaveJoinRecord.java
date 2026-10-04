/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

/**
 * A {@link JoinRecord} whose slave side is positioned on first use: reading a slave column asks
 * the {@link SlavePositioner} to position the slave record for the current master row, which it
 * does once per row. A join whose consumer reads only master columns never positions the slave.
 */
public class LazySlaveJoinRecord extends JoinRecord {
    private final SlavePositioner positioner;

    public LazySlaveJoinRecord(int split, SlavePositioner positioner) {
        super(split);
        this.positioner = positioner;
    }

    @Override
    public ArrayView getArray(int col, int columnType) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getArray(col, columnType);
    }

    @Override
    public int getArrayDimLen(int col, int columnType, int dim) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getArrayDimLen(col, columnType, dim);
    }

    @Override
    public double getArrayDouble1d2d(int col, int columnType, int idx0, int idx1) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getArrayDouble1d2d(col, columnType, idx0, idx1);
    }

    @Override
    public BinarySequence getBin(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getBin(col);
    }

    @Override
    public long getBinLen(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getBinLen(col);
    }

    @Override
    public boolean getBool(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getBool(col);
    }

    @Override
    public byte getByte(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getByte(col);
    }

    @Override
    public char getChar(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getChar(col);
    }

    @Override
    public long getDate(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getDate(col);
    }

    @Override
    public void getDecimal128(int col, Decimal128 sink) {
        if (col >= split) {
            positioner.positionSlave();
        }
        super.getDecimal128(col, sink);
    }

    @Override
    public short getDecimal16(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getDecimal16(col);
    }

    @Override
    public void getDecimal256(int col, Decimal256 sink) {
        if (col >= split) {
            positioner.positionSlave();
        }
        super.getDecimal256(col, sink);
    }

    @Override
    public int getDecimal32(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getDecimal32(col);
    }

    @Override
    public long getDecimal64(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getDecimal64(col);
    }

    @Override
    public byte getDecimal8(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getDecimal8(col);
    }

    @Override
    public double getDouble(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getDouble(col);
    }

    @Override
    public float getFloat(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getFloat(col);
    }

    @Override
    public byte getGeoByte(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getGeoByte(col);
    }

    @Override
    public int getGeoInt(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getGeoInt(col);
    }

    @Override
    public long getGeoLong(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getGeoLong(col);
    }

    @Override
    public short getGeoShort(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getGeoShort(col);
    }

    @Override
    public int getIPv4(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getIPv4(col);
    }

    @Override
    public int getInt(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getInt(col);
    }

    @Override
    public Interval getInterval(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getInterval(col);
    }

    @Override
    public long getLong(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getLong(col);
    }

    @Override
    public long getLong128Hi(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getLong128Hi(col);
    }

    @Override
    public long getLong128Lo(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getLong128Lo(col);
    }

    @Override
    public void getLong256(int col, CharSink<?> sink) {
        if (col >= split) {
            positioner.positionSlave();
        }
        super.getLong256(col, sink);
    }

    @Override
    public Long256 getLong256A(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getLong256A(col);
    }

    @Override
    public Long256 getLong256B(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getLong256B(col);
    }

    @Override
    public Record getRecord(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getRecord(col);
    }

    @Override
    public short getShort(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getShort(col);
    }

    @Override
    public CharSequence getStrA(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getStrA(col);
    }

    @Override
    public CharSequence getStrB(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getStrB(col);
    }

    @Override
    public int getStrLen(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getStrLen(col);
    }

    @Override
    public CharSequence getSymA(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getSymA(col);
    }

    @Override
    public CharSequence getSymB(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getSymB(col);
    }

    @Override
    public long getTimestamp(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getTimestamp(col);
    }

    @Override
    public Utf8Sequence getVarcharA(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getVarcharA(col);
    }

    @Override
    public Utf8Sequence getVarcharB(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getVarcharB(col);
    }

    @Override
    public int getVarcharSize(int col) {
        if (col >= split) {
            positioner.positionSlave();
        }
        return super.getVarcharSize(col);
    }

    @FunctionalInterface
    public interface SlavePositioner {
        /** Positions the slave record for the current master row, unless it already is. */
        void positionSlave();
    }
}
