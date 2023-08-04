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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.sql.Function;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;

public class UnionCastRecord extends AbstractUnionRecord {

    private final ObjList<Function> castFunctionsA;
    private final ObjList<Function> castFunctionsB;

    public UnionCastRecord(ObjList<Function> castFunctionsA, ObjList<Function> castFunctionsB) {
        this.castFunctionsA = castFunctionsA;
        this.castFunctionsB = castFunctionsB;
    }

    @Override
    public BinarySequence getBin(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getBin(recordA);
        }
        return castFunctionsB.getQuick(col).getBin(recordB);
    }

    @Override
    public long getBinLen(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getBinLen(recordA);
        }
        return castFunctionsB.getQuick(col).getBinLen(recordB);
    }

    @Override
    public boolean getBool(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getBool(recordA);
        }
        return castFunctionsB.getQuick(col).getBool(recordB);
    }

    @Override
    public byte getByte(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getByte(recordA);
        }
        return castFunctionsB.getQuick(col).getByte(recordB);
    }

    public ObjList<Function> getCastFunctionsA() {
        return castFunctionsA;
    }

    public ObjList<Function> getCastFunctionsB() {
        return castFunctionsB;
    }

    @Override
    public char getChar(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getChar(recordA);
        }
        return castFunctionsB.getQuick(col).getChar(recordB);
    }

    @Override
    public long getDate(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getDate(recordA);
        }
        return castFunctionsB.getQuick(col).getDate(recordB);
    }

    @Override
    public double getDouble(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getDouble(recordA);
        }
        return castFunctionsB.getQuick(col).getDouble(recordB);
    }

    @Override
    public float getFloat(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getFloat(recordA);
        }
        return castFunctionsB.getQuick(col).getFloat(recordB);
    }

    @Override
    public byte getGeoByte(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getGeoByte(recordA);
        }
        return castFunctionsB.getQuick(col).getGeoByte(recordB);
    }

    @Override
    public int getGeoInt(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getGeoInt(recordA);
        }
        return castFunctionsB.getQuick(col).getGeoInt(recordB);
    }

    @Override
    public long getGeoLong(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getGeoLong(recordA);
        }
        return castFunctionsB.getQuick(col).getGeoLong(recordB);
    }

    @Override
    public short getGeoShort(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getGeoShort(recordA);
        }
        return castFunctionsB.getQuick(col).getGeoShort(recordB);
    }

    // symbol is not supported by set functions

    @Override
    public int getIPv4(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getIPv4(recordA);
        }
        return castFunctionsB.getQuick(col).getIPv4(recordB);
    }

    @Override
    public int getInt(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getInt(recordA);
        }
        return castFunctionsB.getQuick(col).getInt(recordB);
    }

    @Override
    public long getLong(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getLong(recordA);
        }
        return castFunctionsB.getQuick(col).getLong(recordB);
    }

    @Override
    public long getLong128Hi(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getLong128Hi(recordA);
        }
        return castFunctionsB.getQuick(col).getLong128Hi(recordB);
    }

    @Override
    public long getLong128Lo(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getLong128Lo(recordA);
        }
        return castFunctionsB.getQuick(col).getLong128Lo(recordB);
    }

    @Override
    public void getLong256(int col, CharSink sink) {
        if (useA) {
            castFunctionsA.getQuick(col).getLong256(recordA, sink);
        } else {
            castFunctionsB.getQuick(col).getLong256(recordB, sink);
        }
    }

    @Override
    public Long256 getLong256A(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getLong256A(recordA);
        }
        return castFunctionsB.getQuick(col).getLong256A(recordB);
    }

    @Override
    public Long256 getLong256B(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getLong256B(recordA);
        }
        return castFunctionsB.getQuick(col).getLong256B(recordB);
    }

    @Override
    public long getRowId() {
        assert useA;
        return recordA.getRowId();
    }

    @Override
    public short getShort(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getShort(recordA);
        }
        return castFunctionsB.getQuick(col).getShort(recordB);
    }

    @Override
    public CharSequence getStr(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getStr(recordA);
        }
        return castFunctionsB.getQuick(col).getStr(recordB);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        if (useA) {
            castFunctionsA.getQuick(col).getStr(recordA, sink);
        } else {
            castFunctionsB.getQuick(col).getStr(recordB, sink);
        }
    }

    @Override
    public CharSequence getStrB(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getStrB(recordA);
        }
        return castFunctionsB.getQuick(col).getStrB(recordB);
    }

    @Override
    public int getStrLen(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getStrLen(recordA);
        }
        return castFunctionsB.getQuick(col).getStrLen(recordB);
    }

    @Override
    public long getTimestamp(int col) {
        if (useA) {
            return castFunctionsA.getQuick(col).getTimestamp(recordA);
        }
        return castFunctionsB.getQuick(col).getTimestamp(recordB);
    }
}
