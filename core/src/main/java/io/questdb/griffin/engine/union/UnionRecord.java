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

import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf16Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;

public class UnionRecord extends AbstractUnionRecord {

    @Override
    public BinarySequence getBin(int col) {
        if (useA) {
            return recordA.getBin(col);
        }
        return recordB.getBin(col);
    }

    @Override
    public long getBinLen(int col) {
        if (useA) {
            return recordA.getBinLen(col);
        }
        return recordB.getBinLen(col);
    }

    @Override
    public boolean getBool(int col) {
        if (useA) {
            return recordA.getBool(col);
        }
        return recordB.getBool(col);
    }

    @Override
    public byte getByte(int col) {
        if (useA) {
            return recordA.getByte(col);
        }
        return recordB.getByte(col);
    }

    @Override
    public char getChar(int col) {
        if (useA) {
            return recordA.getChar(col);
        }
        return recordB.getChar(col);
    }

    @Override
    public long getDate(int col) {
        if (useA) {
            return recordA.getDate(col);
        }
        return recordB.getDate(col);
    }

    @Override
    public double getDouble(int col) {
        if (useA) {
            return recordA.getDouble(col);
        }
        return recordB.getDouble(col);
    }

    @Override
    public float getFloat(int col) {
        if (useA) {
            return recordA.getFloat(col);
        }
        return recordB.getFloat(col);
    }

    @Override
    public byte getGeoByte(int col) {
        if (useA) {
            return recordA.getGeoByte(col);
        }
        return recordB.getGeoByte(col);
    }

    @Override
    public int getGeoInt(int col) {
        if (useA) {
            return recordA.getGeoInt(col);
        }
        return recordB.getGeoInt(col);
    }

    @Override
    public long getGeoLong(int col) {
        if (useA) {
            return recordA.getGeoLong(col);
        }
        return recordB.getGeoLong(col);
    }

    @Override
    public short getGeoShort(int col) {
        if (useA) {
            return recordA.getGeoShort(col);
        }
        return recordB.getGeoShort(col);
    }

    // symbol is not supported by set functions

    @Override
    public int getIPv4(int col) {
        if (useA) {
            return recordA.getIPv4(col);
        }
        return recordB.getIPv4(col);
    }

    @Override
    public int getInt(int col) {
        if (useA) {
            return recordA.getInt(col);
        }
        return recordB.getInt(col);
    }

    @Override
    public long getLong(int col) {
        if (useA) {
            return recordA.getLong(col);
        }
        return recordB.getLong(col);
    }

    @Override
    public long getLong128Hi(int col) {
        if (useA) {
            return recordA.getLong128Hi(col);
        }
        return recordB.getLong128Hi(col);
    }

    @Override
    public long getLong128Lo(int col) {
        if (useA) {
            return recordA.getLong128Lo(col);
        }
        return recordB.getLong128Lo(col);
    }

    @Override
    public void getLong256(int col, CharSink<?> sink) {
        if (useA) {
            recordA.getLong256(col, sink);
        } else {
            recordB.getLong256(col, sink);
        }
    }

    @Override
    public Long256 getLong256A(int col) {
        if (useA) {
            return recordA.getLong256A(col);
        }
        return recordB.getLong256A(col);
    }

    @Override
    public Long256 getLong256B(int col) {
        if (useA) {
            return recordA.getLong256B(col);
        }
        return recordB.getLong256B(col);
    }

    @Override
    public short getShort(int col) {
        if (useA) {
            return recordA.getShort(col);
        }
        return recordB.getShort(col);
    }

    @Override
    public CharSequence getStrA(int col) {
        if (useA) {
            return recordA.getStrA(col);
        }
        return recordB.getStrA(col);
    }

    @Override
    public void getStr(int col, Utf16Sink utf16Sink) {
        if (useA) {
            recordA.getStr(col, utf16Sink);
        } else {
            recordB.getStr(col, utf16Sink);
        }
    }

    @Override
    public CharSequence getStrB(int col) {
        if (useA) {
            return recordA.getStrB(col);
        }
        return recordB.getStrB(col);
    }

    @Override
    public int getStrLen(int col) {
        if (useA) {
            return recordA.getStrLen(col);
        }
        return recordB.getStrLen(col);
    }

    @Override
    public long getTimestamp(int col) {
        if (useA) {
            return recordA.getTimestamp(col);
        }
        return recordB.getTimestamp(col);
    }

    @Override
    public void getVarchar(int col, Utf8Sink utf8Sink) {
        if (useA) {
            recordA.getVarchar(col, utf8Sink);
        } else {
            recordB.getVarchar(col, utf8Sink);
        }
    }

    @Override
    public Utf8Sequence getVarcharA(int col) {
        if (useA) {
            return recordA.getVarcharA(col);
        }
        return recordB.getVarcharA(col);
    }

    @Override
    public Utf8Sequence getVarcharB(int col) {
        if (useA) {
            return recordA.getVarcharB(col);
        }
        return recordB.getVarcharB(col);
    }
}
