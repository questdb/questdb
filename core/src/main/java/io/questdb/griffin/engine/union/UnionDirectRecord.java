/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

public class UnionDirectRecord extends AbstractUnionRecord {

    @Override
    public BinarySequence getBin(int col) {
        if (ab) {
            return recordA.getBin(col);
        }
        return recordB.getBin(col);
    }

    @Override
    public long getBinLen(int col) {
        if (ab) {
            return recordA.getBinLen(col);
        }
        return recordB.getBinLen(col);
    }

    @Override
    public boolean getBool(int col) {
        if (ab) {
            return recordA.getBool(col);
        }
        return recordB.getBool(col);
    }

    @Override
    public byte getByte(int col) {
        if (ab) {
            return recordA.getByte(col);
        }
        return recordB.getByte(col);
    }

    @Override
    public char getChar(int col) {
        if (ab) {
            return recordA.getChar(col);
        }
        return recordB.getChar(col);
    }

    @Override
    public long getDate(int col) {
        if (ab) {
            return recordA.getDate(col);
        }
        return recordB.getDate(col);
    }

    @Override
    public double getDouble(int col) {
        if (ab) {
            return recordA.getDouble(col);
        }
        return recordB.getDouble(col);
    }

    @Override
    public float getFloat(int col) {
        if (ab) {
            return recordA.getFloat(col);
        }
        return recordB.getFloat(col);
    }

    @Override
    public int getInt(int col) {
        if (ab) {
            return recordA.getInt(col);
        }
        return recordB.getInt(col);
    }

    @Override
    public long getLong(int col) {
        if (ab) {
            return recordA.getLong(col);
        }
        return recordB.getLong(col);
    }

    @Override
    public void getLong256(int col, CharSink sink) {
        if (ab) {
            recordA.getLong256(col, sink);
        } else {
            recordB.getLong256(col, sink);
        }
    }

    @Override
    public Long256 getLong256A(int col) {
        if (ab) {
            return recordA.getLong256A(col);
        }
        return recordB.getLong256A(col);
    }

    // symbol is not supported by set functions

    @Override
    public Long256 getLong256B(int col) {
        if (ab) {
            return recordA.getLong256B(col);
        }
        return recordB.getLong256B(col);
    }

    @Override
    public short getShort(int col) {
        if (ab) {
            return recordA.getShort(col);
        }
        return recordB.getShort(col);
    }

    @Override
    public CharSequence getStr(int col) {
        if (ab) {
            return recordA.getStr(col);
        }
        return recordB.getStr(col);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        if (ab) {
            recordA.getStr(col, sink);
        } else {
            recordB.getStr(col, sink);
        }
    }

    @Override
    public CharSequence getStrB(int col) {
        if (ab) {
            return recordA.getStrB(col);
        }
        return recordB.getStrB(col);
    }

    @Override
    public int getStrLen(int col) {
        if (ab) {
            return recordA.getStrLen(col);
        }
        return recordB.getStrLen(col);
    }

    @Override
    public long getTimestamp(int col) {
        if (ab) {
            return recordA.getTimestamp(col);
        }
        return recordB.getTimestamp(col);
    }

    @Override
    public byte getGeoByte(int col) {
        if (ab) {
            return recordA.getGeoByte(col);
        }
        return recordB.getGeoByte(col);
    }

    @Override
    public short getGeoShort(int col) {
        if (ab) {
            return recordA.getGeoShort(col);
        }
        return recordB.getGeoShort(col);
    }

    @Override
    public int getGeoInt(int col) {
        if (ab) {
            return recordA.getGeoInt(col);
        }
        return recordB.getGeoInt(col);
    }

    @Override
    public long getGeoLong(int col) {
        if (ab) {
            return recordA.getGeoLong(col);
        }
        return recordB.getGeoLong(col);
    }
}
