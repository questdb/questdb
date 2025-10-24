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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.groupby.InterpolationGroupByFunction;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;

public class SampleByFillRecord implements Record {
    private final ObjList<? extends Function> functionsA;
    private final ObjList<? extends Function> functionsB;
    private final ObjList<InterpolationGroupByFunction> interpolations = new ObjList<>();
    private Record base;
    private ObjList<? extends Function> current;

    public SampleByFillRecord(ObjList<? extends Function> functionsA, ObjList<? extends Function> functionsB) {
        this.current = functionsA;
        this.functionsA = functionsA;
        this.functionsB = functionsB;

        for (int i = 0, n = functionsB.size(); i < n; i++) {
            Function f = functionsB.get(i);
            if (f instanceof InterpolationGroupByFunction) {
                interpolations.add((InterpolationGroupByFunction) f);
            }
        }
    }

    @Override
    public BinarySequence getBin(int col) {
        return getFunction(col).getBin(base);
    }

    @Override
    public long getBinLen(int col) {
        BinarySequence sequence = getBin(col);
        if (sequence == null) {
            return -1L;
        }
        return sequence.length();
    }

    @Override
    public boolean getBool(int col) {
        return getFunction(col).getBool(base);
    }

    @Override
    public byte getByte(int col) {
        return getFunction(col).getByte(base);
    }

    @Override
    public char getChar(int col) {
        return getFunction(col).getChar(base);
    }

    @Override
    public long getDate(int col) {
        return getFunction(col).getDate(base);
    }

    @Override
    public void getDecimal128(int col, Decimal128 sink) {
        getFunction(col).getDecimal128(base, sink);
    }

    @Override
    public short getDecimal16(int col) {
        return getFunction(col).getDecimal16(base);
    }

    @Override
    public void getDecimal256(int col, Decimal256 sink) {
        getFunction(col).getDecimal256(base, sink);
    }

    @Override
    public int getDecimal32(int col) {
        return getFunction(col).getDecimal32(base);
    }

    @Override
    public long getDecimal64(int col) {
        return getFunction(col).getDecimal64(base);
    }

    @Override
    public byte getDecimal8(int col) {
        return getFunction(col).getDecimal8(base);
    }

    @Override
    public double getDouble(int col) {
        return getFunction(col).getDouble(base);
    }

    @Override
    public float getFloat(int col) {
        return getFunction(col).getFloat(base);
    }

    @Override
    public byte getGeoByte(int col) {
        return getFunction(col).getGeoByte(base);
    }

    @Override
    public int getGeoInt(int col) {
        return getFunction(col).getGeoInt(base);
    }

    @Override
    public long getGeoLong(int col) {
        return getFunction(col).getGeoLong(base);
    }

    @Override
    public short getGeoShort(int col) {
        return getFunction(col).getGeoShort(base);
    }

    @Override
    public int getIPv4(int col) {
        return getFunction(col).getIPv4(base);
    }

    @Override
    public int getInt(int col) {
        return getFunction(col).getInt(base);
    }

    @Override
    public Interval getInterval(int col) {
        return getFunction(col).getInterval(base);
    }

    @Override
    public long getLong(int col) {
        return getFunction(col).getLong(base);
    }

    @Override
    public long getLong128Hi(int col) {
        return getFunction(col).getLong128Hi(base);
    }

    @Override
    public long getLong128Lo(int col) {
        return getFunction(col).getLong128Lo(base);
    }

    @Override
    public Record getRecord(int col) {
        return getFunction(col).extendedOps().getRecord(base);
    }

    @Override
    public short getShort(int col) {
        return getFunction(col).getShort(base);
    }

    @Override
    public CharSequence getStrA(int col) {
        return getFunction(col).getStrA(base);
    }

    @Override
    public CharSequence getStrB(int col) {
        return getFunction(col).getStrB(base);
    }

    @Override
    public int getStrLen(int col) {
        return getFunction(col).getStrLen(base);
    }

    @Override
    public CharSequence getSymA(int col) {
        return getFunction(col).getSymbol(base);
    }

    @Override
    public CharSequence getSymB(int col) {
        return getFunction(col).getSymbolB(base);
    }

    @Override
    public long getTimestamp(int col) {
        return getFunction(col).getTimestamp(base);
    }

    @Override
    public Utf8Sequence getVarcharA(int col) {
        return getFunction(col).getVarcharA(base);
    }

    @Override
    public Utf8Sequence getVarcharB(int col) {
        return getFunction(col).getVarcharB(base);
    }

    @Override
    public int getVarcharSize(int col) {
        return getFunction(col).getVarcharSize(base);
    }

    public void setActiveA() {
        current = functionsA;
        for (int i = 0, n = interpolations.size(); i < n; i++) {
            interpolations.get(i).stopInterpolating();
        }
    }

    public void setActiveB() {
        current = functionsB;
    }

    public void setActiveB(long startTime, long currentTime, long endTime) {
        current = functionsB;
        for (int i = 0, n = interpolations.size(); i < n; i++) {
            interpolations.get(i).startInterpolating(startTime, currentTime, endTime);
        }
    }

    public void setInterpolationTarget(Record target) {
        for (int i = 0, n = interpolations.size(); i < n; i++) {
            interpolations.get(i).setTarget(target);
        }
    }

    private Function getFunction(int columnIndex) {
        return current.getQuick(columnIndex);
    }

    void of(Record record) {
        this.base = record;
    }
}
