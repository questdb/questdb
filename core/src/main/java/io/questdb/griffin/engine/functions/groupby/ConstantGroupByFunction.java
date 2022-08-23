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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

public class ConstantGroupByFunction implements GroupByFunction, UnaryFunction {
    private final ConstantFunction arg;
    public ConstantGroupByFunction(@NotNull ConstantFunction arg) {
        this.arg = arg;
    }
    @Override
    public void computeFirst(MapValue mapValue, Record record) {

    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {

    }

    @Override
    public Function getArg() {
        return this.arg;
    }

    @Override
    public int getArrayLength() {
        return this.arg.getArrayLength();
    }

    @Override
    public BinarySequence getBin(Record rec) {
        return this.arg.getBin(null);
    }

    @Override
    public long getBinLen(Record rec) {
        return this.arg.getBinLen(null);
    }

    @Override
    public boolean getBool(Record rec) {
        return this.arg.getBool(null);
    }

    @Override
    public byte getByte(Record rec) {
        return this.arg.getByte(null);
    }

    @Override
    public char getChar(Record rec) {
        return this.arg.getChar(null);
    }

    @Override
    public long getDate(Record rec) {
        return this.arg.getDate(null);
    }

    @Override
    public double getDouble(Record rec) {
        return this.arg.getDouble(null);
    }

    @Override
    public float getFloat(Record rec) {
        return this.arg.getFloat(null);
    }

    @Override
    public byte getGeoByte(Record rec) {
        return this.arg.getGeoByte(null);
    }

    @Override
    public int getGeoInt(Record rec) {
        return this.arg.getGeoInt(null);
    }

    @Override
    public long getGeoLong(Record rec) {
        return this.arg.getGeoLong(null);
    }

    @Override
    public short getGeoShort(Record rec) {
        return this.arg.getGeoShort(null);
    }

    @Override
    public int getInt(Record rec) {
        return this.arg.getInt(null);
    }

    @Override
    public long getLong(Record rec) {
        return this.arg.getLong(null);
    }

    @Override
    public void getLong256(Record rec, CharSink sink) {
        this.arg.getLong256(null, sink);
    }

    @Override
    public Long256 getLong256A(Record rec) {
        return this.arg.getLong256A(null);
    }

    @Override
    public Long256 getLong256B(Record rec) {
        return this.arg.getLong256B(null);
    }

    @Override
    public long getLong128Hi(Record rec) {
        return this.arg.getLong128Hi(null);
    }

    @Override
    public long getLong128Lo(Record rec) {
        return this.arg.getLong128Lo(null);
    }

    @Override
    public Record getRecord(Record rec) {
        return this.arg.getRecord(null);
    }

    @Override
    public RecordCursorFactory getRecordCursorFactory() {
        return this.arg.getRecordCursorFactory();
    }

    @Override
    public short getShort(Record rec) {
        return this.arg.getShort(null);
    }

    @Override
    public CharSequence getStr(Record rec) {
        return this.arg.getStr(null);
    }

    @Override
    public CharSequence getStr(Record rec, int arrayIndex) {
        return this.arg.getStr(null, arrayIndex);
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        this.arg.getStr(null, sink);
    }

    @Override
    public void getStr(Record rec, CharSink sink, int arrayIndex) {
        this.arg.getStr(null, sink, arrayIndex);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return this.arg.getStrB(null);
    }

    @Override
    public CharSequence getStrB(Record rec, int arrayIndex) {
        return this.arg.getStrB(null, arrayIndex);
    }

    @Override
    public int getStrLen(Record rec) {
        return this.arg.getStrLen(null);
    }

    @Override
    public int getStrLen(Record rec, int arrayIndex) {
        return this.arg.getStrLen(null, arrayIndex);
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return this.arg.getSymbol(null);
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        return this.arg.getSymbolB(null);
    }

    @Override
    public long getTimestamp(Record rec) {
        return this.arg.getTimestamp(null);
    }

    @Override
    public int getType() {
        return this.arg.getType();
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {

    }

    @Override
    public void setNull(MapValue mapValue) {

    }

    @Override
    public boolean isConstant() {
        return true;
    }
}
