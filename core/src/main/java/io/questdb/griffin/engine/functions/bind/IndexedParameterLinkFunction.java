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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.FunctionExtension;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.Misc;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

public class IndexedParameterLinkFunction implements Function, FunctionExtension {
    private final int position;
    private final int variableIndex;
    private Function base;
    private int type;

    public IndexedParameterLinkFunction(int variableIndex, int type, int position) {
        this.variableIndex = variableIndex;
        this.type = type;
        this.position = position;
    }

    @Override
    public void assignType(int type, BindVariableService bindVariableService) throws SqlException {
        this.type = bindVariableService.define(variableIndex, type, position);
    }

    @Override
    public void close() {
        base = Misc.free(base);
    }

    @Override
    public FunctionExtension extendedOps() {
        return this;
    }

    @Override
    public ArrayView getArray(Record rec) {
        return getBase().getArray(rec);
    }

    @Override
    public int getArrayLength() {
        return getBase().extendedOps().getArrayLength();
    }

    @Override
    public BinarySequence getBin(Record rec) {
        return getBase().getBin(rec);
    }

    @Override
    public long getBinLen(Record rec) {
        return getBase().getBinLen(rec);
    }

    @Override
    public boolean getBool(Record rec) {
        return getBase().getBool(rec);
    }

    @Override
    public byte getByte(Record rec) {
        return getBase().getByte(rec);
    }

    @Override
    public char getChar(Record rec) {
        return getBase().getChar(rec);
    }

    @Override
    public long getDate(Record rec) {
        return getBase().getDate(rec);
    }

    @Override
    public void getDecimal128(Record rec, Decimal128 sink) {
        getBase().getDecimal128(rec, sink);
    }

    @Override
    public short getDecimal16(Record rec) {
        return getBase().getDecimal16(rec);
    }

    @Override
    public void getDecimal256(Record rec, Decimal256 sink) {
        getBase().getDecimal256(rec, sink);
    }

    @Override
    public int getDecimal32(Record rec) {
        return getBase().getDecimal32(rec);
    }

    @Override
    public long getDecimal64(Record rec) {
        return getBase().getDecimal64(rec);
    }

    @Override
    public byte getDecimal8(Record rec) {
        return getBase().getDecimal8(rec);
    }

    @Override
    public double getDouble(Record rec) {
        return getBase().getDouble(rec);
    }

    @Override
    public float getFloat(Record rec) {
        return getBase().getFloat(rec);
    }

    @Override
    public byte getGeoByte(Record rec) {
        return getBase().getGeoByte(rec);
    }

    @Override
    public int getGeoInt(Record rec) {
        return getBase().getGeoInt(rec);
    }

    @Override
    public long getGeoLong(Record rec) {
        return getBase().getGeoLong(rec);
    }

    @Override
    public short getGeoShort(Record rec) {
        return getBase().getGeoShort(rec);
    }

    @Override
    public final int getIPv4(Record rec) {
        return getBase().getIPv4(rec);
    }

    @Override
    public int getInt(Record rec) {
        return getBase().getInt(rec);
    }

    @Override
    public @NotNull Interval getInterval(Record rec) {
        return getBase().getInterval(rec);
    }

    @Override
    public long getLong(Record rec) {
        return getBase().getLong(rec);
    }

    @Override
    public long getLong128Hi(Record rec) {
        return getBase().getLong128Hi(rec);
    }

    @Override
    public long getLong128Lo(Record rec) {
        return getBase().getLong128Lo(rec);
    }

    @Override
    public void getLong256(Record rec, CharSink<?> sink) {
        getBase().getLong256(rec, sink);
    }

    @Override
    public Long256 getLong256A(Record rec) {
        return getBase().getLong256A(rec);
    }

    @Override
    public Long256 getLong256B(Record rec) {
        return getBase().getLong256B(rec);
    }

    @Override
    public Record getRecord(Record rec) {
        return getBase().extendedOps().getRecord(rec);
    }

    @Override
    public RecordCursorFactory getRecordCursorFactory() {
        return getBase().getRecordCursorFactory();
    }

    @Override
    public short getShort(Record rec) {
        return getBase().getShort(rec);
    }

    @Override
    public CharSequence getStrA(Record rec, int arrayIndex) {
        return getBase().extendedOps().getStrA(rec, arrayIndex);
    }

    @Override
    public CharSequence getStrA(Record rec) {
        return getBase().getStrA(rec);
    }

    @Override
    public CharSequence getStrB(Record rec, int arrayIndex) {
        return getBase().extendedOps().getStrB(rec, arrayIndex);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return getBase().getStrB(rec);
    }

    @Override
    public int getStrLen(Record rec, int arrayIndex) {
        return getBase().extendedOps().getStrLen(rec, arrayIndex);
    }

    @Override
    public int getStrLen(Record rec) {
        return getBase().getStrLen(rec);
    }

    @Override
    public CharSequence getSymbol(Record rec) {
        return getBase().getSymbol(rec);
    }

    @Override
    public CharSequence getSymbolB(Record rec) {
        return getBase().getSymbolB(rec);
    }

    @Override
    public long getTimestamp(Record rec) {
        return getBase().getTimestamp(rec);
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public Utf8Sequence getVarcharA(Record rec) {
        return getBase().getVarcharA(rec);
    }

    @Override
    public Utf8Sequence getVarcharB(Record rec) {
        return getBase().getVarcharB(rec);
    }

    @Override
    public int getVarcharSize(Record rec) {
        return getBase().getVarcharSize(rec);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        base = executionContext.getBindVariableService().getFunction(variableIndex);
        if (base == null) {
            throw SqlException.position(position).put("undefined bind variable: ").put(variableIndex);
        }
        this.type = base.getType();
        base.init(symbolTableSource, executionContext);
    }

    @Override
    public boolean isNonDeterministic() {
        return true;
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("$").val(variableIndex).val("::").val(Chars.toLowerCaseAscii(ColumnType.nameOf(type)));
    }

    private Function getBase() {
        assert base != null;
        return base;
    }
}
