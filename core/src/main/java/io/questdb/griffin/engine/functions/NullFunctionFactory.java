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

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.*;
import io.questdb.std.BinarySequence;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;

public class NullFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "null()";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new CursorFunction(new NullFactory());
    }

    private static class NullFactory implements RecordCursorFactory {
        private final static GenericRecordMetadata METADATA = new GenericRecordMetadata();

        static {
            METADATA.add(new TableColumnMetadata("null", ColumnType.UNDEFINED, null));
        }

        private final NullRecordCursor cursor = new NullRecordCursor();

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            cursor.toTop();
            return cursor;
        }

        @Override
        public RecordMetadata getMetadata() {
            return METADATA;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return true;
        }
    }

    private static class NullRecordCursor implements RecordCursor {

        private int remaining = 1;

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return NullRecord.INSTANCE;
        }

        @Override
        public boolean hasNext() {
            return remaining-- > 0;
        }

        @Override
        public Record getRecordB() {
            return NullRecord.INSTANCE;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
        }

        @Override
        public void toTop() {
            remaining = 1;
        }

        @Override
        public long size() {
            return remaining > 0? remaining : 0;
        }
    }

    private static class NullRecord implements Record {

        private static final NullRecord INSTANCE = new NullRecord();

        @Override
        public BinarySequence getBin(int col) {
            return NullBinConstant.INSTANCE.getBin(null);
        }

        @Override
        public long getBinLen(int col) {
            return NullBinConstant.INSTANCE.getBinLen(null);
        }

        @Override
        public boolean getBool(int col) {
            return BooleanConstant.FALSE.getBool(null);
        }

        @Override
        public byte getByte(int col) {
            return ByteConstant.ZERO.getByte(null);
        }

        @Override
        public char getChar(int col) {
            return CharConstant.ZERO.getChar(null);
        }

        @Override
        public long getDate(int col) {
            return DateConstant.NULL.getDate(null);
        }

        @Override
        public double getDouble(int col) {
            return DoubleConstant.NULL.getFloat(null);
        }

        @Override
        public float getFloat(int col) {
            return FloatConstant.NULL.getFloat(null);
        }

        @Override
        public int getInt(int col) {
            return IntConstant.NULL.getInt(null);
        }

        @Override
        public long getLong(int col) {
            return LongConstant.NULL.getLong(null);
        }

        @Override
        public void getLong256(int col, CharSink sink) {
            Long256NullConstant.INSTANCE.getLong256(null, sink);
        }

        @Override
        public Long256 getLong256A(int col) {
            return Long256NullConstant.INSTANCE.getLong256A(null);
        }

        @Override
        public Long256 getLong256B(int col) {
            return Long256NullConstant.INSTANCE.getLong256B(null);
        }

        @Override
        public Record getRecord(int col) {
            return this;
        }

        @Override
        public long getRowId() {
            return 0L;
        }

        @Override
        public short getShort(int col) {
            return ShortConstant.ZERO.getShort(null);
        }

        @Override
        public CharSequence getStr(int col) {
            return StrConstant.NULL.getStr(null);
        }

        @Override
        public void getStr(int col, CharSink sink) {
            sink.put(getStr(col));
        }

        @Override
        public CharSequence getStrB(int col) {
            return StrConstant.NULL.getStrB(null);
        }

        @Override
        public int getStrLen(int col) {
            return StrConstant.NULL.getStrLen(null);
        }

        @Override
        public CharSequence getSym(int col) {
            return SymbolConstant.NULL.getSymbol(null);
        }

        @Override
        public CharSequence getSymB(int col) {
            return SymbolConstant.NULL.getSymbolB(null);
        }

        @Override
        public long getTimestamp(int col) {
            return TimestampConstant.NULL.getTimestamp(null);
        }
    }
}
