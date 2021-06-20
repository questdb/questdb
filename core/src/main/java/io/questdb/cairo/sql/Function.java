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

package io.questdb.cairo.sql;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;

import java.io.Closeable;

public interface Function extends Closeable {

    static void init(
            ObjList<? extends Function> args,
            SymbolTableSource symbolTableSource,
            SqlExecutionContext executionContext
    ) throws SqlException {
        for (int i = 0, n = args.size(); i < n; i++) {
            args.getQuick(i).init(symbolTableSource, executionContext);
        }
    }

    @Override
    default void close() {
    }

    default boolean supportsRandomAccess() {
        return true;
    }

    int getArrayLength();

    BinarySequence getBin(Record rec);

    long getBinLen(Record rec);

    boolean getBool(Record rec);

    byte getByte(Record rec);

    char getChar(Record rec);

    long getDate(Record rec);

    double getDouble(Record rec);

    float getFloat(Record rec);

    int getInt(Record rec);

    long getLong(Record rec);

    void getLong256(Record rec, CharSink sink);

    Long256 getLong256A(Record rec);

    Long256 getLong256B(Record rec);

    // when function returns factory it becomes factory
    // on other words this is not a tear-away instance
    RecordCursorFactory getRecordCursorFactory();

    // function returns a record of values
    Record getRecord(Record rec);

    default RecordMetadata getMetadata() {
        return null;
    }

    short getShort(Record rec);

    CharSequence getStr(Record rec);

    CharSequence getStr(Record rec, int arrayIndex);

    void getStr(Record rec, CharSink sink);

    void getStr(Record rec, CharSink sink, int arrayIndex);

    CharSequence getStrB(Record rec);

    CharSequence getStrB(Record rec, int arrayIndex);

    int getStrLen(Record rec);

    int getStrLen(Record rec, int arrayIndex);

    CharSequence getSymbol(Record rec);

    CharSequence getSymbolB(Record rec);

    long getTimestamp(Record rec);

    int getType();

    default boolean isUndefined() {
        return getType() == ColumnType.UNDEFINED;
    }

    default void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
    }

    default void assignType(int type, BindVariableService bindVariableService) throws SqlException {
        throw new UnsupportedOperationException();
    }

    default boolean isConstant() {
        return false;
    }

    // If function is constant for query, e.g. record independent
    // For example now() and bind variables are Runtime Constants
    default boolean isRuntimeConstant() {
        return false;
    }

    default void toTop() {
    }
}
