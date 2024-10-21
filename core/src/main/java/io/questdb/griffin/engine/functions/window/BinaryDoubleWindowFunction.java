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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.IntList;

public abstract class BinaryDoubleWindowFunction extends DoubleFunction implements WindowFunction, BinaryFunction {
    protected final Function arg1;
    protected final Function arg2;
    protected int columnIndex;

    public BinaryDoubleWindowFunction(Function arg1, Function arg2) {
        this.arg1 = arg1;
        this.arg2 = arg2;
    }

    @Override
    public void close() {
        arg1.close();
        arg2.close();
    }

    @Override
    public void cursorClosed() {
        arg1.cursorClosed();
        arg2.cursorClosed();
    }

    @Override
    public double getDouble(Record rec) {
        //unused
        throw new UnsupportedOperationException();
    }

    @Override
    public Function getLeft() {
        return arg1;
    }

    @Override
    public abstract String getName();

    @Override
    public Function getRight() {
        return arg2;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        super.init(symbolTableSource, executionContext);
        arg1.init(symbolTableSource, executionContext);
        arg2.init(symbolTableSource, executionContext);
    }

    @Override
    public void initRecordComparator(RecordComparatorCompiler recordComparatorCompiler, ArrayColumnTypes chainTypes, IntList order) {
    }

    @Override
    public void reset() {

    }

    @Override
    public void setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(getName());
        sink.val('(').val(arg1).val(", ").val(arg2).val(')');
        sink.val(" over ()");
    }

    @Override
    public void toTop() {
        arg1.toTop();
        arg2.toTop();
    }
}
