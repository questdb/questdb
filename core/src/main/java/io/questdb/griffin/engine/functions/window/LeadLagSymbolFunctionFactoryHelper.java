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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

class LeadLagSymbolFunctionFactoryHelper {

    static void checkDefaultValue(Function defaultValue, int position, String functionName) throws SqlException {
        if (!defaultValue.isNullConstant()) {
            throw SqlException.$(position, "non-null default value is not supported for symbol ").put(functionName);
        }
    }

    private LeadLagSymbolFunctionFactoryHelper() {
    }

    private abstract static class BaseSymbolWindowFunction extends SymbolFunction implements WindowFunction {
        protected final SymbolFunction arg;
        protected final Function defaultValue;
        protected final boolean ignoreNulls;
        protected final long offset;
        protected int columnIndex;
        protected int value = SymbolTable.VALUE_IS_NULL;

        private BaseSymbolWindowFunction(Function arg, Function defaultValue, long offset, boolean ignoreNulls) {
            this.arg = (SymbolFunction) arg;
            this.defaultValue = defaultValue;
            this.offset = offset;
            this.ignoreNulls = ignoreNulls;
        }

        @Override
        public void close() {
            Misc.free(arg);
            Misc.free(defaultValue);
        }

        @Override
        public void cursorClosed() {
            arg.cursorClosed();
            if (defaultValue != null) {
                defaultValue.cursorClosed();
            }
        }

        @Override
        public int getInt(Record rec) {
            return value;
        }

        @Override
        public @Nullable StaticSymbolTable getStaticSymbolTable() {
            return arg.getStaticSymbolTable();
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            return valueOf(getInt(rec));
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            return valueBOf(getInt(rec));
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            if (defaultValue != null) {
                defaultValue.init(symbolTableSource, executionContext);
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return ignoreNulls;
        }

        @Override
        public boolean isSymbolTableStatic() {
            return arg.isSymbolTableStatic();
        }

        @Override
        public @Nullable SymbolTable newSymbolTable() {
            return arg.newSymbolTable();
        }

        @Override
        public void reset() {
            value = SymbolTable.VALUE_IS_NULL;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toTop() {
            arg.toTop();
            if (defaultValue != null) {
                defaultValue.toTop();
            }
            value = SymbolTable.VALUE_IS_NULL;
        }

        @Override
        public CharSequence valueBOf(int key) {
            return arg.valueBOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            return arg.valueOf(key);
        }

        protected void toPlanArgs(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(", ").val(offset).val(", ");
            if (defaultValue != null) {
                sink.val(defaultValue);
            } else {
                sink.val("NULL");
            }
            sink.val(')');
            if (ignoreNulls) {
                sink.val(" ignore nulls");
            }
        }
    }

    static class LagFunction extends BaseSymbolWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        protected long count = 0;
        protected int loIdx = 0;

        public LagFunction(Function arg, Function defaultValue, long offset, MemoryARW memory, boolean ignoreNulls) {
            super(arg, defaultValue, offset, ignoreNulls);
            this.buffer = memory;
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public void computeNext(Record record) {
            if (computeNext0(record)) {
                loIdx = (int) ((loIdx + 1) % offset);
                count++;
            }
        }

        @Override
        public String getName() {
            return LeadLagWindowFunctionFactoryHelper.LAG_NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void reopen() {
            loIdx = 0;
            count = 0;
            value = SymbolTable.VALUE_IS_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            loIdx = 0;
            count = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            toPlanArgs(sink);
            sink.val(" over ()");
        }

        @Override
        public void toTop() {
            super.toTop();
            loIdx = 0;
            count = 0;
        }

        protected boolean computeNext0(Record record) {
            final int currentValue = arg.getInt(record);
            if (count < offset) {
                value = SymbolTable.VALUE_IS_NULL;
            } else {
                value = buffer.getInt((long) loIdx * Integer.BYTES);
            }

            final boolean respectNulls = !ignoreNulls || currentValue != SymbolTable.VALUE_IS_NULL;
            if (respectNulls) {
                buffer.putInt((long) loIdx * Integer.BYTES, currentValue);
            }
            return respectNulls;
        }
    }

    static class LagOverPartitionFunction extends BasePartitionedSymbolWindowFunction {

        public LagOverPartitionFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                MemoryARW memory,
                Function arg,
                boolean ignoreNulls,
                Function defaultValue,
                long offset
        ) {
            super(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            final MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            final MapValue mapValue = key.createValue();
            long startOffset;
            long firstIdx;
            long count = 0;

            if (mapValue.isNew()) {
                startOffset = memory.appendAddressFor(offset * Integer.BYTES) - memory.getPageAddress(0);
                firstIdx = 0;
            } else {
                startOffset = mapValue.getLong(0);
                firstIdx = mapValue.getLong(1);
                count = mapValue.getLong(2);
            }

            if (computeNext0(count, startOffset, firstIdx, record)) {
                firstIdx++;
                count++;
            }

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, firstIdx % offset);
            mapValue.putLong(2, count);
        }

        @Override
        public String getName() {
            return LeadLagWindowFunctionFactoryHelper.LAG_NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }

        private boolean computeNext0(long count, long startOffset, long firstIdx, Record record) {
            final int currentValue = arg.getInt(record);
            if (count < offset) {
                value = SymbolTable.VALUE_IS_NULL;
            } else {
                value = memory.getInt(startOffset + firstIdx * Integer.BYTES);
            }

            final boolean respectNulls = !ignoreNulls || currentValue != SymbolTable.VALUE_IS_NULL;
            if (respectNulls) {
                memory.putInt(startOffset + firstIdx * Integer.BYTES, currentValue);
            }
            return respectNulls;
        }
    }

    static class LeadFunction extends BaseSymbolWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        protected long count = 0;
        protected int loIdx = 0;

        public LeadFunction(Function arg, Function defaultValue, long offset, MemoryARW memory, boolean ignoreNulls) {
            super(arg, defaultValue, offset, ignoreNulls);
            this.buffer = memory;
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public String getName() {
            return LeadLagWindowFunctionFactoryHelper.LEAD_NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (doPass1(record, recordOffset, spi)) {
                loIdx = (int) ((loIdx + 1) % offset);
                count++;
            }
        }

        @Override
        public void reopen() {
            loIdx = 0;
            count = 0;
            value = SymbolTable.VALUE_IS_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            loIdx = 0;
            count = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            toPlanArgs(sink);
            sink.val(" over ()");
        }

        @Override
        public void toTop() {
            super.toTop();
            loIdx = 0;
            count = 0;
        }

        private boolean doPass1(Record record, long recordOffset, WindowSPI spi) {
            final int currentValue = arg.getInt(record);
            if (count < offset) {
                value = SymbolTable.VALUE_IS_NULL;
            } else {
                value = buffer.getInt((long) loIdx * Integer.BYTES);
            }

            final boolean respectNulls = !ignoreNulls || currentValue != SymbolTable.VALUE_IS_NULL;
            if (respectNulls) {
                buffer.putInt((long) loIdx * Integer.BYTES, currentValue);
            }
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
            return respectNulls;
        }
    }

    static class LeadOverPartitionFunction extends BasePartitionedSymbolWindowFunction {

        public LeadOverPartitionFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                MemoryARW memory,
                Function arg,
                boolean ignoreNulls,
                Function defaultValue,
                long offset
        ) {
            super(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset);
        }

        @Override
        public String getName() {
            return LeadLagWindowFunctionFactoryHelper.LEAD_NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            final MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            final MapValue mapValue = key.createValue();
            long startOffset;
            long firstIdx;
            long count = 0;

            if (mapValue.isNew()) {
                startOffset = memory.appendAddressFor(offset * Integer.BYTES) - memory.getPageAddress(0);
                firstIdx = 0;
            } else {
                startOffset = mapValue.getLong(0);
                firstIdx = mapValue.getLong(1);
                count = mapValue.getLong(2);
            }

            if (doPass1(count, startOffset, firstIdx, record, recordOffset, spi)) {
                firstIdx++;
                count++;
            }

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, firstIdx % offset);
            mapValue.putLong(2, count);
        }

        private boolean doPass1(long count, long startOffset, long firstIdx, Record record, long recordOffset, WindowSPI spi) {
            final int currentValue = arg.getInt(record);
            if (count < offset) {
                value = SymbolTable.VALUE_IS_NULL;
            } else {
                value = memory.getInt(startOffset + firstIdx * Integer.BYTES);
            }

            final boolean respectNulls = !ignoreNulls || currentValue != SymbolTable.VALUE_IS_NULL;
            if (respectNulls) {
                memory.putInt(startOffset + firstIdx * Integer.BYTES, currentValue);
            }
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
            return respectNulls;
        }
    }

    static class LeadLagCurrentRowFunction extends BaseSymbolWindowFunction {
        private final boolean ignoreNulls;
        private final String name;
        private final VirtualRecord partitionByRecord;

        public LeadLagCurrentRowFunction(VirtualRecord partitionByRecord, Function arg, String name, boolean ignoreNulls) {
            super(arg, null, 0, ignoreNulls);
            this.partitionByRecord = partitionByRecord;
            this.name = name;
            this.ignoreNulls = ignoreNulls;
        }

        @Override
        public void close() {
            super.close();
            if (partitionByRecord != null) {
                Misc.freeObjList(partitionByRecord.getFunctions());
            }
        }

        @Override
        public void computeNext(Record record) {
            value = arg.getInt(record);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putInt(spi.getAddress(recordOffset, columnIndex), value);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(", ").val(0).val(", NULL)");
            if (ignoreNulls) {
                sink.val(" ignore nulls");
            }
            sink.val(" over ()");
        }
    }

    private abstract static class BasePartitionedSymbolWindowFunction extends BaseSymbolWindowFunction implements Reopenable {
        protected final Map map;
        protected final MemoryARW memory;
        protected final VirtualRecord partitionByRecord;
        protected final RecordSink partitionBySink;

        private BasePartitionedSymbolWindowFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                MemoryARW memory,
                Function arg,
                boolean ignoreNulls,
                Function defaultValue,
                long offset
        ) {
            super(arg, defaultValue, offset, ignoreNulls);
            this.map = map;
            this.partitionByRecord = partitionByRecord;
            this.partitionBySink = partitionBySink;
            this.memory = memory;
        }

        @Override
        public void close() {
            super.close();
            Misc.free(map);
            Misc.free(memory);
            Misc.freeObjList(partitionByRecord.getFunctions());
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            Function.init(partitionByRecord.getFunctions(), symbolTableSource, executionContext, null);
        }

        @Override
        public void reopen() {
            if (map != null) {
                map.reopen();
            }
            value = SymbolTable.VALUE_IS_NULL;
        }

        @Override
        public void reset() {
            super.reset();
            Misc.free(map);
            Misc.free(memory);
        }

        @Override
        public void toPlan(PlanSink sink) {
            toPlanArgs(sink);
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            Misc.clear(map);
            memory.truncate();
        }
    }
}
