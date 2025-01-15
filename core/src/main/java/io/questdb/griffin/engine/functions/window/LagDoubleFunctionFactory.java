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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class LagDoubleFunctionFactory extends AbstractWindowFunctionFactory {

    private static final String NAME = "lag";
    private static final String SIGNATURE = NAME + "(DV)";
    public static final ArrayColumnTypes LAG_COLUMN_TYPES;

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final WindowContext windowContext = sqlExecutionContext.getWindowContext();
        if (windowContext.isEmpty()) {
            throw SqlException.emptyWindowContext(position);
        }

        if (args.size() > 3) {
            throw SqlException.$(argPositions.getQuick(3), "too many arguments");
        }

        long offset = 1;
        if (args.size() >= 2) {
            final Function offsetFunc = args.getQuick(1);
            if (!offsetFunc.isConstant() && !offsetFunc.isRuntimeConstant()) {
                throw SqlException.$(argPositions.getQuick(1), "offset must be a constant");
            }

            offset = offsetFunc.getLong(null);
            if (offset < 0) {
                throw SqlException.$(argPositions.getQuick(1), "offset must be a positive integer");
            }
        }

        Function defaultValue = null;
        if (args.size() == 3) {
            defaultValue = args.getQuick(2);
            if (!(defaultValue instanceof DoubleFunction)) {
                throw SqlException.$(argPositions.getQuick(2), "default value must be a double");
            }

            if (defaultValue instanceof WindowFunction) {
                throw SqlException.$(argPositions.getQuick(2), "default value can not be a window function");
            }
        }

        if (offset == 0) {
            return new LeadLagValueCurrentRow(args.get(0), NAME, windowContext.isIgnoreNulls());
        }

        if (windowContext.getPartitionByRecord() != null) {
            Map map = MapFactory.createUnorderedMap(
                    configuration,
                    windowContext.getPartitionByKeyTypes(),
                    LAG_COLUMN_TYPES
            );
            MemoryARW mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER
            );

            return new LagOverPartitionFunction(
                    map,
                    windowContext.getPartitionByRecord(),
                    windowContext.getPartitionBySink(),
                    mem,
                    args.get(0),
                    windowContext.isIgnoreNulls(),
                    defaultValue,
                    offset
            );
        }

        MemoryARW mem = Vm.getCARWInstance(
                configuration.getSqlWindowStorePageSize(),
                configuration.getSqlWindowStoreMaxPages(),
                MemoryTag.NATIVE_CIRCULAR_BUFFER
        );
        return new LagFunction(args.get(0), defaultValue, offset, mem, windowContext.isIgnoreNulls());
    }

    static class LagOverPartitionFunction extends BasePartitionedDoubleWindowFunction {
        private final Function defaultValue;
        private final long offset;
        private double lagValue;
        private final MemoryARW memory;
        private final boolean ignoreNulls;

        public LagOverPartitionFunction(Map map,
                                        VirtualRecord partitionByRecord,
                                        RecordSink partitionBySink,
                                        MemoryARW memory,
                                        Function arg,
                                        boolean ignoreNulls,
                                        Function defaultValue,
                                        long offset) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.defaultValue = defaultValue;
            this.offset = offset;
            this.memory = memory;
            this.ignoreNulls = ignoreNulls;
        }

        @Override
        public boolean isIgnoreNulls() {
            return ignoreNulls;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();

            long startOffset;
            long firstIdx;
            long count = 0;
            double d = arg.getDouble(record);

            if (mapValue.isNew()) {
                startOffset = memory.appendAddressFor(offset * Double.BYTES) - memory.getPageAddress(0);
                firstIdx = 0;
            } else {
                startOffset = mapValue.getLong(0);
                firstIdx = mapValue.getLong(1);
                count = mapValue.getLong(2);
            }
            if (count < offset) {
                lagValue = defaultValue == null ? Double.NaN : defaultValue.getDouble(record);
            } else {
                lagValue = memory.getDouble(startOffset + firstIdx * Double.BYTES);
            }

            if (!ignoreNulls || Numbers.isFinite(d)) {
                memory.putDouble(startOffset + firstIdx * Double.BYTES, d);
                firstIdx++;
                count++;
            }
            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, firstIdx % offset);
            mapValue.putLong(2, count);
        }

        @Override
        public double getDouble(Record rec) {
            return lagValue;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), lagValue);
        }

        @Override
        public void close() {
            super.close();
            Misc.free(memory);
        }

        @Override
        public void reset() {
            super.reset();
            Misc.free(memory);
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
        }

        @Override
        public void toPlan(PlanSink sink) {
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
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }
    }

    public static class LagFunction extends BaseDoubleWindowFunction implements Reopenable {
        private final MemoryARW buffer;
        private final long offset;
        private double lagValue;
        private final Function defaultValue;
        private int loIdx = 0;
        private long count = 0;
        private final boolean ignoreNulls;

        public LagFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls) {
            super(arg);
            this.offset = offset;
            this.buffer = memory;
            this.defaultValue = defaultValueFunc;
            this.ignoreNulls = ignoreNulls;
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
        }

        @Override
        public boolean isIgnoreNulls() {
            return ignoreNulls;
        }

        @Override
        public void computeNext(Record record) {
            if (count < offset) {
                lagValue = defaultValue == null ? Double.NaN : defaultValue.getDouble(record);
            } else {
                lagValue = buffer.getDouble((long) loIdx * Double.BYTES);
            }
            double d = arg.getDouble(record);
            if (!ignoreNulls || Numbers.isFinite(d)) {
                buffer.putDouble((long) loIdx * Double.BYTES, d);
                loIdx = (int) ((loIdx + 1) % offset);
                count++;
            }
        }

        @Override
        public double getDouble(Record rec) {
            return lagValue;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), lagValue);
        }

        @Override
        public void reopen() {
            loIdx = 0;
            count = 0;
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            loIdx = 0;
            count = 0;
        }

        @Override
        public void toTop() {
            super.toTop();
            loIdx = 0;
            count = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
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
            sink.val(" over ()");
        }
    }

    static class LeadLagValueCurrentRow extends BaseDoubleWindowFunction {
        private double value;
        private final String name;
        private final boolean ignoreNulls;

        public LeadLagValueCurrentRow(Function arg, String name, boolean ignoreNulls) {
            super(arg);
            this.name = name;
            this.ignoreNulls = ignoreNulls;
        }

        @Override
        public void computeNext(Record record) {
            value = arg.getDouble(record);
        }

        @Override
        public double getDouble(Record rec) {
            return value;
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
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), value);
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

    static {
        LAG_COLUMN_TYPES = new ArrayColumnTypes();
        LAG_COLUMN_TYPES.add(ColumnType.LONG); // position of current oldest element
        LAG_COLUMN_TYPES.add(ColumnType.LONG); // start offset of native array
        LAG_COLUMN_TYPES.add(ColumnType.LONG); // count
    }
}
